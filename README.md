# edge-gateway

A resilience-aware edge gateway, built to demonstrate three patterns that are usually adopted as
configuration and therefore usually misunderstood:

| Pattern | What it prevents |
|---|---|
| **Timeout budgets** (deadline propagation) | Work continuing on behalf of a client that has already given up |
| **Circuit breaking** | Hammering a dependency that is already failing, and turning its outage into yours |
| **Bulkheads** | One slow dependency consuming all the capacity that healthy ones need |

The patterns are implemented here rather than imported, because the point of the project is to be able
to explain *why* a half-open circuit must limit trial concurrency, or *why* a budget-exhausted refusal
must not count as a dependency failure. A `timeout: 1s` line in a YAML file demonstrates neither.

## Status

**Slices 1–2 of 4 complete: timeout budgets and circuit breaking.** The remaining slices are the
bulkhead and the HTTP surface that composes all three.

| | Slice | State |
|---|---|---|
| 1 | Deadlines / timeout-budget propagation | ✅ done — 42 tests, 10/10 mutants killed |
| 2 | Circuit breaker (sliding window, half-open with bounded trials) | ✅ done — 68 tests, 20/20 mutants killed |
| 3 | Bulkhead (bounded concurrency + bounded queue) | ⬜ not started |
| 4 | Composition + HTTP proxy, failure classification | ⬜ not started |

## Quick start

Requires Python 3.12+. Nothing else — the resilience core has no runtime dependencies, so there is no
install step, no service to start, and no network access needed.

```bash
git clone <this repo> && cd edge-gateway
python -m pytest                       # 110 tests, ~0.2s — none of them sleep
python -m pip install -e ".[dev]"      # only needed for ruff / mypy
python -m ruff check . && python -m ruff format --check .
python -m mypy --strict src tests
```

## The idea: budgets, not timeouts

Nearly every service configures timeouts *per call* — "the call to pricing times out after 1 second".
That statement is local, and it composes badly. A client with a 1 s expectation calling a chain where
every hop is "configured for 1 second":

```
client(1s) -> gateway(1s) -> A(1s) -> B(1s) -> C(1s)
```

The worst case is 4 seconds, not 1. Each hop restarts its own stopwatch. The client gave up after 1 s
and disconnected; A, B and C are still working — holding a connection, a worker and a database
transaction each — on behalf of nobody. Under load this is the mechanism by which one slow dependency
takes down healthy services: they spend all their capacity computing answers nobody will read.

A budget makes the limit a property of the **request** instead of each call. It is decided once at the
edge and then spent down:

```
client(1s) -> gateway: 1000ms left -> A: 940ms left -> B: 880ms left -> C: 820ms left
```

Two things follow. The whole call tree now finishes or is abandoned inside the original 1 s. And — the
more valuable half — every hop *knows how much time it has*, so it can decline work it cannot finish:

```python
if not deadline.is_viable(minimum_ms=50, reserve_ms=20):
    return  # do not even attempt the call
```

Refusing locally when 3 ms remain is not just an optimisation. Making the call would cost a connection
and a bulkhead slot that a request with a real chance could have used; the dependency would do the work
and have its answer discarded; and the resulting timeout would be recorded as *that dependency's*
failure and count toward its circuit breaker. A stingy client could then trip a breaker and cause
traffic from unrelated callers to be shed, while the dependency was healthy the whole time.

### Design decisions worth knowing

- **Deadline, not timeout.** Stored internally as an absolute instant, so passing it down cannot
  extend it. There is no field holding "the timeout".
- **The wire carries remaining milliseconds, not an absolute time.** An absolute deadline would either
  be a monotonic value (meaningless in another process) or a wall-clock one (correct only while every
  host's clock agrees — it fails in the dangerous direction, where a host whose clock runs ahead
  rejects all traffic as already expired). This is the trade gRPC makes with `grpc-timeout`.
- **Monotonic clock, injected.** Wall-clock time can move backwards; an NTP correction would either
  extend live deadlines or expire all of them at once. Injecting the clock is also what makes "the
  circuit has been open for 30 seconds" a one-line test rather than a 30-second one. No test sleeps.
- **A per-hop reserve is withheld before propagating.** Hand downstream 100% of what remains and the
  failure mode is downstream succeeding on the final millisecond while this hop blows the client's
  deadline formatting the reply — the worst outcome available, since the expensive work was done and
  then discarded. The reserve also absorbs the network transit the wire format cannot charge for.
- **The incoming budget is clamped.** `max_ms` is a trust boundary, not a tuning knob: the value
  arrives in a header, so without a ceiling `x-timeout-budget-ms: 86400000` pins a worker, a
  connection and a bulkhead slot for a day at no cost to the sender.
- **A malformed budget degrades to the default instead of returning 400.** The budget is an
  optimisation hint; failing a request the service could otherwise serve turns one caller's header bug
  into a user-visible outage. It is reported through `BudgetSource.MALFORMED` so it is loud in metrics
  and harmless in production.
- **`BudgetSource` is returned alongside the number.** `DEFAULTED` must be distinguishable from
  `INHERITED` — otherwise internal traffic that has silently lost its budget header looks identical to
  traffic that inherited one, and broken propagation is invisible while the whole scheme quietly stops
  working.

## The idea: a circuit breaker is a hypothesis, not a counter

A dependency starts failing. Every call now takes the full timeout and then errors. Each one still
costs a connection from the pool and a worker for the duration of that timeout — three times both if
there are retries — so the caller's capacity drains into a service that cannot use it, and the caller
begins failing requests that had nothing to do with the broken dependency. That is what a cascading
failure *is*: an outage spreading outward from the thing that actually broke.

There is a second cost that gets less attention. The failing dependency is now receiving its full
request load plus retries at exactly the moment it can least serve it. If it broke because it was
overloaded, its callers' persistence is what prevents it from recovering. A dependency can be held
underwater indefinitely by clients faithfully retrying.

Failing fast locally solves both. Fast local failure is not a worse outcome than a slow remote
failure — it is the *same* outcome, without paying for it.

```
CLOSED ──(failure rate too high, with enough traffic to mean it)──▶ OPEN
OPEN ──(cooldown elapsed)──▶ HALF_OPEN
HALF_OPEN ──(enough trial calls succeeded)──▶ CLOSED
HALF_OPEN ──(any trial call failed)──▶ OPEN   [cooldown restarts]
```

That much is standard. The four decisions below are where naive implementations go wrong, and each
one has a test named after it.

**1. A rate over a window, not consecutive failures.** "Five failures in a row" sounds reasonable and
is nearly useless: a dependency failing 50% of the time — catastrophic — may never produce five in a
row, so the breaker never fires. Partial failure is the common case in distributed systems.

**2. A minimum throughput before the rate is allowed to mean anything.** This is the one most often
missing. At 03:00 one health check runs, it fails, the failure rate is 100%, and the breaker takes
the service offline on a sample of one. A proportion computed from n=1 is not a measurement.

**3. Bounded concurrency in HALF_OPEN.** The naive half-open lets *all* traffic through the moment the
cooldown expires. If the dependency is still fragile — which, having just been broken, is likely —
the full accumulated load arrives at once and knocks it straight back down. The breaker has become
the thundering herd it exists to prevent, on a repeating cycle.

**4. The window is reset when the circuit closes.** Skip it and recovery is impossible: the failures
that opened the circuit are still in the window, so the breaker consults it, sees the same high rate,
and re-opens. The symptom is a circuit flapping forever while the dependency is verifiably healthy,
and it is hard to see because every individual state transition looks correct.

### What must not count as a failure

A breaker is a hypothesis about one question: *is this dependency unhealthy?* Anything recorded as a
failure that is not evidence for that hypothesis makes the breaker fire when it should not — and a
breaker that fires spuriously gets its threshold raised until it never fires at all. So `Outcome` has
three values rather than two:

| Situation | Outcome | Why |
|---|---|---|
| 500, connection refused, timeout | `FAILURE` | The dependency could not serve us |
| 400, 403, 404 | `NOT_COUNTED` | *We* sent a bad request; it answered correctly and fast |
| 429 | `FAILURE` | The dependency itself is saying it cannot serve us |
| Refused locally because the time budget is exhausted | `NOT_COUNTED` | We never called it |
| Bulkhead rejection | `NOT_COUNTED` | Evidence about our own saturation, not its health |

Getting this wrong is the single most common way a resilience layer makes a system *less* reliable
than it was without one. Without a third value every outcome has to be forced into success or
failure, and the forcing is where breakers acquire their reputation for firing at random.

### Other decisions worth knowing

- **A permit object, not an `allow()` boolean.** Bounded probe concurrency requires knowing how many
  probes are *currently in flight*, which is impossible if permission is fire-and-forget. It is a
  context manager because the leak that matters is an unresolved permit: leak
  `half_open_max_concurrent` of them and the circuit can never close again for the lifetime of the
  process — with no failures recorded and nothing in the logs to explain it.
- **An unresolved permit is recorded as "no evidence", never guessed.** Guessing `FAILURE` would let
  a bug in our own code trip a breaker on a healthy dependency; guessing `SUCCESS` would hide real
  failures. It is counted in `unresolved_permits`, which should be zero forever and is therefore a
  clean thing to alert on.
- **Every permit is stamped with a generation.** A call issued while CLOSED can still be in flight
  when the circuit trips; a probe can return after the circuit has already closed. Applying that
  result would re-open a circuit that has just recovered and is serving traffic happily, *with no
  current evidence of a problem* — an incident that is close to unexplainable from the logs. Stale
  outcomes are counted and then dropped, window contribution included.
- **Bucketed counters, not a deque of timestamps.** A deque is exact, and its memory scales with
  *throughput* rather than with the window: at 20k rps over 10 s that is 200,000 live entries per
  breaker per process, so the component whose job is to protect the service under load becomes a
  source of pressure under load. The cost of buckets is granularity — the effective span varies
  between `window_seconds - bucket_seconds` and `window_seconds` — which is fine for a threshold
  comparison on an already-noisy sample and **not** fine for billing or a contractual rate quota.
  Being able to say which of those you are in is the point; "buckets are faster" is a preference.
- **No background sweeper.** Each bucket carries the epoch it was written for, so expiry is lazy and
  free: no task to schedule, no lifecycle to leak, and no dependence on real time.
- **Time-driven transitions are evaluated on access.** Reading `state` can move OPEN to HALF_OPEN. The
  alternative is a background task, which needs a lifecycle, makes the class untestable without real
  time, and introduces genuine concurrency into a class that has none. The state is *observed* rather
  than changed — the cooldown had already elapsed whether or not anyone looked.
- **Not thread-safe, deliberately.** The intended deployment is a single asyncio event loop, where
  every method runs to completion with no `await` inside and is therefore already atomic with respect
  to other coroutines. A lock would cost an acquisition on the hottest path and would silently paper
  over an actual misuse. One breaker per dependency, never one per process: a shared breaker converts
  a partial outage into a total one.
- **Every threshold is required, with no library default.** What counts as "too many failures" for a
  payments provider is not what it means for a recommendations service. `bucket_count` is the one
  exception, because it is not a policy decision about the dependency at all — it is an internal
  accuracy-versus-memory trade inside the window.

## Verification

Claims here are limited to what was actually run on the development machine.

**Verified:** 110 unit tests passing in ~0.2 s; `ruff check` and `ruff format --check` clean;
`mypy --strict` clean across 9 files; and both slices mutation-tested, with **every** reintroduced
defect caught.

Slice 1 — timeout budgets, 10 mutants, 10 caught:

| Mutant | Tests failed |
|---|---|
| Float-flooring epsilon removed | 2 |
| `math.floor(x + ε)` reverted to `int(x)` truncation | 3 |
| Strict digit regex bypassed (parse with `int()`) | 14 |
| `max_ms` clamp removed | 1 |
| Reserve not subtracted before propagating | 5 |
| `expired()` boundary loosened from `<= 0` to `< 0` | 3 |
| `is_viable` off-by-one (`>=` → `>`) | 3 |
| Ceiling misreported as CLAMPED (`>` → `>=`) | 1 |
| Header lookup made case-sensitive | 1 |
| `headers_for_downstream` returns a shared dict | 4 |

Slice 2 — circuit breaker and sliding window, 20 mutants, 20 caught. Each row is a defect a plausible
implementation could plausibly ship:

| Mutant | Caught by |
|---|---|
| Window liveness `<` loosened to `<=` (window one bucket too long) | `test_the_window_spans_exactly_bucket_count_buckets` |
| `math.floor` reverted to `int()` for the epoch | `test_expiry_is_correct_when_the_clock_origin_is_negative` |
| A reused bucket added to instead of cleared | `test_successes_alone_never_open_the_circuit` |
| Empty window reports `NaN` | `test_a_new_breaker_is_closed_and_empty` |
| `reset` clears the counts but not the epoch stamps | `test_reset_returns_the_window_to_its_constructed_state` |
| A future-stamped bucket treated as live | `test_a_bucket_stamped_in_the_future_is_not_counted` |
| Threshold comparison made exclusive | `test_a_partial_failure_rate_trips_the_circuit` |
| Minimum-throughput guard removed | `test_a_partial_failure_rate_trips_the_circuit` |
| Closing no longer resets the window (decision 4) | `test_closing_the_circuit_clears_the_evidence_that_opened_it` |
| Half-open probe cap off by one | `test_half_open_admits_only_the_configured_number_of_probes` |
| Stale outcomes applied instead of dropped | `test_an_outcome_from_before_a_trip_is_discarded_window_and_all` |
| A stale outcome also releases a probe slot | `test_a_late_probe_does_not_steal_a_slot_from_the_next_round` |
| Cooldown comparison made exclusive | `test_the_cooldown_boundary_admits_a_probe` |
| An unresolved permit leaks its probe slot | `test_an_unresolved_permit_is_released_by_the_context_manager` |
| Probe successes carry over into the next round | `test_probe_successes_do_not_carry_over_from_an_abandoned_round` |
| A failed probe no longer re-opens the circuit | `test_a_failed_probe_reopens_the_circuit_and_restarts_the_cooldown` |
| Resolving a permit twice permitted | `test_resolving_a_permit_twice_raises` |
| `Retry-After` allowed to go negative | `test_retry_after_is_never_negative` |
| `NOT_COUNTED` recorded as a failure | `test_calls_that_are_not_evidence_do_not_count_toward_throughput` |
| A config validation check removed | `test_invalid_configuration_is_rejected_at_construction` |

**Not verified, and not claimed:** there is no HTTP surface yet, so nothing here has served a real
request; no load test, no cluster, no cross-host clock-skew experiment. Nothing here has been run
under real concurrency either — the breaker is documented as single-event-loop and is not
thread-safe, and that claim is an argument rather than a measurement. Those belong to slice 4 and to
the sibling reliability project.

### A bug this found

The end-to-end test asserting "a budget can only shrink across a chain of hops" failed on first run:

```
expected [1000, 950, 900, 850]
got      [1000, 950, 899, 849]
```

`0.98 - 0.06` is `0.9199999999999999` in binary floating point, so `int(x * 1000)` truncated
`919.9999999999999` to 919 — a hop with 920 ms reported 919. The representation error is ~1e-16 s and
physically meaningless; the damage was done by **flooring**, which amplified it into a whole
millisecond, in the same direction, at every hop. A four-hop chain silently lost several milliseconds,
and the loss grew with chain length.

The fix strips sub-nanosecond noise before flooring. The general lesson is the same as rounding money:
flooring a float is only safe when the float is exact — when it is the result of arithmetic, decide
explicitly what counts as noise and remove it before the operation that magnifies it.

Worth noting that a test written with tidy values (0.5, 0.25) would not have caught this: those are
exactly representable in binary.

### A test this found

Mutation testing slice 2 turned up something more uncomfortable than a bug: a passing test that
proved nothing. Deleting `self._window.reset()` from the breaker's close path — decision 4, the one
without which recovery is impossible — left the entire suite green.

The test looked right. It tripped the circuit with four failures, waited out the 30-second cooldown,
sent two successful probes, and asserted that a single failure afterwards did not re-open the circuit.
The flaw was in the numbers: the default window is 10 seconds and the default cooldown is 30, so the
four failures had already expired *on their own* while the circuit was open. The reset had nothing
left to clear, and a test that cannot distinguish the two implementations is not testing the line it
names.

Fixing it meant making the window (60 s) outlive the cooldown (30 s) — which is also the more
dangerous real-world configuration, and the one under which a missing reset actually bites.

The general lesson: a test can pass for a reason unrelated to the behaviour it claims to cover, and
reading it will not tell you, because reading it is how it got written. Deleting the code and watching
the test still pass is what tells you.

The same run flagged one line as genuinely unconstrained — clearing the epoch stamps in
`SlidingWindow.reset`, which is unobservable through the public surface because a bucket with zero
counts contributes nothing whether its stamp is stale or not. Rather than delete it or leave it
unconstrained, the invariant it maintains ("a reset window is indistinguishable from a new one") is
now asserted directly against the internals, with the reasoning written down in the test.

## Layout

```
src/edge_gateway/
  resilience/          the patterns — pure Python, no dependencies, no I/O
    clock.py           injectable monotonic time source
    deadline.py        timeout budgets and their propagation
    window.py          bounded-memory sliding window of successes and failures
    breaker.py         the circuit breaker and its permits
tests/
  test_deadline.py     42 tests
  test_window.py       22 tests
  test_breaker.py      46 tests
DECISIONS.md           architecture decision records
```

None of the 110 tests sleep.
