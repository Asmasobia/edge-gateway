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

**Slices 1–3 of 4 complete: timeout budgets, circuit breaking and bulkheads.** The remaining slice is
the HTTP surface that composes all three.

| | Slice | State |
|---|---|---|
| 1 | Deadlines / timeout-budget propagation | ✅ done — 42 tests, 10/10 mutants killed |
| 2 | Circuit breaker (sliding window, half-open with bounded trials) | ✅ done — 68 tests, 20/20 mutants killed |
| 3 | Bulkhead (bounded concurrency + bounded queue) | ✅ done — 32 tests, 22/22 mutants killed |
| 4 | Composition + HTTP proxy, failure classification | ⬜ not started |

## Quick start

Requires Python 3.12+. The resilience core itself has no runtime dependencies — no service to start
and no network access needed — so the only install is the test and lint toolchain.

```bash
git clone <this repo> && cd edge-gateway
python -m pip install -e ".[dev]"      # pytest, pytest-asyncio, ruff, mypy
python -m pytest                       # 142 tests, ~0.3s — none of them sleep
python -m ruff check . && python -m ruff format --check .
python -m mypy --strict src tests
```

`pytest-asyncio` is needed from slice 3 onward: the bulkhead is the first component that can wait, so
the suite has async tests, and `pyproject.toml` sets `asyncio_mode = "strict"` (see ADR-034 for why
`strict` and not `auto`). The project itself is still never *built* to run the tests — `pythonpath`
points at `src/`, so the editable install above is for the tools, not to make imports work.

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

## The idea: a bulkhead limits how *many*, and a deadline limits how *long*

The name is from shipbuilding: a hull divided into sealed compartments, so a breach floods one
compartment instead of the ship. Applied to a service, the compartments are shares of finite capacity —
worker slots, connections, event-loop concurrency — one share per dependency.

The failure it exists for is worth spelling out, because the interesting part is that **nothing else is
broken when it happens.** Search degrades from 50 ms to 30 s. Every call still completes inside its 30 s
budget: no deadline is violated, no timeout fires, the timeout code has no bug. But each call now
occupies a worker 600× longer than it used to, so within a minute every worker in the process is parked
inside a search call. Payments traffic — healthy, fast, unrelated — queues behind it and times out.

"Each call is limited to 30 s" and "all capacity is held by search" are entirely compatible statements.
That is the whole reason these are two components and not one setting: a deadline bounds the duration of
one call and says nothing about how many calls hold capacity at once.

```
without a bulkhead                    with a bulkhead
------------------                    ---------------
search   ████████████████████ (all)   search   ████████░░░░░░░░░░░░ (capped at its share)
payments ░░░░░░░░░░░░░░░░░░░░ (none)  payments ████░░░░░░░░░░░░░░░░ (served normally)
```

### Why the queue must be bounded, and small

Bounded concurrency on its own is one line — `asyncio.Semaphore(n)` — and it is not enough, because a
semaphore's waiter list is unlimited. That does not remove the dependency's backlog; it *relocates* it
into this process, where it costs memory, holds a client connection open per entry, and grows latency
without growing throughput. Throughput is set by the dependency's capacity, not by how many callers are
hopeful. A queue is good at exactly one thing: absorbing a burst the pool will drain shortly.

So the queue is bounded, and its depth is arithmetic rather than taste. With per-call latency `L` and
concurrency `C`, the pool completes `C / L` calls per second, so the last entrant to a queue of depth `Q`
waits about:

```
Q * L / C        e.g. 100 * 0.05 / 20 = 250ms of pure queueing, before the call even starts
```

Pick `Q` from the queueing latency you are willing to add. If the answer is "none", `max_queue=0` is
legal and says so exactly. Two more sizing notes:

- **`max_concurrent` must be smaller than the process's total capacity, but the *sum* across dependencies
  is allowed to exceed it.** Sizing the shares so they add up exactly leaves capacity idle whenever a
  dependency is quiet, which is most of the time. Deliberate over-subscription with each share capped is
  the design, not an oversight.
- **A rejection is not an outage.** `BulkheadFullError` during a spike is the component working: it sheds
  load so that the requests which are served are served properly. Alert on the rejection *rate*, never on
  individual occurrences. And a rejection is never evidence against the dependency — the call was not
  made — which is why it is classified `NOT_COUNTED`. Otherwise a queue sized slightly too small could
  trip a circuit breaker on a service that was healthy the entire time.

### Why cancellation is the hard part

`acquire()` has no `timeout` parameter. `asyncio.timeout` already composes with any awaitable, and a
caller holding a `Deadline` already knows its remaining budget, so a second abandonment mechanism would
add no capability and would make "who gave up, and against which budget" unanswerable from the metrics.

That makes cancellation *the* mechanism rather than an edge case — and a caller can vanish at two
different moments, which need opposite responses:

| Moment | Response | What happens if you get it wrong |
|---|---|---|
| Cancelled while queued | Remove it from the queue | The queue fills with ghosts, and live callers are rejected on behalf of callers that left |
| Cancelled after a slot was granted, before resuming | **Pass the slot on** to the next waiter | The slot is gone permanently |

The second one is the dangerous one, because it is silent. A dropped slot is never recovered: usable
capacity falls by one for the lifetime of the process, and after `max_concurrent` such events the
bulkhead rejects everything while truthfully reporting itself full — no failing dependency to blame, and
nothing in the logs. It is the same failure class as an unresolved permit in the breaker: *a resource
whose release depends on control returning to code that may never run.* Recognising the class is what
makes it findable in review.

The cleanup catches `BaseException`, not `CancelledError`, because `coroutine.close()` raises
`GeneratorExit` — an abandoned coroutine that reached the queue and was then dropped must not leave a
ghost either. There is a test for precisely that.

### Other decisions worth knowing

- **A freed slot is handed directly to the head of the queue.** The obvious implementation — decrement
  the in-flight count, wake a waiter, let it re-check capacity — opens a window in which a brand-new
  caller takes the fast path and walks off with capacity already promised to somebody who has been
  waiting. Under load that is the normal interleaving, not a rare one, and the queue starves. With direct
  handover the count never dips, so no such instant exists and FIFO fairness needs no fairness mechanism.
- **FIFO, and the argument against it is real.** LIFO has genuinely better goodput under sustained
  overload — the newest request is the one most likely to still have a client attached, while the oldest
  has probably been abandoned upstream. FIFO wins here anyway because it cannot starve anyone, and
  because a correctly-sized small queue does not hold a request long enough for the difference to be
  worth the surprise of "your request waited while later ones were served".
- **`admitted` counts calls that got a slot they could actually use**, so it is incremented on resumption
  rather than at handover. Counting at handover would inflate it with calls that never ran — and would do
  so worst under overload, when cancellations cluster, so the metric would look *better* as the incident
  deepened. `cancelled_at_handover` is tracked separately, and is also the tell for the silent capacity
  leak above.
- **High-water marks, not just averages.** Mean queue depth is approximately zero right up until it
  isn't, and a one-second shedding spike vanishes into a per-minute average, so a 15 s or 60 s scrape
  cannot see the thing that hurt. `peak_queued` is the number to size `max_queue` from.
  `queue_wait_max_seconds` earns its place twice over: queue wait happens entirely *before* the outbound
  call, so it appears in no dependency's latency dashboard. It is the number that distinguishes "the
  dependency is slow" from "we are the ones being slow".
- **`release()` is strict, `__exit__` is tolerant.** A double release returns capacity that was never
  taken, so the pool would admit more concurrency than configured — the exact failure the bulkhead exists
  to prevent, reached from the inside, silently — so it raises. `__exit__` has a different job, which is
  to guarantee the slot comes back; being strict there would crash *during unwinding* and replace a real
  exception with a bookkeeping complaint.
- **Not thread-safe, deliberately** — same reasoning as the breaker. Every mutation is straight-line with
  no `await` between reading and writing state, which is a property to preserve when editing it.

## Verification

Claims here are limited to what was actually run on the development machine.

**Verified:** 142 unit tests passing in ~0.3 s; `ruff check` and `ruff format --check` clean;
`mypy --strict` clean across 11 files; and all three slices mutation-tested, with **every** reintroduced
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

Slice 3 — the bulkhead, 22 mutants, 22 caught. Three of them are caught **by a hang** rather than by an
assertion, and that is worth reading as a result rather than as a footnote: each of those three breaks the
handover, which parks a waiter on a future that nobody will ever resolve. A hang is a detected difference
— CI times out and goes red — so it counts as caught, but it is a worse signal than a failure, because
what it tells you is "something is stuck somewhere" rather than "this behaviour changed". Reported
distinctly for that reason.

| Mutant | Caught by |
|---|---|
| Pool limit off by one (admits `max_concurrent + 1`) | `test_a_call_beyond_the_limit_waits_in_the_queue` |
| Queue limit off by one (queues one deeper than configured) | a hang |
| A rejection is not counted | `test_a_full_pool_and_a_full_queue_reject_immediately` |
| A cancelled waiter is left in the queue | `test_a_freed_slot_goes_to_the_queue_and_not_to_a_newcomer` |
| A cancellation at handover drops the slot instead of passing it on | a hang |
| A cancellation at handover treated as a plain queued cancellation | `test_a_waiter_cancelled_after_being_granted_a_slot_passes_it_on` |
| `except BaseException` narrowed to `CancelledError` (`GeneratorExit` leaks a queue entry) | `test_a_freed_slot_goes_to_the_queue_and_not_to_a_newcomer` |
| A released slot is always freed and never handed over | a hang |
| A handover *also* frees the slot (double release) | `test_a_call_beyond_the_limit_waits_in_the_queue` |
| A cancelled future in the queue is granted a slot anyway | `test_the_pool_survives_a_cancellation_storm` |
| The handover does not report that it happened | `test_a_call_beyond_the_limit_waits_in_the_queue` |
| Double release permitted | `test_releasing_a_slot_twice_raises` |
| `__exit__` does not release the slot | `test_the_context_manager_releases_the_slot` |
| Admission counted at handover instead of on resumption | `test_a_call_admitted_after_queueing_is_counted_as_an_admission` |
| A resumed waiter double-counts its own slot | `test_a_call_beyond_the_limit_waits_in_the_queue` |
| Queue wait is never measured | `test_queue_wait_is_measured` |
| Queue-wait maximum overwritten instead of maximised | `test_queue_wait_is_measured` |
| Peak in-flight not tracked | `test_high_water_marks_record_the_peak_and_not_the_average` |
| Peak queue depth not tracked | `test_high_water_marks_record_the_peak_and_not_the_average` |
| `queued_total` also counts calls that never waited | `test_calls_up_to_the_limit_are_admitted_without_waiting` |
| `max_concurrent` validation removed | `test_construction_rejects_impossible_limits` |
| `max_queue` validation removed | `test_construction_rejects_impossible_limits` |

**Not verified, and not claimed:** there is no HTTP surface yet, so nothing here has served a real
request; no load test, no cluster, no cross-host clock-skew experiment. The bulkhead tests do run many
tasks concurrently on a real event loop, including cancellation storms — but they run no I/O and no
threads, so the "not thread-safe, single event loop" claim remains an argument about the code rather
than a measurement, and so do the sizing formulas above, which are arithmetic and not benchmarks. Those
belong to slice 4 and to the sibling reliability project.

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

### And two more, in the bulkhead

The slice 3 matrix ran 20/22 on its first pass. Both survivors were test gaps of the same shape as the
one above — a test that constrained less than its name implied — and both are the kind you cannot see by
reading:

- **`admitted` counted at handover survived.** Every existing assertion about `admitted` happened to use
  a queue-free configuration, so no test ever observed the increment on the *queued* path at all. Fixed
  by a test that queues first and then checks the count.
- **Overwriting `queue_wait_max_seconds` instead of maximising it survived**, because the test's two waits
  were 0.4 s then 0.5 s — under which "overwrite" and "take the maximum" produce the same answer. The
  test was rewritten with the *longer* wait first, which is the only ordering that can tell them apart.

The general lesson, restated because slice 3 found it independently: a test that exercises the right code
can still be blind to the defect, either because the configuration avoids the path or because the fixture
values make two implementations agree. Neither is visible on the page.

## Layout

```
src/edge_gateway/
  resilience/          the patterns — pure Python, no dependencies, no I/O
    clock.py           injectable monotonic time source
    deadline.py        timeout budgets and their propagation
    window.py          bounded-memory sliding window of successes and failures
    breaker.py         the circuit breaker and its permits
    bulkhead.py        bounded concurrency with a bounded queue
tests/
  test_deadline.py     42 tests
  test_window.py       22 tests
  test_breaker.py      46 tests
  test_bulkhead.py     32 tests
DECISIONS.md           architecture decision records (ADR-001 … ADR-034)
```

None of the 142 tests sleep. The async ones drive concurrency with `create_task` plus `await
asyncio.sleep(0)` — a yield to the scheduler, not a wait — and provoke abandonment with `task.cancel()`
directly instead of with a timer, which is both faster and far more precise than hoping a timeout lands
in the right window.
