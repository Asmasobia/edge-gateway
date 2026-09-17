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

**Slice 1 of 4 complete: timeout budgets.** The remaining slices are the circuit breaker, the
bulkhead, and the HTTP surface that composes all three.

| | Slice | State |
|---|---|---|
| 1 | Deadlines / timeout-budget propagation | ✅ done — 42 tests, 10/10 mutants killed |
| 2 | Circuit breaker (sliding window, half-open with bounded trials) | ⬜ not started |
| 3 | Bulkhead (bounded concurrency + bounded queue) | ⬜ not started |
| 4 | Composition + HTTP proxy, failure classification | ⬜ not started |

## Quick start

Requires Python 3.12+. Nothing else — the resilience core has no runtime dependencies, so there is no
install step, no service to start, and no network access needed.

```bash
git clone <this repo> && cd edge-gateway
python -m pytest                       # 42 tests, ~0.2s — none of them sleep
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

## Verification

Claims here are limited to what was actually run on the development machine.

**Verified:** 42 unit tests passing; `ruff check` and `ruff format --check` clean; `mypy --strict`
clean across 5 files; and the suite mutation-tested with 10 reintroduced defects, **all 10 caught**:

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

**Not verified, and not claimed:** there is no HTTP surface yet, so nothing here has served a real
request; no load test, no cluster, no cross-host clock-skew experiment. Those belong to slice 4 and to
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

## Layout

```
src/edge_gateway/
  resilience/          the patterns — pure Python, no dependencies, no I/O
    clock.py           injectable monotonic time source
    deadline.py        timeout budgets and their propagation
tests/
  test_deadline.py     42 tests, none of which sleep
DECISIONS.md           architecture decision records
```
