# Architecture decision records

One entry per decision that a reviewer might reasonably have made differently. The value of an ADR is
in the *rejected* alternative and the cost accepted — a record that only states what was built is a
changelog. Entries are append-only; a reversal gets a new ADR that supersedes the old one rather than
an edit, because the reasoning that turned out to be wrong is the useful part.

---

## ADR-001 — Implement the resilience patterns rather than adopt a library

**Status:** accepted (slice 1)

**Context.** Timeout budgets, circuit breakers and bulkheads are all available off the shelf:
`resilience4j` on the JVM, Polly on .NET, `pybreaker` / `purgatory` in Python, and — at the
infrastructure layer — Envoy and Istio, where all three are configuration rather than code.

**Decision.** Implement them here, in-process, with no third-party resilience dependency.

**Why.** In production the right answer is usually the opposite: use Envoy, because outlier detection
that has survived years of real traffic is worth more than anything written in an afternoon. But the
purpose of this project is to be able to *reason* about the patterns — why a half-open circuit must
bound trial concurrency, why a bulkhead rejection must not count as a dependency failure, why a
budget-exhausted call should be refused locally. Those are precisely the questions that a
`consecutive_5xx: 5` line in a YAML file lets you avoid ever answering.

**Cost accepted.** This code is less battle-tested than any library named above and should not be
preferred to them for real traffic. That is stated plainly in the README rather than glossed.

**Rejected alternative.** Wrapping a library and writing about the wrapping. It would demonstrate
integration and hide every decision that is actually interesting.

---

## ADR-002 — The resilience core has no runtime dependencies

**Status:** accepted (slice 1)

**Context.** The project will eventually serve HTTP, which means FastAPI/Starlette, `httpx` and
`uvicorn`. The tempting shape is to build the patterns directly against those types — a middleware
that reads `Request.headers`, a breaker that catches `httpx.HTTPStatusError`.

**Decision.** `edge_gateway.resilience` imports nothing outside the standard library and performs no
I/O. `edge_gateway.gateway` — the HTTP surface, slice 4 — is the only place framework types appear.
`pyproject.toml` declares `dependencies = []` today; the web stack is added in the slice that needs it,
because a dependency list that names things nothing imports has stopped describing the software.

**Why.**

1. **Testability.** The patterns can be driven directly. Reaching "the circuit is half-open and the
   trial call fails" is three function calls, not a server, a fault-injecting stub, and a sleep.
2. **The interesting behaviour is not HTTP-shaped.** A deadline shrinking across hops is arithmetic; a
   breaker's state machine is a state machine. Coupling either to `Request` mixes the logic being
   demonstrated with the transport carrying it, and the reader can no longer tell which is which.
3. **`git clone && pytest` works.** No install step, no network, no service to start. Worth more than
   it sounds: a test suite that needs a working package index is a test suite that stops running the
   moment the network is restricted, and "the tests don't run here" is how a suite quietly dies.

**Cost accepted.** The boundary has to be crossed explicitly in slice 4 (adapting real headers into
the `Mapping[str, str]` this code takes, classifying real exceptions into failure categories). That
adapter is a genuine extra layer. It is also the right place for a genuine extra test.

---

## ADR-003 — Time is an injected dependency, and it is monotonic

**Status:** accepted (slice 1)

**Context.** Every pattern here is defined in terms of elapsed time. The default is to call
`time.monotonic()` where it is needed.

**Decision.** A one-method `Clock` Protocol. Production passes `MonotonicClock`; tests pass
`ManualClock`, which moves only when told and refuses to move backwards.

**Why injected.** Otherwise tests must actually wait, and that fails in three escalating ways: the
suite gets slow, so it gets run less (or the shipped timeout gets lowered purely to make tests
bearable, at which point the tested value is not the shipped value); the suite gets flaky, because
"roughly 30 seconds" needs a tolerance wide enough to absorb slow CI, and a fortnightly failure gets
retried rather than read; and some states become unreachable — being *inside* a half-open window at a
controlled instant is a race against a real clock and a single `clock.advance(30)` with a fake.

**Why monotonic.** `time.time()` can move backwards: NTP correction, VM resume from suspend, an
operator fixing a drifted clock, a leap-second smear. Both directions are bad, and one is worse.
Backwards: `remaining` on a live deadline *grows*, and a request that should have been abandoned keeps
running. Forwards: every in-flight deadline expires at once and the gateway sheds all traffic
simultaneously — a routine clock correction becomes an outage.

**Cost accepted.** A `clock=` parameter threaded through every constructor, and the discipline never to
call `time.monotonic()` directly. Enforced by review only; there is no lint rule for it.

**Note on `ManualClock` starting at 0.0.** Failure messages read better (`expected 0.4, got 0.35`
against `expected 913847.4, got 913847.35`), and code that mistakes a monotonic reading for a UNIX
timestamp surfaces as a date in 1970 instead of hiding behind a plausible number.

---

## ADR-004 — A deadline (absolute instant), not a timeout (duration)

**Status:** accepted (slice 1)

**Context.** The limit on a request can be modelled either as a duration each hop is allowed, or as an
instant by which the whole request must be finished.

**Decision.** `Deadline` stores `expires_at`, an absolute monotonic instant. There is no field holding
a duration, and no method that extends one.

**Why.** A duration resets every time it is passed on — that is not an implementation detail of
timeouts, it is what a duration *means* relative to whenever counting starts. Four hops each
"configured for 1 second" therefore permit 4 seconds, and the client that gave up at 1 s leaves three
services holding a connection, a worker and a database transaction each, computing an answer nobody
will read. Under load that is the mechanism by which one slow dependency exhausts healthy services.
Retries make it multiplicative rather than merely additive.

An absolute instant cannot be extended by being passed down. The type makes the invariant structural
instead of aspirational. Reinforced by `@dataclass(frozen=True)`: no handler can quietly buy itself
more time. (`slots=True` too — one of these is allocated per request per hop.)

**Rejected alternative.** A mutable `remaining_ms` counter decremented by each hop. Equivalent when
correct, but every hop must remember to decrement, and a hop that forgets silently grants itself a
fresh budget. Absolute expiry has no step anyone can forget.

---

## ADR-005 — The wire format is remaining milliseconds, not an absolute timestamp

**Status:** accepted (slice 1)

**Context.** The model is an absolute instant (ADR-004), so the obvious wire representation is that
instant. It is also wrong, twice over.

**Decision.** The header `x-timeout-budget-ms` carries a *duration* — the milliseconds remaining at the
moment of sending. Each receiver converts it back to an absolute deadline against its own clock. This
is the trade gRPC makes with `grpc-timeout`.

**Why not a monotonic instant.** `time.monotonic()` has a per-process arbitrary origin. The number is
meaningless in another process, even on the same machine.

**Why not a wall-clock instant.** It would work, and only while every host's clock agrees. Correctness
would depend on NTP, and the failure is not graceful: a receiver whose clock runs 5 seconds ahead sees
every arriving deadline as already expired and rejects *all* traffic. A silent per-host misconfiguration
becomes a total outage for everything routed through that host.

**Cost accepted.** Network transit is not charged to the budget — the value is computed before the
request leaves and interpreted after it arrives, so the flight time is invisible. That is what the
per-hop reserve pays for (ADR-006). The error is small, bounded, and in the safe direction only because
the reserve is set generously enough to cover it; on a high-latency link the reserve must go up.

**Header name is lower-case** because HTTP/2 and HTTP/3 require lower-case field names on the wire, and
matching that avoids a class of "works on HTTP/1.1" surprise. Incoming lookup is case-insensitive
anyway (ADR-009).

---

## ADR-006 — A per-hop reserve is withheld before propagating

**Status:** accepted (slice 1)

**Context.** Having computed the remaining budget, how much of it is handed downstream?

**Decision.** `remaining_ms() - reserve_ms`, floored at zero. `reserve_ms` is a per-call-site
parameter, not a module constant.

**Why not all of it.** After the downstream call returns, this hop still has work to do:
deserialise the response, act on it, serialise its own reply. Give away 100% and the failure mode is
downstream succeeding on the final millisecond while *this* hop blows the client's deadline formatting
the reply. That is the worst available outcome — the full cost was paid and the result thrown away.
The reserve also absorbs the transit time the wire format cannot account for (ADR-005).

**Why a parameter.** The right reserve depends on the hop: a call whose response is a boolean needs
less than one returning a 5 MB payload to be parsed and re-serialised. A constant would be tuned for
whichever call site was noticed first.

**A negative reserve raises.** It would *add* time on the way down, manufacturing budget from nothing
and breaking the only invariant this module exists to enforce. Almost always a sign error at a call
site, so it fails loudly rather than propagating a corrupted budget.

---

## ADR-007 — `max_ms` is a trust boundary; the value is clamped and the clamp is recorded

**Status:** accepted (slice 1)

**Context.** The budget arrives in a request header. At the edge, that means it is attacker-controlled.

**Decision.** `deadline_from_headers` requires an explicit `max_ms` and clamps anything above it,
reporting `BudgetSource.CLAMPED`. Both `default_ms` and `max_ms` are required keyword-only arguments
with no library default.

**Why.** Without a ceiling, `x-timeout-budget-ms: 86400000` pins a worker, a connection and a bulkhead
slot for a day, at no cost to the sender — a denial of service that needs no volume, just patience. The
ceiling turns an unbounded resource commitment into a bounded one.

**Why no default value for the parameters.** A default timeout is a product decision that differs per
endpoint: a health check and a batch report do not share one. Baking a number into a shared module is
how an entire fleet inherits a value nobody chose, then discovers it during an incident. Requiring the
argument forces the decision to be made where the context exists to make it.

**Exactly at the ceiling is `INHERITED`, not `CLAMPED`.** The ceiling is a permitted value. Reporting it
as clamped would make the metric fire constantly for well-behaved callers, and a signal that is always
on is a signal nobody reads.

---

## ADR-008 — A malformed or absent budget degrades to the default; it does not fail the request

**Status:** accepted (slice 1)

**Context.** A caller sends `x-timeout-budget-ms: 100ms`. Reject with HTTP 400, or ignore and proceed?

**Decision.** Fall back to `default_ms` and record `BudgetSource.MALFORMED`.

**Why.** The budget is an optimisation: it lets the chain abandon doomed work early. Its absence costs
efficiency, not correctness. Returning 400 would fail a request the service is perfectly capable of
serving, turning one caller's header bug into a user-visible outage — and the header is exactly the
kind of thing a client library gets wrong in a release. `MALFORMED` makes it loud in metrics and
harmless in production, which is the correct split: alert the operator, serve the user.

**Why `DEFAULTED` is a distinct value from `INHERITED`.** This is the one that must not be collapsed.
If both reported the same thing, internal traffic that has *lost* its budget header would be
indistinguishable from traffic that correctly inherited one — so broken propagation would be invisible
while the entire scheme silently stopped working. `DEFAULTED` on an internal hop is a bug; on an edge
request it is normal. Only the source field can tell them apart.

Same shape as returning a `basis` field alongside a computed number elsewhere in this portfolio: hand
back *why* along with *what*, so the caller can tell a real answer from a fallback.

**A budget of `0` is honoured, not treated as unset.** "You have no time left" is a legitimate and
useful statement — it means the work is already pointless. Substituting the default would *lengthen* a
budget the caller deliberately set to nothing. Correspondingly, `headers_for_downstream` emits `0`
rather than omitting the header: an absent header reads as `DEFAULTED` downstream, and the callee would
grant itself a fresh full budget, losing the deadline at the precise moment it matters most.

---

## ADR-009 — Strict `^[0-9]+$` parsing, not `int()`

**Status:** accepted (slice 1)

**Context.** The header value is a string of digits. `int(raw)` is the obvious parse.

**Decision.** Reject anything not matching `[0-9]+` before calling `int()`. Look the header up
case-insensitively.

**Why.** `int()` accepts considerably more than "a string of digits". Verified on CPython 3.12:

| Input | `int()` gives | Should be |
|---|---|---|
| `"100_000"` | `100000` (PEP 515 underscores) | rejected |
| `"+100"` | `100` | rejected |
| `"  100  "` | `100` | rejected |
| Arabic-Indic digits `١٢٣` | `123` | rejected |
| Devanagari digits `१२` | `12` | rejected |

Using `int()` would give the wire format a grammar nobody documented and nobody would guess —
including, demonstrably, Arabic-Indic numerals. It also produces values that do not round-trip: a
budget logged as `100_000` parsed as `100000`, so the log and the behaviour disagree. A wire format
should accept exactly what it specifies.

**Case-insensitive lookup** because HTTP/1.1 field names are case-insensitive, so `X-Timeout-Budget-Ms`
is the same header. Relying on the caller to pass a case-insensitive mapping holds right up until
somebody passes a plain `dict` — at which point propagation silently breaks for one client, which is
the hardest kind of bug to notice.

**`headers_for_downstream` returns a fresh `dict` per call** for the same class of reason: a shared or
mutated header container leaks one request's deadline into another, and presents as "some requests
randomly get a tiny budget".

---

## ADR-010 — Flooring is done through an explicit epsilon, and the reason is a bug this had

**Status:** accepted (slice 1)

**Context.** `remaining_ms()` converts float seconds to integer milliseconds. Round, truncate, or
floor?

**Decision.** `math.floor(remaining_s * 1000.0 + _FLOOR_EPSILON_MS)`, where the epsilon is one
nanosecond expressed in milliseconds (`1e-6`).

**Why not round.** The number must never over-state the time available. With 0.6 ms left, rounding
reports 1 ms and hands downstream a budget that has already elapsed — converting a free local refusal
into a remote timeout that costs a connection, does discarded work, and is then recorded as *the
dependency's* failure.

**Why floor and not `int()`.** `int()` truncates toward zero, so a 249.9999 ms overrun reports `-249`
where floor reports `-250`. Floor is both honest and the more conservative magnitude. A diagnostic that
flatters you is worse than no diagnostic.

**Why the epsilon — the actual bug.** The end-to-end chain test failed on first run:

```
expected [1000, 950, 900, 850]
got      [1000, 950, 899, 849]
```

With a deadline at 0.98 s and the clock at 0.06 s, `0.98 - 0.06` is `0.9199999999999999`, so
`int(x * 1000)` floored `919.9999999999999` to **919** for a budget that genuinely had 920 ms. The
representation error is ~1e-16 s and physically meaningless. The damage was done by *flooring*, which
amplified it into a whole millisecond — in the same direction, at every hop, so a chain lost budget in
proportion to its length. Exactly the same shape as flooring money.

One nanosecond is a defensible cut-off: far larger than double-precision error on any plausible
deadline value, far smaller than anything a network can resolve.

**Cost accepted.** A deadline with a true remaining time inside one nanosecond of a millisecond
boundary rounds up by that nanosecond. Immaterial, and the alternative is losing whole milliseconds.

**A note on the test.** A test using tidy values (0.5 s, 0.25 s) would not have caught this — those are
exactly representable in binary. The regression test deliberately uses 950 ms and 30 ms. Choosing
inputs that are awkward for the *representation*, not just for the logic, is the transferable lesson.

---

## ADR-011 — `asyncio_mode` is absent from `pyproject.toml` until slice 3

**Status:** accepted (slice 1)

**Context.** The bulkhead in slice 3 is async, so `pytest-asyncio` will be needed. Its `asyncio_mode`
setting could be added now.

**Decision.** Leave it out until the slice that has async tests.

**Why.** Two reasons, one mechanical and one about honesty. Mechanically, `addopts` includes
`--strict-config`, which makes an unrecognised key a hard error, and `asyncio_mode` is only recognised
while `pytest-asyncio` is installed — so the setting would break the suite for anyone who installed
only `pytest`. More generally: configuration should not reference tooling the project does not use. A
settings block describing capabilities the code lacks is the same class of defect as a README claiming
an unbuilt feature.

`--strict-markers` *is* on from the start, and earns its place immediately — without it a typo'd
`@pytest.mark.asyncio` is silently ignored, the coroutine is never awaited, and the test passes
vacuously.
---

## ADR-012 — A bucketed ring, not a deque of timestamps

**Status:** accepted (slice 2)

**Context.** The breaker needs the failure *rate over the recent past*. The obvious implementation is a
deque of `(timestamp, was_success)` pairs with anything older than the window evicted from the front.
It is exact.

**Decision.** Chop the window into `bucket_count` fixed slots holding two integers each, and stamp each
slot with the epoch it was written for.

**Why.** The deque's memory scales with THROUGHPUT, not with the window: 20k requests/second over a
10-second window is 200,000 live entries per breaker per process. The one component whose job is to
protect the service under load becomes a source of pressure under load, and it grows precisely when
the service is busiest — the worst possible correlation. Eviction is also unbounded work at an
arbitrary moment: a quiet period followed by a burst means one unlucky request pays to evict
everything that expired while nothing was happening. The ring is constant memory (ten buckets is
twenty integers at any throughput), O(1) to record with no allocation, and O(bucket_count) to read.

**Cost accepted.** Granularity. Events leave a whole bucket at a time, so the effective span varies
between `window_seconds - bucket_seconds` and `window_seconds` — a 9-to-10-second window with the
defaults. That is acceptable because the number feeds a threshold comparison on an already-noisy
sample: "is more than half the traffic failing" does not become wrong because the sample covered 9.4
seconds rather than ten. It would be unacceptable for billing, or for rate limiting against a
contractual quota, where the deque is the right choice. Being able to say which of those you are in is
the point; "buckets are faster" is not an argument, it is a preference.

**Rejected alternative.** A background sweeper to expire buckets on a timer. It needs a lifecycle
(forgetting to stop it leaks one per breaker), it makes the class untestable without real time, and it
is unnecessary: because each bucket carries its epoch, expiry can be lazy and free.

---

## ADR-013 — An empty window reports a failure rate of 0.0, not `NaN`

**Status:** accepted (slice 2)

**Context.** `failures / total` with `total == 0` has no correct answer. The options are 0.0, `NaN`, or
raising.

**Decision.** Return 0.0.

**Why.** This is a choice about which lie is safer, not about which is more truthful. 0.0 means "no
evidence of failure", so a breaker consulting it does nothing — the fail-safe direction, since with no
traffic there is nothing to protect. Raising would push the empty case onto every caller. `NaN` is the
most honest about missing data and the most dangerous in practice: `NaN >= threshold` is False, which
happens to produce the same behaviour *by accident*, and accidental correctness stops holding the
moment someone rewrites the comparison as `not (rate < threshold)`.

**Cost accepted.** A caller could mistake a low rate for evidence of health. That is what the breaker's
minimum-throughput check exists for, and the docstring says so where the two ideas meet.

---

## ADR-014 — Trip on a failure rate over a window, not on consecutive failures

**Status:** accepted (slice 2)

**Context.** "Open after five consecutive failures" is the most common formulation, and the easiest to
implement — a single counter, reset on success.

**Decision.** Compute a failure rate over a sliding window.

**Why.** A dependency failing 50% of the time is catastrophic and may never produce five failures in a
row, so a consecutive-failure breaker never fires. Partial failure — one bad replica out of four, a
degraded shard, an intermittently exhausted connection pool — is the *common* case in distributed
systems. Total failure is the easy case, and it is the only one a counter detects.

**Cost accepted.** More state and more configuration: a window, a bucket count, a threshold and a
minimum throughput, versus one integer. Each is a number someone has to choose, and choosing badly
produces a breaker that looks installed and does not work (see ADR-015 and ADR-023).

---

## ADR-015 — A minimum throughput before the rate is allowed to mean anything

**Status:** accepted (slice 2)

**Context.** A rate needs a denominator, and nothing about the arithmetic requires it to be large.

**Decision.** Draw no conclusion until the window holds at least `minimum_throughput` calls.

**Why.** This is the check most often missing from hand-written breakers, and its absence is
spectacular rather than subtle. At 03:00 a health check produces one request in the window; it fails;
the failure rate is 100%; the breaker opens and every subsequent request is rejected on the evidence of
a single call. The service has taken itself offline over one blip. The guard is a single comparison, and
what it encodes is only the statistical point that a proportion computed from n=1 is not a measurement.

**Cost accepted.** A genuinely broken dependency called very rarely will not trip the breaker, because
the evidence never reaches the sample size. That is the correct trade: with that little traffic there
is little to protect, and `minimum_throughput=1` remains available for a caller who disagrees.

---

## ADR-016 — HALF_OPEN admits a bounded number of probes

**Status:** accepted (slice 2)

**Context.** Recovery has to be discovered somehow, and the only way to learn whether a service is
healthy is to send it something. The naive half-open state lets all traffic through when the cooldown
expires and watches what happens.

**Decision.** Admit at most `half_open_max_concurrent` calls at a time; refuse the rest exactly as if
the circuit were still open. One probe failure re-opens.

**Why.** If the dependency is still fragile — which, having just been broken, is likely — the full
accumulated load arrives at once and knocks it straight back down. The breaker has become the
thundering herd it exists to prevent, and on a repeating cycle. Rationing recovery is the whole benefit
from the dependency's point of view: it comes back up under a trickle rather than under everything that
was waiting for it.

**Cost accepted.** Recovery is slower, by exactly the amount configured. Requests refused during
half-open see a `CircuitOpenError` with no `Retry-After`, because a trial slot may free in a
microsecond or a minute, and inventing a number the client would trust is worse than admitting the wait
is unknowable.

---

## ADR-017 — The window is reset when the circuit closes

**Status:** accepted (slice 2)

**Context.** On the way back to CLOSED the window still contains the failures that opened the circuit.

**Decision.** Clear the window on the CLOSED transition. Do *not* clear it when tripping.

**Why.** Without the reset, recovery is impossible: the next single failure finds a rate already above
the threshold and re-opens the circuit immediately, no matter how healthy the dependency now is. The
symptom is a circuit flapping between OPEN and HALF_OPEN forever while the dependency is verifiably
fine — and it is a hard bug to see, because every individual state transition looks correct. Tripping
is the mirror image: the window's contents are the *reason* for the trip, so keeping them means a
snapshot taken during the incident explains itself.

**Cost accepted.** Immediately after recovery the breaker is blind: it needs `minimum_throughput` fresh
calls before it can conclude anything, so a dependency that fails again at once is detected a little
later than it would be otherwise. That is the price of being able to recover at all.

**A note on verifying it.** The test for this initially proved nothing — with the default 10-second
window and 30-second cooldown, the failures expired on their own while the circuit was open, so
deleting the reset changed no observable behaviour and left the suite green. Mutation testing found
that; reading the test did not. It now uses a window longer than the cooldown, which is also the
configuration under which a missing reset actually bites in production.

---

## ADR-018 — `Outcome` has three values, not two

**Status:** accepted (slice 2)

**Context.** Every call has to be reported back to the breaker. The obvious shape is success or failure.

**Decision.** A third value, `NOT_COUNTED`: the call finished, release its slot, and it is not evidence
about the dependency's health.

**Why.** A breaker is a hypothesis about one question — *is this dependency unhealthy?* — and anything
recorded as a failure that is not evidence for that hypothesis makes it fire when it should not. An
HTTP 400 means *we* sent a bad request; a 404 means the thing does not exist; in both the dependency
answered correctly and quickly, so a client deploying a 404 bug would otherwise trip the breaker and
take the dependency out for every other caller. A call refused locally because the time budget was
exhausted never reached the dependency at all. A bulkhead rejection is evidence about our own
saturation. Without a third value each of these must be forced into success or failure, and the forcing
is where breakers acquire their reputation for firing at random. Getting this wrong is the single most
common way a resilience layer makes a system *less* reliable than it was without one.

**Cost accepted.** A dependency that only ever answers 404 keeps the circuit half-open indefinitely,
with no failures recorded — because a 404 probe genuinely is not evidence of health. That is correct,
and it is a state worth being able to recognise on a dashboard. Note also that 429 is deliberately
*not* in this category: it is the dependency itself saying it cannot serve us.

---

## ADR-019 — Permission is a `Permit` object, and a context manager

**Status:** accepted (slice 2)

**Context.** The simplest API is `if breaker.allow(): ...` followed by `breaker.record_success()`.

**Decision.** `acquire()` returns a `Permit` that must be resolved, and supports `with`.

**Why.** Bounded probe concurrency (ADR-016) requires the breaker to know how many probes are
*currently in flight*, which is impossible if permission is a fire-and-forget boolean. Something has to
represent "this call is still running", and making it a value the caller holds is the only shape where
the breaker can count it. The context manager then closes the failure mode that matters: an unresolved
permit never decrements the in-flight count, so leaking `half_open_max_concurrent` of them means the
circuit can never close again for the lifetime of the process — with no failures recorded and nothing
in the logs to explain it. A `finally` that runs no matter what is the whole defence, which is why it
is a context manager and not a convention.

An unresolved permit is recorded as `NOT_COUNTED` rather than guessed at. Guessing `FAILURE` would let
a bug in *our* code trip a breaker on a healthy dependency; guessing `SUCCESS` would hide real
failures. Because that silence would otherwise be invisible it is counted in `unresolved_permits`,
which should be zero forever and is therefore a clean thing to alert on.

**Cost accepted.** A slightly heavier API, one object allocated per call, and an exception type callers
must handle. Resolving a permit twice raises rather than being ignored, because double-counting
evidence halves the traffic needed to open the circuit.

---

## ADR-020 — Every permit carries a generation, and late outcomes are dropped

**Status:** accepted (slice 2)

**Context.** Calls are concurrent, and a state transition does not cancel work already in flight. A call
issued while CLOSED can still be running when the circuit trips; a probe can return after the circuit
has re-opened or closed.

**Decision.** Bump an integer on every transition, stamp it onto each permit, and discard any outcome
whose stamp does not match — counting it in `stale_outcomes`.

**Why.** Applying a stale outcome is wrong in a specific and damaging way: a late probe *failure* would
re-open a circuit that has just recovered and is serving traffic happily, with no current evidence of
any problem at all, producing an incident that is close to unexplainable from the logs. Comparing
integers makes "is this still relevant?" cheap, with no timestamps to compare and no window to reason
about. Two details are load-bearing. A stale outcome must not reach the window either, because the
window was deliberately cleared on recovery (ADR-017) and re-admitting a pre-transition result would
quietly undo that. And it must not decrement the in-flight probe count, because the transition already
zeroed that counter — decrementing would steal a slot from the current round and, with enough late
results, drive the count negative and admit unbounded probes.

**Cost accepted.** Evidence is thrown away. On a busy breaker `stale_outcomes` is expected to be small
but non-zero; a large value means calls routinely outlive the cooldown, which is itself worth knowing —
it says the cooldown is too short for this dependency's latency.

---

## ADR-021 — Time-driven transitions are evaluated on access, not by a background task

**Status:** accepted (slice 2)

**Context.** OPEN becomes HALF_OPEN when a duration elapses. Nothing calls the breaker to tell it so.

**Decision.** Apply any due transition at the start of `state`, `acquire` and `snapshot`.

**Why.** The alternative is a task that flips the state when the cooldown expires, and it is worse in
three ways: it needs a lifecycle, so something must start and stop it and forgetting to stop it leaks a
task per breaker; it makes the class untestable without real time, which would undo the entire point of
injecting the clock; and it introduces genuine concurrency into a class that otherwise has none.

**Cost accepted.** Reading a property can change observable state, which is normally a smell. The
mitigation is conceptual and worth stating precisely: the state is *observed* rather than changed — the
cooldown had already elapsed whether or not anyone looked. Two tests pin both halves: a probe is
admitted after a cooldown that nobody watched, and a thousand reads do not extend one.

---

## ADR-022 — Not thread-safe, and no lock

**Status:** accepted (slice 2)

**Context.** `self._half_open_in_flight += 1` is not atomic. Sharing one breaker across OS threads would
genuinely be unsafe.

**Decision.** Document it, do not lock it.

**Why.** The intended deployment is a single asyncio event loop, and every method runs start to finish
with no `await` inside it — so each is already atomic with respect to other coroutines. A lock would
buy nothing there and would cost an acquisition on the hottest path in the gateway. Worse, it would
make an actual misuse silent: a breaker shared across threads is a design conversation about what a
breaker's opinion is scoped to, and a lock answers that question by hiding it.

**Cost accepted.** A caller who ignores the documentation gets a data race. The honest response is a
note in the docs and in the module docstring rather than a lock that turns a design error into a
permanent performance cost. Related: one breaker per dependency, never one per process — a shared
breaker means a failing search service stops calls to a healthy payments service, converting a partial
outage into a total one.

---

## ADR-023 — Every threshold is required; `bucket_count` is the only default

**Status:** accepted (slice 2)

**Context.** `BreakerConfig` has seven fields. Library defaults for all of them would make the breaker a
one-liner to construct.

**Decision.** Six are required with no default. `bucket_count` defaults to 10. Construction validates
every field and raises on anything unusable.

**Why.** Same argument as the timeout budget's `default_ms` (ADR-007): what counts as "too many
failures" for a payments provider is not what it means for a recommendations service, and a shared
default is how an entire fleet inherits a number nobody chose and then discovers it during an incident.
`bucket_count` is a principled exception because it is not a policy decision about the dependency at
all — it is an internal accuracy-versus-memory trade inside the window, the way a hash table has a
default load factor.

Validation is not defensive padding. Each rejected value produces a breaker that *looks* installed and
does not work, which is worse than having none because it is believed in. A `failure_rate_threshold` of
0 opens on any traffic at all, because an empty window reports 0.0 and `0.0 >= 0.0`. Above 1.0 is
unreachable, so the breaker is present, configured, monitored and inert. A `half_open_max_concurrent`
of 0 admits no probes, so the circuit can never leave HALF_OPEN — permanently open, with no error and
nothing in the logs.

**Cost accepted.** Verbose construction, and a startup failure for a configuration that would
previously have started. Both are the intent: fail at deploy time rather than at 3am.

---

## ADR-024 — An unobservable invariant is asserted on internals rather than deleted

**Status:** accepted (slice 2)

**Context.** Mutation testing found one line in `SlidingWindow.reset` that no test constrained: clearing
the per-bucket epoch stamps. It is genuinely unobservable through the public surface, because a bucket
whose counts are zero contributes nothing whether its stamp is stale or current.

**Decision.** Keep the line and assert the invariant it maintains — "a reset window is
indistinguishable from a new one" — directly against the private arrays, with the reasoning in the
test.

**Why.** Two honest options existed. Deleting the line is defensible: this codebase argues elsewhere
that unreachable code cannot be tested and untested code should not be trusted, and it leaves
`_settle`'s unreachable OPEN branch unwritten for exactly that reason. But that argument applies to
*branches*, not to a state-restoring invariant: keeping "reset returns the object to its constructed
state" true is what stops a later change — a per-bucket flag, or an aggregate maintained alongside the
buckets — from leaving half the state behind. An invariant nothing checks is a comment.

**Cost accepted.** One test reaches into private attributes and will need updating if the
representation changes. That is the correct coupling: the test exists precisely to catch a
representation change that forgets part of the reset.
---

## ADR-025 — The bulkhead is a separate component from the deadline, not a setting on it

**Status:** accepted (slice 3)

**Context.** Both components exist to stop a slow dependency from hurting us, and both are configured
per dependency, so the obvious simplification is to fold concurrency limiting into the timeout budget:
one object, one config block, one thing to explain.

**Decision.** Keep them separate. `Deadline` bounds how long *one* call may run. `Bulkhead` bounds how
many calls may run *at once*. Neither can be expressed in terms of the other.

**Why.** The two limits answer different questions, and the failure that motivates the bulkhead happens
while every deadline is being honoured perfectly. Search degrades from 50 ms to 30 s. Every call still
finishes inside its 30 s budget — nothing has timed out, no deadline was violated, the timeout code has
no bug. But each of those calls now occupies a worker for 600× longer than it used to, so within a
minute every worker in the process is parked inside a search call. Payments traffic, which is healthy
and fast and has nothing to do with search, queues behind it and times out. "Each call is limited to
30 s" and "all capacity is held by search" are entirely compatible statements; that is the whole point.

The reverse also holds: a bulkhead without deadlines is a pool that fills up and never drains, because
nothing forces a call to end. They compose (slice 4) precisely because neither subsumes the other.

**Cost accepted.** Two objects and two config blocks per dependency instead of one, and a reader who
has to understand why. The alternative is one object with a name that lies about what it does.

---

## ADR-026 — The queue is bounded, small, and full means fail immediately

**Status:** accepted (slice 3)

**Context.** Bounded concurrency alone is one line: `asyncio.Semaphore(max_concurrent)`. Callers past
the limit wait, and the semaphore's waiter list is unbounded, so nobody is ever turned away.

**Decision.** A bounded queue of configurable depth `max_queue`, and `BulkheadFullError` raised
synchronously — before any suspension point — once both the pool and the queue are full. `max_queue=0`
is legal and means pure fast failure.

**Why.** An unbounded waiter list does not remove the dependency's backlog, it *relocates* it into this
process. The queue costs memory, holds a client connection open per entry, and grows latency without
growing throughput — throughput is set by the dependency's capacity, not by how many callers are
hopeful. A queue is only good at one thing: absorbing a burst the pool will drain shortly.

Sizing is arithmetic, not taste. With per-call latency `L` and concurrency `C`, the pool completes
`C / L` calls per second, so the last entrant to a queue of depth `Q` waits roughly `Q * L / C`. At
L = 50 ms, C = 20, Q = 100 that is 250 ms of pure queueing on top of the call itself. Choose `Q` from
the queueing latency you are willing to add; if the answer is "none", `max_queue=0` says so exactly.

Two consequences worth stating plainly. First, **a rejection is not an outage.** `BulkheadFullError`
during a spike is the component doing its job — shedding load so the requests that are served are
served properly. Alert on the rejection *rate*, not on individual occurrences. Second, **a rejection is
never evidence against the dependency**: the call was not made, so nothing was learnt about its health.
Slice 4 classifies it as not-counted for exactly this reason; a queue sized slightly too small must not
be able to trip a circuit breaker on a service that is perfectly healthy.

`max_concurrent` is deliberately allowed to over-subscribe: each dependency's share must be smaller than
the process's total capacity, but the *sum* of the shares may exceed it. Sizing the shares to add up
exactly leaves capacity idle whenever any dependency is quiet, which is most of the time.

**Cost accepted.** Callers must handle a new error, and a mis-sized queue sheds load that a larger one
would have served. That is the trade being made on purpose: bounded harm instead of unbounded latency.

---

## ADR-027 — `acquire()` takes no timeout parameter

**Status:** accepted (slice 3)

**Context.** "Wait for a slot, but not forever" is a real requirement, and a `timeout=` argument on
`acquire()` is the conventional way to spell it.

**Decision.** No timeout parameter. Callers who need one wrap the call: `async with
asyncio.timeout(budget): slot = await bulkhead.acquire()`.

**Why.** `asyncio.timeout` already composes with any awaitable, and a caller holding a `Deadline`
(ADR-005) already knows its remaining budget — so the parameter would add a second, independent
abandonment mechanism with no new capability. It would also make attribution ambiguous: with two ways
to give up, "who abandoned this call, and against which budget" stops being answerable from the
metrics, and abandonment attribution is the thing the deadline work exists to make clear.

The consequence is that **cancellation is the mechanism**, not an edge case — which is precisely why
cancellation-safety (ADR-028) is not optional here.

**Cost accepted.** Two lines at the call site instead of one argument, and a caller who forgets the
wrapper waits as long as the queue makes them. The queue is bounded and small, so that wait is bounded
by construction.

---

## ADR-028 — Cancellation is handled at two distinct moments, and a granted slot is passed on

**Status:** accepted (slice 3)

**Context.** With cancellation as the abandonment mechanism, a caller can vanish mid-operation. There
are two moments at which that can happen, and they need opposite responses.

**Decision.**

1. **Cancelled while queued** — remove the waiter from the queue (`_discard_waiter`), count
   `cancelled_while_queued`, re-raise.
2. **Cancelled after a slot was granted but before resuming** — the slot is already promised to this
   waiter, so hand it on to the next waiter (`_release_or_hand_over`), count `cancelled_at_handover`,
   re-raise.

The cleanup is `except BaseException`, not `except asyncio.CancelledError`.

**Why.** Case 1 left unhandled fills the queue with ghosts: entries belonging to callers that are long
gone, occupying capacity and causing `BulkheadFullError` to be raised at live callers on behalf of dead
ones.

Case 2 is worse, because it is silent and permanent. A dropped slot is never recovered — `_in_flight`
stays incremented for the lifetime of the process. After `max_concurrent` such events the bulkhead
rejects everything while reporting itself completely full, with nothing in the logs and no failing
dependency to blame. This is the same failure class as an unresolved `Permit` in the breaker (ADR-019):
*a resource whose release depends on control returning to code that may never run.* Recognising the
class is what makes it findable in review; the fix is always the same shape — release in a handler that
runs even when the happy path does not.

`BaseException` rather than `CancelledError` because `coroutine.close()` throws `GeneratorExit`. An
abandoned coroutine — one that was started, suspended in the queue, and then dropped without ever being
awaited — must not leave a ghost either, and a handler that only catches cancellation would let it.
There is a test asserting exactly that, because it is the kind of thing no reviewer thinks to check.

**Cost accepted.** The cleanup path is the most intricate code in the module, and its correctness is not
apparent from reading it once. It is therefore the most heavily tested: the distinction in case 2 rests
on `waiter.done() and not waiter.cancelled()`, and the mutation matrix confirms that breaking either
moment is caught — by a hang, in the cases where the pool's capacity silently disappears.

---

## ADR-029 — A freed slot is handed directly to the head of the queue

**Status:** accepted (slice 3)

**Context.** On release, the alternative implementation is the obvious one: decrement `_in_flight`, then
wake a waiter and let it re-run the same "is there capacity?" check that every caller runs.

**Decision.** Direct handover. `_release_or_hand_over` passes the slot straight to the head of the
queue and `_in_flight` never dips; only if the queue is empty is the count decremented.

**Why.** Waking a waiter to re-check capacity opens a window. Between the decrement and the waiter
actually resuming, `_in_flight` is below the limit, so a brand-new caller arriving in that window takes
the fast path and walks off with capacity that was already promised to somebody who has been waiting.
Under sustained load, that is not a rare interleaving — it is the normal one, and the queue starves
while the newcomers are served.

Because `_in_flight` never dips, there is no such instant, and FIFO fairness holds with no fairness
mechanism at all: it falls out of the fact that spare capacity and a non-empty queue are mutually
exclusive states. That property is what licenses the unguarded fast path in `acquire()`, and the comment
there says so, because it looks like a race and is not.

**Cost accepted.** `_in_flight` no longer means "calls currently executing" at every instant — during
the one-tick handover window it counts a slot that is promised but not yet in use. That is the honest
reading (the capacity genuinely is not available), and there is a test that pins the behaviour by
advancing a newcomer's coroutine exactly one step into that window.

---

## ADR-030 — FIFO, not LIFO

**Status:** accepted (slice 3)

**Context.** A queue under overload has a real argument for serving the *newest* waiter first. The
newest request is the one most likely to still have a client attached; the oldest has probably already
been abandoned upstream, so serving it produces a response nobody reads. LIFO measurably improves
goodput under sustained overload, and this is not a fringe position.

**Decision.** FIFO — `collections.deque`, append at the tail, pop from the head.

**Why.** LIFO's advantage appears only under *sustained* overload, which is the regime this design is
built to avoid rather than optimise for: the queue is deliberately small (ADR-026), so a waiter cannot
sit in it long enough to have gone stale. Meanwhile LIFO can starve an individual waiter indefinitely,
and "your request waited 4 seconds while later ones were served" is a debugging experience nobody
should have to reason about at 3am when the queue is 100 deep and the cause is invisible.

FIFO also composes better with deadlines: a caller's budget is bounded and its position only improves,
so its worst case is computable (`Q * L / C`, ADR-026) rather than unbounded.

**Cost accepted.** Slightly worse goodput in a regime the sizing rules are meant to keep us out of. If
that regime turns out to be routine, LIFO is a one-line change to `_hand_over_to_next_waiter` — and the
right response would probably be a smaller queue rather than a different discipline.

---

## ADR-031 — `admitted` counts calls that got a usable slot, measured on resumption

**Status:** accepted (slice 3)

**Context.** A queued caller could be counted as admitted at the moment the slot is handed to it, which
is where the state change happens and is one line shorter.

**Decision.** Count on resumption, in `_wait_for_slot` after the `await` returns. A waiter granted a
slot and then cancelled is *not* an admission; it is counted separately as `cancelled_at_handover`.

**Why.** `admitted` exists to be divided by attempts, and to size the pool from real usage. Counting at
handover inflates it with calls that never ran, and it does so *worst under overload* — cancellations at
handover cluster exactly when clients are timing out. The metric would improve as the incident
deepened, which is the most misleading shape a metric can have.

Keeping `cancelled_at_handover` separate means nothing is hidden: the three outcomes of a queued call
(admitted, cancelled while queued, cancelled at handover) are individually visible, and the third is
also the tell for the silent capacity leak in ADR-028.

**Cost accepted.** Three counters where one would nearly do. Mutation testing found that the resumption
increment was originally unconstrained — every other assertion about `admitted` happened to use a
queue-free configuration — so there is now a test that queues first and then checks the count.

---

## ADR-032 — `release()` is strict, `__exit__` is tolerant

**Status:** accepted (slice 3)

**Context.** `BulkheadSlot` can be released explicitly or by leaving a `with` block, so a caller who
does both would release twice. One rule has to apply to each path, and they are not the same rule.

**Decision.** `release()` raises `RuntimeError` if the slot is not held. `__exit__` releases only if the
slot is still held, and is silent otherwise.

**Why.** A double release returns capacity that was never taken, so the pool admits more concurrency
than it is configured for — the exact failure the bulkhead exists to prevent, reached from the inside,
silently, with the snapshot still reporting the configured limit. Nothing about that is recoverable by
guessing, so it is an error and it is loud.

`__exit__` has a different job: guarantee the slot comes back. Being strict there would turn the
perfectly reasonable "release early, then leave the block" pattern into a crash *during unwinding* —
and if the block is unwinding because of an exception, that crash would replace the real error with a
bookkeeping complaint. The asymmetry is deliberate: strict where a mistake is a bug, tolerant where
strictness would destroy information.

**Cost accepted.** Two release paths with different strictness, which needs the comment it has. Both are
tested, including the explicit-release-inside-the-block case that motivates the tolerance.

---

## ADR-033 — High-water marks are recorded alongside totals

**Status:** accepted (slice 3)

**Context.** `BulkheadSnapshot` could report only counters and let the metrics backend derive the rest,
which is the usual division of labour.

**Decision.** Also record `peak_in_flight`, `peak_queued` and `queue_wait_max_seconds` in-process, next
to the totals they belong to.

**Why.** Averages hide exactly the events these numbers exist to expose. Mean queue depth is
approximately zero right up until the moment it is not, and a one-second shedding spike disappears
completely into a per-minute average — so a scrape interval of 15 s or 60 s cannot see the thing that
hurt. `peak_queued` is the number to size `max_queue` from, and it is only available to the process that
observed it.

`queue_wait_max_seconds` earns its place for a second reason: queue wait happens entirely *before* the
outbound call, so it appears in no downstream metric and in no dependency's latency dashboard. Without
it, a gateway that is itself the source of the latency looks innocent, and the investigation goes
looking at the dependency. It is the number that distinguishes "the dependency is slow" from "we are the
ones being slow."

**Cost accepted.** Three more fields, and marks that only reset when the object does — a peak from an
incident an hour ago still shows. For sizing decisions that is the desirable behaviour; a caller who
wants windowed peaks can diff snapshots.

---

## ADR-034 — `asyncio_mode = "strict"`, superseding ADR-011's "deliberately absent"

**Status:** accepted (slice 3), supersedes part of ADR-011

**Context.** ADR-011 recorded that `pyproject.toml` set no `asyncio_mode` because there was no async
code, and that adding the key would couple the suite to pytest-asyncio for no benefit. Slice 3 added the
first async code — a bulkhead has to be able to wait — so the premise expired.

**Decision.** Set `asyncio_mode = "strict"`: every async test carries `@pytest.mark.asyncio`.

**Why.** `strict` over `auto` for the same reason `--strict-markers` is on. In `auto`, any coroutine
test function runs on an event loop with no marker, which is convenient right up to the moment a *sync*
test grows an `await`: it is silently reinterpreted rather than reported. Explicit costs one line per
test and makes "this test needs an event loop" a visible property of the test rather than a property of
the config file.

The coupling ADR-011 worried about is real and is now accepted: `--strict-config` makes an unrecognised
ini key a hard error, and `asyncio_mode` is only recognised while pytest-asyncio is installed — so a
missing plugin fails collection for *every* test, not just the async ones. That is why the plugin is
pinned in the `dev` extra rather than left to chance, and why the README's quick start installs that
extra before running pytest. The `pythonpath` comment was corrected at the same time: the suite still
never has to *build* this project, but it is no longer true that it needs nothing installed at all.

**Cost accepted.** A marker on every async test, and a hard dependency on one pytest plugin. Recorded as
a supersession rather than an edit to ADR-011, because the interesting part is not the current setting —
it is that the decision was correct when made and stopped being correct when the code changed.
