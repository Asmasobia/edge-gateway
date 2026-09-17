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
