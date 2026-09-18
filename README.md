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

**All four slices complete: timeout budgets, circuit breaking, bulkheads, and the HTTP surface that
composes them.**

| | Slice | State |
|---|---|---|
| 1 | Deadlines / timeout-budget propagation | ✅ done — 42 tests, 10/10 mutants killed |
| 2 | Circuit breaker (sliding window, half-open with bounded trials) | ✅ done — 68 tests, 20/20 mutants killed |
| 3 | Bulkhead (bounded concurrency + bounded queue) | ✅ done — 32 tests, 22/22 mutants killed |
| 4 | Composition + HTTP proxy, failure classification | ✅ done — 192 tests, 51/52 mutants killed |

The one surviving mutant is documented rather than hidden: see [ADR-039](DECISIONS.md) and the note
under the slice 4 table in [Verification](#verification).

## Quick start

Requires Python 3.12+. The `resilience/` package still has no runtime dependencies of its own — it is
importable and testable with nothing installed but the toolchain — and the HTTP surface in `gateway/`
adds FastAPI and `httpx`.

```bash
git clone <this repo> && cd edge-gateway
python -m pip install -e ".[dev]"      # fastapi, httpx + pytest, pytest-asyncio, ruff, mypy
python -m pytest                       # 334 tests, ~2s — none of them sleep
python -m ruff check . && python -m ruff format --check .
python -m mypy --strict src tests
```

To actually serve traffic, install the extra that adds the server and point it at a config:

```bash
python -m pip install -e ".[dev,serve]"     # adds uvicorn
python -m uvicorn --factory your_module:build_gateway --port 8000
```

There is deliberately no `main.py` and no bundled config: every number in `GatewayConfig` is a policy
decision about a specific dependency, and shipping a plausible-looking default set is how somebody
ends up running production on numbers this repository invented (ADR-051). `create_app(config)` is the
entry point, and `tests/test_gateway.py` shows it wired up.

`pytest-asyncio` is needed from slice 3 onward: the bulkhead is the first component that can wait, so
the suite has async tests, and `pyproject.toml` sets `asyncio_mode = "strict"` (see ADR-034 for why
`strict` and not `auto`). The project itself is still never *built* to run the tests — `pythonpath`
points at `src/`, so the editable install above is for the tools and the runtime libraries, not to
make imports work.

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

## The idea: the composition is where the bugs are

Three protections that each work in isolation still have to be arranged, and this is the part that is
easy to get wrong *and* hard to notice: **most of the wrong arrangements pass every test written per
component.** They return the right status codes, they refuse the right requests, and they cost
something on every request during an outage.

`Guard.call` fixes one order:

```
1. deadline viability   is this call worth making at all?      (one subtraction, acquires nothing)
2. circuit breaker      is the dependency worth calling?       (an integer comparison, cannot block)
3. bulkhead slot        do we have capacity to spare on it?    (the scarcest resource, so: last)
4. deadline viability   is it STILL worth making?              (queueing spent real budget)
5. the call             bounded by whatever budget remains, minus the reserve
```

The rule generating it is that the cheapest refusal comes first, and nothing is acquired before
something that could have refused without it. Invert steps 2 and 3 — the most natural-looking mistake,
since "get a slot, then check" reads fine — and a caller waits in the queue for its entire budget
before discovering the circuit was open the whole time, having occupied a queue position that a viable
request could have used. Nothing errors. The only symptom is latency during an outage, which is the
last place anybody looks for a design bug.

Step 4 is the one that gets left out. Queue wait is real elapsed time out of the request's budget, so a
call that was viable when it joined the queue may have 4 ms left when it reaches the front — and issuing
it then costs a connection, makes the dependency do work nobody will read, and records a timeout
against *its* breaker. One comparison stands between a busy queue and a self-inflicted outage.

The honest cost of this order is in [ADR-035](DECISIONS.md): because the breaker sits outside, its
permit is held while we queue, and in HALF_OPEN that permit is one of a small number of probe slots. A
queued request therefore occupies probe capacity without probing anything. That is accepted — the wait
is bounded by the deadline, so it is self-limiting, whereas the alternative ordering pays its cost on
every request in the far more common OPEN state.

### Attribution: which failures are the dependency's fault

There are eight ways out of `Guard.call`. One of them is a real answer, and it defers to `classify`. Of
the remaining seven — every one of which is a failure of some kind — exactly **two** are evidence about
the dependency, and the other five are evidence about us:

| Way out | Recorded as | Why |
|---|---|---|
| The call returned | whatever `classify` says | 5xx is FAILURE, 4xx is not — see the table above |
| We abandoned the call at our budget | `FAILURE` | It was called and did not answer. This is what a breaker is for |
| The call raised (connection refused, TLS, DNS) | `FAILURE` | The dependency failed to serve us |
| Budget too small on arrival | `NOT_COUNTED` | We never called it |
| Budget ran out while queued | `NOT_COUNTED` | *Our* queue spent it |
| Bulkhead full | `NOT_COUNTED` | Evidence about our saturation |
| Slot wait expired | `NOT_COUNTED` | Same — and it moves `max_queue` the opposite way |
| The client disconnected | `NOT_COUNTED` | Ambiguous, and the honest reading of ambiguity is "no evidence" |

This is why there are **two separate `asyncio.timeout` scopes** rather than one around the whole thing.
A single scope is simpler, produces correct total behaviour, and is unattributable: a timeout that fires
while queueing and one that fires during the call are the same exception and mean opposite things.
Collapse them and every period of *local* saturation records failures against a dependency that was
healthy throughout — which trips its breaker, sheds traffic that had nothing to do with the saturation,
and turns our capacity problem into their outage. That is the classification bug from the breaker
section re-entering through the back door ([ADR-036](DECISIONS.md)).

### The proxy part is not a one-line copy

`outbound.headers = incoming.headers` is where every hand-rolled proxy starts, and it already contains
all of these. Each one's symptom points somewhere other than the gateway:

| Header | Handling | What forwarding it does |
|---|---|---|
| `connection`, `te`, `trailer`, `transfer-encoding`, `upgrade`, `keep-alive`, `proxy-*` | dropped both ways | `Connection: close` tears down a pooled connection per request — the latency of having no pool, with a pool's memory cost and no error |
| anything named *inside* `Connection` | dropped both ways | RFC 9110 §7.6.1 makes the set extensible per connection; ignoring it leaks a peer's connection-scoped header to an origin |
| `host` | dropped outbound | It names *us*, so a virtual-hosted origin serves the wrong site — or somebody else's |
| `content-length` | dropped both ways, recomputed | This hop buffers, so a stale value truncates the body or makes the peer wait for bytes that never come |
| `content-encoding` | dropped inbound | `httpx` already decompressed the body; relaying `gzip` tells the client to inflate plaintext |
| `x-timeout-budget-ms` | dropped outbound, re-added | Forwarding it verbatim un-spends the queue wait and our own work |

Four more decisions in the same category, each with a test named after the bug rather than after the
assertion:

- **The outbound client keeps no cookies, and this was a real defect found by testing for it.**
  `httpx.AsyncClient` is a *user agent*: it stores every `Set-Cookie` and replays it to the same host.
  One shared client, therefore: caller A logs in, the dependency answers `Set-Cookie: session=…`, and
  caller B — a different person holding no credentials — gets that cookie attached to their outbound
  request and is authenticated as A. There is no bug anywhere in this project's own code; it is one
  line of default behaviour in a well-behaved library. It is invisible from either caller's side, so
  the test that found it asserts on what the **dependency** saw ([ADR-041](DECISIONS.md)).
- **Redirects are returned, not followed.** A 301 is the answer the client asked for. Following it
  spends one budget and one bulkhead slot on an unbounded chain and records only the last outcome, so a
  dependency stuck in a redirect loop looks perfectly healthy while consuming everything.
- **Repeated headers survive.** Response headers are assigned as raw pairs, never through a `Mapping`,
  because `Set-Cookie` is repeated rather than comma-joined. A dict keeps the last one, and the symptom
  — "login succeeds but the next request is unauthenticated" — points nowhere near a proxy.
- **`httpx` is given no timeout of its own.** `timeout=None`, deliberately. Two timeouts for one call
  are two deadlines with no relationship: whichever fires first decides the outcome, which one that is
  depends on numbers in two files nobody reads together, and the one that loses is the request's actual
  budget ([ADR-038](DECISIONS.md)).

### What it refuses, and how it says so

Five distinct refusals collapse onto two status codes, which is correct — a client only needs "retry
later" or "the deadline passed", and private status codes would break every intermediary that
understands the standard ones. But the five have five different *fixes*, so the reason travels in
`x-gateway-refusal`:

| Situation | Status | `x-gateway-refusal` | What to do about it |
|---|---|---|---|
| Budget too small on arrival | 504 | `budget-exhausted-on-arrival` | Talk to the caller, or find the hop eating the budget |
| Budget spent in our queue | 504 | `budget-exhausted-while-queued` | Lower `max_queue` |
| Circuit open | 503 + `Retry-After` | `circuit-open` | Go and look at the dependency |
| Bulkhead full | 503, no `Retry-After` | `bulkhead-full` | Raise `max_concurrent`, or shed sooner |
| Slot wait expired | 504 | `slot-wait-expired` | The queue is too *deep* for the budget |
| Dependency did not answer | 504 | `upstream-timeout` | The dependency |
| No such upstream | 404 | `unknown-upstream` | The client's URL, or our config |

Three details in that table are decisions rather than mechanics. A full bulkhead is **503 and never
429**: Too Many Requests says *you* sent too many, and a caller refused because two hundred other
callers are in flight has been told something false — which it acts on, because well-written clients
back off harder on 429. `Retry-After` appears **only where a number can be justified**, which is an open
circuit whose cooldown the breaker scheduled itself; inventing one for saturation would synchronise
every refused client onto the same instant and produce a thundering herd we caused ourselves. And the
exception's own message is **never** in the response body: `BulkheadFullError`'s message carries our
in-flight count, queue depth and queue capacity, which is a free readout of how much load we can absorb
and how close to the limit we are right now.

**Nothing above is a 500, and nothing unrecognised is dressed up as a 502.** `refusal_for` returns
`None` for an exception it does not know, the handler re-raises, and a traceback naming our own file
lands in the log. A tidy catch-all 502 would file our own `KeyError` under "the dependency is flaky",
and it would stay there, because a 502 from a gateway means "upstream is broken" to everyone who reads
one ([ADR-045](DECISIONS.md)).

### Two operational endpoints, and why they are two

`GET /healthz` answers exactly one question — can this process accept and route a request — and it
returns 200 **while every circuit in the gateway is open**. That looks wrong and is the most important
small decision in `app.py`. A health check that reports unhealthy during a dependency outage tells the
orchestrator to restart or remove this instance, destroying the capacity that was about to serve
degraded traffic, at the moment it is needed, and doing it to every replica at once because they all see
the same outage. A gateway whose circuit is open is *working*: it is shedding load on purpose.

`GET /resilience` publishes every breaker and bulkhead snapshot for dashboards and incident response —
including `unresolved_permits` and `trips`, which are meant to be zero forever and are therefore the
two cleanest things here to alert on. Nothing automatically kills anything based on it. It is
unauthenticated, which is a stated limitation below rather than an oversight.

## Verification

Claims here are limited to what was actually run on the development machine.

**Verified:** 334 unit tests passing in ~2 s; `ruff check` and `ruff format --check` clean;
`mypy --strict` clean across all 26 modules — 15 under `src/` and the 11 test modules too, because a
test that type-checks is a test whose fixtures cannot silently drift from the code they exercise; all
four slices mutation-tested, with 103 of 104 reintroduced defects caught and the one survivor described
below; and one end-to-end run over a real TCP socket, through uvicorn, to a real HTTP dependency (see
[Beyond the unit tests](#beyond-the-unit-tests)).

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

Slice 4 — the composition and the HTTP surface, 52 mutants, **51 caught and one deliberate survivor**.
This is the slice where mutation testing earns its keep, because the defects it reintroduces are not
off-by-ones in arithmetic that a careful reader would spot: they are plausible, shippable readings of the
same requirement. "A full bulkhead is a failure" is what most implementations do. `follow_redirects=True`
is what the `httpx` documentation shows. `timeout=5.0` on the client looks like defensive programming.
Every row below is a version of this gateway that somebody could review and approve.

First `guard.py`, where the composition lives, and where nearly every mutation is about **attribution**
rather than behaviour — the code still refuses and still forwards, it just records the wrong verdict
about whose fault the refusal was, and that error is invisible until a breaker trips on the strength of
our own saturation:

| Mutant | Caught by |
|---|---|
| The viability check on arrival is skipped | `test_a_budget_too_small_to_be_worth_spending_is_refused_before_the_call` |
| The viability re-check after queueing is skipped | `test_the_budget_can_run_out_while_queueing_and_that_is_reported_separately` |
| The `queued` flag on an exhausted budget is inverted (the two 504s swap names) | `test_a_budget_too_small_to_be_worth_spending_is_refused_before_the_call` |
| A full bulkhead recorded as evidence against the dependency | `test_a_full_bulkhead_is_never_evidence_against_the_dependency` |
| An expired slot wait recorded as evidence against the dependency | `test_waiting_for_a_slot_stops_at_the_budget_and_is_not_evidence` |
| An expired slot wait surfaces as a bare `TimeoutError` | `test_waiting_for_a_slot_stops_at_the_budget_and_is_not_evidence` |
| A cancellation while queued recorded as evidence | `test_a_cancellation_while_queueing_is_not_evidence_either` |
| Our own upstream timeout *not* counted | `test_a_timeout_during_the_call_is_the_one_timeout_that_counts` |
| A `TimeoutError` the operation raised itself relabelled as ours | `test_a_timeout_the_operation_raised_itself_is_not_relabelled_as_ours` |
| A cancellation during the call recorded as evidence | `test_a_cancellation_during_the_call_is_not_evidence` |
| An exception from the operation not counted | `test_an_exception_from_the_operation_counts_and_propagates` |
| The classifier's verdict ignored — everything is a success | `test_an_open_circuit_refuses_with_a_503_and_a_retry_time` |
| The reserve not withheld from our own patience | `test_the_reserve_also_shortens_the_timeout_the_call_is_given` |
| Negative `reserve_ms` accepted | `test_construction_rejects_negative_millisecond_values` |
| Negative `minimum_call_ms` accepted | `test_construction_rejects_negative_millisecond_values` |

Then the two modules that decide what a response *means* and which bytes are allowed to cross the hop:

| Mutant | Caught by |
|---|---|
| A 4xx counted as a dependency failure | `test_client_errors_are_not_evidence[400]` |
| A 5xx not counted at all | `test_server_errors_are_failures[500]` |
| Hop-by-hop headers forwarded | `test_a_hop_by_hop_header_from_the_dependency_is_not_relayed` |
| The `Connection` header's contents ignored | `test_a_header_named_in_connection_is_also_dropped` |
| The direction-specific drops ignored | `test_the_host_header_names_the_dependency_and_not_the_gateway` |
| Header matching made case-sensitive | `test_content_length_matches_the_body_we_actually_send` |
| The client's budget header forwarded verbatim | `test_the_budget_sent_downstream_is_recomputed_and_not_the_one_we_received` |
| Our `Host` header forwarded to the dependency | `test_the_host_header_names_the_dependency_and_not_the_gateway` |
| `content-encoding` relayed beside a body `httpx` already decompressed | `test_content_encoding_is_dropped_inbound_but_kept_outbound` |

Then the refusal table and the outbound client — the client row that survived is discussed below:

| Mutant | Caught by |
|---|---|
| The timeout branch removed, so every timeout becomes a 502 | `test_a_timeout_from_httpx_is_matched_before_the_general_transport_error` |
| A full bulkhead blames the client with a 429 | `test_a_full_bulkhead_is_a_503_and_never_a_429` |
| An unrecognised exception dressed up as a 502 | `test_an_unrecognised_exception_is_not_dressed_up_as_a_refusal[KeyError]` |
| `Retry-After` rounds down, so a sub-second cooldown becomes `0` | `test_retry_after_rounds_up_and_never_falls_below_one_second[0.0-1]` |
| The exception's own message sent to the client | `test_the_exception_message_never_reaches_the_client` |
| The cookie jar remembers again (the cross-caller leak) | `test_one_callers_cookies_are_never_attached_to_another_callers_request` |
| Redirects followed on the caller's behalf | `test_a_redirect_is_handed_back_to_the_caller_rather_than_followed` |
| `httpx` applies its own timeout alongside the guard's | `test_the_outbound_client_applies_no_timeout_of_its_own` |
| The outbound budget not reduced by the reserve | `test_the_budget_sent_downstream_is_recomputed_and_not_the_one_we_received` |
| The query string dropped | `test_the_query_string_is_passed_through_byte_for_byte` |
| Every response classified as a success | `test_an_open_circuit_refuses_with_a_503_and_a_retry_time` |
| **The connection pool sized smaller than the bulkhead** | **nothing — see below** |

And the wiring and the startup validation. The `config.py` rows are the cheapest mutations in the whole
matrix — each one deletes a single `if` from a `__post_init__` — and they are worth running precisely
because validation is the code most likely to be quietly weakened during a refactor:

| Mutant | Caught by |
|---|---|
| The request body not forwarded | `test_a_request_body_is_forwarded_unchanged` |
| Request headers forwarded unfiltered | `test_the_host_header_names_the_dependency_and_not_the_gateway` |
| Response headers collapsed into a dict (repeated `Set-Cookie` lost) | `test_repeated_set_cookie_headers_all_survive` |
| `Content-Length` off by one | `test_content_length_matches_the_body_we_actually_send` |
| A 204 given a `Content-Length` | `test_a_204_carries_no_content_length_at_all` |
| An unknown upstream reported as a dependency failure (502) | `test_an_unknown_upstream_is_a_404_that_says_it_was_ours` |
| Every refusal becomes a 500 | `test_a_budget_too_small_to_be_worth_spending_is_refused_before_the_call` |
| Routing ignores the path and always picks the first upstream | `test_an_unknown_upstream_is_a_404_that_says_it_was_ours` |
| The ceiling used as the default budget | `test_a_request_with_no_budget_header_is_given_the_configured_default` |
| The connection pools never closed on shutdown | `test_the_lifespan_closes_the_connection_pools_on_shutdown` |
| A reserved upstream name accepted | `test_a_name_that_shadows_an_operational_endpoint_is_rejected[healthz]` |
| A name containing a slash accepted | `test_a_name_containing_a_slash_is_rejected` |
| A schemeless `base_url` accepted | `test_a_base_url_without_an_http_scheme_is_rejected[search.internal]` |
| `max_concurrent` of zero accepted | `test_a_max_concurrent_of_zero_is_rejected` |
| A ceiling below the default budget accepted | `test_a_ceiling_below_the_default_is_rejected` |
| Duplicate upstream names accepted | `test_two_upstreams_with_the_same_name_are_rejected` |

#### The survivor, and why it is still in the matrix

Setting `max_connections=1` on the outbound client, while the bulkhead still admits four concurrent
calls, changes nothing that any of the 334 tests can see. The reason is structural rather than an
oversight in the assertions: every test injects an `httpx.MockTransport`, and a mock transport has no
connection pool for the limits to reach. Nothing observable is downstream of that constructor argument.

Asserting it would mean reaching into `client._transport._pool._max_connections` — a triple-underscore
path into `httpx` internals that would break on a patch release and would be testing `httpx` rather than
this code — or standing up a real server and measuring concurrency, which is a load test rather than a
unit test. Both were rejected; the invariant is recorded in [ADR-039](DECISIONS.md) instead, with the
sizing argument written out (a pool smaller than the bulkhead reintroduces exactly the queueing the
bulkhead exists to make explicit, but *inside* `httpx`, where no metric and no `Retry-After` can see it).

The mutation stays in the matrix anyway. Deleting it would take the headline from 51/52 to 51/51, and
would delete the only executable record that the gap exists — which is the opposite of what a mutation
matrix is for. A known survivor with a written reason is a better artefact than a clean score.

### Beyond the unit tests

Everything in `tests/` runs the app through `httpx.ASGITransport` and answers the outbound call from an
`httpx.MockTransport`. That exercises real routing, real raw ASGI header bytes, the real `httpx` client
and the real request construction — everything except a socket, which is what makes 334 tests finish in
about two seconds with no ports, no fixtures and no flakiness.

It also leaves one honest gap, so it was closed separately: a throwaway script started **two real
uvicorn servers** on loopback — the gateway, and an "echo" dependency that returns as JSON exactly what
it received — and made real HTTP requests to the gateway. Eighteen checks, all passing, and four of them
are things no in-process test can see because they require the request to actually cross a wire:

```
ok  the budget crossed the wire, reduced by the reserve   (x-timeout-budget-ms: 1000 in -> 980 out)
ok  Host names the dependency, not the gateway
ok  a header named in Connection did not survive the hop
ok  no permit was left unresolved, and no slot was leaked  (read back from /resilience)
```

The rest cover the path-segment strip, the query string surviving byte for byte, a POST body arriving
intact, an unknown upstream as a labelled 404, and a 10 ms budget being refused as
`budget-exhausted-on-arrival` before any call is made. The script is not in the repository: it is a
measurement taken once, not a test to be maintained, and a suite that needs two servers and a free port
is a suite that starts being skipped.

**Not verified, and not claimed:** no load test, no cluster, no cross-host clock-skew experiment, and no
sustained-traffic run of any kind — so every sizing formula above is arithmetic rather than a benchmark.
The bulkhead tests do run many tasks concurrently on a real event loop, including cancellation storms,
but they run no I/O and no threads, so the "not thread-safe, single event loop" claim remains an argument
about the code rather than a measurement. Nothing here has run under a real client population, behind a
real load balancer, or for longer than a few seconds at a time. Those belong to the sibling reliability
project.

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

## Limitations

Deliberate, and each one is a decision with an ADR behind it rather than an omission:

- **Bodies are buffered, not streamed** (ADR-044). Both directions are read fully into memory before
  being passed on, which is what makes `Content-Length` correct and the deadline enforceable — and what
  makes this unsuitable for file transfer, for server-sent events, and for anything whose whole point is
  that the first byte arrives before the last one. Worth being precise about the memory: the request body
  is read *before* the guard is entered, so the ceiling is not `max_concurrent × body` but the number of
  connections uvicorn is willing to accept multiplied by the largest body — the bulkhead does not bound it,
  and nothing here enforces a maximum body size. Put a limit in front of it.
- **No retries, anywhere** (ADR-050). A failed call fails. Retrying is a decision that needs to know
  whether the operation is idempotent, and the gateway does not and cannot know that; a retry layer here
  would also multiply load on a dependency that is already the reason the breaker exists. That belongs to
  the caller, which knows what it asked for.
- **`/resilience` is unauthenticated** (ADR-049). It exposes failure rates, queue depths and breaker
  states — operationally useful, and useful to an attacker probing which dependency to push over. In a
  real deployment it belongs on a separate port bound to an internal interface, or behind the same auth
  as everything else. Recorded as a limitation rather than half-fixed.
- **The header filter is not a security boundary.** It is a deny list of hop-by-hop names plus whatever
  the peer put in `Connection` (ADR-043), which is correct for protocol hygiene and *not* a defence: an
  allow list is what you want if the threat model includes a caller smuggling headers your origin trusts.
  Nor is anything here an authentication or authorisation layer — the gateway forwards `Authorization`
  untouched and forms no opinion about it.
- **`CONNECT` and `TRACE` are not proxied** (ADR-052) — the method list is written out rather than
  wildcarded, so neither can be acquired by accident. **WebSockets are not proxied either**, and that one
  has no ADR because it is not a decision so much as a different problem: an upgrade has no
  request/response pair, so there is no budget to spend, nothing for a bulkhead slot to bound for a
  bounded time, and no status code to classify.
- **Single process, single event loop.** Nothing here is thread-safe by design, and the state is
  per-process — so with *n* replicas behind a load balancer, each breaker forms its own opinion from its
  own share of the traffic and each bulkhead bounds only its own concurrency. Effective limits are
  per-replica, and must be sized that way.

## Layout

```
src/edge_gateway/
  resilience/          the patterns — pure Python, no dependencies, no I/O
    clock.py           injectable monotonic time source
    deadline.py        timeout budgets and their propagation
    window.py          bounded-memory sliding window of successes and failures
    breaker.py         the circuit breaker and its permits
    bulkhead.py        bounded concurrency with a bounded queue
    classify.py        which answers are evidence about a dependency's health
    guard.py           the composition: deadline + breaker + bulkhead, in one fixed order
  gateway/             the HTTP surface — FastAPI and httpx live only in here
    config.py          frozen configuration, validated at construction
    headers.py         per-direction header filtering, including Connection's contents
    errors.py          the refusal table: which exception becomes which status
    upstream.py        one guarded dependency and its outbound client
    app.py             routing, the proxy handler, and the operational endpoints
tests/
  test_deadline.py     42 tests
  test_window.py       22 tests
  test_breaker.py      46 tests
  test_bulkhead.py     32 tests
  test_classify.py     37 tests
  test_guard.py        21 tests
  test_config.py       32 tests
  test_headers.py      21 tests
  test_errors.py       26 tests
  test_gateway.py      39 tests   end-to-end through ASGI, upstream mocked at the transport
  test_layering.py     16 tests   asserts resilience/ imports nothing from gateway/
DECISIONS.md           architecture decision records (ADR-001 … ADR-053)
```

The layering is enforced rather than intended: `test_layering.py` walks the AST of every module under
`resilience/` and fails if one of them imports `fastapi`, `httpx`, or anything from `gateway/`. That is
what keeps the patterns reusable — they can be lifted into a worker, a CLI or a message consumer, none of
which has an HTTP request to hang a deadline on.

None of the 334 tests sleep. The async ones drive concurrency with `create_task` plus `await
asyncio.sleep(0)` — a yield to the scheduler, not a wait — and provoke abandonment with `task.cancel()`
directly instead of with a timer, which is both faster and far more precise than hoping a timeout lands
in the right window.
