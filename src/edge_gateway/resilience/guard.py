"""Composing the three patterns into one call path.

Slices 1-3 each solved one problem in isolation. This module is where they meet, and the meeting is
not a formality: **the nesting order is a design decision with consequences, and most of the ways to
get it wrong still work in testing.** A composition that is merely "all three are present" produces
a gateway that is slower than no gateway and fails in ways none of the three components would fail
on their own.

THE ORDER, AND WHY IT IS THIS ORDER
--------------------------------------------------------------------------------
    1. deadline viability   — is this call worth making at all?
    2. circuit breaker      — is the dependency worth calling?
    3. bulkhead             — do we have capacity to spare on it?
    4. re-check viability   — is it *still* worth making? (queueing consumed budget)
    5. the call, bounded by whatever budget is left

The rule generating that order is: **the cheapest refusal comes first, and nothing is acquired
before something that can refuse without it.** Each step can only be reached by paying for the ones
above it, so every inversion below is a real cost paid on every request.

*Viability before everything.* A request with 3 ms left is going to be refused whatever the other
two say, so checking it first costs one subtraction and saves everything else. It is also the only
check that consumes nothing at all.

*The breaker before the bulkhead.* Invert these and a caller waits in the bulkhead queue — possibly
for the entire remaining budget — and then discovers the circuit is open and no call was ever going
to be made. It has occupied a queue slot that a viable request could have used, and it has added its
full queue wait to a response that was predetermined. The breaker check is a comparison against an
integer: it can never block, so there is no reason for anything to be held while it runs.

*The bulkhead innermost.* Its slot is the scarcest resource here, and it is the one that must be
held for the shortest possible time — precisely the duration of the outbound call, and no longer.

THE HONEST COST OF THAT ORDER
--------------------------------------------------------------------------------
Putting the breaker outside means its permit is held while we wait for a bulkhead slot. In HALF_OPEN
that permit is one of a small, bounded number of probe slots (slice 2), so a request that is stuck
in the bulkhead queue is occupying probe capacity without probing anything. In the worst case every
probe slot is held by a queued request and recovery stalls.

This is a genuine trade, not an oversight, and it is accepted for two reasons. The wait is bounded
by the deadline, so the stall is temporary by construction rather than indefinite. And the
alternative ordering pays its cost on *every* request in the far more common OPEN state, whereas
this one only bites in HALF_OPEN while the bulkhead is simultaneously saturated. Choosing the
failure mode that is rarer and self-limiting is the whole of the reasoning; if probe capacity turns
out to stall in practice, the fix is to acquire the bulkhead slot with `max_queue` effectively zero
for trial calls, not to reorder.

WHY THERE ARE TWO SEPARATE TIMEOUTS AND NOT ONE
--------------------------------------------------------------------------------
The obvious implementation wraps steps 3-5 in a single `asyncio.timeout(remaining)`: one budget, one
timeout, correct total behaviour. It is also unattributable. A timeout that fires while queueing and
one that fires during the call are the same exception, and they mean opposite things:

  * timed out while queued  — the dependency was never called. It is not evidence about the
                              dependency; it is evidence that *we* are saturated.
  * timed out during a call — the dependency was called and did not answer in time. That is exactly
                              what a breaker is for.

Collapse them and every period of local saturation records failures against a dependency that was
healthy throughout, which trips its breaker and sheds traffic that had nothing to do with the
saturation. The single timeout is not a simplification; it is the classification bug from
`classify.py` re-entering through the back door.

WHY VIABILITY IS CHECKED TWICE
--------------------------------------------------------------------------------
Queue wait is real elapsed time out of the request's budget. A call that was viable when it joined
the queue may have 4 ms left when it reaches the front, and issuing it then is the specific harm
`is_viable` exists to prevent — a doomed call that costs a connection, makes the dependency do work
nobody reads, and then records a timeout against its breaker. The second check is one comparison,
and it is the only thing standing between a busy queue and a self-inflicted outage.

WHY THE RESERVE IS HONOURED LOCALLY TOO
--------------------------------------------------------------------------------
The outbound call is abandoned at `remaining - reserve`, not at `remaining`. The same reserve that
is withheld from the budget on the wire (slice 1) is withheld from our own patience, because it
exists to pay for reading the response, doing whatever we do with it, and getting our own reply onto
the wire. Waiting until our own deadline and *then* starting that work means blowing the client's
deadline while holding a complete, correct, expensive answer — the worst available outcome. A
reserve promised downstream and not honoured locally is not a reserve, it is a comment.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from edge_gateway.resilience.breaker import CircuitBreaker, Outcome, Permit
from edge_gateway.resilience.bulkhead import Bulkhead, BulkheadFullError, BulkheadSlot
from edge_gateway.resilience.deadline import Deadline


class BudgetExhaustedError(Exception):
    """The request's remaining time is too short for the call to be worth attempting.

    Raised before anything at all is acquired: no breaker permit, no bulkhead slot, no connection.
    It is therefore never evidence about the dependency, and there is no bookkeeping to unwind.

    `at_least_ms` is carried because the useful log line is "17 ms left, needed 50", not "budget
    exhausted". The first tells you whether to raise the client's timeout or to go and find the hop
    that is eating the budget; the second tells you nothing.
    """

    def __init__(
        self,
        guard_name: str,
        *,
        remaining_ms: int,
        at_least_ms: int,
        queued: bool,
    ) -> None:
        where = "after waiting for a slot" if queued else "on arrival"
        super().__init__(
            f"{guard_name}: {remaining_ms}ms of budget left {where}, "
            f"which is below the {at_least_ms}ms minimum for attempting the call"
        )
        self.guard_name = guard_name
        self.remaining_ms = remaining_ms
        self.at_least_ms = at_least_ms
        self.queued = queued
        """True if the budget ran down while queueing for a bulkhead slot rather than before.

        Worth separating in metrics: the first says the caller's budget was too small to begin with,
        the second says our own queue ate it. They lead to different fixes — one is a conversation
        with the caller, the other is a change to `max_queue` — and a single counter cannot tell you
        which conversation to have.
        """


class SlotWaitExpiredError(Exception):
    """The request's budget ran out while it was waiting for a bulkhead slot.

    Distinct from `BulkheadFullError`, which means "the queue was full so you were never admitted to
    it". This one means "you were admitted to the queue and it did not move fast enough". Both are
    statements about our own saturation and neither is evidence about the dependency — but they are
    fixed differently: a full queue suggests the queue is too shallow for the burst, while an
    expired wait suggests it is too *deep* for the budget (see the `Q * L / C` arithmetic in the
    bulkhead module). One counter covering both would hide which way to move the number.
    """

    def __init__(self, guard_name: str, *, waited_ms: int) -> None:
        super().__init__(
            f"{guard_name}: the time budget expired after {waited_ms}ms waiting for a bulkhead "
            f"slot; the dependency was never called"
        )
        self.guard_name = guard_name
        self.waited_ms = waited_ms


class UpstreamTimeoutError(Exception):
    """The dependency was called and did not answer within the remaining budget.

    The one refusal in this module that IS evidence against the dependency, and the reason the two
    timeouts are kept apart. A timeout here is recorded as `Outcome.FAILURE`, because "did not
    answer in time" is precisely the condition a breaker is meant to notice — a dependency that has
    stopped erroring and started hanging is more dangerous than one returning 500s, since each
    hanging call holds capacity for the full budget instead of returning it immediately.
    """

    def __init__(self, guard_name: str, *, waited_ms: int) -> None:
        super().__init__(
            f"{guard_name}: no response within the {waited_ms}ms of budget available for the call"
        )
        self.guard_name = guard_name
        self.waited_ms = waited_ms


class Guard:
    """One dependency's full protection: a deadline check, a breaker, a bulkhead, and a timeout.

    `call` is generic in the operation's return type — not the class — so that classification stays
    type-checked without pinning one guard to one result type: `classify` must accept whatever
    `operation` produces, and mypy enforces that rather than trusting the wiring. Parameterising the
    class instead would make `Guard("search", ...)` un-inferable, since the type variable appears
    nowhere in the constructor.

    One guard per dependency, for the same reason there is one breaker and one bulkhead per
    dependency: sharing either converts a partial outage into a total one.

    Usage:

        guard = Guard("search", breaker=breaker, bulkhead=bulkhead,
                      reserve_ms=20, minimum_call_ms=50)
        response = await guard.call(
            deadline,
            operation=lambda: client.get(url, timeout=None),
            classify=lambda response: classify_status(response.status_code),
        )

    Note `timeout=None` on the client call. The guard owns the timeout, and a second one inside the
    HTTP client would be a competing deadline with no relationship to the request's budget —
    whichever fired first would decide the outcome, and which that is would depend on configuration
    nobody reads together.
    """

    __slots__ = ("_breaker", "_bulkhead", "_minimum_call_ms", "_name", "_reserve_ms")

    def __init__(
        self,
        name: str,
        *,
        breaker: CircuitBreaker,
        bulkhead: Bulkhead,
        reserve_ms: int,
        minimum_call_ms: int,
    ) -> None:
        """Both millisecond values are required, for the reason given in ADR-023: a shared default
        is how an entire fleet inherits a number nobody chose.

        There is no clock parameter. The guard reads time only through the `Deadline` it is handed,
        which already carries the clock it was built with (slice 1) — so a guard cannot disagree
        with its own deadline about what time it is, which is a class of bug that simply cannot
        occur here.
        """
        if reserve_ms < 0:
            raise ValueError(f"reserve_ms must not be negative (got {reserve_ms!r})")
        if minimum_call_ms < 0:
            raise ValueError(f"minimum_call_ms must not be negative (got {minimum_call_ms!r})")

        self._name = name
        self._breaker = breaker
        self._bulkhead = bulkhead
        self._reserve_ms = reserve_ms
        self._minimum_call_ms = minimum_call_ms

    @property
    def name(self) -> str:
        return self._name

    async def call[T](
        self,
        deadline: Deadline,
        *,
        operation: Callable[[], Awaitable[T]],
        classify: Callable[[T], Outcome],
    ) -> T:
        """Run `operation` under all three protections, recording what happened.

        `operation` takes no arguments: it is a closure the caller builds, which keeps this module
        free of any knowledge about HTTP, clients or URLs. It must not apply a timeout of its own.

        `classify` maps the result to evidence. It is a parameter rather than a hard-coded
        status-code check because "what counts as a failure" is dependency-specific — a service that
        answers a cache miss with 503 is not unhealthy, however unusual that choice is — and because
        the alternative is a composition layer that only works for HTTP.

        Raises, in the order the checks happen: `BudgetExhaustedError`, `CircuitOpenError`,
        `BulkheadFullError`, `SlotWaitExpiredError`, `BudgetExhaustedError` again (with
        `queued=True`), `UpstreamTimeoutError`, or whatever `operation` itself raised.
        """
        self._require_viable(deadline, queued=False)

        # The permit is acquired outside the bulkhead (see the module docstring) and is a context
        # manager so that the probe slot comes back no matter how we leave — including on a
        # cancellation, which is the case a `finally` you wrote by hand tends to miss.
        with self._breaker.acquire() as permit:
            slot = await self._acquire_slot(permit, deadline)
            with slot:
                # Re-checked *after* queueing, because the queue spends the request's budget. The
                # permit is resolved explicitly rather than left to `__exit__`: both record
                # NOT_COUNTED, but `__exit__` also increments `unresolved_permits`, which is meant
                # to be zero forever and alertable. Letting a routine refusal bump it would make
                # that signal useless within a day.
                try:
                    self._require_viable(deadline, queued=True)
                except BudgetExhaustedError:
                    permit.not_counted()
                    raise

                return await self._invoke(permit, deadline, operation, classify)

    def _require_viable(self, deadline: Deadline, *, queued: bool) -> None:
        if deadline.is_viable(minimum_ms=self._minimum_call_ms, reserve_ms=self._reserve_ms):
            return
        raise BudgetExhaustedError(
            self._name,
            remaining_ms=deadline.remaining_ms(),
            at_least_ms=self._minimum_call_ms,
            queued=queued,
        )

    async def _acquire_slot(self, permit: Permit, deadline: Deadline) -> BulkheadSlot:
        """Wait for a bulkhead slot, but never past this request's budget.

        `asyncio.timeout` is the mechanism because the bulkhead deliberately has no timeout
        parameter (ADR-027) — the cancellation it raises is exactly what the bulkhead's cleanup is
        built to survive, so a waiter abandoned here leaves no ghost in the queue and never drops a
        slot that had already been handed to it.
        """
        budget_s = self._budget_s(deadline)
        try:
            # No `expired()` check here, unlike `_invoke`. The only thing awaited inside this scope
            # is our own bulkhead, whose documented failure is `BulkheadFullError` and which raises
            # it before ever suspending — so a `TimeoutError` reaching the handler below can only
            # have come from this timeout. `_invoke` awaits arbitrary caller code and cannot assume
            # that.
            async with asyncio.timeout(budget_s):
                return await self._bulkhead.acquire()
        except BulkheadFullError:
            # Never admitted to the queue at all. Not evidence: the dependency was not called, and a
            # queue sized slightly too small must not be able to trip a breaker on a healthy
            # service.
            permit.not_counted()
            raise
        except TimeoutError:
            permit.not_counted()
            raise SlotWaitExpiredError(self._name, waited_ms=int(budget_s * 1000)) from None
        except asyncio.CancelledError:
            # The client went away while we were queued. Not evidence about the dependency, and not
            # a leak either — which is why it is resolved here rather than left to `__exit__` to
            # record as an unresolved permit. A client disconnect is a normal event, not a bug in
            # our code.
            permit.not_counted()
            raise

    async def _invoke[T](
        self,
        permit: Permit,
        deadline: Deadline,
        operation: Callable[[], Awaitable[T]],
        classify: Callable[[T], Outcome],
    ) -> T:
        budget_s = self._budget_s(deadline)
        scope = asyncio.timeout(budget_s)
        try:
            async with scope:
                result = await operation()
        except TimeoutError:
            if not scope.expired():
                # A `TimeoutError` that this scope did not cause — the operation raised one of its
                # own. Some clients do: a bare `TimeoutError` is a builtin, and a connect timeout
                # deep inside a socket library surfaces as exactly this type. Reporting it as
                # `UpstreamTimeoutError` would claim we abandoned the call at our budget when in
                # fact the client gave up on its own schedule, and the "waited_ms" in the message
                # would be a number nothing actually waited. It is still a dependency failure, so it
                # counts — it just gets reported as what it is.
                permit.failure()
                raise
            # Ours, and counted, unlike every other timeout in this module: the call was made and
            # the dependency did not answer within the budget.
            permit.failure()
            raise UpstreamTimeoutError(self._name, waited_ms=int(budget_s * 1000)) from None
        except asyncio.CancelledError:
            # A cancellation is ambiguous by nature — the dependency may have been about to answer
            # perfectly — and the honest reading of an ambiguous signal is "no evidence". Guessing
            # FAILURE here would let a wave of client disconnects trip breakers across every
            # dependency at once, which is a spectacular way to turn a client-side problem into a
            # fleet-wide one.
            permit.not_counted()
            raise
        except Exception:
            # Everything the operation itself raised: connection refused, DNS failure, TLS error, a
            # malformed response the client could not parse. All of them are the dependency failing
            # to serve us, so all of them count. Note that this is `Exception` and not
            # `BaseException`: a `KeyboardInterrupt` or a `SystemExit` is our process shutting down
            # and says nothing about the dependency, so it is left to `__exit__` — which records
            # NOT_COUNTED, correctly, and flags it as unresolved, also correctly, because a permit
            # abandoned during shutdown is worth being able to see.
            permit.failure()
            raise

        permit.resolve(classify(result))
        return result

    def _budget_s(self, deadline: Deadline) -> float:
        """Seconds we are willing to spend, with the per-hop reserve already withheld.

        Recomputed at each step rather than captured once, because time passes between the steps and
        a captured value would hand the outbound call a budget that was measured before the queue
        wait — quietly restoring the very overrun the reserve exists to prevent.
        """
        return deadline.downstream_budget_ms(reserve_ms=self._reserve_ms) / 1000.0
