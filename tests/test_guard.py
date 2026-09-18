"""Tests for the composition — the ordering, and who gets blamed for what.

WHAT THESE TESTS ARE ACTUALLY FOR
--------------------------------------------------------------------------------
Each component was tested in isolation in slices 1-3. Almost nothing here re-tests them. These tests
exist for the two things that only appear once the three are wired together, and that a
component-level test cannot see:

  1. **The order.** Several orderings produce a gateway that works. The tests below therefore
     construct situations where *two* protections would both refuse, and assert which one does — the
     only way to observe an ordering rather than a result. An inverted order still returns the right
     status code; it just wastes a bulkhead slot or a queue position on every request in an outage.

  2. **Attribution.** Every path through `Guard.call` records exactly one outcome against the
     breaker, and getting that wrong is the defect this whole slice is about: a refusal misfiled as
     a dependency failure trips a breaker on a healthy service. So most assertions here are about
     `breaker.snapshot()` after a refusal, not about the refusal itself.

HOW THEY AVOID WAITING
--------------------------------------------------------------------------------
Two clocks are in play and the split is deliberate.

`ManualClock` drives the `Deadline`, so "the budget ran out while queueing" is a
`clock.advance(...)` call rather than a sleep. The real event loop drives `asyncio.timeout`, which
cannot be faked — so wherever a *timeout* is the thing under test, the budget is set to zero and
`asyncio.timeout(0)` fires at the first suspension point. Neither mechanism waits for a duration.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

import pytest

from edge_gateway.resilience import (
    BreakerConfig,
    BudgetExhaustedError,
    BudgetSource,
    Bulkhead,
    BulkheadFullError,
    BulkheadSlot,
    CircuitBreaker,
    CircuitOpenError,
    Deadline,
    Guard,
    ManualClock,
    Outcome,
    SlotWaitExpiredError,
    UpstreamTimeoutError,
)


@dataclass(slots=True)
class Rig:
    """A guard plus the pieces it is composed of, so a test can inspect and manipulate each.

    Handing back the breaker and bulkhead rather than only the guard is what makes attribution
    testable: the assertion after a refusal is almost always about what the breaker recorded, and a
    guard that hid its components would only be checkable through its own return values — which is
    precisely the part that is easy to get right.
    """

    guard: Guard
    breaker: CircuitBreaker
    bulkhead: Bulkhead
    clock: ManualClock

    def deadline(self, budget_ms: int) -> Deadline:
        return Deadline.after_ms(budget_ms, clock=self.clock, source=BudgetSource.INHERITED)


def make(
    *,
    max_concurrent: int = 2,
    max_queue: int = 2,
    minimum_call_ms: int = 50,
    reserve_ms: int = 0,
    failure_rate_threshold: float = 0.5,
    minimum_throughput: int = 2,
    half_open_max_concurrent: int = 1,
    half_open_successes_to_close: int = 1,
) -> Rig:
    clock = ManualClock(0.0)
    breaker = CircuitBreaker(
        "search",
        config=BreakerConfig(
            failure_rate_threshold=failure_rate_threshold,
            minimum_throughput=minimum_throughput,
            window_seconds=60.0,
            open_duration_seconds=30.0,
            half_open_max_concurrent=half_open_max_concurrent,
            half_open_successes_to_close=half_open_successes_to_close,
        ),
        clock=clock,
    )
    bulkhead = Bulkhead("search", max_concurrent=max_concurrent, max_queue=max_queue, clock=clock)
    guard = Guard(
        "search",
        breaker=breaker,
        bulkhead=bulkhead,
        reserve_ms=reserve_ms,
        minimum_call_ms=minimum_call_ms,
    )
    return Rig(guard=guard, breaker=breaker, bulkhead=bulkhead, clock=clock)


def spawn[T](coro: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
    return asyncio.create_task(coro)


async def settle(times: int = 1) -> None:
    """Yield to the scheduler. Not a wait — see the bulkhead tests for the full argument."""
    for _ in range(times):
        await asyncio.sleep(0)


def returning(value: int) -> Any:
    """An operation that succeeds immediately with `value` as its "status code"."""

    async def operation() -> int:
        return value

    return operation


def by_status(status: int) -> Outcome:
    """A deliberately trivial classifier: the tests here are about the guard, not about the table.

    `test_classify.py` owns the status-code rules. Using the real `classify_status` here would
    couple every composition test to that table, so a change to how 429 is treated would break tests
    about bulkhead ordering — which is how a suite becomes something people stop changing.
    """
    if status >= 500:
        return Outcome.FAILURE
    if status >= 400:
        return Outcome.NOT_COUNTED
    return Outcome.SUCCESS


async def hold_the_pool(rig: Rig) -> list[BulkheadSlot]:
    """Occupy every slot in the bulkhead, directly rather than through the guard.

    Directly on purpose: a test that filled the pool by issuing guard calls would also be filling
    the breaker's window and consuming probe slots, so the state under test would be entangled with
    the setup. Reaching for the bulkhead itself keeps the arrangement to exactly one thing.
    """
    return [await rig.bulkhead.acquire() for _ in range(rig.bulkhead.available)]


async def trip_the_circuit(rig: Rig) -> None:
    """Drive the breaker to OPEN through the guard, using failing calls."""
    for _ in range(2):
        result = await rig.guard.call(
            rig.deadline(1000), operation=returning(500), classify=by_status
        )
        assert result == 500
    assert rig.breaker.snapshot().state.value == "open"


# ---------------------------------------------------------------------------
# The happy path, and classification flowing through it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_successful_call_returns_its_result_and_records_a_success() -> None:
    rig = make()

    result = await rig.guard.call(rig.deadline(1000), operation=returning(200), classify=by_status)

    assert result == 200
    counts = rig.breaker.snapshot().counts
    assert (counts.successes, counts.failures) == (1, 0)
    assert rig.bulkhead.in_flight == 0, "the slot came back"
    assert rig.breaker.snapshot().unresolved_permits == 0


@pytest.mark.asyncio
async def test_the_classifier_decides_what_counts_not_the_guard() -> None:
    """A 404 must reach the caller untouched while contributing no evidence.

    This is the whole point of `classify` being a parameter. The guard has no opinion about status
    codes — it cannot, since it does not know the result is even an HTTP response — so a caller that
    wants "cache miss returns 503 and that is fine" gets it without patching the composition.
    """
    rig = make()

    result = await rig.guard.call(rig.deadline(1000), operation=returning(404), classify=by_status)

    assert result == 404, "the response is passed through, not swallowed"
    counts = rig.breaker.snapshot().counts
    assert counts.total == 0, "a 404 is not evidence either way, so the window stays empty"


@pytest.mark.asyncio
async def test_a_failing_status_is_recorded_against_the_dependency() -> None:
    rig = make()

    await rig.guard.call(rig.deadline(1000), operation=returning(503), classify=by_status)

    counts = rig.breaker.snapshot().counts
    assert (counts.successes, counts.failures) == (0, 1)


# ---------------------------------------------------------------------------
# The order, observed by making two protections refuse at once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_viability_is_checked_before_the_breaker_is_even_consulted() -> None:
    """With an open circuit AND an unviable budget, the budget wins.

    Not a cosmetic preference. The budget check consumes nothing, so it is free to run first; and
    reporting `CircuitOpenError` here would tell the operator that the dependency is broken when in
    fact this request would have been refused against a perfectly healthy one. That is a wrong
    diagnosis handed to whoever is on call.
    """
    rig = make(minimum_call_ms=50)
    await trip_the_circuit(rig)
    before = rig.breaker.snapshot().rejected_calls

    with pytest.raises(BudgetExhaustedError) as caught:
        await rig.guard.call(rig.deadline(10), operation=returning(200), classify=by_status)

    assert caught.value.queued is False
    assert caught.value.remaining_ms == 10
    assert caught.value.at_least_ms == 50
    assert rig.breaker.snapshot().rejected_calls == before, (
        "the breaker was never asked, so it cannot have counted a rejection"
    )


@pytest.mark.asyncio
async def test_an_open_circuit_refuses_before_a_bulkhead_slot_is_taken() -> None:
    """The ordering assertion that matters most in a real outage.

    Invert breaker and bulkhead and every request during an outage joins the bulkhead queue, waits
    there — possibly for its entire budget — and is then refused for a reason that was knowable
    before it waited at all. It has occupied a queue position that a request to a *working* path
    could have used, and it has added its full queue wait to a predetermined answer.
    """
    rig = make(max_concurrent=1, max_queue=1)
    await trip_the_circuit(rig)
    admitted_before = rig.bulkhead.snapshot().admitted

    with pytest.raises(CircuitOpenError):
        await rig.guard.call(rig.deadline(1000), operation=returning(200), classify=by_status)

    snapshot = rig.bulkhead.snapshot()
    assert snapshot.admitted == admitted_before, "no slot was taken"
    assert snapshot.queued_total == 0, "and nothing ever queued"


@pytest.mark.asyncio
async def test_an_open_circuit_wins_even_when_the_bulkhead_is_also_full() -> None:
    """Both would refuse; the breaker is asked first, so it is the breaker that answers.

    Stated as its own test because it is the direct observation of the ordering. The previous test
    shows the bulkhead was untouched, which an implementation could also achieve by checking the
    bulkhead first and happening not to record anything.
    """
    rig = make(max_concurrent=1, max_queue=0)
    await trip_the_circuit(rig)
    held = await hold_the_pool(rig)
    assert rig.bulkhead.available == 0

    with pytest.raises(CircuitOpenError):
        await rig.guard.call(rig.deadline(1000), operation=returning(200), classify=by_status)

    for slot in held:
        slot.release()


# ---------------------------------------------------------------------------
# Attribution: what each refusal records against the dependency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_full_bulkhead_is_never_evidence_against_the_dependency() -> None:
    """The rule that stops our own saturation from tripping a breaker on a healthy service.

    Without it, a `max_queue` set slightly too low for a traffic spike opens the circuit on a
    dependency that was answering every call correctly throughout — and then the fix everyone
    reaches for is raising the breaker's threshold, which disables the breaker instead of fixing the
    queue.
    """
    rig = make(max_concurrent=1, max_queue=0, minimum_throughput=1)
    held = await hold_the_pool(rig)

    with pytest.raises(BulkheadFullError):
        await rig.guard.call(rig.deadline(1000), operation=returning(200), classify=by_status)

    snapshot = rig.breaker.snapshot()
    assert snapshot.counts.total == 0, "not evidence: the dependency was never called"
    assert snapshot.state.value == "closed"
    assert snapshot.unresolved_permits == 0, (
        "the permit was resolved explicitly rather than left to __exit__, which would also have "
        "recorded NOT_COUNTED but would have polluted the unresolved counter that exists to be "
        "alerted on and to be zero forever"
    )
    for slot in held:
        slot.release()


@pytest.mark.asyncio
async def test_the_budget_can_run_out_while_queueing_and_that_is_reported_separately() -> None:
    """The second viability check, and the reason it exists.

    The call was viable when it joined the queue and is not viable by the time it reaches the front,
    because queue wait is real elapsed time out of the request's budget. Issuing it anyway is the
    specific harm `is_viable` exists to prevent: a doomed call that costs a connection, makes the
    dependency do work nobody will read, and then records a timeout against its breaker.
    """
    rig = make(max_concurrent=1, max_queue=1, minimum_call_ms=50)
    held = await hold_the_pool(rig)
    deadline = rig.deadline(1000)

    queued = spawn(rig.guard.call(deadline, operation=returning(200), classify=by_status))
    await settle()
    assert rig.bulkhead.queued == 1

    rig.clock.advance(0.98)  # 20ms of budget left: below the 50ms minimum
    held[0].release()  # the slot is handed straight to the waiter

    with pytest.raises(BudgetExhaustedError) as caught:
        await queued

    assert caught.value.queued is True, (
        "distinguishable from the on-arrival case: this one says our queue ate the budget, "
        "which is a change to max_queue, not a conversation with the caller"
    )
    assert rig.breaker.snapshot().counts.total == 0
    assert rig.breaker.snapshot().unresolved_permits == 0
    assert rig.bulkhead.in_flight == 0, "the slot it briefly held was given back"


@pytest.mark.asyncio
async def test_waiting_for_a_slot_stops_at_the_budget_and_is_not_evidence() -> None:
    """A zero budget makes `asyncio.timeout` fire at the first suspension, so nothing waits.

    `SlotWaitExpiredError` rather than `BulkheadFullError`: this caller *was* admitted to the queue
    and the queue did not move fast enough, which suggests the queue is too deep for the budget —
    the opposite correction to the one a full queue suggests.
    """
    rig = make(max_concurrent=1, max_queue=1, minimum_call_ms=0, reserve_ms=0)
    held = await hold_the_pool(rig)

    with pytest.raises(SlotWaitExpiredError):
        await rig.guard.call(rig.deadline(0), operation=returning(200), classify=by_status)

    snapshot = rig.breaker.snapshot()
    assert snapshot.counts.total == 0, "the dependency was never called"
    assert snapshot.unresolved_permits == 0
    assert rig.bulkhead.queued == 0, "the abandoned waiter left no ghost in the queue"
    for slot in held:
        slot.release()


@pytest.mark.asyncio
async def test_a_timeout_during_the_call_is_the_one_timeout_that_counts() -> None:
    """The distinction that justifies having two timeouts instead of one.

    A dependency that has stopped erroring and started hanging is more dangerous than one returning
    500s: each hanging call holds capacity for the full budget rather than returning it at once.
    That is exactly what a breaker should notice, so unlike every other refusal in the module, this
    one is recorded as a failure.
    """
    rig = make(max_concurrent=1, max_queue=0, minimum_call_ms=0, reserve_ms=0, minimum_throughput=1)
    never = asyncio.Event()

    async def hangs() -> int:
        await never.wait()
        return 200

    with pytest.raises(UpstreamTimeoutError):
        await rig.guard.call(rig.deadline(0), operation=hangs, classify=by_status)

    snapshot = rig.breaker.snapshot()
    assert snapshot.counts.failures == 1, "the call was made and the dependency did not answer"
    assert rig.bulkhead.in_flight == 0, "and the slot came back"


@pytest.mark.asyncio
async def test_a_timeout_the_operation_raised_itself_is_not_relabelled_as_ours() -> None:
    """`TimeoutError` is a builtin, so an HTTP client can raise one that this scope did not cause.

    Reporting it as `UpstreamTimeoutError` would claim we abandoned the call at our own budget, and
    the `waited_ms` in that message would be a number nothing actually waited — a plausible-looking
    log line that sends an investigation to the wrong place. It is still a dependency failure, so it
    still counts; it is just reported as what it is. `asyncio.Timeout.expired()` is what tells the
    two apart.
    """
    rig = make(minimum_throughput=1)

    async def raises_its_own() -> int:
        raise TimeoutError("the client's own connect timeout")

    with pytest.raises(TimeoutError) as caught:
        await rig.guard.call(rig.deadline(1000), operation=raises_its_own, classify=by_status)

    assert type(caught.value) is TimeoutError, "not rewrapped"
    assert not isinstance(caught.value, UpstreamTimeoutError)
    assert rig.breaker.snapshot().counts.failures == 1, "still a failure, just honestly labelled"


@pytest.mark.asyncio
async def test_an_exception_from_the_operation_counts_and_propagates() -> None:
    """Connection refused, DNS failure, TLS error, an unparseable response: all the dependency's.

    The exception is re-raised untouched rather than wrapped, because the caller is better placed to
    interpret it than this module is — and a wrapper would hide the type that the gateway layer
    needs in order to choose between 502 and 504.
    """
    rig = make(minimum_throughput=1)

    async def refuses() -> int:
        raise ConnectionRefusedError("nothing listening")

    with pytest.raises(ConnectionRefusedError):
        await rig.guard.call(rig.deadline(1000), operation=refuses, classify=by_status)

    assert rig.breaker.snapshot().counts.failures == 1
    assert rig.bulkhead.in_flight == 0, "the slot is released by the `with`, exception or not"


@pytest.mark.asyncio
async def test_a_cancellation_during_the_call_is_not_evidence() -> None:
    """A client disconnect must not be able to trip breakers across every dependency at once.

    Cancellation is ambiguous by nature — the dependency may have been about to answer perfectly —
    and the honest reading of an ambiguous signal is "no evidence". Guessing FAILURE here is how a
    client-side problem becomes a fleet-wide one; guessing SUCCESS would hide real failures.
    """
    rig = make()
    never = asyncio.Event()

    async def hangs() -> int:
        await never.wait()
        return 200

    task = spawn(rig.guard.call(rig.deadline(1000), operation=hangs, classify=by_status))
    await settle(2)
    assert rig.bulkhead.in_flight == 1, "it got as far as the call"

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    snapshot = rig.breaker.snapshot()
    assert snapshot.counts.total == 0, "no evidence recorded"
    assert snapshot.unresolved_permits == 0, "and not counted as a leak either"
    assert rig.bulkhead.in_flight == 0, "the slot came back"


@pytest.mark.asyncio
async def test_a_cancellation_while_queueing_is_not_evidence_either() -> None:
    rig = make(max_concurrent=1, max_queue=1)
    held = await hold_the_pool(rig)

    task = spawn(rig.guard.call(rig.deadline(1000), operation=returning(200), classify=by_status))
    await settle()
    assert rig.bulkhead.queued == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    snapshot = rig.breaker.snapshot()
    assert snapshot.counts.total == 0
    assert snapshot.unresolved_permits == 0
    assert rig.bulkhead.queued == 0, "no ghost left behind in the queue"
    for slot in held:
        slot.release()


@pytest.mark.asyncio
async def test_no_path_through_the_guard_leaves_an_unresolved_permit() -> None:
    """One invariant, asserted across every refusal, because it is the silent failure.

    An unresolved permit is not merely untidy: in HALF_OPEN it is a probe slot that never comes
    back, and `half_open_max_concurrent` of them means the circuit can never close again for the
    lifetime of the process — with no failures recorded and nothing in the logs.
    `unresolved_permits` is supposed to be zero forever, which makes it a clean thing to alert on
    and worthless the moment a routine refusal is allowed to bump it.
    """
    # Budget exhausted on arrival: no permit is ever taken.
    rig = make(minimum_call_ms=50)
    with pytest.raises(BudgetExhaustedError):
        await rig.guard.call(rig.deadline(1), operation=returning(200), classify=by_status)
    assert rig.breaker.snapshot().unresolved_permits == 0

    # Bulkhead full.
    rig = make(max_concurrent=1, max_queue=0)
    held = await hold_the_pool(rig)
    with pytest.raises(BulkheadFullError):
        await rig.guard.call(rig.deadline(1000), operation=returning(200), classify=by_status)
    assert rig.breaker.snapshot().unresolved_permits == 0
    for slot in held:
        slot.release()

    # The operation blew up.
    rig = make(minimum_throughput=1)

    async def boom() -> int:
        raise ZeroDivisionError

    with pytest.raises(ZeroDivisionError):
        await rig.guard.call(rig.deadline(1000), operation=boom, classify=by_status)
    assert rig.breaker.snapshot().unresolved_permits == 0


# ---------------------------------------------------------------------------
# The reserve, and configuration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_reserve_is_withheld_from_our_own_patience_too() -> None:
    """60 ms left, a 40 ms reserve and a 50 ms minimum: refused, because only 20 ms is spendable.

    A reserve promised downstream but not honoured locally is not a reserve, it is a comment. The
    failure it prevents is the worst one available: downstream answers on the final millisecond and
    this hop then blows the client's deadline while formatting the reply, throwing away work that
    was already paid for.
    """
    rig = make(minimum_call_ms=50, reserve_ms=40)

    with pytest.raises(BudgetExhaustedError):
        await rig.guard.call(rig.deadline(60), operation=returning(200), classify=by_status)

    # Same budget, no reserve: now viable. Proves the refusal above came from the reserve and not
    # from an unrelated off-by-one in the comparison.
    generous = make(minimum_call_ms=50, reserve_ms=0)
    assert (
        await generous.guard.call(
            generous.deadline(60), operation=returning(200), classify=by_status
        )
        == 200
    )


@pytest.mark.asyncio
async def test_the_reserve_also_shortens_the_timeout_the_call_is_given() -> None:
    """The second half of the reserve, and the half a mutation test proved nothing was checking.

    The test above covers the *viability* decision — whether to call at all. This one covers the
    timeout window handed to the call once that decision is yes, which is a separate use of the same
    number in `_budget_s`. A reserve honoured in the first place and forgotten in the second still
    produces the failure the reserve exists to prevent: the call is allowed to consume the entire
    remaining budget, answers on the final millisecond, and this hop then overruns the client's
    deadline while writing the reply — having done, and paid for, all of the work.

    The arrangement needs no real waiting, and that is the reason for these particular numbers. With
    a 500 ms budget and a 500 ms reserve there is nothing spendable, so `_budget_s` is exactly zero
    and `asyncio.timeout(0)` fires at the operation's first suspension — deterministically, on the
    first scheduler yield rather than after a delay. Drop the reserve from that computation and the
    same operation is given the full half second, which it finishes in microseconds. So the two
    outcomes are "immediate timeout" and "immediate success", with no sleep in either.

    `minimum_call_ms=0` is what lets the call be attempted at all here; anything larger would refuse
    it at the viability check and this timeout would never be reached.
    """

    async def suspends_once() -> int:
        await asyncio.sleep(0)
        return 200

    rig = make(minimum_call_ms=0, reserve_ms=500)

    with pytest.raises(UpstreamTimeoutError):
        await rig.guard.call(rig.deadline(500), operation=suspends_once, classify=by_status)

    # And it counts, because the call was made: this is the one timeout in the module that is
    # genuine evidence about the dependency.
    assert rig.breaker.snapshot().counts.failures == 1

    # The control, with the reserve removed and everything else identical. Without it, a bug that
    # timed out every call regardless of the reserve would pass the assertion above.
    generous = make(minimum_call_ms=0, reserve_ms=0)
    assert (
        await generous.guard.call(
            generous.deadline(500), operation=suspends_once, classify=by_status
        )
        == 200
    )


@pytest.mark.asyncio
async def test_a_probe_call_flows_through_the_guard_and_can_close_the_circuit() -> None:
    """End to end through HALF_OPEN, which is the state most likely to be wired up wrongly.

    Worth its own test because the probe path is the one where the permit is scarce: if the guard
    mishandled a trial permit the circuit would stay open forever, and every other test in this file
    would still pass.
    """
    rig = make(half_open_max_concurrent=1, half_open_successes_to_close=1)
    await trip_the_circuit(rig)

    rig.clock.advance(30.0)  # the cooldown elapses; the next read promotes OPEN to HALF_OPEN
    assert rig.breaker.state.value == "half_open"

    result = await rig.guard.call(rig.deadline(1000), operation=returning(200), classify=by_status)

    assert result == 200
    assert rig.breaker.state.value == "closed", "a successful probe through the guard closed it"


@pytest.mark.parametrize(
    ("reserve_ms", "minimum_call_ms", "expected"),
    [
        (-1, 50, "reserve_ms must not be negative"),
        (10, -1, "minimum_call_ms must not be negative"),
    ],
)
def test_construction_rejects_negative_millisecond_values(
    reserve_ms: int, minimum_call_ms: int, expected: str
) -> None:
    """Fail at startup rather than at 3am. A negative reserve would *add* time to the budget on the
    wire, quietly defeating the one guarantee the deadline exists to provide."""
    clock = ManualClock(0.0)
    breaker = CircuitBreaker(
        "search",
        config=BreakerConfig(
            failure_rate_threshold=0.5,
            minimum_throughput=2,
            window_seconds=60.0,
            open_duration_seconds=30.0,
            half_open_max_concurrent=1,
            half_open_successes_to_close=1,
        ),
        clock=clock,
    )
    bulkhead = Bulkhead("search", max_concurrent=1, max_queue=1, clock=clock)

    with pytest.raises(ValueError, match=expected):
        Guard(
            "search",
            breaker=breaker,
            bulkhead=bulkhead,
            reserve_ms=reserve_ms,
            minimum_call_ms=minimum_call_ms,
        )


def test_a_guard_needs_no_event_loop_to_construct() -> None:
    """Deliberately a sync test. Guards are built at startup, before any loop is running, and a
    constructor that reached for `get_running_loop()` would work in every async test and fail on
    boot."""
    rig = make()
    assert rig.guard.name == "search"
