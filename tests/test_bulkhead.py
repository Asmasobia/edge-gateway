"""Tests for the bulkhead.

HOW THESE TESTS AVOID WAITING
--------------------------------------------------------------------------------
The bulkhead is the first component here that can block, so the obvious test is "start more calls
than the pool allows, sleep a bit, check what happened". That test would be slow, flaky, and — worse
than either — vague: it would assert on what the event loop happened to have got round to, so it
would pass for the wrong reasons and fail on a loaded machine.

Nothing below sleeps. Concurrency is driven by two primitives instead:

  * `asyncio.create_task(...)` plus `await asyncio.sleep(0)` to hand control to a task and let it
    run up to its next suspension point. `sleep(0)` yields to the scheduler and returns on the next
    pass; it does not wait for a duration, so it introduces no timing at all.
  * `task.cancel()` to reproduce, deterministically, what a client disconnect or an
    `asyncio.timeout` does to a waiter. Cancellation is the mechanism the bulkhead has to survive,
    and provoking it directly is both faster and far more precise than provoking it with a timer.

The one place `sleep(0)` needs care is that a single yield advances each pending task by *one* step.
Where a test needs a waiter to have reached the queue before continuing, it says so explicitly
rather than relying on a generous number of yields.

`ManualClock` is used only for the queue-wait metrics: the bulkhead itself has no timeouts, so it
consults the clock exactly twice per queued call.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Coroutine
from typing import Any

import pytest

from edge_gateway.resilience import (
    Bulkhead,
    BulkheadFullError,
    BulkheadSlot,
    ManualClock,
)


def make(
    *,
    max_concurrent: int = 2,
    max_queue: int = 2,
    start: float = 0.0,
) -> tuple[Bulkhead, ManualClock]:
    clock = ManualClock(start)
    return (
        Bulkhead("search", max_concurrent=max_concurrent, max_queue=max_queue, clock=clock),
        clock,
    )


def spawn[T](coro: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
    """`create_task` with a name that reads better at the call site than the noise it replaces."""
    return asyncio.create_task(coro)


async def settle(times: int = 1) -> None:
    """Let every currently-runnable task advance to its next suspension point.

    One `sleep(0)` is one pass of the scheduler, so a chain of tasks that each need to be resumed in
    turn needs more than one. Tests that depend on this say how many and why, because "yield enough
    times and hope" is how a concurrency test becomes a coin flip.
    """
    for _ in range(times):
        await asyncio.sleep(0)


async def queue_up(bulkhead: Bulkhead) -> asyncio.Task[BulkheadSlot]:
    """Start a caller, let it reach the queue, and assert that it actually did.

    The assertion is the point: without it, a test that meant to exercise a queued waiter could be
    silently exercising a caller that was admitted immediately, and it would still pass.
    """
    before = bulkhead.queued
    task = spawn(bulkhead.acquire())
    await settle()
    assert bulkhead.queued == before + 1, "expected this caller to be waiting in the queue"
    return task


async def cancel_and_collect(task: asyncio.Task[Any]) -> None:
    """Cancel a task and wait for the cancellation to actually take effect.

    Cancellation is a request: `cancel()` only schedules a `CancelledError` to be thrown into the
    coroutine. The bulkhead's cleanup lives in that coroutine's `except` block, so a test that
    inspects the bulkhead before the task has resumed is inspecting a state the fix has not reached
    yet. Awaiting the task is what makes the observation meaningful rather than lucky.
    """
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def cancel_all(tasks: list[asyncio.Task[BulkheadSlot]]) -> None:
    """Cancel several waiters, tolerating the ones that got in first.

    Every `cancel()` is issued before anything is awaited, because awaiting one task lets the others
    run: a waiter that was already granted a slot would finish holding it, and `cancel()` on a
    finished task does nothing. Whether a particular waiter ends up cancelled or admitted depends on
    that interleaving and is not what these tests assert — so both outcomes are accepted here, and
    any slot that was genuinely obtained is released rather than leaked, which would poison the
    capacity-recovery assertion that follows.
    """
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            slot = await task
        except asyncio.CancelledError:
            pass
        else:
            slot.release()


# ---------------------------------------------------------------------------
# The pool: bounded concurrency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_new_bulkhead_is_empty() -> None:
    bulkhead, _ = make(max_concurrent=2, max_queue=2)
    snapshot = bulkhead.snapshot()

    assert snapshot.name == "search"
    assert snapshot.in_flight == 0
    assert snapshot.queued == 0
    assert snapshot.admitted == 0
    assert snapshot.rejected_full == 0
    assert bulkhead.available == 2


@pytest.mark.asyncio
async def test_calls_up_to_the_limit_are_admitted_without_waiting() -> None:
    """The fast path, and the assertion that it *is* fast: neither acquisition suspends.

    Checked by acquiring twice with no intervening yield. If either call had queued, the awaits
    would not have completed and the test would hang rather than fail — which is why the in-flight
    count is also asserted, so the reason is visible in the output instead of in a timeout.
    """
    bulkhead, _ = make(max_concurrent=2, max_queue=0)

    first = await bulkhead.acquire()
    second = await bulkhead.acquire()

    assert bulkhead.in_flight == 2
    assert bulkhead.available == 0
    assert bulkhead.snapshot().queued_total == 0, "neither call had to wait"
    first.release()
    second.release()


@pytest.mark.asyncio
async def test_releasing_a_slot_frees_capacity() -> None:
    bulkhead, _ = make(max_concurrent=1, max_queue=0)

    slot = await bulkhead.acquire()
    assert bulkhead.in_flight == 1
    slot.release()

    assert bulkhead.in_flight == 0
    assert bulkhead.available == 1
    second = await bulkhead.acquire()  # would raise if the slot had not come back
    second.release()


@pytest.mark.asyncio
async def test_the_context_manager_releases_the_slot() -> None:
    bulkhead, _ = make(max_concurrent=1, max_queue=0)

    async with bulkhead.hold() as slot:
        assert slot.held
        assert bulkhead.in_flight == 1

    assert bulkhead.in_flight == 0


@pytest.mark.asyncio
async def test_an_exception_releases_the_slot_and_still_propagates() -> None:
    """The failure that matters most, because it is the one that is invisible.

    A slot lost to an exception is not recovered by anything: the pool's usable capacity is smaller
    for the lifetime of the process, and after `max_concurrent` such events the bulkhead rejects
    everything while reporting itself full — with no errors of its own to point at.
    """
    bulkhead, _ = make(max_concurrent=1, max_queue=0)

    with pytest.raises(ZeroDivisionError):
        async with bulkhead.hold():
            raise ZeroDivisionError("the downstream call blew up")

    assert bulkhead.in_flight == 0, "the slot came back despite the exception"


@pytest.mark.asyncio
async def test_an_explicit_release_inside_the_block_is_not_a_double_release() -> None:
    """`__exit__` is tolerant where `release()` is strict, and the asymmetry is deliberate: the job
    of `__exit__` is to guarantee the slot comes back, and "already back" satisfies it."""
    bulkhead, _ = make(max_concurrent=1, max_queue=0)

    async with bulkhead.hold() as slot:
        slot.release()
        assert not slot.held

    assert bulkhead.in_flight == 0


@pytest.mark.asyncio
async def test_releasing_a_slot_twice_raises() -> None:
    """Not ignored, because a double release returns capacity that was never taken — so the pool
    would admit more concurrency than it is configured for, which is the one thing it exists to
    prevent, reached from the inside and without a single log line."""
    bulkhead, _ = make(max_concurrent=1, max_queue=0)
    slot = await bulkhead.acquire()
    slot.release()

    with pytest.raises(RuntimeError, match="already been released"):
        slot.release()

    assert bulkhead.in_flight == 0, "and the failed release did not corrupt the count"


# ---------------------------------------------------------------------------
# The queue: bounded waiting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_call_beyond_the_limit_waits_in_the_queue() -> None:
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()

    waiter = await queue_up(bulkhead)

    assert bulkhead.in_flight == 1, "the queued caller does not occupy a slot"
    assert not waiter.done()

    held.release()
    assert await waiter is not None
    assert bulkhead.in_flight == 1, "the slot moved from one holder to the next"


@pytest.mark.asyncio
async def test_the_queue_is_served_in_order() -> None:
    """FIFO, asserted rather than assumed.

    Three waiters are started in a known order and released one at a time; the completion order has
    to match. LIFO is a defensible alternative (see the module docstring in bulkhead.py) and this is
    the test that would fail if someone quietly changed it.
    """
    bulkhead, _ = make(max_concurrent=1, max_queue=3)
    held = await bulkhead.acquire()

    order: list[int] = []

    async def caller(index: int) -> None:
        async with bulkhead.hold():
            order.append(index)

    tasks = [spawn(caller(index)) for index in range(3)]
    await settle()
    assert bulkhead.queued == 3

    held.release()
    await asyncio.gather(*tasks)

    assert order == [0, 1, 2]


@pytest.mark.asyncio
async def test_a_full_pool_and_a_full_queue_reject_immediately() -> None:
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()
    waiter = await queue_up(bulkhead)

    with pytest.raises(BulkheadFullError) as caught:
        await bulkhead.acquire()

    assert caught.value.name == "search"
    assert caught.value.in_flight == 1
    assert caught.value.queued == 1
    assert caught.value.queue_capacity == 1
    assert bulkhead.snapshot().rejected_full == 1

    held.release()
    await waiter


@pytest.mark.asyncio
async def test_rejection_does_not_suspend() -> None:
    """Shedding load has to be cheap, because it happens exactly when there is no spare capacity to
    be expensive with.

    Asserted by driving `acquire()` one step at a time: a coroutine that raises without ever
    suspending finishes on its first `send`, so if the rejection path awaited anything this would
    see a suspension instead of the exception.
    """
    bulkhead, _ = make(max_concurrent=1, max_queue=0)
    held = await bulkhead.acquire()

    coroutine = bulkhead.acquire()
    with pytest.raises(BulkheadFullError):
        coroutine.send(None)

    held.release()


@pytest.mark.asyncio
async def test_a_zero_length_queue_is_pure_fast_failure() -> None:
    """`max_queue=0` is legal and meaningful, not a degenerate case to guard against: it is the
    right configuration for a dependency whose latency budget cannot absorb any queueing at all."""
    bulkhead, _ = make(max_concurrent=1, max_queue=0)
    held = await bulkhead.acquire()

    with pytest.raises(BulkheadFullError) as caught:
        await bulkhead.acquire()

    assert caught.value.queue_capacity == 0
    assert bulkhead.queued == 0, "nobody was ever enqueued"
    held.release()


@pytest.mark.asyncio
async def test_the_queue_drains_one_slot_at_a_time() -> None:
    """Two slots, three waiters. Releasing one holder admits exactly one waiter — not all of them.

    This is the property that makes the pool a limit rather than a suggestion. An implementation
    that woke every waiter on release and let them re-check would pass most of the tests above and
    fail here, because the woken waiters would all see spare capacity in the same tick.
    """
    bulkhead, _ = make(max_concurrent=2, max_queue=3)
    first = await bulkhead.acquire()
    second = await bulkhead.acquire()

    waiters = [await queue_up(bulkhead) for _ in range(3)]
    assert bulkhead.queued == 3

    first.release()
    await settle()
    assert bulkhead.queued == 2
    assert sum(1 for task in waiters if task.done()) == 1
    assert bulkhead.in_flight == 2

    second.release()
    await settle()
    assert bulkhead.queued == 1
    assert sum(1 for task in waiters if task.done()) == 2
    assert bulkhead.in_flight == 2

    # Drain the rest so the test leaves no pending tasks behind.
    for task in waiters:
        if task.done():
            (await task).release()
    await asyncio.gather(*(task for task in waiters if not task.done()))


@pytest.mark.asyncio
async def test_a_freed_slot_goes_to_the_queue_and_not_to_a_newcomer() -> None:
    """No barging. The slot is handed straight to the head of the queue rather than freed and
    re-acquired, so there is no instant at which an arriving caller can take capacity that has
    already been promised to someone else. Without this a busy service can starve a queued request
    indefinitely while every individual admission looks perfectly fair.

    The window being tested is one tick wide, so it is opened deliberately rather than waited for:
    after `release()` the slot belongs to `waiter`, but `waiter` has not been scheduled yet. Driving
    a newcomer's `acquire()` with a bare `send(None)` runs it synchronously inside exactly that
    window, which is the only moment at which barging would be possible.
    """
    bulkhead, _ = make(max_concurrent=1, max_queue=2)
    held = await bulkhead.acquire()
    waiter = await queue_up(bulkhead)

    held.release()  # the slot is now promised to `waiter`, which has not resumed yet
    assert not waiter.done()

    newcomer = bulkhead.acquire()
    newcomer.send(None)  # advances it to its first suspension point, and no further
    assert bulkhead.queued == 1, "the newcomer queued: the free-looking slot was already promised"
    newcomer.close()  # abandon it — only its admission decision was of interest
    assert bulkhead.queued == 0, (
        "closing an abandoned coroutine must clear its queue entry too: `close()` throws "
        "GeneratorExit rather than CancelledError, and a cleanup path that only handles "
        "cancellation would leave a ghost holding queue space forever"
    )

    slot = await waiter
    assert bulkhead.in_flight == 1, "and the promise was kept"
    slot.release()


# ---------------------------------------------------------------------------
# Cancellation, which is the whole reason this component is hard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_cancelled_waiter_leaves_the_queue() -> None:
    """Otherwise the queue fills with callers who have gone, and starts rejecting live traffic on
    behalf of clients that disconnected."""
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()
    waiter = await queue_up(bulkhead)

    await cancel_and_collect(waiter)

    assert bulkhead.queued == 0
    assert bulkhead.snapshot().cancelled_while_queued == 1

    # And the freed queue space is usable, which is the whole point of freeing it.
    replacement = await queue_up(bulkhead)
    held.release()
    await replacement


@pytest.mark.asyncio
async def test_cancelling_a_waiter_does_not_disturb_the_others() -> None:
    """The middle waiter of three is cancelled. The other two must keep their places and their
    order: removing an entry from the middle of a queue is easy to get wrong in a way that only
    shows up as an occasional starved request."""
    bulkhead, _ = make(max_concurrent=1, max_queue=3)
    held = await bulkhead.acquire()

    order: list[int] = []

    async def caller(index: int) -> None:
        async with bulkhead.hold():
            order.append(index)

    tasks = [spawn(caller(index)) for index in range(3)]
    await settle()
    assert bulkhead.queued == 3

    tasks[1].cancel()
    with pytest.raises(asyncio.CancelledError):
        await tasks[1]
    assert bulkhead.queued == 2

    held.release()
    await asyncio.gather(tasks[0], tasks[2])

    assert order == [0, 2]


@pytest.mark.asyncio
async def test_a_waiter_cancelled_after_being_granted_a_slot_passes_it_on() -> None:
    """The race the module docstring is about, provoked deterministically.

    The sequence is exact. `held.release()` resolves the first waiter's future — the slot is now
    granted, and `_in_flight` already counts it — but that waiter has not been scheduled yet, so it
    has not returned from its `await`. Cancelling it *now* is the interleaving where a naive
    implementation loses the slot forever: nobody holds it, nobody is waiting on it, and the pool is
    permanently one smaller with no error anywhere to explain it.

    The second waiter getting in is the assertion that the slot was passed on rather than dropped.
    """
    bulkhead, _ = make(max_concurrent=1, max_queue=2)
    held = await bulkhead.acquire()
    granted = await queue_up(bulkhead)
    next_in_line = await queue_up(bulkhead)

    held.release()  # `granted`'s future is now resolved, but `granted` has not run
    assert not granted.done()

    await cancel_and_collect(granted)

    assert bulkhead.snapshot().cancelled_at_handover == 1
    assert bulkhead.snapshot().cancelled_while_queued == 0, "it was granted, not merely queued"

    slot = await next_in_line
    assert bulkhead.in_flight == 1, "the slot moved on; it was not lost"
    slot.release()
    assert bulkhead.in_flight == 0


@pytest.mark.asyncio
async def test_a_cancelled_grant_with_nobody_waiting_frees_the_slot() -> None:
    """The same race with an empty queue behind it. There is no one to hand the slot to, so it has
    to be released outright — the branch that a "just wake the next waiter" fix silently misses,
    leaking a slot whenever the cancelled waiter happens to be the last one."""
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()
    granted = await queue_up(bulkhead)

    held.release()
    await cancel_and_collect(granted)

    assert bulkhead.in_flight == 0
    assert bulkhead.available == 1
    assert bulkhead.snapshot().cancelled_at_handover == 1

    slot = await bulkhead.acquire()  # would raise BulkheadFullError if the slot had leaked
    slot.release()


@pytest.mark.asyncio
async def test_the_pool_survives_a_cancellation_storm() -> None:
    """Capacity is fully recovered after many cancellations at both moments.

    A single leak per thousand calls is invisible in a test that checks one interleaving and fatal
    in a process that runs for a week, so this asserts the aggregate: after all the churn, the pool
    admits exactly `max_concurrent` calls again and not one fewer.
    """
    bulkhead, _ = make(max_concurrent=2, max_queue=8)
    holders = [await bulkhead.acquire() for _ in range(2)]

    for round_number in range(50):
        waiters = [await queue_up(bulkhead) for _ in range(4)]
        if round_number % 2 == 0:
            # Every waiter is cancelled while still queued: the holders never let go.
            for waiter in waiters:
                await cancel_and_collect(waiter)
        else:
            # Both holders let go first, so two waiters are granted slots. Then all four are
            # cancelled — two at handover, two still queued.
            for holder in holders:
                holder.release()
            await cancel_all(waiters)
            holders = [await bulkhead.acquire() for _ in range(2)]

    for holder in holders:
        holder.release()

    assert bulkhead.in_flight == 0
    assert bulkhead.queued == 0

    # The real assertion: both slots are still available on the fast path. `queued_total` not moving
    # is what proves it — a pool that had leaked a slot would admit one call and make the other
    # wait, which is a difference `in_flight == 0` alone cannot see.
    queued_before = bulkhead.snapshot().queued_total
    fresh = [await bulkhead.acquire() for _ in range(2)]
    assert bulkhead.snapshot().queued_total == queued_before, "both fitted without queueing"
    assert bulkhead.available == 0
    for slot in fresh:
        slot.release()


@pytest.mark.asyncio
async def test_a_timeout_around_acquire_behaves_like_a_cancellation() -> None:
    """`acquire()` has no timeout parameter, and this is why it does not need one.

    `asyncio.timeout` composes with any awaitable, so a caller that has a deadline can impose it
    without the bulkhead knowing how. Adding a second mechanism able to abandon a call would only
    make "who gave up, and why" ambiguous.

    The zero-second timeout fires on the first suspension rather than after a delay, so this test
    still does not wait for anything.
    """
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0):
            await bulkhead.acquire()

    assert bulkhead.queued == 0, "the abandoned waiter left the queue"
    assert bulkhead.snapshot().cancelled_while_queued == 1
    held.release()
    assert bulkhead.available == 1


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_snapshot_counts_admissions_and_rejections() -> None:
    bulkhead, _ = make(max_concurrent=1, max_queue=0)

    for _ in range(3):
        slot = await bulkhead.acquire()
        slot.release()

    held = await bulkhead.acquire()
    for _ in range(2):
        with pytest.raises(BulkheadFullError):
            await bulkhead.acquire()
    held.release()

    snapshot = bulkhead.snapshot()
    assert snapshot.admitted == 4
    assert snapshot.rejected_full == 2
    assert snapshot.queued_total == 0


@pytest.mark.asyncio
async def test_a_call_admitted_after_queueing_is_counted_as_an_admission() -> None:
    """Both routes into the pool have to be counted, or the ratio `admitted` exists for is a lie.

    The pair `queued_total / admitted` is how you tell a pool that is the right size from one that
    quietly makes most of its traffic wait. Counting only the calls that walked straight in would
    make that ratio look best exactly when the pool is most overloaded — the reading would improve
    as the situation got worse, which is the most misleading shape a metric can have.

    A mutation test asked for this one: deleting the increment on the queued path left the suite
    green, because every other assertion about `admitted` happened to use a queue-free
    configuration.
    """
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()
    waiter = await queue_up(bulkhead)
    assert bulkhead.snapshot().admitted == 1, "only the caller that did not wait, so far"

    held.release()
    (await waiter).release()

    snapshot = bulkhead.snapshot()
    assert snapshot.admitted == 2, "the queued caller was admitted too"
    assert snapshot.queued_total == 1


@pytest.mark.asyncio
async def test_a_grant_that_was_cancelled_is_not_counted_as_an_admission() -> None:
    """`admitted` means "got a slot it could actually use", because it is the number used to judge
    whether the pool is the right size. Counting a slot that was handed back before its owner ever
    ran would inflate it, and the inflation would be worst precisely when cancellations are worst —
    which is when the number is being looked at."""
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()
    granted = await queue_up(bulkhead)

    held.release()
    await cancel_and_collect(granted)

    snapshot = bulkhead.snapshot()
    assert snapshot.admitted == 1, "only the original holder ever used a slot"
    assert snapshot.queued_total == 1, "but the cancelled caller did occupy queue space"


@pytest.mark.asyncio
async def test_high_water_marks_record_the_peak_and_not_the_average() -> None:
    """Mean queue depth on a healthy service is approximately zero right up until it is not, and a
    one-second spike that shed traffic vanishes entirely into a per-minute average. The peaks are
    the numbers to size the pool and the queue from."""
    bulkhead, _ = make(max_concurrent=2, max_queue=3)
    holders = [await bulkhead.acquire() for _ in range(2)]
    waiters = [await queue_up(bulkhead) for _ in range(3)]

    # Released one layer at a time, because the third waiter cannot be admitted until one of the
    # first two lets go — gathering all three at once would be waiting for a slot this test is
    # holding.
    for holder in holders:
        holder.release()
    for slot in await asyncio.gather(waiters[0], waiters[1]):
        slot.release()
    (await waiters[2]).release()

    snapshot = bulkhead.snapshot()
    assert snapshot.in_flight == 0, "nothing is in flight now"
    assert snapshot.queued == 0
    assert snapshot.peak_in_flight == 2, "but two were, at once"
    assert snapshot.peak_queued == 3
    assert snapshot.queued_total == 3


@pytest.mark.asyncio
async def test_queue_wait_is_measured() -> None:
    """Two waits, the longer one first, because that ordering is what the assertions need.

    Queue wait is latency the client pays and the dependency never sees: it happens entirely
    before the outbound call starts, so it appears in no downstream metric and no dependency's
    latency graph. Without this number, "the dependency is slow" and "we are the ones being slow"
    look identical from the outside."""
    bulkhead, clock = make(max_concurrent=1, max_queue=1)

    first_holder = await bulkhead.acquire()
    slow = await queue_up(bulkhead)
    clock.advance(0.5)
    first_holder.release()
    (await slow).release()  # waited 0.5 s

    second_holder = await bulkhead.acquire()
    quick = await queue_up(bulkhead)
    clock.advance(0.2)
    second_holder.release()
    (await quick).release()  # waited 0.2 s

    snapshot = bulkhead.snapshot()
    assert snapshot.queued_total == 2
    assert snapshot.queue_wait_total_seconds == pytest.approx(0.7)
    assert snapshot.queue_wait_max_seconds == pytest.approx(0.5), (
        "a high-water mark, so the later and shorter wait must not replace it"
    )


@pytest.mark.asyncio
async def test_a_call_that_did_not_wait_does_not_touch_the_wait_metrics() -> None:
    """So that the mean derived from `queue_wait_total_seconds / queued_total` is the mean wait of
    calls that actually waited, not a number diluted by every fast-path call in the process."""
    bulkhead, clock = make(max_concurrent=2, max_queue=1)

    slot = await bulkhead.acquire()
    clock.advance(5.0)
    slot.release()

    snapshot = bulkhead.snapshot()
    assert snapshot.queued_total == 0
    assert snapshot.queue_wait_total_seconds == 0.0
    assert snapshot.queue_wait_max_seconds == 0.0


@pytest.mark.asyncio
async def test_the_snapshot_is_a_consistent_frozen_reading() -> None:
    """One read, so a dashboard cannot show `queued` from one instant beside `in_flight` from
    another — a combination that never existed and that looks, to whoever is reading it
    mid-incident, exactly like a bug in the bulkhead."""
    bulkhead, _ = make(max_concurrent=1, max_queue=1)
    held = await bulkhead.acquire()
    waiter = await queue_up(bulkhead)

    snapshot = bulkhead.snapshot()
    assert (snapshot.in_flight, snapshot.queued) == (1, 1)

    with pytest.raises(AttributeError):
        snapshot.in_flight = 99  # type: ignore[misc]

    held.release()
    (await waiter).release()
    assert (snapshot.in_flight, snapshot.queued) == (1, 1), "the earlier reading did not move"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("max_concurrent", "max_queue", "message"),
    [
        (0, 1, "max_concurrent must be at least 1"),
        (-1, 1, "max_concurrent must be at least 1"),
        (1, -1, "max_queue must not be negative"),
    ],
)
def test_construction_rejects_impossible_limits(
    max_concurrent: int, max_queue: int, message: str
) -> None:
    """Synchronous on purpose: construction needs no event loop, and an `asyncio` marker would imply
    it did.

    A `max_concurrent` of 0 is the interesting case. It is not merely useless — it is an outage
    wearing a configuration's clothes: every call is rejected, the dependency is reported as
    saturated, and it receives no traffic whatsoever. Rejecting it at startup turns a 3am mystery
    into a deployment that refuses to start.
    """
    with pytest.raises(ValueError, match=message):
        Bulkhead(
            "search",
            max_concurrent=max_concurrent,
            max_queue=max_queue,
            clock=ManualClock(),
        )


def test_a_bulkhead_needs_no_event_loop_to_construct() -> None:
    """Constructing one at import time or in a module-level registry must not require a running
    loop.

    The queue is a plain deque and the futures are created inside `acquire()`, where a loop is
    guaranteed to exist. An implementation that reached for `get_event_loop()` in `__init__` would
    work under pytest-asyncio and fail in production wiring, which is the worst place to find out.
    """
    bulkhead = Bulkhead("search", max_concurrent=1, max_queue=1, clock=ManualClock())
    assert bulkhead.available == 1


def test_acquire_is_awaitable_and_typed_as_such() -> None:
    """A guard against the most tempting simplification: making the fast path synchronous.

    An `acquire()` that returned a slot directly when capacity was free and a coroutine otherwise
    would be faster and would be a trap — callers would forget the `await` on the path they test
    most, and the bug would surface only under load. The signature is uniformly awaitable.
    """
    bulkhead = Bulkhead("search", max_concurrent=1, max_queue=0, clock=ManualClock())
    pending = bulkhead.acquire()
    awaitable: Awaitable[BulkheadSlot] = pending  # a type assertion, checked by mypy --strict
    assert asyncio.iscoroutine(awaitable)
    pending.close()  # never awaited; closing it keeps "coroutine was never awaited" quiet
