"""Bulkheads: bounded concurrency, a bounded queue, and immediate rejection past both.

WHAT A BULKHEAD IS FOR
--------------------------------------------------------------------------------
The name comes from shipbuilding: a hull divided into sealed compartments so that a breach floods
one compartment instead of the whole vessel. The software version divides a service's finite
capacity — workers, connection-pool entries, event-loop slots — into compartments, one per
dependency, so that a dependency which has gone slow can only ever consume its own share.

Without one, this is what happens. A gateway calls three dependencies: payments, search and
recommendations. Search starts taking 30 seconds instead of 50 ms. Every request to search now
occupies a worker for 30 seconds, and since search traffic keeps arriving, within a minute every
worker in the process is parked on a search call. Requests to payments — a healthy dependency
answering in 20 ms — queue behind them and time out. The gateway is down, and nothing that broke
was on the payments path.

Notice what the deadline did *not* do here. Every search call was correctly abandoned at its
deadline; the timeout budget worked exactly as designed. A deadline bounds how long ONE call may
hold a worker, and says nothing at all about how many calls may hold workers simultaneously. Given
enough arriving traffic, "each call is limited to 30 s" is entirely compatible with "all capacity is
held by search". Bounding concurrency is the missing half, and that is why these are two components
rather than one setting.

WHY THE QUEUE MUST BE BOUNDED, AND SMALL
--------------------------------------------------------------------------------
Bounded concurrency on its own is `asyncio.Semaphore(n)`. The problem is what happens to the
arrivals that find the pool full: they wait, and the waiting list has no limit. The dependency's
backlog has simply been moved inside this process, where it costs memory and where every entry is a
client still holding a connection open. Latency grows without bound while throughput does not grow
at all — throughput is set by the dependency, not by how many callers are hopeful.

That is why rejection is part of the pattern rather than a failure of it. A queue is worth having,
but only for what a queue is actually good at: absorbing a *burst* the pool will drain in a moment.
It cannot absorb sustained overload. It can only delay the moment the overload becomes visible, and
it charges every queued client for the delay.

Sizing then follows from arithmetic rather than taste. If the dependency answers in `L` seconds and
the pool allows `C` concurrent calls, the pool completes `C / L` calls per second, so the last
entrant to a queue of depth `Q` waits roughly `Q * L / C` seconds before its call even starts. With
L = 50 ms, C = 20 and Q = 100 that is 250 ms of pure queueing. So choose `Q` from the latency you
are willing to add, and if the answer is "none", `max_queue=0` is a legal and meaningful
configuration: reject the instant the pool is full.

The same reasoning constrains `max_concurrent`. It has to be smaller than the process's total
capacity — a bulkhead sized at 100% of the workers is not a compartment — while the *sum* across
dependencies is allowed to exceed it. Sizing every bulkhead so the total fits exactly would leave
capacity idle whenever a dependency is quiet, which is most of the time. Deliberate
over-subscription with each individual share capped is the point.

WHY A REJECTION IS NOT AN OUTAGE
--------------------------------------------------------------------------------
`BulkheadFullError` under a traffic spike is this component doing its job, and it should not be
logged at ERROR level once per occurrence. What deserves an alert is the *rate* — a bulkhead that
rejects continuously is either too small or in front of a dependency that has stopped coping, and
those are different problems with different fixes. The snapshot exists to tell them apart.

One thing a rejection must never be is evidence against the dependency. It is a statement about our
own saturation; the dependency was not called and has said nothing. Counting it as a circuit-breaker
failure would let a queue we sized too small trip a breaker on a service that is perfectly healthy,
which is one of the two or three most common ways a resilience layer makes things worse.

WHY CANCELLATION IS THE HARD PART
--------------------------------------------------------------------------------
Every other component in this package is synchronous. This one has to wait, so it is the first place
where a caller can vanish mid-operation: `asyncio.timeout` fires, a client disconnects, a
`TaskGroup` sibling fails and cancellation propagates. Two moments matter, and they need different
handling:

  * cancelled while queued — the waiter must be removed from the queue, or the queue slowly fills
    with ghosts and starts rejecting live callers on behalf of callers that left;
  * cancelled after a slot was granted but before the waiter resumed — the slot must be passed on,
    not dropped. A dropped slot is permanent: the pool's usable capacity falls by one for the
    lifetime of the process, and after `max_concurrent` such events the bulkhead rejects everything
    forever while reporting itself full. There are no failures to point at and nothing in the logs.

That second case is the same failure class as an unresolved `Permit` in `breaker.py`, and it is
worth noticing that both arise from the same shape: a resource whose release depends on control
returning to code that may never run. It is reachable in ordinary operation, not just under
pathological timing, because cancellation and completion genuinely do race — so it is tested
directly rather than argued about.

DESIGN NOTES
--------------------------------------------------------------------------------
* `acquire()` takes no timeout. `asyncio.timeout` already composes with any awaitable, and a caller
  that has a `Deadline` already knows how long it can wait; re-implementing that here would add a
  second thing able to abandon a call and make "who gave up, and why" ambiguous. Cancellation is the
  mechanism, which is precisely why cancellation-safety above is not optional.
* A slot is handed *directly* from the caller releasing it to the longest-waiting caller, rather
  than freed and then re-acquired. `_in_flight` therefore never dips during a handover, so there is
  no window in which a newly arriving caller can see spare capacity that has already been promised.
* FIFO. Under sustained overload LIFO has better goodput — it serves the freshest request, the one
  most likely to still have a client waiting for it, while FIFO serves the oldest, the one most
  likely to have been abandoned already. It is a real argument and it is not taken here: FIFO cannot
  starve anyone, and a queue kept small enough (see the sizing arithmetic above) does not hold
  requests long enough for the difference to be worth the surprise.
* Not thread-safe, and no lock. Same reasoning as `breaker.py`: the target is a single asyncio event
  loop. Every mutation below happens in straight-line code with no `await` between reading state and
  writing it, which is what makes the bookkeeping safe here — and it is a property to preserve when
  editing, not an accident.
* One bulkhead per dependency. Sharing one across dependencies rebuilds the shared pool the pattern
  exists to break up.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import TracebackType

from edge_gateway.resilience.clock import Clock


class BulkheadFullError(RuntimeError):
    """Raised when the pool is full and the queue is full (or absent).

    Carries the numbers rather than only a message, because the useful response differs by case and
    a formatted string forces the handler to parse prose. `queued == queue_capacity` with a large
    capacity says the dependency has stopped coping; `queue_capacity == 0` says the configuration
    chose to shed load immediately and there is nothing surprising happening at all.

    Subclasses `RuntimeError` rather than a project-specific base for the same reason as
    `CircuitOpenError`: a caller that fails to handle it should get something recognisable, and the
    HTTP mapping (503, with the retry hint the gateway is actually able to justify) belongs at the
    edge in slice 4 rather than in here.
    """

    def __init__(self, name: str, *, in_flight: int, queued: int, queue_capacity: int) -> None:
        self.name = name
        self.in_flight = in_flight
        self.queued = queued
        self.queue_capacity = queue_capacity
        super().__init__(
            f"bulkhead {name!r} is saturated: {in_flight} in flight, "
            f"{queued}/{queue_capacity} queued"
        )


@dataclass(frozen=True, slots=True)
class BulkheadSnapshot:
    """A consistent read of one bulkhead, for metrics and for a health endpoint.

    Frozen and taken in one go, so that a dashboard cannot show `queued=8` alongside an `in_flight`
    read a moment later — a combination which never existed and which looks, to whoever is reading
    it during an incident, exactly like a bug in the bulkhead.

    The high-water marks are here because averages are useless for saturation. Mean queue depth on a
    healthy service is approximately zero right up until the moment it is not, and a one-second
    spike that rejected traffic disappears entirely into a per-minute average. `peak_queued` is the
    number that answers "did we ever come close?", and it is the number to size the queue from.

    Counters are cumulative for the lifetime of the object; they are meant to be scraped and
    differenced, not read as instantaneous values.
    """

    name: str
    in_flight: int
    queued: int
    max_concurrent: int
    max_queue: int
    admitted: int = 0
    """Calls that got a slot, whether immediately or after queueing."""
    queued_total: int = 0
    """Calls that had to wait at all. Compare with `admitted`: a pool that queues most of its
    traffic is under-sized for its steady state, whatever the peaks look like."""
    rejected_full: int = 0
    """Calls refused because pool and queue were both full."""
    cancelled_while_queued: int = 0
    """Waiters that gave up or were cancelled while still in the queue. Expected to be non-zero on a
    service with client disconnects; a large value means callers are timing out in our queue, which
    is latency we added and the dependency never saw."""
    cancelled_at_handover: int = 0
    """Waiters interrupted in the same tick a slot was granted to them — the race the module
    docstring warns about, counted so that "we handle it" is an observation rather than a claim. A
    handful under load is normal. Zero forever on a busy, cancellation-heavy service is more likely
    to mean the counter is unreachable than that the race never happens."""
    peak_in_flight: int = 0
    peak_queued: int = 0
    queue_wait_total_seconds: float = 0.0
    """Summed queue wait of admitted calls. Divide by `queued_total` for the mean."""
    queue_wait_max_seconds: float = 0.0
    """The worst wait any admitted call suffered. This is the number that distinguishes "the
    dependency is slow" from "we are the ones being slow", and without it queue latency is
    invisible: it happens entirely before the outbound call starts, so it appears in no downstream
    metric."""


class BulkheadSlot:
    """One unit of admitted concurrency, held for the duration of the call.

    A slot is held across the *whole* downstream interaction, response body included. Releasing it
    when the headers arrive would leave the real work — streaming a large body over a slow link —
    outside the limit, which is precisely the work most able to hold a worker for a long time.

    Released through the context-manager protocol, not by convention, for the reason spelled out in
    the module docstring: a slot that is never released reduces the pool permanently. Prefer
    `Bulkhead.hold()`, which removes the gap between acquiring and entering the `with`.
    """

    __slots__ = ("_bulkhead", "_held")

    def __init__(self, bulkhead: Bulkhead) -> None:
        self._bulkhead = bulkhead
        self._held = True

    @property
    def held(self) -> bool:
        return self._held

    def release(self) -> None:
        """Give the slot back, handing it to the longest-waiting caller if there is one.

        Raises on a second call rather than ignoring it. A double release would return capacity that
        was never taken, so the pool would admit more concurrent calls than its configuration allows
        — the exact failure this class exists to prevent, arrived at from the inside, and silently.
        Loud is better: the bug is in the caller's control flow and it is cheap to find once it
        raises.
        """
        if not self._held:
            raise RuntimeError("this bulkhead slot has already been released")
        self._held = False
        self._bulkhead._release_or_hand_over()

    def __enter__(self) -> BulkheadSlot:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release if still held, and let any exception propagate.

        Deliberately tolerant of an explicit `release()` inside the block, unlike `release()`
        itself: the point of `__exit__` is to guarantee the slot comes back, and "it is already
        back" satisfies that. Returns `None` so exceptions are not suppressed — a bulkhead has no
        opinion about them, and classifying the failure is the breaker's job.
        """
        if self._held:
            self.release()


class Bulkhead:
    """Bounded concurrency with a bounded FIFO queue in front of it.

    Construct one per dependency. See the module docstring for what the two numbers mean and how to
    choose them; neither has a default, for the same reason the breaker's thresholds do not — what
    counts as a safe share of this process's capacity is a property of this deployment, and a
    library-supplied number would be a guess that looks like a recommendation.
    """

    __slots__ = (
        "_admitted",
        "_cancelled_at_handover",
        "_cancelled_while_queued",
        "_clock",
        "_in_flight",
        "_max_concurrent",
        "_max_queue",
        "_name",
        "_peak_in_flight",
        "_peak_queued",
        "_queue_wait_max_seconds",
        "_queue_wait_total_seconds",
        "_queued_total",
        "_rejected_full",
        "_waiters",
    )

    def __init__(
        self,
        name: str,
        *,
        max_concurrent: int,
        max_queue: int,
        clock: Clock,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError(
                f"max_concurrent must be at least 1 (got {max_concurrent!r}); a bulkhead that "
                "admits nothing is an outage, not a limit"
            )
        if max_queue < 0:
            raise ValueError(f"max_queue must not be negative (got {max_queue!r})")

        self._name = name
        self._max_concurrent = max_concurrent
        self._max_queue = max_queue
        self._clock = clock

        self._in_flight = 0
        # Futures for callers waiting for a slot, oldest first. Every future in here is pending: a
        # future is popped at the moment it is resolved (see `_hand_over_to_next_waiter`) and
        # removed by its own waiter on cancellation, so `len(self._waiters)` is the live queue depth
        # and can be compared against `max_queue` directly. Keeping that true is what stops
        # abandoned callers from occupying queue space.
        self._waiters: deque[asyncio.Future[None]] = deque()

        self._admitted = 0
        self._queued_total = 0
        self._rejected_full = 0
        self._cancelled_while_queued = 0
        self._cancelled_at_handover = 0
        self._peak_in_flight = 0
        self._peak_queued = 0
        self._queue_wait_total_seconds = 0.0
        self._queue_wait_max_seconds = 0.0

    # -- observation --------------------------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def in_flight(self) -> int:
        """Slots currently held. Includes a slot handed to a waiter that has not yet resumed."""
        return self._in_flight

    @property
    def queued(self) -> int:
        return len(self._waiters)

    @property
    def available(self) -> int:
        """Slots that could be taken right now, which is the fast-path admission test.

        Never negative: `_in_flight` is only incremented under this check or held constant across a
        handover, so it cannot exceed `max_concurrent`. Exposed because it is genuinely useful to a
        caller deciding whether to attempt optional work — a cache warm, a recommendation lookup —
        that is not worth queueing for.
        """
        return self._max_concurrent - self._in_flight

    def snapshot(self) -> BulkheadSnapshot:
        return BulkheadSnapshot(
            name=self._name,
            in_flight=self._in_flight,
            queued=len(self._waiters),
            max_concurrent=self._max_concurrent,
            max_queue=self._max_queue,
            admitted=self._admitted,
            queued_total=self._queued_total,
            rejected_full=self._rejected_full,
            cancelled_while_queued=self._cancelled_while_queued,
            cancelled_at_handover=self._cancelled_at_handover,
            peak_in_flight=self._peak_in_flight,
            peak_queued=self._peak_queued,
            queue_wait_total_seconds=self._queue_wait_total_seconds,
            queue_wait_max_seconds=self._queue_wait_max_seconds,
        )

    # -- admission ---------------------------------------------------------------------------

    async def acquire(self) -> BulkheadSlot:
        """Take a slot, waiting in the queue if the pool is full.

        Raises `BulkheadFullError` immediately — without awaiting — when pool and queue are both
        full. That the rejection path never yields matters: shedding load has to be cheap, because
        it happens exactly when the process has no spare capacity to be expensive with.

        Prefer `hold()`. The gap between this returning and the caller entering a `with` block is a
        place where an exception leaks a slot, and `hold()` does not have one.
        """
        if self._in_flight < self._max_concurrent:
            # No live waiter can exist here. A slot only becomes free by being handed to the head of
            # the queue, so spare capacity and a non-empty queue are mutually exclusive states, and
            # taking the slot directly cannot barge past anybody.
            self._admit()
            return BulkheadSlot(self)

        if len(self._waiters) >= self._max_queue:
            self._rejected_full += 1
            raise BulkheadFullError(
                self._name,
                in_flight=self._in_flight,
                queued=len(self._waiters),
                queue_capacity=self._max_queue,
            )

        return await self._wait_for_slot()

    @asynccontextmanager
    async def hold(self) -> AsyncIterator[BulkheadSlot]:
        """`async with bulkhead.hold(): ...` — the recommended form.

        Acquire and `with` in one step, so there is no instruction between them at which an
        exception could strand a slot. Yields the slot for callers that want to read `held`; most
        bodies ignore it.
        """
        slot = await self.acquire()
        with slot:
            yield slot

    # -- internals ---------------------------------------------------------------------------

    def _admit(self) -> None:
        """Account for a slot becoming occupied by a caller that did not have to wait."""
        self._in_flight += 1
        self._admitted += 1
        self._peak_in_flight = max(self._peak_in_flight, self._in_flight)

    async def _wait_for_slot(self) -> BulkheadSlot:
        """Join the queue and wait to be handed a slot.

        The cancellation handling is the substance of this method, and it is deliberately written as
        one `except BaseException` rather than as separate `CancelledError` and generic clauses.
        What needs deciding is not *why* control left the `await` but *whether a slot had already
        been given to us*, and that question has the same two answers however we were interrupted.
        """
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        self._queued_total += 1
        self._peak_queued = max(self._peak_queued, len(self._waiters))
        queued_at = self._clock.now()

        try:
            await waiter
        except BaseException:
            if waiter.done() and not waiter.cancelled():
                # A slot was granted in the same tick we were interrupted: the handover already
                # happened and `_in_flight` already counts us. Dropping it here is the permanent
                # capacity leak described in the module docstring, so pass it on instead. This is
                # not a theoretical ordering — a release and a cancellation racing is ordinary.
                self._cancelled_at_handover += 1
                self._release_or_hand_over()
            else:
                # Still queued. Leaving the future in place would let abandoned callers accumulate
                # and eventually reject live ones on their behalf.
                self._discard_waiter(waiter)
                self._cancelled_while_queued += 1
            raise

        # Counted here rather than at the moment of the handover, so that `admitted` means "got a
        # slot it could actually use". A waiter granted a slot and then cancelled before resuming
        # gave it straight back; calling that an admission would inflate the number used to judge
        # whether the pool is the right size.
        self._admitted += 1
        waited = self._clock.now() - queued_at
        self._queue_wait_total_seconds += waited
        self._queue_wait_max_seconds = max(self._queue_wait_max_seconds, waited)
        return BulkheadSlot(self)

    def _discard_waiter(self, waiter: asyncio.Future[None]) -> None:
        """Remove a waiter from the queue if it is still there.

        The membership test is not defensive padding: `_hand_over_to_next_waiter` skips and discards
        futures that were cancelled before it reached them, so by the time a cancelled waiter's own
        handler runs, the queue may already have moved on without it.
        """
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass

    def _release_or_hand_over(self) -> None:
        """A holder is finished: pass the slot to the head of the queue, or free it.

        `_in_flight` is left untouched on a handover. The slot never becomes available, so there is
        no instant at which an arriving caller could take it ahead of someone already queued, and
        FIFO holds without a fairness mechanism.
        """
        if self._hand_over_to_next_waiter():
            return
        self._in_flight -= 1

    def _hand_over_to_next_waiter(self) -> bool:
        """Resolve the oldest live waiter's future. True if a slot was handed over.

        Cancelled futures are dropped as they are encountered rather than swept periodically. They
        are rare, they can only be at the front of the queue for as long as it takes their own task
        to be scheduled, and a sweep would be a second mechanism maintaining the same invariant.
        """
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.done():
                continue  # cancelled while queued; its own handler will find it already gone
            waiter.set_result(None)
            return True
        return False
