"""A bounded-memory sliding window of successes and failures.

WHAT THIS IS FOR
--------------------------------------------------------------------------------
A circuit breaker needs to answer one question: "of the calls that happened *recently*, what
fraction failed?" The word doing the work is "recently". A counter that only ever increments
answers a different and useless question — the fraction that failed since process start — under
which a service that was broken for an hour this morning is still considered broken at midnight.

So the window has to forget. How it forgets is the design decision.

WHY BUCKETS AND NOT A LIST OF TIMESTAMPS
--------------------------------------------------------------------------------
The obvious implementation is a deque of `(timestamp, was_success)` pairs, evicting from the front
anything older than the window. It is exact, and it is unusable on the hot path:

  - Memory scales with THROUGHPUT, not with the window. At 20k requests/second over a 10-second
    window that is 200,000 live entries per breaker, per process. The one component whose job is to
    protect the service under load becomes a source of pressure under load — and it grows precisely
    when the service is busiest, which is the worst possible correlation.
  - Eviction is unbounded work at an arbitrary moment. A quiet period followed by a burst means one
    unlucky request pays to evict everything that expired while nothing was happening.

The bucketed ring below fixes both. Time is chopped into `bucket_count` fixed slots; each slot holds
two integers. Memory is constant — ten buckets is twenty integers regardless of whether the service
handles ten requests a second or a hundred thousand. Recording is O(1) with no allocation, and
reading is O(bucket_count) with a fixed, tiny constant.

THE COST, STATED PLAINLY
--------------------------------------------------------------------------------
Granularity. Events leave the window a whole bucket at a time rather than individually, so the
effective span is not exactly `window_seconds` — it varies between `window_seconds - bucket_seconds`
and `window_seconds` as the current bucket fills and rotates. With the defaults (10 s / 10 buckets)
that is a 9-to-10-second window rather than a 10-second one.

That inaccuracy is acceptable here because of what the number is *used for*: a threshold comparison
against a rate, where the input is already a noisy sample of a changing world. Deciding "is more
than half of the traffic failing" does not become wrong because the sample covered 9.4 seconds
rather than a full ten. Contrast billing, or rate limiting against a contractual quota, where the
same approximation would be unacceptable and the deque is the right choice. It is worth being able
to say which situation you are in — "use buckets, they are faster" is not an argument, it is a
preference.

More buckets narrows the error and costs a little memory and scan time; fewer buckets is coarser.
That is the entire tuning story, which is why both numbers are constructor arguments.

WHY THERE IS NO BACKGROUND SWEEPER
--------------------------------------------------------------------------------
Nothing here runs on a timer. A bucket carries the epoch it was written for, and a bucket whose
epoch is too old is simply *treated* as empty and overwritten the moment someone writes to it.
Expiry is therefore lazy and free: no task to schedule, no thread to join, nothing to shut down
cleanly, and — the reason it matters most for this project — no dependence on real time, so the
whole thing is driven by an injected clock and tested without sleeping.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from edge_gateway.resilience.clock import Clock

# Sentinel epoch meaning "this bucket has never been written". Any value far enough below zero that
# it can never be mistaken for a live bucket: a `ManualClock` starts at 0.0, so epoch 0 is
# legitimate and the sentinel cannot merely be -1 in general. Using a large negative integer means
# the staleness arithmetic (`epoch - stamp < bucket_count`) rejects it without needing a separate
# `is_empty` check on every bucket read.
_NEVER_WRITTEN = -(2**62)


@dataclass(frozen=True, slots=True)
class WindowCounts:
    """An immutable reading of the window at one instant.

    Returned as a value object rather than as two out-parameters or a live view, because a breaker
    makes several decisions from one reading (is there enough traffic? is the rate too high?) and
    those must be based on the *same* sample. Handing back a live object would let the window rotate
    between two questions about it, so the answers could describe different windows — a race that is
    rare, unreproducible, and would present as a breaker that very occasionally trips for no reason
    the logs can explain.
    """

    successes: int
    failures: int

    @property
    def total(self) -> int:
        return self.successes + self.failures

    @property
    def failure_rate(self) -> float:
        """Failures as a fraction of total, in [0.0, 1.0].

        An empty window reports 0.0, not `NaN` and not an exception. This is a deliberate choice
        about which lie is safer. 0.0 means "no evidence of failure", so a breaker consulting it
        does nothing — the fail-safe direction, since with no traffic there is nothing to
        protect. `NaN` would be more honest about the absence of data but would poison every
        comparison silently: `NaN >= threshold` is False, which happens to be the same behaviour
        by accident, and accidental correctness stops holding the moment someone writes `not
        (rate < threshold)`.

        Callers must not treat a low rate as proof of health regardless — that is what the breaker's
        minimum-throughput check is for, and this docstring is where the two ideas meet.
        """
        if self.total == 0:
            return 0.0
        return self.failures / self.total


class SlidingWindow:
    """Counts of successes and failures over the recent past, in fixed-memory buckets.

    Not thread-safe, and deliberately not. See the note in `breaker.py`: the intended deployment is
    a single asyncio event loop, where every method here runs to completion without an await point
    and is therefore already atomic with respect to other coroutines. Adding a lock would cost an
    acquisition on the hottest path in the gateway to solve a problem this deployment does not have
    — and would silently paper over an actual misuse (sharing one breaker across threads) that
    ought to be a design conversation, not a lock.
    """

    __slots__ = ("_bucket_count", "_bucket_seconds", "_clock", "_epochs", "_failures", "_successes")

    def __init__(self, *, window_seconds: float, bucket_count: int, clock: Clock) -> None:
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive (got {window_seconds!r})")
        if bucket_count < 1:
            raise ValueError(f"bucket_count must be at least 1 (got {bucket_count!r})")

        self._clock = clock
        self._bucket_count = bucket_count
        self._bucket_seconds = window_seconds / bucket_count
        self._successes = [0] * bucket_count
        self._failures = [0] * bucket_count
        self._epochs = [_NEVER_WRITTEN] * bucket_count

    def record_success(self) -> None:
        index = self._live_bucket()
        self._successes[index] += 1

    def record_failure(self) -> None:
        index = self._live_bucket()
        self._failures[index] += 1

    def counts(self) -> WindowCounts:
        """Sum the buckets that are still inside the window.

        Includes the current, partially-filled bucket: it holds the freshest evidence available, and
        excluding it would delay every decision by up to one bucket for no benefit.
        """
        current_epoch = self._epoch_now()
        successes = 0
        failures = 0
        for index in range(self._bucket_count):
            if self._is_live(self._epochs[index], current_epoch):
                successes += self._successes[index]
                failures += self._failures[index]
        return WindowCounts(successes=successes, failures=failures)

    def reset(self) -> None:
        """Forget everything.

        Exists for one specific caller: a breaker returning to CLOSED after a successful recovery.
        Without it the failures that opened the circuit are still inside the window, so the breaker
        would consult them, see the same high failure rate that tripped it, and immediately re-open
        — recovery would be impossible and the symptom would be a circuit that flaps between OPEN
        and HALF_OPEN forever while the dependency is demonstrably healthy. See `breaker.py`.
        """
        for index in range(self._bucket_count):
            self._successes[index] = 0
            self._failures[index] = 0
            self._epochs[index] = _NEVER_WRITTEN

    def _epoch_now(self) -> int:
        """Which bucket-sized slice of time we are currently in.

        A monotonically increasing integer, so `epoch_a - epoch_b` is how many buckets apart two
        instants are. `math.floor` rather than `int()` because the clock's origin is arbitrary and
        may be negative in principle, and `int()` truncates toward zero — which would make two
        adjacent instants either side of zero share an epoch.
        """
        return math.floor(self._clock.now() / self._bucket_seconds)

    def _live_bucket(self) -> int:
        """The index to write to now, clearing it first if it holds expired data.

        This is where lazy expiry happens. `epoch % bucket_count` means bucket indices are reused
        every `window_seconds`, so a slot's contents are only meaningful together with the epoch
        they were written for. If the stored epoch is not the current one, whatever is in the slot
        belongs to a previous lap of the ring and must be discarded rather than added to. Otherwise
        counts from ten seconds ago — or ten hours ago, on an idle service — would be resurrected
        and blended into the present.
        """
        current_epoch = self._epoch_now()
        index = current_epoch % self._bucket_count
        if self._epochs[index] != current_epoch:
            self._successes[index] = 0
            self._failures[index] = 0
            self._epochs[index] = current_epoch
        return index

    def _is_live(self, stamp: int, current_epoch: int) -> bool:
        """Whether a bucket stamped `stamp` still counts as recent.

        Strictly `< bucket_count`, which is what makes the window include exactly `bucket_count`
        buckets: the current one plus the `bucket_count - 1` before it. `<=` would include one extra
        bucket and quietly make every window one bucket longer than configured.

        The `stamp > current_epoch` guard covers a bucket stamped in the FUTURE. That cannot arise
        from a monotonic clock and therefore signals a real bug — a shared window handed two
        different clocks, most likely. Treating it as live would mix in counts from a timeline this
        window is not on; treating it as stale means the bad data ages out on its own.
        """
        if stamp > current_epoch:
            return False
        return current_epoch - stamp < self._bucket_count
