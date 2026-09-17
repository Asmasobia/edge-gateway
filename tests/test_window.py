"""Tests for the bucketed sliding window.

The window is the breaker's only source of evidence, so a bug here presents as a breaker that trips
for no reason or fails to trip at all — and in both cases the breaker looks like the culprit. These
tests therefore go after the window's own failure modes directly rather than through a breaker.

Weighted toward what the bucketing gets wrong rather than what it gets right. "Two failures were
recorded, so the rate is 1.0" is compatible with an implementation that never forgets anything,
which is the one thing a sliding window must do. So most of what follows is about expiry: at the
boundary, after a long idle period, when a ring index is reused, and when the clock origin is
negative.

Not one test sleeps. Every temporal assertion is driven by `ManualClock.advance`.
"""

from __future__ import annotations

import dataclasses

import pytest

from edge_gateway.resilience import ManualClock, SlidingWindow, WindowCounts


def make(
    *,
    window_seconds: float = 10.0,
    bucket_count: int = 10,
    start: float = 0.0,
) -> tuple[SlidingWindow, ManualClock]:
    clock = ManualClock(start)
    return SlidingWindow(
        window_seconds=window_seconds, bucket_count=bucket_count, clock=clock
    ), clock


class _BrokenClock:
    """A clock that can move backwards, which a monotonic clock cannot.

    `ManualClock` refuses to rewind on purpose (see clock.py), so this local fake is the only way to
    reach the one branch that requires an impossible clock: a bucket stamped in the future. Testing
    a defence against a broken invariant needs a way to break the invariant.
    """

    __slots__ = ("now_value",)

    def __init__(self, now_value: float) -> None:
        self.now_value = now_value

    def now(self) -> float:
        return self.now_value


# ---------------------------------------------------------------------------
# WindowCounts, the value object
# ---------------------------------------------------------------------------


def test_empty_window_reports_a_zero_rate_rather_than_nan() -> None:
    """0.0 is the fail-safe lie: "no evidence of failure", so a breaker consulting it does nothing.

    Pinned by a test because the alternative is tempting — `NaN` is more honest about missing data —
    and because `NaN >= threshold` happens to be False, so the wrong choice would pass every test
    that only checks the breaker's behaviour. It stops being correct the moment someone rewrites the
    comparison as `not (rate < threshold)`.
    """
    counts = WindowCounts(successes=0, failures=0)
    assert counts.total == 0
    assert counts.failure_rate == 0.0
    assert counts.failure_rate == counts.failure_rate  # i.e. not NaN, which is unequal to itself


def test_failure_rate_is_failures_over_total() -> None:
    assert WindowCounts(successes=3, failures=1).failure_rate == 0.25
    assert WindowCounts(successes=0, failures=7).failure_rate == 1.0
    assert WindowCounts(successes=7, failures=0).failure_rate == 0.0


def test_counts_are_frozen() -> None:
    """Frozen so a caller cannot adjust the evidence after reading it. Cheap to state, and it is the
    property that makes passing the object around safe."""
    counts = WindowCounts(successes=1, failures=1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        counts.failures = 5  # type: ignore[misc]


def test_a_reading_does_not_change_when_the_window_moves_on() -> None:
    """The reason `counts()` returns a value object and not a live view.

    A breaker asks two questions of one reading — is there enough traffic, and is the rate too high?
    If the answers could come from different windows, the breaker would occasionally act on a
    combination that never existed: a rare, unreproducible trip with nothing in the logs to explain
    it.
    """
    window, clock = make()
    window.record_failure()
    reading = window.counts()

    clock.advance(60)
    window.record_success()

    assert reading == WindowCounts(successes=0, failures=1)
    assert window.counts() == WindowCounts(successes=1, failures=0)


# ---------------------------------------------------------------------------
# Recording and reading
# ---------------------------------------------------------------------------


def test_events_in_the_same_bucket_accumulate() -> None:
    window, clock = make()
    window.record_success()
    window.record_success()
    window.record_failure()
    clock.advance(0.5)  # still inside the first one-second bucket
    window.record_failure()

    assert window.counts() == WindowCounts(successes=2, failures=2)
    assert window.counts().failure_rate == 0.5


def test_the_current_partial_bucket_is_included() -> None:
    """It holds the freshest evidence there is. Excluding it to keep the span exact would delay
    every decision by up to a full bucket in exchange for accuracy the breaker cannot use."""
    window, clock = make()
    window.record_failure()
    clock.advance(0.1)
    assert window.counts().failures == 1


def test_events_spread_across_buckets_are_summed() -> None:
    window, clock = make()
    for _ in range(5):
        window.record_failure()
        clock.advance(1.0)  # one event per bucket, five buckets
    assert window.counts() == WindowCounts(successes=0, failures=5)


# ---------------------------------------------------------------------------
# Forgetting, which is the whole point
# ---------------------------------------------------------------------------


def test_the_window_spans_exactly_bucket_count_buckets() -> None:
    """Pins `<` rather than `<=` in `_is_live`.

    With ten one-second buckets, an event is still counted 9 seconds later and gone at 10. `<=`
    would include one extra bucket, making every window in the system one bucket longer than its
    configuration says — a discrepancy nobody would notice and everybody would eventually rely on.
    """
    window, clock = make(window_seconds=10.0, bucket_count=10)
    window.record_failure()

    clock.advance(9.0)
    assert window.counts().failures == 1, "still inside the ten-bucket window"

    clock.advance(1.0)
    assert window.counts().failures == 0, "the tenth bucket back has left the window"


def test_a_long_idle_period_does_not_resurrect_old_counts() -> None:
    """The failure mode a naive ring has: bucket indices repeat every `window_seconds`, so without
    an epoch stamp per bucket, a failure from an hour ago lands back in the current window the next
    time that index comes round — and the first request after a quiet night trips the breaker."""
    window, clock = make()
    for _ in range(9):
        window.record_failure()

    clock.advance(3600)
    window.record_success()

    assert window.counts() == WindowCounts(successes=1, failures=0)
    assert window.counts().failure_rate == 0.0


def test_a_reused_bucket_index_is_cleared_before_being_written() -> None:
    """Exactly one window later, the ring is back at the same index. The slot must be zeroed, not
    added to — this is the lazy expiry that replaces a background sweeper."""
    window, clock = make(window_seconds=10.0, bucket_count=10)
    window.record_failure()
    window.record_failure()

    clock.advance(10.0)  # epoch 10, which maps to index 0 again
    window.record_success()

    assert window.counts() == WindowCounts(successes=1, failures=0)


def test_events_expire_one_bucket_at_a_time() -> None:
    """The granularity cost, stated as behaviour rather than as prose.

    Ten failures spread one per bucket leave the window one per second rather than all at once, so
    the count decays in steps. Anything relying on the span being exactly ten seconds would be
    relying on something this implementation does not promise.
    """
    window, clock = make(window_seconds=10.0, bucket_count=10)
    for bucket in range(10):
        window.record_failure()
        if bucket < 9:
            clock.advance(1.0)  # one failure in each of epochs 0 through 9

    assert window.counts().failures == 10, "at t=9 the whole ring is live"
    clock.advance(1.0)
    assert window.counts().failures == 9, "epoch 0 has left, one bucket at a time"
    clock.advance(3.0)
    assert window.counts().failures == 6, "and three more with it"


def test_a_single_bucket_window_is_all_or_nothing() -> None:
    """The degenerate configuration, checked because it is legal and because off-by-one bugs in the
    liveness arithmetic show up here first: with one bucket the window is either the current bucket
    or nothing at all."""
    window, clock = make(window_seconds=1.0, bucket_count=1)
    window.record_failure()
    assert window.counts().failures == 1

    clock.advance(0.99)
    assert window.counts().failures == 1
    clock.advance(0.02)
    assert window.counts().failures == 0


def test_expiry_is_correct_when_the_clock_origin_is_negative() -> None:
    """Pins `math.floor` over `int()` in `_epoch_now`.

    `time.monotonic()`'s origin is arbitrary and the language does not promise it is positive, so
    instants either side of zero are possible. `int()` truncates toward zero, which folds the two
    epochs adjacent to zero into one — and the observable consequence is not a wrong count but a
    failure to expire: two buckets share an index, so old data stays live one bucket too long.

    With two one-second buckets and an event at t=-1.5 (epoch -2), t=+0.5 is epoch 0 and the event
    is exactly out of the window. Under `int()` the event would be stamped epoch -1 and still be
    counted here.
    """
    window, clock = make(window_seconds=2.0, bucket_count=2, start=-1.5)
    window.record_failure()
    assert window.counts().failures == 1

    clock.advance(2.0)  # now at +0.5
    assert window.counts().failures == 0


def test_a_bucket_stamped_in_the_future_is_not_counted() -> None:
    """Impossible from a monotonic clock, so it signals a real bug — one window handed two clocks,
    most likely. Treating such a bucket as live would blend in counts from a timeline this window is
    not on; treating it as stale means the bad data ages out by itself."""
    clock = _BrokenClock(10.0)
    window = SlidingWindow(window_seconds=10.0, bucket_count=10, clock=clock)
    window.record_failure()
    assert window.counts().failures == 1

    clock.now_value = 0.0  # the impossible part
    assert window.counts() == WindowCounts(successes=0, failures=0)


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------


def test_reset_forgets_everything() -> None:
    """Exists for one caller: a breaker returning to CLOSED. See `test_breaker.py` for why skipping
    it makes recovery impossible."""
    window, _ = make()
    for _ in range(5):
        window.record_failure()
        window.record_success()

    window.reset()

    assert window.counts() == WindowCounts(successes=0, failures=0)
    assert window.counts().failure_rate == 0.0


def test_a_window_is_usable_immediately_after_a_reset() -> None:
    """A reset window keeps working: the next write lands in a live bucket and is counted. Resetting
    the stamps to a sentinel that happened to look current would show up here."""
    window, clock = make()
    window.record_failure()
    window.reset()

    window.record_success()
    assert window.counts() == WindowCounts(successes=1, failures=0)

    clock.advance(1.0)
    window.record_failure()
    assert window.counts() == WindowCounts(successes=1, failures=1)


def test_reset_returns_the_window_to_its_constructed_state() -> None:
    """Asserted on the internals, because this one is not observable from the outside.

    A mutation test made the point: deleting the epoch-stamp clearing from `reset` left the whole
    suite green. It is genuinely unobservable through `counts()` — a bucket whose counts are zero
    contributes nothing whether its stamp is stale or current — so behavioural tests cannot
    distinguish the two implementations.

    The invariant worth keeping is nonetheless real: a reset window is indistinguishable from a new
    one. That is what makes `reset` safe to reason about, and it is what stops a future change (a
    per-bucket `is_empty` flag, say, or an aggregate maintained alongside the buckets) from leaving
    half of the state behind. An invariant nothing checks is a comment, so it is checked here rather
    than deleted along with the line it protects.
    """
    window, clock = make()
    for _ in range(4):
        window.record_failure()
        window.record_success()
        clock.advance(1.0)

    window.reset()

    fresh, _ = make()
    assert window._successes == fresh._successes
    assert window._failures == fresh._failures
    assert window._epochs == fresh._epochs


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("window_seconds", "bucket_count", "message"),
    [
        (0.0, 10, "window_seconds must be positive"),
        (-1.0, 10, "window_seconds must be positive"),
        (10.0, 0, "bucket_count must be at least 1"),
        (10.0, -3, "bucket_count must be at least 1"),
    ],
)
def test_construction_rejects_impossible_geometry(
    window_seconds: float, bucket_count: int, message: str
) -> None:
    """A zero or negative window means dividing by zero or by a negative when computing the bucket
    size, and a zero bucket count means a ring with no slots. Both are startup-time mistakes, and
    catching them at construction is what stops them from being 3am mistakes instead."""
    with pytest.raises(ValueError, match=message):
        SlidingWindow(window_seconds=window_seconds, bucket_count=bucket_count, clock=ManualClock())


def test_memory_does_not_grow_with_throughput() -> None:
    """The claim that justifies buckets over a deque of timestamps, asserted rather than assumed.

    This one test reaches into the private arrays on purpose: the property being checked is about
    the *representation*, not the behaviour, so there is nothing observable to assert instead. A
    deque implementation would hold 50,000 entries after this loop; the ring holds twenty integers.
    """
    window, clock = make(window_seconds=10.0, bucket_count=10)
    for i in range(50_000):
        window.record_success()
        if i % 5_000 == 0:
            clock.advance(1.0)

    assert len(window._successes) == 10
    assert len(window._failures) == 10
    assert len(window._epochs) == 10
