"""Tests for the circuit breaker.

Organised around the four decisions in `breaker.py`'s module docstring, because those are the ones a
plausible-looking implementation gets wrong:

 1. a rate over a window, not consecutive failures
 2. a minimum throughput before the rate means anything
 3. bounded concurrency in HALF_OPEN
 4. the window is reset when the circuit closes

Then the two mechanisms that make the state machine honest under real timing — generation stamping
for outcomes that arrive after the state they belonged to, and the permit context manager that
cannot leak a probe slot — followed by configuration validation.

A note on what is deliberately NOT asserted: nothing here checks that a call to a dependency
actually happened, because there is no dependency. The breaker's contract is entirely about
permission and bookkeeping, and slice 4 is where it is wired to something that can fail for real.

Not one test sleeps.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable

import pytest

from edge_gateway.resilience import (
    BreakerConfig,
    BreakerState,
    CircuitBreaker,
    CircuitOpenError,
    ManualClock,
    Outcome,
)

# ---------------------------------------------------------------------------
# Fixtures and helpers
#
# A factory rather than a module-level constant config, so that a test which needs one threshold
# changed says exactly that and inherits the rest. `config(half_open_max_concurrent=2)` tells
# you what the test is about; a wall of seven keyword arguments in every test does not.
# ---------------------------------------------------------------------------


def config(
    *,
    failure_rate_threshold: float = 0.5,
    minimum_throughput: int = 4,
    window_seconds: float = 10.0,
    open_duration_seconds: float = 30.0,
    half_open_max_concurrent: int = 1,
    half_open_successes_to_close: int = 2,
    bucket_count: int = 10,
) -> BreakerConfig:
    return BreakerConfig(
        failure_rate_threshold=failure_rate_threshold,
        minimum_throughput=minimum_throughput,
        window_seconds=window_seconds,
        open_duration_seconds=open_duration_seconds,
        half_open_max_concurrent=half_open_max_concurrent,
        half_open_successes_to_close=half_open_successes_to_close,
        bucket_count=bucket_count,
    )


def make(cfg: BreakerConfig | None = None) -> tuple[CircuitBreaker, ManualClock]:
    clock = ManualClock()
    breaker = CircuitBreaker("payments", config=cfg if cfg is not None else config(), clock=clock)
    return breaker, clock


def state_of(breaker: CircuitBreaker) -> BreakerState:
    """`breaker.state`, read through a function call. The indirection is for the type checker.

    `mypy --strict` narrows the type of a property access: after `assert breaker.state is CLOSED` it
    believes the value is `Literal[CLOSED]` for the rest of the function, and reports a later
    `assert breaker.state is OPEN` as a comparison that can never be true. The narrowing is wrong
    here — the property is time-dependent and the clock moves in between — but the checker is not
    unreasonable to be suspicious of a value that changes with no assignment in sight. Reading it
    through a call is a smaller price than an ignore comment on every state transition in the file.
    """
    return breaker.state


def call(breaker: CircuitBreaker, outcome: Outcome) -> None:
    """One call, resolved with the given outcome. The `with` block is how production code is
    meant to use this, so the tests use it too — a test that bypasses the context manager would not
    be exercising the shape that ships."""
    with breaker.acquire() as permit:
        if outcome is Outcome.SUCCESS:
            permit.success()
        elif outcome is Outcome.FAILURE:
            permit.failure()
        else:
            permit.not_counted()


def succeed(breaker: CircuitBreaker, times: int = 1) -> None:
    for _ in range(times):
        call(breaker, Outcome.SUCCESS)


def fail(breaker: CircuitBreaker, times: int = 1) -> None:
    for _ in range(times):
        call(breaker, Outcome.FAILURE)


def trip(breaker: CircuitBreaker, failures: int = 4) -> None:
    """Drive the breaker from CLOSED to OPEN the ordinary way, and assert it got there.

    Asserting inside the helper matters: if a future change to the defaults stopped four failures
    from being enough, every test using this helper would otherwise fail somewhere further along
    with a confusing message about probes instead of here with a clear one.
    """
    fail(breaker, failures)
    assert state_of(breaker) is BreakerState.OPEN, (
        "helper expected these failures to open the circuit"
    )


def enter_half_open(breaker: CircuitBreaker, clock: ManualClock) -> None:
    trip(breaker)
    clock.advance(30.0)
    assert state_of(breaker) is BreakerState.HALF_OPEN


# ---------------------------------------------------------------------------
# Starting state
# ---------------------------------------------------------------------------


def test_a_new_breaker_is_closed_and_empty() -> None:
    breaker, _ = make()
    snapshot = breaker.snapshot()

    assert breaker.name == "payments"
    assert snapshot.state is BreakerState.CLOSED
    assert snapshot.counts.total == 0
    assert snapshot.counts.failure_rate == 0.0
    assert snapshot.retry_after_s is None, "a closed circuit implies no wait"
    assert (snapshot.trips, snapshot.rejected_calls) == (0, 0)
    assert (snapshot.stale_outcomes, snapshot.unresolved_permits) == (0, 0)


def test_successes_alone_never_open_the_circuit() -> None:
    breaker, _ = make()
    succeed(breaker, times=100)
    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.successes == 100


# ---------------------------------------------------------------------------
# Decision 1: a rate over a window, not consecutive failures
# ---------------------------------------------------------------------------


def test_a_partial_failure_rate_trips_the_circuit() -> None:
    """The case a consecutive-failure breaker misses entirely.

    Every failure below is followed immediately by a success, so the longest run of failures is one.
    A "trip after five in a row" breaker — or even "two in a row" — would never fire here, while the
    dependency is failing half the requests sent to it.
    """
    breaker, _ = make(config(failure_rate_threshold=0.5, minimum_throughput=4))

    succeed(breaker)
    fail(breaker)
    succeed(breaker)
    fail(breaker)

    assert state_of(breaker) is BreakerState.OPEN
    assert breaker.snapshot().counts.failure_rate == 0.5
    assert breaker.snapshot().trips == 1


def test_a_rate_below_the_threshold_never_trips_however_much_traffic() -> None:
    """A breaker must tolerate a background error rate. One failure in four, sustained over eighty
    calls, is a 25% rate against a 50% threshold — plenty of absolute failures, and correctly not a
    reason to stop calling the dependency."""
    breaker, _ = make(config(failure_rate_threshold=0.5, minimum_throughput=4))

    for _ in range(20):
        fail(breaker)
        succeed(breaker, times=3)

    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.failure_rate == 0.25


def test_the_threshold_is_inclusive() -> None:
    """A rate exactly equal to the threshold opens it. Which side of the boundary this falls
    on barely matters, but it must be pinned: a refactor flipping `>=` to `>` would make
    a threshold of 1.0 mean "never trip", which is the single most consequential silent change this
    class can undergo."""
    breaker, _ = make(config(failure_rate_threshold=0.5, minimum_throughput=4))

    succeed(breaker, times=2)
    fail(breaker, times=2)

    assert breaker.snapshot().counts.failure_rate == 0.5
    assert state_of(breaker) is BreakerState.OPEN


def test_a_threshold_of_one_requires_total_failure() -> None:
    """Legal, and defensible for a dependency with a flaky baseline: open only when nothing
    is working. Worth a test because it is where an off-by-one in the comparison shows up as a
    breaker that never fires."""
    breaker, _ = make(config(failure_rate_threshold=1.0, minimum_throughput=4))

    fail(breaker, times=3)
    succeed(breaker)
    fail(breaker, times=6)
    assert state_of(breaker) is BreakerState.CLOSED, (
        "one success in the window means not everything failed"
    )

    # A fresh window with nothing but failures in it does reach 1.0.
    breaker, _ = make(config(failure_rate_threshold=1.0, minimum_throughput=4))
    fail(breaker, times=4)
    assert state_of(breaker) is BreakerState.OPEN


def test_the_rate_is_only_re_evaluated_after_a_failure() -> None:
    """The accepted imprecision, asserted rather than left in a comment.

    Here two failures then two successes leave the window at exactly the threshold, yet the circuit
    stays closed: the rate was below the minimum throughput when the failures arrived, and successes
    do not trigger an evaluation. The next failure trips it. The alternative — re-checking on every
    `acquire` — would double the window scans on the healthiest path to shorten a delay that only
    exists while the dependency is still failing.
    """
    breaker, _ = make(config(failure_rate_threshold=0.5, minimum_throughput=4))

    fail(breaker, times=2)
    succeed(breaker, times=2)
    assert breaker.snapshot().counts.failure_rate == 0.5
    assert state_of(breaker) is BreakerState.CLOSED

    fail(breaker)
    assert state_of(breaker) is BreakerState.OPEN


def test_failures_that_leave_the_window_stop_counting() -> None:
    """Three failures, a ten-second gap, three more. Six failures and the circuit stays closed,
    because only three of them are recent enough to be evidence. Without expiry this would trip, and
    a service would circuit-break on failures spread across an entire afternoon."""
    breaker, clock = make(
        config(failure_rate_threshold=0.5, minimum_throughput=4, window_seconds=10.0)
    )

    fail(breaker, times=3)
    clock.advance(10.0)
    fail(breaker, times=3)

    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.total == 3


# ---------------------------------------------------------------------------
# Decision 2: a minimum throughput before the rate means anything
# ---------------------------------------------------------------------------


def test_one_failed_call_does_not_open_the_circuit() -> None:
    """The 3am health-check scenario: one request in the window, it fails, the rate is 100% —
    and without a minimum throughput the service takes itself offline on a sample of one."""
    breaker, _ = make(config(minimum_throughput=4))

    fail(breaker)

    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.failure_rate == 1.0, (
        "the rate really is 100%; it just means nothing"
    )


def test_the_circuit_opens_on_the_call_that_reaches_minimum_throughput() -> None:
    """Pins the boundary from below: three failures are not enough, the fourth is. An off-by-one
    here is invisible in production, since both versions eventually trip."""
    breaker, _ = make(config(minimum_throughput=4))

    fail(breaker, times=3)
    assert state_of(breaker) is BreakerState.CLOSED

    fail(breaker)
    assert state_of(breaker) is BreakerState.OPEN


def test_calls_that_are_not_evidence_do_not_count_toward_throughput() -> None:
    """A NOT_COUNTED outcome must not pad the denominator either. If it did, a burst of 404s would
    satisfy the minimum throughput and let a single genuine failure open the circuit — the sample
    size would be real and the sample would not be."""
    breaker, _ = make(config(minimum_throughput=4))

    for _ in range(20):
        call(breaker, Outcome.NOT_COUNTED)
    assert breaker.snapshot().counts.total == 0

    fail(breaker)
    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.total == 1


# ---------------------------------------------------------------------------
# OPEN: fail fast, and say when to come back
# ---------------------------------------------------------------------------


def test_an_open_circuit_rejects_immediately_and_counts_what_it_saved() -> None:
    breaker, _ = make()
    trip(breaker)

    for _ in range(5):
        with pytest.raises(CircuitOpenError) as caught:
            breaker.acquire()
        assert caught.value.state is BreakerState.OPEN
        assert caught.value.breaker_name == "payments"

    snapshot = breaker.snapshot()
    assert snapshot.rejected_calls == 5
    assert snapshot.state is BreakerState.OPEN


def test_retry_after_counts_down_toward_the_end_of_the_cooldown() -> None:
    """This is the number that becomes an HTTP `Retry-After`, so it has to describe the wait that is
    actually left rather than the configured duration."""
    breaker, clock = make(config(open_duration_seconds=30.0))
    trip(breaker)

    assert breaker.snapshot().retry_after_s == pytest.approx(30.0)
    clock.advance(10.0)
    assert breaker.snapshot().retry_after_s == pytest.approx(20.0)
    clock.advance(19.5)
    assert breaker.snapshot().retry_after_s == pytest.approx(0.5)


def test_retry_after_is_never_negative() -> None:
    """White-box, deliberately, and the reason is worth being explicit about: through the public API
    `_retry_after_s` is only ever reached just after an elapsed cooldown has been promoted, so the
    remaining time is always positive and the clamp cannot be reached. It guards a future caller
    rather than a current one, and an invariant worth keeping in the code is worth pinning in a test
    instead of trusting the next person to preserve the call ordering that makes it hold."""
    breaker, clock = make(config(open_duration_seconds=30.0))
    trip(breaker)

    breaker._opened_at = clock.now() - 999.0
    assert breaker._retry_after_s() == 0.0


def test_the_error_message_carries_enough_to_debug_from() -> None:
    """An operator reading one log line should learn which dependency, what state, and how long. The
    breaker name in particular: a gateway with a dozen breakers produces a dozen identical messages
    otherwise."""
    breaker, _ = make(config(open_duration_seconds=30.0))
    trip(breaker)

    with pytest.raises(CircuitOpenError) as caught:
        breaker.acquire()

    message = str(caught.value)
    assert "payments" in message
    assert "open" in message
    assert "30.000s" in message


# ---------------------------------------------------------------------------
# Decision 3: bounded concurrency in HALF_OPEN
# ---------------------------------------------------------------------------


def test_the_cooldown_boundary_admits_a_probe() -> None:
    """Pins `>=` on the cooldown comparison. The boundary instant is inconsequential — a probe
    a microsecond early cannot matter — but pinning it means a refactor cannot quietly make every
    cooldown in the fleet one tick longer than its configuration says."""
    breaker, clock = make(config(open_duration_seconds=30.0))
    trip(breaker)

    clock.advance(29.999)
    assert state_of(breaker) is BreakerState.OPEN

    clock.advance(0.001)
    assert state_of(breaker) is BreakerState.HALF_OPEN


def test_half_open_admits_only_the_configured_number_of_probes() -> None:
    """The thundering-herd defence. Without the cap, the moment the cooldown expires every waiting
    request is dispatched at a dependency that has just been broken, which knocks it straight back
    down — so the breaker becomes the stampede it exists to prevent, on a repeating cycle."""
    breaker, clock = make(config(half_open_max_concurrent=2))
    enter_half_open(breaker, clock)

    first = breaker.acquire()
    second = breaker.acquire()
    assert first.is_trial and second.is_trial

    with pytest.raises(CircuitOpenError) as caught:
        breaker.acquire()
    assert caught.value.state is BreakerState.HALF_OPEN
    assert caught.value.retry_after_s is None, (
        "a trial slot may free in a microsecond or a minute; inventing a number the client would "
        "trust is worse than admitting the wait is unknowable"
    )
    assert breaker.snapshot().trials_in_flight == 2

    # A resolved probe returns its slot, so recovery keeps making progress under the cap.
    first.not_counted()
    assert breaker.snapshot().trials_in_flight == 1
    third = breaker.acquire()
    assert third.is_trial


def test_a_permit_issued_while_closed_is_not_a_trial() -> None:
    """`is_trial` drives the probe accounting, so a normal call being mistaken for a probe would
    decrement a counter that was never incremented and eventually admit unbounded probes."""
    breaker, _ = make()
    with breaker.acquire() as permit:
        assert permit.is_trial is False
        permit.success()


def test_a_failed_probe_reopens_the_circuit_and_restarts_the_cooldown() -> None:
    """One failed probe is enough: a probe asks "is it better yet?", and this one answered no. There
    is no rate to compute and nothing to gain from continuing to probe a dependency that is still
    down."""
    breaker, clock = make(config(open_duration_seconds=30.0))
    enter_half_open(breaker, clock)

    call(breaker, Outcome.FAILURE)

    assert state_of(breaker) is BreakerState.OPEN
    assert breaker.snapshot().retry_after_s == pytest.approx(30.0), "the full cooldown starts again"
    assert breaker.snapshot().trips == 1, (
        "trips counts CLOSED-to-OPEN transitions; re-opening from HALF_OPEN is the same outage "
        "continuing, and counting it again would make one incident look like several"
    )

    clock.advance(30.0)
    assert state_of(breaker) is BreakerState.HALF_OPEN


def test_a_probe_that_is_not_evidence_neither_closes_nor_reopens() -> None:
    """A 404 from a probe is not proof of recovery. The consequence is worth recognising: a
    dependency that only ever answers 404 keeps the circuit half-open indefinitely, with no failures
    recorded. That is correct, and it is a real state to be able to identify in a dashboard."""
    breaker, clock = make(config(half_open_successes_to_close=1))
    enter_half_open(breaker, clock)

    call(breaker, Outcome.NOT_COUNTED)

    snapshot = breaker.snapshot()
    assert snapshot.state is BreakerState.HALF_OPEN
    assert snapshot.trial_successes == 0
    assert snapshot.trials_in_flight == 0, "the slot came back, so probing can continue"


def test_the_circuit_closes_only_after_enough_probe_successes() -> None:
    """More than one success required, because one can be luck: a load balancer with one healthy
    backend in five returns a success 20% of the time, and closing on that sends full production
    traffic at a mostly broken pool."""
    breaker, clock = make(config(half_open_successes_to_close=3))
    enter_half_open(breaker, clock)

    succeed(breaker)
    assert state_of(breaker) is BreakerState.HALF_OPEN
    succeed(breaker)
    assert state_of(breaker) is BreakerState.HALF_OPEN
    succeed(breaker)
    assert state_of(breaker) is BreakerState.CLOSED


def test_probe_successes_do_not_carry_over_from_an_abandoned_round() -> None:
    """Two successes, then a failure that re-opens the circuit, then a fresh round. The new
    round must start from zero — carrying the old successes forward would close the circuit on fewer
    probes than configured, which is precisely the confidence the configuration asked for."""
    breaker, clock = make(config(half_open_successes_to_close=3, half_open_max_concurrent=1))
    enter_half_open(breaker, clock)

    succeed(breaker, times=2)
    assert breaker.snapshot().trial_successes == 2

    call(breaker, Outcome.FAILURE)
    clock.advance(30.0)
    assert state_of(breaker) is BreakerState.HALF_OPEN
    assert breaker.snapshot().trial_successes == 0

    succeed(breaker, times=2)
    assert state_of(breaker) is BreakerState.HALF_OPEN, (
        "two successes is one short of the three required"
    )
    succeed(breaker)
    assert state_of(breaker) is BreakerState.CLOSED


# ---------------------------------------------------------------------------
# Decision 4: the window is reset when the circuit closes
# ---------------------------------------------------------------------------


def test_closing_the_circuit_clears_the_evidence_that_opened_it() -> None:
    """Without this the breaker cannot recover, and the bug is genuinely hard to see because every
    individual transition looks right. The failures that opened the circuit are still in the window,
    so the first failure after recovery finds a rate already over the threshold and re-opens at once
    — a circuit flapping between OPEN and HALF_OPEN forever while the dependency is verifiably fine.

    The window here is deliberately LONGER than the cooldown, and that detail is the whole test.
    With the defaults (a 10 s window, a 30 s cooldown) the failures expire on their own while the
    circuit is open, so the reset has nothing left to clear and removing it changes nothing
    observable — a mutation test proved exactly that by deleting `reset()` and leaving the suite
    green. A 60-second window against a 30-second cooldown is also the more dangerous real
    configuration: a window long enough to outlive the cooldown is what makes a missing reset bite.
    """
    breaker, clock = make(
        config(
            failure_rate_threshold=0.5,
            minimum_throughput=4,
            window_seconds=60.0,
            open_duration_seconds=30.0,
        )
    )
    enter_half_open(breaker, clock)

    succeed(breaker, times=2)
    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.total == 0, "the window that justified the trip is gone"

    # The proof that it matters: one failure after recovery must not be enough to re-open, because
    # the four failures that opened the circuit are no longer part of the evidence.
    fail(breaker)
    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.total == 1


def test_a_recovered_breaker_can_trip_again_on_new_evidence() -> None:
    """The other half of the reset: forgetting the old window must not make the breaker inert. A
    full second cycle, with `trips` counting two distinct outages."""
    breaker, clock = make()
    enter_half_open(breaker, clock)
    succeed(breaker, times=2)
    assert state_of(breaker) is BreakerState.CLOSED

    trip(breaker)
    assert breaker.snapshot().trips == 2


# ---------------------------------------------------------------------------
# Generations: outcomes that arrive after the state they belonged to
# ---------------------------------------------------------------------------


def test_an_outcome_from_before_a_trip_is_discarded_window_and_all() -> None:
    """A call issued while CLOSED can still be in flight when the circuit trips, recovers and closes
    again. Its result describes a state the breaker has left, and — the subtle half — it must not
    reach the window either. The window was deliberately cleared on recovery, so admitting a
    pre-transition result would quietly undo that reset."""
    breaker, clock = make()
    in_flight_since_before_the_outage = breaker.acquire()

    trip(breaker)
    clock.advance(30.0)
    succeed(breaker, times=2)
    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().counts.total == 0

    in_flight_since_before_the_outage.success()

    snapshot = breaker.snapshot()
    assert snapshot.stale_outcomes == 1, (
        "counted for visibility, because silent discards are unlucky"
    )
    assert snapshot.counts.total == 0, "and dropped, window contribution included"


def test_a_late_probe_failure_cannot_reopen_a_recovered_circuit() -> None:
    """The damaging case this mechanism exists for. Two probes go out; the first succeeds and closes
    the circuit; the second fails afterwards. Applying that failure would re-open a circuit that is
    now serving traffic happily, with no current evidence of any problem at all — and the incident
    would be near-impossible to explain from the logs."""
    breaker, clock = make(config(half_open_max_concurrent=2, half_open_successes_to_close=1))
    enter_half_open(breaker, clock)

    first_probe = breaker.acquire()
    second_probe = breaker.acquire()

    first_probe.success()
    assert state_of(breaker) is BreakerState.CLOSED

    second_probe.failure()
    assert state_of(breaker) is BreakerState.CLOSED
    assert breaker.snapshot().stale_outcomes == 1


def test_a_late_probe_does_not_steal_a_slot_from_the_next_round() -> None:
    """Continuing from above: the stale result must not decrement `trials_in_flight`, because the
    transition already reset that counter to zero. Decrementing would drive it negative and, with
    enough late results, let the breaker admit unbounded probes — the cap failing open, which is the
    worst way for it to fail."""
    breaker, clock = make(config(half_open_max_concurrent=2, half_open_successes_to_close=1))
    enter_half_open(breaker, clock)

    first_probe = breaker.acquire()
    second_probe = breaker.acquire()
    first_probe.success()
    second_probe.failure()

    assert breaker.snapshot().trials_in_flight == 0

    # And the cap still holds exactly on the next round: two probes, not three.
    trip(breaker)
    clock.advance(30.0)
    assert state_of(breaker) is BreakerState.HALF_OPEN
    breaker.acquire()
    breaker.acquire()
    with pytest.raises(CircuitOpenError):
        breaker.acquire()


# ---------------------------------------------------------------------------
# Permits: the leak that would silently disable recovery forever
# ---------------------------------------------------------------------------


def test_an_unresolved_permit_is_released_by_the_context_manager() -> None:
    """The leak that matters, and the reason `Permit` is a context manager rather than a convention.
    Leak `half_open_max_concurrent` permits and the breaker rejects every probe for the lifetime of
    the process: the circuit can never close again, with no failures recorded and nothing in the
    logs to point at."""
    breaker, clock = make(config(half_open_max_concurrent=1))
    enter_half_open(breaker, clock)

    with breaker.acquire():
        pass  # a code path that forgot to record anything

    snapshot = breaker.snapshot()
    assert snapshot.trials_in_flight == 0, "the slot came back regardless"
    assert snapshot.unresolved_permits == 1
    assert breaker.acquire().is_trial, "so probing continues and recovery is still possible"


def test_an_exception_releases_the_slot_and_still_propagates() -> None:
    """`__exit__` returns None on purpose. Swallowing the exception would be a resilience layer
    hiding application errors, and deciding whether an exception counts as a dependency failure is
    classification — which belongs to the layer that knows what the exceptions mean (slice 4)."""
    breaker, clock = make(config(half_open_max_concurrent=1))
    enter_half_open(breaker, clock)

    with pytest.raises(RuntimeError, match="dependency exploded"):
        with breaker.acquire():
            raise RuntimeError("dependency exploded")

    snapshot = breaker.snapshot()
    assert snapshot.trials_in_flight == 0
    assert snapshot.unresolved_permits == 1
    assert snapshot.state is BreakerState.HALF_OPEN, (
        "an unresolved permit is NOT_COUNTED, never guessed: guessing FAILURE would let "
        "a bug in our own code trip a breaker on a healthy dependency"
    )


def test_an_unresolved_permit_contributes_no_evidence() -> None:
    breaker, _ = make()
    for _ in range(10):
        with breaker.acquire():
            pass

    snapshot = breaker.snapshot()
    assert snapshot.counts.total == 0
    assert snapshot.unresolved_permits == 10
    assert snapshot.state is BreakerState.CLOSED


def test_an_explicit_outcome_is_not_overwritten_on_exit() -> None:
    """`__exit__` must only act on permits nobody resolved. If it recorded a second outcome for a
    resolved permit, every call in the system would be counted twice."""
    breaker, _ = make()
    with breaker.acquire() as permit:
        permit.failure()

    snapshot = breaker.snapshot()
    assert snapshot.counts.failures == 1
    assert snapshot.counts.total == 1
    assert snapshot.unresolved_permits == 0


def test_resolving_a_permit_twice_raises() -> None:
    """Loudly, because the quiet version distorts the rate the breaker acts on: one call recorded as
    two failures halves the traffic needed to open the circuit."""
    breaker, _ = make()
    permit = breaker.acquire()
    permit.success()

    with pytest.raises(RuntimeError, match="already been resolved"):
        permit.failure()

    assert breaker.snapshot().counts.total == 1


# ---------------------------------------------------------------------------
# Time-driven transitions happen whether or not anyone is watching
# ---------------------------------------------------------------------------


def test_the_cooldown_elapses_without_anyone_reading_the_state() -> None:
    """Reading `state` applies a due transition, which looks like a property with a side effect.
    This is the test that shows the state is *observed* rather than caused: nothing reads the state
    between the trip and the acquire, and the probe is admitted anyway."""
    breaker, clock = make(config(open_duration_seconds=30.0))
    fail(breaker, times=4)

    clock.advance(30.0)

    permit = breaker.acquire()
    assert permit.is_trial, "the cooldown had elapsed whether or not anyone looked"


def test_polling_the_state_does_not_extend_the_cooldown() -> None:
    """The inverse worry: if reading the state restarted or advanced anything, a busy metrics scrape
    would change the breaker's behaviour, and the system would behave differently when observed."""
    breaker, clock = make(config(open_duration_seconds=30.0))
    trip(breaker)

    for _ in range(1000):
        assert state_of(breaker) is BreakerState.OPEN
        breaker.snapshot()

    clock.advance(30.0)
    assert state_of(breaker) is BreakerState.HALF_OPEN


# ---------------------------------------------------------------------------
# Configuration: a misconfigured breaker is worse than no breaker
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: config(failure_rate_threshold=0.0), "failure_rate_threshold must be in"),
        (lambda: config(failure_rate_threshold=-0.1), "failure_rate_threshold must be in"),
        (lambda: config(failure_rate_threshold=1.5), "failure_rate_threshold must be in"),
        (lambda: config(minimum_throughput=0), "minimum_throughput must be at least 1"),
        (lambda: config(window_seconds=0.0), "window_seconds must be positive"),
        (lambda: config(open_duration_seconds=0.0), "open_duration_seconds must be positive"),
        (lambda: config(half_open_max_concurrent=0), "half_open_max_concurrent must be at least 1"),
        (
            lambda: config(half_open_successes_to_close=0),
            "half_open_successes_to_close must be at least 1",
        ),
        (lambda: config(bucket_count=0), "bucket_count must be at least 1"),
    ],
)
def test_invalid_configuration_is_rejected_at_construction(
    build: Callable[[], BreakerConfig], message: str
) -> None:
    """Each of these would produce a breaker that looks installed and does not work, which is worse
    than having none because it is believed in.

    The two most dangerous, spelled out: a `failure_rate_threshold` of 0.0 opens on any traffic at
    all, because an empty window reports a rate of 0.0 and 0.0 >= 0.0. A
    `half_open_max_concurrent` of 0 admits no probes, so the circuit can never leave HALF_OPEN —
    permanently open, with no error and nothing in the logs.
    """
    with pytest.raises(ValueError, match=message):
        build()


def test_bucket_count_is_the_only_field_with_a_default() -> None:
    """Every threshold is a policy decision about one specific dependency, and a library default is
    how a whole fleet inherits a number nobody chose. `bucket_count` is different in kind: it is an
    internal accuracy-versus-memory trade inside the window, not a claim about the dependency."""
    cfg = config()
    assert cfg.bucket_count == 10

    with pytest.raises(TypeError):
        BreakerConfig()  # type: ignore[call-arg]


def test_configuration_cannot_drift_at_runtime() -> None:
    """Frozen, so a breaker's policy is whatever it was constructed with. A mutable config invites
    "temporarily" raising a threshold during an incident and leaving it raised."""
    cfg = config()
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.failure_rate_threshold = 0.9  # type: ignore[misc]


def test_a_threshold_of_one_and_a_throughput_of_one_are_both_legal() -> None:
    """The permissive edges of the validation. A minimum throughput of 1 is a real choice for a
    dependency called rarely enough that waiting for a fourth call would mean never deciding."""
    breaker, _ = make(config(failure_rate_threshold=1.0, minimum_throughput=1))
    fail(breaker)
    assert state_of(breaker) is BreakerState.OPEN
