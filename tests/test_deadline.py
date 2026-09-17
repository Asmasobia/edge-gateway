"""Tests for the timeout-budget primitive.

Weighted heavily toward the cases that go WRONG, on the reasoning that a deadline test proving only
"a 1000 ms budget reports about 1000 ms left" is compatible with an implementation that ignores the
header entirely and always grants the default. The interesting behaviour of this module is all at
the boundaries: expiry at exactly zero, a reserve larger than what remains, a hostile header value,
and a budget of zero meaning "already too late" rather than "unset".

Not one test sleeps. Every temporal assertion is driven by `ManualClock.advance`, which is the whole
reason the clock is injected — see clock.py.
"""

from __future__ import annotations

import pytest

from edge_gateway.resilience import (
    TIMEOUT_BUDGET_HEADER,
    BudgetSource,
    Deadline,
    ManualClock,
    MonotonicClock,
    deadline_from_headers,
    headers_for_downstream,
)

# ---------------------------------------------------------------------------
# The clock itself
# ---------------------------------------------------------------------------


def test_monotonic_clock_does_not_go_backwards() -> None:
    """Weak by necessity, but not worthless: it pins the type contract (a float) and would catch
    someone swapping in `time.time()`, which CAN go backwards. The strong guarantee is provided by
    the platform, not by this project, so this asserts only what is ours to break."""
    clock = MonotonicClock()
    readings = [clock.now() for _ in range(100)]
    assert readings == sorted(readings)
    assert all(isinstance(r, float) for r in readings)


def test_manual_clock_starts_at_zero_and_only_moves_when_told() -> None:
    clock = ManualClock()
    assert clock.now() == 0.0
    assert clock.now() == 0.0  # no implicit passage of time between reads

    clock.advance(1.5)
    assert clock.now() == 1.5
    clock.advance_ms(250)
    assert clock.now() == 1.75


def test_manual_clock_refuses_to_rewind() -> None:
    """A monotonic clock cannot move backwards, so a test that rewinds one would be specifying
    behaviour production can never produce."""
    clock = ManualClock()
    with pytest.raises(ValueError, match="cannot move backwards"):
        clock.advance(-0.001)


# ---------------------------------------------------------------------------
# Deadline arithmetic
# ---------------------------------------------------------------------------


def test_budget_is_spent_down_as_time_passes() -> None:
    clock = ManualClock()
    deadline = Deadline.after_ms(1000, clock=clock, source=BudgetSource.INHERITED)

    assert deadline.remaining_ms() == 1000
    clock.advance_ms(400)
    assert deadline.remaining_ms() == 600
    clock.advance_ms(600)
    assert deadline.remaining_ms() == 0


def test_expiry_boundary_fails_closed_at_exactly_zero() -> None:
    """At exactly zero there is no time in which to do anything, so the boundary must be `<= 0`.
    `< 0` would let a request through with a budget of nothing, and the resulting downstream call
    would be guaranteed to be wasted work."""
    clock = ManualClock()
    deadline = Deadline.after_ms(100, clock=clock, source=BudgetSource.INHERITED)

    clock.advance_ms(99)
    assert not deadline.expired()

    clock.advance_ms(1)  # now exactly at the instant of expiry
    assert deadline.remaining_ms() == 0
    assert deadline.expired()


def test_remaining_goes_negative_and_is_not_clamped() -> None:
    """How far past the deadline we are is diagnostic information ("we overran by 250 ms"), so it is
    reported rather than flattened to zero. Clamping here would also make `expired()` unable to
    distinguish 'just now' from 'ages ago'."""
    clock = ManualClock()
    deadline = Deadline.after_ms(100, clock=clock, source=BudgetSource.INHERITED)

    clock.advance_ms(350)
    assert deadline.remaining_ms() == -250
    assert deadline.remaining_s() == pytest.approx(-0.25)


def test_remaining_ms_truncates_rather_than_rounds() -> None:
    """Truncation toward zero, so the number can never over-state the time available.

    With 0.6 ms left, rounding would report 1 ms and hand a downstream hop a budget that has already
    elapsed — converting a free local refusal into a remote timeout that costs a connection and is
    then blamed on the dependency.
    """
    clock = ManualClock()
    deadline = Deadline.after_ms(1, clock=clock, source=BudgetSource.INHERITED)

    clock.advance(0.0004)  # 0.6 ms remain
    assert deadline.remaining_s() == pytest.approx(0.0006)
    assert deadline.remaining_ms() == 0


def test_no_millisecond_is_lost_to_float_representation_error() -> None:
    """Regression test for a bug this module actually had.

    Deadlines are float seconds, so `expires_at - now` is inexact: with a deadline at 0.98 s and the
    clock at 0.06 s, the subtraction gives 0.9199999999999999, and `int(x * 1000)` floored that to
    919 — losing a millisecond from a budget that genuinely had 920.

    The error is ~1e-16 s and physically meaningless; the damage was done by FLOORING, which turned
    it into a full millisecond, in the same direction, at every hop. This pins the exact arithmetic
    rather than a rounded approximation of it, because a test written with tidy values (0.5, 0.25)
    would not have caught it — those are exactly representable in binary.
    """
    clock = ManualClock()
    deadline = Deadline.after_ms(950, clock=clock, source=BudgetSource.INHERITED)
    assert deadline.expires_at == pytest.approx(0.95)

    clock.advance_ms(30)
    # The raw subtraction is inexact; the reported millisecond count must not be.
    assert deadline.remaining_s() != 0.92
    assert deadline.remaining_ms() == 920


def test_a_zero_budget_is_already_expired() -> None:
    """An upstream saying "you have no time left" is legitimate and useful — it means the work is
    already pointless — and the right response is to refuse immediately, not to treat the value as
    invalid and grant a fresh default, which would lengthen a budget the caller deliberately set to
    nothing."""
    clock = ManualClock()
    deadline = Deadline.after_ms(0, clock=clock, source=BudgetSource.INHERITED)
    assert deadline.expired()
    assert deadline.remaining_ms() == 0


def test_deadline_is_immutable() -> None:
    """Frozen so that no handler can quietly buy itself more time; extending a deadline in place
    would defeat the guarantee the whole module exists to provide."""
    deadline = Deadline.after_ms(100, clock=ManualClock(), source=BudgetSource.INHERITED)
    with pytest.raises((AttributeError, TypeError)):
        deadline.expires_at = 999.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Handing budget to the next hop
# ---------------------------------------------------------------------------


def test_downstream_gets_less_than_we_have() -> None:
    """The reserve is what pays for this hop's own remaining work — deserialising the response,
    doing something with it, serialising ours. Passing on 100% means downstream can succeed on the
    final millisecond and this hop still blows the client's deadline while formatting the reply."""
    clock = ManualClock()
    deadline = Deadline.after_ms(1000, clock=clock, source=BudgetSource.INHERITED)

    assert deadline.downstream_budget_ms(reserve_ms=60) == 940
    clock.advance_ms(500)
    assert deadline.downstream_budget_ms(reserve_ms=60) == 440


def test_downstream_budget_floors_at_zero_and_never_goes_negative() -> None:
    """A negative budget on the wire is meaningless. Whether the call is worth making is
    `is_viable`'s job, not this method's."""
    clock = ManualClock()
    deadline = Deadline.after_ms(50, clock=clock, source=BudgetSource.INHERITED)

    assert deadline.downstream_budget_ms(reserve_ms=200) == 0

    clock.advance_ms(500)  # well past expiry
    assert deadline.downstream_budget_ms(reserve_ms=20) == 0


def test_negative_reserve_is_rejected() -> None:
    """A negative reserve would ADD time on the way down — manufacturing budget out of nothing and
    breaking the invariant that a deadline can only shrink as it propagates. Almost certainly a sign
    error at a call site, so it should be loud."""
    deadline = Deadline.after_ms(100, clock=ManualClock(), source=BudgetSource.INHERITED)
    with pytest.raises(ValueError, match="reserve_ms"):
        deadline.downstream_budget_ms(reserve_ms=-1)


def test_viability_is_judged_after_the_reserve_is_taken() -> None:
    """The subtle part: viability must be assessed on what downstream would ACTUALLY receive, not on
    raw remaining time. 120 ms left with a 100 ms reserve leaves 20 ms — not viable for a 50 ms
    minimum, even though 120 > 50."""
    clock = ManualClock()
    deadline = Deadline.after_ms(120, clock=clock, source=BudgetSource.INHERITED)

    assert deadline.remaining_ms() == 120
    assert not deadline.is_viable(minimum_ms=50, reserve_ms=100)
    assert deadline.is_viable(minimum_ms=50, reserve_ms=60)


def test_viability_boundary_is_inclusive() -> None:
    """Exactly the minimum is enough. An off-by-one to `>` here would reject calls that had
    precisely the budget they were declared to need."""
    clock = ManualClock()
    deadline = Deadline.after_ms(100, clock=clock, source=BudgetSource.INHERITED)
    assert deadline.is_viable(minimum_ms=100, reserve_ms=0)
    assert not deadline.is_viable(minimum_ms=101, reserve_ms=0)


def test_expired_deadline_is_never_viable() -> None:
    clock = ManualClock()
    deadline = Deadline.after_ms(100, clock=clock, source=BudgetSource.INHERITED)
    clock.advance_ms(101)
    assert not deadline.is_viable(minimum_ms=1, reserve_ms=0)


def test_headers_for_downstream_emits_the_reduced_budget() -> None:
    clock = ManualClock()
    deadline = Deadline.after_ms(1000, clock=clock, source=BudgetSource.INHERITED)
    clock.advance_ms(200)

    assert headers_for_downstream(deadline, reserve_ms=50) == {TIMEOUT_BUDGET_HEADER: "750"}


def test_headers_for_downstream_emits_zero_rather_than_omitting_the_header() -> None:
    """Omitting it would be read downstream as DEFAULTED, and the callee would grant itself a fresh
    full budget — silently losing the deadline at the exact moment it matters most. Emitting `0`
    keeps the chain honest and lets the callee refuse."""
    clock = ManualClock()
    deadline = Deadline.after_ms(10, clock=clock, source=BudgetSource.INHERITED)
    clock.advance_ms(500)

    assert headers_for_downstream(deadline, reserve_ms=5) == {TIMEOUT_BUDGET_HEADER: "0"}


def test_headers_for_downstream_returns_a_fresh_dict_each_call() -> None:
    """Guards against a shared/mutated header container leaking one request's budget into another —
    a bug that shows up as 'some requests randomly get a tiny budget' and is near-untraceable."""
    deadline = Deadline.after_ms(100, clock=ManualClock(), source=BudgetSource.INHERITED)
    first = headers_for_downstream(deadline, reserve_ms=0)
    second = headers_for_downstream(deadline, reserve_ms=0)

    assert first == second
    assert first is not second
    first[TIMEOUT_BUDGET_HEADER] = "tampered"
    assert second[TIMEOUT_BUDGET_HEADER] == "100"


# ---------------------------------------------------------------------------
# Parsing the incoming header — the trust boundary
# ---------------------------------------------------------------------------


def test_absent_header_is_defaulted_and_says_so() -> None:
    """DEFAULTED must be distinguishable from INHERITED. If it were not, internal traffic that has
    silently lost its budget header would look identical to traffic that inherited one, and broken
    propagation would be invisible — the scheme would quietly not be working."""
    deadline = deadline_from_headers({}, clock=ManualClock(), default_ms=2000, max_ms=30_000)

    assert deadline.source is BudgetSource.DEFAULTED
    assert deadline.remaining_ms() == 2000
    assert deadline.granted_ms == 2000


def test_valid_header_is_inherited() -> None:
    deadline = deadline_from_headers(
        {TIMEOUT_BUDGET_HEADER: "750"}, clock=ManualClock(), default_ms=2000, max_ms=30_000
    )
    assert deadline.source is BudgetSource.INHERITED
    assert deadline.remaining_ms() == 750


def test_header_lookup_is_case_insensitive() -> None:
    """HTTP/1.1 field names are case-insensitive, so `X-Timeout-Budget-Ms` is the same header.
    Relying on the caller to pass a case-insensitive mapping holds right up until somebody passes
    a plain dict, at which point propagation silently stops working for one client."""
    for spelling in ("X-Timeout-Budget-Ms", "X-TIMEOUT-BUDGET-MS", "x-Timeout-Budget-ms"):
        deadline = deadline_from_headers(
            {spelling: "300"}, clock=ManualClock(), default_ms=2000, max_ms=30_000
        )
        assert deadline.source is BudgetSource.INHERITED, spelling
        assert deadline.remaining_ms() == 300, spelling


def test_oversized_budget_is_clamped_to_the_ceiling() -> None:
    """`max_ms` is a trust boundary, not a tuning knob. Without it, `x-timeout-budget-ms: 86400000`
    pins a worker, a connection and a bulkhead slot for a day at no cost to the sender."""
    deadline = deadline_from_headers(
        {TIMEOUT_BUDGET_HEADER: "86400000"}, clock=ManualClock(), default_ms=2000, max_ms=30_000
    )
    assert deadline.source is BudgetSource.CLAMPED
    assert deadline.remaining_ms() == 30_000


def test_budget_exactly_at_the_ceiling_is_inherited_not_clamped() -> None:
    """The ceiling is a permitted value. Reporting it as CLAMPED would make the metric fire
    constantly for well-behaved callers, which is how a useful signal gets muted."""
    deadline = deadline_from_headers(
        {TIMEOUT_BUDGET_HEADER: "30000"}, clock=ManualClock(), default_ms=2000, max_ms=30_000
    )
    assert deadline.source is BudgetSource.INHERITED
    assert deadline.remaining_ms() == 30_000


def test_zero_budget_on_the_wire_is_honoured_as_already_expired() -> None:
    deadline = deadline_from_headers(
        {TIMEOUT_BUDGET_HEADER: "0"}, clock=ManualClock(), default_ms=2000, max_ms=30_000
    )
    assert deadline.source is BudgetSource.INHERITED
    assert deadline.expired()


@pytest.mark.parametrize(
    ("raw", "why"),
    [
        ("", "empty value"),
        ("   ", "whitespace only"),
        (" 100 ", "surrounding whitespace — int() would accept this"),
        ("+100", "leading sign — int() would accept this"),
        ("-5", "negative; there is no such thing as negative remaining time"),
        ("100_000", "PEP 515 underscores — int() would accept this as 100000"),
        ("١٢٣", "Arabic-Indic digits — int() would accept this as 123"),
        ("१२", "Devanagari digits — int() would accept this as 12"),
        ("1e3", "scientific notation"),
        ("0x10", "hex"),
        ("10.5", "fractional milliseconds"),
        ("100ms", "value with a unit suffix"),
        ("NaN", "not a number"),
        ("Infinity", "unbounded budget by another name"),
    ],
)
def test_malformed_values_fall_back_to_the_default_and_are_flagged(raw: str, why: str) -> None:
    """Falling back rather than returning 400: the budget is an optimisation hint, and failing a
    request the service could otherwise serve turns one caller's header bug into a user-visible
    outage. It is reported through `source` so it is loud in metrics and harmless in production.

    Four of these cases (`100_000`, the two non-ASCII digit sets, and the whitespace/sign forms)
    exist specifically because Python's `int()` accepts them. Parsing with `int()` would give this
    wire format a grammar nobody documented — including, verifiably, Arabic-Indic numerals.
    """
    deadline = deadline_from_headers(
        {TIMEOUT_BUDGET_HEADER: raw}, clock=ManualClock(), default_ms=2000, max_ms=30_000
    )
    assert deadline.source is BudgetSource.MALFORMED, why
    assert deadline.remaining_ms() == 2000, why


# ---------------------------------------------------------------------------
# Configuration validation — fail at startup, not per request
# ---------------------------------------------------------------------------


def test_non_positive_default_is_rejected() -> None:
    with pytest.raises(ValueError, match="default_ms"):
        deadline_from_headers({}, clock=ManualClock(), default_ms=0, max_ms=30_000)


def test_ceiling_below_the_default_is_rejected() -> None:
    """Such a configuration would clamp every request including those with no header at all, and the
    symptom would be 'everything times out' with no obvious cause."""
    with pytest.raises(ValueError, match="max_ms"):
        deadline_from_headers({}, clock=ManualClock(), default_ms=5000, max_ms=1000)


# ---------------------------------------------------------------------------
# The property the whole module exists for
# ---------------------------------------------------------------------------


def test_budget_cannot_grow_across_a_chain_of_hops() -> None:
    """The end-to-end invariant, and the thing a per-call timeout gets wrong.

    Four hops each "configured for 1 second" would allow 4 seconds of total work. Here, the client's
    1000 ms is decided once and only shrinks — even though every hop is given the same reserve and
    each one burns real time doing its own work.
    """
    clock = ManualClock()
    hop = deadline_from_headers(
        {TIMEOUT_BUDGET_HEADER: "1000"}, clock=clock, default_ms=2000, max_ms=30_000
    )

    budgets = [hop.remaining_ms()]
    for _ in range(3):
        clock.advance_ms(30)  # this hop does some work before calling on
        outgoing = headers_for_downstream(hop, reserve_ms=20)
        hop = deadline_from_headers(outgoing, clock=clock, default_ms=2000, max_ms=30_000)
        assert hop.source is BudgetSource.INHERITED
        budgets.append(hop.remaining_ms())

    assert budgets == sorted(budgets, reverse=True), budgets
    assert budgets == [1000, 950, 900, 850]
    assert budgets[-1] < 1000
