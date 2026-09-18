"""Tests for status-code classification.

These are cheap tests of an important table, and the reason there are so many of them is that every
row is a decision somebody could reasonably disagree with. A parametrised sweep of "4xx is not
counted, 5xx is a failure" would pass against an implementation that had lost both exceptions, which
are the only interesting content in the module — so the exceptions get named tests and the ranges
get boundary tests, rather than one loop over a hundred numbers.
"""

from __future__ import annotations

import pytest

from edge_gateway.resilience import Outcome, classify_status


@pytest.mark.parametrize("status", [100, 200, 201, 204, 301, 302, 304, 399])
def test_anything_below_400_is_a_success(status: int) -> None:
    """Including 3xx, deliberately.

    A redirect is a correct, cheap, fast answer. Whether we follow it is our decision, and the call
    that follows it gets classified on its own merits. Recording the redirect as a failure would
    open a circuit on a dependency behaving exactly as designed.
    """
    assert classify_status(status) is Outcome.SUCCESS


@pytest.mark.parametrize("status", [400, 401, 403, 404, 405, 409, 418, 422, 499])
def test_client_errors_are_not_evidence(status: int) -> None:
    """The rule that stops a caller's bug from taking down a healthy dependency for everyone else.

    404 is the clearest case: a successful lookup that found nothing. The dependency did its job,
    did it quickly, and did it cheaply. A deploy that starts requesting a renamed path would
    otherwise trip the breaker within seconds.
    """
    assert classify_status(status) is Outcome.NOT_COUNTED


@pytest.mark.parametrize("status", [500, 502, 503, 504, 599])
def test_server_errors_are_failures(status: int) -> None:
    """503 and 504 are included on purpose and are NOT special-cased anywhere.

    503 is the clearest possible statement of ill health. 504 means something behind our dependency
    was too slow — which still leaves our dependency unable to serve us, and from here the
    difference is invisible and not actionable.
    """
    assert classify_status(status) is Outcome.FAILURE


# ---------------------------------------------------------------------------
# The exceptions, which are the whole point of the module
# ---------------------------------------------------------------------------


def test_429_is_a_failure_despite_being_a_4xx() -> None:
    """The dependency is explicitly telling us it cannot serve us at this rate.

    Filed under "not our problem" — the naive 4xx rule — this is how a caller keeps pushing a
    service that is already asking for relief, which is the retry-storm behaviour a breaker exists
    to stop.
    """
    assert classify_status(429) is Outcome.FAILURE


def test_408_is_a_failure_despite_being_a_4xx() -> None:
    """A dependency that gave up waiting for our request is either too slow to read or overloaded.

    Either way a healthy, fast service does not emit these, so it is evidence.
    """
    assert classify_status(408) is Outcome.FAILURE


@pytest.mark.parametrize("status", [501, 505])
def test_permanent_protocol_errors_are_not_evidence_despite_being_5xx(status: int) -> None:
    """501 and 505 are 5xx codes that say nothing about health.

    Both are permanent, deterministic answers about what we asked for: they cost the dependency
    nothing, they will never succeed however long we wait, and no amount of shed traffic changes
    them. Functionally they are a 405 that was given the wrong number. Counting them means a routing
    or protocol mistake on our side opens a circuit on a service that is perfectly healthy — and
    since the mistake is permanent, the circuit would re-open on the first probe, forever.
    """
    assert classify_status(status) is Outcome.NOT_COUNTED


def test_500_is_still_a_failure_next_door_to_501() -> None:
    """Pins the 501 exception as an exception rather than as a shifted boundary.

    Without this, an implementation that treated `500 <= status <= 501` as not-counted would pass
    every other test in this file while silently ignoring the single most common failure code there
    is.
    """
    assert classify_status(500) is Outcome.FAILURE
    assert classify_status(502) is Outcome.FAILURE


# ---------------------------------------------------------------------------
# Boundaries and nonsense
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (399, Outcome.SUCCESS),
        (400, Outcome.NOT_COUNTED),
        (499, Outcome.NOT_COUNTED),
        (500, Outcome.FAILURE),
    ],
)
def test_the_range_boundaries_fall_on_the_right_side(status: int, expected: Outcome) -> None:
    """Off-by-one on either boundary is a plausible defect with a large blast radius.

    Shifting the 400 boundary by one makes every 400 Bad Request evidence against the dependency;
    shifting the 500 boundary makes every 500 free. Both are one character.
    """
    assert classify_status(status) is expected


@pytest.mark.parametrize("status", [0, 99, 600, 999, -1])
def test_an_impossible_status_is_a_failure_rather_than_an_exception(status: int) -> None:
    """A status outside 100-599 means something in the path is broken or is not speaking HTTP.

    That is exactly the condition worth shedding traffic over, so FAILURE is the honest reading.
    Raising instead would convert a misbehaving upstream into a crash inside the gateway — replacing
    their outage with ours, which is the precise failure this whole project exists to prevent.
    """
    assert classify_status(status) is Outcome.FAILURE


def test_classification_is_exhaustive_over_every_possible_status() -> None:
    """No input may fall through without a verdict.

    Cheap to assert and worth asserting: a table of ranges is exactly the shape of code where a gap
    is invisible on the page, and a gap here would mean `None` reaching `permit.resolve` and blowing
    up inside the resilience layer at the worst possible moment.
    """
    for status in range(-50, 700):
        assert classify_status(status) in set(Outcome)
