"""Tests for the exception-to-status-code table.

WHAT IS ACTUALLY WORTH TESTING IN A LOOKUP TABLE
--------------------------------------------------------------------------------
A table of `isinstance` checks looks too simple to test, and two of its properties are not simple at
all:

  * **The order of the checks.** `httpx`'s exceptions form a hierarchy, and one pair below is
    nested: `TimeoutException` is a `TransportError`. Put the general case first and every timeout
    silently becomes a 502, which is the difference between "the dependency is slow" and "the
    dependency is unreachable" — the two most different diagnoses in the file. Nothing about the
    code *looks* wrong when that happens, so the ordering is asserted directly.
  * **The fall-through.** `refusal_for` returning None for an unrecognised exception is what keeps a
    `KeyError` in our own routing from being dressed up as a dependency failure. It is one `return
    None` and it is the single most valuable line here, so it gets its own test.

The status codes themselves are asserted too, but as a record of a decision rather than as
verification of logic: if someone changes the full-bulkhead refusal from 503 to 429, the failing
test name and the docstring on it are the argument they need to read before doing it.
"""

from __future__ import annotations

import httpx
import pytest

from edge_gateway.gateway import Refusal, refusal_for
from edge_gateway.gateway.errors import REFUSAL_HEADER
from edge_gateway.resilience import (
    BreakerState,
    BudgetExhaustedError,
    BulkheadFullError,
    CircuitOpenError,
    SlotWaitExpiredError,
    UpstreamTimeoutError,
)


def refusal(exc: BaseException) -> Refusal:
    """`refusal_for` with the None case turned into a clear failure.

    Every test below except the fall-through one expects a mapping to exist. Asserting that here
    keeps each test's own assertions about the *contents*, and turns a missing branch into "expected
    a refusal, got None" rather than an `AttributeError` on None three lines later.
    """
    mapped = refusal_for(exc)
    assert mapped is not None, f"{type(exc).__name__} was not recognised as a refusal"
    return mapped


def a_request() -> httpx.Request:
    """A throwaway request, because several `httpx` exceptions require one.

    The URL is irrelevant to every assertion here; it exists only because the constructors take it.
    """
    return httpx.Request("GET", "http://search.internal/products")


# ---------------------------------------------------------------------------
# The resilience refusals
# ---------------------------------------------------------------------------


def test_a_budget_too_small_on_arrival_is_a_504() -> None:
    """504 even though no call was made, because the client's question is "why no response".

    The answer is "the deadline you set had already passed", which is what Gateway Timeout means.
    503 would say "come back later" and a client obeying it retries with the same too-small budget
    and fails identically — a retry loop that can never succeed.
    """
    mapped = refusal(BudgetExhaustedError("search", remaining_ms=12, at_least_ms=50, queued=False))

    assert mapped.status == 504
    assert mapped.reason == "budget-exhausted-on-arrival"


def test_a_budget_exhausted_while_queued_is_a_different_reason_on_the_same_status() -> None:
    """The status is identical and the reason is not, which is the whole argument for the header.

    "The caller's budget was too small" and "our own queue ate the caller's budget" are the same 504
    to the client and two different fixes for us: a conversation with the caller, or a smaller
    `max_queue`. One label distinguishes them in a dashboard; the status code cannot.
    """
    mapped = refusal(BudgetExhaustedError("search", remaining_ms=3, at_least_ms=50, queued=True))

    assert mapped.status == 504
    assert mapped.reason == "budget-exhausted-while-queued"


def test_an_open_circuit_is_a_503_carrying_when_to_come_back() -> None:
    """The only refusal that can honestly state a retry time, because the breaker schedules its own.

    `retry_after_s` comes from the breaker's cooldown, not from a guess, which is what makes it safe
    to print. A client will trust the number.
    """
    mapped = refusal(CircuitOpenError("search", BreakerState.OPEN, 30.0))

    assert mapped.status == 503
    assert mapped.reason == "circuit-open"
    assert mapped.headers()["retry-after"] == "30"


def test_a_half_open_circuit_refuses_without_inventing_a_retry_time() -> None:
    """None must survive as None rather than being filled in with something plausible.

    In HALF_OPEN the trial slots are momentarily occupied and a slot may free in a microsecond or a
    minute. There is no honest number, so there is no header — and the absence is meaningful,
    because a client with no `Retry-After` applies its own backoff instead of synchronising with
    every other refused caller onto one instant.
    """
    mapped = refusal(CircuitOpenError("search", BreakerState.HALF_OPEN, None))

    assert mapped.status == 503
    assert mapped.reason == "circuit-open"
    assert "retry-after" not in mapped.headers()


def test_a_full_bulkhead_is_a_503_and_never_a_429() -> None:
    """429 would blame a client that sent one request. Read the module docstring before changing.

    Too Many Requests says *you* sent too many. A caller refused because two hundred other callers
    are mid-flight to the same dependency has been told something false, and it will act on it:
    well-written clients back off harder on 429 than on 503. The overload is ours. 429 would be
    correct if this gateway did per-client rate limiting, which it does not.
    """
    mapped = refusal(BulkheadFullError("search", in_flight=8, queued=4, queue_capacity=4))

    assert mapped.status == 503
    assert mapped.reason == "bulkhead-full"


def test_a_full_bulkhead_carries_no_retry_after() -> None:
    """Saturation has no scheduled end, so any number here would be fiction.

    It clears when in-flight calls finish, which is a latency distribution rather than a deadline. A
    made-up value would also synchronise every refused client onto the same instant — a thundering
    herd we would have caused ourselves, at the exact moment we were already out of capacity.
    """
    mapped = refusal(BulkheadFullError("search", in_flight=8, queued=4, queue_capacity=4))

    assert "retry-after" not in mapped.headers()


def test_an_expired_slot_wait_is_a_504_distinct_from_a_full_queue() -> None:
    """Two saturation refusals that move `max_queue` in opposite directions.

    A full queue says the queue is too shallow for the burst. An expired wait says it is too *deep*
    for the budget — callers are admitted and then time out before reaching the front, so the queue
    is adding latency to calls that get refused anyway. One counter covering both hides which way to
    move the number.
    """
    mapped = refusal(SlotWaitExpiredError("search", waited_ms=180))

    assert mapped.status == 504
    assert mapped.reason == "slot-wait-expired"


def test_a_dependency_that_did_not_answer_is_a_504() -> None:
    """The one refusal here that is genuinely about the dependency rather than about us.

    Distinct from `slot-wait-expired` for that reason: this one was recorded against the breaker as
    a failure and will contribute to opening the circuit, while the queue refusals deliberately were
    not. Same status code, and the reason header is what tells an operator whether to go and look at
    the dependency or at our own capacity.
    """
    mapped = refusal(UpstreamTimeoutError("search", waited_ms=940))

    assert mapped.status == 504
    assert mapped.reason == "upstream-timeout"


# ---------------------------------------------------------------------------
# The httpx branches, including the one whose order matters
# ---------------------------------------------------------------------------


def test_a_timeout_from_httpx_is_matched_before_the_general_transport_error() -> None:
    """The ordering test: `httpx.ConnectTimeout` is BOTH a timeout and a `TransportError`.

    Reorder the two `isinstance` checks in `errors.py` and this is the only test that fails: the
    request still gets a refusal, still gets a 5xx, and still looks entirely reasonable in a log —
    it just gets 502 "could not reach it" for something that was reached and did not answer. That
    mislabelling sends an investigation to the network team instead of to the dependency's owners.
    """
    mapped = refusal(httpx.ConnectTimeout("timed out", request=a_request()))

    assert mapped.status == 504
    assert mapped.reason == "client-timeout"


def test_a_transport_failure_is_a_502() -> None:
    """Connection refused, DNS failure, TLS handshake failure, reset mid-body.

    502 Bad Gateway is literally this situation: we are a gateway and we could not obtain a valid
    response from upstream. Kept distinct from 504 because "nothing is listening" is a deployment or
    service-discovery problem and "it did not answer in time" is a capacity problem, and those go to
    different people.
    """
    mapped = refusal(httpx.ConnectError("connection refused", request=a_request()))

    assert mapped.status == 502
    assert mapped.reason == "transport-error"


def test_a_url_that_cannot_be_built_is_the_clients_fault_and_a_400() -> None:
    """Because the path and query in it are the client's input, forwarded deliberately unnormalised.

    `Upstream._target` re-attaches the raw query string rather than parsing and re-encoding it, so a
    URL httpx refuses to construct is far more likely to be a request we could never have served
    than a mistake in our own string handling. `InvalidURL` is also not an `HTTPError` in httpx's
    tree, so without this branch it would fall through to a 500 and blame us in the wrong dashboard.
    """
    mapped = refusal(httpx.InvalidURL("invalid target"))

    assert mapped.status == 400
    assert mapped.reason == "invalid-target"


def test_the_remaining_httpx_error_tree_degrades_to_a_labelled_502() -> None:
    """A defensive net with no known trigger today, tested so that it is a net and not a hole.

    The only `HTTPError` that is neither a transport error nor a timeout is `HTTPStatusError`,
    raised by `raise_for_status()`, which a proxy must never call — forwarding a 500 is the job. The
    branch exists so that "httpx grew a new error class" degrades to a 502 with its own label rather
    than to a 500 that reads as our own bug and pages somebody.
    """
    mapped = refusal(
        httpx.HTTPStatusError(
            "500 from upstream",
            request=a_request(),
            response=httpx.Response(500),
        )
    )

    assert mapped.status == 502
    assert mapped.reason == "upstream-protocol-error"


# ---------------------------------------------------------------------------
# The fall-through, and what the client is allowed to see
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        KeyError("upstream_name"),
        ValueError("bad config"),
        TypeError("wrong argument"),
        RuntimeError("something else entirely"),
        ZeroDivisionError("division by zero"),
    ],
    ids=["KeyError", "ValueError", "TypeError", "RuntimeError", "ZeroDivisionError"],
)
def test_an_unrecognised_exception_is_not_dressed_up_as_a_refusal(exc: BaseException) -> None:
    """The most valuable line in `errors.py`: `return None`, so real bugs stay visible.

    A tidy 502 for everything unrecognised is tempting and would be actively harmful. The `KeyError`
    in this list is the realistic one — a routing mistake in our own code — and reported as a 502 it
    would send every investigation to the dependency and keep it there, because a 502 from a gateway
    means "upstream is broken" to everyone who reads one. None means the handler re-raises, the
    framework returns 500, and a traceback naming our own file lands in the log.
    """
    assert refusal_for(exc) is None


def test_the_exception_message_never_reaches_the_client() -> None:
    """The body carries the slug only. The detail is ours, and it describes our capacity.

    `BulkheadFullError`'s message contains in-flight counts, queue depth and queue capacity. Handed
    to an untrusted caller that is a free readout of how much load we can absorb and how close to
    the limit we currently are — precisely the reconnaissance a load-based attack wants, and it
    turns the attack from guesswork into a measurement. It belongs in the log, where it is genuinely
    the first thing anyone wants, and nowhere else.
    """
    exc = BulkheadFullError("search", in_flight=8, queued=4, queue_capacity=4)
    mapped = refusal(exc)

    assert mapped.body() == {"error": "bulkhead-full"}
    assert mapped.detail == str(exc)
    assert "8" not in str(mapped.body())


def test_every_refusal_names_itself_in_the_reason_header() -> None:
    """The header is why five refusals can share two status codes without losing meaning.

    Asserted once, generically, because it is a property of the type rather than of any one branch:
    a refusal without a reason header is a 503 in a dashboard with nothing to break it down by,
    which is the state this table was written to avoid.
    """
    for exc in (
        BudgetExhaustedError("search", remaining_ms=1, at_least_ms=50, queued=False),
        CircuitOpenError("search", BreakerState.OPEN, 30.0),
        BulkheadFullError("search", in_flight=1, queued=0, queue_capacity=0),
        SlotWaitExpiredError("search", waited_ms=1),
        UpstreamTimeoutError("search", waited_ms=1),
    ):
        mapped = refusal(exc)
        assert mapped.headers()[REFUSAL_HEADER] == mapped.reason


# ---------------------------------------------------------------------------
# Retry-After arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("retry_after_s", "expected"),
    [
        (0.0, "1"),
        (0.001, "1"),
        (0.4, "1"),
        (1.0, "1"),
        (1.2, "2"),
        (29.5, "30"),
        (30.0, "30"),
    ],
)
def test_retry_after_rounds_up_and_never_falls_below_one_second(
    retry_after_s: float, expected: str
) -> None:
    """Two rules in one table, and both of them are about not causing the storm we are refusing.

    `delta-seconds` is an integer (RFC 9110 §10.2.3), so a float has to be rounded, and rounding
    *down* is the trap: 0.4s becomes `Retry-After: 0`, which tells every refused client to retry
    immediately and reproduces exactly the hammering the breaker opened to stop. Ceiling fixes that
    for anything above zero; the floor of 1 covers a cooldown that has already effectively elapsed,
    where ceiling alone still yields 0.
    """
    mapped = Refusal(503, "circuit-open", "detail", retry_after_s=retry_after_s)

    assert mapped.headers()["retry-after"] == expected
