"""The single place a resilience exception becomes an HTTP status code.

WHY THIS IS ONE TABLE AND NOT AN `except` CLAUSE IN THE HANDLER
--------------------------------------------------------------------------------
The mapping is a policy, and policies that are spread across handlers diverge. Two proxy routes that
disagree about whether a full bulkhead is a 503 or a 429 will be discovered by a client's retry
logic behaving differently on two paths, which is a very expensive way to find out. One table, one
test file, one place to change.

WHY THE STATUS CODE IS NOT ENOUGH, AND WHAT THE HEADER IS FOR
--------------------------------------------------------------------------------
Five distinct refusals below collapse onto two status codes, and that collapse is correct: the
client only needs to know "retry later" or "the deadline passed", and inventing private status codes
would break every intermediary that understands the standard ones.

But *we* need the distinction, because the five have five different fixes:

    circuit-open           the dependency is unhealthy — go and look at the dependency
    bulkhead-full          we are out of capacity for it — raise max_concurrent, or shed sooner
    slot-wait-expired      our queue is too deep for the budget — lower max_queue
    budget-exhausted-*     the budget was too small, or something upstream ate it
    upstream-timeout       the dependency was called and did not answer

So the reason travels in `x-gateway-refusal`, which is one label, cheap to log, cheap to graph, and
usable in a dashboard as a breakdown of a single 503 line. Deriving the same information from a
status code alone is impossible; deriving it by grepping message prose is possible and awful.

WHY A FULL BULKHEAD IS 503 AND NOT 429
--------------------------------------------------------------------------------
429 Too Many Requests says *you* sent too many. A client that sent exactly one request, and had it
refused because two hundred other clients are mid-flight to the same dependency, has been told
something false — and it will act on it, because well-written clients back off harder on 429 than on
503. The overload is ours, not theirs, and 503 Service Unavailable with a reason header states that
accurately. 429 would be right if this gateway did per-client rate limiting, which it does not.

WHY NOTHING HERE IS A 500
--------------------------------------------------------------------------------
Every refusal below is the gateway working exactly as designed. 500 Internal Server Error means "we
have a bug", and using it for deliberate load-shedding destroys the one signal that reliably means
somebody has to be paged: a 500 rate that rises during every traffic spike is a 500 rate nobody
looks at. An unexpected exception — a real bug — still becomes a 500, because `refusal_for` returns
None for anything it does not recognise rather than guessing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import httpx

from edge_gateway.resilience import (
    BudgetExhaustedError,
    BulkheadFullError,
    CircuitOpenError,
    SlotWaitExpiredError,
    UpstreamTimeoutError,
)

REFUSAL_HEADER = "x-gateway-refusal"


@dataclass(frozen=True, slots=True)
class Refusal:
    """A refusal, split into what the client is told and what we record.

    The split is the point of the class. `reason` is a stable slug safe to hand to anyone; `detail`
    is the exception's own message, which contains our queue depths, our concurrency limits and our
    internal component names. Handing that to an untrusted caller describes our capacity and our
    topology to them for free, and it is exactly the reconnaissance a load-based attack wants. It
    goes to the log, where it is genuinely useful, and no further.
    """

    status: int
    reason: str
    detail: str
    retry_after_s: float | None = None

    def headers(self) -> dict[str, str]:
        """Response headers for this refusal, including `Retry-After` when we can justify one."""
        headers = {REFUSAL_HEADER: self.reason}
        if self.retry_after_s is not None:
            # `delta-seconds` is an integer (RFC 9110 §10.2.3), so the float has to be rounded — and
            # the direction matters. Rounding down turns 0.4s into `Retry-After: 0`, which tells a
            # client to retry immediately and produces precisely the retry storm the breaker opened
            # to stop. Ceiling, with a floor of 1, so the header is never an invitation to hammer.
            headers["retry-after"] = str(max(1, math.ceil(self.retry_after_s)))
        return headers

    def body(self) -> dict[str, str]:
        """The JSON body sent to the client. Deliberately only the slug.

        JSON rather than the origin's own error format, because at the moment of refusal there is no
        origin response to imitate — the request was never made, or never answered. A
        machine-readable envelope is the honest thing to send, and a client that receives 503 with
        an unexpected body shape is a client that was already going to have to handle a proxy error
        page.
        """
        return {"error": self.reason}


def refusal_for(exc: BaseException) -> Refusal | None:
    """Map an exception to a refusal, or None if it is not one of ours.

    None means "this is a bug, let it become a 500". Returning a tidy 502 for every unrecognised
    exception is tempting and wrong: it would hide a `KeyError` in our own routing code as a
    dependency problem, and the investigation would start at the dependency and stay there.

    The ordering of the `isinstance` checks matters in one place, flagged at the line itself.
    """
    if isinstance(exc, BudgetExhaustedError):
        # 504 rather than 503, even though on the `queued=False` path no call was ever made. The
        # client's question is "why is there no response", and the answer is "the deadline you set
        # had passed" — which is what 504 Gateway Timeout means. 503 would say "come back later",
        # and a client that obeys it will retry with the same too-small budget and fail identically.
        where = "while-queued" if exc.queued else "on-arrival"
        return Refusal(504, f"budget-exhausted-{where}", str(exc))

    if isinstance(exc, CircuitOpenError):
        # The one refusal that can honestly state when to come back, because the breaker knows its
        # own cooldown. `retry_after_s` is None in HALF_OPEN — the trial slots are momentarily full
        # and a slot may free in a microsecond or a minute — and None must stay None rather than
        # becoming a guess, since a client will trust whatever number we print.
        return Refusal(503, "circuit-open", str(exc), retry_after_s=exc.retry_after_s)

    if isinstance(exc, BulkheadFullError):
        # No `Retry-After`. Saturation has no scheduled end: it clears when in-flight calls finish,
        # which is a latency distribution and not a deadline. A made-up number would synchronise
        # every refused client onto the same instant — a thundering herd we would have caused
        # ourselves.
        return Refusal(503, "bulkhead-full", str(exc))

    if isinstance(exc, SlotWaitExpiredError):
        return Refusal(504, "slot-wait-expired", str(exc))

    if isinstance(exc, UpstreamTimeoutError):
        return Refusal(504, "upstream-timeout", str(exc))

    # MUST precede the `TransportError` check below: `httpx.TimeoutException` is a subclass of it,
    # so the general case first would swallow every timeout into a 502 and the distinction between
    # "did not answer" and "could not be reached" would be lost — which are the two most different
    # diagnoses in this whole file.
    if isinstance(exc, httpx.TimeoutException):
        # Should be unreachable: the client is built with `timeout=None` precisely so the guard owns
        # every timeout (ADR-038). Mapped anyway, and mapped to its own reason, because if it ever
        # fires it means a timeout exists that the deadline does not know about — a real bug in our
        # wiring — and `x-gateway-refusal: client-timeout` in a dashboard is how we would find out.
        return Refusal(504, "client-timeout", str(exc))

    if isinstance(exc, httpx.TransportError):
        # Connection refused, DNS failure, TLS handshake failure, connection reset mid-body. 502 Bad
        # Gateway is exactly this: we are a gateway and we could not get a valid response from
        # upstream. Distinct from 504 on purpose — "nothing is listening" is a deployment or
        # discovery problem, "it did not answer in time" is a capacity problem, and they go to
        # different people.
        return Refusal(502, "transport-error", str(exc))

    if isinstance(exc, httpx.InvalidURL):
        # Not an `HTTPError` in httpx's tree, so it would otherwise fall through to a 500 — and it
        # is worth catching separately because of where the input comes from. The path and query are
        # the CLIENT's, forwarded deliberately unnormalised (see `Upstream._target`), so a URL that
        # cannot be constructed from them is far more likely to be a request we could not have
        # served than a mistake in our own string handling. 400 says that; 500 would blame ourselves
        # for it and put it in the wrong dashboard.
        return Refusal(400, "invalid-target", str(exc))

    if isinstance(exc, httpx.HTTPError):
        # A defensive net over the rest of httpx's error tree rather than a path with a known
        # trigger: today the only `HTTPError` that is neither a `TransportError` nor a timeout is
        # `HTTPStatusError`, which `raise_for_status()` raises and which this gateway never calls —
        # a proxy must forward a 500, not raise on it. Kept because "httpx grew a new error class"
        # should degrade to a 502 with a distinct label, not to a 500 that reads as our own bug.
        return Refusal(502, "upstream-protocol-error", str(exc))

    return None
