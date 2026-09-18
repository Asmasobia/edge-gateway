"""End-to-end tests of the HTTP surface, from an ASGI request to what the dependency actually saw.

WHY THESE EXIST WHEN EVERY PART IS ALREADY TESTED
--------------------------------------------------------------------------------
The pieces below all have their own tests: the guard's ordering in `test_guard.py`, the header
filter in `test_headers.py`, the status table in `test_errors.py`. What none of those can see is the
*wiring* — and every defect this file has caught so far was a wiring defect, not a logic defect:

  * a header filtered correctly and then re-added by the client library (the cookie leak, below);
  * a budget recomputed correctly and then not attached to the outbound request;
  * a refusal mapped correctly and then never reached, because the exception came out of a layer
    that was not inside the `try`.

Each of those passes every unit test in the project.

HOW THEY RUN WITH NO SERVER, NO PORT AND NO SLEEPING
--------------------------------------------------------------------------------
Two transports, stacked, and it is worth being clear about which does what:

  * `httpx.ASGITransport` puts a real `httpx` request into the real FastAPI app in-process. It
    exercises routing, the raw ASGI header bytes and the real `Request` object.
  * `httpx.MockTransport` answers the gateway's *outbound* call from a function. The gateway's own
    client, its pool limits, its request construction and its header handling are all real; only the
    socket is missing.

So a test asserts on both sides at once: the response the caller received, and the request the
dependency was handed. That second half is the one that matters here — several of the bugs above are
completely invisible from the caller's side.

`ManualClock` is the default because a deadline that does not move on its own is a deadline that
cannot make a test flaky. The two tests that genuinely need concurrency use an `asyncio.Event` to
sequence themselves rather than a sleep, for the same reason.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
import pytest
from fastapi import FastAPI

from edge_gateway.gateway import GatewayConfig, Upstream, UpstreamConfig, create_app
from edge_gateway.resilience import BreakerConfig, Clock, ManualClock

pytestmark = pytest.mark.asyncio

Handler = Callable[[httpx.Request], object]


# ---------------------------------------------------------------------------
# Rig
# ---------------------------------------------------------------------------


@dataclass
class Recorder:
    """Answers the gateway's outbound calls and keeps every request it was given.

    A list rather than a single "last request", because half the tests here are about what happens
    across *two* requests — the cookie leak is only visible in the second one — and a rig that
    remembers only the most recent call cannot express that.
    """

    response: Callable[[httpx.Request], httpx.Response]
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.response(request)

    @property
    def last(self) -> httpx.Request:
        assert self.requests, "the dependency was never called"
        return self.requests[-1]

    def header(self, name: str) -> str | None:
        """What the dependency saw for one header on the most recent call. None if absent.

        Most assertions in this file are of this shape. The dependency's view is the only place
        where "we filtered that header" and "we filtered it and then something re-added it" look
        different.
        """
        # The annotation is not decoration: `httpx.Headers.get` is typed as returning `Any`, so
        # under `--strict` returning it directly is a `no-any-return` error. Naming the type here is
        # where the untyped value stops being untyped, and a future httpx returning bytes is caught.
        value: str | None = self.last.headers.get(name)
        return value


def recording(
    response: Callable[[httpx.Request], httpx.Response] | None = None,
) -> Recorder:
    """A recorder that answers 200 `ok` unless told otherwise."""
    return Recorder(response or (lambda _: httpx.Response(200, text="ok")))


def breaker(**overrides: object) -> BreakerConfig:
    """A breaker that trips on the second failure, so an end-to-end test needs two requests.

    `minimum_throughput=2` is unrealistically low for production and exactly right here: the
    threshold arithmetic is already tested in `test_breaker.py`, so paying twenty requests to reach
    it would only make this file slower and its intent less obvious.
    """
    kwargs: dict[str, object] = {
        "failure_rate_threshold": 0.5,
        "minimum_throughput": 2,
        "window_seconds": 60.0,
        "open_duration_seconds": 30.0,
        "half_open_max_concurrent": 1,
        "half_open_successes_to_close": 1,
    }
    kwargs.update(overrides)
    return BreakerConfig(**kwargs)  # type: ignore[arg-type]


def one_upstream(**overrides: object) -> UpstreamConfig:
    kwargs: dict[str, object] = {
        "name": "search",
        "base_url": "http://search.internal",
        "breaker": breaker(),
        "max_concurrent": 4,
        "max_queue": 4,
        "reserve_ms": 20,
        "minimum_call_ms": 50,
    }
    kwargs.update(overrides)
    return UpstreamConfig(**kwargs)  # type: ignore[arg-type]


def make_app(
    handler: Handler,
    *,
    clock: Clock | None = None,
    upstreams: tuple[UpstreamConfig, ...] | None = None,
    default_budget_ms: int = 1000,
    max_budget_ms: int = 5000,
) -> FastAPI:
    return create_app(
        GatewayConfig(
            upstreams=upstreams if upstreams is not None else (one_upstream(),),
            default_budget_ms=default_budget_ms,
            max_budget_ms=max_budget_ms,
        ),
        clock=clock if clock is not None else ManualClock(0.0),
        transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
    )


def caller(app: FastAPI) -> httpx.AsyncClient:
    """A client that speaks to the app in-process.

    `cookies=None` disables the *outer* client's own jar. Without it, a `Set-Cookie` in a gateway
    response would be stored by the test client and replayed on the next request — which would make
    the cookie-leak test below pass for the wrong reason, or fail for one, depending on the day. The
    test needs to observe the gateway's jar, so the test's own jar has to be out of the picture.
    """
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gateway.test",
        cookies=None,
    )


@asynccontextmanager
async def gateway(handler: Handler, **kwargs: object) -> AsyncIterator[httpx.AsyncClient]:
    """The common case: one app, one caller, closed afterwards."""
    app = make_app(handler, **kwargs)  # type: ignore[arg-type]
    async with caller(app) as client:
        yield client


# ---------------------------------------------------------------------------
# The happy path, and what reaches the dependency
# ---------------------------------------------------------------------------


async def test_a_request_is_proxied_and_the_path_segment_is_stripped() -> None:
    """`/search/products` reaches the `search` upstream as `/products`.

    The strip is the entire routing scheme: the first segment names the upstream and is consumed by
    the gateway, so the dependency sees the path it would have seen without a proxy in front — which
    is what lets a service be put behind this gateway without changing it.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        response = await client.get("/search/products")

    assert response.status_code == 200
    assert response.text == "ok"
    assert upstream.last.url.path == "/products"
    assert upstream.last.url.host == "search.internal"


async def test_a_request_to_an_upstreams_root_is_proxied_too() -> None:
    """`/search` with no trailing path, which `/{upstream_name}/{path:path}` does not match.

    The literal slash in that pattern is required, so without the second `api_route` decorator this
    would 404 — and an upstream whose root is the useful endpoint is completely normal. The
    alternative, a redirect to `/search/`, would spend a round trip teaching the client something we
    already knew.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        response = await client.get("/search")

    assert response.status_code == 200
    assert upstream.last.url.path == "/"


async def test_a_nested_path_is_forwarded_whole() -> None:
    """Only the first segment is consumed, however many follow it.

    `path:path` is what makes the rest arbitrary. A route that matched a fixed number of segments
    would work for every URL in the tests and break on the first real API with a nested resource.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        await client.get("/search/v2/products/42/reviews")

    assert upstream.last.url.path == "/v2/products/42/reviews"


@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def test_each_proxied_method_is_forwarded_as_itself(method: str) -> None:
    """The method must survive, because a proxy that rewrites one is worse than one that refuses it.

    Parametrised over the list the app declares, minus HEAD (tested separately, since a HEAD
    response has its own body rule). CONNECT and TRACE are deliberately not in that list — a tunnel
    is a different piece of software, and TRACE echoes headers back to a client that could not
    otherwise read them.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        response = await client.request(method, "/search/thing")

    assert response.status_code == 200
    assert upstream.last.method == method


async def test_a_request_body_is_forwarded_unchanged() -> None:
    """Bytes in, the same bytes out, with no reinterpretation on the way through.

    FastAPI is used here for routing only, never for validation: a proxy that parsed bodies would
    reject anything it did not have a model for, which is every request. The body is read as bytes
    and written as bytes, so a content type this gateway has never heard of passes through intact.
    """
    upstream = recording()
    payload = b'{"query": "socks", "page": 2}'

    async with gateway(upstream) as client:
        await client.post("/search/query", content=payload, headers={"content-type": "text/weird"})

    assert upstream.last.content == payload
    assert upstream.header("content-type") == "text/weird"


async def test_the_query_string_is_passed_through_byte_for_byte() -> None:
    """Repeated keys, a `+`, a `%20` and a bare key with no value — all of it preserved exactly.

    This is why `Upstream._target` re-attaches the raw query string instead of parsing it into pairs
    and re-encoding. Round-tripping through a parser is lossy in ways that break real APIs: order is
    lost, repeated keys may collapse to one, and `+` versus `%20` versus a literal space are
    decisions the client already made and may be signing. A proxy passes the query through; it does
    not tidy it.
    """
    upstream = recording()
    query = "q=a+b&q=c%20d&flag&empty="

    async with gateway(upstream) as client:
        await client.get(f"/search/products?{query}")

    assert upstream.last.url.query.decode() == query


async def test_an_end_to_end_header_reaches_the_dependency() -> None:
    """Authorization in particular, because a proxy that eats it breaks every authenticated call.

    The filter is a deny-list precisely so this keeps working for headers written after this code
    was.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        await client.get(
            "/search/products",
            headers={"authorization": "Bearer t0ken", "x-request-hint": "abc"},
        )

    assert upstream.header("authorization") == "Bearer t0ken"
    assert upstream.header("x-request-hint") == "abc"


async def test_the_host_header_names_the_dependency_and_not_the_gateway() -> None:
    """Our `Host` is dropped and httpx sets the correct one from `base_url`.

    Forwarding ours would hand a virtual-hosted origin the wrong name, and it would serve whatever
    is configured for `gateway.test` — on a shared host, potentially somebody else's site — with a
    200 and no error anywhere.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        await client.get("/search/products")

    assert upstream.header("host") == "search.internal"


async def test_a_redirect_is_handed_back_to_the_caller_rather_than_followed() -> None:
    """A 302 is a real answer, and following it would spend one budget on an unbounded chain.

    The client asked for *this* resource, and it may want to cache the redirect, rewrite it, or
    decline to follow it at all — none of which it can do if we followed it on its behalf and
    returned the destination. Worse for the resilience story: a followed chain spends one budget
    and one bulkhead slot on however many calls it takes, and records only the last outcome against
    the breaker, so a dependency stuck in a redirect loop looks perfectly healthy while consuming
    everything. The single outbound request is the assertion that matters here.
    """
    upstream = recording(
        lambda _: httpx.Response(302, headers=[("location", "http://search.internal/moved")])
    )

    async with gateway(upstream) as client:
        response = await client.get("/search/products")

    assert response.status_code == 302
    assert response.headers["location"] == "http://search.internal/moved"
    assert len(upstream.requests) == 1


async def test_the_outbound_client_applies_no_timeout_of_its_own() -> None:
    """Asserted on the client directly, because nothing observable can prove it otherwise.

    Two timeouts for one call are two deadlines with no relationship: whichever fires first decides
    the outcome, the winner depends on configuration in two files nobody reads together, and the
    request's real budget — the only number that reflects what the caller is still waiting for — is
    the one that loses. The guard owns the timeout because the guard is the only thing that knows
    the budget.

    This reaches into `_client`, which a test normally should not do. The justification is that the
    alternative is no test at all: httpx's own timeout can only be observed by letting one *fire*,
    which means a real socket, a real delay, and a test that is slow and flaky in exchange for
    checking a constant. Pinning the setting is the honest trade.
    """
    upstream = Upstream(one_upstream(), clock=ManualClock(0.0))
    try:
        assert upstream._client.timeout == httpx.Timeout(None)
    finally:
        await upstream.aclose()


# ---------------------------------------------------------------------------
# The response direction
# ---------------------------------------------------------------------------


async def test_the_status_code_and_body_come_back_untouched() -> None:
    """Including a 500. A proxy must forward a dependency's error, not raise on it.

    This is the reason `raise_for_status()` is never called anywhere in the project: it would turn
    every upstream 500 into an exception inside our handler, and the client would get our error page
    instead of the dependency's — losing the body that says what actually went wrong.
    """
    upstream = recording(lambda _: httpx.Response(500, text="upstream exploded"))

    async with gateway(upstream) as client:
        response = await client.get("/search/products")

    assert response.status_code == 500
    assert response.text == "upstream exploded"


async def test_repeated_set_cookie_headers_all_survive() -> None:
    """Two cookies out, two cookies back — which a `Mapping` of headers cannot do.

    `_proxied_response` assigns `raw_headers` rather than passing `headers=` for exactly this. With
    a mapping, one of these is silently dropped, and the symptom — "login works but the next request
    is unauthenticated" — points at the auth service, the browser, or the session store. Anywhere
    but the proxy.
    """
    upstream = recording(
        lambda _: httpx.Response(
            200,
            headers=[
                ("set-cookie", "session=abc; Path=/"),
                ("set-cookie", "csrf=xyz; Path=/"),
            ],
        )
    )

    async with gateway(upstream) as client:
        response = await client.get("/search/login")

    assert response.headers.get_list("set-cookie") == [
        "session=abc; Path=/",
        "csrf=xyz; Path=/",
    ]


async def test_content_length_matches_the_body_we_actually_send() -> None:
    """Recomputed from the bytes, because this hop may have changed them.

    `httpx` decompresses transparently, so a gzipped upstream body arrives as plaintext and the
    origin's `Content-Length` no longer describes it. Forwarded, that value is too small and the
    client truncates the body — silently, with a 200.
    """
    upstream = recording(lambda _: httpx.Response(200, text="a longer body than before"))

    async with gateway(upstream) as client:
        response = await client.get("/search/products")

    assert response.headers["content-length"] == str(len(b"a longer body than before"))


async def test_a_204_carries_no_content_length_at_all() -> None:
    """A body-less status must not announce a body length, not even zero.

    `Content-Length: 0` on a 204 is a spec violation (RFC 9110 §6.4.1) that most clients forgive and
    some intermediaries reject outright — which makes it the kind of defect that appears only after
    a CDN is put in front, months after the code was written.
    """
    upstream = recording(lambda _: httpx.Response(204))

    async with gateway(upstream) as client:
        response = await client.get("/search/thing")

    assert response.status_code == 204
    assert "content-length" not in response.headers


async def test_a_hop_by_hop_header_from_the_dependency_is_not_relayed() -> None:
    """The filter runs on the way back too, which is easy to forget and hard to notice.

    A dependency answering `Connection: close` would, relayed, tear down the *caller's* keep-alive
    connection to us — turning one origin's connection policy into a latency cost for every client
    of the gateway.
    """
    upstream = recording(
        lambda _: httpx.Response(200, headers=[("connection", "close"), ("x-kept", "yes")])
    )

    async with gateway(upstream) as client:
        response = await client.get("/search/products")

    assert "connection" not in {name.lower() for name in response.headers}
    assert response.headers["x-kept"] == "yes"


# ---------------------------------------------------------------------------
# THE COOKIE LEAK — the regression test `upstream.py` promises lives here
# ---------------------------------------------------------------------------


async def test_one_callers_cookies_are_never_attached_to_another_callers_request() -> None:
    """The worst bug this slice could have shipped, and the test that found it.

    `httpx.AsyncClient` is a user agent: it stores every `Set-Cookie` it sees and replays the jar to
    the same host. Correct for a browser-like client, catastrophic for a shared proxy, because one
    client serves every caller — client A logs in, the jar keeps `session=SECRET`, and client B's
    unrelated request goes out carrying it. B is then authenticated as A at the dependency, with no
    bug anywhere in this project's own code.

    Note where the assertion is. Both callers get a perfectly normal 200 with a perfectly normal
    body, so a test that checked responses passes cheerfully while the leak is live. The only place
    it is visible is the `Cookie` header the *dependency* was handed, which is what this asserts on
    — and the two separate callers matter too, since a single client would explain the cookie by its
    own jar.
    """
    upstream = recording(
        lambda request: (
            httpx.Response(200, headers=[("set-cookie", "session=SECRET; Path=/")])
            if request.url.path == "/login"
            else httpx.Response(200, text="ok")
        )
    )
    app = make_app(upstream)

    async with caller(app) as client_a:
        login = await client_a.get("/search/login")
    assert login.headers.get_list("set-cookie") == ["session=SECRET; Path=/"]

    async with caller(app) as client_b:
        other = await client_b.get("/search/products")
    assert other.status_code == 200

    assert upstream.header("cookie") is None, (
        "a cookie set for one caller was attached to another caller's outbound request"
    )


async def test_a_callers_own_cookie_header_is_still_forwarded() -> None:
    """The other half of the fix, and the one it would be easy to break while making the first.

    Suppressing the client's *jar* must not suppress a `Cookie` header the caller sent deliberately
    — that header is end-to-end, it is how session authentication works, and dropping it would log
    every user out. `_NoCookies` only declines to *remember*; it does not filter.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        await client.get("/search/products", headers={"cookie": "mine=1"})

    assert upstream.header("cookie") == "mine=1"


# ---------------------------------------------------------------------------
# Budget propagation
# ---------------------------------------------------------------------------


async def test_the_budget_sent_downstream_is_recomputed_and_not_the_one_we_received() -> None:
    """1000 in, 980 out: the reserve is deducted before the header is written.

    Forwarding the client's value verbatim is the failure that quietly disables the whole scheme —
    every hop grants itself a fresh full budget, the deadline stops shrinking along the chain, and a
    ten-hop request can spend ten times what its caller was willing to wait. With a `ManualClock` no
    time passes during the test, so the 20ms difference is the reserve alone and the assertion is
    exact rather than approximate.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        await client.get("/search/products", headers={"x-timeout-budget-ms": "1000"})

    assert upstream.header("x-timeout-budget-ms") == "980"


async def test_a_request_with_no_budget_header_is_given_the_configured_default() -> None:
    """The edge case: a browser cannot send the header, so the default is the normal path there.

    Recorded as DEFAULTED internally, which is a signal worth watching on an *internal* hop — if
    internal traffic is regularly defaulted then propagation is broken somewhere and this whole
    mechanism has quietly stopped working, with nothing failing to show it.
    """
    upstream = recording()

    async with gateway(upstream, default_budget_ms=400) as client:
        await client.get("/search/products")

    assert upstream.header("x-timeout-budget-ms") == "380"


async def test_an_absurd_requested_budget_is_clamped_to_the_ceiling() -> None:
    """The trust boundary. The budget arrives in a header, so at the edge it is attacker-controlled.

    Without a ceiling, `x-timeout-budget-ms: 86400000` pins a worker, a connection and a bulkhead
    slot for a day, and a few dozen of those are a denial of service that costs the sender nothing
    but a handful of requests.
    """
    upstream = recording()

    async with gateway(upstream, max_budget_ms=2000) as client:
        await client.get("/search/products", headers={"x-timeout-budget-ms": "86400000"})

    assert upstream.header("x-timeout-budget-ms") == "1980"


async def test_a_malformed_budget_falls_back_to_the_default_rather_than_failing() -> None:
    """A peer's bad header must not become a user-visible error.

    The budget is an optimisation hint. Rejecting the request with a 400 would turn one caller's
    header bug into an outage for its users, so the value is ignored, the default applies, and the
    event is recorded as MALFORMED where it is loud in metrics and harmless in production.
    """
    upstream = recording()

    async with gateway(upstream, default_budget_ms=600) as client:
        response = await client.get(
            "/search/products", headers={"x-timeout-budget-ms": "soon please"}
        )

    assert response.status_code == 200
    assert upstream.header("x-timeout-budget-ms") == "580"


async def test_a_budget_too_small_to_be_worth_spending_is_refused_before_the_call() -> None:
    """504 with no outbound request at all, which is the point of the viability check.

    With 10ms granted, a 20ms reserve and a 50ms floor there is nothing to spend, and issuing the
    call anyway would cost a connection and a bulkhead slot, make the dependency do the work,
    discard the answer, and then record the resulting timeout as *the dependency's* failure.
    Refusing locally costs nothing and blames the right thing — which the empty request list is the
    proof of.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        response = await client.get("/search/products", headers={"x-timeout-budget-ms": "10"})

    assert response.status_code == 504
    assert response.json() == {"error": "budget-exhausted-on-arrival"}
    assert response.headers["x-gateway-refusal"] == "budget-exhausted-on-arrival"
    assert upstream.requests == []


# ---------------------------------------------------------------------------
# Refusals, end to end
# ---------------------------------------------------------------------------


async def test_an_unknown_upstream_is_a_404_that_says_it_was_ours() -> None:
    """404 rather than 502, and labelled, because two very different 404s exist here.

    "There is no such route on the gateway" is a client mistake or a stale config; "the dependency
    returned 404" is a normal, successful lookup that found nothing. They are indistinguishable by
    status code, they lead to completely different investigations, and the reason header is what
    separates them in a dashboard.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        response = await client.get("/pricing/quotes")

    assert response.status_code == 404
    assert response.json() == {"error": "unknown-upstream"}
    assert response.headers["x-gateway-refusal"] == "unknown-upstream"
    assert upstream.requests == []


async def test_an_open_circuit_refuses_with_a_503_and_a_retry_time() -> None:
    """Two failures trip it; the third request is refused without touching the dependency.

    The refusal is the entire point of a breaker, and the assertion that matters is the last one:
    the dependency received exactly two calls. A breaker that opened but still called through would
    look identical in every response the client sees, and would provide none of the relief it exists
    to give — which is that a struggling dependency stops receiving traffic while it recovers.
    """
    upstream = recording(lambda _: httpx.Response(500, text="broken"))

    async with gateway(upstream) as client:
        first = await client.get("/search/products")
        second = await client.get("/search/products")
        third = await client.get("/search/products")

    assert (first.status_code, second.status_code) == (500, 500)
    assert third.status_code == 503
    assert third.json() == {"error": "circuit-open"}
    assert third.headers["x-gateway-refusal"] == "circuit-open"
    assert third.headers["retry-after"] == "30"
    assert len(upstream.requests) == 2


async def test_a_4xx_from_the_dependency_never_opens_a_circuit() -> None:
    """Ten 404s in a row, and the eleventh request still reaches the dependency.

    A 404 is a *successful* lookup that found nothing: the dependency understood the request,
    decided it was wrong, and said so quickly and cheaply. Counting these is how one caller with a
    stale URL or a deleted resource takes a healthy dependency offline for everybody else — a real
    and frequently-shipped bug, which is why it is asserted end to end and not only in the
    classifier's own tests.
    """
    upstream = recording(lambda _: httpx.Response(404, text="not found"))

    async with gateway(upstream) as client:
        for _ in range(10):
            response = await client.get("/search/missing")
            assert response.status_code == 404

    assert len(upstream.requests) == 10


async def test_a_transport_failure_becomes_a_502() -> None:
    """Connection refused, DNS failure, TLS failure: none are our bug, and none are a 500.

    502 Bad Gateway is exactly this: we are a gateway and could not obtain a valid response. A 500
    here would file a dependency's outage as ours, and the 500 rate — the one number that should
    always mean "somebody has to look at this code" — would stop meaning that.
    """

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with gateway(refuse) as client:
        response = await client.get("/search/products")

    assert response.status_code == 502
    assert response.headers["x-gateway-refusal"] == "transport-error"


async def test_a_full_bulkhead_refuses_immediately_with_no_retry_after() -> None:
    """One slot, no queue, one call held open: the second caller is shed on arrival.

    Sequenced with an `asyncio.Event` rather than a sleep — the first request signals that it is
    inside the dependency, and only then is the second sent — so the test is deterministic rather
    than dependent on how fast the machine running it happens to be.

    No `Retry-After`, deliberately: saturation clears when in-flight calls finish, which is a
    latency distribution and not a deadline. Any number here would be fiction, and a shared one
    would synchronise every refused caller onto the same instant.
    """
    inside = asyncio.Event()
    release = asyncio.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        inside.set()
        await release.wait()
        return httpx.Response(200, text="eventually")

    async with gateway(slow, upstreams=(one_upstream(max_concurrent=1, max_queue=0),)) as client:
        first = asyncio.create_task(client.get("/search/slow"))
        await inside.wait()

        second = await client.get("/search/products")

        release.set()
        held = await first

    assert second.status_code == 503
    assert second.json() == {"error": "bulkhead-full"}
    assert second.headers["x-gateway-refusal"] == "bulkhead-full"
    assert "retry-after" not in second.headers

    assert held.status_code == 200
    assert held.text == "eventually"


async def test_a_queued_caller_is_admitted_when_the_slot_frees() -> None:
    """The other side of the bulkhead: a queue that works is not the same as a queue that refuses.

    Worth asserting alongside the refusal, because a bulkhead that shed everything the moment it was
    busy would pass the previous test perfectly. With one slot and a queue of one, the second caller
    waits and then succeeds — which is the behaviour that makes a queue worth having at all, since
    most saturation is a burst rather than a sustained overload.
    """
    inside = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def slow(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            inside.set()
            await release.wait()
        return httpx.Response(200, text="done")

    async with gateway(slow, upstreams=(one_upstream(max_concurrent=1, max_queue=1),)) as client:
        first = asyncio.create_task(client.get("/search/slow"))
        await inside.wait()

        queued = asyncio.create_task(client.get("/search/products"))
        # Yield to the loop so the second request definitely reaches the queue before the slot
        # frees. `sleep(0)` rather than a duration: it is a scheduling hint, not a wait, so it
        # cannot become a flaky timing dependency.
        await asyncio.sleep(0)

        release.set()
        assert (await first).status_code == 200
        assert (await queued).status_code == 200

    assert calls == 2


# ---------------------------------------------------------------------------
# The operational endpoints
# ---------------------------------------------------------------------------


async def test_healthz_reports_ok_even_while_every_circuit_is_open() -> None:
    """The most important small decision in the app, and the one a plausible mistake inverts.

    A health check that failed here would tell the orchestrator to restart or remove this instance
    during a dependency outage — destroying the capacity that was about to serve degraded traffic,
    and doing it to every replica at once because they all see the same outage. The health check
    would then be the outage. A gateway with an open circuit is *working*: it is shedding load on
    purpose.
    """
    upstream = recording(lambda _: httpx.Response(500, text="broken"))

    async with gateway(upstream) as client:
        await client.get("/search/products")
        await client.get("/search/products")
        refused = await client.get("/search/products")
        health = await client.get("/healthz")

    assert refused.status_code == 503
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}


async def test_resilience_publishes_the_state_that_healthz_deliberately_hides() -> None:
    """Dependency state has to be *somewhere*; the point is that nothing here kills a pod.

    A human or a dashboard reads this. The pairing is what makes it diagnostic: an open breaker
    beside an empty bulkhead says the dependency broke, while a closed breaker beside a full queue
    says the dependency is fine and we are the bottleneck. Those have different fixes and a single
    number cannot distinguish them.
    """
    upstream = recording(lambda _: httpx.Response(500, text="broken"))

    async with gateway(upstream) as client:
        await client.get("/search/products")
        await client.get("/search/products")
        payload = (await client.get("/resilience")).json()

    (entry,) = payload["upstreams"]
    assert entry["name"] == "search"
    assert entry["breaker"]["state"] == "open"
    assert entry["breaker"]["failures"] == 2
    assert entry["breaker"]["retry_after_s"] == 30.0
    assert entry["bulkhead"]["in_flight"] == 0


async def test_resilience_reports_every_upstream_separately() -> None:
    """Isolation, visible. One upstream's circuit opening must leave the other's alone.

    This is the whole thesis of the project reduced to one assertion: shared protection converts a
    partial outage into a total one, and per-upstream protection is only worth having if it can be
    *seen* to be per-upstream during an incident.
    """
    upstream = recording(
        lambda request: (
            httpx.Response(500)
            if request.url.host == "search.internal"
            else httpx.Response(200, text="fine")
        )
    )
    app = make_app(
        upstream,
        upstreams=(
            one_upstream(name="search", base_url="http://search.internal"),
            one_upstream(name="pricing", base_url="http://pricing.internal"),
        ),
    )

    async with caller(app) as client:
        await client.get("/search/products")
        await client.get("/search/products")
        assert (await client.get("/pricing/quotes")).status_code == 200
        payload = (await client.get("/resilience")).json()

    states = {entry["name"]: entry["breaker"]["state"] for entry in payload["upstreams"]}
    assert states == {"search": "open", "pricing": "closed"}


async def test_the_counters_that_should_always_be_zero_are_published() -> None:
    """`unresolved_permits` and `trips` are exported because one of them must stay at zero.

    A non-zero `unresolved_permits` is a bug in this project — a permit acquired and never resolved,
    which slowly starves the half-open state — and it is invisible from the outside. Publishing it
    is what gives anyone a chance to notice. Hiding a counter because it *should* be zero is how a
    counter designed to be alerted on ends up read by nobody.
    """
    async with gateway(recording()) as client:
        await client.get("/search/products")
        (entry,) = (await client.get("/resilience")).json()["upstreams"]

    assert entry["breaker"]["unresolved_permits"] == 0
    assert entry["breaker"]["trips"] == 0
    assert entry["breaker"]["successes"] == 1


async def test_an_upstream_cannot_shadow_the_operational_routes() -> None:
    """The declaration-order defence, asserted from the outside rather than trusted.

    `UpstreamConfig` already refuses to be *named* `healthz` (see `test_config.py`), so this checks
    the second line of defence: even a legitimately-named upstream must not swallow `/healthz`
    through the catch-all. Both defences are cheap and the failure they prevent — a route that
    answers 200 while being permanently unreachable — is diagnosed as a networking problem for a
    day.
    """
    upstream = recording()

    async with gateway(upstream) as client:
        assert (await client.get("/healthz")).json() == {"status": "ok"}
        assert "upstreams" in (await client.get("/resilience")).json()

    assert upstream.requests == []


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_the_lifespan_closes_the_connection_pools_on_shutdown() -> None:
    """An unclosed pool leaks sockets until the process exits, and the symptom hides the cause.

    On a development server that reloads on every edit, each reload strands a pool: it "stops
    working after twenty saves", which looks like a bug in whatever was edited twentieth. The
    assertion is indirect on purpose — the pools live in a closure and are deliberately not
    reachable from outside `create_app` — so the test observes the only externally visible
    consequence of a closed client, which is that it refuses to send anything.
    """
    app = make_app(recording())

    async with caller(app) as client:
        async with app.router.lifespan_context(app):
            assert (await client.get("/search/products")).status_code == 200

        with pytest.raises(RuntimeError, match="closed"):
            await client.get("/search/products")
