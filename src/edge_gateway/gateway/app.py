"""The ASGI application: routing, the proxy handler, and the two operational endpoints.

WHY ROUTING IS BY FIRST PATH SEGMENT
--------------------------------------------------------------------------------
`/search/products?q=x` goes to the upstream named `search`, as `/products?q=x`. One segment, one
upstream, no pattern language.

The alternative — regex route matching, or `Host`-header routing — is more flexible and buys nothing
this project needs. The property worth keeping is that **the URL in an access log tells you which
breaker and which bulkhead the request used**, with no lookup and no config file open beside you. At
3am that is worth more than flexibility, and it is the sort of thing that is easy to give up and
hard to get back.

The cost is stated plainly: an upstream's name is part of its public URL, so renaming one is a
client-visible change. Accepted.

WHY THE OPERATIONAL ROUTES ARE DECLARED FIRST — AND WHY THAT IS NOT THE REAL DEFENCE
--------------------------------------------------------------------------------
Starlette matches routes in declaration order, so `/healthz` must be registered before the catch-all
`/{upstream_name}/{path:path}` or it would be routed as an upstream named `healthz`. That is
necessary and it is not sufficient, because a config naming an upstream `healthz` would then be
silently unreachable — diagnosed for a day as a networking problem, since every layer involved is
behaving exactly as documented. `UpstreamConfig` rejects the reserved names outright, and the
ordering here is the second line of defence rather than the first.

WHY BODIES ARE BUFFERED, AND WHAT THAT COSTS
--------------------------------------------------------------------------------
`await request.body()` reads the whole request into memory, and `httpx` reads the whole response
into memory, before either is forwarded. A streaming proxy would hold neither.

Buffering is chosen because it makes every other guarantee in this project simple and true: the
bulkhead slot covers the entire interaction, the timeout covers the entire transfer, and the
response that gets classified is a complete response. Streaming makes all three conditional on where
the consumer stops reading.

The cost is real and is not hidden: a large upload or download occupies memory proportional to its
size for the duration of the call, so this gateway is unsuitable for file transfer or for streaming
responses (server-sent events would buffer until the connection closed, which is to say forever).
`README.md` lists this as a limitation rather than leaving it to be discovered.

WHY HEADERS ARE MOVED AROUND AS `latin-1`
--------------------------------------------------------------------------------
ASGI and `httpx` both expose raw header bytes. This module decodes them with `latin-1`, filters the
result, and encodes it back with `latin-1` — which is a lossless round trip for every possible byte
sequence, because `latin-1` maps bytes 0-255 onto code points 0-255 and back. Using `utf-8` instead
would raise on a header a misbehaving peer sent as raw bytes, turning their protocol violation into
a 500 from us; using `errors="replace"` would silently corrupt the value instead. Decoding at all is
only to make the filtering readable.
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, Response

from edge_gateway.gateway.config import GatewayConfig
from edge_gateway.gateway.errors import REFUSAL_HEADER, refusal_for
from edge_gateway.gateway.headers import headers_for_client, headers_for_upstream
from edge_gateway.gateway.upstream import UpstreamSnapshot, build_upstreams
from edge_gateway.resilience import (
    BreakerSnapshot,
    Clock,
    MonotonicClock,
    deadline_from_headers,
)

# CONNECT and TRACE are deliberately absent. CONNECT asks a proxy to open a tunnel, which is a
# different piece of software with a different threat model; TRACE echoes the request back and is
# switched off on anything public because it can be used to read headers a client cannot otherwise
# see. Neither is something a resilience gateway should quietly acquire by listing "all methods".
_PROXIED_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]

# Statuses that must not carry a body, and therefore must not carry a `Content-Length` (RFC 9110
# §6.4.1 for 204, §15.4.5 for 304). Sending one is tolerated by most clients and rejected by some
# proxies, which makes it exactly the sort of defect that only appears once there is a CDN in front.
_BODILESS_STATUSES = frozenset({204, 304})


def create_app(
    config: GatewayConfig,
    *,
    clock: Clock | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    """Build the gateway application.

    `clock` and `transport` are the two injection points, and they are the reason the HTTP layer can
    be tested in milliseconds with no server and no sleeping. `clock` lets a test drive deadlines
    and breaker cooldowns with `ManualClock.advance`; `transport` lets it answer requests from a
    function while still exercising the real `httpx` client. Both default to the production choice,
    so nothing has to be remembered when wiring the real thing.

    The upstreams are held in a closure rather than on `app.state`. `state` is an untyped bag: a
    typo in an attribute name there is an `AttributeError` at request time, whereas a closure
    variable is checked when the module is imported and by mypy before that.
    """
    the_clock = clock if clock is not None else MonotonicClock()
    upstreams = build_upstreams(config.upstreams, clock=the_clock, transport=transport)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        """Own the connection pools for the life of the process.

        The close happens after the `yield`, on shutdown. Deliberately not in a `finally`: if
        startup itself raised there would be nothing to close, and wrapping it would only convert a
        clear startup error into a confusing pair of them.
        """
        yield
        for upstream in upstreams.values():
            await upstream.aclose()

    app = FastAPI(
        title="edge-gateway",
        description="A resilience-aware reverse proxy: timeout budgets, breakers, bulkheads.",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Liveness only. Deliberately says nothing about the dependencies.

        This is the most important small decision in the file. A health check that reports unhealthy
        when a dependency is unhealthy tells the orchestrator to restart or remove this instance
        during a dependency outage — destroying the capacity that was about to serve degraded
        traffic, exactly when it is needed, and doing it to every replica simultaneously because
        they all see the same outage. The result is a total outage caused by the health check.

        A gateway whose circuit is open is *working*: it is shedding load on purpose. So this
        endpoint answers one question — is this process able to accept and route a request — and
        dependency state is published separately at `/resilience`, where a human or a dashboard
        reads it and nothing automatically kills anything.
        """
        return {"status": "ok"}

    @app.get("/resilience")
    async def resilience() -> dict[str, Any]:
        """Every upstream's breaker and bulkhead state, in one consistent read.

        For dashboards and for incident response, and worth being explicit about its exposure: it
        describes our concurrency limits, queue depths and current saturation, which is
        reconnaissance for anyone planning to overload us. In a real deployment this belongs on an
        admin port or behind authentication. It is unauthenticated here because this repository is a
        demonstration of the resilience patterns and adding an auth scheme would be a second,
        unrelated project — and saying so is better than shipping a token check that implies more
        than it does.
        """
        return {
            "upstreams": [_snapshot_payload(u.snapshot()) for u in upstreams.values()],
        }

    @app.api_route("/{upstream_name}", methods=_PROXIED_METHODS)
    @app.api_route("/{upstream_name}/{path:path}", methods=_PROXIED_METHODS)
    async def proxy(upstream_name: str, request: Request, path: str = "") -> Response:
        """Route, establish the deadline, forward under the guard, and map any refusal.

        Both routes point here because `/{upstream_name}/{path:path}` does not match `/search` — the
        literal slash is required — and an upstream whose root is the useful endpoint is entirely
        normal. Two decorators are cheaper than a redirect, which would spend a whole round trip to
        teach the client something we already knew.
        """
        upstream = upstreams.get(upstream_name)
        if upstream is None:
            # 404 rather than 502: nothing was wrong with any dependency, and there is no such
            # route. The reason header is included so that "client asked for a path we do not serve"
            # is distinguishable in a dashboard from a 404 the *dependency* returned, which is a
            # completely different investigation.
            return JSONResponse(
                {"error": "unknown-upstream"},
                status_code=404,
                headers={REFUSAL_HEADER: "unknown-upstream"},
            )

        # The deadline is established from the request's own headers before anything else happens,
        # so that every subsequent step — including reading the body — is inside the budget the
        # client actually granted.
        deadline = deadline_from_headers(
            request.headers,
            clock=the_clock,
            default_ms=config.default_budget_ms,
            max_ms=config.max_budget_ms,
        )
        body = await request.body()

        try:
            response = await upstream.forward(
                deadline,
                method=request.method,
                path=path,
                query=request.url.query,
                headers=headers_for_upstream(_decoded(request.headers.raw)),
                body=body,
            )
        except Exception as exc:
            # `Exception`, not `BaseException`: a `CancelledError` is the client having gone away,
            # and there is nobody left to send a status code to. Letting it propagate is what allows
            # the server to abandon the request instead of carefully composing a 504 for a closed
            # socket.
            refusal = refusal_for(exc)
            if refusal is None:
                # Not one of ours, so it is a bug in this gateway. Re-raised so it becomes a 500
                # with a traceback, which is the only way anybody will find out about it. Dressing
                # it up as a 502 would file our own defect under "the dependency is flaky"
                # indefinitely.
                raise
            return JSONResponse(
                refusal.body(),
                status_code=refusal.status,
                headers=refusal.headers(),
            )

        return _proxied_response(response)

    return app


def _decoded(raw: Sequence[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
    """Raw header bytes as `latin-1` text, for filtering. Lossless; see the module docstring."""
    return [(name.decode("latin-1"), value.decode("latin-1")) for name, value in raw]


def _proxied_response(response: httpx.Response) -> Response:
    """Turn a dependency's response into ours, preserving repeated headers.

    Built by assigning `raw_headers` rather than by passing `headers=` to `Response`, because that
    parameter takes a `Mapping` and a mapping cannot hold `Set-Cookie` twice. A login response that
    sets a session cookie and a CSRF cookie would lose one of them, intermittently depending on
    order, and the symptom — "login succeeds but the next request is unauthenticated" — points
    nowhere near the proxy. Repeated headers are legal, common, and load-bearing.
    """
    content = response.content
    pairs = headers_for_client(_decoded(response.headers.raw))
    raw = [(name.encode("latin-1"), value.encode("latin-1")) for name, value in pairs]

    # `Content-Length` was stripped by the filter because this hop may have changed the body's
    # length — httpx decompresses transparently — so the correct value is the length of what we are
    # about to write, computed here from the bytes themselves. Omitted entirely for statuses that
    # must not carry a body; sending `Content-Length: 0` on a 204 is a spec violation that most
    # clients forgive and some intermediaries do not.
    if response.status_code not in _BODILESS_STATUSES:
        raw.append((b"content-length", str(len(content)).encode("latin-1")))

    proxied = Response(content=content, status_code=response.status_code)
    proxied.raw_headers = raw
    return proxied


def _snapshot_payload(snapshot: UpstreamSnapshot) -> dict[str, Any]:
    """Serialise one upstream's state as JSON.

    Written out by hand for the breaker rather than run through `dataclasses.asdict`, for two
    reasons: the state is an `Enum` and would not serialise, and — more to the point — these key
    names are a contract. Something is graphing them. Deriving them automatically from field names
    means a rename inside the breaker silently breaks a dashboard, with the change that did it
    looking purely internal.

    The bulkhead snapshot IS run through `asdict`, because every one of its fields is already a
    number with a name chosen for exactly this purpose, and duplicating fourteen of them here would
    guarantee that one gets missed when a fifteenth is added.
    """
    return {
        "name": snapshot.name,
        "breaker": _breaker_payload(snapshot.breaker),
        "bulkhead": dataclasses.asdict(snapshot.bulkhead),
    }


def _breaker_payload(snapshot: BreakerSnapshot) -> dict[str, Any]:
    return {
        "name": snapshot.name,
        "state": snapshot.state.value,
        "successes": snapshot.counts.successes,
        "failures": snapshot.counts.failures,
        "failure_rate": snapshot.counts.failure_rate,
        "retry_after_s": snapshot.retry_after_s,
        "trials_in_flight": snapshot.trials_in_flight,
        "trial_successes": snapshot.trial_successes,
        "rejected_calls": snapshot.rejected_calls,
        "stale_outcomes": snapshot.stale_outcomes,
        # Both of these are meant to be zero forever, which is what makes them worth publishing: a
        # non-zero value is a bug in this project rather than a problem with a dependency, and a
        # dashboard is where somebody will notice. Hiding them because they *should* be zero is how
        # a counter designed to be alerted on ends up being read by nobody.
        "unresolved_permits": snapshot.unresolved_permits,
        "trips": snapshot.trips,
    }


__all__ = ["create_app"]
