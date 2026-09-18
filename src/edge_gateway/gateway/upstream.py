"""One dependency, with everything it needs to be called safely: breaker, bulkhead, guard, client.

WHY ONE OBJECT PER DEPENDENCY AND NOT ONE PER GATEWAY
--------------------------------------------------------------------------------
This is the whole thesis of the project, restated at the level where it is easy to get wrong. Share
a breaker across two dependencies and either one's failures open the circuit on both, converting a
partial outage into a total one. Share a bulkhead and the slow dependency starves the fast one of
capacity — the exact failure the pattern exists to prevent, reintroduced by a convenience.

So the constructor takes one `UpstreamConfig` and builds one of each, and nothing in this module
offers a way to pass in a shared component. Making the wrong thing *unavailable* is more reliable
than documenting that it is wrong.

WHY THE HTTP CLIENT IS BUILT HERE AND OWNED HERE
--------------------------------------------------------------------------------
`httpx.AsyncClient` is a connection pool, and its lifetime is what makes it worth having: a client
created per request re-does DNS, TCP and TLS every time, which on a TLS connection is typically more
latency than the request itself. One long-lived client per upstream, closed by the app's lifespan.

It is built here rather than passed in because two of its settings are *derived from the bulkhead*
and must not be able to drift from it — see the comment on `limits` below. A caller who could pass a
pre-built client could pass one whose pool is smaller than the bulkhead, and the resulting hidden
queue is invisible in every metric this project exposes.

WHY THERE ARE NO RETRIES
--------------------------------------------------------------------------------
Conspicuously absent, and absent on purpose rather than unfinished.

A retry inside the guarded call would spend one deadline on two attempts while holding one bulkhead
slot for both, so the second attempt runs with whatever the first one left — usually not enough,
which makes it a call that exists only to fail. It would also feed two outcomes into the breaker for
one client request, doubling the weight of a failing dependency in its own window and making the
failure rate mean something other than what its name says.

Where a retry belongs is *outside* the breaker: attempt, and if the breaker rejects or the call
fails, decide at that level whether a second attempt is affordable within the remaining budget. That
is a real feature and it is not built here; `README.md` lists it as not built rather than implying
it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

import httpx

from edge_gateway.gateway.config import UpstreamConfig
from edge_gateway.resilience import (
    BreakerSnapshot,
    Bulkhead,
    BulkheadSnapshot,
    CircuitBreaker,
    Clock,
    Deadline,
    Guard,
    classify_status,
    headers_for_downstream,
)


class _NoCookies(httpx.Cookies):
    """A cookie jar that never remembers anything. Always empty, therefore always falsy.

    `extract_cookies` is the only method that needs overriding: it is what `httpx` calls after every
    response to store `Set-Cookie` values. Doing nothing keeps the jar empty, and an empty jar is
    falsy, which is what makes `httpx` skip injecting a `Cookie` header on the way out — so one
    override closes both halves of the problem described on `_CookielessAsyncClient`.
    """

    def extract_cookies(self, response: httpx.Response) -> None:
        return


class _CookielessAsyncClient(httpx.AsyncClient):
    """An `httpx.AsyncClient` with cookie persistence removed. Not optional; see below.

    THIS FIXES A REAL AND SERIOUS BUG, FOUND BY TESTING FOR IT
    ----------------------------------------------------------------------------
    `httpx.AsyncClient` is a *user agent*. It keeps a cookie jar, stores every `Set-Cookie` it sees,
    and replays the jar on subsequent requests to the same host. That is correct behaviour for a
    browser-like client and catastrophic for a shared proxy, because one client serves every caller:

        1. Client A logs in. The dependency answers `Set-Cookie: session=SECRET`.
        2. Our `AsyncClient` quietly stores `session=SECRET` in its jar.
        3. Client B — a different person, holding no credentials — requests anything.
        4. `httpx` attaches `Cookie: session=SECRET` to B's outbound request.

    Client B is now authenticated as client A, at the dependency, with no bug anywhere in this
    project's own code. It is one line of default behaviour in a well-behaved library, and it is the
    single worst defect this slice could have shipped.

    It was found by writing a test that logged in as one caller and then issued an unrelated request
    as another, and asserting on what the dependency *saw* — the leak is invisible from either
    client's side, so a test that only checks status codes and response bodies passes cheerfully.
    `test_gateway.py` keeps that test.

    WHY OVERRIDING THE PROPERTY, RATHER THAN CLEARING THE JAR
    ----------------------------------------------------------------------------
    `client.cookies.clear()` after each response is the obvious fix and it is a race: with
    `max_concurrent` above one, request B is built between A's response being stored and the clear
    running, and the leak reappears under exactly the concurrency this project exists to handle —
    intermittently, which is worse than reliably.

    Passing `cookies=` to the constructor does not work either: `httpx` wraps whatever it is given
    in a fresh `Cookies`, which discards a subclass. Overriding the property is what actually holds,
    because `httpx` reads `self.cookies` at both the store and the inject site.
    """

    _NULL_JAR = _NoCookies()

    @property
    def cookies(self) -> httpx.Cookies:
        return self._NULL_JAR

    @cookies.setter
    def cookies(self, cookies: Any) -> None:
        """Silently ignored, deliberately.

        Raising would be more honest in the abstract and worse in practice: `httpx` assigns to this
        in code paths we do not control, and a proxy that crashed rather than declining to remember
        a cookie would be trading a fixed leak for an outage.
        """
        return


@dataclass(frozen=True, slots=True)
class UpstreamSnapshot:
    """A consistent read of one upstream's protection, for the operational endpoint.

    The two component snapshots are kept as themselves rather than flattened into a single bag of
    numbers, because the pairing is what makes them diagnostic. `state=open` next to `in_flight=0`
    says the dependency broke and we have stopped calling it; `state=closed` next to `queued=8` says
    the dependency is fine and *we* are the bottleneck. Flatten them and that reading gets harder
    for no benefit.
    """

    name: str
    breaker: BreakerSnapshot
    bulkhead: BulkheadSnapshot


class Upstream:
    """A single dependency behind a full set of protections."""

    __slots__ = ("_breaker", "_bulkhead", "_client", "_config", "_guard")

    def __init__(
        self,
        config: UpstreamConfig,
        *,
        clock: Clock,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """`transport` is the seam that makes the HTTP layer testable without a server.

        `httpx.MockTransport` answers requests from a function, so a test exercises the real client,
        the real request construction, the real pool limits and the real header handling with no
        socket, no port and no fixture teardown. Injecting a fake *client* instead would test the
        fake; injecting the transport tests everything above it.

        Defaulting to None — the real network transport — means production wiring needs no argument
        and cannot accidentally be left pointing at a mock.
        """
        self._config = config
        self._breaker = CircuitBreaker(config.name, config=config.breaker, clock=clock)
        self._bulkhead = Bulkhead(
            config.name,
            max_concurrent=config.max_concurrent,
            max_queue=config.max_queue,
            clock=clock,
        )
        self._guard = Guard(
            config.name,
            breaker=self._breaker,
            bulkhead=self._bulkhead,
            reserve_ms=config.reserve_ms,
            minimum_call_ms=config.minimum_call_ms,
        )
        self._client = _CookielessAsyncClient(
            base_url=config.base_url,
            transport=transport,
            # `timeout=None` disables every timeout inside httpx, and that is the point rather than
            # an oversight. Two timeouts for one call is two deadlines with no relationship to each
            # other: whichever fires first decides the outcome, the winner depends on configuration
            # in two files nobody reads together, and the request's actual budget — the only number
            # that reflects what the client is still waiting for — loses. The guard owns the timeout
            # because the guard is the only thing that knows the budget.
            timeout=None,
            # Locked to the bulkhead, both of them. A pool smaller than `max_concurrent` is a SECOND
            # queue, inside httpx, that the bulkhead cannot see: calls the bulkhead admitted then
            # wait for a connection, invisibly, and that wait appears in no metric this project
            # exposes and is bounded by nothing. Equal is the only value that cannot lie — the
            # bulkhead already guarantees no more than `max_concurrent` calls are in flight, so the
            # pool can never be the binding constraint and can never queue. Larger would merely be
            # dead capacity.
            limits=httpx.Limits(
                max_connections=config.max_concurrent,
                max_keepalive_connections=config.max_concurrent,
            ),
            # A proxy must not follow redirects on the client's behalf. The client asked for *this*
            # resource, and a 301 is a real answer it may want to cache or rewrite. Following it
            # also spends one budget and one bulkhead slot on an unbounded chain of calls, and
            # records only the last one's outcome against the breaker — so a dependency stuck in a
            # redirect loop would look perfectly healthy while consuming everything.
            follow_redirects=False,
        )

    @property
    def name(self) -> str:
        return self._config.name

    async def forward(
        self,
        deadline: Deadline,
        *,
        method: str,
        path: str,
        query: str,
        headers: Sequence[tuple[str, str]],
        body: bytes,
    ) -> httpx.Response:
        """Proxy one request under the guard, returning the dependency's response.

        Raises whatever the guard raises — `CircuitOpenError`, `BulkheadFullError`,
        `SlotWaitExpiredError`, `BudgetExhaustedError`, `UpstreamTimeoutError` — plus any `httpx`
        error from the call itself. Mapping those onto status codes is `errors.py`'s job, not this
        one's: an object that both makes the call and decides its HTTP representation is an object
        that cannot be reused by anything that is not an HTTP handler.

        `headers` must already be filtered (see `headers.py`). This method adds exactly one header,
        the recomputed budget, and it adds it *after* the filter has run so that a client cannot
        suppress it by sending one of its own.
        """
        outbound: list[tuple[str, str]] = list(headers)
        outbound.extend(
            headers_for_downstream(deadline, reserve_ms=self._config.reserve_ms).items()
        )

        # Built before the guarded call rather than inside the closure, so that a URL-construction
        # bug raises here — plainly, out of any resilience bookkeeping — instead of being classified
        # as a dependency failure and counted towards opening a circuit on a service that is
        # perfectly fine.
        request = self._client.build_request(
            method,
            self._target(path, query),
            headers=outbound,
            content=body,
        )

        # The whole response, body included, is read inside the guard: `send` without `stream=True`
        # reads to completion, so the bulkhead slot is held until the last byte arrives and the
        # timeout covers the body transfer. Releasing at the headers would leave the part most able
        # to occupy a worker for a long time — a large body over a slow link — outside both limits.
        return await self._guard.call(
            deadline,
            operation=lambda: self._client.send(request),
            classify=lambda response: classify_status(response.status_code),
        )

    def _target(self, path: str, query: str) -> str:
        """The URL to request, relative to `base_url`.

        The already-encoded query string is re-attached as a string rather than parsed into pairs
        and re-encoded. Round-tripping it is lossy in ways that break real APIs: parameter order is
        lost, repeated keys may be collapsed, and `+` versus `%20` versus a literal space are
        decisions the client already made. A proxy's job is to pass the query through unchanged, not
        to normalise it.
        """
        relative = path.lstrip("/")
        return f"{relative}?{query}" if query else relative

    def snapshot(self) -> UpstreamSnapshot:
        return UpstreamSnapshot(
            name=self._config.name,
            breaker=self._breaker.snapshot(),
            bulkhead=self._bulkhead.snapshot(),
        )

    async def aclose(self) -> None:
        """Close the connection pool. Called from the app's lifespan, once, on shutdown.

        Worth doing rather than leaving to the garbage collector: an unclosed pool leaves sockets in
        the OS until the process exits, and on a repeatedly-reloaded development server that is a
        slow file-descriptor leak whose symptom — "it stops working after twenty reloads" — looks
        nothing like its cause.
        """
        await self._client.aclose()


def build_upstreams(
    configs: Iterable[UpstreamConfig],
    *,
    clock: Clock,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Upstream]:
    """Build one `Upstream` per config, keyed by name for routing.

    Duplicate names are already impossible — `GatewayConfig` rejects them — which is why there is no
    check here. Two checks for one rule is how the second one gets deleted as redundant and the
    first one turns out to have been the redundant one.
    """
    return {config.name: Upstream(config, clock=clock, transport=transport) for config in configs}
