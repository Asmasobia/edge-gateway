"""The HTTP surface: everything that knows what a request, a header and a status code are.

WHY THIS IS A SEPARATE PACKAGE FROM `resilience/`
--------------------------------------------------------------------------------
The split is the same one ADR-002 set up in slice 1, now with a second half to point at. Below this
line live the patterns — deadlines, breakers, bulkheads, their composition — and they import nothing
but the standard library. Above it lives the part that speaks HTTP, and it may import whatever it
needs.

The value of the line is not tidiness. It is that the interesting logic can be tested by passing
integers, and the boring logic can be tested by passing requests. Fold them together and every test
of "does 429 count as a failure" needs an HTTP client, a server and a port — at which point the
tests get slow, then get skipped, then get deleted.

The line is checked by `tests/test_layering.py`, which walks the imports of every module under
`resilience/` and fails if a third-party name appears. A rule this easy to break by accident, in a
one-line import added under time pressure, is worth thirty lines of test.

WHAT IS IN HERE
--------------------------------------------------------------------------------
    config.py    the declarative description of upstreams and budgets, validated at startup
    headers.py   which headers may cross a hop, in each direction, and why
    errors.py    the one place a resilience exception becomes a status code
    upstream.py  one dependency: its breaker, its bulkhead, its guard and its client
    app.py       routing, the proxy handler and the operational endpoints
"""

from __future__ import annotations

from edge_gateway.gateway.app import create_app
from edge_gateway.gateway.config import GatewayConfig, UpstreamConfig
from edge_gateway.gateway.errors import Refusal, refusal_for
from edge_gateway.gateway.headers import (
    HOP_BY_HOP_HEADERS,
    headers_for_client,
    headers_for_upstream,
)
from edge_gateway.gateway.upstream import Upstream, UpstreamSnapshot

__all__ = [
    "HOP_BY_HOP_HEADERS",
    "GatewayConfig",
    "Refusal",
    "Upstream",
    "UpstreamConfig",
    "UpstreamSnapshot",
    "create_app",
    "headers_for_client",
    "headers_for_upstream",
    "refusal_for",
]
