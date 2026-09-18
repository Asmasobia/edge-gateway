"""Tests for hop-by-hop header filtering, in both directions.

Every test here corresponds to a bug a naive `headers = incoming.headers` produces, and each
docstring names the bug rather than restating the assertion. That is the useful thing to record: the
rule is one line of code and three paragraphs of reason, and it is the reason that gets lost.
"""

from __future__ import annotations

import pytest

from edge_gateway.gateway import (
    HOP_BY_HOP_HEADERS,
    headers_for_client,
    headers_for_upstream,
)


def names(pairs: list[tuple[str, str]]) -> list[str]:
    return [name.lower() for name, _ in pairs]


# ---------------------------------------------------------------------------
# Hop-by-hop, both directions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("header", sorted(HOP_BY_HOP_HEADERS))
def test_every_hop_by_hop_header_is_dropped_in_both_directions(header: str) -> None:
    """Parametrised over the set itself, so adding a name to it cannot skip being tested.

    The alternative — a hand-written test per header — leaves the newest entry, which is the one
    most likely to be wrong, as the only one nothing covers.
    """
    incoming = [(header, "whatever"), ("accept", "application/json")]

    assert names(headers_for_upstream(incoming)) == ["accept"]
    assert names(headers_for_client(incoming)) == ["accept"]


def test_connection_close_does_not_reach_the_dependency() -> None:
    """The one whose damage is pure latency, so nothing ever errors and nobody investigates.

    Forwarded, it tears down a pooled connection after every request: a keep-alive pool becomes a
    fresh TCP and TLS handshake per call, which on TLS is usually more time than the request itself.
    The gateway is then slower than no gateway, with a pool's memory cost and no error anywhere.
    """
    assert headers_for_upstream([("connection", "close")]) == []


def test_a_header_named_in_connection_is_also_dropped() -> None:
    """RFC 9110 §7.6.1 makes the hop-by-hop set extensible per connection.

    A peer that says `Connection: close, X-Internal-Trace` is declaring `X-Internal-Trace` to be
    connection-scoped, and forwarding it leaks a header the origin has no business seeing. Honouring
    this costs one `split`, and skipping it is the sort of omission that is only found by an origin
    behaving strangely for one client.
    """
    incoming = [
        ("connection", "close, X-Internal-Trace"),
        ("x-internal-trace", "abc123"),
        ("authorization", "Bearer token"),
    ]

    assert names(headers_for_upstream(incoming)) == ["authorization"]


def test_a_malformed_connection_header_is_tolerated_rather_than_fatal() -> None:
    """Empty entries and stray whitespace are skipped, not raised on.

    A peer's malformed `Connection` is a reason to forward one header too few. Failing the request
    instead would turn their minor protocol slip into our outage, which inverts the entire point of
    a resilience gateway.
    """
    incoming = [("Connection", " , close ,, TE , "), ("te", "trailers"), ("accept", "*/*")]

    assert names(headers_for_upstream(incoming)) == ["accept"]


def test_matching_is_case_insensitive() -> None:
    """HTTP field names are case-insensitive, so a capitalised hop-by-hop header must still go.

    Worth an explicit test because the implementation lower-cases at comparison time rather than
    normalising the stored pairs, and that is exactly the shape of code where one comparison gets
    written without the `.lower()`.
    """
    incoming = [("Transfer-Encoding", "chunked"), ("KEEP-ALIVE", "timeout=5")]

    assert headers_for_upstream(incoming) == []
    assert headers_for_client(incoming) == []


# ---------------------------------------------------------------------------
# The three that are not hop-by-hop and are dropped anyway
# ---------------------------------------------------------------------------


def test_host_is_not_forwarded_to_the_dependency() -> None:
    """It names the gateway, not the dependency, and a virtual-hosted origin routes by it.

    Forwarded, the origin serves the wrong site — or, on a shared host, somebody else's site
    entirely. The outbound client sets the correct `Host` from its own base URL, so the fix is to
    remove ours and say nothing.
    """
    assert names(headers_for_upstream([("host", "gateway.example")])) == []


def test_host_survives_on_the_way_back_because_a_response_has_no_host() -> None:
    """A deliberately asymmetric rule, stated as a test so it is not read as an oversight.

    `Host` is a request header. If one appears on a response the origin is doing something unusual,
    and a proxy silently deleting unusual-but-harmless response headers is a proxy that eventually
    deletes one that mattered.
    """
    assert names(headers_for_client([("host", "search.internal")])) == ["host"]


def test_content_length_is_dropped_in_both_directions() -> None:
    """This hop buffers and may change the length, so the value must match the bytes we write.

    A stale length is short — the peer truncates the body and the message is silently corrupt — or
    long, and the peer blocks waiting for bytes that will never arrive until its own read timeout
    fires. Both clients compute it correctly from the bytes they are handed, so the only safe value
    is no value.
    """
    assert names(headers_for_upstream([("content-length", "42")])) == []
    assert names(headers_for_client([("content-length", "42")])) == []


def test_content_encoding_is_dropped_inbound_but_kept_outbound() -> None:
    """The subtlest rule in the module, and the asymmetry is the whole content of it.

    Inbound: `httpx` transparently decompresses, so `response.content` is plain bytes even when the
    origin sent gzip. Forwarding `content-encoding: gzip` beside a body that is no longer gzipped
    tells the client to inflate plaintext, and the decode error it raises looks like the origin's
    fault.

    Outbound: nothing decompresses the *request* body — it is forwarded byte for byte — so the
    header still describes it accurately and removing it would break an upload the origin could
    otherwise have read.
    """
    assert names(headers_for_client([("content-encoding", "gzip")])) == []
    assert names(headers_for_upstream([("content-encoding", "gzip")])) == ["content-encoding"]


def test_the_incoming_budget_header_is_not_forwarded_verbatim() -> None:
    """Forwarding it would un-spend the budget, which defeats slice 1 entirely.

    The client's header says how long the client will wait. Passed straight through, the dependency
    is told it has all of that time — as though the queue wait, the body read and our own work had
    cost nothing — so every hop in a chain grants itself a fresh full budget and the deadline stops
    shrinking. `Upstream.forward` adds the recomputed value after this filter runs, which is also
    why a client cannot smuggle one past us.
    """
    incoming = [("x-timeout-budget-ms", "5000"), ("accept", "*/*")]

    assert names(headers_for_upstream(incoming)) == ["accept"]


# ---------------------------------------------------------------------------
# What must survive
# ---------------------------------------------------------------------------


def test_end_to_end_headers_are_forwarded_untouched() -> None:
    """The filter is a deny-list, and this is the test that keeps it one.

    A proxy that forwards only headers it recognises breaks the next application header somebody
    adds, and breaks it silently: the request succeeds and behaves as though the header were absent,
    which is far harder to diagnose than a rejection. Authentication, tracing and content
    negotiation are all end-to-end and none of them are a gateway's business.
    """
    incoming = [
        ("authorization", "Bearer token"),
        ("accept", "application/json"),
        ("content-type", "application/json"),
        ("user-agent", "curl/8.0"),
        ("traceparent", "00-abc-def-01"),
        ("x-some-future-thing", "value"),
    ]

    assert headers_for_upstream(incoming) == incoming


def test_repeated_headers_are_preserved() -> None:
    """`Set-Cookie` is repeated, not comma-joined, and comma-joining it is invalid.

    A dict-based filter keeps the last occurrence, so a login response setting a session cookie and
    a CSRF cookie loses one of them — intermittently, depending on order — and the symptom, "login
    succeeds but the next request is unauthenticated", points nowhere near the proxy. The
    list-of-pairs signature exists for this one case and it earns it.
    """
    incoming = [("set-cookie", "session=abc"), ("set-cookie", "csrf=xyz")]

    assert headers_for_client(incoming) == incoming


def test_order_is_preserved() -> None:
    """Ordering is semantic for repeated fields, and free to preserve.

    Comma-separated list headers such as `Accept` and `Via` mean something in order, and reordering
    `Via` misrepresents the path a message took. Nothing here needs to reorder, so nothing does.
    """
    incoming = [("a", "1"), ("connection", "close"), ("b", "2"), ("c", "3")]

    assert names(headers_for_upstream(incoming)) == ["a", "b", "c"]


def test_an_empty_header_list_is_not_a_special_case() -> None:
    """A request genuinely can arrive with nothing forwardable, and it must not crash.

    Cheap to assert, and the sort of thing an implementation that reached for `headers[0]` anywhere
    would fail on.
    """
    assert headers_for_upstream([]) == []
    assert headers_for_client([]) == []
