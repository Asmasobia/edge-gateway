"""Which headers may cross a hop, in which direction, and why each exclusion exists.

WHY THIS IS NOT A ONE-LINE COPY
--------------------------------------------------------------------------------
`upstream_request.headers = incoming.headers` is the line every hand-rolled proxy starts with, and
every one of the bugs below is already present in it. They are not exotic; they are the reason
`Connection`, `Transfer-Encoding` and `Content-Length` are specified the way they are.

The concept underneath is the RFC 9110 §7.6.1 distinction:

  * **End-to-end** headers describe the *message*. `Authorization`, `Accept`, `Content-Type`,
    `User-Agent`, and anything application-specific. They are meant to reach the origin, so a proxy
    forwards them untouched.
  * **Hop-by-hop** headers describe *this TCP connection*. `Connection`, `Transfer-Encoding`,
    `Upgrade`. They are meaningful only between two adjacent parties and are meaningless — or
    actively wrong — one hop further on.

Forwarding a hop-by-hop header is not a style problem. `Connection: close` copied outbound tears
down a pooled connection after every single request, which turns a keep-alive pool into a fresh TCP
and TLS handshake per call — the same latency regression as having no pool at all, with a pool's
memory cost and no error anywhere.

THE THREE THAT ARE NOT HOP-BY-HOP AND ARE STILL DROPPED
--------------------------------------------------------------------------------
These are the ones a checklist copied from the RFC misses, and each is a real bug.

`host` (outbound). It names the gateway, not the dependency. Forwarded, a virtual-hosted origin
routes by it and serves the wrong site — or, on a shared host, somebody else's site.

`content-length` (both directions). This gateway buffers bodies, so the length it writes must be the
length of what it actually writes. A stale value from the original message is either short — the
peer truncates the body and the request is silently corrupt — or long, and the peer waits for bytes
that will never arrive until *its* read timeout fires. Both clients (`httpx` outbound, Starlette
inbound) compute it correctly from the bytes they are given, so the fix is to say nothing and let
them.

`content-encoding` (inbound only, and this is the subtle one). `httpx` transparently decompresses:
`response.content` is plain bytes even when the origin sent gzip. Forwarding `content-encoding:
gzip` alongside a body that is no longer gzipped tells the client to inflate plaintext, and it fails
with a decode error that appears to come from the origin. The header describes a transformation that
this hop has already undone, so this hop must stop advertising it.

WHY THE `Connection` HEADER'S *CONTENTS* ARE ALSO CONSULTED
--------------------------------------------------------------------------------
RFC 9110 §7.6.1 makes the set extensible per connection: any field name listed in `Connection` is
hop-by-hop for that connection. `Connection: close, X-Internal-Trace` means `X-Internal-Trace` must
not be forwarded. Ignoring this is how a peer's connection-scoped header leaks to an origin that has
no business seeing it, and honouring it costs one split.

WHY PAIRS AND NOT A DICT
--------------------------------------------------------------------------------
HTTP allows repeated field names, and one of them matters constantly: `Set-Cookie` is repeated, not
comma-joined, and comma-joining it is invalid because cookie values may contain commas. A `dict`
keeps the last occurrence, so a login response setting a session cookie and a CSRF cookie loses one
— a bug that presents as "login works but the next request is unauthenticated", intermittently,
depending on header order.
"""

from __future__ import annotations

from collections.abc import Iterable

# RFC 9110 §7.6.1, plus `keep-alive` which RFC 7230 listed and which is still emitted in the wild.
# Lower-cased because HTTP field names are case-insensitive and every comparison below is on a
# lower-cased name; storing them any other way invites a `Transfer-Encoding` that slips through
# because it was capitalised.
HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)

# Dropped outbound in addition to the hop-by-hop set. See the module docstring for each.
_DROP_TOWARDS_UPSTREAM = frozenset(
    {
        "host",
        "content-length",
        # The budget is recomputed for the next hop, never forwarded verbatim: forwarding it would
        # hand the dependency the time we had when the request *arrived*, silently un-spending
        # everything the queue and our own work consumed. `headers_for_downstream` in `deadline.py`
        # produces the replacement, and `Upstream.forward` adds it after this filter has run.
        "x-timeout-budget-ms",
    }
)

# Dropped inbound in addition to the hop-by-hop set.
_DROP_TOWARDS_CLIENT = frozenset(
    {
        "content-length",
        "content-encoding",
    }
)


def _connection_scoped(headers: Iterable[tuple[str, str]]) -> frozenset[str]:
    """Field names the peer declared hop-by-hop for this connection via `Connection`.

    Tolerant of junk by design: an empty entry or stray whitespace is skipped rather than raised on.
    A malformed `Connection` header is a reason to forward one header too few, not a reason to fail
    a request that is otherwise perfectly well formed.
    """
    scoped: set[str] = set()
    for name, value in headers:
        if name.lower() != "connection":
            continue
        for token in value.split(","):
            cleaned = token.strip().lower()
            if cleaned:
                scoped.add(cleaned)
    return frozenset(scoped)


def _filtered(
    headers: Iterable[tuple[str, str]], *, also_drop: frozenset[str]
) -> list[tuple[str, str]]:
    pairs = list(headers)
    drop = HOP_BY_HOP_HEADERS | also_drop | _connection_scoped(pairs)
    return [(name, value) for name, value in pairs if name.lower() not in drop]


def headers_for_upstream(headers: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    """Filter a client's request headers for forwarding to a dependency.

    Deliberately an allow-everything-except list rather than a deny-everything-except one, and that
    is a defensible choice in *this* direction only. A proxy that forwards only headers it
    recognises breaks the next application header somebody adds, and it breaks it silently — the
    request succeeds and behaves as though the header were absent, which is far harder to diagnose
    than a rejection. Authentication headers, tracing headers and content negotiation are all
    end-to-end, and all of them are things a gateway has no business editing.

    The opposite choice would be right at a security boundary that is filtering untrusted input into
    a trusted network. This gateway does not claim to be that, and `README.md` says so rather than
    implying a guarantee it does not provide.
    """
    return _filtered(headers, also_drop=_DROP_TOWARDS_UPSTREAM)


def headers_for_client(headers: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    """Filter a dependency's response headers for returning to the client."""
    return _filtered(headers, also_drop=_DROP_TOWARDS_CLIENT)
