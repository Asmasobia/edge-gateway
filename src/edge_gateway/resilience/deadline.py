"""Timeout budgets: one deadline for a whole request, propagated across hops.

THE PROBLEM THIS SOLVES
--------------------------------------------------------------------------------
Almost every service configures timeouts *per call*: "the call to the pricing service times out
after 1 second". That is a local statement, and it composes badly. Consider a client with a 1 s
expectation calling a gateway that calls A, which calls B, then C, each hop set to "1 second":

    client(1s) -> gateway(1s) -> A(1s) -> B(1s) -> C(1s)

The worst case is not 1 second, it is 4 — each hop restarts its own stopwatch. The client gave up
after 1 s and disconnected; A, B and C are still working, holding a connection, a thread and a
database transaction each, on behalf of nobody. Under load this is the mechanism by which a slow
dependency takes down healthy services: they spend all their capacity computing answers that will
never be read. Retries make it multiplicative rather than additive.

A *budget* (equivalently, a deadline) fixes this by making the time limit a property of the REQUEST
rather than of each call. The client's 1 s is decided once at the edge and then spent down:

    client(1s) -> gateway: 1000 ms left -> A: 940 ms left -> B: 880 ms left -> C: 820 ms left

Now the whole tree finishes or is abandoned within the original 1 s, and — the more valuable half —
every hop *knows how much time it has*, so it can decline work it cannot possibly finish.

This is not a novel idea: it is what gRPC deadlines are, and the mechanism below is deliberately
modelled on `grpc-timeout`. Reimplementing it here rather than importing a library is the point of
the project — a config file that sets `timeout: 1s` demonstrates nothing about understanding
deadlines.

DEADLINE, NOT TIMEOUT
--------------------------------------------------------------------------------
The distinction is the whole design. A *timeout* is a duration and it is relative to whenever you
happen to start counting, which is why it resets at every hop. A *deadline* is a point in time; it
is absolute, so passing it down cannot extend it. Internally this class stores `expires_at`, an
absolute monotonic instant, and every duration is derived from it — no field holds "the timeout".

WHY THE WIRE FORMAT IS A DURATION EVEN THOUGH THE MODEL IS ABSOLUTE
--------------------------------------------------------------------------------
An absolute deadline cannot be serialised as-is. Two independent reasons:

 1. `time.monotonic()` has a per-process arbitrary origin (see clock.py), so the number is literally
    meaningless in another process — even on the same machine.
 2. Using wall-clock time instead would make correctness depend on clock synchronisation between
    hosts. Skew of a few hundred milliseconds is entirely normal, and it fails in the dangerous
    direction: a receiver whose clock is ahead sees every arriving deadline as already expired and
    rejects all traffic. You would have built a distributed system that stops working when NTP
    drifts.

So the wire carries REMAINING MILLISECONDS, and each receiver immediately converts it back to an
absolute local deadline against its own monotonic clock. The cost is that network transit time is
not charged to the budget (the sender computes "940 ms left", and the 15 ms of transit is spent
before the receiver starts counting). That is a real, bounded inaccuracy, and it is the same trade
gRPC makes. The `reserve_ms` mechanism below is what pays for it.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum

from edge_gateway.resilience.clock import Clock

# One nanosecond, expressed in milliseconds. Added before flooring in `remaining_ms`.
#
# WHY THIS EXISTS — a bug this project actually had, found by a test asserting an end-to-end
# property.
#
# Deadlines are held as float seconds, so `expires_at - now` carries the usual binary representation
# error. `0.98 - 0.06` evaluates to 0.9199999999999999, not 0.92. Multiplied by 1000 that is
# 919.9999999999999, and flooring it yields 919 — so a hop that genuinely had 920 ms reported 919.
#
# The error itself is around 1e-16 seconds, which is physically meaningless. The problem is that
# FLOORING AMPLIFIES IT: an error of one part in 10^16 becomes an error of a whole millisecond, and
# it recurs at every hop, always in the same direction. A four-hop chain silently loses several
# milliseconds of budget, and the loss grows with chain length.
#
# The fix is to strip sub-nanosecond noise before flooring. A nanosecond of over-statement cannot
# matter — nothing in a network stack, an event loop or a syscall resolves anywhere near that fine —
# whereas the millisecond it prevents losing is a real quantity that appears in real budgets.
#
# The general lesson, which is the same one as rounding money: flooring or truncating a float is
# only safe when the float is exact. When it is the result of arithmetic, decide explicitly what
# counts as noise, and remove it before the operation that magnifies it.
_FLOOR_EPSILON_MS = 1e-6

# Lower-case by convention: HTTP/2 and HTTP/3 require lower-case field names on the wire, and
# HTTP/1.1 field names are case-insensitive, so lower-case is the only spelling that is correct
# everywhere.
TIMEOUT_BUDGET_HEADER = "x-timeout-budget-ms"

# Strict, rather than `int(raw)`.
#
# Python's `int()` is far more permissive than an HTTP header should be. Verified on CPython 3.12:
#     int("100_000")  -> 100000     (PEP 515 underscores)
#     int("+100")     -> 100        (leading sign)
#     int(" 100 ")    -> 100        (surrounding whitespace)
#     int("١٢٣") -> 123   (Arabic-Indic digits; likewise Devanagari, and others)
#
# None of those are things a peer should be able to send and have honoured. The last one is the
# genuinely surprising case: `int()` accepts any Unicode character with the decimal-digit property,
# so without this regex the budget header would have a parsing grammar nobody documented and nobody
# intended — and a value that round-trips through logs as a different string than it parsed as. Wire
# formats should be boring and explicit; the language's convenience constructor is not a spec.
_DIGITS_ONLY = re.compile(r"[0-9]+")


class BudgetSource(Enum):
    """Where the effective budget came from.

    Returned alongside the deadline rather than discarded, because the four cases need different
    operational responses and a bare number cannot tell them apart:

    - INHERITED  the normal path: an upstream set a budget and we are spending it down.
    - DEFAULTED  no budget header at all. Expected at the true edge (a browser cannot send one), but
                 *unexpected* on an internal hop — and if internal traffic is silently DEFAULTED
                 then propagation is broken somewhere and the whole scheme is quietly not working.
                 That is invisible unless this distinction is recorded, which is exactly why it is
                 here.
    - CLAMPED    the request asked for more than the ceiling allows. Worth a metric: a caller that
                 is regularly clamped either has an unrealistic expectation or is trying it on.
    - MALFORMED  a syntactically invalid value. Always someone's bug; someone should be told.
    """

    INHERITED = "inherited"
    DEFAULTED = "defaulted"
    CLAMPED = "clamped"
    MALFORMED = "malformed"


@dataclass(frozen=True, slots=True)
class Deadline:
    """An absolute instant by which a request must be finished, plus how we arrived at it.

    Frozen on purpose. A deadline is a fact about a request, and code that can quietly extend one is
    code that can defeat the entire mechanism — the failure mode being a handler that "just needs a
    bit longer" and pushes the instant back, so the client's guarantee silently stops holding.
    Deriving a *new* deadline for a downstream hop is supported (`downstream_budget_ms`); mutating
    this one is not.

    `slots=True` because one of these is constructed per request per hop; it is the hottest
    allocation in the gateway and slots removes the per-instance `__dict__`.
    """

    expires_at: float
    """Absolute monotonic instant, in seconds, from `clock`'s arbitrary origin."""

    clock: Clock
    """Carried with the deadline so `remaining_ms()` needs no ambient global time source."""

    source: BudgetSource

    granted_ms: int
    """The budget this hop was actually granted, after clamping. Kept for logging: "we were given
    200 ms and used 340" is a diagnosable statement, whereas a bare expiry instant is not."""

    @classmethod
    def after_ms(cls, budget_ms: int, *, clock: Clock, source: BudgetSource) -> Deadline:
        """Build a deadline `budget_ms` from now.

        A non-positive budget is allowed through and produces an already-expired deadline rather
        than an error. That is deliberate: "you have no time left" is a legitimate and useful thing
        for an upstream to say — it means the work is already pointless — and the correct response
        is to refuse the request immediately, which an expired deadline achieves naturally.
        Rejecting it as invalid and substituting a default would *lengthen* a budget the caller
        explicitly set to nothing, which breaks the one guarantee this module exists to provide.
        """
        return cls(
            expires_at=clock.now() + (budget_ms / 1000.0),
            clock=clock,
            source=source,
            granted_ms=budget_ms,
        )

    def remaining_s(self) -> float:
        """Seconds left. Negative once expired — the magnitude is how far over we are, which is
        worth logging, so it is not clamped here."""
        return self.expires_at - self.clock.now()

    def remaining_ms(self) -> int:
        """Milliseconds left, floored — never rounded up.

        Flooring rather than rounding, so the number cannot over-state the time available. Rounding
        0.6 ms up to 1 ms would hand a downstream hop a budget that has already elapsed, which turns
        a clean local refusal into a remote timeout — strictly worse, since the remote timeout costs
        a connection and pollutes that dependency's error rate.

        `math.floor` rather than `int()`, and with `_FLOOR_EPSILON_MS` added first. Both details
        matter:

        - The epsilon removes float representation noise that flooring would otherwise amplify into
          a whole lost millisecond at every hop. See the constant's definition — this was a live
          bug.
        - `int()` truncates toward zero, so for an overrun it would report -249 where the true value
          is -249.9999. `math.floor` gives -250, which is the honest (and more conservative)
          magnitude. Negative values are diagnostic only, but a diagnostic that flatters you is
          worse than none.
        """
        return math.floor(self.remaining_s() * 1000.0 + _FLOOR_EPSILON_MS)

    def expired(self) -> bool:
        """True once there is no time left.

        `<= 0` rather than `< 0`: at exactly zero there is no time in which to do anything, and the
        boundary should fail closed. This is also why `after_ms(0, ...)` is immediately expired.
        """
        return self.remaining_s() <= 0

    def downstream_budget_ms(self, *, reserve_ms: int) -> int:
        """The budget to hand to the next hop: what is left, minus a reserve, floored at zero.

        WHY A RESERVE: the remaining budget cannot all be given away. Between "downstream returned
        its answer" and "the client has the response" there is still work to do: read and
        deserialise the response, run whatever this hop exists to do with it, serialise our own
        response, and put it on the wire. Hand over 100% and the sequence that follows is downstream
        succeeding at the very last millisecond and *this* hop then blowing the client's deadline
        while formatting the reply — the worst outcome available, because the expensive work was
        done and then thrown away.

        The reserve also absorbs the transit time the wire format cannot charge for (see the module
        docstring). It is a per-hop tuning value, so it is a parameter rather than a constant here:
        a hop that only forwards bytes needs a couple of milliseconds, one that fans out to five
        dependencies and merges them needs considerably more, and a single library-level default
        would be wrong for both.

        Floored at zero because a negative budget on the wire is meaningless, and because the
        caller's decision ("is there enough time to bother?") belongs in `is_viable`, not here.
        """
        if reserve_ms < 0:
            raise ValueError(f"reserve_ms must not be negative (got {reserve_ms!r})")
        return max(0, self.remaining_ms() - reserve_ms)

    def is_viable(self, *, minimum_ms: int, reserve_ms: int) -> bool:
        """Whether calling downstream is worth attempting at all.

        This is the payoff of the whole mechanism, and the part that a per-call timeout cannot
        express. If 3 ms remain, issuing the call is not merely futile — it is actively harmful:

          - It costs a connection from the pool and a bulkhead slot (slice 3), both of which are
            capacity that a request with a real chance of succeeding could have used.
          - The downstream service does the work, and its answer is discarded because we have
            already given up. Under load this is how a struggling dependency gets pushed further
            under: its remaining capacity is spent on doomed requests.
          - The resulting timeout is recorded as a *failure of that dependency* and counts toward
            its circuit breaker (slice 2). So a slow client, or a caller that set a stingy budget,
            can trip a breaker and cause traffic from unrelated callers to be shed. The dependency
            was healthy the entire time.

        Declining locally instead costs nothing and is attributable to the right cause. Slice 4
        makes that refusal a distinct outcome that explicitly does NOT count against the breaker.
        """
        if minimum_ms < 0:
            raise ValueError(f"minimum_ms must not be negative (got {minimum_ms!r})")
        return self.downstream_budget_ms(reserve_ms=reserve_ms) >= minimum_ms


def deadline_from_headers(
    headers: Mapping[str, str],
    *,
    clock: Clock,
    default_ms: int,
    max_ms: int,
) -> Deadline:
    """Derive this hop's deadline from an incoming request's headers.

    `default_ms` and `max_ms` are keyword-only and REQUIRED — no library-level defaults. A default
    timeout is a product decision about how long a user will wait, and it differs per endpoint;
    baking one into a shared module is how every service in a fleet ends up with the same accidental
    value that nobody chose and nobody can justify. Forcing them in from configuration keeps the
    decision where it can be reviewed.

    `max_ms` is a trust boundary, not a tuning knob. The budget arrives in a header, so at the edge
    it is attacker-controlled: without a ceiling, `x-timeout-budget-ms: 86400000` pins a worker, a
    connection and a bulkhead slot for a day, and a handful of such requests are a denial of service
    that costs the sender nothing. Clamping is the fix, and recording it as CLAMPED is what makes it
    visible rather than silent.

    Malformed values fall back to the default rather than rejecting the request with a 400. The
    reasoning: the budget is an optimisation hint from a peer, and failing a request the service
    could otherwise have served turns one caller's header bug into a user-visible outage. It is
    reported via `BudgetSource.MALFORMED` instead, so it is loud in metrics and harmless in
    production. This cuts the other way at a strict internal boundary — there, refusing is
    defensible — which is precisely why the decision is recorded in the return value rather than
    hidden.
    """
    if default_ms <= 0:
        raise ValueError(f"default_ms must be positive (got {default_ms!r})")
    if max_ms < default_ms:
        # A ceiling below the floor would silently clamp every single request, including the ones
        # with no header at all, and the symptom would be "everything times out" with no obvious
        # cause.
        raise ValueError(f"max_ms ({max_ms}) must be >= default_ms ({default_ms})")

    raw = _lookup_header(headers, TIMEOUT_BUDGET_HEADER)

    if raw is None:
        return Deadline.after_ms(default_ms, clock=clock, source=BudgetSource.DEFAULTED)

    if not _DIGITS_ONLY.fullmatch(raw):
        return Deadline.after_ms(default_ms, clock=clock, source=BudgetSource.MALFORMED)

    budget_ms = int(raw)

    if budget_ms > max_ms:
        return Deadline.after_ms(max_ms, clock=clock, source=BudgetSource.CLAMPED)

    return Deadline.after_ms(budget_ms, clock=clock, source=BudgetSource.INHERITED)


def headers_for_downstream(deadline: Deadline, *, reserve_ms: int) -> dict[str, str]:
    """The headers to add when calling the next hop.

    Returns a fresh dict for the caller to merge, rather than mutating a passed-in mapping. Mutating
    the incoming request's headers in place is a habit that eventually leaks one request's deadline
    into another when a client library reuses a header container across calls — a bug that presents
    as "some requests mysteriously get a tiny budget" and is close to untraceable.

    Note this happily emits `0`. Emitting the truth and letting the callee refuse is better than
    omitting the header, because an absent header means DEFAULTED — the callee would grant itself a
    fresh full budget and the chain would silently lose its deadline at exactly the moment it
    matters most. Callers should normally check `is_viable` first and not make the call at all.
    """
    return {TIMEOUT_BUDGET_HEADER: str(deadline.downstream_budget_ms(reserve_ms=reserve_ms))}


def _lookup_header(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup.

    HTTP/1.1 field names are case-insensitive, so `X-Timeout-Budget-Ms` and `x-timeout-budget-ms`
    are the same header and both must work. Most frameworks hand over a case-insensitive mapping
    already, but this module accepts a plain `Mapping` so it can be tested and reused without one —
    and relying on the caller's container to be case-insensitive is the kind of assumption that
    holds until somebody passes a dict, at which point deadline propagation silently stops working
    for one client.

    The fast path is a direct hit on the already-lower-case name; the scan is the fallback.
    """
    if name in headers:
        return headers[name]
    for key, value in headers.items():
        if key.lower() == name:
            return value
    return None
