"""The declarative description of what this gateway fronts, validated once at startup.

WHY CONFIGURATION IS A FROZEN DATACLASS AND NOT A DICT
--------------------------------------------------------------------------------
Every number in here is a policy decision that somebody has to be able to defend during an incident,
and the two failure modes of configuration are both addressed by the same choice:

  * **A typo in a key name.** In a dict, `max_queue` misspelt as `max_quue` is silently ignored and
    the queue quietly takes its default. In a dataclass it is a `TypeError` at import time, before
    the process ever accepts a request.
  * **Drift at runtime.** Frozen, so a request handler cannot reach into the config and change a
    threshold. A breaker whose threshold changed halfway through a window is a breaker whose window
    means nothing, and the symptom — "it opened at 40% once" — is unreproducible.

WHY THERE ARE NO DEFAULTS FOR THE POLICY NUMBERS
--------------------------------------------------------------------------------
Same argument as ADR-007 and `BreakerConfig`, applied one level up. `max_concurrent`, `max_queue`,
`reserve_ms`, `minimum_call_ms` and the budgets are all required. A default here would be a number
that applies to every dependency this gateway fronts, and no such number exists: a payments provider
and a recommendations service differ by an order of magnitude in every one of them. A default is how
an entire fleet inherits a value nobody chose and then discovers it at 3am.

`bucket_count` inside `BreakerConfig` remains the one principled exception, for the reason given
there: it is an accuracy-versus-memory trade inside the window, not a statement about a dependency.

WHY VALIDATION HAPPENS HERE AND NOT AT FIRST USE
--------------------------------------------------------------------------------
A gateway that starts successfully and then refuses every request to one upstream because its
`base_url` was missing a scheme is much worse than one that refuses to start. The first looks like
an outage in the dependency and gets investigated as one; the second prints a line naming the field.
Rolling deployments make this concrete: a config error caught at startup fails the new pods' health
checks and the rollout stops with the old version still serving.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from edge_gateway.resilience import BreakerConfig

# Path segments that name an operational endpoint rather than an upstream. An upstream called
# `healthz` would be permanently unreachable, because Starlette matches routes in declaration order
# and the operational routes are declared first (see `app.py`). Shadowing like that is exactly the
# kind of thing that gets diagnosed as a networking problem for a day, so it is rejected by name.
RESERVED_PREFIXES = frozenset({"healthz", "resilience"})


@dataclass(frozen=True, slots=True)
class UpstreamConfig:
    """One dependency: where it is, how much of us it may occupy, and when to give up on it.

    The bulkhead and breaker settings sit together deliberately. They are read together during an
    incident — "how many can be in flight, and what does it take to open the circuit" is a single
    question — and splitting them across two files is how the two end up describing different
    intentions.
    """

    name: str
    """The first path segment that routes to this upstream, and the label on every metric about it.

    One string doing both jobs is a deliberate simplification: it makes "which breaker did this
    request use?" answerable from the URL in an access log, with no lookup table. The cost is that
    renaming an upstream is a client-visible change.
    """

    base_url: str
    """Scheme, host and optional port. The matched path is appended to it.

    Required to carry a scheme, because `httpx` treats a schemeless URL as relative and would
    silently produce a request to a path on the gateway itself — a proxy loop that presents as
    mysterious recursion rather than as a configuration error.
    """

    breaker: BreakerConfig

    max_concurrent: int
    """Outbound calls allowed to be in flight at once. Also caps the HTTP connection pool.

    See ADR-039 on why these two numbers are locked together: a connection pool smaller than the
    bulkhead is a second queue that the bulkhead cannot see, does not bound, and does not report.
    """

    max_queue: int
    """How many may wait for a slot. Zero means shed immediately.

    Sizing it is the `Q * L / C` arithmetic from `bulkhead.py`: with a queue of Q, a latency of L
    and C concurrent slots, the last caller in the queue waits about `Q * L / C` before it even
    starts. If that exceeds a typical budget the queue is doing nothing but adding latency to calls
    that will be refused anyway.
    """

    reserve_ms: int
    """Time withheld from the outbound budget to pay for our own work on the way back.

    Reading the response, mapping it, writing our reply. Withheld from what we send downstream AND
    from our own patience — see the `guard.py` docstring on why honouring it in only one place makes
    it a comment rather than a reserve.
    """

    minimum_call_ms: int
    """Below this much remaining budget, refuse rather than attempt.

    Should be at least a plausible connect time. Set it to zero and the gateway will happily open a
    connection with 2ms left, do the dependency's work for it, and abandon the answer.
    """

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("upstream name must not be empty")
        if "/" in self.name:
            raise ValueError(
                f"upstream name {self.name!r} must be a single path segment with no '/': it is "
                f"matched against exactly one segment of the request path"
            )
        if self.name in RESERVED_PREFIXES:
            raise ValueError(
                f"upstream name {self.name!r} is reserved for an operational endpoint and would be "
                f"unreachable; the reserved names are {sorted(RESERVED_PREFIXES)}"
            )
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError(
                f"base_url {self.base_url!r} for upstream {self.name!r} must start with http:// or "
                f"https://; a schemeless URL is treated as relative and would proxy to ourselves"
            )
        if self.max_concurrent < 1:
            raise ValueError(
                f"max_concurrent for {self.name!r} must be at least 1 "
                f"(got {self.max_concurrent!r}); zero would take the dependency permanently "
                f"offline while reporting it as full"
            )
        if self.max_queue < 0:
            raise ValueError(f"max_queue for {self.name!r} must not be negative")
        if self.reserve_ms < 0:
            raise ValueError(f"reserve_ms for {self.name!r} must not be negative")
        if self.minimum_call_ms < 0:
            raise ValueError(f"minimum_call_ms for {self.name!r} must not be negative")


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    """The whole gateway: its upstreams, and the budget policy for requests that arrive without one.

    `default_budget_ms` and `max_budget_ms` live here rather than per-upstream because they describe
    the *incoming* request, which is routed to an upstream only after its deadline has been
    established. Deriving the deadline after routing would mean the budget depended on the path,
    which is a surprising thing for a timeout to depend on and an easy thing to get wrong when a
    route is added.
    """

    upstreams: tuple[UpstreamConfig, ...]
    default_budget_ms: int
    """Budget granted when the request carries no `x-timeout-budget-ms`.

    Expected at the true edge — a browser cannot send the header — and a signal worth watching on an
    internal hop, where DEFAULTED means propagation is broken somewhere upstream and the whole
    scheme has quietly stopped working. `BudgetSource` exists to make that visible; see
    `deadline.py`.
    """

    max_budget_ms: int
    """Ceiling on any budget, however large a value the header asks for.

    A trust boundary rather than a tuning knob. The budget arrives in a header, so at the edge it is
    attacker-controlled: without a ceiling, `x-timeout-budget-ms: 86400000` pins a worker, a
    connection and a bulkhead slot for a day, and a few dozen such requests are a denial of service
    that costs the sender nothing.
    """

    by_name: dict[str, UpstreamConfig] = field(init=False, repr=False)
    """Routing index, built once. Not part of the constructor.

    Built here rather than in `create_app` so that the duplicate-name check happens for anyone who
    constructs a config, including a test that never builds an app. Two upstreams with the same name
    is the sort of mistake a dict comprehension silently resolves in favour of the last one, taking
    the first one's breaker settings out of service with no error anywhere.
    """

    def __post_init__(self) -> None:
        if not self.upstreams:
            raise ValueError(
                "a gateway with no upstreams would answer 404 to everything; if that is genuinely "
                "wanted, it is a static file server and not this"
            )
        if self.default_budget_ms < 1:
            raise ValueError(
                f"default_budget_ms must be positive (got {self.default_budget_ms!r}); zero would "
                f"make every request without the header refuse immediately"
            )
        if self.max_budget_ms < self.default_budget_ms:
            raise ValueError(
                f"max_budget_ms ({self.max_budget_ms}) must be at least default_budget_ms "
                f"({self.default_budget_ms}); otherwise the ceiling silently *shortens* the budget "
                f"of every request that does not ask for one, which is a default nobody chose"
            )

        seen: dict[str, UpstreamConfig] = {}
        for upstream in self.upstreams:
            if upstream.name in seen:
                raise ValueError(
                    f"duplicate upstream name {upstream.name!r}: routing is by name, so one of the "
                    f"two would be unreachable and its breaker settings would never apply"
                )
            seen[upstream.name] = upstream

        # `object.__setattr__` because the dataclass is frozen. This is the sanctioned way to set a
        # derived field in `__post_init__` and is preferable to dropping `frozen`, which would give
        # up immutability everywhere to initialise one cache.
        object.__setattr__(self, "by_name", seen)
