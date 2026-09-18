"""Tests for startup validation of the gateway's configuration.

WHY VALIDATION IS WORTH THIS MANY TESTS
--------------------------------------------------------------------------------
Every rejection below exists because the alternative is not a crash but a *misdiagnosis*. A gateway
that starts happily and then behaves strangely for one upstream sends an investigation to the
dependency, where there is nothing to find. That is the expensive failure mode, and it is what makes
config validation worth more tests than it looks like it deserves:

    empty / slashed name      routes that never match, or match a path segment that cannot exist
    reserved name            an upstream permanently shadowed by an operational endpoint
    schemeless base_url      a proxy loop, presenting as mysterious recursion
    max_concurrent of 0      a dependency taken offline while reported as merely full
    max_budget < default     a ceiling that silently *shortens* every unlabelled request
    duplicate names          one upstream's breaker settings never applied, with no error anywhere

Each test asserts on a fragment of the message as well as the exception type, because the message is
the entire value of failing at startup. `ValueError` in a log tells an operator to go and read the
code; "base_url 'search.internal' must start with http://" tells them what to edit.
"""

from __future__ import annotations

import pytest

from edge_gateway.gateway import GatewayConfig, UpstreamConfig
from edge_gateway.gateway.config import RESERVED_PREFIXES
from edge_gateway.resilience import BreakerConfig


def breaker() -> BreakerConfig:
    """A valid breaker config, so that these tests fail for the reason they name.

    The numbers are unremarkable on purpose: nothing in this file is about breaker policy, and a
    shared constructor keeps a future change to `BreakerConfig`'s required fields to one edit rather
    than twenty.
    """
    return BreakerConfig(
        failure_rate_threshold=0.5,
        minimum_throughput=20,
        window_seconds=60.0,
        open_duration_seconds=30.0,
        half_open_max_concurrent=2,
        half_open_successes_to_close=3,
    )


def upstream(**overrides: object) -> UpstreamConfig:
    """A valid upstream, with one field replaced per test.

    Written as overrides rather than as a literal per test so that each test body contains only the
    thing under test. When a test says `upstream(max_concurrent=0)`, the field being rejected is the
    only field on the line.
    """
    kwargs: dict[str, object] = {
        "name": "search",
        "base_url": "http://search.internal",
        "breaker": breaker(),
        "max_concurrent": 8,
        "max_queue": 16,
        "reserve_ms": 20,
        "minimum_call_ms": 50,
    }
    kwargs.update(overrides)
    return UpstreamConfig(**kwargs)  # type: ignore[arg-type]


def gateway(**overrides: object) -> GatewayConfig:
    kwargs: dict[str, object] = {
        "upstreams": (upstream(),),
        "default_budget_ms": 1000,
        "max_budget_ms": 5000,
    }
    kwargs.update(overrides)
    return GatewayConfig(**kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# The happy path, and immutability
# ---------------------------------------------------------------------------


def test_a_valid_configuration_is_accepted_and_indexed_by_name() -> None:
    """The index is built in `__post_init__`, not by the caller, and this is what proves it exists.

    Routing reads `by_name`, so an index that were merely documented rather than built would fail at
    the first request instead of at construction — the failure mode this whole module exists to
    avoid.
    """
    config = gateway()

    assert list(config.by_name) == ["search"]
    assert config.by_name["search"] is config.upstreams[0]


def test_configuration_cannot_be_mutated_after_construction() -> None:
    """Frozen, so a request handler cannot retune a threshold mid-flight.

    A breaker whose threshold changed halfway through its window is a breaker whose window means
    nothing, and the resulting incident — "it opened at 40% once" — is unreproducible, so nobody can
    ever confirm the fix. Immutability is what makes the numbers in a snapshot trustworthy.
    """
    config = gateway()

    with pytest.raises((AttributeError, TypeError)):
        config.default_budget_ms = 99  # type: ignore[misc]


def test_a_misspelt_field_is_rejected_at_construction_rather_than_defaulted() -> None:
    """The argument for a dataclass over a dict, stated as a test.

    In a dict, `max_quue` is silently ignored and the queue takes whatever default the code supplies
    — a value nobody chose, discovered during an incident. Here it is a `TypeError` before the
    process accepts a single request.
    """
    with pytest.raises(TypeError):
        upstream(max_quue=16)


# ---------------------------------------------------------------------------
# Upstream names
# ---------------------------------------------------------------------------


def test_an_empty_upstream_name_is_rejected() -> None:
    """An empty name is a route that can never match, and a metric label that says nothing.

    The name is both the routing key and the label on every breaker and bulkhead metric, so an empty
    one produces graphs with a blank series alongside real ones — which reads as a broken exporter
    rather than as a config mistake.
    """
    with pytest.raises(ValueError, match="must not be empty"):
        upstream(name="")


def test_a_name_containing_a_slash_is_rejected() -> None:
    """Routing matches exactly one path segment, so a name with a slash can never be matched.

    `api/search` looks entirely reasonable in a config file and is unreachable: the first segment is
    `api`, and no upstream is called `api`, so every request to it 404s. Rejecting it names the real
    problem at startup instead of leaving an operator to discover that a URL they can see in the
    config does not work.
    """
    with pytest.raises(ValueError, match="single path segment"):
        upstream(name="api/search")


@pytest.mark.parametrize("reserved", sorted(RESERVED_PREFIXES))
def test_a_name_that_shadows_an_operational_endpoint_is_rejected(reserved: str) -> None:
    """Parametrised over the set, so adding a reserved prefix cannot skip being covered.

    This is the rejection with the worst symptom if it is missing. Starlette matches in declaration
    order and `/healthz` is declared first, so an upstream named `healthz` is not *mostly* broken —
    it is silently, permanently unreachable, and every request to it returns a cheerful `{"status":
    "ok"}` from us. That is diagnosed as a networking or service-discovery problem, for a day,
    because something is clearly answering.
    """
    with pytest.raises(ValueError, match="reserved"):
        upstream(name=reserved)


def test_a_name_that_merely_starts_with_a_reserved_word_is_fine() -> None:
    """The check is on the whole segment, not a prefix match, and the distinction is deliberate.

    `healthz-service` shadows nothing: routing compares complete path segments, so
    `/healthz-service/x` cannot reach the `/healthz` route. Rejecting it would be a rule with no
    failure behind it, and rules like that get relaxed by whoever hits them first — usually by
    deleting the check that mattered alongside the one that did not.
    """
    assert upstream(name="healthz-service").name == "healthz-service"


# ---------------------------------------------------------------------------
# base_url
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "base_url",
    ["search.internal", "//search.internal", "/search", "ftp://search.internal", ""],
)
def test_a_base_url_without_an_http_scheme_is_rejected(base_url: str) -> None:
    """A schemeless URL is *relative*, and httpx would resolve it against nothing.

    The result is a request to a path on ourselves — a proxy loop, which presents as mysterious
    recursion, growing latency and eventually resource exhaustion, with no error that mentions
    configuration. `ftp://` is in the list because the failure there is different and also worth
    refusing early: the transport simply cannot make that call, and finding out at request time
    turns a typo into a 500.
    """
    with pytest.raises(ValueError, match="must start with"):
        upstream(base_url=base_url)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://search.internal",
        "https://search.internal",
        "http://search.internal:9000",
        "https://search.internal/v2",
    ],
)
def test_both_schemes_and_a_port_and_a_path_prefix_are_accepted(base_url: str) -> None:
    """A base URL may carry a port and a path prefix, and both are ordinary deployments.

    A path prefix matters in practice: a dependency mounted under `/v2` should be configurable
    without every client having to know about it, which is one of the few rewrites a gateway can do
    safely because it is declared once rather than per request.
    """
    assert upstream(base_url=base_url).base_url == base_url


# ---------------------------------------------------------------------------
# Capacity numbers
# ---------------------------------------------------------------------------


def test_a_max_concurrent_of_zero_is_rejected() -> None:
    """Zero concurrency takes the dependency offline while reporting it as merely full.

    Every call is refused with `bulkhead-full`, which reads in a dashboard as an overload — so the
    response is to investigate load, or worse, to add capacity to the dependency. Nothing about the
    symptom points at a `0` in a config file. One is the smallest number that means anything.
    """
    with pytest.raises(ValueError, match="at least 1"):
        upstream(max_concurrent=0)


def test_a_negative_max_concurrent_is_rejected() -> None:
    """Covered separately from zero because a negative value could arrive from arithmetic.

    A config computed as `cpu_count - 2` on a small machine is negative, and the mistake is in the
    expression rather than in a literal — which is exactly the case a test that only checks `0`
    would let through if the comparison were written as `== 0`.
    """
    with pytest.raises(ValueError, match="at least 1"):
        upstream(max_concurrent=-1)


def test_a_max_queue_of_zero_is_allowed_because_shedding_immediately_is_a_real_policy() -> None:
    """Zero queue is valid and sometimes correct, so it must not be swept up by a `< 1` check.

    For a dependency whose latency is high relative to a typical budget, queueing only adds wait to
    calls that will be refused anyway (the `Q * L / C` arithmetic in `bulkhead.py`). Shedding on
    arrival is then strictly better: the caller learns immediately and can fall back while it still
    has budget to do so.
    """
    assert upstream(max_queue=0).max_queue == 0


@pytest.mark.parametrize("field_name", ["max_queue", "reserve_ms", "minimum_call_ms"])
def test_negative_millisecond_and_depth_values_are_rejected(field_name: str) -> None:
    """Three fields whose only invalid values are negative, checked together.

    Parametrised rather than written out because the interesting part is that all three are checked
    at all: a negative `reserve_ms` would *add* time to the outbound budget, telling the dependency
    it has longer than the client will wait — a reserve that works backwards, and one that no
    downstream behaviour would ever reveal as a config error.
    """
    with pytest.raises(ValueError, match="must not be negative"):
        upstream(**{field_name: -1})


def test_a_zero_minimum_call_is_allowed_but_is_the_documented_footgun() -> None:
    """Accepted, because a genuinely local dependency may have no meaningful connect floor.

    Not silently encouraged either: with zero, the gateway will open a connection with 2ms of budget
    left, make the dependency do the whole piece of work, and abandon the answer — paying full cost
    for a guaranteed timeout. The field's own docstring says so; validation's job is to reject the
    impossible, not to enforce taste.
    """
    assert upstream(minimum_call_ms=0).minimum_call_ms == 0


# ---------------------------------------------------------------------------
# The gateway as a whole
# ---------------------------------------------------------------------------


def test_a_gateway_with_no_upstreams_is_rejected() -> None:
    """Nothing to proxy to means 404 for every request, which is a deployment nobody intended.

    Almost always a config file that failed to load, or an environment where a list came back empty
    — both of which are far easier to recognise from a startup failure than from a service that is
    up, healthy, and answering 404.
    """
    with pytest.raises(ValueError, match="no upstreams"):
        gateway(upstreams=())


def test_a_default_budget_of_zero_is_rejected() -> None:
    """A zero default refuses every request that carries no header, i.e. every browser.

    And it refuses them as `budget-exhausted-on-arrival`, which reads as callers sending impossible
    deadlines rather than as our own default. At the true edge the header is always absent, so this
    is a total outage described in the language of somebody else's mistake.
    """
    with pytest.raises(ValueError, match="must be positive"):
        gateway(default_budget_ms=0)


def test_a_negative_default_budget_is_rejected() -> None:
    """The same check, from the other side, for the same reason as negative `max_concurrent`.

    Config values get computed, and a computed negative is a likelier mistake than a typed one.
    """
    with pytest.raises(ValueError, match="must be positive"):
        gateway(default_budget_ms=-5)


def test_a_ceiling_below_the_default_is_rejected() -> None:
    """The one cross-field rule, and the one whose effect is genuinely counter-intuitive.

    Each number is individually reasonable — a 1000ms default, a 500ms ceiling — and together they
    mean every request without a header is clamped to 500ms. The default nobody chose wins, the
    configured default is dead code, and the only evidence is a timeout rate that does not match the
    documented budget.
    """
    with pytest.raises(ValueError, match="at least default_budget_ms"):
        gateway(default_budget_ms=1000, max_budget_ms=500)


def test_a_ceiling_equal_to_the_default_is_allowed() -> None:
    """Equality is a coherent policy: one fixed budget, and the header may only shorten it.

    Worth an explicit test because it is the boundary, and because a `<=` where the code needs `<`
    would forbid a configuration that is not merely legal but is a sensible way to run a gateway
    whose callers are not trusted to lengthen anything.
    """
    config = gateway(default_budget_ms=1000, max_budget_ms=1000)

    assert config.max_budget_ms == config.default_budget_ms


def test_two_upstreams_with_the_same_name_are_rejected() -> None:
    """A dict comprehension would resolve this silently in favour of the last one.

    The first upstream's breaker and bulkhead settings would then never apply, and the object
    holding them would sit in the config looking authoritative. Nothing errors; the only symptom is
    that a thoroughly configured dependency behaves as though it were configured differently, which
    is unfalsifiable from the outside.
    """
    with pytest.raises(ValueError, match="duplicate upstream name"):
        gateway(
            upstreams=(
                upstream(name="search"),
                upstream(name="search", base_url="http://other.internal"),
            )
        )


def test_distinct_upstreams_are_all_indexed() -> None:
    """The positive case for `by_name`, with more than one entry so ordering bugs have room to show.

    A single-entry index passes almost any implementation, including one that overwrites on every
    iteration.
    """
    config = gateway(
        upstreams=(
            upstream(name="search"),
            upstream(name="pricing", base_url="https://pricing.internal"),
            upstream(name="profiles", base_url="http://profiles.internal:8443"),
        )
    )

    assert sorted(config.by_name) == ["pricing", "profiles", "search"]
    assert config.by_name["pricing"].base_url == "https://pricing.internal"


def test_two_upstreams_may_share_a_base_url() -> None:
    """Deliberately allowed, and the reason is the whole point of per-dependency isolation.

    The same host can serve two logically separate dependencies — a fast read path and a slow report
    path on one deployment — and giving them separate names gives them separate breakers and
    separate bulkheads. The slow one saturating then cannot starve the fast one, which is precisely
    the isolation this project is about. Rejecting a shared host would forbid the most useful
    configuration in the file.
    """
    config = gateway(
        upstreams=(
            upstream(name="reads", base_url="http://reports.internal"),
            upstream(name="reports", base_url="http://reports.internal"),
        )
    )

    assert sorted(config.by_name) == ["reads", "reports"]
