"""A circuit breaker: stop calling a dependency that is already failing.

THE PROBLEM
--------------------------------------------------------------------------------
A dependency starts failing. Every call now takes the full timeout and then errors. Without a
breaker each of those calls still costs a connection from the pool, a worker for the duration of
the timeout, and — if there are retries — three times both. The caller's capacity drains into a
service that cannot use it, so the caller starts failing requests that had nothing to do with that
dependency. The outage spreads outward from the thing that broke, which is the defining behaviour
of a cascading failure.

There is a second, less obvious cost. The failing dependency is now receiving its *full* request
load plus retries, at exactly the moment it is least able to serve it. If it was failing because it
was overloaded, the callers' persistence is what prevents it from recovering. A dependency can be
held down indefinitely by clients faithfully retrying.

A breaker fixes both by *failing fast locally*: once the evidence says the dependency is broken,
stop calling it, return an error immediately at no cost, and try again occasionally to find out
when it is better. Fast local failure is not a worse outcome than a slow remote failure — it is the
same outcome, without paying for it, and without holding the dependency underwater.

THE STATE MACHINE
--------------------------------------------------------------------------------
    CLOSED ──(failure rate too high, with enough traffic to mean it)──▶ OPEN
    OPEN ──(cooldown elapsed)──▶ HALF_OPEN
    HALF_OPEN ──(enough trial calls succeeded)──▶ CLOSED
    HALF_OPEN ──(any trial call failed)──▶ OPEN   [cooldown restarts]

CLOSED means normal operation; calls pass and their outcomes are recorded. OPEN means every call is
rejected immediately without being attempted. HALF_OPEN is the interesting one: a strictly limited
number of calls are let through as probes, because the only way to discover that a service has
recovered is to send it something.

THE FOUR DECISIONS THAT ACTUALLY MATTER
--------------------------------------------------------------------------------
Everything below is standard. These four are where naive implementations go wrong, and each has a
matching test:

 1. **A rate over a window, not a count of consecutive failures.** "Five failures in a row" sounds
    reasonable and is nearly useless: a dependency failing 50% of the time — catastrophic — may
    never produce five in a row, so the breaker never trips. Partial failure is the common case in
    distributed systems, not total failure.

 2. **A minimum throughput before the rate is allowed to mean anything.** This is the one most often
    missing. A rate needs a denominator. At 03:00 a health check produces one request in the
    window; it fails; the failure rate is 100%; the breaker trips and every subsequent request is
    rejected on the evidence of a single call. The service has taken itself offline over one blip.
    The fix is to refuse to draw a conclusion below a configured sample size, which is just the
    statistical point that a proportion computed from n=1 is not a measurement.

 3. **Bounded concurrency in HALF_OPEN.** The naive half-open lets *all* traffic through the moment
    the cooldown expires and watches what happens. If the dependency is still fragile — which,
    having just been broken, is likely — the full accumulated load arrives at once and knocks it
    straight back down. The breaker has now become the thundering herd it exists to prevent, on a
    cycle. So half-open admits at most `half_open_max_concurrent` calls at a time, and a single
    failure is enough to re-open.

 4. **The window is reset when the circuit closes.** Skip this and recovery is impossible: the
    failures that opened the circuit are still inside the window, so the breaker consults it, sees
    the same high rate, and re-opens immediately. The symptom is a circuit flapping between OPEN
    and HALF_OPEN forever while the dependency is verifiably healthy — and it is a hard bug to see,
    because every individual state transition looks correct.

WHAT MUST NOT COUNT AS A FAILURE
--------------------------------------------------------------------------------
A breaker is a hypothesis about one specific question: *is this dependency unhealthy?* Anything
recorded as a failure that is not evidence for that hypothesis makes the breaker fire when it should
not, and a breaker that fires spuriously gets its threshold raised until it never fires at all.

Not evidence, and therefore `Outcome.NOT_COUNTED`:

  - **HTTP 4xx.** A 400 means *we* sent a bad request; a 404 means the thing does not exist; a 403
    means we lack permission. In all three the dependency answered correctly and quickly. A client
    deploying a bug that produces 404s would otherwise trip the breaker and take out the dependency
    for every other caller. (429 is the exception — see slice 4's classifier — because it *is* the
    dependency telling us it cannot serve us.)
  - **A local refusal because the request's time budget is exhausted.** We never called the
    dependency. Recording that as its failure would let a slow client, or a stingy budget, trip a
    breaker on a service that was healthy throughout.
  - **A bulkhead rejection (slice 3).** Same reasoning: the call did not happen, so it is evidence
    about our own saturation, not about the dependency's health.

Getting this wrong is the single most common way a resilience layer makes a system *less* reliable
than it was without one.

CONCURRENCY
--------------------------------------------------------------------------------
Not thread-safe, deliberately. The intended deployment is a single asyncio event loop, and every
method here runs start to finish with no `await` inside it, so each is already atomic with respect
to other coroutines — a lock would buy nothing and cost an acquisition on the hottest path in the
gateway. Sharing one breaker across OS threads would genuinely be unsafe
(`self._half_open_in_flight += 1` is not atomic), and the honest response to that is a note in the
docs plus a design conversation rather than a lock that makes the misuse silent. Per-process
breakers are also the normal deployment: a breaker is one process's opinion about a dependency, and
coordinating that opinion across a fleet is a much larger design problem with its own failure
modes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import TracebackType

from edge_gateway.resilience.clock import Clock
from edge_gateway.resilience.window import SlidingWindow, WindowCounts


class BreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class Outcome(Enum):
    """What a call told us about the dependency's health.

    Three values, not two, and the third is the important one. `NOT_COUNTED` exists so that a
    caller can say "this call finished, release its slot, but it is not evidence" — see the module
    docstring on what must not count. Without a third value every outcome has to be forced into
    success or failure, and the forcing is where breakers acquire their reputation for firing at
    random.
    """

    SUCCESS = "success"
    FAILURE = "failure"
    NOT_COUNTED = "not_counted"


class CircuitOpenError(Exception):
    """Raised instead of calling a dependency the breaker believes is broken.

    An exception rather than a `None` return, so that a caller who has not thought about the open
    case fails loudly instead of dereferencing a missing response. It carries the diagnostic detail
    a gateway needs to produce a useful reply — in particular `retry_after_s`, which maps directly
    onto an HTTP `Retry-After` header and turns "try again whenever" into something a client can
    obey.
    """

    def __init__(self, breaker_name: str, state: BreakerState, retry_after_s: float | None) -> None:
        self.breaker_name = breaker_name
        self.state = state
        self.retry_after_s = retry_after_s
        """Seconds until a probe will be admitted, or None when that is not knowable.

        None in HALF_OPEN: the rejection there is because the trial slots are momentarily full,
        and one may free up in a microsecond or a minute depending on the dependency's latency.
        Inventing a number would be worse than admitting ignorance, because the client would
        trust it.
        """
        detail = (
            f"after {retry_after_s:.3f}s" if retry_after_s is not None else "at an unknown time"
        )
        super().__init__(f"circuit {breaker_name!r} is {state.value}; retry {detail}")


@dataclass(frozen=True, slots=True)
class BreakerConfig:
    """Policy for one breaker. Frozen: policy must not drift at runtime.

    Every threshold is required, with no library default. Same argument as the timeout budget's
    `default_ms` (ADR-007): what counts as "too many failures" for a payments provider is not what
    it means for a recommendations service, and a shared default is how an entire fleet inherits a
    number nobody chose and then discovers it during an incident.

    `bucket_count` is the one exception, and the exception is principled: it is not a policy
    decision about the dependency at all, it is an internal accuracy-versus-memory trade in the
    window implementation (see `window.py`). Ten is a reasonable default for the same reason a hash
    table has a default load factor.
    """

    failure_rate_threshold: float
    """Fraction of failures at which the circuit opens, in (0.0, 1.0]. 0.5 means "half the calls".

    Note this must be reached *and* `minimum_throughput` satisfied. A threshold of 1.0 is legal and
    means "open only when everything is failing" — defensible for a dependency with a genuinely
    flaky baseline, though it will not catch partial failure.
    """

    minimum_throughput: int
    """Calls required in the window before the rate is trusted at all. See decision 2 above.

    Set this from real traffic, not by taste: it should be comfortably below the number of calls a
    healthy window actually contains, or the breaker can never trip. A service handling 2 requests
    a second with a 10-second window sees ~20 calls, so 20 would mean "only trip at peak" — which
    looks like a working breaker in staging and is inert in production at night.
    """

    window_seconds: float
    """How far back the failure rate looks.

    The trade is reaction speed against stability. Short windows react fast and are noisy — a
    two-second blip fills a two-second window. Long windows are stable and slow: a 60-second window
    keeps a resolved outage influencing decisions for a minute after it ended.
    """

    open_duration_seconds: float
    """How long to reject everything before admitting a probe.

    Long enough for the dependency to plausibly recover — restart, failover, garbage collection,
    autoscaling — and short enough that a recovered dependency is not ignored. Too short and probes
    arrive while it is still starting up, so each one re-opens the circuit and the cooldown
    restarts: a service that recovered in 5 seconds stays circuit-broken indefinitely.
    """

    half_open_max_concurrent: int
    """Probes allowed in flight at once while HALF_OPEN. See decision 3 above.

    Small — usually 1 to 3. This is a question, not a workload: the aim is to learn whether the
    dependency works while giving it almost nothing to do.
    """

    half_open_successes_to_close: int
    """Consecutive probe successes needed to close the circuit.

    More than one, because a single success can be luck: a load balancer with one healthy backend
    out of five returns a success 20% of the time, and closing on that sends full traffic to a
    mostly broken pool. Requiring several trades recovery latency for confidence.
    """

    bucket_count: int = 10

    def __post_init__(self) -> None:
        """Validate at construction, so a bad policy fails at startup rather than at 3am.

        Every check below is a configuration that would silently produce a breaker that *looks*
        installed and does not work. A misconfigured breaker is worse than no breaker, because it is
        believed in.
        """
        if not 0.0 < self.failure_rate_threshold <= 1.0:
            # Zero would open on the first recorded failure regardless of rate — and, because an
            # empty window reports 0.0, on any traffic at all. Above 1.0 is unreachable, so the
            # breaker would never open: a resilience mechanism that is present, configured,
            # monitored and inert.
            raise ValueError(
                "failure_rate_threshold must be in (0.0, 1.0] "
                f"(got {self.failure_rate_threshold!r})"
            )
        if self.minimum_throughput < 1:
            raise ValueError(
                f"minimum_throughput must be at least 1 (got {self.minimum_throughput!r})"
            )
        if self.window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive (got {self.window_seconds!r})")
        if self.open_duration_seconds <= 0:
            # Zero would make OPEN last no time at all, so the circuit would be half-open
            # immediately and probe traffic would hit the dependency continuously — removing the
            # breathing room that is the entire point of opening.
            raise ValueError(
                f"open_duration_seconds must be positive (got {self.open_duration_seconds!r})"
            )
        if self.half_open_max_concurrent < 1:
            # Zero admits no probes, so the circuit can never leave HALF_OPEN. Permanently open,
            # with no error and nothing in the logs.
            raise ValueError(
                "half_open_max_concurrent must be at least 1 "
                f"(got {self.half_open_max_concurrent!r})"
            )
        if self.half_open_successes_to_close < 1:
            raise ValueError(
                "half_open_successes_to_close must be at least 1 "
                f"(got {self.half_open_successes_to_close!r})"
            )
        if self.bucket_count < 1:
            raise ValueError(f"bucket_count must be at least 1 (got {self.bucket_count!r})")


@dataclass(frozen=True, slots=True)
class BreakerSnapshot:
    """A consistent reading of a breaker, for metrics, logs and tests.

    One object rather than a set of properties polled individually, for the same reason
    `WindowCounts` is a value object: a dashboard that reads `state` and then `failure_rate` can
    catch a transition between the two and display a combination that never existed, which is how
    "the graph says the circuit was closed at 50% errors" gets investigated for an afternoon.
    """

    name: str
    state: BreakerState
    counts: WindowCounts
    retry_after_s: float | None
    trials_in_flight: int
    trial_successes: int

    rejected_calls: int = 0
    """Calls refused without being attempted. The value the breaker delivered, and the number to
    graph against latency saved — not an error count."""

    stale_outcomes: int = 0
    """Results that arrived after the state they belonged to had already been left. Expected to be
    small but non-zero on a busy breaker; a large number means calls routinely outlive the cooldown,
    which is a sign the cooldown is too short for this dependency's latency."""

    unresolved_permits: int = 0
    """Permits released by the context manager without an outcome having been recorded. Always a bug
    in the calling code — see `Permit.__exit__`. Should be zero forever; alert on any increase."""

    trips: int = 0
    """Total CLOSED-to-OPEN transitions since construction. The headline number for "how often does
    this dependency break", and the one to alert on."""


class Permit:
    """Permission for exactly one call, which must be resolved with its outcome.

    Why a permit object rather than a `breaker.allow()` boolean: bounded probe concurrency requires
    the breaker to know how many probes are *currently in flight*, which is impossible if permission
    is a fire-and-forget yes/no. Something has to represent "this call is still running", and making
    that a value the caller holds is the only shape where the breaker can count it.

    Resolve with `success()`, `failure()` or `not_counted()`. Resolving twice raises, because it
    would double-count evidence and skew the very rate the breaker acts on.
    """

    __slots__ = ("_breaker", "_generation", "_is_trial", "_resolved")

    def __init__(self, breaker: CircuitBreaker, generation: int, *, is_trial: bool) -> None:
        self._breaker = breaker
        self._generation = generation
        self._is_trial = is_trial
        self._resolved = False

    @property
    def is_trial(self) -> bool:
        """True if this call is a HALF_OPEN probe. Worth logging: probe latency is the signal that
        tells you whether a dependency is recovering or merely up."""
        return self._is_trial

    def success(self) -> None:
        self._resolve(Outcome.SUCCESS)

    def failure(self) -> None:
        self._resolve(Outcome.FAILURE)

    def not_counted(self) -> None:
        """The call completed but says nothing about the dependency's health — a 404, say.

        Releases the probe slot without contributing evidence. Note the consequence in HALF_OPEN: a
        probe that returns 404 does not advance recovery, so a dependency that only ever answers 404
        keeps the circuit half-open indefinitely. That is correct — it genuinely is not evidence of
        health — but it means "half-open forever with no failures" is a real state to recognise.
        """
        self._resolve(Outcome.NOT_COUNTED)

    def _resolve(self, outcome: Outcome) -> None:
        if self._resolved:
            raise RuntimeError(
                "this permit has already been resolved; recording an outcome twice would "
                "double-count evidence and distort the failure rate the breaker acts on"
            )
        self._resolved = True
        self._breaker._settle(self._generation, is_trial=self._is_trial, outcome=outcome)

    def __enter__(self) -> Permit:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Guarantee the probe slot is released, even on an exception or a cancellation.

        This is the leak that matters. If a permit is never resolved — an exception on a path that
        forgot to call `failure()`, or an `asyncio.CancelledError` unwinding the coroutine — then
        `_half_open_in_flight` is never decremented. Leak `half_open_max_concurrent` of them and the
        breaker rejects every probe forever: **the circuit can never close again**, for the lifetime
        of the process, with no failures being recorded and nothing in the logs to explain it. A
        `finally` that runs no matter what is the whole defence, which is why this is a context
        manager and not a convention.

        An unresolved permit is recorded as NOT_COUNTED rather than guessed at. Guessing FAILURE
        would let a bug in *our* code trip a breaker on a healthy dependency; guessing SUCCESS
        would hide real failures. Since the outcome is genuinely unknown, the honest reading is
        "no evidence" — and because that silence would otherwise be invisible, it is also counted
        in `unresolved_permits`, which should be zero forever and is therefore a clean thing to
        alert on.

        Returns None (falsy), so any exception in the body propagates untouched. Deciding whether
        an exception counts as a dependency failure is classification, and classification belongs
        to the composition layer that knows what the exceptions mean (slice 4).
        """
        if not self._resolved:
            self._breaker._note_unresolved()
            self._resolve(Outcome.NOT_COUNTED)


class CircuitBreaker:
    """One breaker guarding one dependency.

    Usage, with the context manager doing the slot bookkeeping:

        try:
            with breaker.acquire() as permit:
                response = call_the_dependency()
                permit.success() if response.ok else permit.failure()
        except CircuitOpenError:
            return fail_fast_response()

    One breaker per dependency, never one per process. A shared breaker means a failing search
    service stops calls to a healthy payments service, which converts a partial outage into a total
    one — the exact opposite of the intent.
    """

    __slots__ = (
        "_clock",
        "_config",
        "_generation",
        "_half_open_in_flight",
        "_half_open_successes",
        "_name",
        "_opened_at",
        "_rejected_calls",
        "_stale_outcomes",
        "_state",
        "_trips",
        "_unresolved_permits",
        "_window",
    )

    def __init__(self, name: str, *, config: BreakerConfig, clock: Clock) -> None:
        self._name = name
        self._config = config
        self._clock = clock
        self._window = SlidingWindow(
            window_seconds=config.window_seconds,
            bucket_count=config.bucket_count,
            clock=clock,
        )
        self._state = BreakerState.CLOSED
        self._opened_at: float | None = None

        # Incremented on EVERY state transition, and stamped onto each permit at acquire time.
        #
        # This is how a result that arrives too late is recognised. A call issued while the circuit
        # was CLOSED can still be in flight when the circuit trips; a probe issued in HALF_OPEN can
        # return after the circuit has already re-opened or closed. In both cases the outcome
        # describes a state the breaker has left, and applying it would be wrong in a specific and
        # damaging way: a stale probe failure would re-open a circuit that had just recovered and is
        # serving traffic happily, and it would do so *without any current evidence of a problem*.
        #
        # Comparing generations makes "is this result still relevant?" a cheap integer test, with no
        # timestamps to compare and no window to reason about.
        self._generation = 0

        self._half_open_in_flight = 0
        self._half_open_successes = 0
        self._rejected_calls = 0
        self._stale_outcomes = 0
        self._unresolved_permits = 0
        self._trips = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> BreakerState:
        """The current state, after applying any transition that time alone has caused.

        Reading this can move OPEN to HALF_OPEN, which looks like a property with a side effect and
        deserves a justification. The alternative is a background task that flips the state when
        the cooldown expires, and that is worse in three ways: it needs a lifecycle (something must
        start and stop it, and forgetting to stop it leaks a task per breaker); it makes the class
        untestable without real time, which would undo the entire point of the injected clock; and
        it introduces genuine concurrency into a class that otherwise has none.

        Evaluating time-based transitions on access is the standard alternative, and the state
        is *observed* rather than changed: the cooldown had already elapsed whether or not
        anyone looked.
        """
        self._promote_if_cooldown_elapsed()
        return self._state

    def acquire(self) -> Permit:
        """Ask permission to call the dependency. Raises `CircuitOpenError` if refused.

        Cheap by design — no I/O, no allocation beyond the permit, a handful of integer comparisons.
        It sits in front of every call to the dependency, so anything expensive here is a tax on the
        healthy path, which is the path that matters most.
        """
        self._promote_if_cooldown_elapsed()

        if self._state is BreakerState.OPEN:
            self._rejected_calls += 1
            raise CircuitOpenError(self._name, BreakerState.OPEN, self._retry_after_s())

        if self._state is BreakerState.HALF_OPEN:
            if self._half_open_in_flight >= self._config.half_open_max_concurrent:
                # Recovery is deliberately rationed. Everything beyond the probe allowance is
                # refused exactly as if the circuit were still open, because from the dependency's
                # point of view that is the whole benefit: it gets to come back up under a trickle
                # instead of under the full load that is waiting for it.
                self._rejected_calls += 1
                raise CircuitOpenError(self._name, BreakerState.HALF_OPEN, None)
            self._half_open_in_flight += 1
            return Permit(self, self._generation, is_trial=True)

        return Permit(self, self._generation, is_trial=False)

    def snapshot(self) -> BreakerSnapshot:
        """A consistent reading for metrics and assertions."""
        self._promote_if_cooldown_elapsed()
        return BreakerSnapshot(
            name=self._name,
            state=self._state,
            counts=self._window.counts(),
            retry_after_s=self._retry_after_s(),
            trials_in_flight=self._half_open_in_flight,
            trial_successes=self._half_open_successes,
            rejected_calls=self._rejected_calls,
            stale_outcomes=self._stale_outcomes,
            unresolved_permits=self._unresolved_permits,
            trips=self._trips,
        )

    # ------------------------------------------------------------------
    # Internals. Called by Permit, which lives in this module by design: the two are one mechanism
    # split across two objects, and pretending otherwise would mean a public API for bookkeeping
    # that no user of this class should ever call.
    # ------------------------------------------------------------------

    def _note_unresolved(self) -> None:
        self._unresolved_permits += 1

    def _settle(self, generation: int, *, is_trial: bool, outcome: Outcome) -> None:
        """Apply one call's outcome."""
        if generation != self._generation:
            # A result from a state the breaker has already left. Counted for visibility, then
            # dropped — including its contribution to the window, which is the subtle half. The
            # window is reset on recovery precisely to discard the evidence that opened the circuit,
            # so re-admitting a pre-transition result would quietly undo that reset.
            #
            # It also must not decrement `_half_open_in_flight`: the transition already reset that
            # counter to zero, so decrementing here would steal a probe slot from the *current*
            # half-open round and, with enough late results, drive the count negative and admit
            # unbounded probes.
            self._stale_outcomes += 1
            return

        if is_trial:
            self._half_open_in_flight -= 1

        if self._state is BreakerState.HALF_OPEN:
            if outcome is Outcome.SUCCESS:
                self._half_open_successes += 1
                if self._half_open_successes >= self._config.half_open_successes_to_close:
                    self._close()
            elif outcome is Outcome.FAILURE:
                # One failure is enough; there is no rate to compute and no reason to wait for one.
                # A probe exists to answer "is it better yet?", and a failed probe has answered no.
                # Continuing to probe after that is how a fragile service gets held down.
                self._trip()
            return

        if self._state is BreakerState.CLOSED:
            if outcome is Outcome.SUCCESS:
                self._window.record_success()
            elif outcome is Outcome.FAILURE:
                self._window.record_failure()
                self._trip_if_window_says_so()
            return

        # State OPEN with a matching generation is unreachable: `acquire` refuses in OPEN, so no
        # permit can be issued there, and tripping bumps the generation — which makes every permit
        # issued before the trip stale. Left without a branch rather than with defensive code,
        # because unreachable code cannot be tested and untested code should not be trusted.

    def _trip_if_window_says_so(self) -> None:
        """Open the circuit if the recent evidence justifies it.

        Evaluated only after a recorded failure. A success can only push the rate down, so it can
        never be the thing that crosses the threshold, and checking there would be wasted work on
        the healthy path.

        Bucket expiry can technically raise the rate with no new event — old successes ageing out
        while newer failures remain — so a trip can in principle be delayed until the next failure.
        That is an accepted, bounded imprecision: if no further failure ever arrives, the dependency
        is by definition serving requests successfully, and there is nothing to protect anyone from.
        The alternative, re-evaluating on every `acquire`, would double the window scans on the
        hottest path to fix a case that resolves itself.
        """
        counts = self._window.counts()
        if counts.total < self._config.minimum_throughput:
            # Not enough evidence to draw a conclusion. See decision 2 in the module docstring: this
            # single comparison is what stops one failed request out of one from taking the service
            # offline at three in the morning.
            return
        if counts.failure_rate >= self._config.failure_rate_threshold:
            self._trip()

    def _trip(self) -> None:
        """CLOSED or HALF_OPEN to OPEN.

        The window is NOT cleared here, on purpose: its contents are the reason for the trip, so
        keeping them means the snapshot taken during the incident explains itself. It is cleared on
        the way back to CLOSED instead, where stale evidence would prevent recovery.
        """
        if self._state is BreakerState.CLOSED:
            self._trips += 1
        self._state = BreakerState.OPEN
        self._opened_at = self._clock.now()
        self._enter_new_generation()

    def _close(self) -> None:
        """HALF_OPEN to CLOSED: the dependency has convinced us.

        `self._window.reset()` is decision 4, and it is load-bearing. Without it the failures that
        opened the circuit are still in the window, so the next single failure finds a rate already
        above the threshold and re-opens immediately — no matter how healthy the dependency now is.
        """
        self._state = BreakerState.CLOSED
        self._opened_at = None
        self._window.reset()
        self._enter_new_generation()

    def _enter_half_open(self) -> None:
        """OPEN to HALF_OPEN: the cooldown has elapsed, so admit a few probes."""
        self._state = BreakerState.HALF_OPEN
        self._enter_new_generation()

    def _enter_new_generation(self) -> None:
        """Bump the generation and clear the per-round probe bookkeeping.

        Every transition goes through here, so the invariant "probe counters belong to exactly one
        half-open round" cannot be broken by forgetting to reset one of them in a new transition
        added later. Resetting `_half_open_successes` in particular matters: carrying two
        successes from an abandoned round into the next one would close the circuit on fewer
        probes than configured.
        """
        self._generation += 1
        self._half_open_in_flight = 0
        self._half_open_successes = 0

    def _promote_if_cooldown_elapsed(self) -> None:
        if self._state is not BreakerState.OPEN:
            return
        if self._opened_at is None:  # pragma: no cover - invariant, OPEN always records when
            return
        elapsed = self._clock.now() - self._opened_at
        # `>=` so the boundary instant admits a probe. Which side of the boundary this falls on is
        # genuinely inconsequential — a probe one microsecond early cannot matter — but it is pinned
        # by a test regardless, so that a later refactor cannot change it by accident and leave a
        # cooldown that is quietly one tick longer than configured.
        if elapsed >= self._config.open_duration_seconds:
            self._enter_half_open()

    def _retry_after_s(self) -> float | None:
        """Seconds until a probe will be admitted; None when the state does not imply a wait."""
        if self._state is not BreakerState.OPEN or self._opened_at is None:
            return None
        remaining = self._config.open_duration_seconds - (self._clock.now() - self._opened_at)
        # Never negative: a client told to retry after -0.4 seconds has been handed nonsense, and
        # `Retry-After` has no representation for it.
        return max(0.0, remaining)
