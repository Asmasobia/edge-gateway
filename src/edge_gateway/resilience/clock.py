"""An injectable time source.

WHY TIME IS A DEPENDENCY HERE AND NOT JUST A CALL TO `time.monotonic()`
--------------------------------------------------------------------------------
Every pattern in this package is defined in terms of elapsed time: "the circuit re-tries after 30
seconds", "the budget has 40 ms left", "the error rate over the last 10 seconds". If those modules
call the system clock directly then testing them means *actually waiting*, and that has three
consequences, in increasing order of seriousness:

 1. The suite gets slow. A dozen breaker-recovery tests at 30 s each is six minutes, so the tests
    get run less often, or the timeout gets lowered to 0.1 s purely to make tests bearable — at
    which point the value under test is no longer the value that ships.
 2. The suite gets flaky. Asserting "roughly 30 seconds elapsed" needs a tolerance, the tolerance
    has to absorb CI being slow, and a test that fails once a fortnight gets retried rather than
    read.
 3. Some states become unreachable. To test that a half-open circuit re-opens on a failed trial you
    need to be *inside* the half-open window at a moment you control. With a real clock that is a
    race; with an injected clock it is `clock.advance(30)`.

So `Clock` is a Protocol with one method, production passes `MonotonicClock`, and tests pass
`ManualClock`. This is not indirection for its own sake — it is the difference between the recovery
behaviour being tested and being hoped for.

WHY MONOTONIC AND NOT WALL-CLOCK TIME
--------------------------------------------------------------------------------
`time.time()` is wall-clock time and it can move backwards. NTP corrections, a VM resuming from
suspend, or an operator fixing a drifted clock all adjust it, and a leap-second smear slews it.
Measuring a duration by subtracting two wall-clock readings therefore yields a number that is
occasionally negative or wildly wrong.

Concretely, with wall-clock time: if the clock jumps back 5 seconds mid-request, `remaining` on a
deadline *grows*, and a request that should have been abandoned keeps running. If it jumps forward,
every in-flight deadline expires at once and the gateway sheds all its traffic simultaneously — an
NTP correction becomes an outage.

`time.monotonic()` is guaranteed by the language to never go backwards and to be unaffected by clock
adjustments. Its zero point is arbitrary and differs per process, so the values are meaningless as
timestamps and are useful *only* for subtraction — which is exactly and only what this package does
with them. That arbitrary zero point is also the reason a deadline cannot be put on the wire as an
absolute value; see `deadline.py`.
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """A source of monotonically non-decreasing seconds.

    One method, because one method is all any caller in this package needs. A wider interface
    (`sleep`, `now_utc`, ...) would have to be implemented by every test double even where the test
    does not care, and each unused method is somewhere for a fake to diverge from reality unnoticed.
    """

    def now(self) -> float:
        """Seconds from an arbitrary, fixed origin. Only differences are meaningful."""
        ...


class MonotonicClock:
    """The production clock: `time.monotonic()`, unadorned.

    No caching of the value, no rounding. Deliberately boring — this class exists to be the thing
    the fake is a fake *of*, so any logic living here would be logic the tests never exercise.
    """

    __slots__ = ()

    def now(self) -> float:
        return time.monotonic()


class ManualClock:
    """A clock that only moves when a test moves it.

    Starts at 0.0 rather than at some large arbitrary number. That is a testing-ergonomics choice:
    it makes failure messages readable (`expected 0.4, got 0.35` beats `expected 913847.4, got
    913847.35`). It also means a bug where code accidentally treats the monotonic value as a UNIX
    timestamp shows up immediately as a date in 1970 rather than hiding behind a plausible-looking
    number.
    """

    __slots__ = ("_now",)

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        """Move time forward.

        Refuses to go backwards. The whole premise of this package is that time is monotonic, so a
        test that rewinds the clock would be asserting behaviour under conditions production cannot
        produce — and any code written to satisfy such a test would be dead weight. Failing loudly
        here is cheaper than debugging why a breaker behaved impossibly.
        """
        if seconds < 0:
            raise ValueError(f"a monotonic clock cannot move backwards (got {seconds!r})")
        self._now += seconds

    def advance_ms(self, milliseconds: float) -> None:
        """Convenience for the many callers that think in milliseconds, as budgets do."""
        self.advance(milliseconds / 1000.0)
