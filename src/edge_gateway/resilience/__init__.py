"""Resilience primitives: deadlines, circuit breaking, bulkheads.

Everything in this subpackage is deliberately free of third-party imports and of real I/O, so that
each pattern can be tested by driving it directly rather than by standing up a server and hoping the
interesting state gets reached. Time is injected (see `clock`), which is what makes "the circuit has
been open for 30 seconds" a one-line test instead of a 30-second one.
"""

from edge_gateway.resilience.breaker import (
    BreakerConfig,
    BreakerSnapshot,
    BreakerState,
    CircuitBreaker,
    CircuitOpenError,
    Outcome,
    Permit,
)
from edge_gateway.resilience.clock import Clock, ManualClock, MonotonicClock
from edge_gateway.resilience.deadline import (
    TIMEOUT_BUDGET_HEADER,
    BudgetSource,
    Deadline,
    deadline_from_headers,
    headers_for_downstream,
)
from edge_gateway.resilience.window import SlidingWindow, WindowCounts

__all__ = [
    "TIMEOUT_BUDGET_HEADER",
    "BreakerConfig",
    "BreakerSnapshot",
    "BreakerState",
    "BudgetSource",
    "CircuitBreaker",
    "CircuitOpenError",
    "Clock",
    "Deadline",
    "ManualClock",
    "MonotonicClock",
    "Outcome",
    "Permit",
    "SlidingWindow",
    "WindowCounts",
    "deadline_from_headers",
    "headers_for_downstream",
]
