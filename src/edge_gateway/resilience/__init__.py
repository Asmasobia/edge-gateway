"""Resilience primitives: deadlines, circuit breaking, bulkheads, and their composition.

Everything in this subpackage is deliberately free of third-party imports and of real I/O, so that
each pattern can be tested by driving it directly rather than by standing up a server and hoping the
interesting state gets reached. Time is injected (see `clock`), which is what makes "the circuit has
been open for 30 seconds" a one-line test instead of a 30-second one.

That rule holds for `guard` and `classify` too, which is why the composition takes a callable and a
status *code* rather than an HTTP client and a response object: the ordering decisions in `guard`
are the substance of slice 4, and they should be testable without a socket in sight. Everything that
knows what HTTP is lives in `edge_gateway.gateway`.
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
from edge_gateway.resilience.bulkhead import (
    Bulkhead,
    BulkheadFullError,
    BulkheadSlot,
    BulkheadSnapshot,
)
from edge_gateway.resilience.classify import classify_status
from edge_gateway.resilience.clock import Clock, ManualClock, MonotonicClock
from edge_gateway.resilience.deadline import (
    TIMEOUT_BUDGET_HEADER,
    BudgetSource,
    Deadline,
    deadline_from_headers,
    headers_for_downstream,
)
from edge_gateway.resilience.guard import (
    BudgetExhaustedError,
    Guard,
    SlotWaitExpiredError,
    UpstreamTimeoutError,
)
from edge_gateway.resilience.window import SlidingWindow, WindowCounts

__all__ = [
    "TIMEOUT_BUDGET_HEADER",
    "BreakerConfig",
    "BreakerSnapshot",
    "BreakerState",
    "BudgetExhaustedError",
    "BudgetSource",
    "Bulkhead",
    "BulkheadFullError",
    "BulkheadSlot",
    "BulkheadSnapshot",
    "CircuitBreaker",
    "CircuitOpenError",
    "Clock",
    "Deadline",
    "Guard",
    "ManualClock",
    "MonotonicClock",
    "Outcome",
    "Permit",
    "SlidingWindow",
    "SlotWaitExpiredError",
    "UpstreamTimeoutError",
    "WindowCounts",
    "classify_status",
    "deadline_from_headers",
    "headers_for_downstream",
]
