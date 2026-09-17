"""edge-gateway: a resilience-aware edge gateway.

The package is split in two, and the split is the main architectural decision in the project:

    edge_gateway.resilience   the patterns — deadlines, circuit breaker, bulkhead.
                              Pure Python, no third-party imports, no I/O, no framework.
    edge_gateway.gateway      the HTTP surface that composes them (arrives in slice 4).

See DECISIONS.md ADR-002 for why the core is kept dependency-free.
"""
