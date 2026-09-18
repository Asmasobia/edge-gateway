"""Turning an HTTP status code into evidence about a dependency's health.

WHY THIS IS ITS OWN MODULE, AND WHY IT IS THE MOST IMPORTANT FILE IN SLICE 4
--------------------------------------------------------------------------------
A circuit breaker is a hypothesis about exactly one question: *is this dependency unhealthy?* Every
outcome fed into it is a vote on that question, so anything recorded as a failure that is not
evidence for it makes the breaker fire when it should not. And a breaker that fires spuriously does
not get fixed — it gets its threshold raised, and raised again, until it never fires at all. The
component is then still installed, still monitored, and completely inert.

That is the single most common way a resilience layer makes a system *less* reliable than it was
without one, and it is not a coding error. It is a classification error, made once, in a line that
looks obviously correct:

    permit.failure() if response.status_code >= 400 else permit.success()

Under that rule, a client sending malformed requests to a perfectly healthy service will open the
circuit on it. The dependency answered every one of those requests correctly, quickly, and cheaply.

WHY A STATUS CODE IS NOT A BOOLEAN
--------------------------------------------------------------------------------
The useful question is not "did the request succeed?" but "who was at fault, and does it say
anything about capacity?" Three answers, matching `Outcome`'s three values:

  * SUCCESS     — the dependency served us. It is healthy.
  * FAILURE     — the dependency could not serve us. Evidence of ill health.
  * NOT_COUNTED — the dependency answered correctly and promptly, and the answer happens to be an
                  error *about our request*. No evidence either way.

The 4xx/5xx split is a good first approximation of that question and not the answer to it, which is
why this module is a table with reasons rather than a comparison against 500.

WHY IT TAKES AN `int`
--------------------------------------------------------------------------------
The parameter is a bare status code, not a response object, so this module stays inside the
no-dependencies rule for `resilience/` (ADR-002) and can be tested by passing numbers. Classifying
*exceptions* — a connection refused, a TLS failure, a read timeout — needs to know which HTTP client
raised them, so it lives with that client in the gateway layer instead.
"""

from __future__ import annotations

from edge_gateway.resilience.breaker import Outcome

# 4xx codes that ARE evidence against the dependency, contradicting the usual "4xx is the client's
# fault" rule. Both are the dependency talking about its own state rather than about our request.
_FAILING_4XX = frozenset(
    {
        # 408 Request Timeout: the dependency gave up waiting for a request we were sending.
        # Either it is too slow to read or we are too slow to write; both are worth acting on,
        # and a healthy fast service does not emit these.
        408,
        # 429 Too Many Requests: an explicit "I cannot serve you at this rate". Treating this
        # as "not our problem" is how a caller keeps pushing a service that is already asking
        # for relief — the retry-storm behaviour a breaker exists to stop.
        429,
    }
)

# 5xx codes that are NOT evidence against the dependency, contradicting the usual "5xx is the
# server's fault" rule. Both are permanent, deterministic answers about what we asked for: they cost
# the dependency nothing, they will never succeed however long we wait, and they say nothing about
# capacity. Functionally they behave like a 405 that was given the wrong number.
_NOT_COUNTED_5XX = frozenset(
    {
        # 501 Not Implemented: this method or feature does not exist here. A deployment or routing
        # mistake on our side, and one that a breaker cannot help with.
        501,
        # 505 HTTP Version Not Supported: the same shape — a protocol mismatch, not a capacity
        # problem, and no amount of waiting or traffic-shedding will change the answer.
        505,
    }
)


def classify_status(status: int) -> Outcome:
    """Classify one HTTP status code as evidence about the dependency's health.

    The ordering of the checks matters, because the specific exceptions have to be consulted before
    the ranges they sit inside; that is the whole content of this function.

    A note on 503 and 504, which are deliberately NOT special-cased: 503 Service Unavailable is the
    clearest possible statement of ill health, and 504 Gateway Timeout means something behind our
    dependency was too slow — which still makes *our* dependency unable to serve us, because from
    here the distinction is invisible. Both fall through to FAILURE via the 5xx rule, and both
    should.
    """
    if status in _FAILING_4XX:
        return Outcome.FAILURE

    if status in _NOT_COUNTED_5XX:
        return Outcome.NOT_COUNTED

    if 100 <= status < 400:
        # 3xx counts as success on purpose. A redirect is a correct, cheap, fast answer; whether we
        # follow it is our business, and following it is a *separate* call that gets classified in
        # its own right. Recording the redirect itself as a failure would open a circuit on a
        # dependency that is behaving exactly as designed.
        return Outcome.SUCCESS

    if 400 <= status < 500:
        # The dependency understood the request, decided it was wrong, and said so — quickly, and
        # without doing the expensive work. A 404 is a *successful* lookup that found nothing.
        # Counting these is how a caller with a bad URL, a stale token or a deleted resource takes a
        # healthy dependency offline for everybody else.
        return Outcome.NOT_COUNTED

    if 500 <= status < 600:
        return Outcome.FAILURE

    # Outside 100-599 entirely: not a valid HTTP status. Something between us and the dependency is
    # broken or is not speaking HTTP, which is exactly the condition worth shedding traffic over, so
    # this is FAILURE rather than an exception. Raising here would convert a misbehaving upstream
    # into a crash in the gateway — replacing their outage with ours, which is the failure mode this
    # whole project is about.
    return Outcome.FAILURE
