"""The architectural rule from ADR-002, enforced instead of documented.

WHY A RULE LIKE THIS NEEDS A TEST
--------------------------------------------------------------------------------
"`resilience/` imports nothing but the standard library" is a decision worth keeping and a decision
that is broken by accident, in one line, by someone reaching for a convenience under time pressure.
It will not be caught in review, because the diff is one plausible import.

And the damage is not visible for a long time. What is lost is testability: the moment the
resilience core depends on `httpx`, testing "does 429 count as a failure" requires a client, and
eventually a server, and eventually a port — at which point that test gets slow, then flaky, then
skipped, then deleted. This test costs a millisecond and keeps the thing that made slices 1-4
pleasant to work on.

WHY IT PARSES THE FILES RATHER THAN IMPORTING THEM
--------------------------------------------------------------------------------
A test that imported the modules and inspected `sys.modules` would only see what those imports
actually pulled in — and it would pass on a machine where the forbidden package happened not to be
installed, which is exactly the machine where the mistake was made. Reading the source with `ast`
asks the question the rule is about: what does this file *say* it depends on.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

_RESILIENCE = Path(__file__).resolve().parents[1] / "src" / "edge_gateway" / "resilience"


def _module_names(source: str) -> set[str]:
    """Top-level names of everything a module imports.

    `import a.b.c` and `from a.b import c` both reduce to `a`, because the rule is about which
    *distribution* is depended on and that is what the first component identifies.
    """
    names: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            # `node.module` is None for a relative import (`from . import x`), which by construction
            # cannot reach outside the package and so cannot break this rule.
            if node.module is not None and node.level == 0:
                names.add(node.module.split(".")[0])
    return names


def _resilience_files() -> list[Path]:
    files = sorted(_RESILIENCE.glob("*.py"))
    assert files, f"found no modules under {_RESILIENCE}; the path in this test is wrong"
    return files


@pytest.mark.parametrize("path", _resilience_files(), ids=lambda path: path.name)
def test_the_resilience_core_imports_only_the_standard_library(path: Path) -> None:
    """Parametrised per file so a failure names the module, not just the rule.

    `sys.stdlib_module_names` is the interpreter's own answer to "is this stdlib", which is better
    than a hand-maintained allowlist for the obvious reason: an allowlist would have to be extended
    every time a module here started using a new part of the standard library, and the easiest way
    to extend it is to add whatever name made the test fail.
    """
    imported = _module_names(path.read_text(encoding="utf-8"))
    external = {
        name for name in imported if name not in sys.stdlib_module_names and name != "edge_gateway"
    }
    assert not external, (
        f"{path.name} imports {sorted(external)}, which is outside the standard library; "
        f"ADR-002 keeps the resilience core dependency-free so it can be tested with integers"
    )


@pytest.mark.parametrize("path", _resilience_files(), ids=lambda path: path.name)
def test_the_resilience_core_does_not_import_the_http_layer(path: Path) -> None:
    """The other half of the rule, and the one an allowlist of third parties would miss.

    `from edge_gateway.gateway import ...` uses no third-party name at all, so the check above
    passes it happily — and it is the more likely mistake, because it is the one that looks like
    tidy reuse. A dependency in this direction would make the layering circular and would drag the
    HTTP layer into the import graph of every resilience test.
    """
    source = path.read_text(encoding="utf-8")
    offenders = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom)
        and node.module is not None
        and node.module.startswith("edge_gateway.gateway")
    ]
    assert not offenders, (
        f"{path.name} imports from edge_gateway.gateway; the dependency runs the other way, and "
        f"reversing it would make the layering circular"
    )
