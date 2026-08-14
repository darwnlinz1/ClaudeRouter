"""Shared interpretation of typed test requirements and evidence."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

_SYNTAX_TOKENS = ("syntax", "parse", "compile", "typecheck", "type-check")


def is_syntax_requirement(requirement: str) -> bool:
    normalized = str(requirement).casefold()
    return any(token in normalized for token in _SYNTAX_TOKENS)


def requires_integration_test(requirements: Iterable[str]) -> bool:
    values = tuple(str(requirement) for requirement in requirements)
    return bool(values) and any(not is_syntax_requirement(value) for value in values)


def integration_test_passed(status: str | None) -> bool:
    """Only an executed integration command with an exact pass is complete."""
    return str(status or "").casefold() == "passed"


def item_test_evidence_complete(
    requirements: Iterable[str],
    evidence: Mapping[str, Any],
    *,
    allow_deferred_integration: bool,
) -> bool:
    """Check item evidence at either code-review or final reconciliation time."""
    values = tuple(str(requirement) for requirement in requirements)
    if not values:
        return True
    test_status = str(evidence.get("test_status") or "").casefold()
    syntax_status = str(evidence.get("syntax_status") or "").casefold()
    if not requires_integration_test(values):
        return test_status == "passed" or syntax_status == "passed"
    if test_status == "passed":
        return str(evidence.get("test_scope") or "").casefold() == "integration"
    return bool(
        allow_deferred_integration
        and test_status == "deferred"
        and str(evidence.get("test_scope") or "").casefold() == "integration"
        and syntax_status in {"passed", "not_applicable"}
    )
