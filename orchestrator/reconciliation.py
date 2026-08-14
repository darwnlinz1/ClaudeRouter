"""Completion reconciliation for hierarchical plans and logical model calls."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .models import PlanStatus, TaskPlan, WorkStatus

_BUCKETS = (
    "completed",
    "skipped",
    "blocked",
    "failed",
    "preflight_failed",
)


def _status_bucket(status: WorkStatus, evidence: Mapping[str, Any]) -> str:
    if status == WorkStatus.APPROVED:
        return "completed"
    if status == WorkStatus.BLOCKED:
        return "blocked"
    if status == WorkStatus.CANCELLED:
        return "skipped"
    if status == WorkStatus.FAILED and evidence.get("failure_kind") == "preflight":
        return "preflight_failed"
    return "failed"


def _partition(values: Mapping[str, str]) -> dict[str, Any]:
    ids: dict[str, list[str]] = {bucket: [] for bucket in _BUCKETS}
    invalid: list[str] = []
    for value_id, bucket in values.items():
        if bucket not in ids:
            invalid.append(value_id)
            continue
        ids[bucket].append(value_id)
    for bucket in ids:
        ids[bucket].sort()
    counts = {bucket: len(ids[bucket]) for bucket in _BUCKETS}
    planned = len(values)
    terminal = sum(counts.values())
    return {
        "planned": planned,
        **counts,
        "terminal": terminal,
        "balanced": terminal == planned and not invalid,
        "ids": ids,
        "invalid_ids": sorted(invalid),
    }


def _typed_contract_approval_errors(
    plan: TaskPlan,
    evidence: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[str]]:
    violations: dict[str, list[str]] = {}
    for stream in plan.workstreams:
        if stream.status == WorkStatus.APPROVED and any(
            item.status != WorkStatus.APPROVED for item in stream.work_items
        ):
            violations.setdefault(stream.id, []).append(
                "approved workstream contains a non-approved work item"
            )
        for item in stream.work_items:
            if (
                item.status != WorkStatus.APPROVED
                or item.metadata.get("contract_mode") != "typed"
            ):
                continue
            item_evidence = evidence.get(item.id, {})
            missing: list[str] = []
            if not item_evidence.get("accepted"):
                missing.append("accepted evidence")
            if item_evidence.get("reviewer_verdict") != "approved":
                missing.append("reviewer approval")
            contract = item.contract
            if contract is not None and contract.test_requirements:
                test_status = str(
                    item_evidence.get("test_status") or ""
                ).casefold()
                syntax_status = str(
                    item_evidence.get("syntax_status") or ""
                ).casefold()
                syntax_only = all(
                    any(
                        token in requirement.casefold()
                        for token in ("syntax", "parse", "compile")
                    )
                    for requirement in contract.test_requirements
                )
                if test_status != "passed" and not (
                    syntax_only and syntax_status == "passed"
                ):
                    missing.append("passing test evidence")
            if contract is not None and contract.evidence_requirements and not (
                item_evidence.get("patch_sha256")
                or item_evidence.get("after_sha256")
                or item_evidence.get("effect_id")
            ):
                missing.append("durable artifact evidence")
            if missing:
                violations[item.id] = missing
    return violations


def reconcile_completion(
    plan: TaskPlan,
    *,
    item_evidence: Mapping[str, Mapping[str, Any]] | None = None,
    call_outcomes: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Prove that every planned entity has exactly one terminal outcome."""
    evidence = item_evidence or {}
    stream_values: dict[str, str] = {}
    item_values: dict[str, str] = {}
    agent_values: dict[str, str] = {
        "director:root": (
            "completed" if plan.status == PlanStatus.COMPLETED else "failed"
        )
    }

    for stream in plan.workstreams:
        stream_bucket = _status_bucket(stream.status, stream.metadata)
        stream_values[stream.id] = stream_bucket
        agent_values[f"manager:{stream.id}"] = stream_bucket
        agent_values[f"tester:{stream.id}"] = stream_bucket
        for item in stream.work_items:
            item_bucket = _status_bucket(item.status, evidence.get(item.id, {}))
            item_values[item.id] = item_bucket
            agent_values[f"worker:{item.id}"] = item_bucket

    call_values = dict(call_outcomes or {})
    sections = {
        "workstreams": _partition(stream_values),
        "work_items": _partition(item_values),
        "agents": _partition(agent_values),
        "calls": _partition(call_values),
    }
    contract_violations = _typed_contract_approval_errors(plan, evidence)
    errors = [
        f"{name} terminal partition is incomplete"
        for name, section in sections.items()
        if not section["balanced"]
    ]
    if contract_violations:
        errors.append("typed contract approval evidence is incomplete")
    return {
        "balanced": not errors,
        "errors": errors,
        "contract_violations": contract_violations,
        **sections,
    }
