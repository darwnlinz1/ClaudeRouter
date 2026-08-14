"""Completion reconciliation for hierarchical plans and logical model calls."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from . import test_evidence
from .models import PlanStatus, TaskPlan, WorkStatus

_BUCKETS = (
    "completed",
    "abandoned",
    "skipped",
    "blocked",
    "failed",
    "preflight_failed",
)
_ALLOWED_STARTED_WITHOUT_CALL = frozenset(
    {
        "account_exhausted",
        "account_unavailable",
        "cancelled",
        "canceled",
        "agent_abandoned",
        "remediation_exhausted",
        "provider_unavailable",
    }
)


def _status_bucket(status: WorkStatus, evidence: Mapping[str, Any]) -> str:
    if status == WorkStatus.APPROVED:
        return "completed"
    if status == WorkStatus.ABANDONED:
        return "abandoned"
    if status == WorkStatus.SKIPPED:
        return "skipped"
    if status == WorkStatus.BLOCKED:
        return "blocked"
    if status == WorkStatus.CANCELLED:
        return "skipped"
    if status == WorkStatus.FAILED and evidence.get("failure_kind") == "preflight":
        return "preflight_failed"
    if status == WorkStatus.FAILED:
        return "failed"
    return "nonterminal"


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
        "covered": terminal == planned and not invalid,
        "successful": counts["completed"] == planned and not invalid,
        "ids": ids,
        "invalid_ids": sorted(invalid),
    }


def _manager_report_coverage(
    expected_manager_ids: Iterable[str],
    reports: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    expected = tuple(dict.fromkeys(str(value) for value in expected_manager_ids))
    counts: dict[str, int] = {}
    statuses: dict[str, str] = {}
    unexpected: list[str] = []
    for report in reports:
        manager_id = str(report.get("manager_id") or report.get("agent_instance_id") or "")
        if not manager_id:
            unexpected.append("<missing-manager-id>")
            continue
        counts[manager_id] = counts.get(manager_id, 0) + 1
        report_status = str(report.get("status") or "abandoned").casefold()
        statuses[manager_id] = (
            "completed"
            if report_status == "completed"
            else "abandoned"
            if report_status in {"partial", "abandoned"}
            else "skipped"
            if report_status == "skipped"
            else "failed"
        )
        if manager_id not in expected:
            unexpected.append(manager_id)

    values = {
        manager_id: statuses.get(manager_id, "nonterminal")
        for manager_id in expected
    }
    section = _partition(values)
    duplicates = sorted(manager_id for manager_id, count in counts.items() if count != 1)
    reported = sorted(manager_id for manager_id in expected if counts.get(manager_id) == 1)
    barrier = {
        "expected_manager_ids": list(expected),
        "reported_manager_ids": reported,
        "expected_count": len(expected),
        "reported_count": len(reported),
        "duplicates": duplicates,
        "unexpected_manager_ids": sorted(unexpected),
        "satisfied": (
            len(reported) == len(expected)
            and not duplicates
            and not unexpected
        ),
    }
    errors: list[str] = []
    if not barrier["satisfied"]:
        errors.append("manager report barrier is incomplete")
    return section, barrier, errors


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
            if item.status != WorkStatus.APPROVED or item.metadata.get("contract_mode") != "typed":
                continue
            item_evidence = evidence.get(item.id, {})
            missing: list[str] = []
            if not item_evidence.get("accepted"):
                missing.append("accepted evidence")
            if item_evidence.get("reviewer_verdict") != "approved":
                missing.append("reviewer approval")
            contract = item.contract
            if contract is not None and contract.test_requirements:
                if not test_evidence.item_test_evidence_complete(
                    contract.test_requirements,
                    item_evidence,
                    allow_deferred_integration=False,
                ):
                    missing.append("passing test evidence")
            if (
                contract is not None
                and contract.evidence_requirements
                and not (
                    item_evidence.get("patch_sha256")
                    or item_evidence.get("after_sha256")
                    or item_evidence.get("effect_id")
                )
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
    director_agent_id: str | None = None,
    called_agent_ids: Iterable[str] | None = None,
    started_agent_ids: Iterable[str] | None = None,
    agent_dispositions: Mapping[str, str] | None = None,
    agent_no_call_reasons: Mapping[str, str] | None = None,
    agent_call_purposes: Mapping[str, Iterable[str]] | None = None,
    expected_manager_ids: Iterable[str] | None = None,
    manager_terminal_reports: Iterable[Mapping[str, Any]] | None = None,
    director_final_review_count: int | None = None,
) -> dict[str, Any]:
    """Prove terminal coverage separately from successful completion."""
    evidence = item_evidence or {}
    stream_values: dict[str, str] = {}
    item_values: dict[str, str] = {}
    agent_values: dict[str, str] = {
        director_agent_id or "director:root": (
            "completed"
            if plan.status in {PlanStatus.COMPLETED, PlanStatus.PARTIAL}
            else "skipped"
            if plan.status == PlanStatus.CANCELLED
            else "failed"
        )
    }

    for stream in plan.workstreams:
        stream_bucket = _status_bucket(stream.status, stream.metadata)
        stream_values[stream.id] = stream_bucket
        manager_id = stream.manager_agent_id or f"manager:{stream.id}"
        tester_id = str(stream.metadata.get("tester_agent_id") or f"tester:{stream.id}")
        agent_values[manager_id] = stream_bucket
        agent_values[tester_id] = stream_bucket
        for item in stream.work_items:
            item_bucket = _status_bucket(item.status, evidence.get(item.id, {}))
            item_values[item.id] = item_bucket
            worker_id = str(item.metadata.get("worker_agent_id") or f"worker:{item.id}")
            agent_values[worker_id] = item_bucket

    call_values = dict(call_outcomes or {})
    sections = {
        "workstreams": _partition(stream_values),
        "work_items": _partition(item_values),
        "agents": _partition(agent_values),
        "calls": _partition(call_values),
    }
    if called_agent_ids is not None or agent_dispositions is not None:
        called = {str(value) for value in called_agent_ids or ()}
        dispositions = {
            str(agent_id): str(bucket) for agent_id, bucket in (agent_dispositions or {}).items()
        }
        coverage_values = {
            agent_id: (
                "completed"
                if agent_id in called
                else dispositions.get(
                    agent_id,
                    agent_values[agent_id]
                    if agent_values[agent_id] != "completed"
                    else "nonterminal",
                )
            )
            for agent_id in agent_values
        }
        sections["agent_calls"] = _partition(coverage_values)
    if started_agent_ids is not None:
        called = {str(value) for value in called_agent_ids or ()}
        reasons = {
            str(agent_id): str(reason).casefold()
            for agent_id, reason in (agent_no_call_reasons or {}).items()
        }
        started_coverage = {
            str(agent_id): (
                "completed"
                if str(agent_id) in called
                else "skipped"
                if reasons.get(str(agent_id)) in {"cancelled", "canceled"}
                else "failed"
                if reasons.get(str(agent_id)) in _ALLOWED_STARTED_WITHOUT_CALL
                else "nonterminal"
            )
            for agent_id in started_agent_ids
        }
        sections["started_agent_calls"] = _partition(started_coverage)
    report_barrier: dict[str, Any] = {
        "expected_manager_ids": [],
        "reported_manager_ids": [],
        "expected_count": 0,
        "reported_count": 0,
        "duplicates": [],
        "unexpected_manager_ids": [],
        "satisfied": True,
        "enforced": False,
    }
    coverage_errors: list[str] = [
        f"{name} terminal partition is incomplete"
        for name, section in sections.items()
        if not section["covered"]
    ]
    if expected_manager_ids is not None or manager_terminal_reports is not None:
        manager_reports, report_barrier, barrier_errors = _manager_report_coverage(
            expected_manager_ids or (),
            manager_terminal_reports or (),
        )
        report_barrier["enforced"] = True
        sections["manager_reports"] = manager_reports
        coverage_errors.extend(barrier_errors)

    final_review = {
        "count": director_final_review_count,
        "exactly_once": director_final_review_count == 1,
        "enforced": director_final_review_count is not None,
    }
    if (
        director_final_review_count is not None
        and plan.status in {PlanStatus.COMPLETED, PlanStatus.PARTIAL}
        and director_final_review_count != 1
    ):
        coverage_errors.append("director final review count must equal one")

    contract_violations = _typed_contract_approval_errors(plan, evidence)
    errors = list(coverage_errors)
    # Typed-contract proof is required to claim COMPLETED success. A covered
    # PARTIAL (N/N manager reports, abandoned/skipped work, one Director
    # review) must stay balanced so TaskManager does not rewrite it to FAILED.
    if contract_violations and plan.status != PlanStatus.PARTIAL:
        errors.append("typed contract approval evidence is incomplete")
    covered = not coverage_errors
    successful = (
        covered
        and not contract_violations
        and plan.status == PlanStatus.COMPLETED
        and sections["workstreams"]["successful"]
        and sections["work_items"]["successful"]
        and (not report_barrier["enforced"] or sections["manager_reports"]["successful"])
        and (not final_review["enforced"] or final_review["exactly_once"])
    )
    return {
        "balanced": not errors,
        "covered": covered,
        "successful": successful,
        "errors": errors,
        "contract_violations": contract_violations,
        "report_barrier": report_barrier,
        "director_final_review": final_review,
        "agent_call_purposes": {
            str(agent_id): sorted({str(purpose) for purpose in purposes})
            for agent_id, purposes in (agent_call_purposes or {}).items()
        },
        **sections,
    }
