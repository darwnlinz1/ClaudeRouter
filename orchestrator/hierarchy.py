"""Director -> Managers -> Workers -> Testers hierarchical runtime."""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import re
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable

from . import (
    config,
    context_builder,
    file_agent,
    llm_client,
    path_utils,
    retry_policy,
    safety,
    state_store,
    test_evidence,
    worker_targets,
)
from .llm_client import ToolCallResult, call_agent
from .models import (
    ApprovalPolicy,
    Attempt,
    AttemptStatus,
    HandoffEnvelope,
    PlanStatus,
    RiskLevel,
    TaskPlan,
    WorkContract,
    WorkItem,
    WorkStatus,
    Workstream,
    canonical_json,
    new_id,
    to_dict,
    utc_now,
    work_contract_sha256,
)
from .orchestrator import SessionResult, TurnOutcome
from .reconciliation import reconcile_completion
from .scheduler import (
    HierarchicalScheduler,
    SchedulerLimits,
    SchedulingError,
    max_parallelism_enabled,
    ready_work_items,
    scopes_conflict,
    validate_plan,
)
from .state_repository import StateRepository
from .system_prompt_director import (
    PLAN_PROMPT as DIRECTOR_PLAN_PROMPT,
)
from .system_prompt_director import (
    REVIEW_PROMPT as DIRECTOR_REVIEW_PROMPT,
)
from .system_prompt_manager import (
    PLAN_PROMPT as MANAGER_PLAN_PROMPT,
)
from .system_prompt_manager import (
    REVIEW_PROMPT as MANAGER_REVIEW_PROMPT,
)
from .ticket_executor import TicketExecutionResult, execute_work_item
from .tools_schema import (
    DIRECTOR_PLAN_TOOLS,
    DIRECTOR_REVIEW_TOOLS,
    MANAGER_PLAN_TOOLS,
    MANAGER_REVIEW_TOOLS,
)

logger = logging.getLogger(__name__)


def _worker_launch_stagger_seconds() -> float:
    """Spacing between worker launches inside one batch.

    Every selected worker still starts immediately; this only stops a batch
    from opening all of its provider connections in the same millisecond.
    """
    if max_parallelism_enabled():
        return 0.0
    try:
        configured = float(os.environ.get("ORCH_WORKER_LAUNCH_STAGGER_SECONDS", "1.5"))
    except ValueError:
        configured = 1.5
    return min(max(0.0, configured), 30.0)


def _sleep_unless_cancelled(seconds: float, cancelled: Callable[[], bool]) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if cancelled():
            return
        time.sleep(min(0.25, max(0.01, deadline - time.monotonic())))


def _log_terminal(tag: str, message: str) -> None:
    """One line per milestone so a run can be followed in the terminal.

    Only the account filename is ever printed, never credential contents.
    """
    logger.info("[%s] %s", tag, message)


LLMCallFn = Callable[
    [str, str, list[dict[str, Any]]],
    ToolCallResult,
]
EventSink = Callable[[dict[str, Any]], None]
ApprovedFileSink = Callable[[str], None]
ApprovalCallback = Callable[[dict[str, Any]], bool]
CancellationCallback = Callable[[], bool]
AgentConfigResolver = Callable[
    [str, str, str, str],
    tuple[str, str],
]


class _AccountPoolFatalSignal(BaseException):
    """Carry account exhaustion through legacy ``except Exception`` layers."""

    def __init__(self, cause: llm_client.AccountPoolExhaustedError) -> None:
        super().__init__(str(cause))
        self.cause = cause


_PROJECT_LOCKS_GUARD = threading.RLock()
_PROJECT_LOCKS: dict[str, threading.RLock] = {}
_PLANNER_ID = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
_TYPED_CONTRACT_FIELDS = frozenset(
    {
        "contract_id",
        "contract_version",
        "input_artifacts",
        "expected_outputs",
        "read_scopes",
        "write_scopes",
        "acceptance_criteria",
        "test_requirements",
        "evidence_requirements",
        "consumers",
        "risk_level",
        "priority",
    }
)


def _project_lock_for(root: Path) -> threading.RLock:
    key = str(root.resolve()).casefold()
    with _PROJECT_LOCKS_GUARD:
        return _PROJECT_LOCKS.setdefault(key, threading.RLock())


def _default_llm_call(
    system_prompt: str,
    user_message: str,
    tools: list[dict[str, Any]],
) -> ToolCallResult:
    return call_agent(system_prompt, user_message, tools)


def _emit(on_event: EventSink | None, event: dict[str, Any]) -> None:
    if on_event is not None:
        on_event(event)


def _configure_role_thread(
    *,
    role: str,
    model: str,
    effort: str,
    worker_model: str,
    worker_effort: str,
    reviewer_model: str,
    reviewer_effort: str,
    event_sink: EventSink | None,
    account_mode: str | None = None,
) -> None:
    local = llm_client.thread_local
    local.agent_role = role
    setattr(local, f"{role}_model", model)
    setattr(local, f"{role}_effort", effort)
    # Tester shares the reviewer model selectors in settings/UI.
    if role in {"tester", "reviewer"}:
        local.tester_model = model
        local.tester_effort = effort
        local.reviewer_model = model
        local.reviewer_effort = effort
    else:
        local.worker_model = worker_model
        local.worker_effort = worker_effort
        local.reviewer_model = reviewer_model
        local.reviewer_effort = reviewer_effort
        local.tester_model = reviewer_model
        local.tester_effort = reviewer_effort
    local.event_sink = event_sink
    if account_mode is not None:
        local.account_mode = account_mode
    elif not getattr(local, "account_mode", None):
        local.account_mode = "sticky"


def _resolve_agent_config(
    resolver: AgentConfigResolver | None,
    *,
    agent_id: str,
    role: str,
    model: str,
    effort: str,
) -> tuple[str, str]:
    if resolver is None:
        return model, effort
    selected_model, selected_effort = resolver(
        agent_id,
        role,
        model,
        effort,
    )
    return str(selected_model or model), str(selected_effort or effort)


def _require_planner_ids(values: list[dict[str, Any]], *, label: str) -> None:
    """Reject ambiguous planner IDs before they enter dependency scheduling."""
    ids = [str(value.get("id", "")) for value in values]
    invalid = [value for value in ids if not _PLANNER_ID.fullmatch(value)]
    if invalid:
        raise RuntimeError(f"{label} id must be lowercase kebab-case; invalid: {invalid}")
    if len(ids) != len(set(ids)):
        raise RuntimeError(f"{label} ids must be unique")


def _has_explicit_contract(value: dict[str, Any]) -> bool:
    return _TYPED_CONTRACT_FIELDS.issubset(value)


def _planner_selection_error(
    result: ToolCallResult,
    *,
    list_field: str,
    count_field: str,
    maximum: int,
    label: str,
) -> str | None:
    values = result.tool_input.get(list_field)
    declared = result.tool_input.get(count_field)
    actual = len(values) if isinstance(values, list) else 0
    if actual < 1:
        return f"{label} must select at least one assignment."
    if not isinstance(declared, int) or isinstance(declared, bool):
        return f"{label} {count_field} must be an integer."
    if declared != actual:
        return (
            f"{label} count contract violated: {count_field} must equal "
            f"{list_field}.length; "
            f"{count_field}={declared!r}, {list_field}.length={actual}."
        )
    if actual > maximum:
        return f"{label} fan-out exceeds maximum {maximum}: {list_field}.length={actual}."
    return None


def _logical_agent_id(task_id: str, role: str, assignment_id: str) -> str:
    """Return a stable logical identity across retries and task resume."""
    digest = hashlib.sha256(f"{task_id}\0{role}\0{assignment_id}".encode("utf-8")).hexdigest()[:24]
    return f"{role}_{digest}"


def _call_repository_hook(
    hook: Callable[..., Any],
    *,
    values: dict[str, Any],
    preferred_args: tuple[Any, ...],
) -> Any:
    """Invoke an additive repository hook without coupling to one rollout shape."""
    try:
        signature = inspect.signature(hook)
    except (TypeError, ValueError):
        return hook(*preferred_args)
    parameters = tuple(signature.parameters.values())
    if all(
        parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
        for parameter in parameters
    ):
        return hook(*preferred_args)
    kwargs: dict[str, Any] = {}
    missing_required = False
    for parameter in parameters:
        if parameter.kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            continue
        if parameter.name in values:
            kwargs[parameter.name] = values[parameter.name]
        elif parameter.default is inspect.Parameter.empty:
            missing_required = True
            break
    if not missing_required:
        return hook(**kwargs)
    for count in range(len(preferred_args), -1, -1):
        candidate = preferred_args[:count]
        try:
            signature.bind(*candidate)
        except TypeError:
            continue
        return hook(*candidate)
    raise TypeError(f"Unsupported repository hook signature: {signature}")


def _resolve_repository_agent_id(
    repository: Any,
    *,
    task_id: str,
    role: str,
    assignment_id: str,
) -> str:
    """Resolve one stable logical identity, with a deterministic legacy fallback."""
    fallback = _logical_agent_id(task_id, role, assignment_id)
    hook = getattr(repository, "resolve_agent_identity", None)
    if not callable(hook):
        return fallback
    resolved = _call_repository_hook(
        hook,
        values={
            "task_id": task_id,
            "role": role,
            "assignment_id": assignment_id,
            "logical_agent_id": fallback,
            "agent_id": fallback,
            "proposed_id": fallback,
            "proposed_agent_id": fallback,
            "preferred_id": fallback,
        },
        preferred_args=(task_id, role, assignment_id, fallback),
    )
    if isinstance(resolved, dict):
        resolved = (
            resolved.get("logical_agent_id") or resolved.get("agent_id") or resolved.get("id")
        )
    elif not isinstance(resolved, str):
        resolved = (
            getattr(resolved, "logical_agent_id", None)
            or getattr(resolved, "agent_id", None)
            or getattr(resolved, "id", None)
        )
    value = str(resolved or fallback).strip()
    if not value:
        raise RuntimeError("resolve_agent_identity returned an empty identity")
    return value


def _persist_contract_version(
    repository: Any,
    *,
    task_id: str,
    contract: WorkContract,
    workstream_id: str,
    work_item_id: str | None = None,
) -> None:
    """Persist an immutable contract version when the repository supports it."""
    hook = getattr(repository, "save_contract_version", None)
    if not callable(hook):
        return
    digest = work_contract_sha256(contract)
    payload = canonical_json(contract)
    owner_type = "work_item" if work_item_id is not None else "workstream"
    owner_id = work_item_id or workstream_id
    _call_repository_hook(
        hook,
        values={
            "task_id": task_id,
            "contract": contract,
            "work_contract": contract,
            "contract_id": contract.id,
            "version": contract.version,
            "contract_version": contract.version,
            "canonical_json": payload,
            "contract_json": payload,
            "sha256": digest,
            "contract_sha256": digest,
            "workstream_id": workstream_id,
            "work_item_id": work_item_id,
            "owner_type": owner_type,
            "owner_id": owner_id,
        },
        preferred_args=(task_id, contract),
    )


def _persist_plan_contract_versions(repository: Any, plan: TaskPlan) -> None:
    for stream in plan.workstreams:
        assert stream.contract is not None
        _persist_contract_version(
            repository,
            task_id=plan.task_id,
            contract=stream.contract,
            workstream_id=stream.id,
        )
        for item in stream.work_items:
            assert item.contract is not None
            _persist_contract_version(
                repository,
                task_id=plan.task_id,
                contract=item.contract,
                workstream_id=stream.id,
                work_item_id=item.id,
            )


def _advance_conflicting_contract_versions(
    repository: Any,
    plan: TaskPlan,
) -> TaskPlan:
    """Make planner contract revisions monotonic before immutable persistence."""

    get_contract = getattr(repository, "get_contract", None)
    if not callable(get_contract):
        return plan

    def reconcile(contract: WorkContract) -> WorkContract:
        latest = get_contract(plan.task_id, contract.id)
        if not isinstance(latest, WorkContract):
            return contract
        if replace(contract, version=latest.version) == latest:
            return latest
        if contract.version <= latest.version:
            return replace(contract, version=latest.version + 1)
        return contract

    streams: list[Workstream] = []
    for stream in plan.workstreams:
        assert stream.contract is not None
        stream_contract = reconcile(stream.contract)
        items: list[WorkItem] = []
        for item in stream.work_items:
            assert item.contract is not None
            item_contract = reconcile(item.contract)
            items.append(
                replace(
                    item,
                    contract=item_contract,
                    metadata={
                        **dict(item.metadata),
                        "work_contract": to_dict(item_contract),
                    },
                )
            )
        streams.append(
            replace(
                stream,
                contract=stream_contract,
                work_items=tuple(items),
                metadata={
                    **dict(stream.metadata),
                    "work_contract": to_dict(stream_contract),
                },
            )
        )
    return replace(plan, workstreams=tuple(streams))


def _append_repository_handoff(repository: Any, handoff: HandoffEnvelope) -> None:
    """Append a typed handoff when the additive persistence hook is available."""
    hook = getattr(repository, "append_handoff", None)
    if not callable(hook):
        return
    _call_repository_hook(
        hook,
        values={"handoff": handoff, "envelope": handoff},
        preferred_args=(handoff,),
    )


def _list_repository_handoffs(repository: Any, task_id: str) -> list[Any]:
    """Load persisted handoffs on resume, or safely degrade for old stores."""
    hook = getattr(repository, "list_handoffs", None)
    if not callable(hook):
        return []
    values = _call_repository_hook(
        hook,
        values={"task_id": task_id},
        preferred_args=(task_id,),
    )
    return list(values or ())


def _optional_repository_hook(
    repository: Any,
    names: tuple[str, ...],
    *,
    values: dict[str, Any],
    preferred_args: tuple[Any, ...],
) -> Any:
    """Call the first additive persistence API present on a rollout repository."""
    for name in names:
        hook = getattr(repository, name, None)
        if callable(hook):
            return _call_repository_hook(
                hook,
                values=values,
                preferred_args=preferred_args,
            )
    return None


def _persist_execution_epoch(repository: Any, payload: Mapping[str, Any]) -> None:
    epoch_id = str(
        payload.get("execution_epoch_id") or payload.get("execution_epoch") or ""
    )
    _optional_repository_hook(
        repository,
        ("create_execution_epoch", "begin_execution_epoch", "save_execution_epoch"),
        values={
            **dict(payload),
            "execution_epoch_id": epoch_id,
            "epoch_id": epoch_id,
            "metadata": {
                "expected_manager_ids": list(
                    payload.get("expected_manager_ids") or []
                )
            },
            "payload": dict(payload),
            "epoch": dict(payload),
        },
        preferred_args=(
            str(payload.get("task_id") or ""),
            epoch_id,
        ),
    )
    roster = list(payload.get("roster") or ())
    if roster:
        _optional_repository_hook(
            repository,
            (
                "freeze_execution_roster",
                "save_execution_roster",
                "freeze_manager_roster",
            ),
            values={
                "task_id": str(payload.get("task_id") or ""),
                "execution_epoch_id": epoch_id,
                "execution_epoch": epoch_id,
                "epoch_id": epoch_id,
                "roster": roster,
            },
            preferred_args=(
                str(payload.get("task_id") or ""),
                epoch_id,
                roster,
            ),
        )


def _persist_manager_terminal_report(repository: Any, report: Mapping[str, Any]) -> None:
    epoch_id = str(
        report.get("execution_epoch_id") or report.get("execution_epoch") or ""
    )
    _optional_repository_hook(
        repository,
        (
            "record_manager_terminal_report",
            "save_manager_terminal_report",
            "settle_manager_report",
        ),
        values={
            **dict(report),
            "execution_epoch_id": epoch_id,
            "epoch_id": epoch_id,
            "manager_agent_id": report.get("manager_id"),
            "disposition": report.get("status"),
            "terminal_log_refs": list(report.get("log_refs") or []),
            "report": dict(report),
            "payload": dict(report),
        },
        preferred_args=(dict(report),),
    )


def _persist_manager_report_barrier(repository: Any, barrier: Mapping[str, Any]) -> None:
    epoch_id = str(
        barrier.get("execution_epoch_id") or barrier.get("execution_epoch") or ""
    )
    durable = _optional_repository_hook(
        repository,
        (
            "manager_report_barrier",
            "get_manager_report_barrier",
            "manager_reports_barrier",
            "save_manager_report_barrier",
            "save_report_barrier",
        ),
        values={
            **dict(barrier),
            "execution_epoch_id": epoch_id,
            "epoch_id": epoch_id,
            "barrier": dict(barrier),
            "payload": dict(barrier),
        },
        preferred_args=(
            str(barrier.get("task_id") or ""),
            epoch_id,
        ),
    )
    if isinstance(durable, Mapping) and durable.get("satisfied") is False:
        raise RuntimeError("Durable Manager report barrier is incomplete")


def _persist_director_final_review(repository: Any, review: Mapping[str, Any]) -> None:
    epoch_id = str(
        review.get("execution_epoch_id") or review.get("execution_epoch") or ""
    )
    _optional_repository_hook(
        repository,
        (
            "record_director_final_review",
            "save_director_final_review",
        ),
        values={
            **dict(review),
            "execution_epoch_id": epoch_id,
            "epoch_id": epoch_id,
            "review": dict(review),
            "terminal_disposition": review.get("verdict"),
            "terminal_log_refs": list(review.get("log_refs") or []),
            "payload": dict(review),
        },
        preferred_args=(dict(review),),
    )


def _reserve_director_final_review(
    repository: Any,
    *,
    task_id: str,
    execution_epoch: str,
    review_context: Mapping[str, Any],
) -> bool:
    for name in (
        "reserve_director_final_review",
        "begin_director_final_review",
        "reserve_final_review",
    ):
        hook = getattr(repository, name, None)
        if not callable(hook):
            continue
        reserved = _call_repository_hook(
            hook,
            values={
                "task_id": task_id,
                "execution_epoch_id": execution_epoch,
                "execution_epoch": execution_epoch,
                "epoch_id": execution_epoch,
                "review_context": dict(review_context),
                "context": dict(review_context),
            },
            preferred_args=(task_id, execution_epoch),
        )
        return reserved is not None
    return True


def _manager_log_references(
    repository: Any,
    *,
    task_id: str,
    agent_ids: Iterable[str],
) -> tuple[str, ...]:
    hook = getattr(repository, "list_log_records", None)
    if not callable(hook):
        return ()
    records = _call_repository_hook(
        hook,
        values={"task_id": task_id, "limit": 10000},
        preferred_args=(task_id,),
    )
    wanted = {str(value) for value in agent_ids if value}
    references = {
        str(record.get("path") or record.get("log_id") or "")
        for record in records or ()
        if isinstance(record, Mapping)
        and str(record.get("agent_instance_id") or "") in wanted
        and (record.get("path") or record.get("log_id"))
    }
    return tuple(sorted(references))


def _pin_manager_log_references(
    repository: Any,
    *,
    task_id: str,
    execution_epoch: str,
    manager_id: str,
    log_refs: tuple[str, ...],
) -> None:
    if not log_refs:
        return
    _optional_repository_hook(
        repository,
        ("pin_log_references", "pin_terminal_logs"),
        values={
            "task_id": task_id,
            "execution_epoch": execution_epoch,
            "manager_id": manager_id,
            "log_refs": list(log_refs),
            "references": list(log_refs),
            "reason": "manager_terminal_report",
            "terminal_evidence": True,
            "pinned": True,
        },
        preferred_args=(task_id, list(log_refs)),
    )


def _persist_terminal_disposition(
    repository: Any,
    *,
    task_id: str,
    execution_epoch: str,
    entity_kind: str,
    entity_id: str,
    disposition: str,
    logical_agent_id: str | None,
    reason_code: str,
    summary: str,
    log_refs: Iterable[str] = (),
    metadata: Mapping[str, Any] | None = None,
) -> None:
    payload = {
        "task_id": task_id,
        "execution_epoch_id": execution_epoch,
        "execution_epoch": execution_epoch,
        "epoch_id": execution_epoch,
        "entity_kind": entity_kind,
        "entity_id": entity_id,
        "disposition": disposition,
        "logical_agent_id": logical_agent_id,
        "reason_code": reason_code,
        "summary": summary,
        "terminal_log_refs": list(log_refs),
        "log_refs": list(log_refs),
        "metadata": dict(metadata or {}),
    }
    _optional_repository_hook(
        repository,
        (
            "record_terminal_disposition",
            "save_terminal_disposition",
            "record_agent_terminal_disposition",
        ),
        values={**payload, "payload": payload},
        preferred_args=(payload,),
    )


def _complete_execution_epoch(
    repository: Any,
    *,
    task_id: str,
    execution_epoch: str,
    disposition: str,
    terminal_log_refs: Iterable[str] = (),
) -> None:
    _optional_repository_hook(
        repository,
        ("complete_execution_epoch", "finish_execution_epoch"),
        values={
            "task_id": task_id,
            "execution_epoch_id": execution_epoch,
            "execution_epoch": execution_epoch,
            "epoch_id": execution_epoch,
            "disposition": disposition,
            "terminal_log_refs": list(terminal_log_refs),
        },
        preferred_args=(task_id, execution_epoch),
    )


@dataclass(frozen=True, slots=True)
class _RemediationDecision:
    action: str
    reason: str
    instructions: str = ""
    remediation_attempt_id: str | None = None

    @property
    def retries(self) -> bool:
        return self.action in {"retry", "replan", "remediate"}


@dataclass(slots=True)
class _FallbackCrisisStrategy:
    """Retry a novel retryable crisis; exhaust repeated identical failures."""

    seen_fingerprints: set[str] = field(default_factory=set)
    lock: threading.RLock = field(default_factory=threading.RLock)

    def next_remediation(self, context: Mapping[str, Any]) -> dict[str, str]:
        if not bool(context.get("retryable")):
            return {"action": "abandon", "reason": "crisis_not_retryable"}
        fingerprint_payload = {
            "scope": context.get("scope"),
            "failure_kind": context.get("failure_kind"),
            "affected_work_item_ids": sorted(context.get("affected_work_item_ids") or ()),
            "errors": context.get("errors") or {},
        }
        fingerprint = hashlib.sha256(
            canonical_json(fingerprint_payload).encode("utf-8")
        ).hexdigest()
        with self.lock:
            if fingerprint in self.seen_fingerprints:
                return {
                    "action": "abandon",
                    "reason": "repeated_crisis_without_new_strategy",
                }
            self.seen_fingerprints.add(fingerprint)
        return {
            "action": "retry",
            "reason": "novel_retryable_crisis",
            "instructions": str(context.get("suggested_instructions") or ""),
        }


def _decide_remediation(
    crisis_strategy: Any,
    context: Mapping[str, Any],
) -> _RemediationDecision:
    hook = None
    for name in ("next_remediation", "decide_remediation"):
        candidate = getattr(crisis_strategy, name, None)
        if callable(candidate):
            hook = candidate
            break
    if hook is None and callable(crisis_strategy):
        hook = crisis_strategy
    if hook is None:
        return _RemediationDecision("abandon", "no_crisis_strategy_hook")
    raw = _call_repository_hook(
        hook,
        values={
            **dict(context),
            "context": dict(context),
            "crisis": dict(context),
        },
        preferred_args=(dict(context),),
    )
    if isinstance(raw, bool):
        return _RemediationDecision(
            "retry" if raw else "abandon",
            "strategy_retry" if raw else "strategy_exhausted",
        )
    if isinstance(raw, str):
        return _RemediationDecision(raw.casefold(), f"strategy_{raw.casefold()}")
    if isinstance(raw, Mapping):
        return _RemediationDecision(
            str(raw.get("action") or raw.get("decision") or "abandon").casefold(),
            str(raw.get("reason") or "strategy_decision"),
            str(raw.get("instructions") or raw.get("next_instructions") or ""),
            str(raw.get("remediation_attempt_id") or "") or None,
        )
    return _RemediationDecision("abandon", "strategy_returned_no_remediation")


def _reserve_remediation_attempt(
    repository: Any,
    *,
    context: Mapping[str, Any],
    decision: _RemediationDecision,
) -> tuple[_RemediationDecision, str | None]:
    if not decision.retries:
        return decision, None
    if decision.remediation_attempt_id:
        return decision, decision.remediation_attempt_id
    hook = None
    for name in (
        "reserve_remediation_attempt",
        "begin_remediation_attempt",
        "claim_remediation_strategy",
    ):
        candidate = getattr(repository, name, None)
        if callable(candidate):
            hook = candidate
            break
    if hook is None:
        return decision, None
    failure_payload = {
        "scope": context.get("scope"),
        "failure_kind": context.get("failure_kind"),
        "errors": context.get("errors") or {},
        "affected_work_item_ids": sorted(
            context.get("affected_work_item_ids") or ()
        ),
    }
    failure_signature = hashlib.sha256(
        canonical_json(failure_payload).encode("utf-8")
    ).hexdigest()
    strategy_name = (
        decision.action
        + ":"
        + hashlib.sha256(decision.instructions.encode("utf-8")).hexdigest()[:16]
    )
    logical_agent_id = str(
        context.get("manager_id")
        or context.get("agent_instance_id")
        or context.get("director_id")
        or "director"
    )
    reserved = _call_repository_hook(
        hook,
        values={
            "task_id": str(context.get("task_id") or ""),
            "logical_agent_id": logical_agent_id,
            "failure_signature": failure_signature,
            "strategy": strategy_name,
            "execution_epoch_id": context.get("execution_epoch"),
            "execution_epoch": context.get("execution_epoch"),
            "category": context.get("scope"),
            "details": dict(context),
            "terminal_log_refs": [],
        },
        preferred_args=(
            str(context.get("task_id") or ""),
            logical_agent_id,
            failure_signature,
            strategy_name,
        ),
    )
    if reserved is None:
        return (
            _RemediationDecision(
                "abandon",
                "remediation_strategy_already_used_for_failure",
            ),
            None,
        )
    attempt_id = (
        reserved.get("remediation_attempt_id")
        if isinstance(reserved, Mapping)
        else getattr(reserved, "remediation_attempt_id", None)
    )
    return decision, str(attempt_id or "") or None


def _complete_remediation_attempt(
    repository: Any,
    remediation_attempt_id: str | None,
    *,
    details: Mapping[str, Any],
) -> None:
    if not remediation_attempt_id:
        return
    hook = getattr(repository, "complete_remediation_attempt", None)
    if not callable(hook):
        return
    _call_repository_hook(
        hook,
        values={
            "remediation_attempt_id": remediation_attempt_id,
            "attempt_id": remediation_attempt_id,
            "status": "applied",
            "details": dict(details),
            "terminal_disposition": None,
            "terminal_log_refs": [],
        },
        preferred_args=(remediation_attempt_id,),
    )


def _build_manager_terminal_report(
    *,
    repository: Any,
    task_id: str,
    session_id: str,
    execution_epoch: str,
    stream: Workstream,
    manager_id: str,
    item_statuses: Mapping[str, WorkStatus],
    item_evidence: Mapping[str, Mapping[str, Any]],
    synthesized: bool,
    reasons: Iterable[str] = (),
) -> dict[str, Any]:
    completed_item_ids: list[str] = []
    abandoned_item_ids: list[str] = []
    skipped_item_ids: list[str] = []
    artifacts: set[str] = set()
    report_reasons = {str(reason) for reason in reasons if str(reason).strip()}
    for item in stream.work_items:
        status = WorkStatus(item_statuses.get(item.id, item.status))
        evidence = item_evidence.get(item.id, {})
        if status == WorkStatus.APPROVED:
            completed_item_ids.append(item.id)
            artifacts.update(
                str(value)
                for value in (
                    evidence.get("completed_file_paths")
                    or evidence.get("package_files")
                    or item.write_scopes
                )
                if str(value).strip()
            )
        elif status in {WorkStatus.ABANDONED, WorkStatus.FAILED}:
            abandoned_item_ids.append(item.id)
        else:
            skipped_item_ids.append(item.id)
        if status != WorkStatus.APPROVED:
            reason = (
                evidence.get("error")
                or evidence.get("failure_kind")
                or evidence.get("status")
            )
            if reason:
                report_reasons.add(str(reason))

    if not stream.work_items:
        report_status = (
            "completed"
            if stream.status == WorkStatus.APPROVED
            else "abandoned"
        )
    elif not abandoned_item_ids and not skipped_item_ids:
        report_status = "completed"
    elif completed_item_ids:
        report_status = "partial"
    else:
        report_status = "abandoned"
    child_agent_ids = [
        manager_id,
        str(stream.metadata.get("tester_agent_id") or ""),
        *[
            str(item.metadata.get("worker_agent_id") or "")
            for item in stream.work_items
        ],
    ]
    log_refs = _manager_log_references(
        repository,
        task_id=task_id,
        agent_ids=child_agent_ids,
    )
    report = {
        "type": "manager_terminal_report",
        "task_id": task_id,
        "session_id": session_id,
        "execution_epoch": execution_epoch,
        "execution_epoch_id": execution_epoch,
        "role": "manager",
        "agent_instance_id": manager_id,
        "manager_id": manager_id,
        "workstream_id": stream.id,
        "status": report_status,
        "completed_item_ids": sorted(completed_item_ids),
        "abandoned_item_ids": sorted(abandoned_item_ids),
        "skipped_item_ids": sorted(skipped_item_ids),
        "artifacts": sorted(artifacts),
        "reasons": sorted(report_reasons),
        "log_refs": list(log_refs),
        "synthesized": bool(synthesized),
    }
    _pin_manager_log_references(
        repository,
        task_id=task_id,
        execution_epoch=execution_epoch,
        manager_id=manager_id,
        log_refs=log_refs,
    )
    return report


def _validate_execution_slots(*, label: str, slots: int, maximum: int) -> None:
    if not isinstance(slots, int) or isinstance(slots, bool) or not 1 <= slots <= maximum:
        raise RuntimeError(
            f"{label} execution slots must be between 1 and {maximum}; got {slots!r}"
        )


def _director_plan_from_result(
    result: ToolCallResult,
    *,
    task_id: str,
    session_id: str,
    goal: str,
    revision: int = 1,
    limits: SchedulerLimits | None = None,
    max_manager_count: int | None = None,
    # Compatibility for callers written against the former exact-count API.
    # It is now interpreted as a maximum, never as a quota.
    required_manager_count: int | None = None,
) -> TaskPlan:
    if result.tool_name != "submit_workstream_plan":
        raise RuntimeError(f"Director action không hợp lệ: {result.tool_name}")
    raw = result.tool_input
    raw_streams = list(raw.get("workstreams") or [])
    _require_planner_ids(raw_streams, label="workstream")
    bounds = limits or SchedulerLimits()
    manager_cap = min(
        max_manager_count or required_manager_count or bounds.manager_cap,
        bounds.max_workstreams,
    )
    error = _planner_selection_error(
        result,
        list_field="workstreams",
        count_field="requested_manager_count",
        maximum=manager_cap,
        label="Director",
    )
    if error:
        raise RuntimeError(error)
    _validate_execution_slots(
        label="Manager",
        slots=bounds.max_parallel_managers,
        maximum=manager_cap,
    )
    fanout_reason = str(raw.get("selected_fanout_reason") or raw.get("summary") or "").strip()
    streams = []
    for value in raw_streams:
        stream_id = str(value["id"])
        stream_acceptance = tuple(value["acceptance_criteria"])
        stream_scopes = tuple(value["write_scopes"])
        contract = _planner_contract(
            value,
            fallback_id=stream_id,
            fallback_inputs=tuple(value.get("dependencies") or ()),
            fallback_outputs=stream_scopes or (str(value["goal"]),),
            fallback_write_scopes=stream_scopes,
            fallback_acceptance=stream_acceptance,
            fallback_consumers=("task",),
        )
        streams.append(
            Workstream(
                id=stream_id,
                title=str(value["title"]),
                goal=str(value["goal"]),
                acceptance_criteria=stream_acceptance,
                dependencies=tuple(value["dependencies"]),
                write_scopes=stream_scopes,
                metadata={
                    "director_summary": raw["summary"],
                    "work_contract": to_dict(contract),
                    "contract_mode": (
                        "typed" if _has_explicit_contract(value) else "compatibility"
                    ),
                },
                contract=contract,
            )
        )
    return TaskPlan(
        task_id=task_id,
        session_id=session_id,
        goal=goal,
        workstreams=tuple(streams),
        requested_manager_count=len(streams),
        revision=revision,
        status=PlanStatus.READY,
        metadata={
            "fanout": {
                "manager": {
                    "selected": len(streams),
                    "max": manager_cap,
                    "reason": fanout_reason,
                    "execution_slots": bounds.max_parallel_managers,
                },
                "workers": {},
            },
            "selected_manager_count": len(streams),
            "max_manager_count": manager_cap,
            "selected_fanout_reason": fanout_reason,
            "manager_execution_slots": bounds.max_parallel_managers,
        },
    )


def _declared_worker_target(path: str) -> str:
    """Accept directory-shaped planner targets; reject unsupported concrete files."""
    normalized = str(path).replace("\\", "/").strip()
    if worker_targets.is_possible_directory_target(normalized):
        return normalized
    return worker_targets.ensure_worker_target(normalized)


def _package_files(item: WorkItem, root: Path | None = None) -> list[str]:
    """Resolve ordered file list for a major work package.

    Directory targets such as ``tests/fixtures/`` are expanded into concrete
    text files when ``root`` is provided. An empty or missing directory is
    reported as ``DirectoryWorkerTarget`` so callers can treat it as advisory
    instead of a fatal unsupported Worker target.
    """
    primary = str(item.metadata.get("file_path") or "").replace("\\", "/").strip()
    scopes = [
        str(scope).replace("\\", "/").strip() for scope in item.write_scopes if str(scope).strip()
    ]
    declared: list[str] = []
    if primary:
        declared.append(primary)
    for scope in scopes:
        if scope not in declared:
            declared.append(scope)

    files: list[str] = []
    directory_targets: list[str] = []
    for target in declared:
        if worker_targets.is_directory_shaped_target(target) or (
            root is not None and worker_targets.is_on_disk_directory_target(root, target)
        ):
            if target not in directory_targets:
                directory_targets.append(target)
            continue
        if target not in files:
            files.append(target)

    if directory_targets and root is not None:
        remaining = max(config.MAX_FILES_PER_WORK_PACKAGE - len(files), 0)
        expansions: list[dict[str, Any]] = []
        for directory in directory_targets:
            expanded = worker_targets.expand_directory_worker_target(
                root,
                directory,
                max_files=remaining,
            )
            expansions.append({"from": directory, "files": list(expanded)})
            for path in expanded:
                if path not in files:
                    files.append(path)
                    remaining = max(config.MAX_FILES_PER_WORK_PACKAGE - len(files), 0)
            if remaining == 0:
                break
        if expansions:
            item.metadata["expanded_directory_targets"] = expansions
            if files:
                item.metadata["package_files"] = list(files)
                if primary in directory_targets:
                    item.metadata["directory_target"] = primary
                    item.metadata["file_path"] = files[0]

    if not files:
        if directory_targets:
            raise worker_targets.DirectoryWorkerTarget(
                directory_targets[0],
                "directory target has no concrete text files to expand",
            )
        raise RuntimeError(f"Work item {item.id} thiếu file trong write_scopes/file_path")
    if len(files) > config.MAX_FILES_PER_WORK_PACKAGE:
        raise RuntimeError(
            f"Work item {item.id} declares {len(files)} concrete files; "
            f"limit is {config.MAX_FILES_PER_WORK_PACKAGE}"
        )
    return files


def _normalize_work_item_scopes(value: dict[str, Any]) -> tuple[str, tuple[str, ...]]:
    primary = _declared_worker_target(str(value["file_path"]))
    scopes: list[str] = []
    for raw_scope in value.get("write_scopes") or []:
        scope = _declared_worker_target(str(raw_scope))
        if scope and scope not in scopes:
            scopes.append(scope)
    if primary and primary not in scopes:
        scopes.insert(0, primary)
    if not scopes:
        scopes = [primary]
    if len(scopes) > config.MAX_FILES_PER_WORK_PACKAGE:
        raise RuntimeError(
            f"Work item {value.get('id')!r} declares {len(scopes)} write scopes; "
            f"limit is {config.MAX_FILES_PER_WORK_PACKAGE}. Split it into independent items."
        )
    return primary, tuple(scopes)


def _load_planner_sources(
    root: Path,
    source_files: list[str] | None,
) -> list[context_builder.SourceFile]:
    """Revalidate user-selected context at the hierarchy trust boundary."""
    selected = list(source_files or ())
    if len(selected) > config.HIERARCHY_MAX_SOURCE_FILES:
        raise ValueError(
            "Too many hierarchy source files: "
            f"{len(selected)} > {config.HIERARCHY_MAX_SOURCE_FILES}"
        )
    loaded: list[context_builder.SourceFile] = []
    seen: set[tuple[str, str]] = set()
    for raw_value in selected:
        raw = str(raw_value).strip()
        if not raw:
            continue
        rel_path = raw
        modifier = ""
        if ":" in raw:
            candidate_path, candidate_modifier = raw.rsplit(":", 1)
            if candidate_modifier == "skeleton" or re.fullmatch(
                r"[1-9]\d*-[1-9]\d*", candidate_modifier
            ):
                rel_path, modifier = candidate_path, candidate_modifier
            else:
                raise ValueError(f"Unsupported source file modifier: {candidate_modifier!r}")
        path_utils.ensure_context_path_safe(rel_path)
        normalized, absolute = path_utils.resolve_under_root(root, rel_path)
        if not absolute.exists():
            raise FileNotFoundError(f"Selected source file does not exist: {normalized}")
        if not absolute.is_file():
            raise IsADirectoryError(f"Selected source path is not a file: {normalized}")
        worker_targets.read_prompt_text(normalized, absolute)
        key = (normalized.casefold(), modifier)
        if key in seen:
            continue
        seen.add(key)
        loaded.append(
            context_builder.SourceFile(
                rel_path=normalized,
                absolute_path=absolute,
                request_modifier=modifier,
            )
        )
    return loaded


def _planner_source_context(
    sources: list[context_builder.SourceFile],
    *,
    total_limit: int,
) -> str:
    if not sources:
        return ""
    rendered, _ = context_builder.build_source_blocks(
        sources,
        max_chars_per_file=config.HIERARCHY_SOURCE_FILE_CHARS,
        max_total_chars=total_limit,
    )
    return rendered


def _manager_sources(
    sources: list[context_builder.SourceFile],
    stream: Workstream,
) -> list[context_builder.SourceFile]:
    contract = to_dict(stream.contract) if stream.contract is not None else None
    contract_scopes: list[str] = []
    if isinstance(contract, dict):
        contract_scopes.extend(str(value) for value in contract.get("read_scopes") or ())
        contract_scopes.extend(str(value) for value in contract.get("write_scopes") or ())
    scopes = [*stream.write_scopes, *contract_scopes]
    description = f"{stream.title}\n{stream.goal}".casefold()
    return [
        source
        for source in sources
        if (scopes and scopes_conflict((source.rel_path,), scopes))
        or source.rel_path.casefold() in description
        or Path(source.rel_path).name.casefold() in description
    ]


def _scope_covers(scope: str, target: str) -> bool:
    parent = scope.strip().replace("\\", "/")
    child = target.strip().replace("\\", "/")
    if parent.endswith("/**") or parent.endswith("/*"):
        parent = parent.rsplit("/", 1)[0]
    parent = parent.rstrip("/")
    return parent in {"", "."} or child == parent or child.startswith(parent + "/")


def _depends_on(
    node_id: str,
    possible_dependency: str,
    dependencies: dict[str, tuple[str, ...]],
) -> bool:
    pending = list(dependencies.get(node_id, ()))
    visited: set[str] = set()
    while pending:
        dependency = pending.pop()
        if dependency == possible_dependency:
            return True
        if dependency in visited:
            continue
        visited.add(dependency)
        pending.extend(dependencies.get(dependency, ()))
    return False


def _looks_like_artifact_path(value: str) -> bool:
    """Decide whether a contract entry names a file at all.

    Planners mix prose into these fields, and prose often contains a slash:
    "USER GOAL section 5 (resolution/fps/mirror config, DSHOW/MSMF backend)"
    was being measured against the write scopes as though it were a filename,
    which failed the work item over a file nobody ever intended to exist. A
    real path carries no whitespace, and that alone separates the two.
    """
    normalized = value.strip().replace("\\", "/")
    if not normalized or any(character.isspace() for character in normalized):
        return False
    return "/" in normalized or normalized.startswith(".") or bool(Path(normalized).suffix)


_PATH_ANNOTATION_RE = re.compile(r"\s*\([^()]*\)\s*$")


def _strip_path_annotation(value: str) -> str:
    """Reduce a planner's annotated path to the path itself.

    Models routinely describe the file instead of just naming it, in fields the
    contract treats as literal paths::

        data/config.json (schema only, created at runtime)
        app/capture.py: WebcamCapture class exposing start/stop

    Both forms then fail scope checks against a file nobody ever asked for. The
    annotation is only removed when what remains is a bare path token, so prose
    entries survive untouched, and a Windows drive prefix such as ``C:/x`` is
    left alone because ``C`` on its own does not look like a path.
    """
    text = str(value).strip()
    token = _bare_path_token(text)
    if token is not None:
        return token
    head, separator, tail = text.partition(":")
    if separator and tail:
        head_token = _bare_path_token(head)
        # A lone drive letter is not a path, which keeps "C:/x" intact.
        if head_token and ("/" in head_token.replace("\\", "/") or Path(head_token).suffix):
            return head_token
    return text


def _bare_path_token(text: str) -> str | None:
    """Return ``text`` as a path token with trailing notes removed, else None."""
    candidate = text.strip()
    while True:
        stripped = _PATH_ANNOTATION_RE.sub("", candidate).strip()
        if stripped == candidate:
            break
        candidate = stripped
    if not candidate or any(character.isspace() for character in candidate):
        return None
    # "./x" and "x" name the same file; keeping both spellings around makes
    # scope comparisons disagree with themselves.
    normalized = candidate.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized or candidate


def _clean_path_tuple(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(_strip_path_annotation(str(value)) for value in values)


def _dependency_closure(
    node_id: str,
    dependencies: dict[str, tuple[str, ...]],
) -> set[str]:
    resolved: set[str] = set()
    pending = list(dependencies.get(node_id, ()))
    while pending:
        dependency = pending.pop()
        if dependency in resolved:
            continue
        resolved.add(dependency)
        pending.extend(dependencies.get(dependency, ()))
    return resolved


def _contract_approval_issues(
    item: WorkItem,
    evidence: dict[str, Any],
) -> list[str]:
    """Return missing approval proof for explicitly typed planner contracts."""
    if item.metadata.get("contract_mode") != "typed":
        return []
    contract = item.contract
    assert contract is not None
    issues: list[str] = []
    if evidence.get("reviewer_verdict") != "approved":
        issues.append("independent reviewer approval is missing")
    for field_name in ("worker_feedback", "execution_result", "reviewer_feedback"):
        if not str(evidence.get(field_name) or "").strip():
            issues.append(f"{field_name} evidence is missing")
    if contract.evidence_requirements and not (
        evidence.get("patch_sha256") or evidence.get("after_sha256") or evidence.get("effect_id")
    ):
        issues.append("durable patch/artifact evidence is missing")
    if contract.test_requirements:
        if not test_evidence.item_test_evidence_complete(
            contract.test_requirements,
            evidence,
            allow_deferred_integration=True,
        ):
            issues.append("declared test requirements lack passing evidence")
    return issues


# Only conditions that make execution impossible or unsafe stop plan admission.
#
# Everything else a planner can get wrong is bookkeeping: a contract that forgot
# to list its test requirements, an expected output phrased slightly differently
# from the write scope, a read scope nobody enumerated. Those used to fail the
# item before it ever reached the model, which cost the run real work over
# paperwork. Overlapping scopes are advisory too: inference may run concurrently
# while the ticket executor serializes filesystem effects.
_BLOCKING_PREFLIGHT_CODES = frozenset(
    {
        # Bounded mode cannot order a malformed DAG. Maximum-parallelism mode
        # deliberately ignores dependency edges, so validate_plan will not emit
        # this issue there.
        "invalid_dependencies",
        # Nothing concrete for a worker to write.
        "missing_concrete_target",
        "empty_workstream",
        # Path escapes the project root, or a symlink is in the way.
        "unsafe_target",
        "unsupported_worker_target",
        # Edit-only runs must not invent files.
        "new_file_forbidden",
        # Typed behavioral tests need a real post-review integration command.
        "integration_test_not_configured",
        # The manager never produced a plan for this workstream.
        "manager_plan_failed",
    }
)


def _preflight_plan(
    *,
    root: Path,
    plan: TaskPlan,
    limits: SchedulerLimits,
    allow_new_files: bool,
    test_cmd: list[str] | None = None,
    available_artifacts: tuple[str, ...] = (),
    warnings: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Validate the complete planned DAG before any Worker is announced.

    Returns only blocking issues. Advisory findings are appended to ``warnings``
    when the caller supplies a list to collect them.
    """
    issues: list[dict[str, Any]] = []

    def add_issue(
        code: str,
        message: str,
        *,
        workstream_id: str | None = None,
        work_item_ids: tuple[str, ...] = (),
        paths: tuple[str, ...] = (),
    ) -> None:
        record = {
            "code": code,
            "message": message,
            "workstream_id": workstream_id,
            "work_item_ids": list(work_item_ids),
            "paths": list(paths),
        }
        if code in _BLOCKING_PREFLIGHT_CODES:
            issues.append(record)
        elif warnings is not None:
            warnings.append(record)

    dag_valid = True
    try:
        validate_plan(plan, limits)
    except (SchedulingError, ValueError) as exc:
        dag_valid = False
        add_issue("invalid_dependencies", str(exc))

    stream_dependencies = {stream.id: tuple(stream.dependencies) for stream in plan.workstreams}
    item_dependencies = {
        item.id: tuple(item.dependencies)
        for stream in plan.workstreams
        for item in stream.work_items
    }

    def check_contract(
        *,
        contract: WorkContract,
        workstream_id: str,
        work_item_id: str | None = None,
        parent_contract: WorkContract | None = None,
        strict: bool,
    ) -> None:
        item_ids = (work_item_id,) if work_item_id is not None else ()
        if strict and not contract.test_requirements:
            add_issue(
                "contract_tests_missing",
                f"Contract {contract.id!r} has no test requirements",
                workstream_id=workstream_id,
                work_item_ids=item_ids,
            )
        if (
            strict
            and test_evidence.requires_integration_test(contract.test_requirements)
            and not test_cmd
        ):
            add_issue(
                "integration_test_not_configured",
                f"Contract {contract.id!r} has non-syntax test requirements but no "
                "integration test command is configured",
                workstream_id=workstream_id,
                work_item_ids=item_ids,
            )
        if not contract.evidence_requirements:
            add_issue(
                "contract_evidence_missing",
                f"Contract {contract.id!r} has no evidence requirements",
                workstream_id=workstream_id,
                work_item_ids=item_ids,
            )
        if not contract.consumers:
            add_issue(
                "contract_consumers_missing",
                f"Contract {contract.id!r} has no consumers",
                workstream_id=workstream_id,
                work_item_ids=item_ids,
            )
        invalid_consumers = tuple(
            consumer
            for consumer in contract.consumers
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", consumer)
        )
        if invalid_consumers:
            add_issue(
                "invalid_contract_consumers",
                f"Contract {contract.id!r} has invalid stable consumer IDs",
                workstream_id=workstream_id,
                work_item_ids=item_ids,
                paths=invalid_consumers,
            )

        for artifact in contract.input_artifacts:
            if not _looks_like_artifact_path(artifact):
                continue
            try:
                normalized = path_utils.normalize_rel_path(artifact)
            except (ValueError, path_utils.PathEscapeError):
                add_issue(
                    "invalid_contract_input",
                    f"Contract {contract.id!r} input path is invalid: {artifact!r}",
                    workstream_id=workstream_id,
                    work_item_ids=item_ids,
                    paths=(artifact,),
                )
                continue
            if not any(
                _scope_covers(scope, normalized)
                for scope in (*contract.read_scopes, *contract.write_scopes)
            ):
                add_issue(
                    "contract_read_scope_mismatch",
                    f"Input {normalized!r} is outside contract read/write scopes",
                    workstream_id=workstream_id,
                    work_item_ids=item_ids,
                    paths=(normalized,),
                )

        for output in contract.expected_outputs:
            if not _looks_like_artifact_path(output):
                continue
            try:
                normalized = path_utils.normalize_rel_path(output)
            except path_utils.PathEscapeError:
                normalized = output
            if not any(_scope_covers(scope, normalized) for scope in contract.write_scopes):
                add_issue(
                    "contract_output_scope_mismatch",
                    f"Output {normalized!r} is outside contract write scopes",
                    workstream_id=workstream_id,
                    work_item_ids=item_ids,
                    paths=(normalized,),
                )

        # A work item reading a file its workstream did not enumerate is a gap in
        # the planner's paperwork, not a hazard: read scopes only steer which
        # files are shown to the agent. Write ownership is what must not overlap,
        # and that is still checked above and across the whole plan. Failing the
        # item here stopped real work over a file it merely wanted to read.

    work_entries: list[tuple[Workstream, WorkItem, tuple[str, ...]]] = []
    for stream in plan.workstreams:
        assert stream.contract is not None
        check_contract(
            contract=stream.contract,
            workstream_id=stream.id,
            strict=stream.metadata.get("contract_mode") == "typed",
        )
        plan_error = str(stream.metadata.get("plan_error") or "").strip()
        if plan_error:
            add_issue(
                "manager_plan_failed",
                plan_error,
                workstream_id=stream.id,
            )
        dropped_stream_dependencies = tuple(
            str(value) for value in stream.metadata.get("dropped_dependencies") or ()
        )
        if dropped_stream_dependencies:
            add_issue(
                "unknown_work_item_dependencies",
                "Manager plan referenced unknown work item dependencies: "
                + ", ".join(dropped_stream_dependencies),
                workstream_id=stream.id,
            )
        if not stream.work_items:
            add_issue(
                "empty_workstream",
                f"Workstream {stream.id!r} has no executable work items",
                workstream_id=stream.id,
            )
            continue
        for item in stream.work_items:
            assert item.contract is not None
            check_contract(
                contract=item.contract,
                workstream_id=stream.id,
                work_item_id=item.id,
                parent_contract=stream.contract,
                strict=item.metadata.get("contract_mode") == "typed",
            )
            item_paths: tuple[str, ...] = ()
            try:
                item_paths = tuple(_package_files(item, root=root))
            except worker_targets.DirectoryWorkerTarget as exc:
                add_issue(
                    "directory_target_unexpanded",
                    str(exc),
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                    paths=(exc.path,),
                )
                continue
            except (RuntimeError, ValueError) as exc:
                add_issue(
                    "missing_concrete_target",
                    str(exc),
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                )
                continue
            if item.metadata.get("expanded_directory_targets"):
                add_issue(
                    "directory_target_expanded",
                    f"Work item {item.id!r} directory target(s) expanded into concrete files",
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                    paths=item_paths,
                )
            primary = str(item.metadata.get("file_path") or "").replace("\\", "/")
            if not primary or primary not in item_paths:
                add_issue(
                    "contract_file_mismatch",
                    f"Work item {item.id!r} primary file is not in its package",
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                    paths=item_paths,
                )
            if tuple(item.contract.write_scopes) != tuple(item.write_scopes):
                add_issue(
                    "contract_file_mismatch",
                    f"Work item {item.id!r} contract write scopes differ",
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                    paths=item_paths,
                )
            file_outputs: list[str] = []
            for output in item.contract.expected_outputs:
                raw_output = str(output).strip().replace("\\", "/")
                if worker_targets.is_runtime_only_artifact(raw_output):
                    continue
                # Same rule as everywhere else: prose is not a file, even when
                # it happens to contain a slash.
                if not _looks_like_artifact_path(raw_output) and raw_output not in item_paths:
                    continue
                try:
                    file_outputs.append(path_utils.normalize_rel_path(raw_output))
                except path_utils.PathEscapeError:
                    file_outputs.append(raw_output)
            if file_outputs and set(file_outputs) != set(item_paths):
                add_issue(
                    "contract_file_mismatch",
                    f"Work item {item.id!r} expected file outputs differ from package targets",
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                    paths=tuple(sorted(set(file_outputs) | set(item_paths))),
                )
            metadata_files = tuple(
                str(value).replace("\\", "/") for value in item.metadata.get("package_files") or ()
            )
            if metadata_files and set(metadata_files) != set(item_paths):
                add_issue(
                    "contract_file_mismatch",
                    f"Work item {item.id!r} package metadata differs from write scopes",
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                    paths=item_paths,
                )
            for target in item_paths:
                if target in {"", "."}:
                    add_issue(
                        "missing_concrete_target",
                        f"Work item {item.id!r} target is not a concrete file: {target!r}",
                        workstream_id=stream.id,
                        work_item_ids=(item.id,),
                        paths=(target,),
                    )
                    continue
                if worker_targets.is_directory_shaped_target(target) or (
                    worker_targets.is_on_disk_directory_target(root, target)
                ):
                    add_issue(
                        "directory_target_unexpanded",
                        f"Work item {item.id!r} directory target is advisory, not unsupported: {target!r}",
                        workstream_id=stream.id,
                        work_item_ids=(item.id,),
                        paths=(target,),
                    )
                    continue
                if not any(_scope_covers(scope, target) for scope in stream.write_scopes):
                    add_issue(
                        "stream_scope_mismatch",
                        f"Target {target!r} is outside workstream {stream.id!r} scope",
                        workstream_id=stream.id,
                        work_item_ids=(item.id,),
                        paths=(target,),
                    )
                try:
                    worker_targets.ensure_worker_target(target)
                    path_utils.ensure_context_path_safe(target)
                    # resolve_under_root already refuses anything that escapes
                    # the project or crosses a symlink, so spelling is not a
                    # safety question. A planner writing "./.gitignore" instead
                    # of ".gitignore" used to lose its work item to that.
                    normalized, absolute = path_utils.resolve_under_root(root, target)
                    worker_targets.ensure_worker_target(normalized, absolute_path=absolute)
                    if absolute.exists() and not absolute.is_file():
                        raise worker_targets.DirectoryWorkerTarget(
                            normalized,
                            "directory target must be expanded into concrete files",
                        )
                    if not absolute.exists() and not allow_new_files:
                        raise FileNotFoundError(
                            f"New file is not allowed in edit mode: {normalized}"
                        )
                except worker_targets.DirectoryWorkerTarget as exc:
                    add_issue(
                        "directory_target_unexpanded",
                        str(exc),
                        workstream_id=stream.id,
                        work_item_ids=(item.id,),
                        paths=(target,),
                    )
                except (
                    FileNotFoundError,
                    IsADirectoryError,
                    path_utils.PathEscapeError,
                    path_utils.SensitivePathError,
                    worker_targets.UnsupportedWorkerTarget,
                ) as exc:
                    add_issue(
                        "unsupported_worker_target"
                        if isinstance(exc, worker_targets.UnsupportedWorkerTarget)
                        else "unsafe_target"
                        if not isinstance(exc, FileNotFoundError)
                        else "new_file_forbidden",
                        str(exc),
                        workstream_id=stream.id,
                        work_item_ids=(item.id,),
                        paths=(target,),
                    )
            work_entries.append((stream, item, item_paths))

    if dag_valid:
        for index, (left_stream, left_item, left_paths) in enumerate(work_entries):
            for right_stream, right_item, right_paths in work_entries[index + 1 :]:
                if not scopes_conflict(left_paths, right_paths):
                    continue
                if left_stream.id == right_stream.id:
                    ordered = _depends_on(
                        left_item.id, right_item.id, item_dependencies
                    ) or _depends_on(right_item.id, left_item.id, item_dependencies)
                else:
                    ordered = _depends_on(
                        left_stream.id,
                        right_stream.id,
                        stream_dependencies,
                    ) or _depends_on(
                        right_stream.id,
                        left_stream.id,
                        stream_dependencies,
                    )
                if not ordered:
                    conflicting = tuple(
                        sorted(
                            {
                                left
                                for left in left_paths
                                for right in right_paths
                                if scopes_conflict((left,), (right,))
                            }
                            | {
                                right
                                for right in right_paths
                                for left in left_paths
                                if scopes_conflict((left,), (right,))
                            }
                        )
                    )
                    add_issue(
                        "unserialized_scope_conflict",
                        "Conflicting work items have no dependency ordering: "
                        f"{left_item.id!r}, {right_item.id!r}",
                        work_item_ids=(left_item.id, right_item.id),
                        paths=conflicting,
                    )
    return issues


def _stream_produced_paths(stream: Workstream) -> set[str]:
    """Every file a workstream claims it will write."""
    produced: set[str] = set()
    sources: list[str] = [*stream.write_scopes]
    if stream.contract is not None:
        sources.extend(stream.contract.write_scopes)
        sources.extend(stream.contract.expected_outputs)
    for item in stream.work_items:
        sources.extend(item.write_scopes)
        if item.contract is not None:
            sources.extend(item.contract.write_scopes)
            sources.extend(item.contract.expected_outputs)
    for value in sources:
        text = str(value).strip().replace("\\", "/").rstrip("/")
        if text and _looks_like_artifact_path(text):
            produced.add(text)
    return produced


def _stream_consumed_paths(stream: Workstream) -> set[str]:
    """Every file a workstream says it needs to read."""
    consumed: set[str] = set()
    sources: list[str] = []
    if stream.contract is not None:
        sources.extend(stream.contract.input_artifacts)
    for item in stream.work_items:
        if item.contract is not None:
            sources.extend(item.contract.input_artifacts)
    for value in sources:
        text = str(value).strip().replace("\\", "/").rstrip("/")
        if text and _looks_like_artifact_path(text):
            consumed.add(text)
    return consumed


def _item_produced_paths(item: WorkItem) -> set[str]:
    sources: list[str] = [*item.write_scopes]
    if item.contract is not None:
        sources.extend(item.contract.write_scopes)
        sources.extend(item.contract.expected_outputs)
    return {
        text
        for value in sources
        if (text := str(value).strip().replace("\\", "/").rstrip("/"))
        and _looks_like_artifact_path(text)
    }


def _item_consumed_paths(item: WorkItem) -> set[str]:
    sources = list(item.contract.input_artifacts) if item.contract is not None else []
    return {
        text
        for value in sources
        if (text := str(value).strip().replace("\\", "/").rstrip("/"))
        and _looks_like_artifact_path(text)
    }


def _relax_item_dependencies(stream: Workstream) -> tuple[Workstream, list[dict[str, Any]]]:
    """Same rule as for workstreams, applied to the items inside one."""
    produced = {item.id: _item_produced_paths(item) for item in stream.work_items}
    consumed = {item.id: _item_consumed_paths(item) for item in stream.work_items}
    dropped: list[dict[str, Any]] = []
    items: list[WorkItem] = []
    changed = False

    for item in stream.work_items:
        keep: list[str] = []
        for dependency in item.dependencies:
            upstream_writes = produced.get(dependency)
            if upstream_writes is None:
                keep.append(dependency)
                continue
            justified = any(
                _scope_covers(written, other) or _scope_covers(other, written)
                for written in upstream_writes
                for other in (*consumed.get(item.id, ()), *produced.get(item.id, ()))
            )
            if justified:
                keep.append(dependency)
                continue
            dropped.append(
                {
                    "workstream_id": stream.id,
                    "work_item_id": item.id,
                    "dependency": dependency,
                    "reason": "no shared artifact or write scope",
                }
            )
        if len(keep) != len(item.dependencies):
            changed = True
            items.append(replace(item, dependencies=tuple(keep)))
        else:
            items.append(item)

    if not changed:
        return stream, dropped
    return replace(stream, work_items=tuple(items)), dropped


def _relax_decorative_dependencies(
    streams: list[Workstream],
) -> tuple[list[Workstream], list[dict[str, Any]]]:
    """Drop workstream edges that no file actually justifies.

    A dependency serialises execution, so a plan that chains every workstream
    runs one at a time however many workers it planned. Planners add these edges
    for narrative order ("API before the UI that calls it") even when the goal
    already specifies the contract between them and nothing is actually read.

    An edge is kept when it is load bearing, which means either the downstream
    stream reads a file the upstream stream writes, or the two claim overlapping
    write scopes and therefore must not run together. Everything else is
    removed. Nothing is weakened by this: a stream that really does need a file
    it never declared is still stopped at runtime by the missing-input check.
    """
    produced = {stream.id: _stream_produced_paths(stream) for stream in streams}
    consumed = {stream.id: _stream_consumed_paths(stream) for stream in streams}
    dropped: list[dict[str, Any]] = []
    relaxed: list[Workstream] = []

    for stream in streams:
        keep: list[str] = []
        for dependency in stream.dependencies:
            upstream_writes = produced.get(dependency)
            if upstream_writes is None:
                keep.append(dependency)
                continue
            reads_upstream_output = any(
                _scope_covers(written, needed) or _scope_covers(needed, written)
                for written in upstream_writes
                for needed in consumed.get(stream.id, ())
            )
            writes_collide = any(
                _scope_covers(written, mine) or _scope_covers(mine, written)
                for written in upstream_writes
                for mine in produced.get(stream.id, ())
            )
            if reads_upstream_output or writes_collide:
                keep.append(dependency)
                continue
            dropped.append(
                {
                    "workstream_id": stream.id,
                    "dependency": dependency,
                    "reason": "no shared artifact or write scope",
                }
            )
        settled = (
            replace(stream, dependencies=tuple(keep))
            if len(keep) != len(stream.dependencies)
            else stream
        )
        settled, item_drops = _relax_item_dependencies(settled)
        dropped.extend(item_drops)
        relaxed.append(settled)
    return relaxed, dropped


def _runtime_missing_contract_inputs(
    root: Path,
    contract: WorkContract,
) -> tuple[str, ...]:
    """Check path-like inputs only when their dependency gate has opened."""
    missing: list[str] = []
    # A file this contract is itself going to write is not a missing input; the
    # planner often lists its own output among the artifacts it works on, and
    # blocking on that would stop the very agent that creates the file.
    own_outputs = {
        str(scope).strip().replace("\\", "/").rstrip("/")
        for scope in (*contract.write_scopes, *contract.expected_outputs)
    }
    for artifact in contract.input_artifacts:
        if not _looks_like_artifact_path(artifact):
            continue
        if artifact.strip().replace("\\", "/").rstrip("/") in own_outputs:
            continue
        try:
            _, absolute = path_utils.resolve_under_root(root, artifact)
        except (ValueError, path_utils.PathEscapeError):
            missing.append(artifact)
            continue
        if not absolute.is_file():
            missing.append(artifact)
    return tuple(missing)


def _contract_input_context(
    root: Path,
    contract: WorkContract,
    *,
    max_chars: int = 200_000,
) -> str:
    """Load declared UTF-8 inputs so Workers can implement against real APIs."""

    blocks: list[str] = []
    remaining = max_chars
    own_outputs = {
        str(value).strip().replace("\\", "/").rstrip("/")
        for value in (*contract.write_scopes, *contract.expected_outputs)
    }
    declared_inputs = tuple(
        dict.fromkeys((*contract.input_artifacts, *contract.read_scopes))
    )
    for artifact in declared_inputs:
        normalized = str(artifact).strip().replace("\\", "/").rstrip("/")
        if (
            not normalized
            or normalized in own_outputs
            or not _looks_like_artifact_path(normalized)
        ):
            continue
        try:
            relative, absolute = path_utils.resolve_under_root(root, normalized)
            if not absolute.is_file():
                continue
            content = worker_targets.read_prompt_text(relative, absolute)
        except (
            OSError,
            ValueError,
            path_utils.PathEscapeError,
            worker_targets.UnsupportedPromptSource,
        ):
            continue
        if remaining <= 0:
            break
        selected = content[:remaining]
        blocks.append(f"### {relative}\n```\n{selected}\n```")
        remaining -= len(selected)
    if not blocks:
        return ""
    return "\n\n## DECLARED INPUT ARTIFACT CONTEXT\n" + "\n\n".join(blocks)


def _planner_contract(
    value: dict[str, Any],
    *,
    fallback_id: str,
    fallback_inputs: tuple[str, ...] = (),
    fallback_outputs: tuple[str, ...] = (),
    fallback_write_scopes: tuple[str, ...] = (),
    fallback_acceptance: tuple[str, ...],
    fallback_consumers: tuple[str, ...],
) -> WorkContract:
    """Build a validated contract while keeping legacy planner fixtures usable."""
    acceptance = tuple(value.get("acceptance_criteria") or fallback_acceptance)
    write_scopes = _clean_path_tuple(value.get("write_scopes") or fallback_write_scopes)
    inputs = _clean_path_tuple(value.get("input_artifacts") or fallback_inputs)
    declared_reads = _clean_path_tuple(value.get("read_scopes") or ())
    # Naming a file as an input *is* declaring the intent to read it. Planners
    # routinely list one and forget the other, and rejecting the contract over
    # that bookkeeping gap killed work items for a file they were only going to
    # read. Read scopes are not a security boundary here -- writes are -- so the
    # coherent reading is to widen, not to refuse.
    read_scopes = tuple(
        dict.fromkeys(
            (
                *declared_reads,
                *(artifact for artifact in inputs if _looks_like_artifact_path(artifact)),
            )
        )
    )
    outputs = _clean_path_tuple(
        value.get("expected_outputs")
        or fallback_outputs
        or write_scopes
        or (str(value.get("goal") or fallback_id),)
    )
    tests = tuple(value.get("test_requirements") or acceptance)
    evidence = tuple(value.get("evidence_requirements") or acceptance)
    consumers = tuple(value.get("consumers") or fallback_consumers)
    return WorkContract(
        id=str(value.get("contract_id") or f"{fallback_id}-contract"),
        version=int(value.get("contract_version") or 1),
        input_artifacts=inputs,
        expected_outputs=outputs,
        read_scopes=read_scopes,
        write_scopes=write_scopes,
        acceptance_criteria=acceptance,
        test_requirements=tests,
        evidence_requirements=evidence,
        consumers=consumers,
        risk_level=RiskLevel(str(value.get("risk_level") or RiskLevel.LOW.value)),
        priority=int(value.get("priority") or 0),
        approval_policy=ApprovalPolicy(
            str(value.get("approval_policy") or ApprovalPolicy.RISK_BASED.value)
        ),
    )


def _manager_stream_from_result(
    result: ToolCallResult,
    stream: Workstream,
    limits: SchedulerLimits,
    max_coder_count: int | None = None,
    # Compatibility for callers written against the former exact-count API.
    # It is now interpreted as a maximum.
    required_coder_count: int | None = None,
) -> Workstream:
    if result.tool_name != "submit_work_item_plan":
        raise RuntimeError(f"Manager action không hợp lệ: {result.tool_name}")
    raw = result.tool_input
    work_items_raw = list(raw.get("work_items") or [])
    _require_planner_ids(work_items_raw, label="work item")
    coder_cap = max_coder_count or required_coder_count or limits.coders_per_manager
    error = _planner_selection_error(
        result,
        list_field="work_items",
        count_field="requested_worker_count",
        maximum=coder_cap,
        label="Manager",
    )
    if error:
        raise RuntimeError(error)
    _validate_execution_slots(
        label="Worker",
        slots=limits.worker_parallel_cap,
        maximum=coder_cap,
    )
    fanout_reason = str(raw.get("selected_fanout_reason") or raw.get("summary") or "").strip()
    id_map = {
        str(item["id"]): f"{stream.id}:{item['id']}"
        for item in work_items_raw
        if isinstance(item, dict) and item.get("id") is not None
    }
    if not id_map:
        raise RuntimeError("Manager plan thiếu work item id hợp lệ")
    items = []
    reserved_contract_ids = {stream.contract.id} if stream.contract is not None else set()
    dropped_deps: list[str] = []
    for value in work_items_raw:
        if not isinstance(value, dict) or value.get("id") is None:
            continue
        raw_id = str(value["id"])
        raw_deps = value.get("dependencies") or []
        if not isinstance(raw_deps, list):
            raw_deps = []
        valid_deps: list[str] = []
        for dep in raw_deps:
            dep_id = str(dep)
            if dep_id in id_map:
                valid_deps.append(id_map[dep_id])
            else:
                dropped_deps.append(f"{raw_id}->{dep_id}")
        primary, scopes = _normalize_work_item_scopes(value)
        acceptance = tuple(value["acceptance_criteria"])
        requested_contract_id = str(value.get("contract_id") or f"{id_map[raw_id]}-contract")
        needs_contract_namespace = (
            requested_contract_id in reserved_contract_ids
            or not requested_contract_id.startswith(f"{stream.id}:")
        )
        contract_value = {
            **value,
            "write_scopes": list(scopes),
            "contract_id": (
                f"{id_map[raw_id]}-contract" if needs_contract_namespace else requested_contract_id
            ),
        }
        contract = _planner_contract(
            contract_value,
            fallback_id=id_map[raw_id],
            fallback_inputs=tuple(valid_deps),
            fallback_outputs=scopes or (str(value["goal"]),),
            fallback_write_scopes=scopes,
            fallback_acceptance=acceptance,
            fallback_consumers=(stream.id,),
        )
        reserved_contract_ids.add(contract.id)
        items.append(
            WorkItem(
                id=id_map[raw_id],
                workstream_id=stream.id,
                title=str(value["title"]),
                goal=str(value["goal"]),
                acceptance_criteria=acceptance,
                dependencies=tuple(valid_deps),
                write_scopes=scopes,
                priority=contract.priority,
                contract=contract,
                metadata={
                    "file_path": primary,
                    "package_files": list(scopes),
                    "instructions": str(value["instructions"]),
                    "test_focus": str(value.get("test_focus", "")),
                    "manager_summary": str(raw["summary"]),
                    "planner_contract_id": requested_contract_id,
                    "dropped_dependencies": [
                        item for item in dropped_deps if item.startswith(f"{raw_id}->")
                    ],
                    "contract_mode": (
                        "typed" if _has_explicit_contract(value) else "compatibility"
                    ),
                },
            )
        )
    # One major package → one logical Worker.  Execution slots are a separate cap.
    # Work items are Coder assignments; one separate Tester is reserved for
    # the workstream and is intentionally not part of this count.
    requested = len(items)
    return replace(
        stream,
        work_items=tuple(items),
        requested_worker_count=requested,
        status=WorkStatus.READY,
        metadata={
            **dict(stream.metadata),
            "dropped_dependencies": dropped_deps,
            "declared_worker_count": int(raw.get("requested_worker_count") or requested),
            "fanout": {
                "selected": requested,
                "max": coder_cap,
                "reason": fanout_reason,
                "execution_slots": limits.worker_parallel_cap,
            },
            "selected_worker_count": requested,
            "max_worker_count": coder_cap,
            "selected_fanout_reason": fanout_reason,
            "worker_execution_slots": limits.worker_parallel_cap,
        },
    )


def _run_hierarchy_impl(
    *,
    root: Path,
    task_description: str,
    task_id: str,
    source_files: list[str] | None = None,
    test_cmd: list[str] | None = None,
    allow_new_files: bool = False,
    limits: SchedulerLimits | None = None,
    director_model: str = "claude-sonnet-5",
    director_effort: str = "max",
    manager_model: str = "claude-sonnet-5",
    manager_effort: str = "max",
    worker_model: str = "claude-sonnet-5",
    worker_effort: str = "max",
    reviewer_model: str = "claude-sonnet-5",
    reviewer_effort: str = "high",
    llm_call: LLMCallFn = _default_llm_call,
    on_event: EventSink | None = None,
    on_file_approved: ApprovedFileSink | None = None,
    repository: StateRepository | None = None,
    project_lease: Any | None = None,
    resume_session: bool = False,
    agent_config_resolver: AgentConfigResolver | None = None,
    approval_callback: ApprovalCallback | None = None,
    cancelled: CancellationCallback | None = None,
    crisis_strategy: Any | None = None,
) -> SessionResult:
    """Execute a durable, bounded two-level plan.

    Every Manager selected by the Director is planned eagerly. Planning caps
    are upper bounds, while execution respects separate dependency-aware
    parallel slots.
    Worker model calls may run concurrently, while patch/Git/tests/review use
    one project lock to preserve the local working tree.
    """
    root = root.resolve()
    if config.MAX_FILES_PER_WORK_PACKAGE < 1:
        raise ValueError("ORCH_MAX_FILES_PER_WORK_PACKAGE must be positive")
    bounds = limits or SchedulerLimits()
    repo = repository or StateRepository()
    previous_plan = repo.get_plan(task_id) if resume_session else None
    session_id = previous_plan.session_id if previous_plan is not None else new_id("session")
    director_id = _resolve_repository_agent_id(
        repo,
        task_id=task_id,
        role="director",
        assignment_id="root",
    )
    account_mode = str(getattr(llm_client.thread_local, "account_mode", None) or "sticky")
    previous_attempts = repo.list_attempts(task_id) if previous_plan is not None else []
    previous_handoffs = (
        _list_repository_handoffs(repo, task_id) if previous_plan is not None else []
    )
    persisted_handoff_artifacts = tuple(
        sorted(
            {
                str(artifact)
                for handoff in previous_handoffs
                for artifact in (
                    handoff.get("artifacts", ())
                    if isinstance(handoff, dict)
                    else getattr(handoff, "artifacts", ())
                )
            }
        )
    )
    if previous_attempts:
        refreshed_attempts = []
        for previous_attempt in previous_attempts:
            if previous_attempt.status == AttemptStatus.RUNNING:
                previous_attempt = replace(
                    previous_attempt,
                    status=AttemptStatus.EXPIRED,
                    finished_at=utc_now(),
                    error=previous_attempt.error or "Interrupted before attempt completion",
                )
                repo.save_attempt(previous_attempt)
            refreshed_attempts.append(previous_attempt)
        previous_attempts = refreshed_attempts
    previous_item_evidence: dict[str, dict[str, Any]] = {}
    for previous_attempt in previous_attempts:
        if previous_attempt.status == AttemptStatus.SUCCEEDED and previous_attempt.evidence.get(
            "accepted"
        ):
            previous_item_evidence[previous_attempt.work_item_id] = dict(previous_attempt.evidence)
    previous_revision = previous_plan.revision if previous_plan is not None else 0
    tree = file_agent.build_project_tree(root, max_entries=600)
    planner_sources = _load_planner_sources(root, source_files)
    director_source_context = _planner_source_context(
        planner_sources,
        total_limit=config.HIERARCHY_DIRECTOR_SOURCE_CHARS,
    )
    paths = state_store.ProjectPaths.for_root(root)
    rules = state_store.load_rules_text(paths)
    decisions = state_store.load_decisions_text(paths)
    result = SessionResult()
    crisis_strategy = crisis_strategy or retry_policy.PolicyCrisisStrategy(
        repository=repo,
    )
    model_call_outcomes: dict[str, str] = {}
    called_agent_ids: set[str] = set()
    started_agent_ids: set[str] = set()
    model_start_counts: dict[str, int] = {}
    agent_call_purposes: dict[str, set[str]] = {}
    agent_dispositions: dict[str, str] = {}
    agent_no_call_reasons: dict[str, str] = {}
    # Operator-facing names ("Manager 2", "Worker 2.3") and the account each
    # agent is currently on. The opaque logical id stays the identity; this is
    # only how the run is narrated in the UI and the terminal.
    agent_labels: dict[str, str] = {}
    agent_accounts: dict[str, str] = {}
    event_state_lock = threading.RLock()

    def register_agent_labels(current_plan: TaskPlan | None, manager_id_by_stream: dict[str, str]):
        """Number agents the way an operator reads them, 1-based and by parent."""
        labels: dict[str, str] = {director_id: "Director"}
        if current_plan is not None:
            for stream_index, stream in enumerate(current_plan.workstreams, start=1):
                manager_id = str(
                    stream.manager_agent_id or manager_id_by_stream.get(stream.id) or ""
                )
                if manager_id:
                    labels[manager_id] = f"Manager {stream_index}"
                tester_id = str(stream.metadata.get("tester_agent_id") or "")
                if tester_id:
                    labels[tester_id] = f"Tester {stream_index}"
                for item_index, item in enumerate(stream.work_items, start=1):
                    worker_id = str(item.metadata.get("worker_agent_id") or "")
                    if worker_id:
                        labels[worker_id] = f"Worker {stream_index}.{item_index}"
        with event_state_lock:
            agent_labels.update(labels)

    def agent_display(agent_id: str) -> str:
        label = agent_labels.get(agent_id) or agent_id
        account = agent_accounts.get(agent_id)
        return f"{label} · {account}" if account else label

    def release_known_agent_accounts() -> None:
        llm_client.release_task_account_cohort(task_id)

    logged_agent_milestones: set[tuple[str, str]] = set()

    def _log_agent_event(event_type: str, agent_id: str, event: dict[str, Any]) -> None:
        label = agent_labels.get(agent_id)
        if not label:
            return
        # preflight_failed and agent_failed describe one event to two consumers,
        # and a re-announced worker repeats its spawn. The terminal should read
        # as one line per thing that happened.
        milestone = {
            "agent_planned": "planned",
            "agent_started": "spawn",
            "preflight_failed": "failed",
            "agent_failed": "failed",
            "workstream_failed": "failed",
        }.get(event_type)
        if milestone:
            key = (agent_id, milestone)
            if key in logged_agent_milestones:
                return
            logged_agent_milestones.add(key)
        account = agent_accounts.get(agent_id)
        if event_type in {"agent_planned", "agent_started"}:
            details = [f"account={account or 'unassigned'}"]
            if event.get("work_item_id"):
                details.append(f"work_item={event['work_item_id']}")
            elif event.get("workstream_id"):
                details.append(f"workstream={event['workstream_id']}")
            tag = "AGENT PLANNED" if event_type == "agent_planned" else "AGENT SPAWN"
            _log_terminal(tag, f"{label} | " + " | ".join(details))
        elif event_type == "model_request_started":
            _log_terminal(
                "MODEL CALL",
                f"{label} | logical_request={event.get('logical_request_id') or '-'} | "
                f"attempt={event.get('attempt') or 1}",
            )
        elif event_type in {"agent_blocked", "workstream_blocked"}:
            reason = event.get("failure_kind") or event.get("reason") or "unknown"
            _log_terminal("AGENT BLOCKED", f"{label} | reason={reason}")
        elif event_type in {"preflight_failed", "agent_failed", "workstream_failed"}:
            reason = (
                event.get("failure_kind")
                or event.get("error")
                # workstream_failed carries neither of the above; its "why" is
                # in the summary, and a line reading "unknown" helps nobody.
                or event.get("summary")
                or event.get("verdict")
                or "unknown"
            )
            _log_terminal("AGENT FAILED", f"{label} | reason={str(reason)[:160]}")
        elif event_type == "execution_result" and event.get("accepted"):
            _log_terminal("AGENT COMPLETE", label)

    # The Director is nameable before anything is planned, so its own call is
    # narrated too rather than the terminal staying silent until managers exist.
    agent_labels[director_id] = "Director"

    def contextual_event(event: dict[str, Any]) -> None:
        event.setdefault("task_id", task_id)
        event.setdefault("session_id", session_id)
        if event.get("agent_instance_id"):
            event.setdefault("logical_agent_id", event["agent_instance_id"])
        if event.get("attempt_id"):
            if event.get("logical_request_id"):
                event.setdefault("provider_attempt_id", event["attempt_id"])
            else:
                event.setdefault("execution_attempt_id", event["attempt_id"])
        event_type = str(event.get("type") or "")
        agent_id = str(event.get("agent_instance_id") or "").strip()
        if agent_id:
            if event_type == "agent_started":
                with event_state_lock:
                    started_agent_ids.add(agent_id)
            account = str(event.get("account") or "").strip()
            if account and account != "[REDACTED]":
                with event_state_lock:
                    previous_account = agent_accounts.get(agent_id)
                    agent_accounts[agent_id] = account
                if previous_account and previous_account != account:
                    _log_terminal(
                        "ACCOUNT SWITCH",
                        f"{agent_labels.get(agent_id, agent_id)} | "
                        f"{previous_account} -> {account} | "
                        f"reason={event.get('reason') or event.get('failure_kind') or 'retry'}",
                    )
            label = agent_labels.get(agent_id)
            if label:
                event.setdefault("agent_label", label)
                event.setdefault("agent_display_name", agent_display(agent_id))
            _log_agent_event(event_type, agent_id, event)
            disposition: str | None = None
            if event_type in {"agent_blocked", "workstream_blocked"}:
                disposition = "blocked"
            elif event_type in {"preflight_failed", "agent_failed"}:
                disposition = (
                    "preflight_failed"
                    if event.get("status") == "preflight_failed"
                    or event.get("failure_kind") in {"preflight", "plan_preflight"}
                    else "failed"
                )
            elif event_type == "agent_abandoned":
                disposition = "abandoned"
            elif event_type in {"agent_cancelled", "agent_skipped", "workstream_skipped"}:
                disposition = "skipped"
            elif event_type == "workstream_failed":
                disposition = (
                    "skipped"
                    if event.get("status") == "cancelled"
                    else "abandoned"
                    if event.get("status") == "abandoned"
                    else "failed"
                )
            if disposition is not None:
                with event_state_lock:
                    agent_dispositions[agent_id] = disposition
                    reason = str(
                        event.get("failure_kind")
                        or event.get("reason")
                        or event.get("status")
                        or ""
                    ).strip()
                    if reason:
                        agent_no_call_reasons[agent_id] = reason
            if event_type == "model_request_started":
                purpose = str(
                    event.get("call_purpose")
                    or getattr(llm_client.thread_local, "call_purpose", None)
                    or "other"
                )
                with event_state_lock:
                    called_agent_ids.add(agent_id)
                    model_start_counts[agent_id] = model_start_counts.get(agent_id, 0) + 1
                    agent_call_purposes.setdefault(agent_id, set()).add(purpose)
        call_id = str(event.get("call_id") or event.get("logical_request_id") or "").strip()
        if call_id:
            with event_state_lock:
                if event_type == "model_request_started":
                    model_call_outcomes.setdefault(call_id, "in_flight")
                elif event_type == "model_request_completed":
                    model_call_outcomes[call_id] = "completed"
                elif event_type == "model_request_failed":
                    model_call_outcomes[call_id] = "failed"
                elif event_type in {
                    "model_request_aborted",
                    "model_request_cancelled",
                }:
                    model_call_outcomes[call_id] = "skipped"
        _emit(on_event, event)

    raw_llm_call = llm_call

    def tracked_llm_call(
        system_prompt: str,
        user_message: str,
        tools: list[dict[str, Any]],
    ) -> ToolCallResult:
        tool_name = str(tools[0].get("name") or "") if tools else ""
        purpose = {
            "submit_workstream_plan": "plan",
            "submit_work_item_plan": "plan",
            "submit_patch": "execute",
            "review_patch": "test",
            "complete_workstream": "review",
            "complete_plan": "review",
        }.get(tool_name, tool_name or "other")
        agent_id = str(getattr(llm_client.thread_local, "agent_instance_id", None) or "").strip()
        with event_state_lock:
            starts_before = model_start_counts.get(agent_id, 0) if agent_id else 0
        previous_purpose = getattr(llm_client.thread_local, "call_purpose", None)
        llm_client.thread_local.call_purpose = purpose
        try:
            try:
                response = raw_llm_call(system_prompt, user_message, tools)
            except llm_client.AccountPoolExhaustedError as exc:
                if purpose in {"execute", "test"}:
                    raise _AccountPoolFatalSignal(exc) from exc
                raise
            # Test adapters and custom local providers may not emit transport
            # events. Count them only after they actually return a response;
            # production calls are counted at model_request_started above.
            if agent_id:
                with event_state_lock:
                    if model_start_counts.get(agent_id, 0) == starts_before:
                        called_agent_ids.add(agent_id)
                        agent_call_purposes.setdefault(agent_id, set()).add(purpose)
            return response
        except BaseException:
            # A local adapter can fail without transport events, but invoking
            # it is still a factual model-call attempt for coverage.
            if agent_id:
                with event_state_lock:
                    if model_start_counts.get(agent_id, 0) == starts_before:
                        called_agent_ids.add(agent_id)
                        agent_call_purposes.setdefault(agent_id, set()).add(purpose)
            raise
        finally:
            llm_client.thread_local.call_purpose = previous_purpose

    llm_call = tracked_llm_call

    def signal(
        source_agent_id: str,
        target_agent_id: str,
        signal_type: str,
        summary: str,
        *,
        workstream_id: str | None = None,
        work_item_id: str | None = None,
        contract: WorkContract | None = None,
        artifacts: tuple[str, ...] = (),
        evidence: dict[str, Any] | None = None,
    ) -> None:
        handoff: HandoffEnvelope | None = None
        if contract is not None:
            handoff = HandoffEnvelope(
                handoff_id=new_id("handoff"),
                task_id=task_id,
                contract_id=contract.id,
                contract_version=contract.version,
                source_agent_id=source_agent_id,
                target_agent_id=target_agent_id,
                signal_type=signal_type,
                artifacts=artifacts,
                evidence={"summary": summary[:1000], **dict(evidence or {})},
                workstream_id=workstream_id,
                work_item_id=work_item_id,
            )
            _append_repository_handoff(repo, handoff)
        contextual_event(
            {
                "type": "agent_message",
                "role": "agent",
                "agent_instance_id": source_agent_id,
                "source_agent_id": source_agent_id,
                "target_agent_id": target_agent_id,
                "signal_type": signal_type,
                "summary": summary[:1000],
                "workstream_id": workstream_id,
                "work_item_id": work_item_id,
                "handoff_id": handoff.handoff_id if handoff is not None else None,
                "contract_id": contract.id if contract is not None else None,
                "contract_version": (contract.version if contract is not None else None),
                "contract_sha256": (
                    work_contract_sha256(contract) if contract is not None else None
                ),
                "artifacts": list(artifacts),
                "evidence": dict(evidence or {}),
            }
        )

    active_director_model, active_director_effort = _resolve_agent_config(
        agent_config_resolver,
        agent_id=director_id,
        role="director",
        model=director_model,
        effort=director_effort,
    )
    _configure_role_thread(
        role="director",
        model=active_director_model,
        effort=active_director_effort,
        worker_model=worker_model,
        worker_effort=worker_effort,
        reviewer_model=reviewer_model,
        reviewer_effort=reviewer_effort,
        event_sink=contextual_event,
        account_mode=account_mode,
    )
    llm_client.thread_local.agent_instance_id = director_id
    llm_client.thread_local.task_id = task_id
    llm_client.thread_local.session_id = session_id
    manager_cap = min(bounds.manager_cap, bounds.max_workstreams)
    director_user = (
        "## BOUNDED HIERARCHY CONTRACT\n"
        f"Select between 1 and {manager_cap} Manager workstreams. "
        "This is a maximum, not a quota. Prefer more Managers when the goal has "
        "substantial independent domains with non-conflicting ownership; never "
        "create filler workstreams. Use unique lowercase kebab-case IDs and set "
        "requested_manager_count equal to workstreams.length.\n\n"
        f"## USER GOAL\n{task_description}\n\n"
        f"## PROJECT TREE\n```\n{tree}\n```\n\n"
        f"## RULES\n{rules}\n\n## DECISIONS\n{decisions}\n\n"
        "## FAN-OUT CAPS AND EXECUTION SLOTS\n"
        f"- Maximum logical Managers: {manager_cap}\n"
        f"- Parallel Manager execution slots: {bounds.max_parallel_managers}\n"
        f"- Maximum Coder work items per Manager: {bounds.coders_per_manager}\n"
        "- Dedicated Testers: exactly one shared Tester per selected Manager\n"
        f"- Global parallel workers: {bounds.max_parallel_workers}\n"
        f"- Absolute workstream safety cap: {bounds.max_workstreams}\n"
        "Chia USER GOAL thành workstream lớn hợp lý trong cap. "
        "Mỗi phần tử workstreams[] = 1 id = 1 Manager. "
        "requested_manager_count = workstreams.length. "
        "Mỗi write_scope là path/file hoặc thư mục tương đối trong project. "
        "Dependency dùng đúng ID workstream. "
        "Giải thích lựa chọn fan-out trong selected_fanout_reason. "
        "Trả đúng một JSON `_action=submit_workstream_plan` ở cuối, không prose sau JSON."
    )
    if director_source_context:
        director_user += (
            "\n\n## USER-SELECTED SOURCE CONTEXT (READ-ONLY, BOUNDED)\n"
            + director_source_context
            + "\nUse this source as ground truth when selecting workstreams and "
            "contracts. Do not infer similarly named files."
        )
    if previous_plan is not None:
        checkpoint = {
            "previous_plan": {
                "revision": previous_plan.revision,
                "status": previous_plan.status.value,
                "workstreams": [
                    {
                        "id": stream.id,
                        "title": stream.title,
                        "status": stream.status.value,
                        "dependencies": list(stream.dependencies),
                        "write_scopes": list(stream.write_scopes),
                        "contract": to_dict(stream.contract),
                        "work_items": [
                            {
                                "id": item.id,
                                "status": item.status.value,
                                "dependencies": list(item.dependencies),
                                "write_scopes": list(item.write_scopes),
                                "contract": to_dict(item.contract),
                            }
                            for item in stream.work_items
                        ],
                    }
                    for stream in previous_plan.workstreams
                ],
            },
            "attempts": [
                {
                    "work_item_id": attempt.work_item_id,
                    "status": attempt.status.value,
                    "evidence": attempt.evidence,
                    "error": attempt.error,
                }
                for attempt in previous_attempts[-100:]
            ],
            "handoffs": [to_dict(handoff) for handoff in previous_handoffs[-100:]],
        }
        director_user += (
            "\n\n## RESUME CHECKPOINT\n"
            + json.dumps(checkpoint, ensure_ascii=False, indent=2)[-30000:]
            + "\n\nLập revision tiếp theo, không lặp lại phần đã có evidence approved."
        )
        contextual_event(
            {
                "type": "resume_checkpoint_loaded",
                "role": "director",
                "agent_instance_id": director_id,
                "previous_revision": previous_revision,
                "attempt_count": len(previous_attempts),
                "handoff_count": len(previous_handoffs),
            }
        )
    llm_client.reserve_account_cohort(task_id, (director_id,))
    contextual_event(
        {
            "type": "agent_started",
            "role": "director",
            "agent_instance_id": director_id,
            "status": "planning",
            "goal": task_description,
            "prompt": director_user,
            "model": active_director_model,
            "effort": active_director_effort,
        }
    )
    director_result: ToolCallResult | None = None
    for planner_attempt in range(1, config.PLANNER_COUNT_RETRIES + 2):
        prompt = director_user
        if planner_attempt > 1:
            prompt += (
                "\n\n## REQUIRED CORRECTION\nChoose between 1 and "
                f"{manager_cap} workstreams. requested_manager_count must equal "
                "workstreams.length and must not exceed the cap."
            )
        candidate = llm_call(DIRECTOR_PLAN_PROMPT, prompt, DIRECTOR_PLAN_TOOLS)
        try:
            _require_planner_ids(
                list(candidate.tool_input.get("workstreams") or []),
                label="workstream",
            )
            error = _planner_selection_error(
                candidate,
                list_field="workstreams",
                count_field="requested_manager_count",
                maximum=manager_cap,
                label="Director",
            )
        except RuntimeError as exc:
            error = str(exc)
        if error is None:
            director_result = candidate
            break
        contextual_event(
            {
                "type": "planner_count_rejected",
                "role": "director",
                "agent_instance_id": director_id,
                "attempt": planner_attempt,
                "error": error,
                "max_manager_count": manager_cap,
            }
        )
    if director_result is None:
        raise RuntimeError(f"Director did not return a valid plan within maximum {manager_cap}")
    plan = _director_plan_from_result(
        director_result,
        task_id=task_id,
        session_id=session_id,
        goal=task_description,
        revision=previous_revision + 1,
        limits=bounds,
        max_manager_count=manager_cap,
    )
    # Full DAG validation is deferred until every Manager sub-plan exists so
    # dependency failures share the structured whole-plan preflight path.
    plan = _advance_conflicting_contract_versions(repo, plan)
    _persist_plan_contract_versions(repo, plan)
    repo.save_plan(
        plan,
        expected_previous_revision=(previous_revision if previous_plan is not None else None),
    )
    contextual_event(
        {
            "type": "plan_created",
            "role": "director",
            "agent_instance_id": director_id,
            "action": "submit_workstream_plan",
            "requested_manager_count": plan.requested_manager_count,
            "max_manager_count": manager_cap,
            "selected_fanout_reason": director_result.tool_input.get("selected_fanout_reason")
            or director_result.tool_input.get("summary"),
            "workstreams": [
                {
                    "id": stream.id,
                    "title": stream.title,
                    "goal": stream.goal,
                    "dependencies": list(stream.dependencies),
                    "write_scopes": list(stream.write_scopes),
                    "contract": to_dict(stream.contract),
                }
                for stream in plan.workstreams
            ],
        }
    )
    contextual_event(
        {
            "type": "fanout_selected",
            "role": "director",
            "agent_instance_id": director_id,
            "level": "manager",
            "maximum": manager_cap,
            "selected": plan.requested_manager_count,
            "unused_capacity": manager_cap - plan.requested_manager_count,
            "reason": director_result.tool_input.get("selected_fanout_reason")
            or director_result.tool_input.get("summary"),
        }
    )

    scheduler = HierarchicalScheduler(bounds, repository=repo)
    # Pre-assign Manager IDs so the UI/DAG shows all N managers immediately,
    # even when workstream dependencies serialize execution.
    manager_ids = {
        stream.id: _resolve_repository_agent_id(
            repo,
            task_id=task_id,
            role="manager",
            assignment_id=stream.id,
        )
        for stream in plan.workstreams
    }
    expected_manager_ids = tuple(manager_ids[stream.id] for stream in plan.workstreams)
    llm_client.reserve_account_cohort(task_id, expected_manager_ids)
    epoch_digest = hashlib.sha256(
        (
            f"{task_id}\0{session_id}\0{plan.revision}\0"
            + "\0".join(expected_manager_ids)
        ).encode("utf-8")
    ).hexdigest()[:24]
    execution_epoch = f"epoch_{epoch_digest}"
    execution_epoch_event = {
        "type": "execution_epoch_started",
        "task_id": task_id,
        "session_id": session_id,
        "role": "director",
        "agent_instance_id": director_id,
        "execution_epoch": execution_epoch,
        "execution_epoch_id": execution_epoch,
        "expected_manager_ids": list(expected_manager_ids),
        "expected_manager_count": len(expected_manager_ids),
        "plan_revision": plan.revision,
        "roster": [
            {
                "manager_agent_id": manager_ids[stream.id],
                "workstream_id": stream.id,
                "contract_id": stream.contract.id if stream.contract is not None else None,
                "contract_version": (
                    stream.contract.version if stream.contract is not None else None
                ),
                "roster_position": position,
                "metadata": {"title": stream.title},
            }
            for position, stream in enumerate(plan.workstreams)
        ],
        "status": "running",
    }
    _persist_execution_epoch(repo, execution_epoch_event)
    contextual_event(execution_epoch_event)
    manager_terminal_reports: dict[str, dict[str, Any]] = {}
    manager_report_lock = threading.RLock()
    report_barrier_emitted = False
    director_final_review_count = 0

    def settle_manager(
        stream: Workstream,
        *,
        statuses: Mapping[str, WorkStatus],
        evidence: Mapping[str, Mapping[str, Any]],
        synthesized: bool = False,
        reasons: Iterable[str] = (),
    ) -> dict[str, Any]:
        """Persist and emit exactly one terminal report for one frozen Manager."""
        manager_id = str(stream.manager_agent_id or manager_ids[stream.id])
        with manager_report_lock:
            existing = manager_terminal_reports.get(manager_id)
            if existing is not None:
                return existing
            for item in stream.work_items:
                item_status = WorkStatus(statuses.get(item.id, item.status))
                disposition = (
                    "completed"
                    if item_status == WorkStatus.APPROVED
                    else "abandoned"
                    if item_status in {WorkStatus.ABANDONED, WorkStatus.FAILED}
                    else "cancelled"
                    if item_status == WorkStatus.CANCELLED
                    else "skipped"
                )
                item_reason = str(
                    item_evidence.get(item.id, {}).get("failure_kind")
                    or item_evidence.get(item.id, {}).get("status")
                    or disposition
                )
                worker_id = str(item.metadata.get("worker_agent_id") or "")
                item_log_refs = _manager_log_references(
                    repo,
                    task_id=task_id,
                    agent_ids=(worker_id,),
                )
                _persist_terminal_disposition(
                    repo,
                    task_id=task_id,
                    execution_epoch=execution_epoch,
                    entity_kind="work_item",
                    entity_id=item.id,
                    disposition=disposition,
                    logical_agent_id=worker_id or None,
                    reason_code=item_reason,
                    summary=str(
                        item_evidence.get(item.id, {}).get("error")
                        or item_evidence.get(item.id, {}).get("reviewer_feedback")
                        or item_reason
                    ),
                    log_refs=item_log_refs,
                    metadata={"workstream_id": stream.id},
                )
            report = _build_manager_terminal_report(
                repository=repo,
                task_id=task_id,
                session_id=session_id,
                execution_epoch=execution_epoch,
                stream=stream,
                manager_id=manager_id,
                item_statuses=statuses,
                item_evidence=evidence,
                synthesized=synthesized,
                reasons=reasons,
            )
            _persist_manager_terminal_report(repo, report)
            _persist_terminal_disposition(
                repo,
                task_id=task_id,
                execution_epoch=execution_epoch,
                entity_kind="manager",
                entity_id=manager_id,
                disposition=str(report["status"]),
                logical_agent_id=manager_id,
                reason_code=(
                    "completed"
                    if report["status"] == "completed"
                    else "manager_terminal_report"
                ),
                summary="; ".join(report["reasons"]) or str(report["status"]),
                log_refs=report["log_refs"],
                metadata={"workstream_id": stream.id},
            )
            manager_terminal_reports[manager_id] = report
            contextual_event(report)
        release_ids = {
            manager_id,
            str(stream.metadata.get("tester_agent_id") or ""),
            *(
                str(item.metadata.get("worker_agent_id") or "")
                for item in stream.work_items
            ),
        }
        for agent_id in release_ids:
            if agent_id:
                llm_client.release_reserved_agent_account(task_id, agent_id)
        if report["status"] != "completed":
            hook = getattr(crisis_strategy, "on_abandonment", None)
            if callable(hook):
                _call_repository_hook(
                    hook,
                    values={
                        **report,
                        "report": report,
                        "context": report,
                    },
                    preferred_args=(report,),
                )
        return report

    def emit_manager_report_barrier() -> dict[str, Any]:
        nonlocal report_barrier_emitted
        with manager_report_lock:
            reported_manager_ids = sorted(manager_terminal_reports)
            expected = sorted(expected_manager_ids)
            barrier = {
                "type": "manager_report_barrier",
                "task_id": task_id,
                "session_id": session_id,
                "role": "director",
                "agent_instance_id": director_id,
                "execution_epoch": execution_epoch,
                "execution_epoch_id": execution_epoch,
                "expected_manager_ids": expected,
                "reported_manager_ids": reported_manager_ids,
                "expected_count": len(expected),
                "reported_count": len(reported_manager_ids),
                "satisfied": reported_manager_ids == expected,
                "status": "satisfied" if reported_manager_ids == expected else "blocked",
            }
            if not barrier["satisfied"]:
                missing = sorted(set(expected) - set(reported_manager_ids))
                raise RuntimeError(f"Manager report barrier incomplete; missing: {missing}")
            if not report_barrier_emitted:
                _persist_manager_report_barrier(repo, barrier)
                contextual_event(barrier)
                report_barrier_emitted = True
            return barrier

    register_agent_labels(plan, manager_ids)
    for stream in plan.workstreams:
        contextual_event(
            {
                "type": "agent_planned",
                "role": "manager",
                "agent_instance_id": manager_ids[stream.id],
                "workstream_id": stream.id,
                "workstream_title": stream.title,
                "status": "planned",
                "goal": stream.goal,
                "title": stream.title,
                "dependencies": list(stream.dependencies),
                "model": manager_model,
                "effort": manager_effort,
            }
        )
        signal(
            director_id,
            manager_ids[stream.id],
            "delegate_workstream",
            f"Director giao workstream: {stream.title}",
            workstream_id=stream.id,
            contract=stream.contract,
            artifacts=stream.contract.input_artifacts,
        )

    def plan_stream(stream: Workstream, manager_id: str) -> Workstream:
        active_manager_model, active_manager_effort = _resolve_agent_config(
            agent_config_resolver,
            agent_id=manager_id,
            role="manager",
            model=manager_model,
            effort=manager_effort,
        )
        _configure_role_thread(
            role="manager",
            model=active_manager_model,
            effort=active_manager_effort,
            worker_model=worker_model,
            worker_effort=worker_effort,
            reviewer_model=reviewer_model,
            reviewer_effort=reviewer_effort,
            event_sink=contextual_event,
            account_mode=account_mode,
        )
        llm_client.thread_local.agent_instance_id = manager_id
        llm_client.thread_local.task_id = task_id
        llm_client.thread_local.workstream_id = stream.id
        llm_client.thread_local.manager_id = manager_id
        coder_cap = bounds.coders_per_manager
        prompt = (
            "## BOUNDED HIERARCHY CONTRACT\n"
            f"This Manager may select 1 to {coder_cap} Coders, plus one shared "
            "Tester created by the backend. The cap is not a quota. Prefer more "
            "Coders for substantial independent packages with non-conflicting "
            "write scopes; never create filler packages. Set requested_worker_count "
            "equal to work_items.length and use unique lowercase kebab-case IDs.\n\n"
            f"## USER GOAL\n{task_description}\n\n"
            f"## WORKSTREAM ID\n{stream.id}\n"
            f"## TITLE\n{stream.title}\n"
            f"## GOAL\n{stream.goal}\n"
            "## ACCEPTANCE CRITERIA\n- "
            + "\n- ".join(stream.acceptance_criteria)
            + "\n## WRITE SCOPE\n- "
            + "\n- ".join(stream.write_scopes)
            + "\n## WORKSTREAM CONTRACT\n"
            + json.dumps(
                to_dict(stream.contract),
                ensure_ascii=False,
                indent=2,
            )
            + f"\n\n## PROJECT TREE\n```\n{tree}\n```\n\n"
            "## FAN-OUT CAP AND EXECUTION SLOTS\n"
            f"- Maximum Coders selectable: {coder_cap}\n"
            f"- Parallel coder slots for this Manager: {bounds.worker_parallel_cap}\n"
            f"- Global parallel coder slots: {bounds.max_parallel_workers}\n"
            "- selected_fanout_reason must explain why this count is appropriate.\n\n"
            "Tạo sub-DAG bao phủ 100% yêu cầu của workstream. "
            "Mỗi yêu cầu có thể bàn giao độc lập = 1 work item = 1 logical Worker; "
            "không gộp các yêu cầu không liên quan chỉ để giảm fan-out. "
            "Các file cùng phục vụ một yêu cầu có thể nằm trong cùng Worker pipeline; "
            "write_scopes liệt kê mọi file trong gói (1–"
            f"{config.MAX_FILES_PER_WORK_PACKAGE} file). "
            "KHÔNG tách từng __init__/file skeleton. "
            "Dependency chỉ giữa các gói lớn."
        )
        manager_source_context = _planner_source_context(
            _manager_sources(planner_sources, stream),
            total_limit=config.HIERARCHY_MANAGER_SOURCE_CHARS,
        )
        if manager_source_context:
            prompt += (
                "\n\n## RELEVANT USER-SELECTED SOURCE CONTEXT "
                "(READ-ONLY, BOUNDED)\n"
                + manager_source_context
                + "\nGround work-item paths and contracts in this exact source."
            )
        prior_stream: Workstream | None = None
        if previous_plan is not None:
            prior_stream = next(
                (candidate for candidate in previous_plan.workstreams if candidate.id == stream.id),
                None,
            )
            if prior_stream is not None:
                resume_items = [
                    {
                        "id": item.id.split(":", 1)[-1],
                        "status": item.status.value,
                        "contract": to_dict(item.contract),
                        "evidence": previous_item_evidence.get(item.id),
                    }
                    for item in prior_stream.work_items
                ]
                prompt += (
                    "\n\n## RESUME SUB-DAG\n"
                    + json.dumps(resume_items, ensure_ascii=False, indent=2)[-16000:]
                    + "\nReuse IDs for approved items exactly and do not change their "
                    "scope. Replan only failed, blocked, or pending work."
                )
        contextual_event(
            {
                "type": "agent_started",
                "role": "manager",
                "agent_instance_id": manager_id,
                "workstream_id": stream.id,
                "workstream_title": stream.title,
                "status": "planning",
                "goal": stream.goal,
                "prompt": prompt,
                "model": active_manager_model,
                "effort": active_manager_effort,
            }
        )
        manager_result: ToolCallResult | None = None
        planned: Workstream | None = None
        previous_planner_error = ""
        for planner_attempt in range(1, config.PLANNER_COUNT_RETRIES + 2):
            planner_prompt = prompt
            if planner_attempt > 1:
                planner_prompt += (
                    "\n\n## REQUIRED CORRECTION\n"
                    f"The previous plan was rejected: {previous_planner_error}\n"
                    "Return a corrected plan. Every file_path and write_scopes "
                    "entry must be one concrete text/code file, never a directory. "
                    f"Choose between 1 and {coder_cap} coder work items. "
                    "requested_worker_count must equal work_items.length and "
                    "must not exceed the cap."
                )
            candidate = llm_call(
                MANAGER_PLAN_PROMPT,
                planner_prompt,
                MANAGER_PLAN_TOOLS,
            )
            try:
                _require_planner_ids(
                    list(candidate.tool_input.get("work_items") or []),
                    label="work item",
                )
                error = _planner_selection_error(
                    candidate,
                    list_field="work_items",
                    count_field="requested_worker_count",
                    maximum=coder_cap,
                    label="Manager",
                )
                if error is None:
                    planned_candidate = _manager_stream_from_result(
                        candidate,
                        stream,
                        bounds,
                        max_coder_count=coder_cap,
                    )
                else:
                    planned_candidate = None
            except (RuntimeError, ValueError) as exc:
                error = str(exc)
                planned_candidate = None
            if error is None and planned_candidate is not None:
                manager_result = candidate
                planned = planned_candidate
                break
            previous_planner_error = str(error or "invalid Manager plan")
            contextual_event(
                {
                    "type": "planner_count_rejected",
                    "role": "manager",
                    "agent_instance_id": manager_id,
                    "manager_id": manager_id,
                    "workstream_id": stream.id,
                    "attempt": planner_attempt,
                    "error": error,
                    "max_coder_count": coder_cap,
                }
            )
        if manager_result is None or planned is None:
            raise RuntimeError(f"Manager did not return a valid plan within maximum {coder_cap}")
        if prior_stream is not None:
            prior_by_id = {item.id.split(":", 1)[-1]: item for item in prior_stream.work_items}
            reconciled_items: list[WorkItem] = []
            preserved_approved_ids: list[str] = []
            for item in planned.work_items:
                short_id = item.id.split(":", 1)[-1]
                prior_item = prior_by_id.get(short_id)
                if prior_item is None:
                    reconciled_items.append(item)
                    continue
                was_approved = prior_item.id in previous_item_evidence
                if was_approved and item.contract != prior_item.contract:
                    # Resume may re-ask a Manager for the remaining work. The
                    # model is not allowed to invalidate accepted evidence by
                    # rewriting an already-approved contract; preserve that
                    # durable item and continue planning unfinished siblings.
                    reconciled_items.append(prior_item)
                    preserved_approved_ids.append(prior_item.id)
                    continue
                if (
                    item.contract != prior_item.contract
                    and item.contract.id == prior_item.contract.id
                    and item.contract.version <= prior_item.contract.version
                ):
                    raise RuntimeError(
                        f"Changed Work Contract {item.contract.id!r} must "
                        "increment contract_version"
                    )
                reconciled_items.append(item)
            planned = replace(planned, work_items=tuple(reconciled_items))
            if preserved_approved_ids:
                contextual_event(
                    {
                        "type": "agent_progress",
                        "role": "manager",
                        "agent_instance_id": manager_id,
                        "manager_id": manager_id,
                        "workstream_id": stream.id,
                        "status": "preserved_approved_contracts",
                        "summary": (
                            "Resume kept previously approved Work Contracts unchanged."
                        ),
                        "work_item_ids": preserved_approved_ids,
                    }
                )
        planned = replace(
            planned,
            manager_agent_id=manager_id,
        )
        contextual_event(
            {
                "type": "manager_plan_created",
                "role": "manager",
                "agent_instance_id": manager_id,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "workstream_title": stream.title,
                "action": "submit_work_item_plan",
                "requested_worker_count": planned.requested_worker_count,
                "max_coder_count": coder_cap,
                "selected_fanout_reason": manager_result.tool_input.get("selected_fanout_reason")
                or manager_result.tool_input.get("summary"),
                "status": "ready",
                "dropped_dependencies": list(planned.metadata.get("dropped_dependencies") or []),
                "work_items": [
                    {
                        "id": item.id,
                        "title": item.title,
                        "dependencies": list(item.dependencies),
                        "write_scopes": list(item.write_scopes),
                        "file_path": item.metadata["file_path"],
                        "contract": to_dict(item.contract),
                        "worker_agent_id": item.metadata.get("worker_agent_id"),
                    }
                    for item in planned.work_items
                ],
            }
        )
        contextual_event(
            {
                "type": "fanout_selected",
                "role": "manager",
                "agent_instance_id": manager_id,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "level": "worker",
                "maximum": coder_cap,
                "selected": planned.requested_worker_count,
                "unused_capacity": coder_cap - planned.requested_worker_count,
                "reason": manager_result.tool_input.get("selected_fanout_reason")
                or manager_result.tool_input.get("summary"),
            }
        )
        return planned

    # Eager-plan every Manager selected by the Director. In maximum-parallelism
    # mode no execution cap is allowed to leave a planned Manager idle.
    planned_revision = plan.revision
    with ThreadPoolExecutor(
        max_workers=max(
            1,
            len(plan.workstreams)
            if max_parallelism_enabled()
            else min(len(plan.workstreams), bounds.max_parallel_managers),
        ),
        thread_name_prefix="manager-plan",
    ) as pool:
        plan_futures = {
            pool.submit(plan_stream, stream, manager_ids[stream.id]): stream
            for stream in plan.workstreams
        }
        planned_by_id: dict[str, Workstream] = {}
        plan_errors: dict[str, str] = {}
        for future in as_completed(plan_futures):
            stream = plan_futures[future]
            try:
                planned_by_id[stream.id] = future.result()
            except Exception as exc:
                if isinstance(exc, llm_client.AccountPoolExhaustedError):
                    raise
                plan_errors[stream.id] = str(exc)
                contextual_event(
                    {
                        "type": "workstream_failed",
                        "role": "manager",
                        "agent_instance_id": manager_ids[stream.id],
                        "workstream_id": stream.id,
                        "status": "failed",
                        "summary": f"Manager plan failed: {exc}",
                        "error": str(exc),
                    }
                )
    ordered_streams = []
    for stream in plan.workstreams:
        if stream.id in planned_by_id:
            ordered_streams.append(planned_by_id[stream.id])
        else:
            ordered_streams.append(
                replace(
                    stream,
                    manager_agent_id=manager_ids[stream.id],
                    status=WorkStatus.FAILED,
                    metadata={
                        **dict(stream.metadata),
                        "plan_error": plan_errors.get(stream.id, "unknown"),
                    },
                )
            )
    # Manager sub-plans are in, so the real inputs and outputs of every stream
    # are known and a declared ordering can be checked against them instead of
    # taken on trust.
    ordered_streams, relaxed_dependencies = _relax_decorative_dependencies(ordered_streams)
    if relaxed_dependencies:
        contextual_event(
            {
                "type": "plan_dependencies_relaxed",
                "role": "director",
                "agent_instance_id": director_id,
                "status": "ready",
                "dropped": relaxed_dependencies,
                "summary": (
                    f"Released {len(relaxed_dependencies)} workstream dependency(ies) that no "
                    "shared file justified, so those streams run in parallel."
                ),
            }
        )
    plan = replace(
        plan,
        revision=planned_revision + 1,
        status=PlanStatus.READY,
        workstreams=tuple(ordered_streams),
        metadata={
            **dict(plan.metadata),
            "fanout": {
                **dict(plan.metadata.get("fanout") or {}),
                "workers": {
                    stream.id: dict(stream.metadata.get("fanout") or {})
                    for stream in ordered_streams
                    if stream.metadata.get("fanout")
                },
            },
        },
        updated_at=utc_now(),
    )
    plan = _advance_conflicting_contract_versions(repo, plan)
    _persist_plan_contract_versions(repo, plan)
    # Admit the plan before minting or announcing Worker/Tester identities.
    # Contract paperwork is advisory; this list now contains only conditions
    # that make execution impossible or unsafe. A rejected plan therefore
    # cannot produce ghost Worker nodes that immediately become FAILED.
    preflight_warnings: list[dict[str, Any]] = []
    preflight_issues = _preflight_plan(
        root=root,
        plan=plan,
        limits=bounds,
        allow_new_files=allow_new_files,
        test_cmd=test_cmd,
        available_artifacts=persisted_handoff_artifacts,
        warnings=preflight_warnings,
    )
    preflight_stopped_stream_ids: set[str] = set()
    preflight_item_evidence: dict[str, dict[str, Any]] = {}
    if preflight_issues:
        admission_issues = list(preflight_issues)
        directly_rejected = {
            str(issue["workstream_id"])
            for issue in admission_issues
            if issue.get("workstream_id")
        }
        if any(not issue.get("workstream_id") for issue in admission_issues):
            directly_rejected = {stream.id for stream in plan.workstreams}
        preflight_stopped_stream_ids = set(directly_rejected)
        if not max_parallelism_enabled():
            changed = True
            while changed:
                changed = False
                for stream in plan.workstreams:
                    if stream.id in preflight_stopped_stream_ids:
                        continue
                    if any(
                        dependency in preflight_stopped_stream_ids
                        for dependency in stream.dependencies
                    ):
                        preflight_stopped_stream_ids.add(stream.id)
                        changed = True

        admitted_streams: list[Workstream] = []
        for stream in plan.workstreams:
            if stream.id not in preflight_stopped_stream_ids:
                admitted_streams.append(stream)
                continue
            directly_failed = stream.id in directly_rejected
            item_status = WorkStatus.FAILED if directly_failed else WorkStatus.BLOCKED
            stopped_items = []
            for item in stream.work_items:
                stopped_items.append(replace(item, status=item_status))
                preflight_item_evidence[item.id] = {
                    "accepted": False,
                    "status": "plan_rejected" if directly_failed else "blocked",
                    "failure_kind": (
                        "plan_admission" if directly_failed else "dependency"
                    ),
                    "retryable": False,
                    "error": (
                        "Plan admission rejected before Worker identity creation"
                        if directly_failed
                        else "Upstream plan was rejected before execution"
                    ),
                    "contract_id": item.contract.id,
                    "contract_version": item.contract.version,
                    "package_files": list(item.write_scopes),
                }
            admitted_streams.append(
                replace(
                    stream,
                    status=WorkStatus.FAILED if directly_failed else WorkStatus.BLOCKED,
                    work_items=tuple(stopped_items),
                )
            )
            contextual_event(
                {
                    "type": "workstream_failed" if directly_failed else "workstream_blocked",
                    "role": "manager",
                    "agent_instance_id": stream.manager_agent_id,
                    "workstream_id": stream.id,
                    "status": "plan_rejected" if directly_failed else "blocked",
                    "failure_kind": (
                        "plan_admission" if directly_failed else "dependency"
                    ),
                    "summary": (
                        "Workstream was rejected before any Worker or Tester "
                        "identity was announced."
                    ),
                }
            )

        plan = replace(
            plan,
            workstreams=tuple(admitted_streams),
            metadata={
                **dict(plan.metadata),
                "admission_issues": admission_issues,
            },
            updated_at=utc_now(),
        )
        contextual_event(
            {
                "type": (
                    "plan_admission_rejected"
                    if len(preflight_stopped_stream_ids) == len(plan.workstreams)
                    else "plan_admission_partial"
                ),
                "role": "director",
                "agent_instance_id": director_id,
                "status": (
                    "rejected"
                    if len(preflight_stopped_stream_ids) == len(plan.workstreams)
                    else "degraded"
                ),
                "failure_kind": "unsafe_or_unexecutable_plan",
                "issues": admission_issues,
                "rejected_workstream_ids": sorted(preflight_stopped_stream_ids),
                "summary": (
                    f"{len(preflight_stopped_stream_ids)} workstream(s) were rejected "
                    "before any child identity was announced; safe workstreams continue."
                ),
            }
        )
        # The legacy post-announcement preflight failure branch below must
        # never run. Rejected streams are already settled without child IDs.
        preflight_issues = []
    # Resolve and announce every admitted logical child. A queued event
    # describes a planned identity; the actual spawn is emitted
    # only after the ticket executor has built a runnable prompt.
    announced_streams: list[Workstream] = []
    pending_child_events: list[dict[str, Any]] = []
    pending_delegations: list[tuple[str, str, WorkItem, str]] = []
    for stream_index, stream in enumerate(plan.workstreams, start=1):
        if stream.id in preflight_stopped_stream_ids:
            announced_streams.append(stream)
            continue
        manager_id = (
            stream.manager_agent_id
            or manager_ids.get(stream.id)
            or _resolve_repository_agent_id(
                repo,
                task_id=task_id,
                role="manager",
                assignment_id=stream.id,
            )
        )
        # Name each agent as its id is minted: the announcement below is the
        # first event anyone sees, and a later bulk registration would leave
        # every queued Worker showing a raw id in the UI and the terminal.
        agent_labels[manager_id] = f"Manager {stream_index}"
        stamped_items: list[WorkItem] = []
        for item_index, item in enumerate(stream.work_items, start=1):
            worker_id = _resolve_repository_agent_id(
                repo,
                task_id=task_id,
                role="worker",
                assignment_id=item.id,
            )
            agent_labels[worker_id] = f"Worker {stream_index}.{item_index}"
            stamped_items.append(
                replace(
                    item,
                    metadata={
                        **dict(item.metadata),
                        "worker_agent_id": worker_id,
                    },
                )
            )
            pending_child_events.append(
                {
                    "type": "agent_planned",
                    "role": "worker",
                    "agent_instance_id": worker_id,
                    "manager_id": manager_id,
                    "workstream_id": stream.id,
                    "work_item_id": item.id,
                    "status": "planned",
                    "goal": item.goal,
                    "title": item.title,
                    "contract": to_dict(item.contract),
                    "model": worker_model,
                    "effort": worker_effort,
                }
            )
            if item.id in previous_item_evidence:
                pending_child_events.append(
                    {
                        "type": "agent_skipped",
                        "role": "worker",
                        "agent_instance_id": worker_id,
                        "manager_id": manager_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "status": "skipped",
                        "reason": "approved_resume_evidence",
                    }
                )
            pending_delegations.append((manager_id, worker_id, item, stream.id))
        tester_id = _resolve_repository_agent_id(
            repo,
            task_id=task_id,
            role="tester",
            assignment_id=stream.id,
        )
        agent_labels[tester_id] = f"Tester {stream_index}"
        announced_streams.append(
            replace(
                stream,
                work_items=tuple(stamped_items),
                metadata={
                    **dict(stream.metadata),
                    "tester_agent_id": tester_id,
                },
            )
        )
        pending_child_events.append(
            {
                "type": "agent_planned",
                "role": "tester",
                "agent_instance_id": tester_id,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "status": "planned",
                "goal": f"Review completed workstream: {stream.title}",
                "title": f"Tester · {stream.title}",
                "model": reviewer_model,
                "effort": reviewer_effort,
            }
        )
        if stream.work_items and all(
            item.id in previous_item_evidence for item in stream.work_items
        ):
            pending_child_events.append(
                {
                    "type": "agent_skipped",
                    "role": "tester",
                    "agent_instance_id": tester_id,
                    "manager_id": manager_id,
                    "workstream_id": stream.id,
                    "status": "skipped",
                    "reason": "approved_resume_evidence",
                }
            )
    planned_child_ids = [
        str(item.metadata.get("worker_agent_id"))
        for stream in announced_streams
        for item in stream.work_items
        if item.metadata.get("worker_agent_id")
    ] + [
        str(stream.metadata.get("tester_agent_id"))
        for stream in announced_streams
        if stream.metadata.get("tester_agent_id")
    ]
    if planned_child_ids:
        llm_client.reserve_account_cohort(task_id, planned_child_ids)
    for event in pending_child_events:
        contextual_event(event)
    for manager_id, worker_id, item, workstream_id in pending_delegations:
        signal(
            manager_id,
            worker_id,
            "delegate_work_item",
            f"Manager giao work item: {item.title}",
            workstream_id=workstream_id,
            work_item_id=item.id,
            contract=item.contract,
            artifacts=item.contract.input_artifacts,
        )
    plan = replace(
        plan,
        workstreams=tuple(announced_streams),
        updated_at=utc_now(),
    )
    # Worker and tester ids only exist once the manager sub-plans are announced.
    register_agent_labels(plan, manager_ids)
    planned_manager_ids = [
        str(stream.manager_agent_id or manager_ids[stream.id]) for stream in plan.workstreams
    ]
    planned_worker_ids = [
        str(item.metadata.get("worker_agent_id"))
        for stream in plan.workstreams
        for item in stream.work_items
        if item.metadata.get("worker_agent_id")
    ]
    planned_tester_ids = [
        str(stream.metadata.get("tester_agent_id"))
        for stream in plan.workstreams
        if stream.metadata.get("tester_agent_id")
    ]
    primary_agent_ids = [
        director_id,
        *planned_manager_ids,
        *planned_worker_ids,
        *planned_tester_ids,
    ]
    if len(primary_agent_ids) != len(set(primary_agent_ids)):
        raise RuntimeError("Hierarchy planned duplicate logical agent identities")
    contextual_event(
        {
            "type": "hierarchy_fanout_planned",
            "role": "director",
            "agent_instance_id": director_id,
            "status": "ready",
            "manager_count": len(plan.workstreams),
            "coder_count": len(planned_worker_ids),
            "tester_count": len(planned_tester_ids),
            "child_agent_count": len(planned_worker_ids) + len(planned_tester_ids),
            "primary_agent_count": len(primary_agent_ids),
            "primary_child_request_count": (
                len(plan.workstreams) + len(planned_worker_ids) + len(planned_tester_ids)
            ),
            "manager_agent_ids": planned_manager_ids,
            "worker_agent_ids": planned_worker_ids,
            "tester_agent_ids": planned_tester_ids,
            "primary_agent_ids": primary_agent_ids,
            "max_manager_count": manager_cap,
            "unused_manager_capacity": manager_cap - len(plan.workstreams),
            "max_coders_per_manager": bounds.coders_per_manager,
            "max_parallel_managers": bounds.max_parallel_managers,
            "max_parallel_workers_per_manager": bounds.worker_parallel_cap,
            "max_parallel_workers": bounds.max_parallel_workers,
            "dependency_aware": not max_parallelism_enabled(),
            "summary": (
                f"Planned {len(planned_worker_ids)} coders + "
                f"{len(planned_tester_ids)} testers; "
                + (
                    "all coder model calls may start immediately."
                    if max_parallelism_enabled()
                    else (
                        "execution follows DAG dependencies with at most "
                        f"{bounds.max_parallel_workers} concurrent worker pipelines."
                    )
                )
            ),
        }
    )
    if preflight_warnings:
        contextual_event(
            {
                "type": "plan_contract_warnings",
                "role": "director",
                "agent_instance_id": director_id,
                "status": "ready",
                "issues": preflight_warnings,
                "summary": (
                    f"{len(preflight_warnings)} contract detail(s) look inconsistent; "
                    "execution continues."
                ),
            }
        )
    # Admission already settled rejected streams before child IDs were minted.
    # The legacy branch remains only for replay compatibility and is unreachable
    # for newly planned runs.
    if preflight_issues:
        preflight_evidence: dict[str, dict[str, Any]] = dict(previous_item_evidence)
        directly_failed_streams = {
            str(issue["workstream_id"])
            for issue in preflight_issues
            if issue.get("workstream_id")
            and issue.get("code") not in {"missing_contract_input", "plan_preflight_aborted"}
        }
        blocked_streams: set[str] = set()
        changed = True
        while changed:
            changed = False
            for candidate in plan.workstreams:
                if candidate.id in directly_failed_streams | blocked_streams:
                    continue
                if any(
                    dependency in directly_failed_streams | blocked_streams
                    for dependency in candidate.dependencies
                ):
                    blocked_streams.add(candidate.id)
                    changed = True
        # An issue with no workstream of its own condemns the plan as a whole,
        # for example a dependency cycle. Everything else belongs to the streams
        # it names: those fail, whatever depends on them blocks, and the streams
        # that are actually healthy still run. Without this split one malformed
        # path in one contract used to cancel every worker in the run.
        plan_wide_issue = any(not issue.get("workstream_id") for issue in preflight_issues)
        stopped_stream_ids = directly_failed_streams | blocked_streams
        surviving_streams = [
            stream for stream in plan.workstreams if stream.id not in stopped_stream_ids
        ]
        abort_all = plan_wide_issue or not surviving_streams
        failed_streams: list[Workstream] = []
        for stream in plan.workstreams:
            if not abort_all and stream.id not in stopped_stream_ids:
                continue
            stream_is_blocked = stream.id in blocked_streams
            blocked_by = [
                dependency
                for dependency in stream.dependencies
                if dependency in directly_failed_streams | blocked_streams
            ]
            failed_items: list[WorkItem] = []
            for item in stream.work_items:
                if item.id in previous_item_evidence:
                    failed_items.append(replace(item, status=WorkStatus.APPROVED))
                    continue
                related = [
                    issue
                    for issue in preflight_issues
                    if item.id in issue["work_item_ids"]
                    or (issue["workstream_id"] == stream.id and not issue["work_item_ids"])
                    or (issue["workstream_id"] is None and not issue["work_item_ids"])
                ]
                if stream_is_blocked:
                    error = "Blocked by failed upstream workstream" + (
                        ": " + ", ".join(blocked_by) if blocked_by else ""
                    )
                    evidence = {
                        "accepted": False,
                        "status": "blocked",
                        "failure_kind": "dependency",
                        "retryable": False,
                        "error": error,
                        "blocked_by": blocked_by,
                        "contract_id": item.contract.id,
                        "contract_version": item.contract.version,
                        "package_files": list(item.write_scopes),
                    }
                    preflight_evidence[item.id] = evidence
                    failed_items.append(replace(item, status=WorkStatus.BLOCKED))
                    contextual_event(
                        {
                            "type": "agent_blocked",
                            "role": "worker",
                            "agent_instance_id": _resolve_repository_agent_id(
                                repo,
                                task_id=task_id,
                                role="worker",
                                assignment_id=item.id,
                            ),
                            "manager_id": stream.manager_agent_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "status": "blocked",
                            "failure_kind": "dependency",
                            "blocked_by": blocked_by,
                            "summary": error,
                        }
                    )
                    result.turns.append(
                        TurnOutcome(
                            tool_name="hierarchy_work_item",
                            accepted=False,
                            detail=error,
                            stop_loop=False,
                        )
                    )
                    continue
                if not related:
                    related = [
                        {
                            "code": "plan_preflight_aborted",
                            "message": (
                                "Execution was not started because another "
                                "whole-plan preflight check failed"
                            ),
                            "workstream_id": stream.id,
                            "work_item_ids": [item.id],
                            "paths": [],
                        }
                    ]
                error = "; ".join(str(issue["message"]) for issue in related)
                worker_id = _resolve_repository_agent_id(
                    repo,
                    task_id=task_id,
                    role="worker",
                    assignment_id=item.id,
                )
                attempt_number = (
                    max(
                        (attempt.number for attempt in repo.list_attempts(task_id, item.id)),
                        default=0,
                    )
                    + 1
                )
                attempt = Attempt(
                    id=new_id("attempt"),
                    task_id=task_id,
                    workstream_id=stream.id,
                    work_item_id=item.id,
                    number=attempt_number,
                    worker_agent_id=worker_id,
                    status=AttemptStatus.FAILED,
                    started_at=utc_now(),
                    finished_at=utc_now(),
                    evidence={
                        "accepted": False,
                        "status": "preflight_failed",
                        "failure_kind": "preflight",
                        "retryable": False,
                        "error": error[:2000],
                        "issues": related,
                        "contract_id": item.contract.id,
                        "contract_version": item.contract.version,
                        "package_files": list(item.write_scopes),
                    },
                    error=error[:2000],
                )
                repo.save_attempt(attempt)
                preflight_evidence[item.id] = dict(attempt.evidence)
                failed_items.append(replace(item, status=WorkStatus.FAILED))
                event_payload = {
                    "role": "worker",
                    "agent_instance_id": worker_id,
                    "manager_id": stream.manager_agent_id,
                    "workstream_id": stream.id,
                    "work_item_id": item.id,
                    "attempt_id": attempt.id,
                    "status": "preflight_failed",
                    "error": error[:2000],
                    "failure_kind": "preflight",
                    "issues": related,
                }
                contextual_event({"type": "preflight_failed", **event_payload})
                # Existing consumers already understand agent_failed.
                contextual_event({"type": "agent_failed", **event_payload})
                contextual_event(
                    {
                        "type": "execution_result",
                        "role": "worker",
                        "agent_instance_id": worker_id,
                        "manager_id": stream.manager_agent_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "attempt_id": attempt.id,
                        "package_files": list(item.write_scopes),
                        "contract_id": item.contract.id,
                        "contract_version": item.contract.version,
                        **attempt.evidence,
                    }
                )
                result.turns.append(
                    TurnOutcome(
                        tool_name="hierarchy_work_item",
                        accepted=False,
                        detail=error[:1000],
                        stop_loop=False,
                    )
                )
            failed_streams.append(
                replace(
                    stream,
                    status=(WorkStatus.BLOCKED if stream_is_blocked else WorkStatus.FAILED),
                    work_items=tuple(failed_items),
                )
            )
            tester_id = stream.metadata.get("tester_agent_id")
            if tester_id:
                contextual_event(
                    {
                        "type": "agent_blocked",
                        "role": "tester",
                        "agent_instance_id": tester_id,
                        "manager_id": stream.manager_agent_id,
                        "workstream_id": stream.id,
                        "status": "blocked",
                        "failure_kind": "plan_preflight",
                        "blocked_by": blocked_by,
                        "summary": (
                            "Tester was not called because its workstream "
                            "did not pass plan preflight."
                        ),
                    }
                )
            if stream_is_blocked:
                contextual_event(
                    {
                        "type": "workstream_blocked",
                        "role": "manager",
                        "agent_instance_id": stream.manager_agent_id,
                        "workstream_id": stream.id,
                        "status": "blocked",
                        "blocked_by": blocked_by,
                        "summary": "Blocked by failed upstream workstream",
                    }
                )
            else:
                contextual_event(
                    {
                        "type": "workstream_failed",
                        "role": "manager",
                        "agent_instance_id": stream.manager_agent_id,
                        "workstream_id": stream.id,
                        "status": "preflight_failed",
                        "summary": "Workstream did not pass plan preflight.",
                    }
                )
        if not abort_all:
            # Carry the settled streams into the plan the executor is about to
            # run so reconciliation still sees them, then let the healthy ones
            # proceed through the normal path below.
            settled_by_id = {stream.id: stream for stream in failed_streams}
            plan = replace(
                plan,
                workstreams=tuple(
                    settled_by_id.get(stream.id, stream) for stream in plan.workstreams
                ),
                updated_at=utc_now(),
            )
            preflight_stopped_stream_ids = set(stopped_stream_ids)
            preflight_item_evidence = dict(preflight_evidence)
            contextual_event(
                {
                    "type": "plan_preflight_partial",
                    "role": "director",
                    "agent_instance_id": director_id,
                    "status": "degraded",
                    "issues": preflight_issues,
                    "stopped_workstream_ids": sorted(stopped_stream_ids),
                    "surviving_workstream_ids": [stream.id for stream in surviving_streams],
                    "summary": (
                        f"{len(stopped_stream_ids)} workstream(s) failed preflight; "
                        f"{len(surviving_streams)} continue to execution."
                    ),
                }
            )
        else:
            final_plan = replace(
                plan,
                status=PlanStatus.FAILED,
                workstreams=tuple(failed_streams),
                updated_at=utc_now(),
            )
            repo.save_plan(final_plan, expected_previous_revision=planned_revision)
            with event_state_lock:
                reconciliation = reconcile_completion(
                    final_plan,
                    item_evidence=preflight_evidence,
                    call_outcomes=model_call_outcomes,
                    director_agent_id=director_id,
                    called_agent_ids=called_agent_ids,
                    started_agent_ids=started_agent_ids,
                    agent_dispositions=agent_dispositions,
                    agent_no_call_reasons=agent_no_call_reasons,
                    agent_call_purposes=agent_call_purposes,
                )
            contextual_event(
                {
                    "type": "plan_preflight_failed",
                    "role": "director",
                    "agent_instance_id": director_id,
                    "status": "preflight_failed",
                    "issues": preflight_issues,
                    "worker_calls_started": 0,
                }
            )
            contextual_event(
                {
                    "type": "completion_reconciliation",
                    "role": "director",
                    "agent_instance_id": director_id,
                    "status": ("balanced" if reconciliation["balanced"] else "failed"),
                    **reconciliation,
                }
            )
            contextual_event(
                {
                    "type": "hierarchy_failed",
                    "role": "director",
                    "agent_instance_id": director_id,
                    "status": "preflight_failed",
                    "verdict": "revise",
                    "summary": "Whole-plan preflight rejected execution.",
                    "issues": preflight_issues,
                }
            )
            result.stopped_reason = "hierarchy_failed"
            result.final_state = {
                "session_id": session_id,
                "plan_revision": final_plan.revision,
                "plan_status": final_plan.status.value,
                "turn_count": len(result.turns),
                "completed_tickets": [
                    {
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "file_path": item.metadata.get("file_path"),
                        "status": item.status.value,
                    }
                    for stream in final_plan.workstreams
                    for item in stream.work_items
                ],
                "last_execution_result": "Whole-plan preflight failed",
                "last_review_verdict": "revise",
                "last_reviewer_feedback": "No Worker model calls were started.",
                "preflight_issues": preflight_issues,
                "reconciliation": reconciliation,
            }
            llm_client.thread_local.event_sink = None
            llm_client.thread_local.agent_instance_id = None
            release_known_agent_accounts()
            return result

    plan = replace(
        plan,
        status=PlanStatus.RUNNING,
        updated_at=utc_now(),
    )
    repo.save_plan(plan, expected_previous_revision=planned_revision)
    plan_lock = threading.RLock()

    project_lock = _project_lock_for(root)
    planned_worker_count = sum(len(stream.work_items) for stream in plan.workstreams)
    worker_capacity = (
        max(1, planned_worker_count)
        if max_parallelism_enabled()
        else bounds.max_parallel_workers
    )
    worker_semaphore = threading.BoundedSemaphore(worker_capacity)
    worker_submission_semaphore = threading.BoundedSemaphore(worker_capacity)
    tester_locks = {
        str(stream.metadata["tester_agent_id"]): threading.Lock()
        for stream in plan.workstreams
        if stream.metadata.get("tester_agent_id")
    }
    stream_status: dict[str, WorkStatus] = {
        stream.id: (
            WorkStatus.FAILED
            if stream.id in plan_errors
            else stream.status
            if stream.id in preflight_stopped_stream_ids
            else WorkStatus.APPROVED
            if stream.work_items
            and all(item.id in previous_item_evidence for item in stream.work_items)
            else WorkStatus.PENDING
        )
        for stream in plan.workstreams
    }
    item_evidence: dict[str, dict[str, Any]] = {
        **preflight_item_evidence,
        **previous_item_evidence,
    }
    item_statuses: dict[str, WorkStatus] = {
        item.id: (
            WorkStatus.APPROVED
            if item.id in previous_item_evidence
            else item.status
            if stream.id in preflight_stopped_stream_ids
            else WorkStatus.PENDING
        )
        for stream in plan.workstreams
        for item in stream.work_items
    }
    instruction_overrides: dict[str, str] = {}
    completed_package_files: dict[str, set[str]] = {}
    for previous_attempt in previous_attempts:
        completed = previous_attempt.evidence.get("completed_file_paths") or []
        if completed:
            completed_package_files.setdefault(previous_attempt.work_item_id, set()).update(
                str(path) for path in completed
            )

    def task_cancelled() -> bool:
        if cancelled is not None and cancelled():
            return True
        try:
            from server import is_current_thread_stopped
        except ImportError:
            return False
        return bool(is_current_thread_stopped())

    def failed_execution(
        file_path: str,
        exc: BaseException,
    ) -> TicketExecutionResult:
        if isinstance(exc, llm_client.AccountPoolExhaustedError):
            raise exc
        error = f"{type(exc).__name__}: {exc}"
        decision = retry_policy.classify_exception(exc)
        was_cancelled = decision.failure_kind == "cancelled" or task_cancelled()
        safety_rejection = decision.failure_kind in {
            "safety",
            "unsupported_binary",
            "unsupported_worker_target",
        } or isinstance(exc, safety.GitError)
        failure_kind = (
            "cancelled"
            if was_cancelled
            else "safety"
            if isinstance(exc, safety.GitError)
            else decision.failure_kind
        )
        return TicketExecutionResult(
            accepted=False,
            file_path=file_path,
            worker_feedback="",
            execution_result=f"Fail: {error}",
            reviewer_feedback="",
            reviewer_verdict="not_run",
            next_instructions=(
                "Kiểm tra project mode/path trước khi giao lại."
                if safety_rejection
                else "Manager cần đổi chiến lược trước lần thử tiếp theo."
            ),
            patch_sha256=None,
            additions=0,
            deletions=0,
            syntax_status="not_run",
            test_status="not_run",
            test_output="",
            error=error[:2000],
            failure_kind=failure_kind,
            retryable=retry_policy.retryable_for_failure(failure_kind),
        )

    def execute_item(
        stream: Workstream,
        item: WorkItem,
        tester_id: str,
    ) -> TicketExecutionResult:
        manager_id = (
            stream.manager_agent_id
            or manager_ids.get(stream.id)
            or _resolve_repository_agent_id(
                repo,
                task_id=task_id,
                role="manager",
                assignment_id=stream.id,
            )
        )
        worker_id = str(
            item.metadata.get("worker_agent_id")
            or _resolve_repository_agent_id(
                repo,
                task_id=task_id,
                role="worker",
                assignment_id=item.id,
            )
        )
        last: TicketExecutionResult | None = None
        contextual_event(
            {
                "type": "agent_progress",
                "role": "worker",
                "agent_instance_id": worker_id,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "work_item_id": item.id,
                "status": (
                    "starting" if max_parallelism_enabled() else "waiting_for_slot"
                ),
                "goal": item.goal,
                "title": item.title,
            }
        )
        contextual_event(
            {
                "type": "agent_progress",
                "role": "tester",
                "agent_instance_id": tester_id,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "work_item_id": item.id,
                "status": "waiting_for_worker_output",
                "goal": f"Review {item.title}",
                "title": f"Tester · {item.title}",
            }
        )
        claim_acquired = False
        worker_slot_acquired = False
        try:
            llm_client.thread_local.task_id = task_id

            if not max_parallelism_enabled():
                claim_acquired = scheduler.scope_claims.wait_acquire(
                    worker_id,
                    item.write_scopes,
                    timeout=config.SCOPE_CLAIM_TIMEOUT_SECONDS,
                    cancelled=task_cancelled,
                )
                if not claim_acquired:
                    if task_cancelled():
                        raise llm_client.ModelRequestAborted("🛑 Task đã bị hủy cưỡng chế!")
                    raise RuntimeError(
                        "Không lấy được write-scope claim sau "
                        f"{config.SCOPE_CLAIM_TIMEOUT_SECONDS:g} giây"
                    )
            while not worker_semaphore.acquire(timeout=0.25):
                if task_cancelled():
                    raise llm_client.ModelRequestAborted("🛑 Task đã bị hủy cưỡng chế!")
            worker_slot_acquired = True
            assert item.contract is not None
            missing_item_inputs = _runtime_missing_contract_inputs(
                root,
                item.contract,
            )
            if missing_item_inputs:
                # An absent input is a fact about the workspace, not a verdict
                # on this agent. Whoever writes that file may still be running,
                # or the planner may have named a file the goal never promised.
                # Telling the worker is strictly better than refusing to let it
                # start, which cost the run an agent it had already planned.
                contextual_event(
                    {
                        "type": "agent_progress",
                        "role": "worker",
                        "agent_instance_id": worker_id,
                        "manager_id": manager_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "status": "running",
                        "summary": (
                            "Starting without "
                            f"{', '.join(missing_item_inputs)}; "
                            "those inputs do not exist yet."
                        ),
                        "missing_inputs": list(missing_item_inputs),
                    }
                )
            contextual_event(
                {
                    # The ticket executor emits agent_started only after it has
                    # validated the target and built the actual Worker prompt.
                    # Until then this is preparation, not a spawned model call.
                    "type": "agent_progress",
                    "role": "worker",
                    "agent_instance_id": worker_id,
                    "manager_id": manager_id,
                    "workstream_id": stream.id,
                    "work_item_id": item.id,
                    "status": "starting",
                    "goal": item.goal,
                    "title": item.title,
                }
            )

            missing_input_note = (
                "\n\n## INPUTS NOT PRESENT YET\n"
                + "\n".join(f"- {path}" for path in missing_item_inputs)
                + "\nWrite against the contract and the goal rather than reading "
                "these files, and do not assume their contents.\n"
                if missing_item_inputs
                else ""
            )
            declared_input_context = _contract_input_context(root, item.contract)
            instructions = (
                str(item.metadata["instructions"])
                + missing_input_note
                + declared_input_context
                + "\n\n## TYPED WORK CONTRACT\n"
                + json.dumps(
                    to_dict(item.contract),
                    ensure_ascii=False,
                    indent=2,
                )
                + "\nProduce the required evidence and do not write outside "
                "contract.write_scopes."
            )
            if instruction_overrides.get(item.id):
                instructions += (
                    "\n\n## MANAGER RECOVERY INSTRUCTIONS\n" + instruction_overrides[item.id]
                )
            persisted_attempts = repo.list_attempts(task_id, item.id)
            previous_numbers = [attempt.number for attempt in persisted_attempts]
            first_attempt_number = max(previous_numbers, default=0) + 1
            completed_files = completed_package_files.setdefault(item.id, set())
            work_lease_id = f"task/{task_id}/work-item/{item.id}"
            # One invocation is one factual Attempt. Any subsequent invocation
            # must be authorized by the crisis strategy in execute_stream.
            for attempt_number in (first_attempt_number,):
                active_worker_model, active_worker_effort = _resolve_agent_config(
                    agent_config_resolver,
                    agent_id=worker_id,
                    role="worker",
                    model=worker_model,
                    effort=worker_effort,
                )
                active_tester_model, active_tester_effort = _resolve_agent_config(
                    agent_config_resolver,
                    agent_id=tester_id,
                    role="tester",
                    model=reviewer_model,
                    effort=reviewer_effort,
                )
                attempt_id = new_id("attempt")
                if not scheduler.acquire_work_lease(work_lease_id, worker_id, 900):
                    raise RuntimeError(f"Work item {item.id} đang có lease khác")
                attempt = Attempt(
                    id=attempt_id,
                    task_id=task_id,
                    workstream_id=stream.id,
                    work_item_id=item.id,
                    number=attempt_number,
                    worker_agent_id=worker_id,
                    status=AttemptStatus.RUNNING,
                    started_at=utc_now(),
                )
                repo.save_attempt(attempt)
                _configure_role_thread(
                    role="worker",
                    model=active_worker_model,
                    effort=active_worker_effort,
                    worker_model=active_worker_model,
                    worker_effort=active_worker_effort,
                    reviewer_model=active_tester_model,
                    reviewer_effort=active_tester_effort,
                    event_sink=contextual_event,
                    account_mode=account_mode,
                )
                llm_client.thread_local.agent_instance_id = worker_id
                llm_client.thread_local.manager_id = manager_id
                llm_client.thread_local.workstream_id = stream.id
                llm_client.thread_local.work_item_id = item.id
                llm_client.thread_local.task_id = task_id
                llm_client.thread_local.session_id = session_id
                llm_client.thread_local.execution_attempt_id = attempt_id
                package_files: list[str] = []
                last = None
                file_index = 0
                target_file = str(item.metadata.get("file_path") or item.id)
                try:
                    package_files = _package_files(item, root=root)
                    pending_files = [path for path in package_files if path not in completed_files]
                    if not pending_files:
                        prior = item_evidence.get(item.id, {})
                        last = TicketExecutionResult(
                            accepted=True,
                            file_path=str(prior.get("file_path") or package_files[-1]),
                            worker_feedback=str(prior.get("worker_feedback") or ""),
                            execution_result=str(
                                prior.get("execution_result")
                                or "Previously approved package files reused"
                            ),
                            reviewer_feedback=str(prior.get("reviewer_feedback") or ""),
                            reviewer_verdict="approved",
                            next_instructions="",
                            patch_sha256=prior.get("patch_sha256"),
                            additions=int(prior.get("additions") or 0),
                            deletions=int(prior.get("deletions") or 0),
                            syntax_status=str(prior.get("syntax_status") or "not_run"),
                            test_status=str(prior.get("test_status") or "not_run"),
                            test_output=str(prior.get("test_output") or ""),
                            test_scope=str(prior.get("test_scope") or "item"),
                        )
                        file_index = len(package_files)
                    for target_file in pending_files:
                        file_index = package_files.index(target_file) + 1
                        file_instructions = (
                            f"{instructions}\n\n"
                            f"## GÓI NHU CẦU CHÍNH\n{item.title}\n"
                            f"## FILE TRONG GÓI ({file_index}/{len(package_files)})\n"
                            f"Đang làm: {target_file}\n"
                            "Toàn bộ file gói:\n- "
                            + "\n- ".join(package_files)
                            + "\nChỉ sửa đúng file đang làm ở turn này; các file khác "
                            "sẽ được giao ở lượt tiếp theo trong cùng work item."
                        )
                        last = execute_work_item(
                            root=root,
                            task_goal=task_description,
                            workstream_goal=stream.goal,
                            work_item_id=item.id,
                            file_path=target_file,
                            instructions=file_instructions,
                            acceptance_criteria=item.acceptance_criteria,
                            test_focus=str(item.metadata.get("test_focus", "")),
                            # Full project tests run only after the DAG is complete.
                            test_cmd=None,
                            allow_new_files=allow_new_files,
                            llm_call=llm_call,
                            on_event=contextual_event,
                            execution_lock=project_lock,
                            tester_lock=tester_locks[tester_id],
                            effect_repository=repo,
                            project_lease=project_lease,
                            task_id=task_id,
                            session_id=session_id,
                            workstream_id=stream.id,
                            manager_agent_id=manager_id,
                            worker_agent_id=worker_id,
                            tester_agent_id=tester_id,
                            attempt_id=attempt_id,
                            approval_policy=(
                                item.contract.approval_policy.value
                                if item.contract is not None
                                else ApprovalPolicy.RISK_BASED.value
                            ),
                            risk_level=(
                                item.contract.risk_level.value
                                if item.contract is not None
                                else RiskLevel.LOW.value
                            ),
                            approval_callback=approval_callback,
                            cancelled=task_cancelled,
                            defer_tests_to_integration=bool(
                                item.metadata.get("contract_mode") == "typed"
                                and item.contract is not None
                                and test_evidence.requires_integration_test(
                                    item.contract.test_requirements
                                )
                            ),
                        )
                        if not last.accepted:
                            break
                        completed_files.add(target_file)
                except BaseException as exc:
                    if isinstance(exc, _AccountPoolFatalSignal):
                        repo.save_attempt(
                            replace(
                                attempt,
                                status=AttemptStatus.FAILED,
                                finished_at=utc_now(),
                                evidence={
                                    "accepted": False,
                                    "status": "failed",
                                    "failure_kind": "account_pool_exhausted",
                                    "retryable": False,
                                    "error": str(exc.cause),
                                    "contract_id": item.contract.id,
                                    "contract_version": item.contract.version,
                                    "package_files": package_files,
                                    "completed_file_paths": sorted(completed_files),
                                },
                                error=str(exc.cause),
                            )
                        )
                        raise exc.cause
                    if isinstance(exc, llm_client.AccountPoolExhaustedError):
                        repo.save_attempt(
                            replace(
                                attempt,
                                status=AttemptStatus.FAILED,
                                finished_at=utc_now(),
                                evidence={
                                    "accepted": False,
                                    "status": "failed",
                                    "failure_kind": "account_pool_exhausted",
                                    "retryable": False,
                                    "error": str(exc),
                                    "contract_id": item.contract.id,
                                    "contract_version": item.contract.version,
                                    "package_files": package_files,
                                    "completed_file_paths": sorted(completed_files),
                                },
                                error=str(exc),
                            )
                        )
                        raise
                    last = failed_execution(target_file, exc)
                    contextual_event(
                        {
                            "type": (
                                "agent_cancelled"
                                if last.failure_kind == "cancelled"
                                else "agent_failed"
                            ),
                            "role": "worker",
                            "agent_instance_id": worker_id,
                            "manager_id": manager_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "attempt_id": attempt_id,
                            "status": (
                                "cancelled"
                                if last.failure_kind == "cancelled"
                                else "failed"
                            ),
                            "error": last.error,
                            "failure_kind": last.failure_kind,
                            "file_path": target_file,
                        }
                    )
                assert last is not None
                approval_evidence = {
                    **last.evidence(),
                    "package_files": package_files,
                    "completed_file_paths": sorted(completed_files),
                }
                approval_issues = (
                    _contract_approval_issues(item, approval_evidence) if last.accepted else []
                )
                if approval_issues:
                    message = "Contract approval blocked: " + "; ".join(approval_issues)
                    last = replace(
                        last,
                        accepted=False,
                        reviewer_verdict="revise",
                        next_instructions=message,
                        error=message,
                        failure_kind="contract_approval",
                        retryable=False,
                    )
                if last.failure_kind == "cancelled":
                    for role, agent_id in (
                        ("worker", worker_id),
                        ("tester", tester_id),
                    ):
                        contextual_event(
                            {
                                "type": "agent_cancelled",
                                "role": role,
                                "agent_instance_id": agent_id,
                                "manager_id": manager_id,
                                "workstream_id": stream.id,
                                "work_item_id": item.id,
                                "attempt_id": attempt_id,
                                "status": "cancelled",
                                "error": last.error or "Task cancelled",
                                "failure_kind": "cancelled",
                            }
                        )
                repo.save_attempt(
                    replace(
                        attempt,
                        status=(
                            AttemptStatus.SUCCEEDED
                            if last.accepted
                            else AttemptStatus.CANCELLED
                            if last.failure_kind == "cancelled"
                            else AttemptStatus.FAILED
                        ),
                        finished_at=utc_now(),
                        evidence={
                            **last.evidence(),
                            "contract_id": item.contract.id,
                            "contract_version": item.contract.version,
                            "package_files": package_files,
                            "files_completed": len(completed_files),
                            "completed_file_paths": sorted(completed_files),
                        },
                        error=last.error or None,
                    )
                )
                scheduler.release_work_lease(work_lease_id, worker_id)
                contextual_event(
                    {
                        "type": "execution_result",
                        "role": "worker",
                        "agent_instance_id": worker_id,
                        "manager_id": manager_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "attempt_id": attempt_id,
                        "package_files": package_files,
                        "contract_id": item.contract.id,
                        "contract_version": item.contract.version,
                        **last.evidence(),
                    }
                )
                signal(
                    worker_id,
                    manager_id,
                    "work_item_result",
                    (
                        f"Worker báo work item {item.title}: "
                        f"{'approved' if last.accepted else 'revise'}"
                    ),
                    workstream_id=stream.id,
                    work_item_id=item.id,
                    contract=item.contract,
                    artifacts=tuple(sorted(completed_files)),
                    evidence={
                        **last.evidence(),
                        "attempt_id": attempt_id,
                    },
                )
            assert last is not None
            return last
        finally:
            llm_client.thread_local.execution_attempt_id = None
            scheduler.release_work_lease(
                f"task/{task_id}/work-item/{item.id}",
                worker_id,
            )
            if claim_acquired:
                scheduler.scope_claims.release(worker_id)
            if worker_slot_acquired:
                worker_semaphore.release()

    def execute_stream(stream: Workstream) -> tuple[str, bool]:
        if stream.status == WorkStatus.FAILED and not stream.work_items:
            return stream.id, False
        manager_id = (
            stream.manager_agent_id
            or manager_ids.get(stream.id)
            or _resolve_repository_agent_id(
                repo,
                task_id=task_id,
                role="manager",
                assignment_id=stream.id,
            )
        )
        tester_id = str(
            stream.metadata.get("tester_agent_id")
            or _resolve_repository_agent_id(
                repo,
                task_id=task_id,
                role="tester",
                assignment_id=stream.id,
            )
        )
        statuses = {
            item.id: item_statuses.get(item.id, WorkStatus.PENDING) for item in stream.work_items
        }
        assert stream.contract is not None
        missing_stream_inputs = _runtime_missing_contract_inputs(
            root,
            stream.contract,
        )
        if missing_stream_inputs:
            # Blocking the whole workstream here cost five planned coders their
            # turn because one declared input had not appeared yet. The worker
            # is told what is absent and writes against the contract instead.
            contextual_event(
                {
                    "type": "agent_progress",
                    "role": "manager",
                    "agent_instance_id": manager_id,
                    "workstream_id": stream.id,
                    "status": "running",
                    "summary": (
                        f"Starting without {', '.join(missing_stream_inputs)}; "
                        "those inputs do not exist yet."
                    ),
                    "missing_inputs": list(missing_stream_inputs),
                }
            )
        contextual_event(
            {
                "type": "workstream_started",
                "role": "manager",
                "agent_instance_id": manager_id,
                "workstream_id": stream.id,
                "status": "running",
                "goal": stream.goal,
            }
        )

        def abandon_incomplete(reason: str) -> None:
            for item in stream.work_items:
                current = statuses.get(item.id, WorkStatus.PENDING)
                if current in {WorkStatus.APPROVED, WorkStatus.CANCELLED}:
                    continue
                worker_id = str(item.metadata.get("worker_agent_id") or "")
                attempted = bool(repo.list_attempts(task_id, item.id)) or worker_id in {
                    *started_agent_ids,
                    *called_agent_ids,
                }
                terminal_status = (
                    WorkStatus.ABANDONED if attempted else WorkStatus.SKIPPED
                )
                statuses[item.id] = terminal_status
                item_statuses[item.id] = terminal_status
                existing = dict(item_evidence.get(item.id) or {})
                failure_kind = (
                    "agent_abandoned"
                    if terminal_status == WorkStatus.ABANDONED
                    else "dependency_skipped"
                )
                item_evidence[item.id] = {
                    **existing,
                    "accepted": False,
                    "status": terminal_status.value,
                    "failure_kind": failure_kind,
                    "retryable": False,
                    "error": str(existing.get("error") or reason),
                }
                if terminal_status == WorkStatus.ABANDONED:
                    contextual_event(
                        {
                            "type": "agent_abandoned",
                            "role": "worker",
                            "agent_instance_id": worker_id,
                            "manager_id": manager_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "status": "abandoned",
                            "reason": reason,
                            "failure_kind": "agent_abandoned",
                            "artifacts": sorted(
                                completed_package_files.get(item.id, set())
                            ),
                            "log_refs": [],
                        }
                    )
                    contextual_event(
                        {
                            "type": "work_item_abandoned",
                            "role": "worker",
                            "agent_instance_id": worker_id,
                            "manager_id": manager_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "status": "abandoned",
                            "reason": reason,
                            "failure_kind": "agent_abandoned",
                            "artifacts": sorted(
                                completed_package_files.get(item.id, set())
                            ),
                            "log_refs": [],
                        }
                    )
                else:
                    contextual_event(
                        {
                            "type": "agent_skipped",
                            "role": "worker",
                            "agent_instance_id": worker_id,
                            "manager_id": manager_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "status": "skipped",
                            "reason": reason,
                        }
                    )
                    contextual_event(
                        {
                            "type": "work_item_skipped",
                            "role": "worker",
                            "agent_instance_id": worker_id,
                            "manager_id": manager_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "status": "skipped",
                            "reason": reason,
                            "blocked_by": list(item.dependencies),
                        }
                    )
                if worker_id:
                    llm_client.release_agent_account(worker_id)

        recovery_cycle = 0
        while True:
            while any(status == WorkStatus.PENDING for status in statuses.values()):
                if task_cancelled():
                    for item_id, status in list(statuses.items()):
                        if status == WorkStatus.PENDING:
                            statuses[item_id] = WorkStatus.CANCELLED
                            item_statuses[item_id] = WorkStatus.CANCELLED
                            item_evidence[item_id] = {
                                "accepted": False,
                                "status": "cancelled",
                                "error": "task stopped before scheduling",
                                "failure_kind": "cancelled",
                                "retryable": False,
                            }
                            item = next(
                                candidate
                                for candidate in stream.work_items
                                if candidate.id == item_id
                            )
                            contextual_event(
                                {
                                    "type": "agent_cancelled",
                                    "role": "worker",
                                    "agent_instance_id": item.metadata.get("worker_agent_id"),
                                    "manager_id": manager_id,
                                    "workstream_id": stream.id,
                                    "work_item_id": item_id,
                                    "status": "cancelled",
                                    "error": "Task stopped before scheduling",
                                    "failure_kind": "cancelled",
                                }
                            )
                    break
                ready = ready_work_items(stream, statuses=statuses)
                if not ready:
                    # Remaining items are blocked by failed dependencies.
                    for item_id, status in list(statuses.items()):
                        if status == WorkStatus.PENDING:
                            statuses[item_id] = WorkStatus.BLOCKED
                            item_statuses[item_id] = WorkStatus.BLOCKED
                            item_evidence[item_id] = {
                                "accepted": False,
                                "status": "blocked",
                                "error": "blocked by failed dependency",
                                "failure_kind": "dependency",
                                "retryable": retry_policy.retryable_for_failure("dependency"),
                            }
                            item = next(
                                candidate
                                for candidate in stream.work_items
                                if candidate.id == item_id
                            )
                            contextual_event(
                                {
                                    "type": "agent_blocked",
                                    "role": "worker",
                                    "agent_instance_id": item.metadata.get("worker_agent_id"),
                                    "manager_id": manager_id,
                                    "workstream_id": stream.id,
                                    "work_item_id": item_id,
                                    "status": "blocked",
                                    "failure_kind": "dependency",
                                    "blocked_by": list(item.dependencies),
                                    "summary": "Blocked by failed dependency",
                                }
                            )
                    break
                selected = scheduler.select_work_items(
                    stream,
                    active_for_manager=0,
                    active_global=0,
                    statuses=statuses,
                    cancelled=task_cancelled,
                )
                if not selected:
                    if task_cancelled():
                        continue
                    selected = ready[:1]
                with ThreadPoolExecutor(
                    max_workers=(
                        len(selected)
                        if max_parallelism_enabled()
                        else min(
                            len(selected),
                            stream.requested_worker_count,
                            bounds.worker_parallel_cap,
                        )
                    ),
                    thread_name_prefix=f"workers-{stream.id}",
                ) as pool:
                    futures = {}
                    for launch_index, item in enumerate(selected):
                        submission_acquired = False
                        while not submission_acquired:
                            submission_acquired = worker_submission_semaphore.acquire(timeout=0.25)
                            if not submission_acquired and task_cancelled():
                                break
                        if not submission_acquired:
                            continue
                        if task_cancelled():
                            worker_submission_semaphore.release()
                            continue
                        # Workers start as soon as they are selected, but a short
                        # gap between launches keeps a whole batch from hitting
                        # the provider on the same instant.
                        if launch_index and _worker_launch_stagger_seconds():
                            _sleep_unless_cancelled(
                                _worker_launch_stagger_seconds(),
                                task_cancelled,
                            )
                        try:
                            future = pool.submit(execute_item, stream, item, tester_id)
                        except BaseException:
                            worker_submission_semaphore.release()
                            raise
                        future.add_done_callback(
                            lambda _future: worker_submission_semaphore.release()
                        )
                        futures[future] = item
                    for future in as_completed(futures):
                        item = futures[future]
                        try:
                            execution = future.result()
                        except Exception as exc:
                            if isinstance(exc, llm_client.AccountPoolExhaustedError):
                                raise
                            was_cancelled = task_cancelled()
                            retry_decision = retry_policy.classify_exception(exc)
                            terminal_status = (
                                WorkStatus.CANCELLED if was_cancelled else WorkStatus.FAILED
                            )
                            statuses[item.id] = terminal_status
                            item_statuses[item.id] = terminal_status
                            item_evidence[item.id] = {
                                "accepted": False,
                                "error": f"{type(exc).__name__}: {exc}",
                                "failure_kind": (
                                    "cancelled" if was_cancelled else retry_decision.failure_kind
                                ),
                                "retryable": (
                                    False if was_cancelled else retry_decision.retryable
                                ),
                                "contract_id": item.contract.id,
                                "contract_version": item.contract.version,
                            }
                            contextual_event(
                                {
                                    "type": (
                                        "agent_cancelled" if was_cancelled else "agent_failed"
                                    ),
                                    "role": "worker",
                                    "agent_instance_id": item.metadata.get("worker_agent_id"),
                                    "manager_id": manager_id,
                                    "workstream_id": stream.id,
                                    "work_item_id": item.id,
                                    "status": ("cancelled" if was_cancelled else "failed"),
                                    "error": str(exc),
                                    "failure_kind": ("cancelled" if was_cancelled else "scheduler"),
                                }
                            )
                        else:
                            statuses[item.id] = (
                                WorkStatus.APPROVED
                                if execution.accepted
                                else WorkStatus.CANCELLED
                                if execution.failure_kind == "cancelled"
                                else WorkStatus.FAILED
                            )
                            item_statuses[item.id] = statuses[item.id]
                            item_evidence[item.id] = {
                                **execution.evidence(),
                                "contract_id": item.contract.id,
                                "contract_version": item.contract.version,
                            }

            if task_cancelled() or any(
                status == WorkStatus.CANCELLED for status in statuses.values()
            ):
                for role, agent_id in (
                    ("manager", manager_id),
                    ("tester", tester_id),
                ):
                    contextual_event(
                        {
                            "type": "agent_cancelled",
                            "role": role,
                            "agent_instance_id": agent_id,
                            "manager_id": manager_id,
                            "workstream_id": stream.id,
                            "status": "cancelled",
                            "error": "Task stopped before workstream completion",
                            "failure_kind": "cancelled",
                        }
                    )
                contextual_event(
                    {
                        "type": "workstream_failed",
                        "role": "manager",
                        "agent_instance_id": manager_id,
                        "workstream_id": stream.id,
                        "status": "cancelled",
                        "summary": "Task stopped before workstream completion",
                    }
                )
                return stream.id, False

            if tester_id not in called_agent_ids:
                contextual_event(
                    {
                        "type": "agent_blocked",
                        "role": "tester",
                        "agent_instance_id": tester_id,
                        "manager_id": manager_id,
                        "workstream_id": stream.id,
                        "status": "blocked",
                        "failure_kind": "no_reviewable_output",
                        "summary": (
                            "Tester was not called because no Worker output "
                            "reached the review stage."
                        ),
                    }
                )
            active_manager_model, active_manager_effort = _resolve_agent_config(
                agent_config_resolver,
                agent_id=manager_id,
                role="manager",
                model=manager_model,
                effort=manager_effort,
            )
            _configure_role_thread(
                role="manager",
                model=active_manager_model,
                effort=active_manager_effort,
                worker_model=worker_model,
                worker_effort=worker_effort,
                reviewer_model=reviewer_model,
                reviewer_effort=reviewer_effort,
                event_sink=contextual_event,
                account_mode=account_mode,
            )
            llm_client.thread_local.agent_instance_id = manager_id
            llm_client.thread_local.task_id = task_id
            llm_client.thread_local.session_id = session_id
            llm_client.thread_local.manager_id = manager_id
            llm_client.thread_local.workstream_id = stream.id
            llm_client.thread_local.work_item_id = None
            llm_client.thread_local.execution_attempt_id = None
            review_prompt = (
                f"## WORKSTREAM\n{stream.title}\n{stream.goal}\n\n"
                "## ACCEPTANCE\n- "
                + "\n- ".join(stream.acceptance_criteria)
                + "\n\n## REMEDIATION GENERATION\n"
                + f"{recovery_cycle}\n"
                + "\n## ITEM STATUS\n"
                + json.dumps(
                    {key: value.value for key, value in statuses.items()},
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n\n## BACKEND EVIDENCE\n"
                + json.dumps(
                    {item.id: item_evidence.get(item.id, {}) for item in stream.work_items},
                    ensure_ascii=False,
                    indent=2,
                )[-30000:]
            )
            manager_review = llm_call(
                MANAGER_REVIEW_PROMPT,
                review_prompt,
                MANAGER_REVIEW_TOOLS,
            )
            all_items_approved = all(status == WorkStatus.APPROVED for status in statuses.values())
            approved = (
                all_items_approved
                and manager_review.tool_name == "complete_workstream"
                and manager_review.tool_input.get("verdict") == "approved"
            )
            if approved:
                contextual_event(
                    {
                        "type": "workstream_completed",
                        "role": "manager",
                        "agent_instance_id": manager_id,
                        "workstream_id": stream.id,
                        "status": "approved",
                        "remediation_generation": recovery_cycle,
                        "verdict": manager_review.tool_input.get("verdict"),
                        "summary": manager_review.tool_input.get("summary"),
                    }
                )
                signal(
                    manager_id,
                    director_id,
                    "workstream_result",
                    f"Manager báo workstream {stream.title}: approved",
                    workstream_id=stream.id,
                    contract=stream.contract,
                    artifacts=stream.contract.expected_outputs,
                    evidence={"status": "approved"},
                )
                return stream.id, True

            retryable_ids = [
                item_id
                for item_id, status in statuses.items()
                if status in {WorkStatus.FAILED, WorkStatus.BLOCKED}
                and item_evidence.get(item_id, {}).get("retryable") is True
            ]
            affected_ids = retryable_ids or [
                item_id
                for item_id, status in statuses.items()
                if status != WorkStatus.APPROVED
            ]
            if not affected_ids and statuses:
                # The backend evidence passed but the Manager rejected the
                # workstream contract. Reopen those factual outputs only when
                # the crisis strategy supplies a remediation.
                affected_ids = list(statuses)
            next_instructions = str(
                manager_review.tool_input.get("next_instructions") or ""
            ).strip()
            crisis_id = new_id("crisis")
            failure_kinds = sorted(
                {
                    str(item_evidence.get(item_id, {}).get("failure_kind") or "manager_review")
                    for item_id in affected_ids
                }
            )
            crisis_context = {
                "crisis_id": crisis_id,
                "scope": "workstream",
                "task_id": task_id,
                "session_id": session_id,
                "execution_epoch": execution_epoch,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "failure_kind": ",".join(failure_kinds) or "manager_review",
                "reason": str(
                    manager_review.tool_input.get("summary")
                    or "Manager requested remediation"
                ),
                "retryable": bool(retryable_ids or next_instructions),
                "affected_manager_ids": [manager_id],
                "affected_work_item_ids": affected_ids,
                "errors": {
                    item_id: item_evidence.get(item_id, {}).get("error")
                    for item_id in affected_ids
                },
                "suggested_instructions": next_instructions,
            }
            contextual_event({"type": "crisis_detected", **crisis_context})
            remediation = _decide_remediation(crisis_strategy, crisis_context)
            remediation, remediation_attempt_id = _reserve_remediation_attempt(
                repo,
                context=crisis_context,
                decision=remediation,
            )
            if not remediation.retries or not affected_ids:
                reason = remediation.reason or crisis_context["reason"]
                for item_id in affected_ids:
                    if statuses.get(item_id) == WorkStatus.APPROVED:
                        statuses[item_id] = WorkStatus.FAILED
                        item_statuses[item_id] = WorkStatus.FAILED
                        item_evidence[item_id] = {
                            **dict(item_evidence.get(item_id) or {}),
                            "accepted": False,
                            "failure_kind": "manager_review",
                            "retryable": False,
                            "error": reason,
                        }
                abandon_incomplete(reason)
                has_approved = any(
                    statuses.get(item.id) == WorkStatus.APPROVED
                    for item in stream.work_items
                )
                contextual_event(
                    {
                        "type": "remediation_exhausted",
                        **crisis_context,
                        "reason": reason,
                    }
                )
                contextual_event(
                    {
                        "type": (
                            "workstream_completed" if has_approved else "workstream_failed"
                        ),
                        "role": "manager",
                        "agent_instance_id": manager_id,
                        "workstream_id": stream.id,
                        "status": "partial" if has_approved else "abandoned",
                        "failure_kind": "remediation_exhausted",
                        "summary": reason,
                    }
                )
                signal(
                    manager_id,
                    director_id,
                    "workstream_result",
                    f"Manager báo workstream {stream.title}: "
                    f"{'partial' if has_approved else 'revise'}",
                    workstream_id=stream.id,
                    contract=stream.contract,
                    artifacts=stream.contract.expected_outputs,
                    evidence={"status": "partial" if has_approved else "revise"},
                )
                # Keep the DAG alive when this stream still produced approved
                # work. Downstream workers already tolerate missing inputs.
                return stream.id, has_approved

            remediation_event = {
                "crisis_id": crisis_id,
                "scope": "workstream",
                "role": "manager",
                "agent_instance_id": manager_id,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "action": remediation.action,
                "reason": remediation.reason,
                "instructions": remediation.instructions or next_instructions,
                "affected_manager_ids": [manager_id],
                "affected_work_item_ids": affected_ids,
            }
            contextual_event({"type": "remediation_started", **remediation_event})
            contextual_event(
                {
                    "type": "manager_replan_created",
                    **remediation_event,
                    "status": "replanning",
                    "remediation_generation": recovery_cycle + 1,
                    "summary": manager_review.tool_input.get("summary"),
                    "next_instructions": remediation_event["instructions"],
                }
            )
            for item_id in affected_ids:
                statuses[item_id] = WorkStatus.PENDING
                item_statuses[item_id] = WorkStatus.PENDING
                if remediation_event["instructions"]:
                    instruction_overrides[item_id] = remediation_event["instructions"]
            # Items blocked only by a retried dependency must become schedulable.
            for item_id, status in list(statuses.items()):
                if status == WorkStatus.BLOCKED:
                    statuses[item_id] = WorkStatus.PENDING
                    item_statuses[item_id] = WorkStatus.PENDING
            recovery_cycle += 1
            contextual_event({"type": "remediation_applied", **remediation_event})
            _complete_remediation_attempt(
                repo,
                remediation_attempt_id,
                details=remediation_event,
            )

    def run_ready_stream(stream: Workstream) -> tuple[str, bool]:
        return execute_stream(stream)

    def drain_workstreams() -> None:
        while True:
            with plan_lock:
                snapshot = plan
            if task_cancelled():
                for stream in snapshot.workstreams:
                    if stream_status.get(stream.id) in {
                        WorkStatus.PENDING,
                        WorkStatus.READY,
                    }:
                        stream_status[stream.id] = WorkStatus.CANCELLED
                    for item in stream.work_items:
                        if item_statuses.get(item.id) in {
                            WorkStatus.PENDING,
                            WorkStatus.READY,
                        }:
                            item_statuses[item.id] = WorkStatus.CANCELLED
                            item_evidence[item.id] = {
                                "accepted": False,
                                "status": "cancelled",
                                "error": "task stopped before scheduling",
                                "failure_kind": "cancelled",
                                "retryable": False,
                            }
                            contextual_event(
                                {
                                    "type": "agent_cancelled",
                                    "role": "worker",
                                    "agent_instance_id": item.metadata.get("worker_agent_id"),
                                    "manager_id": stream.manager_agent_id,
                                    "workstream_id": stream.id,
                                    "work_item_id": item.id,
                                    "status": "cancelled",
                                    "error": "Task stopped before scheduling",
                                    "failure_kind": "cancelled",
                                }
                            )
                    manager_id = stream.manager_agent_id or manager_ids.get(stream.id)
                    for role, agent_id in (
                        ("manager", manager_id),
                        ("tester", stream.metadata.get("tester_agent_id")),
                    ):
                        if agent_id:
                            contextual_event(
                                {
                                    "type": "agent_cancelled",
                                    "role": role,
                                    "agent_instance_id": agent_id,
                                    "manager_id": manager_id,
                                    "workstream_id": stream.id,
                                    "status": "cancelled",
                                    "error": "Task stopped before scheduling",
                                    "failure_kind": "cancelled",
                                }
                            )
                return
            ready_streams = scheduler.select_workstreams(
                snapshot,
                active_manager_count=0,
                statuses=stream_status,
                cancelled=task_cancelled,
            )
            if not ready_streams:
                for stream in snapshot.workstreams:
                    if stream_status.get(stream.id) == WorkStatus.PENDING:
                        stream_status[stream.id] = WorkStatus.SKIPPED
                        blocked_by = [
                            dependency
                            for dependency in stream.dependencies
                            if stream_status.get(dependency) != WorkStatus.APPROVED
                        ]
                        for item in stream.work_items:
                            if item_statuses.get(item.id) not in {
                                WorkStatus.PENDING,
                                WorkStatus.READY,
                            }:
                                continue
                            item_statuses[item.id] = WorkStatus.SKIPPED
                            item_evidence[item.id] = {
                                "accepted": False,
                                "status": "skipped",
                                "error": "skipped because an upstream workstream was abandoned",
                                "failure_kind": "dependency_skipped",
                                "retryable": False,
                                "blocked_by": blocked_by,
                            }
                            contextual_event(
                                {
                                    "type": "agent_skipped",
                                    "role": "worker",
                                    "agent_instance_id": item.metadata.get("worker_agent_id"),
                                    "manager_id": stream.manager_agent_id,
                                    "workstream_id": stream.id,
                                    "work_item_id": item.id,
                                    "status": "skipped",
                                    "reason": "upstream_workstream_abandoned",
                                    "blocked_by": blocked_by,
                                }
                            )
                            contextual_event(
                                {
                                    "type": "work_item_skipped",
                                    "role": "worker",
                                    "agent_instance_id": item.metadata.get("worker_agent_id"),
                                    "manager_id": stream.manager_agent_id,
                                    "workstream_id": stream.id,
                                    "work_item_id": item.id,
                                    "status": "skipped",
                                    "reason": "upstream_workstream_abandoned",
                                    "blocked_by": blocked_by,
                                }
                            )
                        tester_id = stream.metadata.get("tester_agent_id")
                        if tester_id:
                            contextual_event(
                                {
                                    "type": "agent_skipped",
                                    "role": "tester",
                                    "agent_instance_id": tester_id,
                                    "manager_id": stream.manager_agent_id,
                                    "workstream_id": stream.id,
                                    "status": "skipped",
                                    "reason": "upstream_workstream_abandoned",
                                    "blocked_by": blocked_by,
                                }
                            )
                        contextual_event(
                            {
                                "type": "workstream_skipped",
                                "role": "manager",
                                "agent_instance_id": (
                                    stream.manager_agent_id
                                    or manager_ids.get(stream.id)
                                    or _resolve_repository_agent_id(
                                        repo,
                                        task_id=task_id,
                                        role="manager",
                                        assignment_id=stream.id,
                                    )
                                ),
                                "workstream_id": stream.id,
                                "status": "skipped",
                                "reason": "upstream_workstream_abandoned",
                                "blocked_by": blocked_by,
                                "summary": "Skipped after upstream workstream abandonment",
                            }
                        )
                return
            batch = ready_streams
            for stream in batch:
                stream_status[stream.id] = WorkStatus.RUNNING
            with ThreadPoolExecutor(
                max_workers=len(batch),
                thread_name_prefix="workstreams",
            ) as pool:
                futures = {pool.submit(run_ready_stream, stream): stream for stream in batch}
                for future in as_completed(futures):
                    stream = futures[future]
                    try:
                        stream_id, approved = future.result()
                    except Exception as exc:
                        if isinstance(exc, llm_client.AccountPoolExhaustedError):
                            raise
                        stream_id, approved = stream.id, False
                        contextual_event(
                            {
                                "type": "workstream_failed",
                                "role": "manager",
                                "agent_instance_id": stream.manager_agent_id,
                                "workstream_id": stream.id,
                                "status": "failed",
                                "summary": f"Manager execution failed: {exc}",
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                    stream_status[stream_id] = (
                        WorkStatus.APPROVED
                        if approved
                        else WorkStatus.CANCELLED
                        if task_cancelled()
                        or any(
                            item_statuses.get(item.id) == WorkStatus.CANCELLED
                            for item in stream.work_items
                        )
                        else WorkStatus.ABANDONED
                    )

    integration_status = "not_run"
    integration_output = ""
    integration_details: dict[str, Any] = {}
    integration_required = bool(test_cmd) or any(
        item.metadata.get("contract_mode") == "typed"
        and item.contract is not None
        and test_evidence.requires_integration_test(item.contract.test_requirements)
        for stream in plan.workstreams
        for item in stream.work_items
    )
    backend_gate_approved = False
    cancelled_run = False
    integration_generation = 0

    # Integration remediation is still execution. It must finish before any
    # Manager publishes its immutable terminal report.
    while True:
        drain_workstreams()
        if task_cancelled() or any(
            status == WorkStatus.CANCELLED for status in stream_status.values()
        ):
            cancelled_run = True
            break
        any_workstream_approved = any(
            status == WorkStatus.APPROVED for status in stream_status.values()
        )
        if not any_workstream_approved:
            integration_status = "not_run"
            break
        if not integration_required:
            integration_status = "not_required"
            backend_gate_approved = True
            break

        with project_lock:
            (
                integration_status,
                integration_output,
                integration_details,
            ) = safety.run_sandbox_tests_detailed(
                root,
                test_cmd,
                cancelled=task_cancelled,
            )
        if task_cancelled():
            cancelled_run = True
            break
        sandbox_outcome = str(integration_details.get("sandbox_outcome") or "")
        integration_retryable = (
            integration_status == "failed"
            and sandbox_outcome not in {"blocked", "unavailable"}
        )
        backend_gate_approved = test_evidence.integration_test_passed(integration_status)
        for stream in plan.workstreams:
            for item in stream.work_items:
                if (
                    item.metadata.get("contract_mode") != "typed"
                    or item.contract is None
                    or not test_evidence.requires_integration_test(
                        item.contract.test_requirements
                    )
                ):
                    continue
                evidence = item_evidence.setdefault(item.id, {})
                evidence["integration_status"] = integration_status
                evidence["test_status"] = integration_status
                evidence["test_scope"] = "integration"
                evidence["test_output"] = integration_output[-12000:]
                persisted_item_attempts = repo.list_attempts(task_id, item.id)
                if persisted_item_attempts:
                    latest_attempt = max(
                        persisted_item_attempts,
                        key=lambda value: value.number,
                    )
                    repo.save_attempt(
                        replace(
                            latest_attempt,
                            evidence={
                                **dict(latest_attempt.evidence),
                                "integration_status": integration_status,
                                "test_status": integration_status,
                                "test_scope": "integration",
                                "test_output": integration_output[-12000:],
                            },
                        )
                    )
        contextual_event(
            {
                "type": "integration_result",
                "role": "director",
                "agent_instance_id": director_id,
                "status": (
                    "blocked"
                    if sandbox_outcome in {"blocked", "unavailable"}
                    else integration_status
                ),
                "accepted": backend_gate_approved,
                "retryable": integration_retryable,
                "failure_kind": (
                    "sandbox_unavailable"
                    if sandbox_outcome == "unavailable"
                    else "sandbox_blocked"
                    if sandbox_outcome == "blocked"
                    else ""
                ),
                "command": " ".join(test_cmd or []),
                "detail": integration_output[-12000:],
                **integration_details,
            }
        )
        if backend_gate_approved:
            break

        normalized_output = integration_output.replace("\\", "/").casefold()
        affected_stream_ids = [
            stream.id
            for stream in plan.workstreams
            if any(
                str(scope).replace("\\", "/").casefold() in normalized_output
                for scope in stream.write_scopes
                if str(scope).strip()
            )
        ] or [stream.id for stream in plan.workstreams]
        affected_item_ids = [
            item.id
            for stream in plan.workstreams
            if stream.id in affected_stream_ids
            for item in stream.work_items
        ]
        crisis_id = new_id("crisis")
        crisis_context = {
            "crisis_id": crisis_id,
            "scope": "integration",
            "task_id": task_id,
            "session_id": session_id,
            "execution_epoch": execution_epoch,
            "failure_kind": (
                "sandbox_unavailable"
                if sandbox_outcome == "unavailable"
                else "sandbox_blocked"
                if sandbox_outcome == "blocked"
                else "integration_failed"
            ),
            "reason": integration_output[-2000:] or "Integration gate failed",
            "retryable": integration_retryable,
            "affected_manager_ids": [
                manager_ids[stream.id]
                for stream in plan.workstreams
                if stream.id in affected_stream_ids
            ],
            "affected_work_item_ids": affected_item_ids,
            "errors": {"integration": integration_output[-12000:]},
            "suggested_instructions": integration_output[-4000:],
        }
        contextual_event({"type": "crisis_detected", **crisis_context})
        remediation = _decide_remediation(crisis_strategy, crisis_context)
        remediation, remediation_attempt_id = _reserve_remediation_attempt(
            repo,
            context=crisis_context,
            decision=remediation,
        )
        if not remediation.retries:
            contextual_event(
                {
                    "type": "remediation_exhausted",
                    **crisis_context,
                    "reason": remediation.reason,
                }
            )
            for stream in plan.workstreams:
                if stream.id not in affected_stream_ids:
                    continue
                stream_status[stream.id] = WorkStatus.ABANDONED
                for item in stream.work_items:
                    if item.id not in affected_item_ids:
                        continue
                    item_statuses[item.id] = WorkStatus.ABANDONED
                    item_evidence[item.id] = {
                        **dict(item_evidence.get(item.id) or {}),
                        "accepted": False,
                        "status": "abandoned",
                        "failure_kind": "integration_failed",
                        "retryable": False,
                        "error": remediation.reason,
                    }
                    contextual_event(
                        {
                            "type": "agent_abandoned",
                            "role": "worker",
                            "agent_instance_id": item.metadata.get("worker_agent_id"),
                            "manager_id": stream.manager_agent_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "status": "abandoned",
                            "reason": remediation.reason,
                            "failure_kind": "integration_failed",
                            "artifacts": sorted(
                                completed_package_files.get(item.id, set())
                            ),
                            "log_refs": [],
                        }
                    )
                    contextual_event(
                        {
                            "type": "work_item_abandoned",
                            "role": "worker",
                            "agent_instance_id": item.metadata.get("worker_agent_id"),
                            "manager_id": stream.manager_agent_id,
                            "workstream_id": stream.id,
                            "work_item_id": item.id,
                            "status": "abandoned",
                            "reason": remediation.reason,
                            "failure_kind": "integration_failed",
                            "artifacts": sorted(
                                completed_package_files.get(item.id, set())
                            ),
                            "log_refs": [],
                        }
                    )
            break

        remediation_event = {
            "crisis_id": crisis_id,
            "scope": "integration",
            "role": "director",
            "agent_instance_id": director_id,
            "action": remediation.action,
            "reason": remediation.reason,
            "instructions": remediation.instructions or integration_output[-4000:],
            "affected_manager_ids": crisis_context["affected_manager_ids"],
            "affected_work_item_ids": affected_item_ids,
        }
        contextual_event({"type": "remediation_started", **remediation_event})
        contextual_event(
            {
                "type": "director_replan_created",
                **remediation_event,
                "status": "replanning",
                "remediation_generation": integration_generation + 1,
                "workstream_ids": affected_stream_ids,
                "summary": remediation.reason,
            }
        )
        for stream in plan.workstreams:
            if stream.id not in affected_stream_ids:
                continue
            stream_status[stream.id] = WorkStatus.PENDING
            for item in stream.work_items:
                item_statuses[item.id] = WorkStatus.PENDING
                completed_package_files.setdefault(item.id, set()).clear()
                if remediation_event["instructions"]:
                    instruction_overrides[item.id] = remediation_event["instructions"]
        integration_generation += 1
        contextual_event({"type": "remediation_applied", **remediation_event})
        _complete_remediation_attempt(
            repo,
            remediation_attempt_id,
            details=remediation_event,
        )

    # Convert every non-cancellation legacy/transient state into the explicit
    # abandonment model before reports are frozen.
    if not cancelled_run:
        for stream in plan.workstreams:
            for item in stream.work_items:
                current = item_statuses.get(item.id, WorkStatus.PENDING)
                if current in {
                    WorkStatus.APPROVED,
                    WorkStatus.ABANDONED,
                    WorkStatus.SKIPPED,
                }:
                    continue
                worker_id = str(item.metadata.get("worker_agent_id") or "")
                attempted = bool(repo.list_attempts(task_id, item.id)) or worker_id in {
                    *started_agent_ids,
                    *called_agent_ids,
                }
                terminal_status = (
                    WorkStatus.ABANDONED if attempted else WorkStatus.SKIPPED
                )
                item_statuses[item.id] = terminal_status
                prior = dict(item_evidence.get(item.id) or {})
                reason = str(
                    prior.get("error")
                    or prior.get("failure_kind")
                    or "work did not reach approval"
                )
                item_evidence[item.id] = {
                    **prior,
                    "accepted": False,
                    "status": terminal_status.value,
                    "failure_kind": (
                        "agent_abandoned"
                        if terminal_status == WorkStatus.ABANDONED
                        else "dependency_skipped"
                    ),
                    "retryable": False,
                    "error": reason,
                }
                contextual_event(
                    {
                        "type": (
                            "agent_abandoned"
                            if terminal_status == WorkStatus.ABANDONED
                            else "agent_skipped"
                        ),
                        "role": "worker",
                        "agent_instance_id": worker_id,
                        "manager_id": stream.manager_agent_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "status": terminal_status.value,
                        "reason": reason,
                        "failure_kind": item_evidence[item.id]["failure_kind"],
                        "artifacts": sorted(
                            completed_package_files.get(item.id, set())
                        ),
                        "log_refs": [],
                    }
                )
                contextual_event(
                    {
                        "type": (
                            "work_item_abandoned"
                            if terminal_status == WorkStatus.ABANDONED
                            else "work_item_skipped"
                        ),
                        "role": "worker",
                        "agent_instance_id": worker_id,
                        "manager_id": stream.manager_agent_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "status": terminal_status.value,
                        "reason": reason,
                        "failure_kind": item_evidence[item.id]["failure_kind"],
                        "artifacts": sorted(
                            completed_package_files.get(item.id, set())
                        ),
                        "log_refs": [],
                        "blocked_by": list(item.dependencies),
                    }
                )
            statuses_for_stream = [
                item_statuses.get(item.id, WorkStatus.SKIPPED)
                for item in stream.work_items
            ]
            if statuses_for_stream and all(
                status == WorkStatus.APPROVED for status in statuses_for_stream
            ):
                stream_status[stream.id] = WorkStatus.APPROVED
            elif any(
                status == WorkStatus.ABANDONED for status in statuses_for_stream
            ) or stream_status.get(stream.id) in {
                WorkStatus.RUNNING,
                WorkStatus.FAILED,
                WorkStatus.BLOCKED,
                WorkStatus.ABANDONED,
            }:
                stream_status[stream.id] = WorkStatus.ABANDONED
            else:
                stream_status[stream.id] = WorkStatus.SKIPPED

    terminal_streams = tuple(
        replace(
            stream,
            status=stream_status[stream.id],
            work_items=tuple(
                replace(item, status=item_statuses.get(item.id, WorkStatus.SKIPPED))
                for item in stream.work_items
            ),
        )
        for stream in plan.workstreams
    )

    # N Managers must produce N reports, even when a report is synthesized for
    # an exit that occurred before workstream execution.
    for stream in terminal_streams:
        manager_id = str(stream.manager_agent_id or manager_ids[stream.id])
        settle_manager(
            stream,
            statuses={
                item.id: item_statuses.get(item.id, WorkStatus.SKIPPED)
                for item in stream.work_items
            },
            evidence=item_evidence,
            synthesized=manager_id not in started_agent_ids,
            reasons=(
                str(stream.metadata.get("plan_error") or ""),
                "task_cancelled" if cancelled_run else "",
            ),
        )
    barrier = emit_manager_report_barrier()

    director_verdict = "revise"
    director_summary = "Task stopped before Director final review."
    director_review: ToolCallResult | None = None
    if not cancelled_run and not task_cancelled():
        review_context = {
            "manager_report_barrier": barrier,
            "integration_status": integration_status,
            "manager_report_count": len(manager_terminal_reports),
        }
        if not _reserve_director_final_review(
            repo,
            task_id=task_id,
            execution_epoch=execution_epoch,
            review_context=review_context,
        ):
            raise RuntimeError(
                "Director final review was already reserved for this execution epoch"
            )
        active_director_model, active_director_effort = _resolve_agent_config(
            agent_config_resolver,
            agent_id=director_id,
            role="director",
            model=director_model,
            effort=director_effort,
        )
        _configure_role_thread(
            role="director",
            model=active_director_model,
            effort=active_director_effort,
            worker_model=worker_model,
            worker_effort=worker_effort,
            reviewer_model=reviewer_model,
            reviewer_effort=reviewer_effort,
            event_sink=contextual_event,
            account_mode=account_mode,
        )
        llm_client.thread_local.agent_instance_id = director_id
        final_prompt = (
            f"## USER GOAL\n{task_description}\n\n"
            "## IMMUTABLE MANAGER TERMINAL REPORTS\n"
            + json.dumps(
                list(manager_terminal_reports.values()),
                ensure_ascii=False,
                indent=2,
            )[-30000:]
            + "\n\n## REPORT BARRIER\n"
            + json.dumps(barrier, ensure_ascii=False, indent=2)
            + f"\n\n## INTEGRATION GATE\n{integration_status}\n"
            + integration_output[-12000:]
            + "\n\nThis is the one and only final review. Call complete_plan exactly once."
        )
        director_review = llm_call(
            DIRECTOR_REVIEW_PROMPT,
            final_prompt,
            DIRECTOR_REVIEW_TOOLS,
        )
        director_final_review_count += 1
        director_verdict = str(director_review.tool_input.get("verdict", "revise"))
        director_summary = str(director_review.tool_input.get("summary", ""))
        final_review_event = {
            "type": "director_final_review",
            "task_id": task_id,
            "session_id": session_id,
            "role": "director",
            "agent_instance_id": director_id,
            "execution_epoch": execution_epoch,
            "execution_epoch_id": execution_epoch,
            "verdict": director_verdict,
            "summary": director_summary,
            "remaining_risks": list(
                director_review.tool_input.get("remaining_risks") or []
            ),
            "integration_status": integration_status,
            "manager_reports_expected": len(expected_manager_ids),
            "manager_reports_reported": len(manager_terminal_reports),
            "final_review_number": director_final_review_count,
            "status": "completed",
        }
        _persist_director_final_review(repo, final_review_event)
        contextual_event(final_review_event)
    elif task_cancelled():
        cancelled_run = True

    all_work_completed = all(
        stream.status == WorkStatus.APPROVED
        and all(item.status == WorkStatus.APPROVED for item in stream.work_items)
        for stream in terminal_streams
    )
    all_approved = bool(
        all_work_completed
        and backend_gate_approved
        and director_review is not None
        and director_review.tool_name == "complete_plan"
        and director_verdict == "approved"
    )
    has_partial_work = any(
        stream.status in {WorkStatus.ABANDONED, WorkStatus.SKIPPED}
        or any(
            item.status in {WorkStatus.ABANDONED, WorkStatus.SKIPPED}
            for item in stream.work_items
        )
        for stream in terminal_streams
    )
    final_status = (
        PlanStatus.CANCELLED
        if cancelled_run
        else PlanStatus.COMPLETED
        if all_approved
        else PlanStatus.PARTIAL
        if has_partial_work
        else PlanStatus.FAILED
    )
    running_revision = plan.revision
    final_plan = replace(
        plan,
        revision=running_revision + 1,
        status=final_status,
        workstreams=terminal_streams,
        updated_at=utc_now(),
    )
    with event_state_lock:
        reconciliation = reconcile_completion(
            final_plan,
            item_evidence=item_evidence,
            call_outcomes=model_call_outcomes,
            director_agent_id=director_id,
            called_agent_ids=called_agent_ids,
            started_agent_ids=started_agent_ids,
            agent_dispositions=agent_dispositions,
            agent_no_call_reasons=agent_no_call_reasons,
            agent_call_purposes=agent_call_purposes,
            expected_manager_ids=expected_manager_ids,
            manager_terminal_reports=manager_terminal_reports.values(),
            director_final_review_count=director_final_review_count,
        )
    if not reconciliation["covered"] and not cancelled_run:
        all_approved = False
        final_plan = replace(final_plan, status=PlanStatus.FAILED)
        director_verdict = "revise"
        director_summary = "Completion coverage failed: " + "; ".join(
            reconciliation["errors"]
        )

    approved_file_paths = sorted(
        {
            path
            for stream in final_plan.workstreams
            for item in stream.work_items
            if item.status == WorkStatus.APPROVED
            for path in (
                completed_package_files.get(item.id)
                or set(item.write_scopes)
            )
        }
    )
    if final_plan.status == PlanStatus.COMPLETED and on_file_approved is not None:
        for approved_path in approved_file_paths:
            on_file_approved(approved_path)
    repo.save_plan(final_plan, expected_previous_revision=running_revision)
    _complete_execution_epoch(
        repo,
        task_id=task_id,
        execution_epoch=execution_epoch,
        disposition=final_plan.status.value,
        terminal_log_refs=(
            ref
            for report in manager_terminal_reports.values()
            for ref in report.get("log_refs", ())
        ),
    )

    contextual_event(
        {
            "type": "completion_reconciliation",
            "role": "director",
            "agent_instance_id": director_id,
            "execution_epoch": execution_epoch,
            "status": "covered" if reconciliation["covered"] else "failed",
            **reconciliation,
        }
    )
    if final_plan.status == PlanStatus.PARTIAL:
        contextual_event(
            {
                "type": "hierarchy_partial",
                "role": "director",
                "agent_instance_id": director_id,
                "execution_epoch": execution_epoch,
                "status": "partial",
                "verdict": director_verdict,
                "summary": director_summary,
                "completed_workstream_ids": sorted(
                    stream.id
                    for stream in final_plan.workstreams
                    if stream.status == WorkStatus.APPROVED
                ),
                "abandoned_workstream_ids": sorted(
                    stream.id
                    for stream in final_plan.workstreams
                    if stream.status == WorkStatus.ABANDONED
                ),
                "skipped_workstream_ids": sorted(
                    stream.id
                    for stream in final_plan.workstreams
                    if stream.status == WorkStatus.SKIPPED
                ),
            }
        )
    else:
        contextual_event(
            {
                "type": (
                    "hierarchy_completed"
                    if final_plan.status == PlanStatus.COMPLETED
                    else "hierarchy_cancelled"
                    if final_plan.status == PlanStatus.CANCELLED
                    else "hierarchy_failed"
                ),
                "role": "director",
                "agent_instance_id": director_id,
                "execution_epoch": execution_epoch,
                "status": final_plan.status.value,
                "verdict": director_verdict,
                "summary": director_summary,
            }
        )
    for stream in final_plan.workstreams:
        for item in stream.work_items:
            if item.id not in item_evidence:
                continue
            evidence = item_evidence.get(item.id, {})
            result.turns.append(
                TurnOutcome(
                    tool_name="hierarchy_work_item",
                    accepted=bool(evidence.get("accepted")),
                    detail=str(
                        evidence.get("reviewer_feedback")
                        or evidence.get("error")
                        or item.title
                    )[:1000],
                    stop_loop=False,
                )
            )
    result.stopped_reason = (
        "task_completed"
        if final_plan.status == PlanStatus.COMPLETED
        else "task_partial"
        if final_plan.status == PlanStatus.PARTIAL
        else "cancelled"
        if final_plan.status == PlanStatus.CANCELLED
        else "hierarchy_failed"
    )
    result.final_state = {
        "session_id": session_id,
        "execution_epoch": execution_epoch,
        "plan_revision": final_plan.revision,
        "plan_status": final_plan.status.value,
        "turn_count": len(result.turns),
        "completed_tickets": [
            {
                "workstream_id": stream.id,
                "work_item_id": item.id,
                "file_path": item.metadata.get("file_path"),
                "status": item.status.value,
            }
            for stream in final_plan.workstreams
            for item in stream.work_items
        ],
        "last_execution_result": (
            f"Hierarchy {director_verdict}; integration={integration_status}"
        ),
        "last_review_verdict": director_verdict,
        "last_reviewer_feedback": director_summary,
        "approved_file_paths": approved_file_paths,
        "manager_terminal_reports": list(manager_terminal_reports.values()),
        "manager_report_barrier": barrier,
        "director_final_review_count": director_final_review_count,
        "reconciliation": reconciliation,
    }
    llm_client.thread_local.event_sink = None
    llm_client.thread_local.agent_instance_id = None
    release_known_agent_accounts()
    return result


def run_hierarchy(
    *,
    root: Path,
    task_description: str,
    task_id: str,
    source_files: list[str] | None = None,
    test_cmd: list[str] | None = None,
    allow_new_files: bool = False,
    limits: SchedulerLimits | None = None,
    director_model: str = "claude-sonnet-5",
    director_effort: str = "max",
    manager_model: str = "claude-sonnet-5",
    manager_effort: str = "max",
    worker_model: str = "claude-sonnet-5",
    worker_effort: str = "max",
    reviewer_model: str = "claude-sonnet-5",
    reviewer_effort: str = "high",
    llm_call: LLMCallFn = _default_llm_call,
    on_event: EventSink | None = None,
    on_file_approved: ApprovedFileSink | None = None,
    repository: StateRepository | None = None,
    project_lease: Any | None = None,
    resume_session: bool = False,
    agent_config_resolver: AgentConfigResolver | None = None,
    approval_callback: ApprovalCallback | None = None,
    cancelled: CancellationCallback | None = None,
    crisis_strategy: Any | None = None,
) -> SessionResult:
    """Run the hierarchy and preserve account-pool exhaustion as a fatal exception."""
    repo = repository or StateRepository()
    observed_reports: dict[str, dict[str, Any]] = {}
    observed_agents: set[str] = set()
    epoch_event: dict[str, Any] | None = None
    barrier_observed = False

    def observed_event(event: dict[str, Any]) -> None:
        nonlocal epoch_event, barrier_observed
        event_type = str(event.get("type") or "")
        agent_id = str(event.get("agent_instance_id") or "")
        if agent_id:
            observed_agents.add(agent_id)
        if event_type == "execution_epoch_started":
            epoch_event = dict(event)
        elif event_type == "manager_terminal_report":
            manager_id = str(event.get("manager_id") or agent_id)
            if manager_id:
                observed_reports.setdefault(manager_id, dict(event))
        elif event_type == "manager_report_barrier":
            barrier_observed = True
        _emit(on_event, event)

    try:
        return _run_hierarchy_impl(
            root=root,
            task_description=task_description,
            task_id=task_id,
            source_files=source_files,
            test_cmd=test_cmd,
            allow_new_files=allow_new_files,
            limits=limits,
            director_model=director_model,
            director_effort=director_effort,
            manager_model=manager_model,
            manager_effort=manager_effort,
            worker_model=worker_model,
            worker_effort=worker_effort,
            reviewer_model=reviewer_model,
            reviewer_effort=reviewer_effort,
            llm_call=llm_call,
            on_event=observed_event,
            on_file_approved=on_file_approved,
            repository=repo,
            project_lease=project_lease,
            resume_session=resume_session,
            agent_config_resolver=agent_config_resolver,
            approval_callback=approval_callback,
            cancelled=cancelled,
            crisis_strategy=crisis_strategy,
        )
    except llm_client.AccountPoolExhaustedError as exc:
        plan = repo.get_plan(task_id)
        if plan is not None:
            attempted_ids = {
                attempt.work_item_id
                for attempt in repo.list_attempts(task_id)
            }
            fatal_streams: list[Workstream] = []
            fatal_evidence: dict[str, dict[str, Any]] = {}
            for stream in plan.workstreams:
                fatal_items: list[WorkItem] = []
                for item in stream.work_items:
                    status = (
                        WorkStatus.ABANDONED
                        if item.id in attempted_ids
                        else WorkStatus.SKIPPED
                    )
                    fatal_items.append(replace(item, status=status))
                    attempts = repo.list_attempts(task_id, item.id)
                    latest = (
                        max(attempts, key=lambda value: value.number)
                        if attempts
                        else None
                    )
                    fatal_evidence[item.id] = {
                        **dict(latest.evidence if latest is not None else {}),
                        "accepted": False,
                        "status": status.value,
                        "failure_kind": "account_pool_exhausted",
                        "retryable": False,
                        "error": str(exc),
                    }
                manager_id = str(
                    stream.manager_agent_id
                    or _resolve_repository_agent_id(
                        repo,
                        task_id=task_id,
                        role="manager",
                        assignment_id=stream.id,
                    )
                )
                fatal_streams.append(
                    replace(
                        stream,
                        manager_agent_id=manager_id,
                        status=(
                            WorkStatus.ABANDONED
                            if manager_id in observed_agents
                            else WorkStatus.SKIPPED
                        ),
                        work_items=tuple(fatal_items),
                    )
                )
            fatal_plan = replace(
                plan,
                revision=plan.revision + 1,
                status=PlanStatus.FAILED,
                workstreams=tuple(fatal_streams),
                metadata={
                    **dict(plan.metadata),
                    "failure_kind": "account_pool_exhausted",
                },
                updated_at=utc_now(),
            )
            repo.save_plan(fatal_plan, expected_previous_revision=plan.revision)
            expected_manager_ids = tuple(
                str(stream.manager_agent_id) for stream in fatal_plan.workstreams
            )
            execution_epoch = str((epoch_event or {}).get("execution_epoch") or "")
            if not execution_epoch:
                epoch_seed = "\0".join(
                    (task_id, fatal_plan.session_id, str(plan.revision))
                )
                execution_epoch = (
                    "epoch_"
                    + hashlib.sha256(epoch_seed.encode("utf-8")).hexdigest()[:24]
                )
            for stream in fatal_plan.workstreams:
                manager_id = str(stream.manager_agent_id)
                if manager_id in observed_reports:
                    continue
                report = _build_manager_terminal_report(
                    repository=repo,
                    task_id=task_id,
                    session_id=fatal_plan.session_id,
                    execution_epoch=execution_epoch,
                    stream=stream,
                    manager_id=manager_id,
                    item_statuses={
                        item.id: item.status for item in stream.work_items
                    },
                    item_evidence=fatal_evidence,
                    synthesized=True,
                    reasons=("account_pool_exhausted", str(exc)),
                )
                _persist_manager_terminal_report(repo, report)
                observed_event(report)
            if expected_manager_ids and not barrier_observed:
                barrier = {
                    "type": "manager_report_barrier",
                    "task_id": task_id,
                    "session_id": fatal_plan.session_id,
                    "execution_epoch": execution_epoch,
                    "role": "director",
                    "expected_manager_ids": sorted(expected_manager_ids),
                    "reported_manager_ids": sorted(observed_reports),
                    "expected_count": len(expected_manager_ids),
                    "reported_count": len(observed_reports),
                    "satisfied": set(expected_manager_ids) == set(observed_reports),
                    "status": "satisfied",
                }
                _persist_manager_report_barrier(repo, barrier)
                observed_event(barrier)
            _complete_execution_epoch(
                repo,
                task_id=task_id,
                execution_epoch=execution_epoch,
                disposition="account_pool_exhausted",
                terminal_log_refs=(
                    ref
                    for report in observed_reports.values()
                    for ref in report.get("log_refs", ())
                ),
            )
            reconciliation = reconcile_completion(
                fatal_plan,
                item_evidence=fatal_evidence,
                expected_manager_ids=expected_manager_ids,
                manager_terminal_reports=observed_reports.values(),
                director_final_review_count=0,
            )
            observed_event(
                {
                    "type": "completion_reconciliation",
                    "task_id": task_id,
                    "session_id": fatal_plan.session_id,
                    "execution_epoch": execution_epoch,
                    "role": "director",
                    "status": "covered" if reconciliation["covered"] else "failed",
                    **reconciliation,
                }
            )
            observed_event(
                {
                    "type": "hierarchy_failed",
                    "task_id": task_id,
                    "session_id": fatal_plan.session_id,
                    "execution_epoch": execution_epoch,
                    "role": "director",
                    "status": "failed",
                    "failure_kind": "account_pool_exhausted",
                    "verdict": "revise",
                    "summary": str(exc),
                }
            )
        for agent_id in observed_agents:
            llm_client.release_agent_account(agent_id)
        llm_client.thread_local.event_sink = None
        llm_client.thread_local.agent_instance_id = None
        raise
    finally:
        llm_client.release_task_account_cohort(task_id)
