"""Director -> Managers -> Workers -> Testers hierarchical runtime."""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import re
import threading
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from . import (
    config,
    context_builder,
    file_agent,
    llm_client,
    path_utils,
    safety,
    state_store,
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


def _package_files(item: WorkItem) -> list[str]:
    """Resolve ordered file list for a major work package."""
    primary = str(item.metadata.get("file_path") or "").replace("\\", "/").strip()
    scopes = [
        str(scope).replace("\\", "/").strip() for scope in item.write_scopes if str(scope).strip()
    ]
    files: list[str] = []
    if primary:
        files.append(primary)
    for scope in scopes:
        # Skip directory wildcards — workers patch concrete files.
        if scope.endswith("/") or scope.endswith("/**") or scope.endswith("/*"):
            continue
        if scope not in files:
            files.append(scope)
    if not files:
        raise RuntimeError(f"Work item {item.id} thiếu file trong write_scopes/file_path")
    if len(files) > config.MAX_FILES_PER_WORK_PACKAGE:
        raise RuntimeError(
            f"Work item {item.id} declares {len(files)} concrete files; "
            f"limit is {config.MAX_FILES_PER_WORK_PACKAGE}"
        )
    return files


def _normalize_work_item_scopes(value: dict[str, Any]) -> tuple[str, tuple[str, ...]]:
    primary = str(value["file_path"]).replace("\\", "/").strip()
    scopes: list[str] = []
    for raw_scope in value.get("write_scopes") or []:
        scope = str(raw_scope).replace("\\", "/").strip()
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
    normalized = value.strip().replace("\\", "/")
    return "/" in normalized or normalized.startswith(".") or bool(Path(normalized).suffix)


_PATH_ANNOTATION_RE = re.compile(r"\s*\([^()]*\)\s*$")


def _strip_path_annotation(value: str) -> str:
    """Drop a planner's trailing note from a path.

    Models routinely answer with ``data/config.json (created at runtime)`` in a
    field the contract treats as a literal path, which then fails scope checks
    against a file nobody ever asked for. The note is only removed when what is
    left is a bare path token, so prose entries such as an acceptance criterion
    ending in parentheses survive untouched.
    """
    text = str(value).strip()
    while True:
        candidate = _PATH_ANNOTATION_RE.sub("", text).strip()
        if candidate == text:
            return text
        if not candidate or any(character.isspace() for character in candidate):
            return text
        text = candidate


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
        test_status = str(evidence.get("test_status") or "").casefold()
        syntax_status = str(evidence.get("syntax_status") or "").casefold()
        syntax_only = all(
            any(token in requirement.casefold() for token in ("syntax", "parse", "compile"))
            for requirement in contract.test_requirements
        )
        if test_status != "passed" and not (syntax_only and syntax_status == "passed"):
            issues.append("declared test requirements lack passing evidence")
    return issues


def _preflight_plan(
    *,
    root: Path,
    plan: TaskPlan,
    limits: SchedulerLimits,
    allow_new_files: bool,
    available_artifacts: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Validate the complete planned DAG before any Worker is announced."""
    issues: list[dict[str, Any]] = []

    def add_issue(
        code: str,
        message: str,
        *,
        workstream_id: str | None = None,
        work_item_ids: tuple[str, ...] = (),
        paths: tuple[str, ...] = (),
    ) -> None:
        issues.append(
            {
                "code": code,
                "message": message,
                "workstream_id": workstream_id,
                "work_item_ids": list(work_item_ids),
                "paths": list(paths),
            }
        )

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

        if parent_contract is not None:
            parent_scopes = (
                *parent_contract.read_scopes,
                *parent_contract.write_scopes,
            )
            for scope in contract.read_scopes:
                if not any(_scope_covers(parent, scope) for parent in parent_scopes):
                    add_issue(
                        "contract_read_scope_mismatch",
                        f"Read scope {scope!r} is outside the workstream contract",
                        workstream_id=workstream_id,
                        work_item_ids=item_ids,
                        paths=(scope,),
                    )

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
                item_paths = tuple(_package_files(item))
            except (RuntimeError, ValueError) as exc:
                add_issue(
                    "missing_concrete_target",
                    str(exc),
                    workstream_id=stream.id,
                    work_item_ids=(item.id,),
                )
                continue
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
                if (
                    "/" not in raw_output
                    and not Path(raw_output).suffix
                    and raw_output not in item_paths
                ):
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
                if (
                    target in {"", "."}
                    or target.endswith("/")
                    or target.endswith("/**")
                    or target.endswith("/*")
                ):
                    add_issue(
                        "missing_concrete_target",
                        f"Work item {item.id!r} target is not a concrete file: {target!r}",
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
                    path_utils.ensure_context_path_safe(target)
                    normalized, absolute = path_utils.resolve_under_root(root, target)
                    if normalized != target:
                        raise path_utils.PathEscapeError(f"Target is not normalized: {target!r}")
                    if absolute.exists() and not absolute.is_file():
                        raise IsADirectoryError(f"Target is not a regular file: {normalized}")
                    if not absolute.exists() and not allow_new_files:
                        raise FileNotFoundError(
                            f"New file is not allowed in edit mode: {normalized}"
                        )
                except (
                    FileNotFoundError,
                    IsADirectoryError,
                    path_utils.PathEscapeError,
                    path_utils.SensitivePathError,
                ) as exc:
                    add_issue(
                        "unsafe_target"
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


def _runtime_missing_contract_inputs(
    root: Path,
    contract: WorkContract,
) -> tuple[str, ...]:
    """Check path-like inputs only when their dependency gate has opened."""
    missing: list[str] = []
    for artifact in contract.input_artifacts:
        if not _looks_like_artifact_path(artifact):
            continue
        try:
            _, absolute = path_utils.resolve_under_root(root, artifact)
        except (ValueError, path_utils.PathEscapeError):
            missing.append(artifact)
            continue
        if not absolute.is_file():
            missing.append(artifact)
    return tuple(missing)


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
        input_artifacts=_clean_path_tuple(value.get("input_artifacts") or fallback_inputs),
        expected_outputs=outputs,
        read_scopes=_clean_path_tuple(value.get("read_scopes") or ()),
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
    max_attempts_per_item: int = 2,
    resume_session: bool = False,
    agent_config_resolver: AgentConfigResolver | None = None,
    approval_callback: ApprovalCallback | None = None,
    cancelled: CancellationCallback | None = None,
) -> SessionResult:
    """Execute a durable, bounded two-level plan.

    Every Manager selected by the Director is planned eagerly. Planning caps
    are upper bounds, while execution respects separate dependency-aware
    parallel slots.
    Worker model calls may run concurrently, while patch/Git/tests/review use
    one project lock to preserve the local working tree.
    """
    root = root.resolve()
    if max_attempts_per_item < 1:
        raise ValueError("max_attempts_per_item must be positive")
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
    model_call_outcomes: dict[str, str] = {}
    called_agent_ids: set[str] = set()
    agent_call_purposes: dict[str, set[str]] = {}
    agent_dispositions: dict[str, str] = {}
    event_state_lock = threading.RLock()

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
            elif event_type in {"agent_cancelled", "agent_skipped"}:
                disposition = "skipped"
            elif event_type == "workstream_failed":
                disposition = "skipped" if event.get("status") == "cancelled" else "failed"
            if disposition is not None:
                with event_state_lock:
                    agent_dispositions[agent_id] = disposition
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
        if agent_id:
            with event_state_lock:
                called_agent_ids.add(agent_id)
                agent_call_purposes.setdefault(agent_id, set()).add(purpose)
        previous_purpose = getattr(llm_client.thread_local, "call_purpose", None)
        llm_client.thread_local.call_purpose = purpose
        try:
            return raw_llm_call(system_prompt, user_message, tools)
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
    for stream in plan.workstreams:
        contextual_event(
            {
                "type": "agent_started",
                "role": "manager",
                "agent_instance_id": manager_ids[stream.id],
                "workstream_id": stream.id,
                "workstream_title": stream.title,
                "status": "queued",
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
                "type": "agent_progress",
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
        for planner_attempt in range(1, config.PLANNER_COUNT_RETRIES + 2):
            planner_prompt = prompt
            if planner_attempt > 1:
                planner_prompt += (
                    "\n\n## REQUIRED CORRECTION\nChoose between 1 and "
                    f"{coder_cap} coder work items. requested_worker_count must "
                    "equal work_items.length and must not exceed the cap."
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
            except RuntimeError as exc:
                error = str(exc)
            if error is None:
                manager_result = candidate
                break
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
        if manager_result is None:
            raise RuntimeError(f"Manager did not return a valid plan within maximum {coder_cap}")
        planned = _manager_stream_from_result(
            manager_result,
            stream,
            bounds,
            max_coder_count=coder_cap,
        )
        if prior_stream is not None:
            prior_by_id = {item.id.split(":", 1)[-1]: item for item in prior_stream.work_items}
            for item in planned.work_items:
                short_id = item.id.split(":", 1)[-1]
                prior_item = prior_by_id.get(short_id)
                if prior_item is None:
                    continue
                was_approved = prior_item.id in previous_item_evidence
                if was_approved and item.contract != prior_item.contract:
                    raise RuntimeError(f"Approved item {item.id!r} changed its Work Contract")
                if (
                    item.contract != prior_item.contract
                    and item.contract.id == prior_item.contract.id
                    and item.contract.version <= prior_item.contract.version
                ):
                    raise RuntimeError(
                        f"Changed Work Contract {item.contract.id!r} must "
                        "increment contract_version"
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

    # Eager-plan every Manager selected by the Director. Execution still
    # follows the workstream DAG and separate parallel slots.
    planned_revision = plan.revision
    with ThreadPoolExecutor(
        max_workers=max(1, min(len(plan.workstreams), bounds.max_parallel_managers)),
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
    _persist_plan_contract_versions(repo, plan)
    # Resolve and announce every planned logical child before preflight.  A
    # preflight failure may correctly prevent model transport, but it must not
    # make planned Workers/Testers disappear from the durable timeline or UI.
    announced_streams: list[Workstream] = []
    for stream in plan.workstreams:
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
        stamped_items: list[WorkItem] = []
        for item in stream.work_items:
            worker_id = _resolve_repository_agent_id(
                repo,
                task_id=task_id,
                role="worker",
                assignment_id=item.id,
            )
            stamped_items.append(
                replace(
                    item,
                    metadata={
                        **dict(item.metadata),
                        "worker_agent_id": worker_id,
                    },
                )
            )
            contextual_event(
                {
                    "type": "agent_started",
                    "role": "worker",
                    "agent_instance_id": worker_id,
                    "manager_id": manager_id,
                    "workstream_id": stream.id,
                    "work_item_id": item.id,
                    "status": "queued",
                    "goal": item.goal,
                    "title": item.title,
                    "contract": to_dict(item.contract),
                    "model": worker_model,
                    "effort": worker_effort,
                }
            )
            if item.id in previous_item_evidence:
                contextual_event(
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
            signal(
                manager_id,
                worker_id,
                "delegate_work_item",
                f"Manager giao work item: {item.title}",
                workstream_id=stream.id,
                work_item_id=item.id,
                contract=item.contract,
                artifacts=item.contract.input_artifacts,
            )
        tester_id = _resolve_repository_agent_id(
            repo,
            task_id=task_id,
            role="tester",
            assignment_id=stream.id,
        )
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
        contextual_event(
            {
                "type": "agent_started",
                "role": "tester",
                "agent_instance_id": tester_id,
                "manager_id": manager_id,
                "workstream_id": stream.id,
                "status": "queued",
                "goal": f"Review completed workstream: {stream.title}",
                "title": f"Tester · {stream.title}",
                "model": reviewer_model,
                "effort": reviewer_effort,
            }
        )
        if stream.work_items and all(
            item.id in previous_item_evidence for item in stream.work_items
        ):
            contextual_event(
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
    plan = replace(
        plan,
        workstreams=tuple(announced_streams),
        updated_at=utc_now(),
    )
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
            "dependency_aware": True,
            "summary": (
                f"Planned {len(planned_worker_ids)} coders + "
                f"{len(planned_tester_ids)} testers; execution follows DAG "
                f"dependencies with at most {bounds.max_parallel_workers} "
                "concurrent worker pipelines."
            ),
        }
    )
    preflight_issues = _preflight_plan(
        root=root,
        plan=plan,
        limits=bounds,
        allow_new_files=allow_new_files,
        available_artifacts=persisted_handoff_artifacts,
    )
    # Streams settled by preflight before the executor starts, so the scheduler
    # below skips them instead of re-running work that already has a verdict.
    preflight_stopped_stream_ids: set[str] = set()
    preflight_item_evidence: dict[str, dict[str, Any]] = {}
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
                    agent_dispositions=agent_dispositions,
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
            return result

    plan = replace(
        plan,
        status=PlanStatus.RUNNING,
        updated_at=utc_now(),
    )
    repo.save_plan(plan, expected_previous_revision=planned_revision)
    plan_lock = threading.RLock()

    project_lock = _project_lock_for(root)
    worker_semaphore = threading.BoundedSemaphore(bounds.max_parallel_workers)
    worker_submission_semaphore = threading.BoundedSemaphore(bounds.max_parallel_workers)
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
        error = f"{type(exc).__name__}: {exc}"
        was_cancelled = isinstance(exc, llm_client.ModelRequestAborted) or task_cancelled()
        preflight = isinstance(
            exc,
            (
                FileNotFoundError,
                IsADirectoryError,
                safety.GitError,
            ),
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
                if preflight
                else "Manager cần đổi chiến lược trước lần thử tiếp theo."
            ),
            patch_sha256=None,
            additions=0,
            deletions=0,
            syntax_status="not_run",
            test_status="not_run",
            test_output="",
            error=error[:2000],
            failure_kind=(
                "cancelled" if was_cancelled else "preflight" if preflight else "backend_failure"
            ),
            retryable=not preflight and not was_cancelled,
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
                "status": "waiting_for_slot",
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
                "status": "queued",
                "goal": f"Review {item.title}",
                "title": f"Tester · {item.title}",
            }
        )
        claim_acquired = False
        worker_slot_acquired = False
        try:
            llm_client.thread_local.task_id = task_id

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
                error = (
                    f"Work item {item.id!r} inputs were not materialized after "
                    f"dependencies completed: {', '.join(missing_item_inputs)}"
                )
                contextual_event(
                    {
                        "type": "preflight_failed",
                        "role": "worker",
                        "agent_instance_id": worker_id,
                        "manager_id": manager_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "status": "preflight_failed",
                        "error": error,
                        "failure_kind": "missing_contract_input",
                        "issues": [
                            {
                                "code": "missing_contract_input",
                                "message": error,
                                "paths": list(missing_item_inputs),
                            }
                        ],
                    }
                )
                return failed_execution(
                    str(item.metadata.get("file_path") or item.id),
                    FileNotFoundError(error),
                )
            contextual_event(
                {
                    "type": "agent_started",
                    "role": "worker",
                    "agent_instance_id": worker_id,
                    "manager_id": manager_id,
                    "workstream_id": stream.id,
                    "work_item_id": item.id,
                    "status": "running",
                    "goal": item.goal,
                    "title": item.title,
                }
            )

            instructions = (
                str(item.metadata["instructions"])
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
            previous_numbers = [attempt.number for attempt in repo.list_attempts(task_id, item.id)]
            first_attempt_number = max(previous_numbers, default=0) + 1
            completed_files = completed_package_files.setdefault(item.id, set())
            for attempt_number in range(
                first_attempt_number,
                first_attempt_number + max_attempts_per_item,
            ):
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
                if not scheduler.acquire_work_lease(item.id, worker_id, 900):
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
                    package_files = _package_files(item)
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
                            on_file_approved=on_file_approved,
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
                        )
                        if not last.accepted:
                            break
                        completed_files.add(target_file)
                except BaseException as exc:
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
                                else "preflight_failed"
                                if last.failure_kind == "preflight"
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
                scheduler.release_work_lease(item.id, worker_id)
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
                if last.accepted:
                    break
                if not last.retryable:
                    break
                instructions += "\n\n## RETRY EVIDENCE\n" + (
                    last.next_instructions or last.error or last.execution_result
                )
            assert last is not None
            return last
        finally:
            llm_client.thread_local.execution_attempt_id = None
            scheduler.release_work_lease(item.id, worker_id)
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
            error = (
                f"Workstream {stream.id!r} inputs were not materialized after "
                f"dependencies completed: {', '.join(missing_stream_inputs)}"
            )
            for item in stream.work_items:
                if statuses[item.id] == WorkStatus.APPROVED:
                    continue
                statuses[item.id] = WorkStatus.BLOCKED
                item_statuses[item.id] = WorkStatus.BLOCKED
                item_evidence[item.id] = {
                    "accepted": False,
                    "status": "blocked",
                    "error": error,
                    "failure_kind": "dependency_input",
                    "retryable": False,
                    "blocked_by": list(stream.dependencies),
                }
                contextual_event(
                    {
                        "type": "agent_blocked",
                        "role": "worker",
                        "agent_instance_id": item.metadata.get("worker_agent_id"),
                        "manager_id": manager_id,
                        "workstream_id": stream.id,
                        "work_item_id": item.id,
                        "status": "blocked",
                        "failure_kind": "dependency_input",
                        "blocked_by": list(stream.dependencies),
                        "summary": error,
                    }
                )
            contextual_event(
                {
                    "type": "agent_blocked",
                    "role": "tester",
                    "agent_instance_id": tester_id,
                    "manager_id": manager_id,
                    "workstream_id": stream.id,
                    "status": "blocked",
                    "failure_kind": "dependency_input",
                    "blocked_by": list(stream.dependencies),
                    "summary": "Tester was not called because workstream inputs are missing.",
                }
            )
            contextual_event(
                {
                    "type": "preflight_failed",
                    "role": "manager",
                    "agent_instance_id": manager_id,
                    "workstream_id": stream.id,
                    "status": "preflight_failed",
                    "error": error,
                    "failure_kind": "dependency_input",
                    "issues": [
                        {
                            "code": "missing_contract_input",
                            "message": error,
                            "paths": list(missing_stream_inputs),
                        }
                    ],
                }
            )
            return stream.id, False
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
        max_recovery_cycles = max(0, config.MANAGER_RECOVERY_CYCLES)
        for recovery_cycle in range(max_recovery_cycles + 1):
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
                                "retryable": True,
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
                    max_workers=min(
                        len(selected),
                        stream.requested_worker_count,
                        bounds.worker_parallel_cap,
                    ),
                    thread_name_prefix=f"workers-{stream.id}",
                ) as pool:
                    futures = {}
                    for item in selected:
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
                            was_cancelled = task_cancelled()
                            terminal_status = (
                                WorkStatus.CANCELLED if was_cancelled else WorkStatus.FAILED
                            )
                            statuses[item.id] = terminal_status
                            item_statuses[item.id] = terminal_status
                            item_evidence[item.id] = {
                                "accepted": False,
                                "error": f"{type(exc).__name__}: {exc}",
                                "failure_kind": ("cancelled" if was_cancelled else "scheduler"),
                                "retryable": not was_cancelled,
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
                + "\n\n## RECOVERY CYCLE\n"
                + f"{recovery_cycle}/{max_recovery_cycles}\n"
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
                event_type = "workstream_completed"
            elif recovery_cycle < max_recovery_cycles:
                event_type = "manager_replan_created"
            else:
                event_type = "workstream_failed"
            contextual_event(
                {
                    "type": event_type,
                    "role": "manager",
                    "agent_instance_id": manager_id,
                    "workstream_id": stream.id,
                    "status": (
                        "approved"
                        if approved
                        else "replanning"
                        if event_type == "manager_replan_created"
                        else "failed"
                    ),
                    "recovery_cycle": recovery_cycle,
                    "verdict": manager_review.tool_input.get("verdict"),
                    "summary": manager_review.tool_input.get("summary"),
                    "next_instructions": manager_review.tool_input.get("next_instructions"),
                }
            )
            if approved:
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
                and item_evidence.get(item_id, {}).get("retryable", True)
            ]
            if recovery_cycle >= max_recovery_cycles or not retryable_ids:
                signal(
                    manager_id,
                    director_id,
                    "workstream_result",
                    f"Manager báo workstream {stream.title}: revise",
                    workstream_id=stream.id,
                    contract=stream.contract,
                    artifacts=stream.contract.expected_outputs,
                    evidence={"status": "revise"},
                )
                return stream.id, False
            next_instructions = str(
                manager_review.tool_input.get("next_instructions") or ""
            ).strip()
            for item_id in retryable_ids:
                statuses[item_id] = WorkStatus.PENDING
                item_statuses[item_id] = WorkStatus.PENDING
                if next_instructions:
                    instruction_overrides[item_id] = next_instructions
            # Items blocked only by a retried dependency must become schedulable.
            for item_id, status in list(statuses.items()):
                if status == WorkStatus.BLOCKED:
                    statuses[item_id] = WorkStatus.PENDING
                    item_statuses[item_id] = WorkStatus.PENDING
        return stream.id, False

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
                        stream_status[stream.id] = WorkStatus.BLOCKED
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
                            item_statuses[item.id] = WorkStatus.BLOCKED
                            item_evidence[item.id] = {
                                "accepted": False,
                                "status": "blocked",
                                "error": "blocked by failed upstream workstream",
                                "failure_kind": "dependency",
                                "retryable": False,
                                "blocked_by": blocked_by,
                            }
                            contextual_event(
                                {
                                    "type": "agent_blocked",
                                    "role": "worker",
                                    "agent_instance_id": item.metadata.get("worker_agent_id"),
                                    "manager_id": stream.manager_agent_id,
                                    "workstream_id": stream.id,
                                    "work_item_id": item.id,
                                    "status": "blocked",
                                    "failure_kind": "dependency",
                                    "blocked_by": blocked_by,
                                    "summary": (
                                        "Worker was not started because an upstream "
                                        "workstream did not complete."
                                    ),
                                }
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
                                    "failure_kind": "dependency",
                                    "blocked_by": blocked_by,
                                    "summary": (
                                        "Tester was not called because its upstream "
                                        "workstream dependency failed."
                                    ),
                                }
                            )
                        contextual_event(
                            {
                                "type": "workstream_blocked",
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
                                "status": "blocked",
                                "blocked_by": blocked_by,
                                "summary": "Blocked by failed upstream workstream",
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
                        else WorkStatus.FAILED
                    )

    integration_status = "not_run"
    integration_output = ""
    integration_details: dict[str, Any] = {}
    integration_retryable = True
    director_verdict = "revise"
    director_summary = "Hierarchy failed before integration approval."
    all_approved = False
    cancelled_run = False
    max_director_recovery = max(0, config.DIRECTOR_RECOVERY_CYCLES)
    for director_cycle in range(max_director_recovery + 1):
        drain_workstreams()
        if task_cancelled() or any(
            status == WorkStatus.CANCELLED for status in stream_status.values()
        ):
            cancelled_run = True
            director_verdict = "revise"
            director_summary = "Task stopped before hierarchy completion."
            break
        workstreams_approved = all(
            status == WorkStatus.APPROVED for status in stream_status.values()
        )
        integration_status = "not_run"
        integration_output = ""
        integration_details = {}
        integration_retryable = True
        backend_gate_approved = workstreams_approved
        if workstreams_approved:
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
            sandbox_outcome = str(integration_details.get("sandbox_outcome") or "")
            integration_retryable = sandbox_outcome not in {
                "blocked",
                "unavailable",
            }
            backend_gate_approved = integration_status != "failed"
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
            "## WORKSTREAM STATUS\n"
            + json.dumps(
                {
                    stream.id: {
                        "title": stream.title,
                        "status": stream_status[stream.id].value,
                    }
                    for stream in plan.workstreams
                },
                ensure_ascii=False,
                indent=2,
            )
            + f"\n\n## INTEGRATION GATE\n{integration_status}\n"
            + integration_output[-12000:]
            + "\n\n## ITEM EVIDENCE\n"
            + json.dumps(item_evidence, ensure_ascii=False, indent=2)[-20000:]
            + f"\n\n## RECOVERY CYCLE\n{director_cycle}/{max_director_recovery}"
        )
        director_review = llm_call(
            DIRECTOR_REVIEW_PROMPT,
            final_prompt,
            DIRECTOR_REVIEW_TOOLS,
        )
        director_verdict = str(director_review.tool_input.get("verdict", "revise"))
        director_summary = str(director_review.tool_input.get("summary", ""))
        all_approved = (
            backend_gate_approved
            and director_review.tool_name == "complete_plan"
            and director_verdict == "approved"
        )
        if all_approved:
            break

        retryable_stream_ids = []
        if integration_status == "failed" and integration_retryable:
            normalized_output = integration_output.replace("\\", "/").casefold()
            affected = [
                stream.id
                for stream in plan.workstreams
                if any(
                    str(scope).replace("\\", "/").casefold() in normalized_output
                    for scope in stream.write_scopes
                    if str(scope).strip()
                )
            ]
            # A global integration error often does not name a path. One
            # bounded Director cycle may then re-open every stream.
            retryable_stream_ids.extend(affected or [stream.id for stream in plan.workstreams])
        for stream in plan.workstreams:
            if stream_status.get(stream.id) != WorkStatus.FAILED:
                continue
            if any(
                item_evidence.get(item.id, {}).get("retryable", True)
                for item in stream.work_items
                if item_statuses.get(item.id)
                in {WorkStatus.FAILED, WorkStatus.BLOCKED, WorkStatus.PENDING}
            ):
                if stream.id not in retryable_stream_ids:
                    retryable_stream_ids.append(stream.id)
        changed = True
        while changed:
            changed = False
            for stream in plan.workstreams:
                if (
                    stream_status.get(stream.id) != WorkStatus.BLOCKED
                    or stream.id in retryable_stream_ids
                ):
                    continue
                if any(dependency in retryable_stream_ids for dependency in stream.dependencies):
                    retryable_stream_ids.append(stream.id)
                    changed = True
        if director_cycle >= max_director_recovery or not retryable_stream_ids:
            break
        contextual_event(
            {
                "type": "director_replan_created",
                "role": "director",
                "agent_instance_id": director_id,
                "status": "replanning",
                "recovery_cycle": director_cycle + 1,
                "workstream_ids": retryable_stream_ids,
                "summary": director_summary,
            }
        )
        for stream in plan.workstreams:
            if stream.id not in retryable_stream_ids:
                continue
            stream_status[stream.id] = WorkStatus.PENDING
            for item in stream.work_items:
                if (
                    integration_status == "failed"
                    or item_statuses.get(item.id) != WorkStatus.APPROVED
                ):
                    item_statuses[item.id] = WorkStatus.PENDING
                    if director_summary:
                        instruction_overrides[item.id] = director_summary

    final_streams = tuple(
        replace(
            stream,
            status=stream_status[stream.id],
            work_items=tuple(
                replace(item, status=item_statuses.get(item.id, WorkStatus.PENDING))
                for item in stream.work_items
            ),
        )
        for stream in plan.workstreams
    )
    running_revision = plan.revision
    final_plan = replace(
        plan,
        revision=running_revision + 1,
        status=(
            PlanStatus.COMPLETED
            if all_approved
            else PlanStatus.CANCELLED
            if cancelled_run
            else PlanStatus.FAILED
        ),
        workstreams=final_streams,
        updated_at=utc_now(),
    )
    with event_state_lock:
        reconciliation = reconcile_completion(
            final_plan,
            item_evidence=item_evidence,
            call_outcomes=model_call_outcomes,
            director_agent_id=director_id,
            called_agent_ids=called_agent_ids,
            agent_dispositions=agent_dispositions,
            agent_call_purposes=agent_call_purposes,
        )
    if not reconciliation["balanced"]:
        all_approved = False
        final_plan = replace(final_plan, status=PlanStatus.FAILED)
        director_verdict = "revise"
        director_summary = "Completion reconciliation failed: " + "; ".join(
            reconciliation["errors"]
        )
    repo.save_plan(final_plan, expected_previous_revision=running_revision)

    contextual_event(
        {
            "type": "completion_reconciliation",
            "role": "director",
            "agent_instance_id": director_id,
            "status": "balanced" if reconciliation["balanced"] else "failed",
            **reconciliation,
        }
    )
    contextual_event(
        {
            "type": (
                "hierarchy_completed"
                if all_approved
                else "hierarchy_cancelled"
                if cancelled_run
                else "hierarchy_failed"
            ),
            "role": "director",
            "agent_instance_id": director_id,
            "status": ("completed" if all_approved else "cancelled" if cancelled_run else "failed"),
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
                        evidence.get("reviewer_feedback") or evidence.get("error") or item.title
                    )[:1000],
                    stop_loop=False,
                )
            )
    result.stopped_reason = (
        "task_completed" if all_approved else "cancelled" if cancelled_run else "hierarchy_failed"
    )
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
        "last_execution_result": (
            f"Hierarchy {director_verdict}; integration={integration_status}"
        ),
        "last_review_verdict": director_verdict,
        "last_reviewer_feedback": director_summary,
        "reconciliation": reconciliation,
    }
    llm_client.thread_local.event_sink = None
    llm_client.thread_local.agent_instance_id = None
    return result
