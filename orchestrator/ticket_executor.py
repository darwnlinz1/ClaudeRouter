"""Execute one hierarchical work item through Worker, gates, and Tester."""

from __future__ import annotations

import difflib
import hashlib
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from . import (
    config,
    file_agent,
    llm_client,
    patch_engine,
    path_utils,
    project_workspace,
    retry_policy,
    safety,
    worker_targets,
)
from .effects import (
    MUST_BE_ABSENT,
    EffectReplayConflictError,
    EffectState,
    PreparedFileEffect,
)
from .llm_client import ToolCallResult, call_agent
from .models import ApprovalPolicy, RiskLevel
from .policy import PolicyAction, PolicyEffect, PolicyEngine, PolicyRequest
from .system_prompt_reviewer import SYSTEM_PROMPT as REVIEW_PROMPT
from .system_prompt_worker import SYSTEM_PROMPT as WORK_PROMPT
from .tools_schema import REVIEWER_TOOLS, WORKER_TOOLS

LLMCallFn = Callable[
    [str, str, list[dict[str, Any]]],
    ToolCallResult,
]
EventSink = Callable[[dict[str, Any]], None]
ApprovedFileSink = Callable[[str], None]
ApprovalCallback = Callable[[dict[str, Any]], bool]
CancellationCallback = Callable[[], bool]


@dataclass(frozen=True)
class TicketExecutionResult:
    accepted: bool
    file_path: str
    worker_feedback: str
    execution_result: str
    reviewer_feedback: str
    reviewer_verdict: str
    next_instructions: str
    patch_sha256: str | None
    additions: int
    deletions: int
    syntax_status: str
    test_status: str
    test_output: str
    error: str = ""
    failure_kind: str = ""
    retryable: bool = False
    before_sha256: str | None = None
    after_sha256: str | None = None
    effect_id: str | None = None
    sandbox_isolation: str = "none"
    test_scope: str = "item"
    failure_category: str = ""
    failure_signature: str = ""
    remediation_hints: tuple[str, ...] = ()
    failure_actor: str = ""
    diagnostic_log_refs: dict[str, str] = field(default_factory=dict)
    remediation_prompt: str = ""
    failure_signature_components: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.failure_kind:
            return
        prompt_inputs = build_failure_prompt_inputs(
            failure_kind=self.failure_kind,
            target=self.file_path,
            source=self.failure_actor,
            patch=self.patch_sha256,
            test={
                "syntax_status": self.syntax_status,
                "test_status": self.test_status,
                "test_output": self.test_output,
            },
            review={
                "verdict": self.reviewer_verdict,
                "feedback": self.reviewer_feedback,
                "next_instructions": self.next_instructions,
            },
            request=self.diagnostic_log_refs,
            error=self.error,
            actor=self.failure_actor,
            diagnostic_log_refs=self.diagnostic_log_refs,
        )
        if not self.failure_category:
            object.__setattr__(
                self,
                "failure_category",
                str(prompt_inputs["failure_category"]),
            )
        if not self.failure_signature:
            object.__setattr__(
                self,
                "failure_signature",
                str(prompt_inputs["failure_signature"]),
            )
        if not self.failure_signature_components:
            object.__setattr__(
                self,
                "failure_signature_components",
                dict(prompt_inputs["failure_signature_components"]),
            )
        if not self.remediation_hints:
            object.__setattr__(
                self,
                "remediation_hints",
                tuple(str(item) for item in prompt_inputs["remediation_hints"]),
            )
        if not self.remediation_prompt:
            object.__setattr__(
                self,
                "remediation_prompt",
                str(prompt_inputs["remediation_prompt"]),
            )

    @property
    def category(self) -> str:
        return self.failure_category

    @property
    def signature(self) -> str:
        return self.failure_signature

    @property
    def actor(self) -> str:
        return self.failure_actor

    @property
    def log_refs(self) -> dict[str, str]:
        return dict(self.diagnostic_log_refs)

    def recovery_context(self) -> dict[str, Any]:
        """Return prompt-ready metadata; hierarchy decides how to persist it."""

        return {
            "failure_kind": self.failure_kind,
            "failure_category": self.failure_category,
            "failure_signature": self.failure_signature,
            "failure_signature_components": dict(self.failure_signature_components),
            "remediation_hints": list(self.remediation_hints),
            "failure_actor": self.failure_actor,
            "diagnostic_log_refs": dict(self.diagnostic_log_refs),
            "remediation_prompt": self.remediation_prompt,
        }

    def evidence(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "file_path": self.file_path,
            "worker_feedback": self.worker_feedback,
            "execution_result": self.execution_result,
            "reviewer_feedback": self.reviewer_feedback,
            "reviewer_verdict": self.reviewer_verdict,
            "next_instructions": self.next_instructions,
            "patch_sha256": self.patch_sha256,
            "additions": self.additions,
            "deletions": self.deletions,
            "syntax_status": self.syntax_status,
            "test_status": self.test_status,
            "test_scope": self.test_scope,
            "test_output": self.test_output[-12000:],
            "error": self.error,
            "failure_kind": self.failure_kind,
            "retryable": self.retryable,
            "before_sha256": self.before_sha256,
            "after_sha256": self.after_sha256,
            "effect_id": self.effect_id,
            "sandbox_isolation": self.sandbox_isolation,
            "failure_category": self.failure_category,
            "failure_signature": self.failure_signature,
            "failure_signature_components": dict(self.failure_signature_components),
            "remediation_hints": list(self.remediation_hints),
            "failure_actor": self.failure_actor,
            "diagnostic_log_refs": dict(self.diagnostic_log_refs),
            "remediation_prompt": self.remediation_prompt,
        }


def build_failure_prompt_inputs(
    *,
    failure_kind: str,
    contract: Any = None,
    target: Any = None,
    source: Any = None,
    patch: Any = None,
    test: Any = None,
    review: Any = None,
    request: Any = None,
    error: str = "",
    actor: str = "",
    diagnostic_log_refs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Public hierarchy hook for category-specific recovery prompts."""

    return retry_policy.prompt_inputs_for_failure(
        failure_kind,
        contract=contract,
        target=target,
        source=source,
        patch=patch,
        test=test,
        review=review,
        request=request,
        error=error,
        actor=actor,
        diagnostic_refs=diagnostic_log_refs,
    )


def render_failure_prompt_inputs(context: Mapping[str, Any] | None) -> str:
    """Render persisted failure input without inventing a generic retry."""

    if not context:
        return ""
    rendered = str(context.get("remediation_prompt") or "").strip()
    if rendered:
        return rendered
    category = str(context.get("failure_category") or "backend_scheduler_unknown")
    signature = str(context.get("failure_signature") or "unavailable")
    hints = tuple(str(item) for item in (context.get("remediation_hints") or ()))
    lines = [
        f"## {category.replace('_', ' ').upper()} RECOVERY",
        f"Failure signature: {signature}",
    ]
    lines.extend(f"- {hint}" for hint in hints)
    refs = context.get("diagnostic_log_refs")
    if isinstance(refs, Mapping) and refs:
        lines.append(
            "Use diagnostic references: "
            + ", ".join(f"{key}={value}" for key, value in sorted(refs.items()))
        )
    return "\n".join(lines)


def _default_llm_call(
    system_prompt: str,
    user_message: str,
    tools: list[dict[str, Any]],
) -> ToolCallResult:
    return call_agent(system_prompt, user_message, tools)


def _emit(on_event: EventSink | None, base: dict[str, Any], **payload: Any) -> None:
    if on_event is not None:
        if payload.get("type") == "agent_failed":
            payload.setdefault("telemetry_scope", "attempt")
            payload.setdefault("attempt_terminal", True)
        on_event({**base, **payload})


def _patch_stats(patch: str) -> tuple[int, int]:
    additions = deletions = 0
    pattern = r"<<<< SEARCH\r?\n(.*?)\r?\n====\r?\n(.*?)\r?\n>>>> REPLACE"
    for before, after in re.findall(pattern, patch, re.DOTALL):
        for line in difflib.ndiff(before.splitlines(), after.splitlines()):
            additions += int(line.startswith("+ "))
            deletions += int(line.startswith("- "))
    return additions, deletions


@dataclass(frozen=True)
class _PreparedTicketPatch:
    file_path: str
    absolute_path: Path
    occurrences_before: int
    file_effect: PreparedFileEffect


def _prepare_ticket_patch(
    root: Path,
    file_path: str,
    patch: str,
) -> _PreparedTicketPatch:
    """Validate and flush a patch result without exposing it at the target."""
    if not patch:
        raise patch_engine.PatchRejected("Worker did not return a patch")
    normalized, absolute = path_utils.resolve_under_root(root, file_path)
    worker_targets.ensure_worker_target(normalized, absolute_path=absolute)
    resolved = absolute.relative_to(root.resolve()).as_posix()
    patch_engine.validate_file_path(
        normalized,
        frozenset({normalized}),
        resolved_file_path=resolved,
    )
    if not absolute.exists():
        create_pattern = r"<<<< SEARCH\r?\n(?:\r?\n)?====\r?\n(.*?)(?:\r?\n)?>>>> REPLACE"
        create_blocks = re.findall(create_pattern, patch, re.DOTALL)
        if len(create_blocks) != 1:
            raise patch_engine.FileMissingError(
                f"'{normalized}' does not exist; creation requires one empty SEARCH"
            )
        prepared = project_workspace.prepare_atomic_write_text(
            absolute,
            create_blocks[0],
            encoding="utf-8",
            expected_before=MUST_BE_ABSENT,
        )
        return _PreparedTicketPatch(normalized, absolute, 0, prepared)

    original = absolute.read_bytes()
    expected_before = project_workspace.sha256_bytes(original)
    content = original.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    pattern = r"<<<< SEARCH\r?\n(.*?)\r?\n====\r?\n(.*?)(?:\r?\n)?>>>> REPLACE"
    blocks = re.findall(pattern, patch, re.DOTALL)
    if not blocks:
        raise patch_engine.PatchRejected(
            "Patch has no valid <<<< SEARCH / ==== / >>>> REPLACE block"
        )
    occurrences_before = 0
    for search_block, replace_block in blocks:
        occurrences = content.count(search_block)
        if occurrences == 0:
            raise patch_engine.AnchorNotFoundError(
                f"SEARCH block was not found in {normalized}: {search_block[:100]!r}"
            )
        if occurrences > 1:
            raise patch_engine.AnchorNotUniqueError(occurrences)
        content = content.replace(search_block, replace_block)
        occurrences_before += occurrences
    prepared = project_workspace.prepare_atomic_write_text(
        absolute,
        content,
        encoding="utf-8",
        expected_before=expected_before,
    )
    return _PreparedTicketPatch(
        normalized,
        absolute,
        occurrences_before,
        prepared,
    )


def _read_target(rel_path: str, path: Path) -> str:
    if not path.exists():
        return "[AUTHORIZED NEW FILE]"
    text = worker_targets.read_prompt_text(rel_path, path)
    if len(text) <= config.MAX_FILE_CHARS:
        return text
    half = config.MAX_FILE_CHARS // 2
    return (
        text[:half]
        + f"\n\n[TRUNCATED {len(text) - config.MAX_FILE_CHARS} CHARACTERS]\n\n"
        + text[-half:]
    )


def execute_work_item(
    *,
    root: Path,
    task_goal: str,
    workstream_goal: str,
    work_item_id: str,
    file_path: str,
    instructions: str,
    acceptance_criteria: list[str] | tuple[str, ...],
    test_focus: str = "",
    test_cmd: list[str] | None = None,
    allow_new_files: bool = False,
    llm_call: LLMCallFn = _default_llm_call,
    on_event: EventSink | None = None,
    on_file_approved: ApprovedFileSink | None = None,
    execution_lock: threading.RLock | threading.Lock | None = None,
    tester_lock: threading.Lock | None = None,
    effect_repository: Any | None = None,
    project_lease: Any | None = None,
    task_id: str = "",
    session_id: str = "",
    workstream_id: str = "",
    manager_agent_id: str = "",
    worker_agent_id: str = "",
    tester_agent_id: str = "",
    attempt_id: str = "",
    approval_policy: str = "risk_based",
    risk_level: str = "low",
    approval_callback: ApprovalCallback | None = None,
    policy_engine: PolicyEngine | None = None,
    write_scopes: tuple[str, ...] = (),
    cancelled: CancellationCallback | None = None,
    defer_tests_to_integration: bool = False,
    recovery_context: Mapping[str, Any] | None = None,
) -> TicketExecutionResult:
    """Run one file-scoped ticket.

    Worker inference happens before the project lock. Patch/Git/tests/review are
    serialized so multiple Workers may reason concurrently without racing the
    working tree.
    """
    root = root.resolve()
    path_utils.ensure_context_path_safe(file_path)
    normalized, absolute = path_utils.resolve_under_root(root, file_path)
    worker_targets.ensure_worker_target(normalized, absolute_path=absolute)
    if absolute.exists():
        normalized, absolute = file_agent.authorize_existing_file(root, normalized)
    elif not allow_new_files:
        raise FileNotFoundError(f"File chưa tồn tại trên máy: {normalized}")

    worker_base = {
        "task_id": task_id,
        "session_id": session_id,
        "workstream_id": workstream_id,
        "work_item_id": work_item_id,
        "manager_id": manager_agent_id,
        "agent_instance_id": worker_agent_id,
        "attempt_id": attempt_id,
        "role": "worker",
    }
    tester_base = {
        **worker_base,
        "agent_instance_id": tester_agent_id,
        "role": "tester",
    }

    def diagnostic_refs(
        actor: str,
        *,
        patch_sha256: str | None = None,
        effect_id: str | None = None,
    ) -> dict[str, str]:
        actor_agent_id = (
            tester_agent_id
            if actor in {"tester", "reviewer"}
            else manager_agent_id
            if actor == "manager"
            else worker_agent_id
        )
        refs: dict[str, str] = {
            "task_id": task_id,
            "session_id": session_id,
            "workstream_id": workstream_id,
            "work_item_id": work_item_id,
            "execution_attempt_id": attempt_id,
            "actor": actor,
            "actor_agent_id": actor_agent_id,
            "manager_agent_id": manager_agent_id,
            "worker_agent_id": worker_agent_id,
            "tester_agent_id": tester_agent_id,
            "file_path": normalized,
            "patch_sha256": patch_sha256 or "",
            "effect_id": effect_id or "",
            "llm_logical_request_id": str(
                getattr(llm_client.thread_local, "logical_request_id", "") or ""
            ),
            "provider_attempt_id": str(
                getattr(llm_client.thread_local, "attempt_id", "") or ""
            ),
            "request_fingerprint": str(
                getattr(llm_client.thread_local, "request_fingerprint", "") or ""
            ),
        }
        return {key: value for key, value in refs.items() if value}

    def failure_fields(
        failure_kind: str,
        actor: str,
        error: str,
        *,
        patch_sha256: str | None = None,
        test: Any = None,
        review: Any = None,
        effect_id: str | None = None,
    ) -> dict[str, Any]:
        refs = diagnostic_refs(
            actor,
            patch_sha256=patch_sha256,
            effect_id=effect_id,
        )
        prompt_inputs = build_failure_prompt_inputs(
            failure_kind=failure_kind,
            contract={
                "instructions": instructions,
                "acceptance_criteria": tuple(acceptance_criteria),
                "write_scopes": tuple(write_scopes),
            },
            target=normalized,
            source=refs.get("actor_agent_id") or actor,
            patch=patch_sha256,
            test=test,
            review=review,
            request={
                "logical_request_id": refs.get("llm_logical_request_id"),
                "request_fingerprint": refs.get("request_fingerprint"),
                "execution_attempt_id": attempt_id,
            },
            error=error,
            actor=actor,
            diagnostic_log_refs=refs,
        )
        return {
            "failure_category": prompt_inputs["failure_category"],
            "failure_signature": prompt_inputs["failure_signature"],
            "failure_signature_components": dict(
                prompt_inputs["failure_signature_components"]
            ),
            "remediation_hints": tuple(prompt_inputs["remediation_hints"]),
            "failure_actor": actor,
            "diagnostic_log_refs": refs,
            "remediation_prompt": prompt_inputs["remediation_prompt"],
        }

    failure_actor = "worker"
    if cancelled is not None and cancelled():
        details = failure_fields("cancelled", "worker", "task cancelled")
        return TicketExecutionResult(
            False,
            normalized,
            "",
            "Blocked: task cancelled before ticket execution",
            "",
            "not_run",
            details["remediation_prompt"],
            None,
            0,
            0,
            "not_run",
            "not_run",
            "",
            "task cancelled",
            "cancelled",
            False,
            **details,
        )
    worker_prompt = (
        f"## MỤC TIÊU TOÀN TASK\n{task_goal}\n\n"
        f"## WORKSTREAM\n{workstream_goal}\n\n"
        f"## WORK ITEM\n{instructions}\n\n"
        "## ACCEPTANCE CRITERIA\n- "
        + "\n- ".join(str(item) for item in acceptance_criteria)
        + f"\n\n## TARGET FILE: {normalized}\n"
        f"## MÃ NGUỒN HIỆN TẠI\n```\n{_read_target(normalized, absolute)}\n```"
    )
    recovery_prompt = render_failure_prompt_inputs(recovery_context)
    if recovery_prompt:
        worker_prompt += f"\n\n{recovery_prompt}"
    _emit(
        on_event,
        worker_base,
        type="agent_started",
        status="running",
        goal=instructions,
        prompt=worker_prompt,
        file_path=normalized,
        model=getattr(llm_client.thread_local, "worker_model", None),
        effort=getattr(llm_client.thread_local, "worker_effort", None),
    )
    llm_client.thread_local.agent_role = "worker"
    llm_client.thread_local.agent_instance_id = worker_agent_id
    llm_client.thread_local.manager_id = manager_agent_id
    llm_client.thread_local.workstream_id = workstream_id
    llm_client.thread_local.work_item_id = work_item_id
    try:
        worker_result = llm_call(WORK_PROMPT, worker_prompt, WORKER_TOOLS)
    except llm_client.ModelRequestAborted as exc:
        retry_decision = retry_policy.classify_exception(exc)
        was_cancelled = retry_decision.failure_kind == "cancelled"
        details = failure_fields(
            retry_decision.failure_kind,
            "worker",
            str(exc),
        )
        _emit(
            on_event,
            worker_base,
            type="agent_cancelled" if was_cancelled else "agent_failed",
            status="cancelled" if was_cancelled else "failed",
            error=str(exc),
            failure_kind=retry_decision.failure_kind,
            file_path=normalized,
        )
        return TicketExecutionResult(
            False,
            normalized,
            "",
            "Cancelled before Worker response"
            if was_cancelled
            else "Worker lease was lost before its response completed",
            "",
            "not_run",
            details["remediation_prompt"],
            None,
            0,
            0,
            "not_run",
            "not_run",
            "",
            str(exc),
            retry_decision.failure_kind,
            retry_decision.retryable,
            **details,
        )
    if cancelled is not None and cancelled():
        details = failure_fields("cancelled", "worker", "task cancelled")
        return TicketExecutionResult(
            False,
            normalized,
            "",
            "Blocked: task cancelled after worker inference",
            "",
            "not_run",
            details["remediation_prompt"],
            None,
            0,
            0,
            "not_run",
            "not_run",
            "",
            "task cancelled",
            "cancelled",
            False,
            **details,
        )
    if worker_result.tool_name != "submit_patch":
        raise RuntimeError(f"Worker action không hợp lệ: {worker_result.tool_name}")
    worker_feedback = str(worker_result.tool_input.get("worker_feedback", ""))
    if worker_result.tool_input.get("task_status") == "failed":
        error = worker_feedback or "Worker báo không thể hoàn thành work item."
        details = failure_fields("contract_infeasible", "worker", error)
        _emit(
            on_event,
            worker_base,
            type="agent_failed",
            status="failed",
            error=error,
            failure_kind="worker_declined",
            file_path=normalized,
        )
        return TicketExecutionResult(
            False,
            normalized,
            worker_feedback,
            f"Fail: {error}",
            "",
            "not_run",
            details["remediation_prompt"],
            None,
            0,
            0,
            "not_run",
            "not_run",
            "",
            error,
            "contract_infeasible",
            False,
            **details,
        )
    patch = str(worker_result.tool_input.get("patch_content", "")).strip()
    if not patch:
        raise RuntimeError("Worker không trả patch")
    additions, deletions = _patch_stats(patch)
    patch_hash = hashlib.sha256(patch.encode("utf-8")).hexdigest()
    _emit(
        on_event,
        worker_base,
        type="agent_action",
        action="submit_patch",
        file_path=normalized,
        patch=patch,
        additions=additions,
        deletions=deletions,
    )
    _emit(
        on_event,
        worker_base,
        type="agent_message",
        role="agent",
        source_agent_id=worker_agent_id,
        target_agent_id=tester_agent_id,
        signal_type="submit_patch",
        summary=f"Worker gửi patch {normalized} cho Tester",
    )

    policy = policy_engine or PolicyEngine()
    policy_request = PolicyRequest(
        action=PolicyAction.EFFECT_APPLY,
        resource=normalized,
        task_id=task_id or None,
        content=patch,
        scopes=write_scopes or (normalized,),
        approval_policy=ApprovalPolicy(approval_policy),
        risk_level=RiskLevel(risk_level),
    )
    policy_decision = policy.evaluate(policy_request)
    if policy_decision.effect is PolicyEffect.REQUIRE_APPROVAL:
        approved = bool(
            approval_callback
            and approval_callback(
                {
                    "task_id": task_id,
                    "session_id": session_id,
                    "workstream_id": workstream_id,
                    "work_item_id": work_item_id,
                    "attempt_id": attempt_id,
                    "kind": "patch_apply",
                    "target": normalized,
                    "reason": (f"{risk_level} risk work contract requires human approval"),
                    "patch_sha256": patch_hash,
                    "additions": additions,
                    "deletions": deletions,
                }
            )
        )
        policy_request = PolicyRequest(
            action=policy_request.action,
            resource=policy_request.resource,
            subject=policy_request.subject,
            task_id=policy_request.task_id,
            content=policy_request.content,
            scopes=policy_request.scopes,
            approval_policy=policy_request.approval_policy,
            risk_level=policy_request.risk_level,
            approval_status="approved" if approved else "rejected",
        )
        policy_decision = policy.evaluate(policy_request)
    if not policy_decision.allowed:
        approval_blocked = any(
            reason in {"approval_rejected", "risk_requires_approval"}
            for reason in policy_decision.reasons
        )
        error = (
            "Human approval rejected or the task stopped."
            if approval_blocked
            else "Patch blocked by policy: " + ", ".join(policy_decision.reasons)
        )
        failure_kind = "approval_rejected" if approval_blocked else "policy_denied"
        details = failure_fields(
            failure_kind,
            "policy",
            error,
            patch_sha256=patch_hash,
        )
        _emit(
            on_event,
            worker_base,
            type="agent_failed",
            status="blocked",
            error=error,
            failure_kind=failure_kind,
            file_path=normalized,
        )
        return TicketExecutionResult(
            False,
            normalized,
            worker_feedback,
            error,
            "",
            "not_run",
            details["remediation_prompt"],
            patch_hash,
            additions,
            deletions,
            "not_run",
            "not_run",
            "",
            error,
            failure_kind,
            False,
            **details,
        )

    lock = execution_lock or threading.RLock()
    backup_hash: str | None = None
    syntax_status = "not_run"
    test_status = "not_run"
    test_scope = "item"
    test_output = ""
    before_sha256: str | None = None
    after_sha256: str | None = None
    expected_applied_sha256: str | None = None
    effect_receipt = None
    effect_fencing_token = (
        getattr(project_lease, "fencing_token", None) if project_lease is not None else None
    )
    sandbox_isolation = "none"

    def fenced_mutation(operation: Callable[[], Any]) -> Any:
        if project_lease is None:
            return operation()
        return project_lease.mutate(operation)

    def rollback_with_receipt(reason: str) -> None:
        nonlocal effect_receipt
        if not backup_hash:
            return
        rollback_before = project_workspace.sha256_file(absolute)
        rollback_expected = expected_applied_sha256
        rollback_receipt = None
        if effect_repository is not None and task_id:
            if effect_receipt is not None and effect_receipt.state is EffectState.PENDING:
                reconciled_original = effect_repository.reconcile_pending_effect_by_target_hash(
                    effect_receipt.effect_id,
                    rollback_before,
                    result={"recovered_before_compensation": True},
                    fencing_token=effect_receipt.fencing_token,
                )
                if reconciled_original is not None:
                    effect_receipt = reconciled_original
            if (
                effect_receipt is not None
                and effect_receipt.state is EffectState.FAILED
                and rollback_before == effect_receipt.before_sha256
            ):
                return
            if effect_receipt is not None:
                rollback_expected = (
                    effect_receipt.expected_after_sha256
                    or effect_receipt.after_sha256
                    or rollback_expected
                )
            if effect_receipt is not None:
                durable_original = effect_repository.get_effect(effect_receipt.effect_id)
                if durable_original is not None and durable_original.compensated:
                    return
        if rollback_expected is None:
            # No file effect reached the mutation boundary, so there is
            # nothing to compensate.
            return
        if rollback_before != rollback_expected:
            raise safety.RollbackConflictError(
                absolute,
                rollback_expected,
                rollback_before,
            )
        if effect_repository is not None and task_id:
            rollback_receipt = effect_repository.begin_effect(
                task_id,
                f"rollback:{effect_receipt.effect_id if effect_receipt else backup_hash}",
                "file_rollback",
                normalized,
                payload={
                    "snapshot": backup_hash,
                    "reason": reason,
                    "attempt_id": attempt_id,
                },
                before_sha256=rollback_before,
                expected_after_sha256=(
                    effect_receipt.before_sha256 if effect_receipt is not None else None
                ),
                fencing_token=effect_fencing_token,
                compensates_effect_id=(
                    effect_receipt.effect_id if effect_receipt is not None else None
                ),
            )
            if rollback_receipt.state in {
                EffectState.APPLIED,
                EffectState.RECONCILED,
            }:
                return
        try:
            fenced_mutation(
                lambda: safety.rollback_to(
                    root,
                    backup_hash,
                    normalized,
                    expected_after_sha256=rollback_expected,
                )
            )
            rollback_after = project_workspace.sha256_file(absolute)
            if rollback_receipt is not None:
                effect_repository.complete_effect(
                    rollback_receipt.effect_id,
                    result={"reason": reason, "restored": True},
                    after_sha256=rollback_after,
                    fencing_token=effect_fencing_token,
                )
        except Exception as rollback_error:
            if rollback_receipt is not None and rollback_receipt.state is EffectState.PENDING:
                effect_repository.fail_effect(
                    rollback_receipt.effect_id,
                    f"{type(rollback_error).__name__}: {rollback_error}",
                    fencing_token=effect_fencing_token,
                )
            raise

    try:
        with lock:
            backup_hash = safety.backup_commit(
                root,
                f"hierarchy pre-patch: {work_item_id}",
                normalized,
            )
            base_effect_key = f"patch:{work_item_id}:{normalized}:{patch_hash}"
            effect_key = base_effect_key
            application_generation = 1
            reapplies_effect_id: str | None = None
            if effect_repository is not None and task_id:
                effect_receipt = effect_repository.get_effect_by_idempotency(
                    task_id,
                    effect_key,
                )
                while effect_receipt is not None:
                    if effect_receipt.state is EffectState.PENDING:
                        effect_receipt = (
                            effect_repository.reconcile_pending_effect_by_target_hash(
                                effect_receipt.effect_id,
                                project_workspace.sha256_file(absolute),
                                result={
                                    "patch_sha256": patch_hash,
                                    "recovered": True,
                                },
                                fencing_token=effect_receipt.fencing_token,
                            )
                            or effect_receipt
                        )
                    if (
                        effect_receipt.state
                        not in {EffectState.APPLIED, EffectState.RECONCILED}
                        or not effect_receipt.compensated
                    ):
                        break
                    current_hash = project_workspace.sha256_file(absolute)
                    if current_hash != effect_receipt.before_sha256:
                        raise EffectReplayConflictError(
                            "Compensated patch receipt does not match its restored target hash"
                        )
                    previous_effect = effect_receipt
                    reapplies_effect_id = previous_effect.effect_id
                    application_generation = (
                        int(previous_effect.payload.get("application_generation") or 1) + 1
                    )
                    effect_key = (
                        f"{base_effect_key}:generation:{application_generation}:"
                        f"after:{previous_effect.compensated_by_effect_id}"
                    )
                    effect_receipt = effect_repository.get_effect_by_idempotency(
                        task_id,
                        effect_key,
                    )
            if effect_receipt is not None and effect_receipt.state in {
                EffectState.APPLIED,
                EffectState.RECONCILED,
            }:
                current_hash = project_workspace.sha256_file(absolute)
                if current_hash != effect_receipt.after_sha256:
                    raise EffectReplayConflictError(
                        "Uncompensated patch receipt does not match current file hash"
                    )
                expected_applied_sha256 = (
                    effect_receipt.expected_after_sha256 or effect_receipt.after_sha256
                )
                patch_result = patch_engine.PatchResult(
                    file_path=normalized,
                    absolute_path=absolute,
                    occurrences_before=0,
                    before_sha256=effect_receipt.before_sha256,
                    after_sha256=effect_receipt.after_sha256,
                )
            else:
                prepared_patch = _prepare_ticket_patch(root, normalized, patch)
                expected_applied_sha256 = prepared_patch.file_effect.after_sha256
                prepared_before = (
                    None
                    if prepared_patch.file_effect.expected_before is MUST_BE_ABSENT
                    else str(prepared_patch.file_effect.expected_before)
                )
                if effect_repository is not None and task_id:
                    try:
                        effect_payload: dict[str, Any] = {
                            "patch_sha256": patch_hash,
                            "workstream_id": workstream_id,
                        }
                        if application_generation > 1:
                            effect_payload.update(
                                {
                                    "application_generation": application_generation,
                                    "reapplies_effect_id": reapplies_effect_id,
                                }
                            )
                        effect_receipt = effect_repository.begin_effect(
                            task_id,
                            effect_key,
                            "file_patch",
                            normalized,
                            payload=effect_payload,
                            before_sha256=prepared_before,
                            expected_after_sha256=(prepared_patch.file_effect.after_sha256),
                            fencing_token=effect_fencing_token,
                        )
                    except BaseException:
                        project_workspace.discard_prepared_file(prepared_patch.file_effect)
                        raise
                if effect_receipt is not None and effect_receipt.state in {
                    EffectState.APPLIED,
                    EffectState.RECONCILED,
                }:
                    project_workspace.discard_prepared_file(prepared_patch.file_effect)
                    current_hash = project_workspace.sha256_file(absolute)
                    if current_hash != effect_receipt.after_sha256:
                        raise EffectReplayConflictError(
                            "Idempotent patch receipt does not match current file hash"
                        )
                    expected_applied_sha256 = (
                        effect_receipt.expected_after_sha256 or effect_receipt.after_sha256
                    )
                    patch_result = patch_engine.PatchResult(
                        normalized,
                        absolute,
                        prepared_patch.occurrences_before,
                        effect_receipt.before_sha256,
                        effect_receipt.after_sha256,
                    )
                else:
                    try:
                        write_result = fenced_mutation(
                            lambda: project_workspace.commit_prepared_file(
                                prepared_patch.file_effect
                            )
                        )
                    except project_workspace.ConcurrentModificationError as exc:
                        raise patch_engine.FileChangedError(str(exc)) from exc
                    except BaseException:
                        project_workspace.discard_prepared_file(prepared_patch.file_effect)
                        raise
                    patch_result = patch_engine.PatchResult(
                        normalized,
                        absolute,
                        prepared_patch.occurrences_before,
                        write_result.before_sha256,
                        write_result.after_sha256,
                    )
            before_sha256 = patch_result.before_sha256
            after_sha256 = patch_result.after_sha256
            expected_applied_sha256 = after_sha256
            if effect_receipt is not None and effect_receipt.state is EffectState.PENDING:
                effect_receipt = effect_repository.complete_effect(
                    effect_receipt.effect_id,
                    result={
                        "patch_sha256": patch_hash,
                        "occurrences_before": patch_result.occurrences_before,
                    },
                    after_sha256=after_sha256,
                    fencing_token=effect_fencing_token,
                )
                _emit(
                    on_event,
                    worker_base,
                    type="effect_applied",
                    effect_id=effect_receipt.effect_id,
                    effect_kind="file_patch",
                    idempotency_key=effect_receipt.idempotency_key,
                    before_sha256=before_sha256,
                    after_sha256=after_sha256,
                    file_path=normalized,
                )
            syntax_status, syntax_detail = safety.run_syntax_gate(absolute)
            if syntax_status == "failed":
                test_status, test_output = "not_run", ""
                sandbox_details = {
                    "actual_isolation": "none",
                    "isolation_details": "syntax gate failed before tests",
                }
                error = syntax_detail
            else:
                (
                    test_status,
                    test_output,
                    sandbox_details,
                ) = safety.run_sandbox_tests_detailed(
                    root,
                    test_cmd,
                    cancelled=cancelled,
                )
                if test_status == "not_configured" and defer_tests_to_integration:
                    test_status = "deferred"
                    test_scope = "integration"
                    test_output = "Deferred to the required post-review integration gate."
                sandbox_isolation = str(sandbox_details.get("actual_isolation") or "none")
                error = test_output if test_status == "failed" else ""

            gate_summary = f"syntax={syntax_status}; tests={test_status}"
            sandbox_outcome = str(sandbox_details.get("sandbox_outcome") or "")
            sandbox_blocked = sandbox_outcome in {"blocked", "unavailable"}
            sandbox_cancelled = bool(sandbox_details.get("cancelled"))
            _emit(
                on_event,
                tester_base,
                type="test_result",
                status=(
                    "blocked"
                    if sandbox_blocked
                    else "cancelled"
                    if sandbox_cancelled
                    else "failed"
                    if syntax_status == "failed" or test_status == "failed"
                    else "deferred"
                    if test_status == "deferred"
                    else "passed"
                ),
                accepted=not error,
                command=" ".join(test_cmd or []),
                test_scope=test_scope,
                detail=(error or gate_summary)[-12000:],
                file_path=normalized,
                requested_isolation=sandbox_details.get("requested_isolation"),
                actual_isolation=sandbox_details.get("actual_isolation"),
                isolation_details=sandbox_details.get("isolation_details"),
                sandbox_outcome=sandbox_details.get("sandbox_outcome"),
                sandbox_backend=sandbox_details.get("sandbox_backend"),
                blocked_reason=sandbox_details.get("blocked_reason"),
                cancelled=sandbox_details.get("cancelled"),
                timed_out=sandbox_details.get("timed_out"),
                output_truncated=sandbox_details.get("output_truncated"),
            )
            if error:
                rollback_with_receipt("machine_gate_failed")
                failure_kind = (
                    "sandbox_unavailable"
                    if sandbox_outcome == "unavailable"
                    else "sandbox_blocked"
                    if sandbox_outcome == "blocked"
                    else "cancelled"
                    if sandbox_cancelled
                    else "machine_gate"
                )
                current_effect_id = (
                    effect_receipt.effect_id if effect_receipt is not None else None
                )
                details = failure_fields(
                    failure_kind,
                    "tester",
                    error,
                    patch_sha256=patch_hash,
                    test={
                        "syntax_status": syntax_status,
                        "test_status": test_status,
                        "test_output": test_output,
                        "sandbox": sandbox_details,
                    },
                    effect_id=current_effect_id,
                )
                return TicketExecutionResult(
                    False,
                    normalized,
                    worker_feedback,
                    f"Fail: {gate_summary}; {error[:2000]}",
                    "",
                    "not_run",
                    details["remediation_prompt"],
                    patch_hash,
                    additions,
                    deletions,
                    syntax_status,
                    test_status,
                    test_output,
                    error[:2000],
                    failure_kind,
                    failure_kind == "machine_gate",
                    before_sha256=before_sha256,
                    after_sha256=after_sha256,
                    effect_id=current_effect_id,
                    sandbox_isolation=sandbox_isolation,
                    **details,
                )

            # The patch and machine gates are complete. Release the project
            # mutation lock while preparing context and waiting on Tester
            # inference; the write-scope claim still protects this target.
            lock.release()
            try:
                verification = (
                    "Verified"
                    if test_status == "passed"
                    else "Partially verified"
                    if syntax_status == "passed"
                    else "Unverified"
                )
                execution_result = f"{verification}: {gate_summary}"
                reviewer_prompt = (
                    f"## YÊU CẦU GỐC\n{task_goal}\n\n"
                    f"## WORKSTREAM\n{workstream_goal}\n\n"
                    f"## WORK ITEM\n{instructions}\n\n"
                    "## ACCEPTANCE CRITERIA\n- "
                    + "\n- ".join(str(item) for item in acceptance_criteria)
                    + f"\n\n## TEST FOCUS\n{test_focus or '(không chỉ định)'}\n\n"
                    f"## FILE\n{normalized}\n\n"
                    f"## PATCH WORKER\n{patch}\n\n"
                    f"## WORKER FEEDBACK\n{worker_feedback}\n\n"
                    f"## MACHINE EVIDENCE\n{execution_result}\n{test_output[-8000:]}"
                )
                _emit(
                    on_event,
                    tester_base,
                    type="agent_started",
                    status="running",
                    goal="Đánh giá độc lập patch và machine evidence",
                    prompt=reviewer_prompt,
                    file_path=normalized,
                    model=getattr(llm_client.thread_local, "reviewer_model", None),
                    effort=getattr(llm_client.thread_local, "reviewer_effort", None),
                )
                llm_client.thread_local.agent_role = "tester"
                llm_client.thread_local.agent_instance_id = tester_agent_id
                llm_client.thread_local.manager_id = manager_agent_id
                llm_client.thread_local.workstream_id = workstream_id
                llm_client.thread_local.work_item_id = work_item_id
                failure_actor = "tester"
                if tester_lock is not None:
                    tester_lock.acquire()
                try:
                    review = llm_call(REVIEW_PROMPT, reviewer_prompt, REVIEWER_TOOLS)
                finally:
                    if tester_lock is not None:
                        tester_lock.release()
            finally:
                # Re-enter before any rollback or accepted-result handoff.
                lock.acquire()
            failure_actor = "worker"
            if review.tool_name != "review_patch":
                raise RuntimeError(f"Tester action không hợp lệ: {review.tool_name}")
            verdict = str(review.tool_input.get("verdict", "revise"))
            feedback = str(review.tool_input.get("reviewer_feedback", ""))
            next_instructions = str(review.tool_input.get("next_instructions", ""))
            accepted = verdict == "approved"
            if not accepted:
                rollback_with_receipt("reviewer_revise")

            _emit(
                on_event,
                tester_base,
                type="review_result",
                status="passed" if accepted else "failed",
                accepted=accepted,
                verdict=verdict,
                reviewer_feedback=feedback,
                next_instructions=next_instructions,
                file_path=normalized,
            )
            _emit(
                on_event,
                tester_base,
                type="agent_message",
                role="agent",
                source_agent_id=tester_agent_id,
                target_agent_id=manager_agent_id,
                signal_type="review_result",
                summary=(f"Tester trả {verdict} cho {normalized}: {feedback or next_instructions}")[
                    :1000
                ],
            )
            worker_terminal_event: dict[str, Any] = {
                "type": "agent_completed" if accepted else "agent_failed",
                "status": "completed" if accepted else "failed",
                "accepted": accepted,
                "file_path": normalized,
            }
            if not accepted:
                worker_terminal_event.update(
                    error=(
                        feedback
                        or next_instructions
                        or "Reviewer requested revision"
                    ),
                    failure_kind="reviewer_revise",
                )
            _emit(on_event, worker_base, **worker_terminal_event)
            current_effect_id = (
                effect_receipt.effect_id if effect_receipt is not None else None
            )
            result_details = (
                failure_fields(
                    "reviewer_revise",
                    "tester",
                    feedback or next_instructions,
                    patch_sha256=patch_hash,
                    test={
                        "syntax_status": syntax_status,
                        "test_status": test_status,
                        "test_output": test_output,
                    },
                    review={
                        "verdict": verdict,
                        "feedback": feedback,
                        "next_instructions": next_instructions,
                    },
                    effect_id=current_effect_id,
                )
                if not accepted
                else {
                    "diagnostic_log_refs": diagnostic_refs(
                        "tester",
                        patch_sha256=patch_hash,
                        effect_id=current_effect_id,
                    )
                }
            )
            return TicketExecutionResult(
                accepted,
                normalized,
                worker_feedback,
                execution_result,
                feedback,
                verdict,
                (
                    next_instructions
                    if accepted
                    else result_details["remediation_prompt"]
                ),
                patch_hash,
                additions,
                deletions,
                syntax_status,
                test_status,
                test_output,
                "" if accepted else (feedback or next_instructions),
                "" if accepted else "reviewer_revise",
                retry_policy.retryable_for_failure("reviewer_revise") if not accepted else False,
                before_sha256=before_sha256,
                after_sha256=after_sha256,
                effect_id=current_effect_id,
                sandbox_isolation=sandbox_isolation,
                test_scope=test_scope,
                **result_details,
            )
    except Exception as exc:
        rollback_conflict = exc if isinstance(exc, safety.RollbackConflictError) else None
        if effect_receipt is not None and effect_receipt.state is EffectState.PENDING:
            try:
                observed_hash = project_workspace.sha256_file(absolute)
                if observed_hash != effect_receipt.expected_after_sha256:
                    effect_receipt = effect_repository.fail_effect(
                        effect_receipt.effect_id,
                        f"{type(exc).__name__}: {exc}",
                        fencing_token=effect_fencing_token,
                    )
            except Exception:
                pass
        if backup_hash and rollback_conflict is None:
            try:
                with lock:
                    rollback_with_receipt("backend_exception")
            except safety.RollbackConflictError as conflict:
                rollback_conflict = conflict
            except Exception:
                pass
        reported_error = rollback_conflict or exc
        retry_decision = retry_policy.classify_exception(reported_error)
        was_cancelled = retry_decision.failure_kind == "cancelled" or bool(
            cancelled is not None and cancelled()
        )
        failure_kind = (
            "rollback_conflict"
            if rollback_conflict is not None
            else "cancelled"
            if was_cancelled
            else retry_decision.failure_kind
        )
        current_effect_id = (
            effect_receipt.effect_id if effect_receipt is not None else None
        )
        details = failure_fields(
            failure_kind,
            failure_actor,
            f"{type(reported_error).__name__}: {reported_error}",
            patch_sha256=patch_hash,
            test={
                "syntax_status": syntax_status,
                "test_status": test_status,
                "test_output": test_output,
            },
            effect_id=current_effect_id,
        )
        _emit(
            on_event,
            tester_base if failure_actor == "tester" else worker_base,
            type="agent_cancelled" if was_cancelled else "agent_failed",
            status="cancelled" if was_cancelled else "failed",
            error=f"{type(reported_error).__name__}: {reported_error}",
            failure_kind=failure_kind,
            file_path=normalized,
        )
        return TicketExecutionResult(
            False,
            normalized,
            worker_feedback,
            f"Fail: {type(reported_error).__name__}: {reported_error}",
            "",
            "not_run",
            details["remediation_prompt"],
            patch_hash,
            additions,
            deletions,
            syntax_status,
            test_status,
            test_output,
            f"{type(reported_error).__name__}: {reported_error}",
            failure_kind,
            retry_policy.retryable_for_failure(failure_kind),
            before_sha256=before_sha256,
            after_sha256=after_sha256,
            effect_id=current_effect_id,
            sandbox_isolation=sandbox_isolation,
            test_scope=test_scope,
            **details,
        )
    finally:
        llm_client.thread_local.agent_role = "manager"
        llm_client.thread_local.agent_instance_id = manager_agent_id
