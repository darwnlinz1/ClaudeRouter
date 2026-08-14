# -*- coding: utf-8 -*-
"""The main per-project turn loop.

One call to `run_session` = the orchestrator repeatedly calling the model
until it either reports task_status == "completed", calls
`request_context` (which always pauses the loop for a human / next round
with more context), or hits MAX_TURNS as a safety valve.

`llm_call` is injected so tests can supply a scripted fake model without
touching the network -- see tests/test_orchestrator.py.
"""

from __future__ import annotations

import difflib
import hashlib
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import (
    config,
    error_utils,
    file_agent,
    llm_client,
    patch_engine,
    path_utils,
    safety,
    state_store,
)
from .context_builder import SourceFile, build_user_message
from .llm_client import ToolCallResult, call_agent
from .system_prompt_reviewer import SYSTEM_PROMPT as REVIEW_PROMPT
from .system_prompt_supervisor import SYSTEM_PROMPT as SUP_PROMPT
from .system_prompt_worker import SYSTEM_PROMPT as WORK_PROMPT
from .tools_schema import REVIEWER_TOOLS, SUPERVISOR_TOOLS, WORKER_TOOLS

logger = logging.getLogger("orchestrator")

LLMCallFn = Callable[[str, str, list[dict[str, Any]]], ToolCallResult]
EventSink = Callable[[dict[str, Any]], None]
ApprovedFileSink = Callable[[str], None]
CancellationCallback = Callable[[], bool]


@dataclass
class TurnOutcome:
    tool_name: str
    accepted: bool
    detail: str
    stop_loop: bool


@dataclass
class SessionResult:
    turns: list[TurnOutcome] = field(default_factory=list)
    final_state: dict[str, Any] = field(default_factory=dict)
    stopped_reason: str = ""


def _default_llm_call(
    system_prompt: str, user_message: str, tools: list[dict[str, Any]]
) -> ToolCallResult:
    return call_agent(system_prompt, user_message, tools)


def _validate_context_note(note: str) -> str:
    note = str(note or "").strip()
    if len(note) > config.CONTEXT_NOTE_MAX_CHARS:
        logger.warning(
            "context_note exceeds %d chars (%d) -- truncating for storage",
            config.CONTEXT_NOTE_MAX_CHARS,
            len(note),
        )
        return note[: config.CONTEXT_NOTE_MAX_CHARS]
    return note


def _validate_decision_entry(entry: Any) -> str | None:
    if entry is None:
        return None
    value = str(entry).strip()
    if not value:
        return None
    return value[:2000]


def _validate_worker_feedback(feedback: str) -> str:
    return str(feedback).strip()


def _validate_reviewer_text(value: str) -> str:
    return str(value).strip()


def _emit(on_event: EventSink | None, event: dict[str, Any]) -> None:
    if on_event is None:
        return
    try:
        on_event(event)
    except Exception:
        logger.exception("Không thể phát sự kiện orchestrator: %s", event.get("type"))


def _record_failed_attempt(state: dict[str, Any], approach_note: str, error_text: str) -> None:
    """Update attempted_approaches / last_error_hash / consecutive_error_count."""
    new_hash = error_utils.error_hash(error_text)
    if state.get("last_error_hash") == new_hash:
        state["consecutive_error_count"] = state.get("consecutive_error_count", 0) + 1
    else:
        state["consecutive_error_count"] = 1
    state["last_error_hash"] = new_hash
    state.setdefault("attempted_approaches", []).append(
        {"approach": approach_note, "error": error_text[:2000]}
    )
    state["attempted_approaches"] = state["attempted_approaches"][-20:]
    state["strategy_reset_required"] = (
        state["consecutive_error_count"] >= config.STRATEGY_RESET_THRESHOLD
    )


def _set_ticket_status(state: dict[str, Any], status: str, detail: str = "") -> None:
    ticket = state.get("active_ticket")
    if not isinstance(ticket, dict):
        return
    ticket = dict(ticket)
    ticket["status"] = status
    if detail:
        ticket["detail"] = str(detail)[:2000]
    state["active_ticket"] = ticket


def _record_completed_ticket(
    state: dict[str, Any],
    *,
    turn: int,
    file_path: str,
    summary: str,
    verification: str,
    reviewer_feedback: str,
) -> None:
    completed = list(state.get("completed_tickets") or [])
    completed.append(
        {
            "turn": turn,
            "file_path": file_path,
            "summary": summary[:1000],
            "verification": verification[:1000],
            "reviewer_feedback": reviewer_feedback,
            "status": "approved",
        }
    )
    state["completed_tickets"] = completed[-100:]
    state["active_ticket"] = None


def _context_manifest(sources: list[SourceFile]) -> list[dict[str, Any]]:
    manifest = []
    for source in sources:
        digest = None
        if source.absolute_path.is_file():
            hasher = hashlib.sha256()
            with source.absolute_path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(65536), b""):
                    hasher.update(chunk)
            digest = hasher.hexdigest()
        manifest.append(
            {
                "path": source.rel_path,
                "modifier": source.request_modifier,
                "sha256": digest,
            }
        )
    return manifest


def _extract_patch_content(work_result: ToolCallResult) -> str:
    """Read a patch from structured input, or from the Worker's XML response."""
    structured_patch = work_result.tool_input.get("patch_content", "")
    if structured_patch:
        return str(structured_patch)

    raw_content = str(work_result.raw_response.get("content", ""))
    match = re.search(r"<patch>[\s\S]*?</patch>", raw_content)
    return match.group(0) if match else ""


def _patch_line_stats(patch_content: str) -> tuple[int, int]:
    additions = 0
    deletions = 0
    pattern = r"<<<< SEARCH\r?\n(.*?)\r?\n====\r?\n(.*?)\r?\n>>>> REPLACE"
    for search_block, replace_block in re.findall(pattern, patch_content, re.DOTALL):
        for line in difflib.ndiff(search_block.splitlines(), replace_block.splitlines()):
            if line.startswith("+ "):
                additions += 1
            elif line.startswith("- "):
                deletions += 1
    if additions == 0 and deletions == 0:
        create_pattern = r"<<<< SEARCH\r?\n====\r?\n(.*?)\r?\n>>>> REPLACE"
        create_blocks = re.findall(create_pattern, patch_content, re.DOTALL)
        if len(create_blocks) == 1:
            additions = len(create_blocks[0].splitlines())
    return additions, deletions


def _rollback_safely(root: Path, backup_hash: str | None, file_path: str) -> None:
    if not backup_hash:
        return
    try:
        safety.rollback_to(root, backup_hash, file_path)
    except Exception:
        logger.exception("Rollback thất bại tại snapshot %s", backup_hash)


def _handle_request_context(
    paths: state_store.ProjectPaths,
    state: dict[str, Any],
    tool_input: dict[str, Any],
) -> TurnOutcome:
    reason = tool_input["reason"]
    context_note = _validate_context_note(tool_input.get("context_note", ""))
    decisions_entry = _validate_decision_entry(tool_input.get("decisions_md_entry"))

    if reason == "strategy_reset_needs_human" and not decisions_entry:
        context_note = (context_note + " [WARNING: missing required Rule E writeup]").strip()

    state["current_task"] = context_note
    if decisions_entry:
        state_store.append_decision(paths, decisions_entry)

    # Trong mô hình Supervisor, xin thêm file thì vòng lặp vẫn chạy tiếp (stop_loop=False)
    # để turn sau Supervisor có file mới và làm việc ngay, không cần pause lại đợi human.
    return TurnOutcome(
        tool_name="request_context",
        accepted=True,
        detail=f"Loaded extra files: {tool_input.get('files_needed', [])}",
        stop_loop=False,
    )


def run_session(
    root: Path,
    task_description: str,
    source_files: list[str],
    test_cmd: list[str] | None = None,
    allow_new_files: bool = False,
    resume_session: bool = False,
    max_turns: int = config.MAX_TURNS,
    llm_call: LLMCallFn = _default_llm_call,
    on_event: EventSink | None = None,
    on_file_approved: ApprovedFileSink | None = None,
    cancelled: CancellationCallback | None = None,
) -> SessionResult:
    """Multi-Agent Turn Loop: Supervisor (Quản lý) -> Worker (Thợ Code)."""
    root = root.resolve()
    paths = state_store.ProjectPaths.for_root(root)
    result = SessionResult()

    sources = []
    for p in source_files:
        # Bóc tách modifier (skeleton hoặc 10-50) nếu AI có gửi kèm
        parts = p.split(":", 1)
        path_utils.ensure_context_path_safe(parts[0])
        rel_path, absolute_path = path_utils.resolve_under_root(root, parts[0])
        modifier = parts[1] if len(parts) > 1 else ""
        sources.append(
            SourceFile(rel_path=rel_path, absolute_path=absolute_path, request_modifier=modifier)
        )

    persisted_state = state_store.load_state(paths)
    if resume_session:
        for item in persisted_state.get("context_manifest") or []:
            rel_path = item.get("path")
            modifier = item.get("modifier", "")
            if not rel_path:
                continue
            try:
                path_utils.ensure_context_path_safe(rel_path)
                normalized, absolute_path = path_utils.resolve_under_root(root, rel_path)
            except (path_utils.PathEscapeError, path_utils.SensitivePathError):
                continue
            if not any(
                source.rel_path == normalized and source.request_modifier == modifier
                for source in sources
            ):
                sources.append(
                    SourceFile(
                        rel_path=normalized,
                        absolute_path=absolute_path,
                        request_modifier=modifier,
                    )
                )

    previous_turn_count = int(persisted_state.get("turn_count", 0)) if resume_session else 0
    # Folder-only mode: local file agent exposes a path listing so Supervisor
    # can request_context / delegate without the user pre-picking every file.
    project_tree = file_agent.build_project_tree(root)
    for session_turn in range(1, max_turns + 1):
        if cancelled is not None and cancelled():
            result.final_state = state_store.load_state(paths)
            result.stopped_reason = "cancelled"
            return result
        nonretryable_gate_failure = ""
        turn = previous_turn_count + session_turn
        _emit(on_event, {"type": "turn_start", "turn": turn, "phase": "supervisor"})
        state = state_store.load_state(paths)
        if not resume_session and session_turn == 1:
            state["session_goal"] = task_description[:12000]
            state["active_ticket"] = None
            state["completed_tickets"] = []
        state["context_manifest"] = _context_manifest(sources)
        state["turn_count"] = turn

        rules_text = state_store.load_rules_text(paths)
        decisions_text = state_store.load_decisions_text(paths)

        built = build_user_message(
            task_description,
            rules_text,
            decisions_text,
            state,
            sources,
            project_tree=project_tree,
        )

        # =========================================================
        # 1. GỌI SUPERVISOR ĐỂ XIN CHỈ THỊ
        # =========================================================
        _emit(
            on_event,
            {
                "type": "agent_progress",
                "turn": turn,
                "role": "supervisor",
                "stage": "planning",
                "message": "Đang đọc checkpoint, project context và chọn action.",
            },
        )
        llm_client.thread_local.agent_role = "supervisor"
        sup_result = llm_call(SUP_PROMPT, built.user_message, SUPERVISOR_TOOLS)
        _emit(
            on_event,
            {
                "type": "agent_action",
                "turn": turn,
                "role": "supervisor",
                "action": sup_result.tool_name,
                "file_path": sup_result.tool_input.get("file_path"),
                "files_needed": sup_result.tool_input.get("files_needed", []),
            },
        )

        if sup_result.tool_name == "request_context":
            new_files = sup_result.tool_input.get("files_needed", [])
            try:
                resolved_sources = []
                for p in new_files:
                    parts = p.split(":", 1)
                    raw_path = parts[0]
                    modifier = parts[1] if len(parts) > 1 else ""
                    if allow_new_files:
                        path_utils.ensure_context_path_safe(raw_path)
                        rel_path, absolute_path = path_utils.resolve_under_root(root, raw_path)
                    else:
                        # Edit / folder-only: chỉ nạp file thật trên đĩa.
                        rel_path, absolute_path = file_agent.authorize_existing_file(root, raw_path)
                    resolved_sources.append(
                        SourceFile(
                            rel_path=rel_path,
                            absolute_path=absolute_path,
                            request_modifier=modifier,
                        )
                    )
            except (
                path_utils.PathEscapeError,
                path_utils.SensitivePathError,
                FileNotFoundError,
                IsADirectoryError,
                OSError,
            ) as exc:
                error_text = str(exc)
                state["last_execution_result"] = f"Fail: {error_text}"
                _record_failed_attempt(
                    state, "Supervisor request_context không an toàn", error_text
                )
                outcome = TurnOutcome("request_context", False, error_text, False)
            else:
                # Cập nhật danh sách file để turn sau Supervisor đọc.
                added_sources = []
                for source in resolved_sources:
                    if not any(
                        item.rel_path == source.rel_path
                        and item.request_modifier == source.request_modifier
                        for item in sources
                    ):
                        sources.append(source)
                        added_sources.append(source)
                if added_sources:
                    outcome = _handle_request_context(paths, state, sup_result.tool_input)
                else:
                    duplicate_paths = [source.rel_path for source in resolved_sources]
                    error_text = (
                        "Context đã được cấp quyền cho các path này: "
                        f"{duplicate_paths}. Không request_context lại; "
                        "hãy delegate_task cho Worker."
                    )
                    state["current_task"] = error_text
                    state["last_execution_result"] = f"Fail: {error_text}"
                    _record_failed_attempt(state, "Supervisor lặp request_context", error_text)
                    outcome = TurnOutcome("request_context", False, error_text, False)

        elif sup_result.tool_name == "delegate_task":
            # =========================================================
            # 2. KHỞI TẠO WORKER & GIAO VIỆC
            # =========================================================
            file_path, abs_path = path_utils.resolve_under_root(
                root, sup_result.tool_input["file_path"]
            )
            if file_path not in built.allowed_files:
                authorize_error = ""
                if allow_new_files and not abs_path.exists():
                    try:
                        path_utils.ensure_context_path_safe(file_path)
                    except (
                        path_utils.PathEscapeError,
                        path_utils.SensitivePathError,
                    ) as exc:
                        authorize_error = str(exc)
                    else:
                        sources.append(
                            SourceFile(
                                rel_path=file_path,
                                absolute_path=abs_path,
                                request_modifier="",
                            )
                        )
                elif abs_path.exists() and abs_path.is_file():
                    # Local file agent: pull existing on-disk file into context.
                    try:
                        file_path, abs_path = file_agent.authorize_existing_file(root, file_path)
                    except (
                        path_utils.PathEscapeError,
                        path_utils.SensitivePathError,
                        FileNotFoundError,
                        IsADirectoryError,
                    ) as exc:
                        authorize_error = str(exc)
                    else:
                        sources.append(
                            SourceFile(
                                rel_path=file_path,
                                absolute_path=abs_path,
                                request_modifier="",
                            )
                        )
                        _emit(
                            on_event,
                            {
                                "type": "file_fetched",
                                "turn": turn,
                                "file_path": file_path,
                                "detail": "Local file agent nạp file từ máy",
                            },
                        )
                else:
                    authorize_error = (
                        "Supervisor không được giao file ngoài context hiện tại: "
                        f"{file_path}. Dùng request_context để nạp file từ máy "
                        "hoặc kiểm tra path trong PROJECT TREE."
                    )
                if authorize_error:
                    state["last_worker_feedback"] = ""
                    state["last_execution_result"] = f"Fail: {authorize_error}"
                    _record_failed_attempt(state, "Kiểm tra quyền delegate_task", authorize_error)
                    outcome = TurnOutcome("delegate_task", False, authorize_error, False)
                    state_store.save_state(paths, state)
                    result.turns.append(outcome)
                    result.final_state = state
                    _emit(
                        on_event,
                        {
                            "type": "turn_end",
                            "turn": turn,
                            "accepted": False,
                            "detail": authorize_error,
                        },
                    )
                    continue
            instructions = sup_result.tool_input["instructions"]
            is_final_ticket = bool(sup_result.tool_input.get("is_final_ticket", False))
            context_note = _validate_context_note(sup_result.tool_input.get("context_note", ""))
            decisions_entry = _validate_decision_entry(
                sup_result.tool_input.get("decisions_md_entry")
            )
            state["active_ticket"] = {
                "turn": turn,
                "file_path": file_path,
                "instructions": instructions[:12000],
                "context_note": context_note,
                "is_final_ticket": is_final_ticket,
                "status": "delegated",
            }
            # Durable handoff checkpoint before another account/role is called.
            state_store.save_state(paths, state)

            file_content_str = (
                abs_path.read_text(encoding="utf-8")
                if abs_path.exists()
                else "[FILE MỚI TINH - CHƯA TỒN TẠI]"
            )

            worker_prompt = (
                f"## TARGET FILE: {file_path}\n"
                f"## INSTRUCTIONS TỪ SUPERVISOR:\n{instructions}\n\n"
                f"## MÃ NGUỒN HIỆN TẠI:\n```\n{file_content_str}\n```"
            )

            logger.info(f"👷 Đang giao task cho Worker: {file_path}...")

            # Gọi AI Thợ Code (Sử dụng WORK_PROMPT và WORKER_TOOLS)
            _emit(
                on_event,
                {
                    "type": "turn_phase",
                    "turn": turn,
                    "phase": "worker",
                    "role": "worker",
                    "file_path": file_path,
                },
            )
            _emit(
                on_event,
                {
                    "type": "agent_progress",
                    "turn": turn,
                    "role": "worker",
                    "stage": "patching",
                    "file_path": file_path,
                    "message": f"Đang tạo patch tối thiểu cho {file_path}.",
                },
            )
            llm_client.thread_local.agent_role = "worker"
            try:
                work_result = llm_call(WORK_PROMPT, worker_prompt, WORKER_TOOLS)
            finally:
                llm_client.thread_local.agent_role = "supervisor"
            if work_result.tool_name != "submit_patch":
                raise RuntimeError(f"Worker trả action không hợp lệ: {work_result.tool_name}")
            _emit(
                on_event,
                {
                    "type": "agent_action",
                    "turn": turn,
                    "role": "worker",
                    "action": work_result.tool_name,
                    "file_path": file_path,
                    "task_status": work_result.tool_input.get("task_status"),
                },
            )

            # Bóc tách dữ liệu JSON do Worker trả về
            patch_content = _extract_patch_content(work_result)
            task_status = work_result.tool_input.get("task_status", "in_progress")
            if task_status not in {"in_progress", "completed", "failed"}:
                raise RuntimeError(f"Worker task_status không hợp lệ: {task_status!r}")
            worker_feedback = _validate_worker_feedback(
                work_result.tool_input.get("worker_feedback", "")
            )
            patch_additions, patch_deletions = _patch_line_stats(patch_content)
            state["last_worker_feedback"] = worker_feedback
            state["last_execution_result"] = "Pending"
            state["last_reviewer_feedback"] = ""
            state["last_review_verdict"] = "pending"
            state["reviewer_next_instructions"] = ""
            logger.info("Worker feedback cho %s: %s", file_path, worker_feedback or "(trống)")
            if task_status == "failed":
                error_text = worker_feedback or "Worker báo không thể hoàn thành ticket."
                _set_ticket_status(state, "worker_failed", error_text)
                state["last_execution_result"] = f"Fail: {error_text}"
                _record_failed_attempt(
                    state, f"Worker từ chối/không thể sửa {file_path}", error_text
                )
                outcome = TurnOutcome(
                    "delegate_task",
                    False,
                    f"Worker failed: {error_text[:200]}",
                    False,
                )
                state_store.save_state(paths, state)
                result.turns.append(outcome)
                result.final_state = state
                _emit(
                    on_event,
                    {
                        "type": "execution_result",
                        "turn": turn,
                        "file_path": file_path,
                        "worker_feedback": worker_feedback,
                        "execution_result": state["last_execution_result"],
                        "accepted": False,
                        "detail": outcome.detail,
                        "additions": patch_additions,
                        "deletions": patch_deletions,
                    },
                )
                _emit(
                    on_event,
                    {
                        "type": "turn_end",
                        "turn": turn,
                        "tool": outcome.tool_name,
                        "accepted": False,
                        "detail": outcome.detail,
                    },
                )
                continue

            # =========================================================
            # 3. THỰC THI PATCH (Atomic Patching)
            # =========================================================
            backup_hash = None
            try:
                backup_hash = safety.backup_commit(
                    root, f"pre-patch snapshot: {file_path}", file_path
                )
                allowed_files = frozenset(set(built.allowed_files) | {file_path})
                # Kích hoạt engine cắt ghép mã
                patch_engine.apply_patch(root, file_path, patch_content, allowed_files)
                _set_ticket_status(state, "patched")
                if isinstance(state.get("active_ticket"), dict):
                    state["active_ticket"]["patch_sha256"] = hashlib.sha256(
                        patch_content.encode("utf-8")
                    ).hexdigest()
                    state["active_ticket"]["backup_commit"] = backup_hash
                state_store.save_state(paths, state)

                # Chạy kiểm tra cổng an toàn (Syntax Gate & Sandbox Test)
                syntax_status, syntax_detail = safety.run_syntax_gate(abs_path)
                if syntax_status == "failed":
                    test_status, tests_out = "not_run", ""
                    sandbox_details: dict[str, object] = {
                        "requested_isolation": "strong",
                        "actual_isolation": "none",
                        "sandbox_outcome": None,
                    }
                else:
                    (
                        test_status,
                        tests_out,
                        sandbox_details,
                    ) = safety.run_sandbox_tests_detailed(
                        root,
                        test_cmd,
                        cancelled=cancelled,
                    )
                gate_summary = f"syntax={syntax_status}; tests={test_status}"
                sandbox_outcome = str(sandbox_details.get("sandbox_outcome") or "")
                sandbox_blocked = sandbox_outcome in {
                    "blocked",
                    "unavailable",
                }
                sandbox_cancelled = bool(sandbox_details.get("cancelled"))
                if sandbox_blocked:
                    nonretryable_gate_failure = "sandbox_unavailable"
                elif sandbox_cancelled:
                    nonretryable_gate_failure = "cancelled"
                _emit(
                    on_event,
                    {
                        "type": "test_result",
                        "turn": turn,
                        "role": "reviewer",
                        "file_path": file_path,
                        "status": (
                            "blocked"
                            if sandbox_blocked
                            else "failed"
                            if syntax_status == "failed" or test_status == "failed"
                            else "passed"
                        ),
                        "accepted": (syntax_status != "failed" and test_status != "failed"),
                        "retryable": not (sandbox_blocked or sandbox_cancelled),
                        "failure_kind": (
                            "sandbox_unavailable"
                            if sandbox_outcome == "unavailable"
                            else "sandbox_blocked"
                            if sandbox_outcome == "blocked"
                            else ""
                        ),
                        "command": " ".join(test_cmd or []),
                        "detail": (syntax_detail if syntax_status == "failed" else tests_out)[
                            -12000:
                        ],
                        **sandbox_details,
                    },
                )

                if syntax_status == "failed" or test_status == "failed":
                    # Gãy test: Rollback ngay lập tức và báo cáo lỗi cho Supervisor
                    error_text = syntax_detail if syntax_status == "failed" else tests_out
                    state["last_worker_feedback"] = worker_feedback
                    state["last_execution_result"] = f"Fail: {gate_summary}; {error_text[:2000]}"
                    _set_ticket_status(state, "machine_gate_failed", error_text)
                    _record_failed_attempt(state, f"Worker sửa {file_path}", error_text)
                    _rollback_safely(root, backup_hash, file_path)
                    outcome = TurnOutcome(
                        "delegate_task", False, f"Test fail: {error_text[:200]}", False
                    )
                else:
                    # Gate không lỗi; chuyển trạng thái xác minh thật cho Reviewer.
                    state["last_worker_feedback"] = worker_feedback
                    if test_status == "passed":
                        verification_label = "Verified"
                    elif syntax_status == "passed":
                        verification_label = "Partially verified"
                    else:
                        verification_label = "Unverified"
                    state["last_execution_result"] = f"{verification_label}: {gate_summary}"
                    _set_ticket_status(
                        state,
                        "awaiting_review",
                        state["last_execution_result"],
                    )
                    state_store.save_state(paths, state)
                    _emit(
                        on_event,
                        {
                            "type": "turn_phase",
                            "turn": turn,
                            "phase": "reviewer",
                            "role": "reviewer",
                            "file_path": file_path,
                        },
                    )
                    _emit(
                        on_event,
                        {
                            "type": "agent_progress",
                            "turn": turn,
                            "role": "reviewer",
                            "stage": "reviewing",
                            "file_path": file_path,
                            "message": ("Đang đối chiếu patch, ticket và kết quả machine gate."),
                        },
                    )
                    reviewer_message = (
                        f"## YÊU CẦU GỐC\n{task_description}\n\n"
                        f"## CHỈ THỊ SUPERVISOR\n{instructions}\n\n"
                        f"## FILE\n{file_path}\n\n"
                        f"## PATCH WORKER\n{patch_content}\n\n"
                        f"## WORKER FEEDBACK\n{worker_feedback}\n\n"
                        "## EXECUTION RESULT\n"
                        f"{state['last_execution_result']}"
                    )
                    llm_client.thread_local.agent_role = "reviewer"
                    try:
                        review_result = llm_call(REVIEW_PROMPT, reviewer_message, REVIEWER_TOOLS)
                    finally:
                        llm_client.thread_local.agent_role = "supervisor"

                    if review_result.tool_name != "review_patch":
                        raise RuntimeError(
                            f"Reviewer trả action không hợp lệ: {review_result.tool_name}"
                        )
                    _emit(
                        on_event,
                        {
                            "type": "agent_action",
                            "turn": turn,
                            "role": "reviewer",
                            "action": review_result.tool_name,
                            "file_path": file_path,
                            "verdict": review_result.tool_input.get("verdict"),
                        },
                    )
                    verdict = review_result.tool_input.get("verdict")
                    if verdict not in {"approved", "revise"}:
                        raise RuntimeError(f"Reviewer verdict không hợp lệ: {verdict!r}")
                    reviewer_feedback = _validate_reviewer_text(
                        review_result.tool_input.get("reviewer_feedback", "")
                    )
                    next_instructions = _validate_reviewer_text(
                        review_result.tool_input.get("next_instructions", "")
                    )
                    state["last_reviewer_feedback"] = reviewer_feedback
                    state["last_review_verdict"] = verdict
                    state["reviewer_next_instructions"] = next_instructions

                    if verdict == "approved":
                        state["consecutive_error_count"] = 0
                        state["last_error_hash"] = None
                        state["strategy_reset_required"] = False
                        if decisions_entry:
                            state_store.append_decision(paths, decisions_entry)
                        if on_file_approved is not None:
                            on_file_approved(file_path)
                        _record_completed_ticket(
                            state,
                            turn=turn,
                            file_path=file_path,
                            summary=worker_feedback or context_note,
                            verification=state["last_execution_result"],
                            reviewer_feedback=reviewer_feedback,
                        )
                        stop_loop = task_status == "completed" and is_final_ticket
                        outcome = TurnOutcome(
                            "review_patch",
                            True,
                            f"Reviewer đã duyệt patch cho {file_path}: {reviewer_feedback}",
                            stop_loop,
                        )
                    else:
                        review_error = (
                            reviewer_feedback
                            or next_instructions
                            or "Reviewer yêu cầu sửa lại patch."
                        )
                        _record_failed_attempt(
                            state, f"Reviewer kiểm tra {file_path}", review_error
                        )
                        _set_ticket_status(state, "reviewer_revision", review_error)
                        _rollback_safely(root, backup_hash, file_path)
                        outcome = TurnOutcome(
                            "review_patch",
                            False,
                            f"Reviewer yêu cầu revise: {review_error[:200]}",
                            False,
                        )

                    _emit(
                        on_event,
                        {
                            "type": "review_result",
                            "turn": turn,
                            "file_path": file_path,
                            "verdict": verdict,
                            "reviewer_feedback": reviewer_feedback,
                            "next_instructions": next_instructions,
                        },
                    )

            except patch_engine.PatchRejected as exc:
                # Gãy cắt ghép (sai thụt lề, mất <patch>): Rollback và ghi lỗi
                state["last_worker_feedback"] = worker_feedback
                state["last_execution_result"] = f"Fail: {str(exc)[:2000]}"
                _record_failed_attempt(state, f"Worker bóc tách {file_path}", str(exc))
                _set_ticket_status(state, "patch_rejected", str(exc))
                _rollback_safely(root, backup_hash, file_path)
                outcome = TurnOutcome("delegate_task", False, f"Patch lỗi: {str(exc)}", False)
            except Exception as exc:
                # Lỗi Git/I/O/test runner vẫn phải được lưu để Supervisor chẩn đoán.
                error_text = f"{type(exc).__name__}: {exc}"
                state["last_worker_feedback"] = worker_feedback
                state["last_execution_result"] = f"Fail: {error_text[:2000]}"
                _record_failed_attempt(state, f"Backend xử lý {file_path}", error_text)
                _set_ticket_status(state, "backend_failed", error_text)
                _rollback_safely(root, backup_hash, file_path)
                logger.exception("Backend không thể xử lý patch cho %s", file_path)
                outcome = TurnOutcome(
                    "delegate_task", False, f"Backend fail: {error_text[:200]}", False
                )

            # Cập nhật trạng thái cho Task
            state["current_task"] = context_note or state.get("current_task")
            _emit(
                on_event,
                {
                    "type": "execution_result",
                    "turn": turn,
                    "file_path": file_path,
                    "worker_feedback": worker_feedback,
                    "execution_result": state["last_execution_result"],
                    "accepted": outcome.accepted,
                    "detail": outcome.detail,
                    "additions": patch_additions,
                    "deletions": patch_deletions,
                },
            )

        else:
            raise RuntimeError(f"Model called unrecognized tool: {sup_result.tool_name}")

        state_store.save_state(paths, state)
        result.turns.append(outcome)
        result.final_state = state

        logger.info("Turn %d: %s -> %s", turn, outcome.tool_name, outcome.detail)
        _emit(
            on_event,
            {
                "type": "turn_end",
                "turn": turn,
                "tool": outcome.tool_name,
                "accepted": outcome.accepted,
                "detail": outcome.detail,
            },
        )

        if nonretryable_gate_failure:
            result.stopped_reason = nonretryable_gate_failure
            return result
        if outcome.stop_loop:
            result.stopped_reason = "task_completed"
            return result

    result.stopped_reason = "max_turns_reached"
    return result
