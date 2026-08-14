# -*- coding: utf-8 -*-
from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import config, worker_targets


@dataclass(frozen=True)
class SourceFile:
    rel_path: str
    absolute_path: Path
    request_modifier: str = ""  # "", "skeleton", hoặc "10-50"


@dataclass(frozen=True)
class BuiltContext:
    user_message: str
    allowed_files: frozenset[str]


_CAPABILITY_REFUSAL_MARKERS = (
    "không có quyền truy cập",
    "không thể truy cập",
    "không thể chạy",
    "filesystem/venv",
    "sandbox claude",
    "phiên chat claude",
    "no access to",
    "cannot access",
    "can't access",
    "chat interface",
)


def _clean_checkpoint_text(value: Any, max_chars: int = 2000) -> str:
    text = str(value or "").strip()
    normalized = text.casefold()
    if any(marker in normalized for marker in _CAPABILITY_REFUSAL_MARKERS):
        return (
            "[Capability discussion omitted: the model only returns structured "
            "text; caller owns file I/O and machine verification.]"
        )
    return text[:max_chars]


def _state_for_prompt(state: dict[str, Any]) -> dict[str, Any]:
    """Keep operational checkpoint facts without replaying refusal transcripts."""
    active = state.get("active_ticket")
    active_summary = None
    if isinstance(active, dict):
        active_summary = {
            key: active.get(key)
            for key in (
                "turn",
                "file_path",
                "context_note",
                "is_final_ticket",
                "status",
                "patch_sha256",
            )
            if key in active
        }

    completed = []
    for ticket in list(state.get("completed_tickets") or [])[-30:]:
        if not isinstance(ticket, dict):
            continue
        completed.append(
            {
                key: (
                    _clean_checkpoint_text(value, 1000)
                    if key in {"summary", "verification", "reviewer_feedback"}
                    else value
                )
                for key, value in ticket.items()
                if key
                in {
                    "turn",
                    "file_path",
                    "summary",
                    "verification",
                    "reviewer_feedback",
                    "status",
                }
            }
        )

    attempted = [
        {"approach": str(item.get("approach", ""))[:300]}
        for item in list(state.get("attempted_approaches") or [])[-10:]
        if isinstance(item, dict) and item.get("approach")
    ]

    return {
        "checkpoint_revision": state.get("checkpoint_revision", 0),
        "current_task": _clean_checkpoint_text(state.get("current_task"), 2000),
        "active_ticket": active_summary,
        "completed_tickets": completed,
        "context_manifest": [
            {
                "path": item.get("path"),
                "modifier": item.get("modifier", ""),
            }
            for item in list(state.get("context_manifest") or [])
            if isinstance(item, dict) and item.get("path")
        ],
        "attempted_approaches": attempted,
        "last_error_hash": state.get("last_error_hash"),
        "last_worker_feedback": _clean_checkpoint_text(state.get("last_worker_feedback")),
        "last_execution_result": _clean_checkpoint_text(state.get("last_execution_result")),
        "last_reviewer_feedback": _clean_checkpoint_text(state.get("last_reviewer_feedback")),
        "last_review_verdict": state.get("last_review_verdict"),
        "reviewer_next_instructions": _clean_checkpoint_text(
            state.get("reviewer_next_instructions")
        ),
        "strategy_reset_required": bool(state.get("strategy_reset_required", False)),
        "consecutive_error_count": int(state.get("consecutive_error_count", 0)),
        "turn_count": int(state.get("turn_count", 0)),
    }


def _truncate_content(content: str, max_chars: int = config.MAX_FILE_CHARS) -> str:
    if len(content) <= max_chars:
        return content
    head_size = max_chars // 2
    tail_size = max_chars - head_size
    omitted = len(content) - max_chars
    return (
        content[:head_size]
        + f"\n\n[TRUNCATED -- {omitted} characters omitted]\n\n"
        + content[-tail_size:]
    )


def _truncate_bounded(content: str, max_chars: int) -> str:
    """Truncate to an actual hard bound, including the omission marker."""
    if len(content) <= max_chars:
        return content
    marker = "\n[TRUNCATED]\n"
    if max_chars <= len(marker):
        return marker[:max_chars]
    payload = max_chars - len(marker)
    head_size = payload // 2
    return content[:head_size] + marker + content[-(payload - head_size) :]


def _generate_skeleton(content: str) -> str:
    """Tạo sơ đồ xương cá (chỉ lấy tên Class và Hàm) bằng AST."""
    try:
        tree = ast.parse(content)
        skeleton = []
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                skeleton.append(f"class {node.name}:")
                for sub_node in node.body:
                    if isinstance(sub_node, ast.FunctionDef):
                        skeleton.append(f"    def {sub_node.name}(...): ...")
            elif isinstance(node, ast.FunctionDef):
                skeleton.append(f"def {node.name}(...): ...")

        if not skeleton:
            return "# Không tìm thấy Class hoặc Hàm nào ở root."
        return "\n".join(skeleton)
    except SyntaxError:
        return "# Không thể parse AST (File lỗi cú pháp hoặc không phải Python chuẩn)."


def build_source_blocks(
    files: list[SourceFile],
    *,
    max_chars_per_file: int | None = None,
    max_total_chars: int | None = None,
) -> tuple[str, frozenset[str]]:
    """Render source context with optional per-file and aggregate bounds."""
    blocks: list[str] = []
    allowed = set()
    per_file_limit = (
        config.MAX_FILE_CHARS if max_chars_per_file is None else max(1, int(max_chars_per_file))
    )

    for sf in files:
        allowed.add(sf.rel_path)
        if not sf.absolute_path.exists():
            blocks.append(
                f"### FILE: {sf.rel_path} [AUTHORIZED NEW FILE]\n"
                "[MISSING -- File chưa tồn tại nhưng path đã có trong allowed context. "
                "Không request_context lại; hãy delegate_task để Worker tạo file.]\n"
            )
            continue

        raw_content = worker_targets.read_prompt_text(sf.rel_path, sf.absolute_path)
        content = (
            _truncate_content(raw_content, per_file_limit)
            if max_chars_per_file is None
            else _truncate_bounded(raw_content, per_file_limit)
        )

        # Xử lý theo Modifier (skeleton, line-range, full)
        if sf.request_modifier == "skeleton":
            if sf.absolute_path.suffix == ".py":
                processed_content = _generate_skeleton(content)
                marker = " [SKELETON VIEW]"
            else:
                processed_content = "# Skeleton view chỉ hỗ trợ file Python."
                marker = " [ERROR]"

        elif "-" in sf.request_modifier:
            # Xử lý yêu cầu đọc theo dòng (Ví dụ: 10-50)
            try:
                start_str, end_str = sf.request_modifier.split("-")
                start, end = int(start_str), int(end_str)
                lines = content.splitlines()
                # Array index bắt đầu từ 0, dòng file bắt đầu từ 1
                slice_lines = lines[max(0, start - 1) : end]
                processed_content = "\n".join(slice_lines)
                marker = f" [LINES {start}-{end}]"
            except ValueError:
                processed_content = content
                marker = " [FULL FILE - Parse line range lỗi]"
        else:
            processed_content = content
            marker = " [FULL FILE]"

        blocks.append(f"### FILE: {sf.rel_path}{marker}\n```\n{processed_content}\n```\n")

    rendered = "\n".join(blocks)
    if max_total_chars is not None:
        rendered = _truncate_bounded(rendered, max(1, int(max_total_chars)))
    return rendered, frozenset(allowed)


def build_user_message(
    task_description: str,
    rules_text: str,
    decisions_text: str,
    state: dict[str, Any],
    source_files: list[SourceFile],
    project_tree: str = "",
) -> BuiltContext:
    import json

    source_blocks, allowed_files = build_source_blocks(source_files)
    state_json = json.dumps(
        _state_for_prompt(state),
        ensure_ascii=False,
        indent=2,
    )

    parts = [
        "## TASK\n" + task_description.strip(),
        "\n## rules.json\n```json\n" + rules_text.strip() + "\n```",
        "\n## DECISIONS.md\n```markdown\n" + decisions_text.strip() + "\n```",
        "\n## state.json\n```json\n" + state_json + "\n```",
    ]
    if project_tree.strip():
        parts.append(
            "\n## PROJECT TREE (đĩa máy — chỉ danh sách path)\n```\n"
            + project_tree.strip()
            + "\n```\n"
            + "User chỉ chọn thư mục gốc. Nội dung file chưa nằm trong "
            "SOURCE FILES: dùng `request_context` để backend nạp từ máy, "
            "hoặc `delegate_task` trực tiếp nếu đã chắc path tồn tại."
        )
    parts.append(
        "\n## SOURCE FILES\n" + (source_blocks or "(chưa nạp nội dung — dùng request_context)")
    )

    return BuiltContext(user_message="\n".join(parts), allowed_files=allowed_files)
