# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import config

DEFAULT_STATE: dict[str, Any] = {
    "checkpoint_revision": 0,
    "session_goal": "",
    "current_task": None,
    "active_ticket": None,
    "completed_tickets": [],
    "context_manifest": [],
    "attempted_approaches": [],
    "last_error_hash": None,
    "last_worker_feedback": "",
    "last_execution_result": None,
    "last_reviewer_feedback": "",
    "last_review_verdict": None,
    "reviewer_next_instructions": "",
    "strategy_reset_required": False,
    "consecutive_error_count": 0,
    "turn_count": 0,
    "last_updated": None,
}


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    rules: Path
    decisions: Path
    state: Path

    @classmethod
    def for_root(cls, root: Path) -> "ProjectPaths":
        root = root.resolve()
        return cls(
            root=root,
            rules=root / config.RULES_FILENAME,
            decisions=root / config.DECISIONS_FILENAME,
            state=root / config.STATE_FILENAME,
        )


def load_rules_text(paths: ProjectPaths) -> str:
    if not paths.rules.exists():
        return "{}"
    return paths.rules.read_text(encoding="utf-8")


def load_decisions_text(paths: ProjectPaths) -> str:
    if not paths.decisions.exists():
        return "# DECISIONS.md\n\n(no decisions recorded yet)\n"
    return paths.decisions.read_text(encoding="utf-8")


def load_state(paths: ProjectPaths) -> dict[str, Any]:
    if not paths.state.exists():
        return deepcopy(DEFAULT_STATE)
    try:
        data = json.loads(paths.state.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        data = {}
    merged = deepcopy(DEFAULT_STATE)
    merged.update(data)
    return merged


def save_state(paths: ProjectPaths, state: dict[str, Any]) -> None:
    next_revision = int(state.get("checkpoint_revision", 0)) + 1
    state["checkpoint_revision"] = next_revision
    state = dict(state)
    state["last_updated"] = datetime.now(timezone.utc).isoformat()
    temp_path = paths.state.with_suffix(paths.state.suffix + ".tmp")
    temp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp_path, paths.state)


def append_decision(paths: ProjectPaths, entry: str, max_entries: int = 5) -> None:
    """Ghi vào DECISIONS.md nhưng CHỈ GIỮ LẠI `max_entries` quyết định gần nhất."""
    if not entry or not entry.strip():
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_block = f"## {timestamp}\n{entry.strip()}\n"

    header = "# DECISIONS.md\n"
    existing_blocks = []

    if paths.decisions.exists():
        content = paths.decisions.read_text(encoding="utf-8")
        # Tìm tất cả các khối bắt đầu bằng ## (định dạng timestamp)
        blocks = re.split(r"\n(?=## )", content)
        if blocks:
            header = blocks[0].strip() + "\n\n" if not blocks[0].startswith("##") else header
            existing_blocks = [b.strip() for b in blocks if b.startswith("##")]

    # Thêm block mới vào cuối danh sách
    existing_blocks.append(new_block.strip())

    # Cắt xén (Rolling Window) - Chỉ lấy N blocks cuối cùng
    kept_blocks = existing_blocks[-max_entries:]

    # Ghi lại toàn bộ file
    final_content = header + "\n\n".join(kept_blocks) + "\n"
    paths.decisions.write_text(final_content, encoding="utf-8")
