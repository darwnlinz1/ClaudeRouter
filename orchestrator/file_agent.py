# -*- coding: utf-8 -*-
"""Local machine-side file agent for folder-only orchestration.

The user only selects a project root. Agents ask for files via
`request_context` / `delegate_task`; this module discovers and loads
paths from disk under that root.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path

from . import path_utils

_SKIP_DIR_NAMES = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "env",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
    "dist",
    "build",
    ".idea",
    ".vscode",
    "cookies",
    ".aws",
    ".ssh",
    ".gnupg",
}


def build_project_tree(
    root: Path,
    *,
    max_entries: int = 250,
    max_depth: int = 12,
    max_path_length: int = 512,
    max_scanned_entries: int | None = None,
) -> str:
    """Return a compact relative path listing for Supervisor orientation."""
    if max_entries < 1:
        raise ValueError("max_entries must be positive")
    if max_depth < 0:
        raise ValueError("max_depth must be non-negative")
    if max_path_length < 1:
        raise ValueError("max_path_length must be positive")
    scan_limit = (
        max(max_entries, max_entries * 8)
        if max_scanned_entries is None
        else max_scanned_entries
    )
    if scan_limit < 1:
        raise ValueError("max_scanned_entries must be positive")

    root = root.resolve()
    if not root.is_dir():
        return "(project root không tồn tại)"

    entries: list[str] = []
    truncated = False
    scanned = 0

    def is_reparse(entry: os.DirEntry[str]) -> bool:
        try:
            attributes = int(
                getattr(entry.stat(follow_symlinks=False), "st_file_attributes", 0)
            )
        except OSError:
            return True
        return bool(
            entry.is_symlink()
            or attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        )

    def scan(directory: Path, relative_directory: Path, depth: int) -> bool:
        nonlocal scanned, truncated
        if depth > max_depth:
            truncated = True
            return False
        children: list[os.DirEntry[str]] = []
        scan_limit_hit = False
        try:
            with os.scandir(directory) as iterator:
                for child in iterator:
                    if scanned >= scan_limit:
                        truncated = True
                        scan_limit_hit = True
                        break
                    scanned += 1
                    children.append(child)
        except OSError:
            return True
        children.sort(key=lambda item: (item.name.casefold(), item.name))
        try:
            for child in children:
                if child.name in _SKIP_DIR_NAMES or is_reparse(child):
                    continue
                relative = relative_directory / child.name
                relative_text = relative.as_posix()
                if len(relative_text) > max_path_length:
                    truncated = True
                    continue
                try:
                    is_directory = child.is_dir(follow_symlinks=False)
                except OSError:
                    continue
                if is_directory:
                    if depth >= max_depth:
                        truncated = True
                        continue
                    if not scan(Path(child.path), relative, depth + 1):
                        return False
                    continue
                try:
                    if not child.is_file(follow_symlinks=False):
                        continue
                    path_utils.ensure_context_path_safe(relative_text)
                except (OSError, path_utils.SensitivePathError):
                    continue
                entries.append(relative_text)
                if len(entries) >= max_entries:
                    truncated = True
                    return False
        except OSError:
            return True
        return not scan_limit_hit

    scan(root, Path(), 0)

    if not entries:
        return "(thư mục trống hoặc chỉ có file bị loại)"
    suffix = "\n... (đã cắt bớt danh sách)" if truncated else ""
    return "\n".join(entries) + suffix


def authorize_existing_file(
    root: Path, rel_path: str
) -> tuple[str, Path]:
    """Validate and resolve an existing project file for agent use."""
    path_utils.ensure_context_path_safe(rel_path)
    normalized, absolute = path_utils.resolve_under_root(root, rel_path)
    if not absolute.exists():
        raise FileNotFoundError(
            f"File chưa tồn tại trên máy: {normalized}"
        )
    if not absolute.is_file():
        raise IsADirectoryError(
            f"Path không phải file: {normalized}"
        )
    return normalized, absolute
