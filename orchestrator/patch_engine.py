# -*- coding: utf-8 -*-
"""Applies a submit_patch call to disk, with every guard re-checked in code.

Design note 3 is explicit that the prompt alone is not a safety boundary:
"prompt injection hoặc model lỗi vẫn có thể xảy ra" -- so every rule the
system prompt states in prose (protected files, allowed-file list, unique
anchor) is re-validated here against ground truth on disk, independent of
whatever the model claims.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from pathlib import Path

from . import config, path_utils, project_workspace
from .effects import MUST_BE_ABSENT


class PatchRejected(Exception):
    """Base class for all reasons a patch is refused before touching disk."""


class ProtectedFileError(PatchRejected):
    pass


class FileNotInContextError(PatchRejected):
    pass


class FileMissingError(PatchRejected):
    pass


class AnchorNotFoundError(PatchRejected):
    pass


class AnchorNotUniqueError(PatchRejected):
    def __init__(self, occurrences: int):
        self.occurrences = occurrences
        super().__init__(f"old_string appears {occurrences} times, must be exactly 1")


class FileChangedError(PatchRejected):
    """The file changed after the patch input was read."""


@dataclass(frozen=True)
class PatchResult:
    file_path: str
    absolute_path: Path
    occurrences_before: int
    before_sha256: str | None = None
    after_sha256: str | None = None


def validate_file_path(
    file_path: str,
    allowed_files: frozenset[str],
    resolved_file_path: str | None = None,
) -> None:
    """Raise if file_path is protected or wasn't part of what we showed the
    model this turn. This is Rule H and Rule I enforced in code."""
    protected = {item.casefold() for item in config.PROTECTED_FILES}
    candidates = {file_path.casefold(), Path(file_path).name.casefold()}
    if resolved_file_path:
        candidates.update(
            {
                resolved_file_path.casefold(),
                Path(resolved_file_path).name.casefold(),
            }
        )
    if candidates.intersection(protected):
        raise ProtectedFileError(
            f"'{file_path}' is a protected file and can never be a submit_patch target"
        )
    if file_path not in allowed_files:
        raise FileNotInContextError(
            f"'{file_path}' was not among the source files provided in this "
            "turn's context; refusing to patch a file the model invented"
        )


def _find_closest_match(file_content: str, search_block: str) -> str | None:
    file_lines = file_content.splitlines()
    search_lines = search_block.splitlines()
    window_size = len(search_lines)
    if window_size == 0 or len(file_lines) < window_size:
        return None
    best_ratio = 0.0
    best_candidate = None
    for i in range(len(file_lines) - window_size + 1):
        candidate = '\n'.join(file_lines[i:i + window_size])
        ratio = difflib.SequenceMatcher(None, search_block, candidate).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_candidate = candidate
    if best_ratio >= 0.4:
        return best_candidate
    return None


def apply_patch(
    root: Path,
    file_path: str,
    patch_content: str,
    allowed_files: frozenset[str],
) -> PatchResult:
    """Thay thế các khối mã dựa trên cú pháp SEARCH/REPLACE."""
    if not patch_content:
        raise PatchRejected("Lỗi: Không tìm thấy thẻ <patch> nào trong phản hồi của AI.")

    try:
        normalized_path, absolute_path = path_utils.resolve_under_root(root, file_path)
        normalized_allowed = frozenset(
            path_utils.normalize_rel_path(item) for item in allowed_files
        )
    except path_utils.PathEscapeError as exc:
        raise FileNotInContextError(str(exc)) from exc
    resolved_relative = absolute_path.relative_to(root.resolve()).as_posix()
    validate_file_path(
        normalized_path,
        normalized_allowed,
        resolved_file_path=resolved_relative,
    )

    if not absolute_path.exists():
        # Accept canonical empty SEARCH and the extra blank line some normalizers emit.
        create_pattern = (
            r"<<<< SEARCH\r?\n(?:\r?\n)?====\r?\n(.*?)(?:\r?\n)?>>>> REPLACE"
        )
        create_blocks = re.findall(create_pattern, patch_content, re.DOTALL)
        if len(create_blocks) != 1:
            raise FileMissingError(
                f"'{normalized_path}' chưa tồn tại; tạo file mới yêu cầu đúng một "
                "khối SEARCH rỗng (<<<< SEARCH\\n====\\n<nội dung>\\n>>>> REPLACE)"
            )
        try:
            write_result = project_workspace.atomic_write_text(
                absolute_path,
                create_blocks[0],
                encoding="utf-8",
                expected_before=MUST_BE_ABSENT,
            )
        except project_workspace.ConcurrentModificationError as exc:
            raise FileChangedError(str(exc)) from exc
        return PatchResult(
            file_path=normalized_path,
            absolute_path=absolute_path,
            occurrences_before=0,
            before_sha256=write_result.before_sha256,
            after_sha256=write_result.after_sha256,
        )

    # Read and hash the exact bytes used to derive the replacement.
    original_content = absolute_path.read_bytes()
    expected_before = project_workspace.sha256_bytes(original_content)
    file_content = (
        original_content.decode("utf-8")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    # Dùng Regex lấy ra các cặp khối SEARCH và REPLACE
    pattern = r"<<<< SEARCH\r?\n(.*?)\r?\n====\r?\n(.*?)(?:\r?\n)?>>>> REPLACE"
    blocks = re.findall(pattern, patch_content, re.DOTALL)

    if not blocks:
        raise PatchRejected("Lỗi: Không tìm thấy cấu trúc `<<<< SEARCH ==== REPLACE >>>>` hợp lệ bên trong thẻ <patch>.")

    occurrences_before = 0

    # Xử lý cắt ghép từng khối
    for search_block, replace_block in blocks:
        occurrences = file_content.count(search_block)

        # Sanity Check 1: Lùi lề sai hoặc AI bịa code
        if occurrences == 0:
            preview = search_block[:100] + "..." if len(search_block) > 100 else search_block
            base_msg = f"Không tìm thấy đoạn code SEARCH trong file gốc (Có thể do lùi lề sai). Trích đoạn: {preview!r}"
            closest = _find_closest_match(file_content, search_block)
            if closest is not None:
                base_msg += (
                    "\n\n🔍 GỢI Ý DEBUG: Bạn có vẻ sai lùi lề/khoảng trắng. "
                    "Đoạn code THỰC SỰ trong file gốc trông như thế này:\n---\n"
                    f"{closest}\n---\nHãy copy chính xác khoảng trắng/thụt lề từ đoạn trên."
                )
            raise AnchorNotFoundError(base_msg)

        # Sanity Check 2: Tính duy nhất
        elif occurrences > 1:
            raise AnchorNotUniqueError(occurrences)

        # Cắt ghép (Search & Replace) an toàn
        file_content = file_content.replace(search_block, replace_block)
        occurrences_before += occurrences

    # Flush a same-directory temporary file before atomically replacing target.
    try:
        write_result = project_workspace.atomic_write_text(
            absolute_path,
            file_content,
            encoding="utf-8",
            expected_before=expected_before,
        )
    except project_workspace.ConcurrentModificationError as exc:
        raise FileChangedError(str(exc)) from exc

    return PatchResult(
        file_path=normalized_path,
        absolute_path=absolute_path,
        occurrences_before=occurrences_before,
        before_sha256=write_result.before_sha256,
        after_sha256=write_result.after_sha256,
    )