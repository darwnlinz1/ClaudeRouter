# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path, PurePosixPath


class PathEscapeError(ValueError):
    """Raised when a relative path resolves outside the selected project."""


class SensitivePathError(ValueError):
    """Raised when a file must not be sent to an external model."""


_SENSITIVE_BASENAMES = {
    ".env",
    ".netrc",
    ".npmrc",
    ".pypirc",
    "credentials.json",
    "service-account.json",
    "service_account.json",
    "secrets.json",
    "token.json",
    "id_rsa",
    "id_ed25519",
}
_SENSITIVE_SUFFIXES = {".pem", ".key", ".p12", ".pfx"}
_SENSITIVE_DIRECTORIES = {"cookies", ".git", ".aws", ".ssh", ".gnupg"}
_SAFE_ENV_TEMPLATES = {".env.example", ".env.sample", ".env.template"}


def normalize_rel_path(path: str) -> str:
    raw = str(path).strip().replace("\\", "/")
    if not raw or "\x00" in raw:
        raise PathEscapeError("Đường dẫn rỗng hoặc chứa ký tự NUL.")
    candidate = PurePosixPath(raw)
    if candidate.is_absolute() or candidate.drive:
        raise PathEscapeError(f"Chỉ chấp nhận đường dẫn tương đối: {path!r}")
    normalized = candidate.as_posix()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    if normalized in {"", "."}:
        raise PathEscapeError("Đường dẫn phải trỏ tới một file.")
    return normalized


def resolve_under_root(root: Path, rel_path: str) -> tuple[str, Path]:
    root = root.resolve()
    normalized = normalize_rel_path(rel_path)
    lexical_path = root / normalized
    current = root
    for part in PurePosixPath(normalized).parts:
        current = current / part
        is_junction = getattr(current, "is_junction", lambda: False)
        if current.is_symlink() or is_junction():
            raise PathEscapeError(
                f"Không chấp nhận symlink/junction trong đường dẫn: {rel_path!r}"
            )
    resolved = lexical_path.resolve()
    if resolved != root and root not in resolved.parents:
        raise PathEscapeError(f"Đường dẫn thoát khỏi project root: {rel_path!r}")
    return normalized, resolved


def ensure_context_path_safe(rel_path: str) -> str:
    """Reject common credential material before placing it in an LLM prompt."""
    normalized = normalize_rel_path(rel_path)
    candidate = PurePosixPath(normalized)
    lowered_parts = {part.lower() for part in candidate.parts}
    basename = candidate.name.lower()
    suffix = candidate.suffix.lower()
    if (
        lowered_parts.intersection(_SENSITIVE_DIRECTORIES)
        or basename in _SENSITIVE_BASENAMES
        or (
            basename.startswith(".env.")
            and basename not in _SAFE_ENV_TEMPLATES
        )
        or suffix in _SENSITIVE_SUFFIXES
    ):
        raise SensitivePathError(
            f"Không được gửi file nhạy cảm vào context AI: {normalized!r}"
        )
    return normalized
