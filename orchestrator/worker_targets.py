"""Worker target and prompt-source classification.

Workers produce UTF-8 source-like text. Runtime code may produce binary
artifacts, but those artifacts must never be sent to a Worker as editable
targets or loaded into model prompts.
"""
from __future__ import annotations

import codecs
import os
from dataclasses import dataclass
from pathlib import Path

from . import path_utils

_RUNTIME_ONLY_SUFFIXES = frozenset(
    {
        ".3gp",
        ".7z",
        ".aac",
        ".accdb",
        ".avi",
        ".avif",
        ".bmp",
        ".bz2",
        ".db",
        ".duckdb",
        ".eot",
        ".flac",
        ".gif",
        ".gz",
        ".heic",
        ".ico",
        ".jpeg",
        ".jpg",
        ".m4a",
        ".mdb",
        ".mkv",
        ".mov",
        ".mp3",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".ogg",
        ".otf",
        ".png",
        ".rar",
        ".sqlite",
        ".sqlite3",
        ".svg",
        ".svgz",
        ".tar",
        ".tgz",
        ".tif",
        ".tiff",
        ".ttf",
        ".wav",
        ".webm",
        ".webp",
        ".woff",
        ".woff2",
        ".xz",
        ".zip",
    }
)

_BINARY_SUFFIXES = frozenset(
    {
        ".a",
        ".bin",
        ".class",
        ".dll",
        ".dylib",
        ".exe",
        ".iso",
        ".o",
        ".obj",
        ".parquet",
        ".pdf",
        ".pickle",
        ".pkl",
        ".pyc",
        ".pyd",
        ".so",
        ".wasm",
    }
)

_TEXT_SUFFIXES = frozenset(
    {
        ".astro",
        ".bat",
        ".c",
        ".cfg",
        ".cmd",
        ".cmake",
        ".conf",
        ".cpp",
        ".cs",
        ".css",
        ".csv",
        ".env",
        ".gitattributes",
        ".gitignore",
        ".go",
        ".gql",
        ".graphql",
        ".gradle",
        ".h",
        ".hpp",
        ".htm",
        ".html",
        ".ini",
        ".ipynb",
        ".java",
        ".j2",
        ".jinja",
        ".jinja2",
        ".js",
        ".json",
        ".jsx",
        ".kt",
        ".kts",
        ".lock",
        ".lua",
        ".md",
        ".mdc",
        ".mjs",
        ".php",
        ".prisma",
        ".properties",
        ".proto",
        ".ps1",
        ".py",
        ".pyi",
        ".r",
        ".rb",
        ".rs",
        ".rst",
        ".scss",
        ".sh",
        ".sql",
        ".svelte",
        ".swift",
        ".template",
        ".tex",
        ".tmpl",
        ".toml",
        ".ts",
        ".tsx",
        ".txt",
        ".vue",
        ".xml",
        ".yaml",
        ".yml",
    }
)

_TEXT_BASENAMES = frozenset(
    {
        ".editorconfig",
        ".dockerignore",
        ".eslintignore",
        ".flake8",
        ".gitignore",
        ".gitkeep",
        ".npmrc",
        ".prettierignore",
        ".prettierrc",
        ".stylelintrc",
        "cmakelists.txt",
        "dockerfile",
        "gemfile",
        "gradle",
        "license",
        "makefile",
        "procfile",
        "readme",
    }
)


class UnsupportedWorkerTarget(ValueError):
    """Raised before inference when a Worker target cannot be edited as UTF-8 text."""

    def __init__(self, path: str, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"Unsupported Worker target {path!r}: {reason}")


class DirectoryWorkerTarget(ValueError):
    """A planner named a directory; expand it instead of treating it as unsupported."""

    def __init__(self, path: str, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"Directory Worker target {path!r}: {reason}")


class UnsupportedPromptSource(ValueError):
    """Raised before file bytes can be included in a model prompt."""

    def __init__(self, path: str, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"Unsupported prompt source {path!r}: {reason}")


@dataclass(frozen=True, slots=True)
class WorkerTargetClassification:
    allowed: bool
    reason: str
    runtime_only: bool = False
    is_directory: bool = False


def _normalized_suffixes(path: str) -> tuple[str, ...]:
    return tuple(suffix.casefold() for suffix in Path(path).suffixes)


def is_directory_shaped_target(path: str) -> bool:
    """Return whether a declared target is a directory or directory wildcard."""
    normalized = str(path).strip().replace("\\", "/")
    return bool(normalized) and (
        normalized.endswith("/") or normalized.endswith("/*") or normalized.endswith("/**")
    )


def directory_target_stem(path: str) -> str:
    """Strip trailing slashes and ``/*`` / ``/**`` wildcards from a directory target."""
    normalized = str(path).strip().replace("\\", "/")
    if normalized.endswith("/**") or normalized.endswith("/*"):
        normalized = normalized.rsplit("/", 1)[0]
    return normalized.rstrip("/")


def is_possible_directory_target(path: str) -> bool:
    """Return whether a planner path may name a directory rather than a file.

    Trailing slashes and wildcards are definitive. A suffix-less path that is
    not a known text basename (``Makefile``, ``LICENSE``, …) is also treated as
    a possible directory so ``tests/fixtures`` can be expanded on disk.
    """
    if is_directory_shaped_target(path):
        return True
    normalized = str(path).strip().replace("\\", "/")
    if not normalized:
        return False
    name = Path(normalized).name.casefold()
    if name in _TEXT_BASENAMES or name.startswith(".env"):
        return False
    return not _normalized_suffixes(normalized)


def is_on_disk_directory_target(root: Path, path: str) -> bool:
    """Return whether ``path`` resolves to a real directory under ``root``."""
    stem = directory_target_stem(path) or str(path).strip().replace("\\", "/")
    if not stem:
        return False
    try:
        _normalized, absolute = path_utils.resolve_under_root(root, stem)
    except path_utils.PathEscapeError:
        return False
    return absolute.exists() and absolute.is_dir() and not absolute.is_symlink()


def expand_directory_worker_target(
    root: Path,
    path: str,
    *,
    max_files: int,
) -> list[str]:
    """Expand a directory target into concrete UTF-8 text files under ``root``."""
    if max_files < 1:
        return []
    stem = directory_target_stem(path)
    if not stem:
        return []
    try:
        _normalized, absolute = path_utils.resolve_under_root(root, stem)
    except path_utils.PathEscapeError:
        return []
    if not absolute.exists() or not absolute.is_dir() or absolute.is_symlink():
        return []

    from .file_agent import _SKIP_DIR_NAMES

    files: list[str] = []
    root_resolved = root.resolve()
    for current, dirnames, filenames in os.walk(absolute, followlinks=False):
        current_path = Path(current)
        if current_path.is_symlink():
            dirnames[:] = []
            continue
        dirnames[:] = [
            name
            for name in sorted(dirnames)
            if name not in _SKIP_DIR_NAMES and not (current_path / name).is_symlink()
        ]
        for name in sorted(filenames):
            child = current_path / name
            if child.is_symlink() or not child.is_file():
                continue
            rel = child.relative_to(root_resolved).as_posix()
            try:
                path_utils.ensure_context_path_safe(rel)
            except path_utils.SensitivePathError:
                continue
            classification = classify_declared_worker_target(rel)
            if not classification.allowed:
                continue
            files.append(rel)
            if len(files) >= max_files:
                return files
    return files


def is_runtime_only_artifact(path: str) -> bool:
    """Return whether runtime code, rather than a Worker patch, must create it."""
    return any(suffix in _RUNTIME_ONLY_SUFFIXES for suffix in _normalized_suffixes(path))


def classify_declared_worker_target(path: str) -> WorkerTargetClassification:
    """Classify a planned target without touching the filesystem."""
    normalized = str(path).strip().replace("\\", "/")
    if not normalized:
        return WorkerTargetClassification(False, "target is not a concrete file")
    if is_directory_shaped_target(normalized):
        return WorkerTargetClassification(
            False,
            "directory target must be expanded into concrete files",
            is_directory=True,
        )
    suffixes = _normalized_suffixes(normalized)
    if any(suffix in _RUNTIME_ONLY_SUFFIXES for suffix in suffixes):
        return WorkerTargetClassification(
            False,
            "binary/runtime artifact must be created by runtime code",
            runtime_only=True,
        )
    if any(suffix in _BINARY_SUFFIXES for suffix in suffixes):
        return WorkerTargetClassification(False, "binary file type is not patchable text")
    name = Path(normalized).name.casefold()
    if (
        name in _TEXT_BASENAMES
        or name.startswith(".env")
        or (suffixes and suffixes[-1] in _TEXT_SUFFIXES)
    ):
        return WorkerTargetClassification(True, "supported UTF-8 text target")
    return WorkerTargetClassification(
        False,
        "only source, text, config, documentation, script, or .gitkeep targets are allowed",
    )


def _read_utf8_bytes(path: Path, *, label: str) -> bytes:
    data = path.read_bytes()
    if b"\x00" in data:
        raise ValueError(f"{label} contains NUL bytes")
    try:
        codecs.getincrementaldecoder("utf-8")(errors="strict").decode(data, final=True)
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} is not valid UTF-8") from exc
    return data


def ensure_worker_target(path: str, *, absolute_path: Path | None = None) -> str:
    """Require a source-like target and strict UTF-8 for an existing file."""
    normalized = str(path).strip().replace("\\", "/")
    classification = classify_declared_worker_target(normalized)
    if classification.is_directory:
        raise DirectoryWorkerTarget(normalized, classification.reason)
    if (
        absolute_path is not None
        and absolute_path.exists()
        and absolute_path.is_dir()
        and not absolute_path.is_symlink()
    ):
        raise DirectoryWorkerTarget(
            normalized,
            "directory target must be expanded into concrete files",
        )
    if not classification.allowed:
        raise UnsupportedWorkerTarget(normalized, classification.reason)
    if absolute_path is not None and absolute_path.exists():
        if not absolute_path.is_file():
            raise UnsupportedWorkerTarget(normalized, "target is not a regular file")
        try:
            _read_utf8_bytes(absolute_path, label="existing target")
        except ValueError as exc:
            raise UnsupportedWorkerTarget(normalized, str(exc)) from exc
    return normalized


def read_prompt_text(path: str, absolute_path: Path) -> str:
    """Read strict UTF-8 text after excluding all binary/runtime source types."""
    normalized = str(path).strip().replace("\\", "/")
    suffixes = _normalized_suffixes(normalized)
    if any(suffix in _RUNTIME_ONLY_SUFFIXES | _BINARY_SUFFIXES for suffix in suffixes):
        raise UnsupportedPromptSource(normalized, "binary/runtime artifacts are never prompt bytes")
    try:
        data = _read_utf8_bytes(absolute_path, label="prompt source")
    except ValueError as exc:
        raise UnsupportedPromptSource(normalized, str(exc)) from exc
    return data.decode("utf-8")
