# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import stat
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from . import config, effects, path_utils, project_workspace, redaction

ARTIFACTS_ROOT = Path(
    os.environ.get(
        "ORCH_ARTIFACTS_DIR",
        str(Path.home() / ".ai_orchestrator" / "artifacts"),
    )
).expanduser()

_lock = threading.RLock()
_registration_callback: Callable[..., object] | None = None
logger = logging.getLogger(__name__)
_INTERNAL_NAMES = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    config.STATE_FILENAME,
    config.RULES_FILENAME,
    config.DECISIONS_FILENAME,
}
_INTERNAL_NAMES_CASEFOLDED = {name.casefold() for name in _INTERNAL_NAMES}


def configure_registration_callback(
    callback: Callable[..., object] | None,
) -> None:
    """Configure the optional production ``record_artifact`` callback."""

    global _registration_callback
    with _lock:
        _registration_callback = callback


def configure_repository(repository: object | None) -> None:
    """Use a repository's artifact registration hook when it exposes one."""

    callback = getattr(repository, "record_artifact", None)
    configure_registration_callback(callback if callable(callback) else None)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_internal_path(relative: Path) -> bool:
    return any(part.casefold() in _INTERNAL_NAMES_CASEFOLDED for part in relative.parts)


def _rmtree_onerror(function: Any, path: str, _error: Any) -> None:
    candidate = Path(path)
    is_junction = getattr(candidate, "is_junction", lambda: False)
    if candidate.is_symlink() or is_junction():
        function(path)
        return
    os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
    function(path)


def _remove_tree(root: Path) -> None:
    """Remove managed trees even when Windows files carry read-only attributes."""
    if not root.exists():
        return
    root_is_junction = getattr(root, "is_junction", lambda: False)
    if root.is_symlink():
        root.unlink()
        return
    if root_is_junction():
        root.rmdir()
        return
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        is_junction = getattr(path, "is_junction", lambda: False)
        if path.is_symlink() or is_junction():
            continue
        try:
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
        except FileNotFoundError:
            pass
    try:
        os.chmod(root, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
    except FileNotFoundError:
        return
    shutil.rmtree(root, onerror=_rmtree_onerror)


def _task_root(task_id: str) -> Path:
    if not task_id or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in task_id):
        raise ValueError("Task ID không hợp lệ.")
    return ARTIFACTS_ROOT / task_id


def _manifest_path(task_id: str) -> Path:
    return _task_root(task_id) / "manifest.json"


def _write_manifest(
    task_id: str,
    manifest: dict[str, Any],
    *,
    expected_before: effects.ExpectedBeforeValue,
) -> None:
    path = _manifest_path(task_id)
    project_workspace.atomic_write_text(
        path,
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        expected_before=expected_before,
    )


def _read_manifest(task_id: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        content = _manifest_path(task_id).read_bytes()
        manifest = json.loads(content.decode("utf-8"))
    except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError, OSError):
        return None, None
    if not isinstance(manifest, dict):
        return None, None
    return manifest, project_workspace.sha256_bytes(content)


def _iter_managed_files(root: Path) -> Iterator[Path]:
    if not root.is_dir():
        return
    for current, directories, filenames in os.walk(root, followlinks=False):
        current_path = Path(current)
        directories[:] = [
            name
            for name in directories
            if not (current_path / name).is_symlink()
            and not getattr(
                current_path / name,
                "is_junction",
                lambda: False,
            )()
        ]
        for name in filenames:
            path = current_path / name
            if path.is_file() and not path.is_symlink():
                yield path


def _register_managed_artifacts(
    task_id: str,
    manifest: dict[str, Any],
    *,
    terminal_evidence: bool,
    registration_callback: Callable[..., object] | None,
) -> None:
    callback = registration_callback or _registration_callback
    if callback is None:
        return
    task_root = _task_root(task_id).resolve()
    approved = {
        f"staging/{item['path']}"
        for item in manifest.get("files", [])
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    }
    retention = manifest.get("retention")
    pinned = bool(
        isinstance(retention, dict) and retention.get("pinned") is True
    )
    for path in _iter_managed_files(task_root):
        try:
            relative = path.relative_to(task_root).as_posix()
            digest = project_workspace.sha256_file(path)
            if digest is None:
                continue
            is_approved = relative in approved or relative == "project.zip"
            callback(
                task_id,
                str(path.resolve()),
                digest,
                size_bytes=path.stat().st_size,
                approved=is_approved,
                terminal_evidence=terminal_evidence,
                status=str(manifest.get("status") or "recorded"),
                metadata={
                    "kind": "artifact",
                    "managed": True,
                    "managed_root": str(ARTIFACTS_ROOT.resolve()),
                    "relative_path": relative,
                    "pinned": pinned,
                    "terminal_evidence": bool(terminal_evidence),
                },
            )
        except Exception:
            # Artifact output is already durable. Registration failure leaks
            # storage safely instead of invalidating a successful task.
            logger.warning(
                "Could not register managed artifact path %s",
                path,
                exc_info=True,
            )


def get_manifest(task_id: str) -> dict[str, Any] | None:
    manifest, _ = _read_manifest(task_id)
    return manifest


def create_workspace(
    task_id: str,
    destination: Path,
    *,
    auto_apply: bool,
    create_zip: bool,
    registration_callback: Callable[..., object] | None = None,
) -> Path:
    destination = destination.resolve()
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("New Project yêu cầu thư mục đích trống.")

    task_root = _task_root(task_id)
    staging = task_root / "staging"
    with _lock:
        if task_root.exists():
            _remove_tree(task_root)
        staging.mkdir(parents=True, exist_ok=True)
        manifest = {
            "task_id": task_id,
            "status": "staging",
            "destination": str(destination),
            "auto_apply": auto_apply,
            "create_zip": create_zip,
            "files": [],
            "incremental_apply": True,
            "created_at": _now(),
            "updated_at": _now(),
        }
        _write_manifest(
            task_id,
            manifest,
            expected_before=effects.MUST_BE_ABSENT,
        )
    _register_managed_artifacts(
        task_id,
        manifest,
        terminal_evidence=False,
        registration_callback=registration_callback,
    )
    return staging


def get_staging_workspace(task_id: str) -> Path:
    manifest = get_manifest(task_id)
    staging = (_task_root(task_id) / "staging").resolve()
    task_root = _task_root(task_id).resolve()
    if (
        manifest is None
        or task_root not in staging.parents
        or not staging.is_dir()
    ):
        raise FileNotFoundError(
            f"Không tìm thấy staging workspace để tiếp tục task {task_id}"
        )
    return staging


def _iter_project_files(staging: Path) -> Iterator[tuple[Path, Path]]:
    for current, directories, filenames in os.walk(staging, followlinks=False):
        current_path = Path(current)
        retained_directories = []
        for name in directories:
            child = current_path / name
            relative = child.relative_to(staging)
            is_junction = getattr(child, "is_junction", lambda: False)
            if (
                _is_internal_path(relative)
                or child.is_symlink()
                or is_junction()
            ):
                continue
            retained_directories.append(name)
        directories[:] = retained_directories
        for name in filenames:
            path = current_path / name
            relative = path.relative_to(staging)
            if (
                _is_internal_path(relative)
                or path.is_symlink()
                or not path.is_file()
                or path.suffix in {".pyc", ".pyo"}
            ):
                continue
            yield relative, path


def _file_metadata(relative: Path, source: Path) -> dict[str, Any]:
    digest = project_workspace.sha256_file(source)
    if digest is None:
        raise FileNotFoundError(source)
    return {
        "path": relative.as_posix(),
        "size": source.stat().st_size,
        "sha256": digest,
    }


def scan_artifact_candidates(
    files: list[tuple[Path, Path]],
    *,
    max_file_bytes: int = 2 * 1024 * 1024,
) -> list[dict[str, Any]]:
    """Scan candidate file contents without returning any secret material."""
    findings: list[dict[str, Any]] = []
    for relative, source in files:
        size = source.stat().st_size
        if size > max_file_bytes:
            findings.append(
                {
                    "path": relative.as_posix(),
                    "kind": "unscanned_oversized_file",
                    "confidence": "high",
                    "start": 0,
                    "end": 0,
                }
            )
            continue
        content = source.read_bytes()
        if b"\x00" in content:
            findings.append(
                {
                    "path": relative.as_posix(),
                    "kind": "binary_content_quarantined",
                    "confidence": "high",
                    "start": 0,
                    "end": 0,
                }
            )
            continue
        for finding in redaction.scan_secrets(
            content,
            candidate_type="artifact",
        ):
            findings.append(
                {
                    "path": relative.as_posix(),
                    "kind": finding.kind,
                    "confidence": finding.confidence,
                    "start": finding.start,
                    "end": finding.end,
                }
            )
    return findings


def _require_secret_free(files: list[tuple[Path, Path]]) -> None:
    findings = scan_artifact_candidates(files)
    if findings:
        summary = sorted(
            {f"{item['path']} ({item['kind']})" for item in findings}
        )
        raise ValueError(
            "Artifact content contains potential credentials: "
            + ", ".join(summary)
        )


def _create_zip(
    task_id: str,
    staging: Path,
    files: list[tuple[Path, Path]],
    expected_hashes: dict[str, str],
    *,
    owned_zip_sha256: str | None,
) -> tuple[Path, str]:
    zip_path = _task_root(task_id) / "project.zip"

    def write_archive(temp_path: Path) -> None:
        with zipfile.ZipFile(
            temp_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            for relative, source in files:
                archive.write(source, relative.as_posix())
        with zipfile.ZipFile(temp_path) as archive:
            for relative, _source in files:
                name = relative.as_posix()
                digest = hashlib.sha256()
                with archive.open(name) as member:
                    for chunk in iter(lambda: member.read(1024 * 1024), b""):
                        digest.update(chunk)
                if digest.hexdigest() != expected_hashes[name]:
                    raise project_workspace.ConcurrentModificationError(
                        staging / relative,
                        expected_hashes[name],
                        digest.hexdigest(),
                    )

    current_zip_sha256 = project_workspace.sha256_file(zip_path)
    expected_before: effects.ExpectedBeforeValue = (
        owned_zip_sha256
        or current_zip_sha256
        or effects.MUST_BE_ABSENT
    )
    prepared = project_workspace.prepare_atomic_replace_with_writer(
        zip_path,
        write_archive,
        expected_before=expected_before,
    )
    if (
        owned_zip_sha256 is None
        and current_zip_sha256 is not None
        and prepared.after_sha256 != current_zip_sha256
    ):
        project_workspace.discard_prepared_file(prepared)
        raise ValueError("Từ chối ghi đè project.zip không thuộc task")
    result = project_workspace.commit_prepared_file(prepared)
    return zip_path, result.after_sha256


def _copy_file_atomic(
    destination: Path,
    relative: Path,
    source: Path,
    *,
    expected_before: effects.ExpectedBeforeValue,
    expected_source_sha256: str,
) -> project_workspace.AtomicWriteResult:
    destination = destination.resolve()
    try:
        _, target = path_utils.resolve_under_root(
            destination, relative.as_posix()
        )
    except path_utils.PathEscapeError as exc:
        raise ValueError(f"Artifact thoát khỏi destination: {relative}") from exc
    return project_workspace.atomic_copy_file(
        source,
        target,
        expected_before=expected_before,
        expected_source_sha256=expected_source_sha256,
    )


def _materialize(
    destination: Path,
    files: list[tuple[Path, Path]],
    *,
    owned_files: dict[str, dict[str, Any]] | None = None,
    source_hashes: dict[str, str],
) -> dict[str, project_workspace.AtomicWriteResult]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    owned_files = owned_files or {}
    expectations: dict[str, effects.ExpectedBeforeValue] = {}
    sources = {relative.as_posix(): source for relative, source in files}
    if destination.exists():
        existing_files = {
            relative.as_posix()
            for relative, _path in _iter_project_files(destination)
        }
        conflicting: set[str] = set()
        for existing_relative in existing_files:
            if existing_relative not in sources:
                if existing_relative not in owned_files:
                    conflicting.add(existing_relative)
                continue
            actual_hash = project_workspace.sha256_file(
                destination / existing_relative
            )
            owned = owned_files.get(existing_relative)
            if owned is not None:
                owned_hash = owned.get("after_sha256")
                if not isinstance(owned_hash, str) or actual_hash != owned_hash:
                    conflicting.add(existing_relative)
                    continue
                expectations[existing_relative] = owned_hash
            elif actual_hash == source_hashes[existing_relative]:
                # A prior durable copy may have won just before its manifest update.
                expectations[existing_relative] = source_hashes[existing_relative]
            else:
                conflicting.add(existing_relative)
        if conflicting:
            raise ValueError(
                "Từ chối auto-apply vì destination có file ngoài task: "
                f"{sorted(conflicting)}"
            )
    for source_relative in sources:
        if source_relative in expectations:
            continue
        owned = owned_files.get(source_relative)
        if owned is not None:
            owned_hash = owned.get("after_sha256")
            if not isinstance(owned_hash, str):
                raise ValueError(
                    f"Artifact manifest thiếu owned destination hash: {source_relative}"
                )
            expectations[source_relative] = owned_hash
        else:
            expectations[source_relative] = effects.MUST_BE_ABSENT
    destination.mkdir(parents=True, exist_ok=True)
    results: dict[str, project_workspace.AtomicWriteResult] = {}
    for relative, source in files:
        normalized = relative.as_posix()
        results[normalized] = _copy_file_atomic(
            destination,
            relative,
            source,
            expected_before=expectations[normalized],
            expected_source_sha256=source_hashes[normalized],
        )
    return results


def materialize_approved_file(
    task_id: str,
    file_path: str,
    *,
    registration_callback: Callable[..., object] | None = None,
) -> dict[str, Any]:
    """Persist one Reviewer-approved staging file immediately."""
    with _lock:
        manifest, manifest_hash = _read_manifest(task_id)
        if manifest is None or manifest_hash is None:
            raise FileNotFoundError(f"Không tìm thấy artifact workspace {task_id}")
        staging = (_task_root(task_id) / "staging").resolve()
        normalized, source = path_utils.resolve_under_root(staging, file_path)
        relative = Path(normalized)
        if (
            not source.is_file()
            or source.is_symlink()
            or _is_internal_path(relative)
        ):
            raise ValueError(f"File approved không hợp lệ: {file_path}")
        _require_secret_free([(relative, source)])

        existing = {
            item.get("path"): item
            for item in manifest.get("files", [])
            if item.get("path")
        }
        metadata = _file_metadata(relative, source)
        prior_metadata = existing.get(normalized)
        if (
            prior_metadata is not None
            and prior_metadata.get("sha256") != metadata["sha256"]
        ):
            raise ValueError(
                f"Staging file changed after approval: {normalized}"
            )
        write_result = None
        if manifest.get("auto_apply", True):
            destination = Path(manifest["destination"]).resolve()
            destination.mkdir(parents=True, exist_ok=True)
            try:
                _, target = path_utils.resolve_under_root(
                    destination, normalized
                )
            except path_utils.PathEscapeError as exc:
                raise ValueError(f"File approved không hợp lệ: {file_path}") from exc
            if prior_metadata is not None:
                expected_before = prior_metadata.get("after_sha256")
                if not isinstance(expected_before, str):
                    raise ValueError(
                        f"Artifact manifest thiếu owned destination hash: {normalized}"
                    )
            elif target.exists():
                target_hash = project_workspace.sha256_file(target)
                if target_hash != metadata["sha256"]:
                    raise ValueError(
                        f"Từ chối ghi đè file không thuộc task: {normalized}"
                    )
                expected_before = target_hash
            else:
                expected_before = effects.MUST_BE_ABSENT
            write_result = _copy_file_atomic(
                destination,
                relative,
                source,
                expected_before=expected_before,
                expected_source_sha256=metadata["sha256"],
            )

        if write_result is not None:
            metadata["before_sha256"] = write_result.before_sha256
            metadata["after_sha256"] = write_result.after_sha256
        metadata["approved_at"] = _now()
        existing[normalized] = metadata
        manifest["files"] = list(existing.values())
        manifest["status"] = (
            "partially_applied"
            if manifest.get("auto_apply", True)
            else "staging"
        )
        manifest["updated_at"] = _now()
        _write_manifest(task_id, manifest, expected_before=manifest_hash)
    _register_managed_artifacts(
        task_id,
        manifest,
        terminal_evidence=False,
        registration_callback=registration_callback,
    )
    return manifest


def finalize_workspace(
    task_id: str,
    *,
    registration_callback: Callable[..., object] | None = None,
) -> dict[str, Any]:
    with _lock:
        manifest, manifest_hash = _read_manifest(task_id)
        if manifest is None or manifest_hash is None:
            raise FileNotFoundError(f"Không tìm thấy artifact workspace {task_id}")
        staging = _task_root(task_id) / "staging"
        files = list(_iter_project_files(staging))
        if not files:
            raise ValueError("Project staging không có file đầu ra.")
        _require_secret_free(files)

        previous_metadata = {
            item.get("path"): item
            for item in manifest.get("files", [])
            if item.get("path")
        }
        candidate_metadata = {
            relative.as_posix(): _file_metadata(relative, source)
            for relative, source in files
        }
        for normalized, prior in previous_metadata.items():
            current = candidate_metadata.get(normalized)
            if current is not None and prior.get("sha256") != current["sha256"]:
                raise ValueError(
                    f"Staging file changed after approval: {normalized}"
                )
        source_hashes = {
            normalized: metadata["sha256"]
            for normalized, metadata in candidate_metadata.items()
        }

        zip_path = None
        zip_sha256 = None
        if manifest.get("create_zip", True):
            zip_path, zip_sha256 = _create_zip(
                task_id,
                staging,
                files,
                source_hashes,
                owned_zip_sha256=(
                    str(manifest["zip_sha256"])
                    if manifest.get("zip_sha256")
                    else None
                ),
            )
        materialized: dict[str, project_workspace.AtomicWriteResult] = {}
        if manifest.get("auto_apply", True):
            materialized = _materialize(
                Path(manifest["destination"]).resolve(),
                files,
                owned_files=previous_metadata,
                source_hashes=source_hashes,
            )

        for relative, source in files:
            normalized = relative.as_posix()
            current_hash = project_workspace.sha256_file(source)
            if current_hash != source_hashes[normalized]:
                raise project_workspace.ConcurrentModificationError(
                    source,
                    source_hashes[normalized],
                    current_hash,
                )
        file_metadata = []
        for relative, _source in files:
            normalized = relative.as_posix()
            metadata = candidate_metadata[normalized]
            write_result = materialized.get(normalized)
            if write_result is not None:
                prior = previous_metadata.get(normalized, {})
                metadata["before_sha256"] = prior.get(
                    "before_sha256", write_result.before_sha256
                )
                metadata["after_sha256"] = write_result.after_sha256
            file_metadata.append(metadata)
        manifest["files"] = file_metadata
        manifest["status"] = (
            "applied" if manifest.get("auto_apply", True) else "ready"
        )
        manifest["zip_path"] = str(zip_path) if zip_path else None
        manifest["zip_sha256"] = zip_sha256
        manifest["updated_at"] = _now()
        _write_manifest(task_id, manifest, expected_before=manifest_hash)
    _register_managed_artifacts(
        task_id,
        manifest,
        terminal_evidence=True,
        registration_callback=registration_callback,
    )
    return manifest


def get_zip_path(task_id: str) -> Path | None:
    manifest = get_manifest(task_id)
    if not manifest or not manifest.get("zip_path"):
        return None
    path = Path(manifest["zip_path"]).resolve()
    task_root = _task_root(task_id).resolve()
    if task_root not in path.parents or not path.is_file():
        return None
    expected_hash = manifest.get("zip_sha256")
    if (
        not isinstance(expected_hash, str)
        or project_workspace.sha256_file(path) != expected_hash
    ):
        return None
    return path


def get_artifact_file(task_id: str, file_path: str) -> Path:
    manifest = get_manifest(task_id)
    if manifest is None:
        raise FileNotFoundError(f"Không tìm thấy artifact workspace {task_id}")
    normalized = path_utils.ensure_context_path_safe(file_path)
    approved = {
        str(item.get("path")): item
        for item in manifest.get("files", [])
        if isinstance(item, dict) and item.get("path")
    }
    if normalized not in approved:
        raise FileNotFoundError(normalized)
    staging = get_staging_workspace(task_id)
    _, target = path_utils.resolve_under_root(staging, normalized)
    if not target.is_file() or target.is_symlink():
        raise FileNotFoundError(normalized)
    expected_hash = approved[normalized].get("sha256")
    if (
        not isinstance(expected_hash, str)
        or project_workspace.sha256_file(target) != expected_hash
    ):
        raise ValueError(f"Artifact file no longer matches its manifest: {normalized}")
    return target


def set_retention(
    task_id: str,
    *,
    pinned: bool,
    registration_callback: Callable[..., object] | None = None,
) -> dict[str, Any]:
    with _lock:
        manifest, manifest_hash = _read_manifest(task_id)
        if manifest is None or manifest_hash is None:
            raise FileNotFoundError(f"Không tìm thấy artifact workspace {task_id}")
        manifest["retention"] = {
            "pinned": bool(pinned),
            "updated_at": _now(),
        }
        manifest["updated_at"] = _now()
        _write_manifest(task_id, manifest, expected_before=manifest_hash)
    _register_managed_artifacts(
        task_id,
        manifest,
        terminal_evidence=manifest.get("status") in {"applied", "ready"},
        registration_callback=registration_callback,
    )
    return manifest


def delete_artifacts(task_id: str, *, force: bool = False) -> bool:
    with _lock:
        manifest = get_manifest(task_id)
        if manifest is None:
            return False
        retention = manifest.get("retention")
        if (
            isinstance(retention, dict)
            and retention.get("pinned") is True
            and not force
        ):
            raise PermissionError("Artifact is pinned by its retention policy")
        task_root = _task_root(task_id).resolve()
        artifacts_root = ARTIFACTS_ROOT.resolve()
        if artifacts_root not in task_root.parents:
            raise ValueError("Artifact path escaped the managed root")
        _remove_tree(task_root)
        return True


def reconcile_workspace(
    task_id: str,
    *,
    target: str = "destination",
    record_result: bool = False,
    registration_callback: Callable[..., object] | None = None,
) -> dict[str, Any]:
    """Compare recorded artifact hashes with staging or materialized content."""
    if target not in {"destination", "staging"}:
        raise ValueError("target must be destination or staging")
    with _lock:
        manifest, manifest_hash = _read_manifest(task_id)
        if manifest is None or manifest_hash is None:
            raise FileNotFoundError(f"Không tìm thấy artifact workspace {task_id}")
        root = (
            (_task_root(task_id) / "staging").resolve()
            if target == "staging"
            else Path(manifest["destination"]).resolve()
        )
        expected = {
            str(item["path"]): item
            for item in manifest.get("files", [])
            if isinstance(item, dict) and item.get("path")
        }
        actual = {
            relative.as_posix(): source
            for relative, source in _iter_project_files(root)
        } if root.is_dir() else {}
        missing: list[str] = []
        tampered: list[dict[str, str | None]] = []
        for relative, metadata in sorted(expected.items()):
            source = actual.get(relative)
            if source is None:
                missing.append(relative)
                continue
            expected_hash = (
                metadata.get("sha256")
                if target == "staging"
                else metadata.get("after_sha256") or metadata.get("sha256")
            )
            actual_hash = hashlib.sha256(source.read_bytes()).hexdigest()
            if not expected_hash or actual_hash != expected_hash:
                tampered.append(
                    {
                        "path": relative,
                        "expected_sha256": expected_hash,
                        "actual_sha256": actual_hash,
                    }
                )
        extras = sorted(set(actual) - set(expected))
        result: dict[str, Any] = {
            "task_id": task_id,
            "target": target,
            "root": str(root),
            "valid": not missing and not tampered and not extras,
            "missing": missing,
            "tampered": tampered,
            "unapproved_extras": extras,
            "expected_count": len(expected),
            "actual_count": len(actual),
            "checked_at": _now(),
        }
        if record_result:
            manifest["last_reconciliation"] = result
            manifest["updated_at"] = result["checked_at"]
            _write_manifest(task_id, manifest, expected_before=manifest_hash)
    if record_result:
        _register_managed_artifacts(
            task_id,
            manifest,
            terminal_evidence=manifest.get("status") in {"applied", "ready"},
            registration_callback=registration_callback,
        )
    return result
