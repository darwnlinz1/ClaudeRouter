"""Verified backup and restore for state, artifacts, logs, and account health."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

from . import effects, project_workspace
from .state_repository import DEFAULT_DB_PATH

BACKUP_FORMAT_VERSION = 1
MAX_BACKUP_MEMBER_COUNT = 100_000
MAX_BACKUP_COMPRESSED_BYTES = 8 * 1024 * 1024 * 1024
MAX_BACKUP_EXPANDED_BYTES = 32 * 1024 * 1024 * 1024
MAX_BACKUP_MEMBER_EXPANDED_BYTES = 4 * 1024 * 1024 * 1024
MAX_BACKUP_COMPRESSION_RATIO = 200.0
MAX_BACKUP_MANIFEST_BYTES = 8 * 1024 * 1024
_COPY_CHUNK_BYTES = 1024 * 1024


def _path_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _reject_root_overlap(candidate: Path, roots: Iterable[Path], *, label: str) -> None:
    for root in roots:
        if _paths_overlap(candidate, root):
            raise ValueError(f"{label} overlaps included root: {root}")


def _sqlite_user_version(database: Path) -> int:
    uri = f"{database.resolve().as_uri()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    try:
        row = connection.execute("PRAGMA user_version").fetchone()
    finally:
        connection.close()
    if row is None:
        raise ValueError(f"unable to inspect SQLite schema version: {database}")
    return int(row[0])


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _copy_sqlite(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not source.is_file():
        connection = sqlite3.connect(destination)
        connection.close()
        return
    source_connection = sqlite3.connect(
        f"{source.resolve().as_uri()}?mode=ro",
        uri=True,
    )
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
    finally:
        destination_connection.close()
        source_connection.close()


def _iter_regular_files(root: Path) -> Iterable[Path]:
    if not root.is_dir():
        return ()
    return (
        path
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    )


def create_backup(
    destination: str | os.PathLike[str],
    *,
    database: str | os.PathLike[str] = DEFAULT_DB_PATH,
    account_database: str | os.PathLike[str] | None = None,
    artifacts_root: str | os.PathLike[str] | None = None,
    logs_root: str | os.PathLike[str] | None = None,
) -> Path:
    archive = Path(destination).expanduser().resolve()
    roots = {
        "artifacts": Path(artifacts_root).expanduser().resolve()
        if artifacts_root is not None
        else None,
        "logs": Path(logs_root).expanduser().resolve()
        if logs_root is not None
        else None,
    }
    configured_roots = [root for root in roots.values() if root is not None]
    _reject_root_overlap(archive, configured_roots, label="backup destination")
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive_before = project_workspace.sha256_file(archive)
    expected_archive = (
        archive_before if archive_before is not None else effects.MUST_BE_ABSENT
    )
    with tempfile.TemporaryDirectory(
        prefix="orchestrator-backup-",
        dir=archive.parent,
    ) as temporary:
        stage = Path(temporary)
        _reject_root_overlap(stage, configured_roots, label="backup staging directory")
        _copy_sqlite(Path(database).expanduser().resolve(), stage / "state.sqlite3")
        if account_database is not None:
            _copy_sqlite(
                Path(account_database).expanduser().resolve(),
                stage / "account-leases.sqlite3",
            )
        included_roots: list[str] = []
        for name, root in roots.items():
            if root is None or not root.is_dir():
                continue
            included_roots.append(name)
            for source in _iter_regular_files(root):
                relative = source.relative_to(root)
                target = stage / name / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)

        files: list[dict[str, Any]] = []
        for path in sorted(_iter_regular_files(stage)):
            archive_relative = path.relative_to(stage).as_posix()
            if archive_relative == "manifest.json":
                continue
            files.append(
                {
                    "path": archive_relative,
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
        manifest = {
            "format_version": BACKUP_FORMAT_VERSION,
            "schema_version": _sqlite_user_version(stage / "state.sqlite3"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "included_roots": included_roots,
            "files": files,
        }
        (stage / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        def write_archive(temporary_archive: Path) -> None:
            with zipfile.ZipFile(
                temporary_archive,
                "w",
                compression=zipfile.ZIP_DEFLATED,
            ) as bundle:
                for path in sorted(_iter_regular_files(stage)):
                    bundle.write(path, path.relative_to(stage).as_posix())

        project_workspace.atomic_replace_with_writer(
            archive,
            write_archive,
            expected_before=expected_archive,
        )
    return archive


def _safe_member(name: str) -> bool:
    candidate = PurePosixPath(name)
    return (
        bool(candidate.parts)
        and not candidate.is_absolute()
        and ".." not in candidate.parts
        and not candidate.parts[0].endswith(":")
    )


def _zip_member_is_symlink(member: zipfile.ZipInfo) -> bool:
    mode = (member.external_attr >> 16) & 0xFFFF
    return stat.S_ISLNK(mode)


def _check_archive_size(source: Path) -> None:
    if source.stat().st_size > MAX_BACKUP_COMPRESSED_BYTES:
        raise ValueError("backup compressed size exceeds the configured limit")


def _validate_bundle(
    bundle: zipfile.ZipFile,
) -> tuple[dict[str, Any], dict[str, zipfile.ZipInfo]]:
    infos = bundle.infolist()
    if len(infos) > MAX_BACKUP_MEMBER_COUNT:
        raise ValueError("backup member count exceeds the configured limit")
    names = [item.filename for item in infos]
    if len(names) != len(set(names)):
        raise ValueError("backup contains duplicate archive members")
    if any(not _safe_member(name) for name in names):
        raise ValueError("backup contains an unsafe archive path")
    if any(item.is_dir() or _zip_member_is_symlink(item) for item in infos):
        raise ValueError("backup contains a non-regular archive member")
    if any(item.flag_bits & 0x1 for item in infos):
        raise ValueError("encrypted backup members are not supported")

    total_compressed = 0
    total_expanded = 0
    for item in infos:
        if item.file_size > MAX_BACKUP_MEMBER_EXPANDED_BYTES:
            raise ValueError(
                f"backup member expanded size exceeds the configured limit: "
                f"{item.filename}"
            )
        total_compressed += item.compress_size
        total_expanded += item.file_size
        ratio = (
            item.file_size / item.compress_size
            if item.compress_size
            else (float("inf") if item.file_size else 0.0)
        )
        if ratio > MAX_BACKUP_COMPRESSION_RATIO:
            raise ValueError(
                f"backup member compression ratio exceeds the configured limit: "
                f"{item.filename}"
            )
    if total_compressed > MAX_BACKUP_COMPRESSED_BYTES:
        raise ValueError("backup compressed size exceeds the configured limit")
    if total_expanded > MAX_BACKUP_EXPANDED_BYTES:
        raise ValueError("backup expanded size exceeds the configured limit")
    total_ratio = (
        total_expanded / total_compressed
        if total_compressed
        else (float("inf") if total_expanded else 0.0)
    )
    if total_ratio > MAX_BACKUP_COMPRESSION_RATIO:
        raise ValueError("backup compression ratio exceeds the configured limit")

    by_name = {item.filename: item for item in infos}
    manifest_info = by_name.get("manifest.json")
    if (
        manifest_info is None
        or manifest_info.file_size > MAX_BACKUP_MANIFEST_BYTES
    ):
        raise ValueError("backup manifest is missing or exceeds the configured limit")
    try:
        manifest = json.loads(bundle.read(manifest_info))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("backup manifest is missing or invalid") from exc
    if not isinstance(manifest, dict):
        raise ValueError("backup manifest is missing or invalid")
    if manifest.get("format_version") != BACKUP_FORMAT_VERSION:
        raise ValueError("unsupported backup format version")
    raw_included_roots = manifest.get("included_roots")
    included_roots: set[str] | None = None
    if raw_included_roots is not None:
        if (
            not isinstance(raw_included_roots, list)
            or any(
                not isinstance(root, str) or root not in {"artifacts", "logs"}
                for root in raw_included_roots
            )
            or len(raw_included_roots) != len(set(raw_included_roots))
        ):
            raise ValueError("backup manifest included roots are invalid")
        included_roots = set(raw_included_roots)
    declared_files = manifest.get("files")
    if not isinstance(declared_files, list):
        raise ValueError("backup manifest files must be a list")
    if len(declared_files) + 1 > MAX_BACKUP_MEMBER_COUNT:
        raise ValueError("backup member count exceeds the configured limit")

    declared: dict[str, dict[str, Any]] = {}
    for record in declared_files:
        if not isinstance(record, dict):
            raise ValueError("backup manifest contains an invalid file record")
        path = record.get("path")
        size = record.get("size_bytes")
        checksum = record.get("sha256")
        if (
            not isinstance(path, str)
            or path == "manifest.json"
            or not _safe_member(path)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or size > MAX_BACKUP_MEMBER_EXPANDED_BYTES
            or not isinstance(checksum, str)
            or not re.fullmatch(r"[0-9a-fA-F]{64}", checksum)
        ):
            raise ValueError("backup manifest contains an invalid file record")
        parts = PurePosixPath(path).parts
        if not (
            path in {"state.sqlite3", "account-leases.sqlite3"}
            or (len(parts) > 1 and parts[0] in {"artifacts", "logs"})
        ):
            raise ValueError(f"backup manifest contains an unsupported member: {path}")
        if (
            included_roots is not None
            and parts[0] in {"artifacts", "logs"}
            and parts[0] not in included_roots
        ):
            raise ValueError("backup manifest file is outside its included roots")
        if path in declared:
            raise ValueError("backup manifest contains duplicate file records")
        declared[path] = record

    expected_names = {"manifest.json", *declared}
    actual_names = set(by_name)
    if actual_names != expected_names:
        extra = sorted(actual_names - expected_names)
        missing = sorted(expected_names - actual_names)
        raise ValueError(
            f"backup member set does not match manifest "
            f"(unlisted={extra}, missing={missing})"
        )
    for name in declared:
        candidate = PurePosixPath(name)
        if any(
            parent.as_posix() in declared
            for parent in candidate.parents
            if parent.as_posix() != "."
        ):
            raise ValueError("backup manifest contains conflicting member paths")

    for path, record in declared.items():
        info = by_name[path]
        expected_size = int(record["size_bytes"])
        if info.file_size != expected_size:
            raise ValueError(f"backup size mismatch: {path}")
        digest = hashlib.sha256()
        actual_size = 0
        with bundle.open(info) as member:
            for block in iter(lambda: member.read(_COPY_CHUNK_BYTES), b""):
                actual_size += len(block)
                if actual_size > expected_size:
                    raise ValueError(f"backup size mismatch: {path}")
                digest.update(block)
        if actual_size != expected_size:
            raise ValueError(f"backup size mismatch: {path}")
        if digest.hexdigest() != str(record["sha256"]).lower():
            raise ValueError(f"backup checksum mismatch: {path}")
    return manifest, by_name


def verify_backup(archive: str | os.PathLike[str]) -> dict[str, Any]:
    source = Path(archive).expanduser().resolve()
    _check_archive_size(source)
    with source.open("rb") as archive_stream, zipfile.ZipFile(archive_stream) as bundle:
        manifest, _ = _validate_bundle(bundle)
    return manifest


def _manifest_included_roots(manifest: dict[str, Any]) -> set[str]:
    raw_roots = manifest.get("included_roots")
    if isinstance(raw_roots, list):
        return set(raw_roots)
    roots: set[str] = set()
    for record in manifest["files"]:
        first_part = PurePosixPath(str(record["path"])).parts[0]
        if first_part in {"artifacts", "logs"}:
            roots.add(first_part)
    return roots


def _make_writable(path: Path) -> None:
    try:
        mode = stat.S_IMODE(path.lstat().st_mode)
    except FileNotFoundError:
        return
    os.chmod(path, mode | stat.S_IWRITE)


def _remove_readonly(
    function: Any,
    path: str,
    _error: tuple[type[BaseException], BaseException, Any],
) -> None:
    os.chmod(path, stat.S_IWRITE)
    function(path)


def _remove_path(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
        shutil.rmtree(path, onerror=_remove_readonly)
        return
    try:
        path.unlink()
    except PermissionError:
        _make_writable(path)
        path.unlink()


def _replace_path(source: Path, target: Path) -> None:
    _make_writable(source)
    if _path_exists(target):
        _make_writable(target)
    os.replace(source, target)


@dataclass(frozen=True)
class _PreparedRestoreSwap:
    target: Path
    workspace: Path
    replacement: Path
    rollback: Path
    had_target: bool
    original_mode: int | None


class _RestoreRollbackError(RuntimeError):
    pass


def _prepare_restore_swap(
    staged: Path,
    target: Path,
    *,
    is_directory: bool,
) -> _PreparedRestoreSwap:
    target.parent.mkdir(parents=True, exist_ok=True)
    had_target = _path_exists(target)
    original_mode = (
        stat.S_IMODE(target.lstat().st_mode) if had_target else None
    )
    if had_target:
        target_is_directory = target.is_dir() and not target.is_symlink()
        if is_directory and not target_is_directory:
            raise NotADirectoryError(target)
        if not is_directory and target_is_directory:
            raise IsADirectoryError(target)
        if not is_directory and (not target.is_file() or target.is_symlink()):
            raise ValueError(f"restore target must be a regular file: {target}")

    workspace = Path(
        tempfile.mkdtemp(
            prefix=f".{target.name}.orchestrator-restore-",
            dir=target.parent,
        )
    )
    replacement = workspace / "replacement"
    rollback = workspace / "rollback"
    try:
        if is_directory:
            shutil.copytree(staged, replacement)
        else:
            expected_sha256 = _sha256(staged)
            shutil.copy2(staged, replacement)
            if _sha256(replacement) != expected_sha256:
                raise OSError(f"staged restore copy changed while preparing: {staged}")
    except BaseException:
        _remove_path(workspace)
        raise
    return _PreparedRestoreSwap(
        target=target,
        workspace=workspace,
        replacement=replacement,
        rollback=rollback,
        had_target=had_target,
        original_mode=original_mode,
    )


def _restore_original_mode(swap: _PreparedRestoreSwap) -> None:
    if swap.original_mode is not None and _path_exists(swap.target):
        os.chmod(swap.target, swap.original_mode)


def _install_restore_swap(swap: _PreparedRestoreSwap) -> None:
    old_moved = False
    if swap.had_target:
        if not _path_exists(swap.target):
            raise FileNotFoundError(swap.target)
        _replace_path(swap.target, swap.rollback)
        old_moved = True
    elif _path_exists(swap.target):
        raise FileExistsError(swap.target)
    try:
        _replace_path(swap.replacement, swap.target)
    except BaseException as install_error:
        if old_moved:
            try:
                _replace_path(swap.rollback, swap.target)
                _restore_original_mode(swap)
            except BaseException:
                raise _RestoreRollbackError(
                    f"restore install and immediate rollback failed for {swap.target}"
                ) from install_error
        raise


def _rollback_restore_swap(swap: _PreparedRestoreSwap) -> None:
    displaced = swap.workspace / "displaced"
    if _path_exists(swap.target):
        _replace_path(swap.target, displaced)
    if swap.had_target:
        try:
            _replace_path(swap.rollback, swap.target)
            _restore_original_mode(swap)
        except BaseException:
            if _path_exists(displaced) and not _path_exists(swap.target):
                _replace_path(displaced, swap.target)
            raise


def _commit_restore_swaps(swaps: list[_PreparedRestoreSwap]) -> None:
    installed: list[_PreparedRestoreSwap] = []
    preserve_workspaces = False
    try:
        for swap in swaps:
            _install_restore_swap(swap)
            installed.append(swap)
    except BaseException as install_error:
        preserve_workspaces = isinstance(install_error, _RestoreRollbackError)
        rollback_errors: list[BaseException] = []
        for swap in reversed(installed):
            try:
                _rollback_restore_swap(swap)
            except BaseException as rollback_error:
                rollback_errors.append(rollback_error)
        if rollback_errors:
            preserve_workspaces = True
            raise RuntimeError(
                "restore failed and one or more targets could not be rolled back"
            ) from install_error
        raise
    finally:
        if not preserve_workspaces:
            for swap in swaps:
                try:
                    _remove_path(swap.workspace)
                except OSError:
                    pass


def _reject_overlapping_targets(targets: dict[str, Path | None]) -> None:
    configured = [(name, path) for name, path in targets.items() if path is not None]
    for index, (left_name, left) in enumerate(configured):
        for right_name, right in configured[index + 1 :]:
            if _paths_overlap(left, right):
                raise ValueError(
                    f"restore targets overlap: {left_name}={left}, "
                    f"{right_name}={right}"
                )


def restore_backup(
    archive: str | os.PathLike[str],
    *,
    database: str | os.PathLike[str],
    account_database: str | os.PathLike[str] | None = None,
    artifacts_root: str | os.PathLike[str] | None = None,
    logs_root: str | os.PathLike[str] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    source = Path(archive).expanduser().resolve()
    _check_archive_size(source)
    targets = {
        "state.sqlite3": Path(database).expanduser().resolve(),
        "account-leases.sqlite3": (
            Path(account_database).expanduser().resolve()
            if account_database is not None
            else None
        ),
        "artifacts": (
            Path(artifacts_root).expanduser().resolve()
            if artifacts_root is not None
            else None
        ),
        "logs": (
            Path(logs_root).expanduser().resolve()
            if logs_root is not None
            else None
        ),
    }
    _reject_overlapping_targets(targets)
    restore_roots = [
        target
        for name, target in targets.items()
        if name in {"artifacts", "logs"} and target is not None
    ]
    with tempfile.TemporaryDirectory(prefix="orchestrator-restore-") as temporary:
        stage = Path(temporary)
        _reject_root_overlap(
            stage,
            restore_roots,
            label="restore staging directory",
        )
        with source.open("rb") as archive_stream, zipfile.ZipFile(
            archive_stream
        ) as bundle:
            manifest, by_name = _validate_bundle(bundle)
            for item in manifest["files"]:
                archive_name = str(item["path"])
                staged = stage.joinpath(*PurePosixPath(archive_name).parts)
                staged.parent.mkdir(parents=True, exist_ok=True)
                with bundle.open(by_name[archive_name]) as member, staged.open(
                    "xb"
                ) as output:
                    shutil.copyfileobj(member, output, length=_COPY_CHUNK_BYTES)
        if not (stage / "state.sqlite3").is_file():
            raise ValueError("backup state database is missing")

        included_roots = _manifest_included_roots(manifest)
        for root_name in included_roots:
            (stage / root_name).mkdir(parents=True, exist_ok=True)

        desired: list[tuple[str, bool]] = [("state.sqlite3", False)]
        if (stage / "account-leases.sqlite3").is_file():
            desired.append(("account-leases.sqlite3", False))
        desired.extend(
            (root_name, True)
            for root_name in ("artifacts", "logs")
            if root_name in included_roots
        )
        for archive_name, _is_directory in desired:
            target = targets[archive_name]
            if target is not None and _path_exists(target) and not overwrite:
                raise FileExistsError(target)

        prepared: list[_PreparedRestoreSwap] = []
        try:
            for archive_name, is_directory in desired:
                target = targets[archive_name]
                if target is None:
                    continue
                prepared.append(
                    _prepare_restore_swap(
                        stage / archive_name,
                        target,
                        is_directory=is_directory,
                    )
                )
        except BaseException:
            for swap in prepared:
                try:
                    _remove_path(swap.workspace)
                except OSError:
                    pass
            raise
        _commit_restore_swaps(prepared)
    return manifest
