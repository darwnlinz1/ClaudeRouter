"""Crash-safe file operations and fenced project workspace leases."""
from __future__ import annotations

import errno
import hashlib
import os
import re
import shutil
import stat
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar, cast

from . import effects, path_utils

_T = TypeVar("_T")
_CAPTURE_CURRENT = object()
_OS_LOCKS_GUARD = threading.RLock()
_OS_LOCKS: dict[str, threading.Lock] = {}


class ConcurrentModificationError(RuntimeError):
    """The target no longer has the state authorized by the caller."""

    def __init__(
        self,
        target: Path,
        expected: effects.ExpectedBeforeValue,
        actual: str | None,
    ) -> None:
        self.target = target
        self.expected = expected
        self.actual = actual
        expected_label = (
            "absence"
            if expected is effects.MUST_BE_ABSENT
            else f"sha256:{expected}"
        )
        actual_label = "absence" if actual is None else f"sha256:{actual}"
        super().__init__(
            f"filesystem target changed before commit: {target} "
            f"(expected {expected_label}, found {actual_label})"
        )


@dataclass(frozen=True)
class AtomicWriteResult:
    target: Path
    before_sha256: str | None
    after_sha256: str


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str | None:
    try:
        stream = path.open("rb")
    except FileNotFoundError:
        return None
    digest = hashlib.sha256()
    with stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _target_sha256(path: Path) -> str | None:
    """Hash a regular target without treating other filesystem objects as absent."""
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ConcurrentModificationError(path, effects.MUST_BE_ABSENT, "<non-regular>")
    return sha256_file(path)


def _normalize_expected_before(
    target: Path,
    expected_before: effects.ExpectedBeforeValue | object,
) -> effects.ExpectedBeforeValue:
    if expected_before is _CAPTURE_CURRENT:
        current = _target_sha256(target)
        return effects.MUST_BE_ABSENT if current is None else current
    if expected_before is effects.MUST_BE_ABSENT:
        return effects.MUST_BE_ABSENT
    if not isinstance(expected_before, str) or not re.fullmatch(
        r"[0-9a-fA-F]{64}", expected_before
    ):
        raise ValueError("expected_before must be a SHA-256 hex digest or MUST_BE_ABSENT")
    return expected_before.lower()


def _assert_expected_before(
    target: Path,
    expected_before: effects.ExpectedBeforeValue,
) -> str | None:
    actual = _target_sha256(target)
    if expected_before is effects.MUST_BE_ABSENT:
        if actual is not None:
            raise ConcurrentModificationError(target, expected_before, actual)
    elif actual != expected_before:
        raise ConcurrentModificationError(target, expected_before, actual)
    return actual


def _fsync_file(stream: Any) -> None:
    stream.flush()
    try:
        descriptor = stream.fileno()
    except (AttributeError, OSError):
        return
    try:
        os.fsync(descriptor)
    except OSError as exc:
        unsupported = {
            errno.EBADF,
            errno.EINVAL,
            getattr(errno, "ENOSYS", -1),
            getattr(errno, "ENOTSUP", -1),
            getattr(errno, "EOPNOTSUPP", -1),
        }
        if exc.errno not in unsupported:
            raise


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _temporary_path(target: Path) -> tuple[int, Path]:
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".orchestrator-tmp",
        dir=str(target.parent),
    )
    return descriptor, Path(raw_path)


def _unlink_temporary(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except PermissionError:
        os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
        path.unlink(missing_ok=True)


def discard_prepared_file(prepared: effects.PreparedFileEffect) -> None:
    """Discard a prepared effect that will not be committed."""
    _unlink_temporary(prepared.temporary_path)


def prepare_atomic_write_bytes(
    target: Path,
    content: bytes,
    *,
    expected_before: effects.ExpectedBeforeValue,
) -> effects.PreparedFileEffect:
    """Flush content to a same-directory temp bound to an expected target state."""
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    expected_before = _normalize_expected_before(target, expected_before)
    _assert_expected_before(target, expected_before)
    previous_mode = None
    if expected_before is not effects.MUST_BE_ABSENT:
        previous_mode = stat.S_IMODE(target.stat().st_mode)

    descriptor, temp_path = _temporary_path(target)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(content)
            _fsync_file(stream)
        if previous_mode is not None:
            os.chmod(temp_path, previous_mode)
        return effects.PreparedFileEffect(
            target=target,
            temporary_path=temp_path,
            expected_before=expected_before,
            after_sha256=sha256_bytes(content),
        )
    except BaseException:
        _unlink_temporary(temp_path)
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def prepare_atomic_write_text(
    target: Path,
    content: str,
    *,
    expected_before: effects.ExpectedBeforeValue,
    encoding: str = "utf-8",
) -> effects.PreparedFileEffect:
    return prepare_atomic_write_bytes(
        target,
        content.encode(encoding),
        expected_before=expected_before,
    )


def prepare_atomic_copy_file(
    source: Path,
    target: Path,
    *,
    expected_before: effects.ExpectedBeforeValue,
    expected_source_sha256: str | None = None,
) -> effects.PreparedFileEffect:
    """Prepare a metadata-preserving copy and verify its authorized source hash."""
    source = Path(source)
    target = Path(target)
    if not source.is_file() or source.is_symlink():
        raise ValueError(f"source must be a regular file: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    expected_before = _normalize_expected_before(target, expected_before)
    _assert_expected_before(target, expected_before)
    if expected_source_sha256 is not None and not re.fullmatch(
        r"[0-9a-fA-F]{64}", expected_source_sha256
    ):
        raise ValueError("expected_source_sha256 must be a SHA-256 hex digest")

    descriptor, temp_path = _temporary_path(target)
    os.close(descriptor)
    try:
        shutil.copy2(source, temp_path)
        with temp_path.open("rb") as stream:
            _fsync_file(stream)
        copied_hash = sha256_file(temp_path)
        if copied_hash is None:
            raise OSError(f"prepared copy disappeared: {temp_path}")
        if (
            expected_source_sha256 is not None
            and copied_hash != expected_source_sha256.lower()
        ):
            raise ConcurrentModificationError(
                source,
                expected_source_sha256.lower(),
                copied_hash,
            )
        return effects.PreparedFileEffect(
            target=target,
            temporary_path=temp_path,
            expected_before=expected_before,
            after_sha256=copied_hash,
        )
    except BaseException:
        _unlink_temporary(temp_path)
        raise


def prepare_atomic_replace_with_writer(
    target: Path,
    writer: Callable[[Path], None],
    *,
    expected_before: effects.ExpectedBeforeValue,
) -> effects.PreparedFileEffect:
    """Prepare generated output without making it externally visible."""
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    expected_before = _normalize_expected_before(target, expected_before)
    _assert_expected_before(target, expected_before)
    descriptor, temp_path = _temporary_path(target)
    os.close(descriptor)
    try:
        writer(temp_path)
        if not temp_path.is_file() or temp_path.is_symlink():
            raise FileNotFoundError(f"writer did not materialize {temp_path}")
        with temp_path.open("rb") as stream:
            _fsync_file(stream)
        after_hash = sha256_file(temp_path)
        if after_hash is None:
            raise OSError(f"prepared output disappeared: {temp_path}")
        return effects.PreparedFileEffect(
            target=target,
            temporary_path=temp_path,
            expected_before=expected_before,
            after_sha256=after_hash,
        )
    except BaseException:
        _unlink_temporary(temp_path)
        raise


def commit_prepared_file(
    prepared: effects.PreparedFileEffect,
    *,
    sync_directory: bool = True,
    before_replace: Callable[[Path, Path], None] | None = None,
) -> AtomicWriteResult:
    """Recheck the target precondition immediately before atomic replacement."""
    target = prepared.target
    temp_path = prepared.temporary_path
    try:
        if before_replace is not None:
            before_replace(temp_path, target)
        prepared_hash = sha256_file(temp_path)
        if prepared_hash != prepared.after_sha256:
            raise ConcurrentModificationError(
                temp_path,
                prepared.after_sha256,
                prepared_hash,
            )
        before_hash = _assert_expected_before(target, prepared.expected_before)
        os.replace(temp_path, target)
        if sync_directory:
            _fsync_directory(target.parent)
    finally:
        _unlink_temporary(temp_path)

    after_hash = sha256_file(target)
    if after_hash != prepared.after_sha256:
        raise ConcurrentModificationError(target, prepared.after_sha256, after_hash)
    return AtomicWriteResult(
        target=target,
        before_sha256=before_hash,
        after_sha256=prepared.after_sha256,
    )


def atomic_write_bytes(
    target: Path,
    content: bytes,
    *,
    sync_directory: bool = True,
    before_replace: Callable[[Path, Path], None] | None = None,
    expected_before: effects.ExpectedBeforeValue | object = _CAPTURE_CURRENT,
) -> AtomicWriteResult:
    """Prepare and commit bytes, defaulting to the target state seen on entry."""
    target = Path(target)
    normalized_expected = _normalize_expected_before(target, expected_before)
    prepared = prepare_atomic_write_bytes(
        target, content, expected_before=normalized_expected
    )
    return commit_prepared_file(
        prepared,
        sync_directory=sync_directory,
        before_replace=before_replace,
    )


def atomic_write_text(
    target: Path,
    content: str,
    *,
    encoding: str = "utf-8",
    sync_directory: bool = True,
    before_replace: Callable[[Path, Path], None] | None = None,
    expected_before: effects.ExpectedBeforeValue | object = _CAPTURE_CURRENT,
) -> AtomicWriteResult:
    return atomic_write_bytes(
        target,
        content.encode(encoding),
        sync_directory=sync_directory,
        before_replace=before_replace,
        expected_before=expected_before,
    )


def atomic_copy_file(
    source: Path,
    target: Path,
    *,
    sync_directory: bool = True,
    before_replace: Callable[[Path, Path], None] | None = None,
    expected_before: effects.ExpectedBeforeValue | object = _CAPTURE_CURRENT,
    expected_source_sha256: str | None = None,
) -> AtomicWriteResult:
    """Atomically copy one regular file while preserving source metadata."""
    target = Path(target)
    normalized_expected = _normalize_expected_before(target, expected_before)
    prepared = prepare_atomic_copy_file(
        source,
        target,
        expected_before=normalized_expected,
        expected_source_sha256=expected_source_sha256,
    )
    return commit_prepared_file(
        prepared,
        sync_directory=sync_directory,
        before_replace=before_replace,
    )


def atomic_replace_with_writer(
    target: Path,
    writer: Callable[[Path], None],
    *,
    sync_directory: bool = True,
    before_replace: Callable[[Path, Path], None] | None = None,
    expected_before: effects.ExpectedBeforeValue | object = _CAPTURE_CURRENT,
) -> AtomicWriteResult:
    """Materialize generated output to temp, then atomically commit it."""
    target = Path(target)
    normalized_expected = _normalize_expected_before(target, expected_before)
    prepared = prepare_atomic_replace_with_writer(
        target,
        writer,
        expected_before=normalized_expected,
    )
    return commit_prepared_file(
        prepared,
        sync_directory=sync_directory,
        before_replace=before_replace,
    )


@dataclass(frozen=True)
class ProjectLease:
    project_key: str
    owner_id: str
    fencing_token: int
    expires_at: datetime
    heartbeat_at: datetime
    purpose: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def expired(self) -> bool:
        return self.expires_at <= datetime.now(timezone.utc)


class ProjectLeaseLostError(RuntimeError):
    """The owner/token pair no longer authorizes project mutation."""


@dataclass
class ProjectMutationLock:
    """A process-and-host exclusive lock for one normalized project key."""

    path: Path
    _stream: Any
    _thread_lock: threading.Lock
    _released: bool = False

    @classmethod
    def acquire(
        cls,
        lock_root: Path,
        project_key: str,
        *,
        blocking: bool = True,
    ) -> "ProjectMutationLock | None":
        if not project_key.strip():
            raise ValueError("project_key is required")
        root = Path(lock_root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(project_key.encode("utf-8")).hexdigest()
        path = root / f"{digest}.lock"
        lock_key = os.path.normcase(str(path))
        with _OS_LOCKS_GUARD:
            thread_lock = _OS_LOCKS.setdefault(lock_key, threading.Lock())
        if not thread_lock.acquire(blocking=blocking):
            return None
        stream = None
        try:
            stream = path.open("a+b")
            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.write(b"\0")
                _fsync_file(stream)
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
                msvcrt.locking(stream.fileno(), mode, 1)
            else:
                import fcntl

                flags = fcntl.LOCK_EX
                if not blocking:
                    flags |= fcntl.LOCK_NB
                fcntl.flock(stream.fileno(), flags)
            return cls(path, stream, thread_lock)
        except (OSError, BlockingIOError):
            if stream is not None:
                stream.close()
            thread_lock.release()
            return None
        except BaseException:
            if stream is not None:
                stream.close()
            thread_lock.release()
            raise

    def release(self) -> None:
        if self._released:
            return
        try:
            self._stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
        finally:
            self._stream.close()
            self._released = True
            self._thread_lock.release()

    def __enter__(self) -> "ProjectMutationLock":
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


@dataclass
class ProjectLeaseHandle:
    """Token-bearing lease handle; stale handles cannot heartbeat or mutate."""

    repository: Any
    lease: ProjectLease
    os_lock: ProjectMutationLock | None = None

    @property
    def fencing_token(self) -> int:
        return self.lease.fencing_token

    def heartbeat(self, ttl_seconds: float) -> ProjectLease:
        renewed = self.repository.heartbeat_project_lease(
            self.lease.project_key,
            self.lease.owner_id,
            self.lease.fencing_token,
            ttl_seconds,
        )
        if renewed is None:
            raise ProjectLeaseLostError("project lease is no longer current")
        self.lease = renewed
        return cast(ProjectLease, renewed)

    def assert_current(self) -> None:
        if not self.repository.validate_project_lease(
            self.lease.project_key,
            self.lease.owner_id,
            self.lease.fencing_token,
        ):
            raise ProjectLeaseLostError("project lease is no longer current")

    def mutate(self, operation: Callable[[], _T]) -> _T:
        if self.os_lock is None:
            return cast(
                _T,
                self.repository.run_fenced_project_mutation(
                    self.lease.project_key,
                    self.lease.owner_id,
                    self.lease.fencing_token,
                    operation,
                ),
            )
        self.assert_current()
        result = operation()
        self.assert_current()
        return result

    def release(self) -> bool:
        try:
            return bool(
                self.repository.release_project_lease(
                    self.lease.project_key,
                    self.lease.owner_id,
                    self.lease.fencing_token,
                )
            )
        finally:
            if self.os_lock is not None:
                self.os_lock.release()
                self.os_lock = None


class ProjectLeaseManager:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def acquire(
        self,
        project_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        purpose: str = "",
    ) -> ProjectLeaseHandle | None:
        os_lock = ProjectMutationLock.acquire(
            Path(self.repository.project_lock_root),
            project_key,
            blocking=False,
        )
        if os_lock is None:
            return None
        try:
            lease = self.repository.acquire_project_lease(
                project_key,
                owner_id,
                ttl_seconds,
                purpose=purpose,
            )
        except BaseException:
            os_lock.release()
            raise
        if lease is None:
            os_lock.release()
            return None
        return ProjectLeaseHandle(self.repository, lease, os_lock)


@dataclass(frozen=True)
class FencedProjectWorkspace:
    """A root whose mutations must pass through a current lease token."""

    root: Path
    lease: ProjectLeaseHandle

    def write_bytes(self, relative_path: str, content: bytes) -> AtomicWriteResult:
        _, target = path_utils.resolve_under_root(self.root, relative_path)
        return self.lease.mutate(lambda: atomic_write_bytes(target, content))

    def write_text(
        self, relative_path: str, content: str, *, encoding: str = "utf-8"
    ) -> AtomicWriteResult:
        _, target = path_utils.resolve_under_root(self.root, relative_path)
        return self.lease.mutate(
            lambda: atomic_write_text(target, content, encoding=encoding)
        )
