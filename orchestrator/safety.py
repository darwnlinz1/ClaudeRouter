# -*- coding: utf-8 -*-
"""External snapshots, rollback, and local syntax/test gates.

Rule I explicitly puts these mechanisms off-limits to the model
("không được đề xuất thay đổi vào chính cơ chế an toàn của orchestrator...
trừ khi đó chính là task được giao tường minh") -- they live entirely on
the orchestrator side and the model never sees or influences this code.
"""

from __future__ import annotations

import ast
import json
import os
import re
import shutil
import subprocess
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from . import config, path_utils, project_workspace
from .policy import PolicyAction, PolicyEngine, PolicyRequest
from .sandbox import (
    IsolationLevel,
    SandboxBackend,
    SandboxBackendUnavailable,
    SandboxOutcome,
    SandboxPolicyError,
    SandboxResult,
    SandboxRunner,
)

SNAPSHOTS_ROOT = Path(
    os.environ.get(
        "ORCH_SNAPSHOTS_DIR",
        str(Path.home() / ".ai_orchestrator" / "snapshots"),
    )
).expanduser()

_snapshot_lock = threading.RLock()
_SNAPSHOT_TOKEN = re.compile(r"^[0-9a-f]{32}$")
_LEGACY_COMMIT = re.compile(r"^[0-9a-fA-F]{7,64}$")


class GitError(RuntimeError):
    pass


class SnapshotError(RuntimeError):
    pass


class RollbackConflictError(SnapshotError):
    """The rollback target no longer contains the effect being compensated."""

    def __init__(
        self,
        target: Path,
        expected_after_sha256: str,
        actual_sha256: str | None,
    ) -> None:
        self.target = target
        self.expected_after_sha256 = expected_after_sha256
        self.actual_sha256 = actual_sha256
        actual = (
            "absence"
            if actual_sha256 is None
            else f"sha256:{actual_sha256}"
        )
        super().__init__(
            f"rollback conflict for {target}: expected applied effect "
            f"sha256:{expected_after_sha256}, found {actual}"
        )


def _validated_expected_after(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise SnapshotError("expected_after_sha256 must be a SHA-256 hex digest")
    return normalized


def _assert_rollback_target(target: Path, expected_after_sha256: str) -> None:
    if target.is_symlink() or (target.exists() and not target.is_file()):
        actual = "<non-regular>"
    else:
        actual = project_workspace.sha256_file(target)
    if actual != expected_after_sha256:
        raise RollbackConflictError(target, expected_after_sha256, actual)


def run_sandbox_command(
    root: Path,
    cmd: list[str],
    *,
    timeout: int | float | None = None,
    isolation_level: IsolationLevel | str = IsolationLevel.STRONG,
    max_output_bytes: int = 1024 * 1024,
    backend: SandboxBackend | None = None,
    trusted_internal: bool = False,
    cancelled: Callable[[], bool] | None = None,
    policy_engine: PolicyEngine | None = None,
) -> SandboxResult:
    """Run a command, failing closed when its isolation cannot be attested."""
    runner = SandboxRunner(
        root,
        isolation_level=isolation_level,
        backend=backend,
        trusted_internal=trusted_internal,
        timeout_seconds=timeout,
        max_output_bytes=max_output_bytes,
        cancellation_poll_seconds=config.SANDBOX_CANCELLATION_POLL_SECONDS,
    )
    if not runner.isolation_request_satisfied():
        # Nothing is launched.  Return the runner's structured unavailable
        # result instead of converting capability absence into an exception.
        return runner.run(cmd, cancelled=cancelled)
    actual_isolation, _ = runner.describe_isolation()
    decision = (policy_engine or PolicyEngine()).evaluate(
        PolicyRequest(
            action=PolicyAction.COMMAND_EXECUTION,
            resource=str(root),
            command=tuple(cmd),
            command_policy=runner.policy,
            requested_isolation=runner.requested_isolation,
            actual_isolation=actual_isolation,
        )
    )
    if not decision.allowed:
        raise SandboxPolicyError(
            "command denied by policy: " + ", ".join(decision.reasons)
        )
    return runner.run(cmd, cancelled=cancelled)


def _run(
    cmd: list[str],
    cwd: Path,
    *,
    timeout: int | float | None = None,
) -> subprocess.CompletedProcess[str]:
    result = run_sandbox_command(
        cwd,
        cmd,
        timeout=timeout,
        isolation_level=IsolationLevel.PROCESS_ONLY,
        trusted_internal=True,
    )
    if result.blocked:
        raise SandboxBackendUnavailable(
            result.blocked_reason or result.isolation_details
        )
    if result.timed_out:
        raise subprocess.TimeoutExpired(
            cmd,
            float(timeout or 0),
            output=result.stdout,
            stderr=result.stderr,
        )
    return subprocess.CompletedProcess(
        cmd,
        result.returncode if result.returncode is not None else -1,
        stdout=result.stdout,
        stderr=result.stderr,
    )


def ensure_git_repo(root: Path) -> None:
    """Validate a repository without initializing or changing Git config."""
    result = _run(["git", "rev-parse", "--is-inside-work-tree"], root)
    if result.returncode != 0:
        raise GitError(f"not a Git work tree: {root}")


def _snapshot_directory(token: str) -> Path:
    if not _SNAPSHOT_TOKEN.fullmatch(token):
        raise SnapshotError("invalid snapshot token")
    return SNAPSHOTS_ROOT / token


def _snapshot_metadata(token: str) -> dict[str, object] | None:
    try:
        value = json.loads(
            (_snapshot_directory(token) / "metadata.json").read_text(
                encoding="utf-8"
            )
        )
    except (FileNotFoundError, json.JSONDecodeError, OSError, SnapshotError):
        return None
    return value if isinstance(value, dict) else None


def backup_commit(root: Path, message: str, file_path: str | None = None) -> str:
    """Create a durable, scoped snapshot outside the target repository.

    The historical function name is retained for callers, but this function
    never stages files, creates commits, initializes Git, or changes Git config.
    """
    root = Path(root).resolve(strict=True)
    if not root.is_dir():
        raise NotADirectoryError(root)
    normalized = None
    target = None
    existed = False
    before_sha256 = None
    if file_path is not None:
        try:
            normalized, target = path_utils.resolve_under_root(root, file_path)
        except path_utils.PathEscapeError as exc:
            raise SnapshotError(str(exc)) from exc
        existed = target.exists()
        if existed and not target.is_file():
            raise SnapshotError(f"snapshot target is not a regular file: {normalized}")
        before_sha256 = project_workspace.sha256_file(target) if existed else None

    token = uuid.uuid4().hex
    snapshot_dir = _snapshot_directory(token)
    with _snapshot_lock:
        snapshot_dir.mkdir(parents=True, exist_ok=False)
        try:
            if existed and target is not None:
                project_workspace.atomic_copy_file(
                    target, snapshot_dir / "content"
                )
            metadata = {
                "version": 1,
                "token": token,
                "root": str(root),
                "message": str(message),
                "file_path": normalized,
                "existed": existed,
                "before_sha256": before_sha256,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            project_workspace.atomic_write_text(
                snapshot_dir / "metadata.json",
                json.dumps(metadata, ensure_ascii=False, sort_keys=True) + "\n",
            )
        except BaseException:
            shutil.rmtree(snapshot_dir, ignore_errors=True)
            raise
    return token


def _rollback_snapshot(
    root: Path,
    token: str,
    file_path: str | None,
    metadata: dict[str, object],
    expected_after_sha256: str | None,
) -> None:
    expected_root = Path(str(metadata.get("root", ""))).resolve()
    if expected_root != root:
        raise SnapshotError("snapshot belongs to a different project root")
    snapshot_path = metadata.get("file_path")
    if not isinstance(snapshot_path, str) or not snapshot_path:
        raise SnapshotError("scoped rollback requires a file snapshot")
    normalized, target = path_utils.resolve_under_root(root, file_path or snapshot_path)
    if normalized != snapshot_path:
        raise SnapshotError("rollback target does not match snapshot target")
    if bool(metadata.get("existed")):
        content = _snapshot_directory(token) / "content"
        expected_hash = metadata.get("before_sha256")
        actual_hash = project_workspace.sha256_file(content)
        if not isinstance(expected_hash, str) or actual_hash != expected_hash:
            raise SnapshotError("snapshot content failed SHA-256 verification")
        if expected_after_sha256 is None:
            project_workspace.atomic_copy_file(
                content,
                target,
                expected_source_sha256=expected_hash,
            )
        else:
            _assert_rollback_target(target, expected_after_sha256)
            try:
                project_workspace.atomic_copy_file(
                    content,
                    target,
                    expected_before=expected_after_sha256,
                    expected_source_sha256=expected_hash,
                )
            except project_workspace.ConcurrentModificationError as exc:
                raise RollbackConflictError(
                    target,
                    expected_after_sha256,
                    project_workspace.sha256_file(target),
                ) from exc
    else:
        if expected_after_sha256 is not None:
            _assert_rollback_target(target, expected_after_sha256)
        try:
            target.unlink()
        except FileNotFoundError:
            if expected_after_sha256 is not None:
                raise RollbackConflictError(
                    target,
                    expected_after_sha256,
                    None,
                ) from None


def _rollback_legacy_git_snapshot(
    root: Path,
    commit_hash: str,
    file_path: str,
    expected_after_sha256: str | None,
) -> None:
    """Read an old commit snapshot without touching the index or worktree."""
    normalized, target = path_utils.resolve_under_root(root, file_path)
    existed = _run(
        ["git", "cat-file", "-e", f"{commit_hash}:{normalized}"], root
    )
    if existed.returncode != 0:
        if expected_after_sha256 is not None:
            _assert_rollback_target(target, expected_after_sha256)
        try:
            target.unlink()
        except FileNotFoundError:
            if expected_after_sha256 is not None:
                raise RollbackConflictError(
                    target,
                    expected_after_sha256,
                    None,
                ) from None
        return
    result = _run(["git", "show", f"{commit_hash}:{normalized}"], root)
    if result.returncode != 0:
        raise GitError(
            f"rollback read of {normalized} from {commit_hash} failed: "
            f"{result.stderr}"
        )
    if expected_after_sha256 is None:
        project_workspace.atomic_write_text(
            target,
            result.stdout,
            encoding="utf-8",
        )
    else:
        _assert_rollback_target(target, expected_after_sha256)
        try:
            project_workspace.atomic_write_text(
                target,
                result.stdout,
                encoding="utf-8",
                expected_before=expected_after_sha256,
            )
        except project_workspace.ConcurrentModificationError as exc:
            raise RollbackConflictError(
                target,
                expected_after_sha256,
                project_workspace.sha256_file(target),
            ) from exc


def rollback_to(
    root: Path,
    commit_hash: str,
    file_path: str | None = None,
    *,
    expected_after_sha256: str | None = None,
) -> None:
    """Restore a snapshot, optionally CAS-bound to an applied effect hash.

    Effect-backed callers must pass ``expected_after_sha256``. The optional
    form remains for legacy snapshots that predate durable effect receipts.
    """
    root = Path(root).resolve(strict=True)
    expected_after_sha256 = _validated_expected_after(expected_after_sha256)
    metadata = _snapshot_metadata(commit_hash)
    if metadata is not None:
        _rollback_snapshot(
            root,
            commit_hash,
            file_path,
            metadata,
            expected_after_sha256,
        )
        return
    if file_path and _LEGACY_COMMIT.fullmatch(commit_hash):
        _rollback_legacy_git_snapshot(
            root,
            commit_hash,
            file_path,
            expected_after_sha256,
        )
        return
    raise SnapshotError(f"unknown snapshot: {commit_hash}")


def run_syntax_gate(absolute_path: Path) -> tuple[str, str]:
    """A minimal, dependency-free sanity check: for Python files, confirm
    the new content still parses. For other file types this is a no-op
    (pass) -- plug in a real linter/build step per-project as needed."""
    if absolute_path.suffix == ".py":
        try:
            ast.parse(absolute_path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            return "failed", f"SyntaxError: {exc}"
        return "passed", ""
    return "not_applicable", ""


def run_sandbox_tests_detailed(
    root: Path,
    cmd: list[str] | None,
    *,
    backend: SandboxBackend | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> tuple[str, str, dict[str, object]]:
    """Run tests and expose the isolation level actually achieved."""
    details: dict[str, object] = {
        "requested_isolation": IsolationLevel.STRONG.value,
        "actual_isolation": IsolationLevel.NONE.value,
        "isolation_details": "test command was not configured",
        "sandbox_outcome": None,
        "sandbox_backend": None,
        "blocked_reason": None,
        "cancelled": False,
        "timed_out": False,
        "output_truncated": False,
    }
    if not cmd:
        return "not_configured", "", details
    try:
        result = run_sandbox_command(
            root,
            cmd,
            timeout=config.TEST_TIMEOUT_SECONDS,
            isolation_level=IsolationLevel.STRONG,
            backend=backend,
            cancelled=cancelled,
        )
    except (
        SandboxBackendUnavailable,
        SandboxPolicyError,
        OSError,
        ValueError,
    ) as exc:
        details["isolation_details"] = f"command rejected before execution: {exc}"
        return "failed", f"Test command rejected: {exc}", details
    attestation = result.backend_attestation
    details.update(
        {
            "requested_isolation": result.requested_isolation.value,
            "actual_isolation": result.actual_isolation.value,
            "isolation_details": result.isolation_details,
            "sandbox_outcome": result.outcome.value,
            "sandbox_backend": (
                attestation.backend_name if attestation is not None else None
            ),
            "blocked_reason": result.blocked_reason,
            "cancelled": result.cancelled,
            "timed_out": result.timed_out,
            "output_truncated": result.output_truncated,
        }
    )
    output = result.output
    if result.outcome in {
        SandboxOutcome.BLOCKED,
        SandboxOutcome.UNAVAILABLE,
    }:
        reason = result.blocked_reason or result.isolation_details
        return "failed", f"Test command unavailable: {reason}", details
    if result.cancelled:
        return "failed", f"Test command cancelled.\n{output}", details
    if result.timed_out:
        return (
            "failed",
            f"Test command timed out after {config.TEST_TIMEOUT_SECONDS}s.\n{output}",
            details,
        )
    normalized = " ".join(str(part).casefold() for part in cmd)
    if result.returncode == 5 and "pytest" in normalized:
        return "no_tests", output, details
    return ("passed" if result.returncode == 0 else "failed"), output, details


def run_sandbox_tests(
    root: Path,
    cmd: list[str] | None,
    *,
    backend: SandboxBackend | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> tuple[str, str]:
    """Run an arbitrary project test command (e.g. ["pytest", "-x"]) if the
    caller supplied one. The status distinguishes a real pass from a skipped
    gate so downstream agents cannot claim tests ran when none were configured."""
    status, output, _ = run_sandbox_tests_detailed(
        root,
        cmd,
        backend=backend,
        cancelled=cancelled,
    )
    return status, output
