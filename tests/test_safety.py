import hashlib
import sys
from pathlib import Path

import pytest

from orchestrator import patch_engine, safety
from orchestrator.sandbox import (
    STRONG_ISOLATION_CAPABILITIES,
    BackendAttestation,
    IsolationLevel,
    ProcessOnlyBackend,
    SandboxCapability,
)


class _AttestedTestBackend(ProcessOnlyBackend):
    """Unit-test backend; production must wire a real strong backend."""

    def attest(self, root: Path) -> BackendAttestation:
        del root
        return BackendAttestation(
            backend_name="test-strong",
            available=True,
            isolation_level=IsolationLevel.STRONG,
            capabilities=STRONG_ISOLATION_CAPABILITIES
            | frozenset({SandboxCapability.PROCESS_GROUP}),
            details="test-only strong backend attestation",
        )


def test_new_file_snapshot_and_scoped_rollback(monkeypatch, tmp_path: Path):
    snapshot_root = tmp_path.parent / "external-snapshots"
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", snapshot_root)
    target = "src/new_file.py"
    snapshot = safety.backup_commit(tmp_path, "before new file", target)
    assert (snapshot_root / snapshot / "metadata.json").is_file()
    assert not (tmp_path / ".git").exists()
    patch_engine.apply_patch(
        tmp_path,
        target,
        "<patch>\n<<<< SEARCH\n====\nVALUE = 1\n>>>> REPLACE\n</patch>",
        frozenset({target}),
    )
    assert (tmp_path / target).exists()

    safety.rollback_to(
        tmp_path,
        snapshot,
        target,
        expected_after_sha256=hashlib.sha256(b"VALUE = 1").hexdigest(),
    )

    assert not (tmp_path / target).exists()


def test_existing_file_snapshot_restores_bytes_without_git(monkeypatch, tmp_path: Path):
    snapshot_root = tmp_path.parent / "external-existing-snapshots"
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", snapshot_root)
    target = tmp_path / "app.py"
    target.write_bytes(b"VALUE = 1\r\n")
    snapshot = safety.backup_commit(tmp_path, "before patch", "app.py")

    target.write_bytes(b"VALUE = 2\n")
    safety.rollback_to(
        tmp_path,
        snapshot,
        "app.py",
        expected_after_sha256=hashlib.sha256(b"VALUE = 2\n").hexdigest(),
    )

    assert target.read_bytes() == b"VALUE = 1\r\n"
    assert not (tmp_path / ".git").exists()


def test_existing_file_rollback_rejects_unrelated_edit(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")
    target = tmp_path / "app.py"
    target.write_bytes(b"VALUE = 1\n")
    snapshot = safety.backup_commit(tmp_path, "before patch", "app.py")
    applied_hash = hashlib.sha256(b"VALUE = 2\n").hexdigest()
    target.write_bytes(b"VALUE = 3\n")

    with pytest.raises(safety.RollbackConflictError, match="rollback conflict"):
        safety.rollback_to(
            tmp_path,
            snapshot,
            "app.py",
            expected_after_sha256=applied_hash,
        )

    assert target.read_bytes() == b"VALUE = 3\n"


def test_new_file_rollback_rejects_unrelated_edit_before_delete(
    monkeypatch,
    tmp_path: Path,
):
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")
    target = tmp_path / "created.py"
    snapshot = safety.backup_commit(tmp_path, "before create", "created.py")
    applied_hash = hashlib.sha256(b"VALUE = 1\n").hexdigest()
    target.write_bytes(b"UNRELATED = True\n")

    with pytest.raises(safety.RollbackConflictError, match="rollback conflict"):
        safety.rollback_to(
            tmp_path,
            snapshot,
            "created.py",
            expected_after_sha256=applied_hash,
        )

    assert target.read_bytes() == b"UNRELATED = True\n"


def test_gate_status_does_not_call_skipped_checks_passed(tmp_path: Path):
    python_file = tmp_path / "app.py"
    python_file.write_text("VALUE = 1\n", encoding="utf-8")
    text_file = tmp_path / "README.md"
    text_file.write_text("# Project\n", encoding="utf-8")

    assert safety.run_syntax_gate(python_file) == ("passed", "")
    assert safety.run_syntax_gate(text_file) == ("not_applicable", "")
    assert safety.run_sandbox_tests(tmp_path, None) == ("not_configured", "")


def test_pytest_no_tests_is_incomplete_not_failed(tmp_path: Path):
    status, output = safety.run_sandbox_tests(
        tmp_path,
        [
            sys.executable,
            "-c",
            "print('no tests ran'); raise SystemExit(5)",
            "pytest",
        ],
        backend=_AttestedTestBackend(),
    )

    assert status == "no_tests"
    assert output.strip() == "no tests ran"


def test_sandbox_test_timeout_is_reported(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(safety.config, "TEST_TIMEOUT_SECONDS", 0.2)

    status, output = safety.run_sandbox_tests(
        tmp_path,
        [
            sys.executable,
            "-c",
            "import time; print('partial', flush=True); time.sleep(10)",
        ],
        backend=_AttestedTestBackend(),
    )

    assert status == "failed"
    assert "timed out after 0.2s" in output
    assert "partial" in output


def test_sandbox_tests_use_structured_runner(tmp_path: Path):
    status, output = safety.run_sandbox_tests(
        tmp_path,
        [sys.executable, "-c", "print('sandbox-ok')"],
        backend=_AttestedTestBackend(),
    )

    assert status == "passed"
    assert output.strip() == "sandbox-ok"


def test_repository_tests_fail_closed_without_strong_backend(tmp_path: Path):
    marker = tmp_path / "must-not-run"
    status, output, details = safety.run_sandbox_tests_detailed(
        tmp_path,
        [
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')",
        ],
    )

    assert status == "failed"
    assert "unavailable" in output
    assert details["requested_isolation"] == IsolationLevel.STRONG.value
    assert details["actual_isolation"] == IsolationLevel.NONE.value
    assert details["sandbox_outcome"] == "unavailable"
    assert details["sandbox_backend"] == "unavailable"
    assert not marker.exists()
