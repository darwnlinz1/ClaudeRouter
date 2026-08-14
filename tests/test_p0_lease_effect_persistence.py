import os
import subprocess
import sys
from pathlib import Path

from orchestrator.effects import EffectState
from orchestrator.project_workspace import (
    ProjectLeaseManager,
    commit_prepared_file,
    prepare_atomic_write_text,
    sha256_file,
)
from orchestrator.state_repository import StateRepository


def test_project_os_lock_excludes_duplicate_owner_across_processes(
    tmp_path: Path,
    monkeypatch,
):
    database = tmp_path / "shared.sqlite3"
    lock_root = tmp_path / "locks"
    monkeypatch.setenv("ORCHESTRATOR_PROJECT_LOCK_ROOT", str(lock_root))
    script = """
import sys
from orchestrator.project_workspace import ProjectLeaseManager
from orchestrator.state_repository import StateRepository

with StateRepository(sys.argv[1]) as repository:
    handle = ProjectLeaseManager(repository).acquire("project-a", "owner-a", 30)
    print("locked" if handle is not None else "failed", flush=True)
    sys.stdin.readline()
    if handle is not None:
        handle.release()
"""
    environment = os.environ.copy()
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(database)],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "locked"
        with StateRepository(database) as repository:
            manager = ProjectLeaseManager(repository)
            assert manager.acquire("project-a", "owner-a", 30) is None
            assert manager.acquire("project-a", "owner-b", 30) is None
    finally:
        if child.stdin is not None:
            child.stdin.write("\n")
            child.stdin.flush()
        _, stderr = child.communicate(timeout=10)
        assert child.returncode == 0, stderr

    with StateRepository(database) as repository:
        acquired = ProjectLeaseManager(repository).acquire(
            "project-a",
            "owner-b",
            30,
        )
        assert acquired is not None
        assert acquired.fencing_token >= 2
        acquired.release()


def test_pending_file_effect_reconciles_and_links_compensation(tmp_path: Path):
    database = tmp_path / "effects.sqlite3"
    target = tmp_path / "value.txt"
    target.write_text("before\n", encoding="utf-8")
    before_hash = sha256_file(target)
    prepared = prepare_atomic_write_text(
        target,
        "after\n",
        expected_before=before_hash,
    )

    with StateRepository(database) as repository:
        receipt = repository.begin_effect(
            "task-a",
            "write:value:v1",
            "file_patch",
            "value.txt",
            before_sha256=before_hash,
            expected_after_sha256=prepared.after_sha256,
            fencing_token=7,
        )
        assert target.read_text(encoding="utf-8") == "before\n"
        committed = commit_prepared_file(prepared)
        assert committed.after_sha256 == receipt.expected_after_sha256

    with StateRepository(database) as repository:
        recovered = repository.reconcile_pending_effect_by_target_hash(
            receipt.effect_id,
            sha256_file(target),
            fencing_token=7,
        )
        assert recovered is not None
        assert recovered.state is EffectState.RECONCILED

        rollback = prepare_atomic_write_text(
            target,
            "before\n",
            expected_before=recovered.after_sha256,
        )
        compensation = repository.begin_effect(
            "task-a",
            "rollback:value:v1",
            "file_rollback",
            "value.txt",
            before_sha256=recovered.after_sha256,
            expected_after_sha256=rollback.after_sha256,
            fencing_token=8,
            compensates_effect_id=recovered.effect_id,
        )
        rollback_result = commit_prepared_file(rollback)
        repository.complete_effect(
            compensation.effect_id,
            after_sha256=rollback_result.after_sha256,
            fencing_token=8,
        )

        original = repository.get_effect(recovered.effect_id)
        assert original is not None
        assert original.compensated_by_effect_id == compensation.effect_id
        assert original.compensated_at is not None
