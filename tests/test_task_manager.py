import json

import pytest

from orchestrator import task_manager
from orchestrator.models import EventEnvelope
from orchestrator.state_repository import StateRepository


def test_task_manager_tracks_reviewed_file_stats(monkeypatch, tmp_path):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-1",
        name="Update API",
        mode="orchestrator",
        prompt="Update API",
        root=str(tmp_path),
        files=["server.py"],
        settings={"worker_model": "test-model"},
    )

    task_manager.record_event(
        "task-1",
        {
            "type": "execution_result",
            "file_path": "server.py",
            "accepted": True,
            "additions": 12,
            "deletions": 3,
            "worker_feedback": "Updated endpoint.",
            "execution_result": "Pass",
        },
    )
    task_manager.finish_task("task-1", status="COMPLETED")

    task = task_manager.get_task("task-1")
    assert task["status"] == "COMPLETED"
    assert task["changed_files"]["server.py"] == {
        "additions": 12,
        "deletions": 3,
    }
    assert (tmp_path / "tasks.json").exists()
    task_manager._records.clear()


def test_agent_progress_tracks_each_role_phase(monkeypatch, tmp_path):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-agents",
        name="Trace agents",
        mode="orchestrator",
        prompt="Trace agents",
        root=str(tmp_path),
        files=[],
        settings={},
    )

    task_manager.record_event(
        "task-agents",
        {
            "type": "agent_progress",
            "role": "worker",
            "stage": "patching",
            "message": "Building patch",
        },
    )

    task = task_manager.get_task("task-agents")
    assert task["status"] == "CODING"
    assert task["phase"] == "patching"
    assert task["current_agent"] == "worker"
    task_manager._records.clear()


def test_task_manager_persists_fanout_calls_and_429_replay(monkeypatch, tmp_path):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-fanout",
        name="Trace fanout",
        mode="orchestrator",
        prompt="Trace fanout",
        root=str(tmp_path),
        files=[],
        settings={"hierarchy_enabled": True},
    )
    task_manager.record_event(
        "task-fanout",
        {
            "type": "hierarchy_fanout_planned",
            "manager_count": 4,
            "coder_count": 16,
            "tester_count": 4,
            "child_agent_count": 20,
            "max_parallel_managers": 4,
            "max_parallel_workers": 8,
        },
    )
    task_manager.record_event(
        "task-fanout",
        {
            "type": "fanout_selected",
            "level": "manager",
            "maximum": 6,
            "selected": 4,
            "unused_capacity": 2,
            "reason": "Four independent domains",
        },
    )
    for replayed, account in ((False, "a.txt"), (True, "b.txt")):
        task_manager.record_event(
            "task-fanout",
            {
                "type": "model_request_started",
                "role": "worker",
                "agent_instance_id": "worker-a",
                "account": account,
                "replayed": replayed,
            },
        )
    task_manager.record_event(
        "task-fanout",
        {
            "type": "account_switch",
            "agent_instance_id": "worker-a",
            "from_account": "a.txt",
            "to_account": "b.txt",
            "reason": "rate_limit",
            "logical_request_id": "llmreq-a",
        },
    )
    task_manager.record_event(
        "task-fanout",
        {
            "type": "model_request_completed",
            "role": "worker",
            "agent_instance_id": "worker-a",
            "account": "b.txt",
        },
    )
    task_manager.record_event(
        "task-fanout",
        {
            "type": "completion_reconciliation",
            "balanced": True,
            "errors": [],
            "workstreams": {"planned": 4, "terminal": 4, "balanced": True},
            "work_items": {"planned": 16, "terminal": 16, "balanced": True},
            "agents": {"planned": 25, "terminal": 25, "balanced": True},
            "calls": {"planned": 1, "terminal": 1, "balanced": True},
        },
    )

    hierarchy = task_manager.get_task("task-fanout")["hierarchy"]
    assert hierarchy["fanout"]["child_agent_count"] == 20
    assert hierarchy["fanout_selections"]["manager"]["selected"] == 4
    assert hierarchy["reconciliation"]["balanced"] is True
    assert hierarchy["execution"] == {
        "request_attempts": 2,
        "completed_requests": 1,
        "replayed_requests": 1,
        "account_switches": 1,
        "called_agent_ids": ["worker-a"],
        "completed_agent_ids": ["worker-a"],
        "called_by_role": {"worker": 1},
        "last_account_switch": {
            "agent_instance_id": "worker-a",
            "from_account": "a.txt",
            "to_account": "b.txt",
            "reason": "rate_limit",
            "logical_request_id": "llmreq-a",
        },
    }
    task_manager._records.clear()


def test_auto_resumable_list_only_returns_interrupted_enabled_tasks(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    for task_id, enabled in (("auto", True), ("manual", False)):
        task_manager.create_task(
            task_id,
            name=task_id,
            mode="orchestrator",
            prompt="Continue",
            root=str(tmp_path),
            files=[],
            settings={"auto_continue": enabled},
        )
        task_manager.set_status(task_id, "INTERRUPTED", "interrupted")

    tasks = task_manager.list_auto_resumable_tasks()

    assert [task["id"] for task in tasks] == ["auto"]
    task_manager._records.clear()


def test_task_manager_tracks_terminal_model_failures(monkeypatch, tmp_path):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-failed-call",
        name="Failed call",
        mode="orchestrator",
        prompt="Trace terminal calls",
        root=str(tmp_path),
        files=[],
        settings={"hierarchy_enabled": True},
    )

    task_manager.record_event(
        "task-failed-call",
        {
            "type": "model_request_failed",
            "role": "worker",
            "agent_instance_id": "worker-1",
            "call_id": "call-1",
            "error": "transport failed",
        },
    )

    task = task_manager.get_task("task-failed-call")
    assert task["status"] == "REVISION"
    assert task["hierarchy"]["execution"]["failed_requests"] == 1
    assert task["hierarchy"]["execution"]["last_terminal_error"] == {
        "call_id": "call-1",
        "type": "model_request_failed",
        "error": "transport failed",
        "role": "worker",
        "agent_instance_id": "worker-1",
    }
    task_manager._records.clear()


def test_stale_legacy_json_cannot_resurrect_deleted_task(monkeypatch, tmp_path):
    legacy_file = tmp_path / "tasks.json"
    database = tmp_path / "state.sqlite3"
    record = {
        "legacy": {
            "id": "legacy",
            "name": "Legacy",
            "mode": "orchestrator",
            "status": "COMPLETED",
            "phase": "finished",
            "settings": {},
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
    }
    legacy_file.write_text(json.dumps(record), encoding="utf-8")
    monkeypatch.setattr(task_manager, "TASKS_FILE", legacy_file)
    monkeypatch.setattr(task_manager, "_LEGACY_TASKS_FILE", legacy_file)
    monkeypatch.setattr(
        task_manager.os,
        "replace",
        lambda *_: (_ for _ in ()).throw(OSError("injected archive crash")),
    )

    try:
        with StateRepository(database) as repository:
            task_manager.configure_repository(repository)
            assert task_manager.get_task("legacy") is not None
            assert legacy_file.is_file()
            assert task_manager.delete_task("legacy")
            assert repository.data_migration(
                task_manager._LEGACY_IMPORT_KEY
            ) is not None

            # Simulate restart after import committed but archival crashed.
            task_manager.configure_repository(repository)
            assert task_manager.get_task("legacy") is None
            assert repository.get_task_snapshot("legacy") is None
    finally:
        task_manager.configure_repository(None)
        task_manager._records.clear()


def test_repository_mutation_writes_only_target_task(monkeypatch, tmp_path):
    legacy_file = tmp_path / "tasks.json"
    monkeypatch.setattr(task_manager, "TASKS_FILE", legacy_file)
    monkeypatch.setattr(task_manager, "_LEGACY_TASKS_FILE", legacy_file)

    try:
        with StateRepository(tmp_path / "state.sqlite3") as repository:
            for number in range(250):
                repository.save_task_snapshot(
                    f"task-{number}",
                    {
                        "id": f"task-{number}",
                        "name": f"Task {number}",
                        "mode": "orchestrator",
                        "status": "COMPLETED",
                        "phase": "finished",
                        "settings": {},
                        "updated_at": f"2026-01-01T00:00:{number % 60:02d}+00:00",
                    },
                )
            task_manager.configure_repository(repository)
            statements = []
            repository._connection.set_trace_callback(statements.append)
            task_manager.set_status("task-125", "REVISION", "review")
            repository._connection.set_trace_callback(None)

            writes = [
                statement
                for statement in statements
                if statement.lstrip().upper().startswith(
                    "INSERT INTO TASK_SNAPSHOTS"
                )
            ]
            assert len(writes) == 1
            assert task_manager.get_task("task-124")["status"] == "COMPLETED"
            assert task_manager.get_task("task-125")["status"] == "REVISION"
    finally:
        task_manager.configure_repository(None)
        task_manager._records.clear()


def test_atomic_event_hook_updates_projection_without_snapshot_events(
    monkeypatch,
    tmp_path,
):
    legacy_file = tmp_path / "tasks.json"
    monkeypatch.setattr(task_manager, "TASKS_FILE", legacy_file)
    monkeypatch.setattr(task_manager, "_LEGACY_TASKS_FILE", legacy_file)

    try:
        with StateRepository(tmp_path / "state.sqlite3") as repository:
            task_manager.configure_repository(repository)
            task_manager.create_task(
                "task-atomic",
                name="Atomic",
                mode="orchestrator",
                prompt="Run",
                root=str(tmp_path),
                files=[],
                settings={},
            )
            envelope = EventEnvelope(
                task_id="task-atomic",
                session_id="session-atomic",
                event_type="status",
                payload={"data": "running", "role": "worker"},
            )

            stored = task_manager.record_event(
                "task-atomic",
                {"type": "status", "data": "running", "role": "worker"},
                envelope=envelope,
            )
            retried = task_manager.record_event(
                "task-atomic",
                {"type": "status", "data": "running", "role": "worker"},
                envelope=envelope,
            )

            assert retried == stored
            assert repository.replay_events("task-atomic") == [stored]
            snapshot = repository.get_task_snapshot("task-atomic")
            assert snapshot["status"] == "RUNNING"
            assert snapshot["current_agent"] == "worker"
            assert "events" not in snapshot
    finally:
        task_manager.configure_repository(None)
        task_manager._records.clear()


def test_invalid_legacy_json_is_not_marked_imported(monkeypatch, tmp_path):
    legacy_file = tmp_path / "tasks.json"
    legacy_file.write_text("{invalid", encoding="utf-8")
    monkeypatch.setattr(task_manager, "TASKS_FILE", legacy_file)
    monkeypatch.setattr(task_manager, "_LEGACY_TASKS_FILE", legacy_file)

    try:
        with StateRepository(tmp_path / "state.sqlite3") as repository:
            with pytest.raises(RuntimeError, match="not valid JSON"):
                task_manager.configure_repository(repository)

            assert repository.data_migration(task_manager._LEGACY_IMPORT_KEY) is None
            assert legacy_file.read_text(encoding="utf-8") == "{invalid"
    finally:
        task_manager.configure_repository(None)
        task_manager._records.clear()
