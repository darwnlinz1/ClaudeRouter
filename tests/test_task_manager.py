import json

import pytest

from orchestrator import task_manager
from orchestrator.models import EventEnvelope
from orchestrator.state_repository import StateRepository


def test_resume_resets_per_run_completion_ledger(monkeypatch, tmp_path):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-resume-ledger",
        name="Resume",
        mode="orchestrator",
        prompt="Resume",
        root=str(tmp_path),
        files=[],
        settings={},
    )
    task_manager.record_event(
        "task-resume-ledger",
        {
            "type": "model_request_started",
            "logical_request_id": "stale-request",
            "role": "worker",
        },
    )
    task_manager.finish_task("task-resume-ledger", status="FAILED")

    resumed = task_manager.resume_task("task-resume-ledger")

    assert resumed["hierarchy"]["execution"]["calls"] == {}
    assert "completion_invariant" not in resumed["hierarchy"]
    task_manager._records.clear()


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


def test_task_snapshot_projects_agent_topology_independently_of_event_retention(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-agent-projection",
        name="Project agents",
        mode="orchestrator",
        prompt="Project agents",
        root=str(tmp_path),
        files=[],
        settings={"hierarchy_enabled": True},
    )

    task_manager.record_event(
        "task-agent-projection",
        {
            "type": "agent_started",
            "agent_instance_id": "tester-stable",
            "role": "tester",
            "manager_id": "manager-stable",
            "workstream_id": "stream-stable",
            "status": "queued",
            "title": "Tester stable",
        },
    )
    task_manager.record_event(
        "task-agent-projection",
        {
            "type": "agent_blocked",
            "agent_instance_id": "tester-stable",
            "role": "tester",
            "manager_id": "manager-stable",
            "workstream_id": "stream-stable",
            "status": "blocked",
            "failure_kind": "dependency",
        },
    )

    task = task_manager.get_task("task-agent-projection")
    assert task["hierarchy"]["agents"]["tester-stable"] == {
        "id": "tester-stable",
        "role": "tester",
        "manager_id": "manager-stable",
        "workstream_id": "stream-stable",
        "status": "blocked",
        "title": "Tester stable",
        "updated_at": task["hierarchy"]["agents"]["tester-stable"]["updated_at"],
    }
    assert "work_item_id" not in task["hierarchy"]["agents"]["tester-stable"]
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
        # One agent, two accounts: the 429 replay moved it to a second cookie,
        # so accounts used exceeds agents called by exactly the one switch.
        "used_accounts": ["a.txt", "b.txt"],
        "accounts_used": 2,
        "last_account_switch": {
            "agent_instance_id": "worker-a",
            "from_account": "a.txt",
            "to_account": "b.txt",
            "reason": "rate_limit",
            "logical_request_id": "llmreq-a",
        },
    }
    task_manager._records.clear()


def test_partial_projection_requires_report_barrier_and_one_final_review(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-partial",
        name="Partial hierarchy",
        mode="orchestrator",
        prompt="Run",
        root=str(tmp_path),
        files=[],
        settings={"hierarchy_enabled": True},
    )
    task_manager.record_event(
        "task-partial",
        {
            "type": "execution_epoch_started",
            "execution_epoch": "epoch-1",
            "expected_manager_ids": ["manager-a", "manager-b"],
            "expected_manager_count": 2,
            "plan_revision": 1,
        },
    )
    for manager_id, stream_id, status in (
        ("manager-a", "a", "completed"),
        ("manager-b", "b", "abandoned"),
    ):
        task_manager.record_event(
            "task-partial",
            {
                "type": "manager_terminal_report",
                "execution_epoch": "epoch-1",
                "manager_id": manager_id,
                "agent_instance_id": manager_id,
                "workstream_id": stream_id,
                "status": status,
                "completed_item_ids": [],
                "abandoned_item_ids": [],
                "skipped_item_ids": [],
                "artifacts": [],
                "reasons": [],
                "log_refs": [],
                "synthesized": False,
            },
        )
    task_manager.record_event(
        "task-partial",
        {
            "type": "manager_report_barrier",
            "execution_epoch": "epoch-1",
            "expected_manager_ids": ["manager-a", "manager-b"],
            "reported_manager_ids": ["manager-a", "manager-b"],
            "expected_count": 2,
            "reported_count": 2,
            "satisfied": True,
        },
    )
    task_manager.record_event(
        "task-partial",
        {
            "type": "director_final_review",
            "execution_epoch": "epoch-1",
            "verdict": "approved",
            "summary": "Partial delivery accepted.",
            "remaining_risks": [],
            "manager_reports_expected": 2,
            "manager_reports_reported": 2,
            "final_review_number": 1,
        },
    )
    task_manager.record_event(
        "task-partial",
        {
            "type": "completion_reconciliation",
            "balanced": True,
            "covered": True,
            "successful": False,
            "errors": [],
            "calls": {"balanced": True},
        },
    )
    task_manager.record_event(
        "task-partial",
        {
            "type": "hierarchy_partial",
            "verdict": "approved",
            "summary": "One workstream was abandoned.",
            "completed_workstream_ids": ["a"],
            "abandoned_workstream_ids": ["b"],
            "skipped_workstream_ids": [],
        },
    )
    before = task_manager.get_task("task-partial")
    task_manager.record_event(
        "task-partial",
        {"type": "agent_progress", "role": "worker", "stage": "late"},
    )
    after = task_manager.get_task("task-partial")

    assert before["status"] == "PARTIAL"
    assert before["hierarchy"]["manager_reports"]["reported_count"] == 2
    assert before["hierarchy"]["manager_report_barrier"]["satisfied"] is True
    assert before["hierarchy"]["director_final_review"]["count"] == 1
    assert after == before
    task_manager._records.clear()


def _seed_partial_hierarchy_barrier(task_id: str) -> None:
    task_manager.record_event(
        task_id,
        {
            "type": "execution_epoch_started",
            "execution_epoch": "epoch-1",
            "expected_manager_ids": ["manager-a", "manager-b"],
            "expected_manager_count": 2,
            "plan_revision": 1,
        },
    )
    for manager_id, stream_id, status in (
        ("manager-a", "a", "completed"),
        ("manager-b", "b", "abandoned"),
    ):
        task_manager.record_event(
            task_id,
            {
                "type": "manager_terminal_report",
                "execution_epoch": "epoch-1",
                "manager_id": manager_id,
                "agent_instance_id": manager_id,
                "workstream_id": stream_id,
                "status": status,
                "completed_item_ids": [],
                "abandoned_item_ids": [],
                "skipped_item_ids": [],
                "artifacts": [],
                "reasons": [],
                "log_refs": [],
                "synthesized": False,
            },
        )
    task_manager.record_event(
        task_id,
        {
            "type": "manager_report_barrier",
            "execution_epoch": "epoch-1",
            "expected_manager_ids": ["manager-a", "manager-b"],
            "reported_manager_ids": ["manager-a", "manager-b"],
            "expected_count": 2,
            "reported_count": 2,
            "satisfied": True,
        },
    )
    task_manager.record_event(
        task_id,
        {
            "type": "director_final_review",
            "execution_epoch": "epoch-1",
            "verdict": "approved",
            "summary": "Partial delivery accepted.",
            "remaining_risks": [],
            "manager_reports_expected": 2,
            "manager_reports_reported": 2,
            "final_review_number": 1,
        },
    )


def test_covered_partial_survives_incomplete_typed_contract_reconciliation(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-partial-typed",
        name="Partial typed",
        mode="orchestrator",
        prompt="Run",
        root=str(tmp_path),
        files=[],
        settings={"hierarchy_enabled": True},
    )
    _seed_partial_hierarchy_barrier("task-partial-typed")
    task_manager.record_event(
        "task-partial-typed",
        {
            "type": "completion_reconciliation",
            "balanced": False,
            "covered": True,
            "successful": False,
            "errors": ["typed contract approval evidence is incomplete"],
            "contract_violations": {"core:api": ["reviewer approval"]},
            "calls": {"balanced": True},
        },
    )
    task_manager.record_event(
        "task-partial-typed",
        {
            "type": "hierarchy_partial",
            "verdict": "approved",
            "summary": "One workstream was abandoned.",
            "completed_workstream_ids": ["a"],
            "abandoned_workstream_ids": ["b"],
            "skipped_workstream_ids": [],
        },
    )
    task_manager.finish_task(
        "task-partial-typed",
        status="PARTIAL",
        reason="task_partial",
    )
    record = task_manager.get_task("task-partial-typed")

    assert record["status"] == "PARTIAL"
    assert record["finished_at"]
    assert record["stopped_reason"] == "task_partial"
    assert record["last_error"] is None
    assert record["hierarchy"]["reconciliation"]["covered"] is True
    assert record["hierarchy"]["reconciliation"]["successful"] is False
    task_manager._records.clear()


def test_finish_task_stamps_finished_at_when_terminal_status_already_differs(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-finished-at",
        name="Coverage failed",
        mode="orchestrator",
        prompt="Run",
        root=str(tmp_path),
        files=[],
        settings={"hierarchy_enabled": True},
    )
    task_manager.record_event(
        "task-finished-at",
        {
            "type": "completion_reconciliation",
            "balanced": False,
            "covered": False,
            "successful": False,
            "errors": ["manager report barrier is incomplete"],
            "calls": {"balanced": True},
        },
    )
    before = task_manager.get_task("task-finished-at")
    assert before["status"] == "FAILED"
    assert before["finished_at"] is None

    task_manager.finish_task(
        "task-finished-at",
        status="PARTIAL",
        reason="task_partial",
    )
    after = task_manager.get_task("task-finished-at")

    assert after["status"] == "FAILED"
    assert after["finished_at"]
    assert after["last_error"] == "manager report barrier is incomplete"
    task_manager._records.clear()


def test_auto_resumable_list_only_returns_interrupted_enabled_tasks(monkeypatch, tmp_path):
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
            assert repository.data_migration(task_manager._LEGACY_IMPORT_KEY) is not None

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
                        "status": "RUNNING",
                        "phase": "execution",
                        "settings": {},
                        "updated_at": f"2026-01-01T00:00:{number % 60:02d}+00:00",
                    },
                )
            task_manager.configure_repository(repository)
            assert task_manager.get_task("task-124")["status"] == "RUNNING"
            interrupted = task_manager.interrupt_active_tasks()
            assert len(interrupted) == 250
            statements = []
            repository._connection.set_trace_callback(statements.append)
            task_manager.set_status("task-125", "REVISION", "review")
            repository._connection.set_trace_callback(None)

            writes = [
                statement
                for statement in statements
                if statement.lstrip().upper().startswith("INSERT INTO TASK_SNAPSHOTS")
            ]
            assert len(writes) == 1
            assert task_manager.get_task("task-124")["status"] == "INTERRUPTED"
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
