from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

import server
from orchestrator.event_broker import ReplayEventBroker


@pytest.fixture(autouse=True)
def isolated_task_store(monkeypatch, tmp_path):
    monkeypatch.setattr(
        server.task_manager,
        "TASKS_FILE",
        tmp_path / "tasks.json",
    )
    monkeypatch.setattr(
        server.artifact_manager,
        "ARTIFACTS_ROOT",
        tmp_path / "artifacts",
    )
    server.task_manager._records.clear()
    server.active_queues.clear()
    server.chat_input_queues.clear()
    server.stop_flags.clear()
    yield
    server.task_manager._records.clear()
    server.active_queues.clear()
    server.chat_input_queues.clear()
    server.stop_flags.clear()


class _FakeThread:
    last_args = None

    def __init__(self, target, args, daemon):
        self.target = target
        self.args = args
        self.daemon = daemon
        _FakeThread.last_args = args

    def start(self):
        return None


class _ImmediateThread(_FakeThread):
    def start(self):
        self.target(*self.args)


def test_chat_mode_does_not_require_project_root(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)

    response = server.run_task(
        server.TaskRequest(root="", task="Xin chào", files="", mode="chat")
    )

    assert response["status"] == "started"
    assert _FakeThread.last_args[1] == Path.cwd().resolve()
    stored = server.get_task(response["task_id"])
    assert stored["name"] == "Xin chào"
    assert stored["mode"] == "chat"
    server.active_queues.pop(response["task_id"], None)


def test_orchestrator_mode_requires_project_root():
    with pytest.raises(HTTPException) as exc_info:
        server.run_task(
            server.TaskRequest(
                root="",
                task="Sửa code",
                files="",
                mode="orchestrator",
            )
        )

    assert exc_info.value.status_code == 400


def test_dashboard_lists_persisted_tasks(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    first = server.run_task(
        server.TaskRequest(
            name="Task dashboard",
            root="",
            task="Hello",
            files="",
            mode="chat",
        )
    )

    tasks = server.list_tasks()["tasks"]
    assert [task["id"] for task in tasks] == [first["task_id"]]
    assert tasks[0]["name"] == "Task dashboard"
    assert "events" not in tasks[0]
    server.active_queues.pop(first["task_id"], None)


def test_task_uses_user_selected_max_turns(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)

    created = server.run_task(
        server.TaskRequest(
            root="",
            task="Custom turn budget",
            files="",
            mode="chat",
            max_turns=77,
        )
    )

    stored = server.get_task(created["task_id"])
    assert stored["settings"]["max_turns"] == 77
    assert _FakeThread.last_args[16] == 77


@pytest.mark.parametrize("value", [0, 501])
def test_task_rejects_invalid_max_turns(value):
    with pytest.raises(ValidationError):
        server.TaskRequest(
            root="",
            task="Invalid turns",
            files="",
            mode="chat",
            max_turns=value,
        )


def test_manual_resume_reuses_same_task_and_settings(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    created = server.run_task(
        server.TaskRequest(
            root=str(tmp_path),
            task="Long orchestrator task",
            files="",
            mode="orchestrator",
            max_turns=44,
            auto_continue=True,
        )
    )
    task_id = created["task_id"]
    server.task_manager.finish_task(
        task_id, status="MAX_TURNS", reason="max_turns_reached"
    )

    resumed = server.resume_task(task_id)

    assert resumed["task_id"] == task_id
    assert server.get_task(task_id)["status"] == "RESUMING"
    assert _FakeThread.last_args[16] == 44
    assert _FakeThread.last_args[17] is True
    assert _FakeThread.last_args[18] is True


def test_auto_continue_runs_next_turn_cycle(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    calls = []

    def fake_run_session(root, **kwargs):
        calls.append(kwargs["resume_session"])
        stopped_reason = (
            "max_turns_reached" if len(calls) == 1 else "task_completed"
        )
        return type(
            "Result",
            (),
            {
                "stopped_reason": stopped_reason,
                "turns": [],
                "final_state": {
                    "turn_count": len(calls),
                    "last_execution_result": "Pass",
                    "last_review_verdict": "approved",
                },
            },
        )()

    monkeypatch.setattr(server, "run_session", fake_run_session)

    created = server.run_task(
        server.TaskRequest(
            root=str(tmp_path),
            task="Continue automatically",
            files="",
            mode="orchestrator",
            max_turns=1,
            auto_continue=True,
        )
    )

    assert calls == [False, True]
    assert server.get_task(created["task_id"])["status"] == "COMPLETED"


def test_hierarchy_task_uses_dynamic_scheduler_limits(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    captured = {}

    def fake_run_hierarchy(**kwargs):
        captured.update(kwargs)
        return type(
            "Result",
            (),
            {
                "stopped_reason": "task_completed",
                "turns": [],
                "final_state": {
                    "turn_count": 2,
                    "completed_tickets": [],
                    "last_review_verdict": "approved",
                },
            },
        )()

    monkeypatch.setattr(server, "run_hierarchy", fake_run_hierarchy)
    created = server.run_task(
        server.TaskRequest(
            root=str(tmp_path),
            task="Build with hierarchy",
            mode="orchestrator",
            hierarchy_enabled=True,
            max_parallel_managers=3,
            max_workers_per_manager=5,
            max_parallel_workers=9,
        )
    )

    assert captured["task_id"] == created["task_id"]
    assert captured["limits"].max_parallel_managers == 3
    assert captured["limits"].max_workers_per_manager == 5
    assert captured["limits"].max_parallel_workers == 9
    assert server.get_task(created["task_id"])["status"] == "COMPLETED"


def test_agent_model_override_is_persisted_for_next_call(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    created = server.run_task(
        server.TaskRequest(root="", task="Agent config", mode="chat")
    )

    response = server.update_agent_config(
        created["task_id"],
        "worker-1",
        server.AgentConfigRequest(
            model="claude-sonnet-4-6",
            effort="high",
        ),
    )

    assert response["status"] == "saved"
    assert response["applies_to"] == "next_model_call"
    assert server.task_manager.get_agent_override(
        created["task_id"], "worker-1"
    )["model"] == "claude-sonnet-4-6"


def test_terminal_task_can_be_deleted_even_if_stream_queue_remains(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    created = server.run_task(
        server.TaskRequest(root="", task="Hello", files="", mode="chat")
    )
    task_id = created["task_id"]
    server.task_manager.finish_task(task_id, status="COMPLETED")

    assert server.delete_task(task_id)["status"] == "deleted"
    assert task_id not in server.active_queues
    assert server.task_manager.get_task(task_id) is None


def test_stop_marks_dashboard_task_as_stopping(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    created = server.run_task(
        server.TaskRequest(root="", task="Hello", files="", mode="chat")
    )

    server.stop_task(created["task_id"])

    assert server.task_manager.get_task(created["task_id"])["status"] == "STOPPING"


def test_stream_replays_only_events_after_client_sequence():
    task_id = "reconnect-task"
    broker = ReplayEventBroker()
    server.active_queues[task_id] = broker
    broker.put({"type": "status", "data": "planning"})
    broker.put({"type": "status", "data": "reviewing"})
    broker.put({"type": "done"})

    response = TestClient(server.app).get(f"/api/stream/{task_id}?after=1")

    assert response.status_code == 200
    assert "planning" not in response.text
    assert "reviewing" in response.text
    assert '"_seq": 2' in response.text
    assert task_id in server.active_queues


def test_new_project_accepts_empty_destination(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    destination = tmp_path / "jarvis"
    destination.mkdir()

    created = server.run_task(
        server.TaskRequest(
            name="Build Jarvis",
            root=str(destination),
            task="Create a local assistant",
            files="",
            mode="orchestrator",
            project_mode="new_project",
            auto_apply=True,
            create_zip=True,
        )
    )

    task = server.get_task(created["task_id"])
    assert task["project_mode"] == "new_project"
    assert task["settings"]["auto_apply"] is True


def test_new_project_background_finalizes_artifact(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)

    def fake_run_session(root, **kwargs):
        assert kwargs["max_turns"] == 7
        (root / "main.py").write_text("print('jarvis')\n", encoding="utf-8")
        kwargs["on_file_approved"]("main.py")
        assert (destination / "main.py").read_text(
            encoding="utf-8"
        ) == "print('jarvis')\n"
        return type(
            "Result",
            (),
            {
                "stopped_reason": "task_completed",
                "turns": [],
                "final_state": {
                    "turn_count": 1,
                    "last_execution_result": "Pass",
                    "last_review_verdict": "approved",
                },
            },
        )()

    monkeypatch.setattr(server, "run_session", fake_run_session)
    destination = tmp_path / "jarvis"
    destination.mkdir()

    created = server.run_task(
        server.TaskRequest(
            name="Jarvis",
            root=str(destination),
            task="Build Jarvis",
            files="",
            mode="orchestrator",
            project_mode="new_project",
            auto_apply=True,
            create_zip=True,
            max_turns=7,
        )
    )

    task = server.get_task(created["task_id"])
    assert task["status"] == "COMPLETED"
    assert task["artifact"]["status"] == "applied"
    assert (destination / "main.py").read_text(encoding="utf-8") == "print('jarvis')\n"
    assert server.artifact_manager.get_zip_path(created["task_id"]).is_file()
