import threading
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

import server
from orchestrator.event_broker import ReplayEventBroker
from orchestrator.state_repository import StateRepository


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


def test_startup_recovery_is_limited_to_the_cli_owner_process(monkeypatch):
    monkeypatch.delenv("ORCH_RECOVERY_OWNER_PID", raising=False)
    assert server._owns_startup_recovery() is False

    monkeypatch.setenv("ORCH_RECOVERY_OWNER_PID", str(server.os.getpid()))
    assert server._owns_startup_recovery() is True

    monkeypatch.setenv("ORCH_RECOVERY_OWNER_PID", str(server.os.getpid() + 1))
    assert server._owns_startup_recovery() is False


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

    response = server.run_task(server.TaskRequest(root="", task="Xin chào", files="", mode="chat"))

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


def test_account_health_endpoint_exposes_state_without_credentials(monkeypatch):
    class Store:
        def list_health(self, *, now):
            return [
                {
                    "account_id": "cookie-a.txt",
                    "provider": "legacy_web",
                    "state": "cooldown",
                    "cooldown_active": True,
                    "active_leases": 0,
                }
            ]

        def list_active(self, *, now):
            return []

    monkeypatch.setattr(server, "_ensure_account_lease_store", lambda: Store())

    result = server.get_account_health()

    assert result["accounts"][0]["state"] == "cooldown"
    assert "cookie_string" not in result["accounts"][0]


def test_request_attempt_endpoint_hides_large_content_by_default(monkeypatch):
    class Repository:
        def list_llm_request_attempts(self, task_id, *, schema_errors_only, limit):
            assert task_id == "task-request-log"
            assert schema_errors_only is True
            assert limit == 5
            return [
                {
                    "attempt_id": "attempt-1",
                    "response_status": 400,
                    "error_classification": "provider_conversation_input",
                    "logical_request": {"rendered_prompt": "full prompt"},
                    "tool_schema": [{"name": "submit_patch"}],
                    "wire_body": {"prompt": "full prompt"},
                    "response_body": {"error": "bad request"},
                    "parser_result": {"parsed": False},
                }
            ]

    monkeypatch.setattr(server, "hierarchy_repository", Repository())
    monkeypatch.setattr(server.task_manager, "get_task", lambda task_id: {"id": task_id})

    compact = server.get_task_request_attempts(
        "task-request-log",
        limit=5,
        schema_errors_only=True,
    )
    assert compact["attempts"][0]["response_status"] == 400
    assert "logical_request" not in compact["attempts"][0]
    assert "wire_body" not in compact["attempts"][0]

    full = server.get_task_request_attempts(
        "task-request-log",
        limit=5,
        schema_errors_only=True,
        include_content=True,
    )
    assert full["attempts"][0]["wire_body"]["prompt"] == "full prompt"


def test_operator_can_explicitly_delete_quarantined_credential(monkeypatch):
    deleted = []

    class Manager:
        def delete_credential(self, source):
            deleted.append(source)
            return True

    monkeypatch.setattr(server.llm_runtime, "cookie_manager", Manager())

    assert server.delete_account_credential("disabled.txt") == {
        "status": "deleted",
        "source": "disabled.txt",
    }
    assert deleted == ["disabled.txt"]


def test_approval_api_persists_and_decides_operator_action(
    monkeypatch,
    tmp_path,
):
    with StateRepository(tmp_path / "approvals.sqlite3") as repository:
        monkeypatch.setattr(server, "hierarchy_repository", repository)
        monkeypatch.setattr(
            server.task_manager,
            "get_task",
            lambda task_id: {"id": task_id},
        )
        monkeypatch.setattr(
            server.task_manager,
            "record_event",
            lambda *args, **kwargs: None,
        )

        requested = server.request_task_approval(
            "task-a",
            server.ApprovalRequest(
                idempotency_key="patch:item-a:sha",
                kind="patch_apply",
                target="src/app.py",
                reason="high risk",
            ),
        )
        decided = server.decide_task_approval(
            "task-a",
            requested["approval_id"],
            server.ApprovalDecisionRequest(decision="approved"),
        )

        assert decided["status"] == "approved"
        assert server.get_task_approvals("task-a")["approvals"] == [decided]


def test_local_api_requires_same_origin_session_and_csrf(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    client = TestClient(server.app)

    denied = client.post(
        "/api/run",
        json={"root": "", "task": "Hello", "files": "", "mode": "chat"},
    )
    assert denied.status_code == 403

    session = client.get("/api/session")
    assert session.status_code == 200
    csrf = session.json()["csrf_token"]
    allowed = client.post(
        "/api/run",
        headers={"X-CSRF-Token": csrf, "Origin": "http://testserver"},
        json={"root": "", "task": "Hello", "files": "", "mode": "chat"},
    )
    assert allowed.status_code == 200

    cross_origin = client.get(
        "/api/tasks",
        headers={"Origin": "https://evil.example"},
    )
    assert cross_origin.status_code == 403
    rebound_host = client.get(
        "/api/tasks",
        headers={
            "Host": "evil.example",
            "Origin": "http://evil.example",
        },
    )
    assert rebound_host.status_code == 403


@pytest.mark.parametrize(
    "unknown_field",
    [
        "api_key",
        "provider_key",
        "provider",
        "provider_config",
        "max_model_calls",
        "max_wall_clock_seconds",
        "max_estimated_input_tokens",
        "unexpected",
    ],
)
def test_run_api_rejects_removed_or_unknown_fields(unknown_field):
    client = TestClient(server.app)
    session = client.get("/api/session")
    csrf = session.json()["csrf_token"]
    payload = {
        "root": "",
        "task": "Hello",
        "files": "",
        "mode": "chat",
        unknown_field: "must-not-be-accepted",
    }

    response = client.post(
        "/api/run",
        headers={"X-CSRF-Token": csrf, "Origin": "http://testserver"},
        json=payload,
    )

    assert response.status_code == 422
    assert any(
        item["type"] == "extra_forbidden" and item["loc"] == ["body", unknown_field]
        for item in response.json()["detail"]
    )


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (
            server.TaskRequest,
            {"root": "", "task": "Hello", "files": "", "mode": "chat"},
        ),
        (server.ChatReplyRequest, {"message": "Continue"}),
        (
            server.AgentConfigRequest,
            {"model": "claude-sonnet-5", "effort": "high"},
        ),
        (
            server.ApprovalRequest,
            {
                "idempotency_key": "approval:key",
                "kind": "patch_apply",
                "target": "src/app.py",
                "reason": "required",
            },
        ),
        (server.ApprovalDecisionRequest, {"decision": "approved"}),
        (server.ArtifactRetentionRequest, {"pinned": True}),
    ],
)
def test_every_inbound_request_model_forbids_unknown_fields(model, payload):
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model(**payload, provider_key="must-not-be-accepted")


def test_local_boundary_applies_to_every_route_and_checks_peer():
    client = TestClient(server.app)

    denied = client.get("/legacy", headers={"Host": "evil.example"})

    assert denied.status_code == 403
    non_loopback = Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [(b"host", b"localhost")],
            "client": ("192.0.2.10", 1234),
            "server": ("127.0.0.1", 8000),
        }
    )
    loopback = Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [(b"host", b"localhost")],
            "client": ("127.0.0.1", 1234),
            "server": ("127.0.0.1", 8000),
        }
    )
    assert server._loopback_peer(non_loopback) is False
    assert server._loopback_peer(loopback) is True


def test_legacy_routes_are_gone_and_security_headers_cover_404s():
    client = TestClient(server.app)

    for path in ("/legacy", "/task/task-a", "/main.js", "/api.js", "/style.css"):
        response = client.get(path)
        assert response.status_code == 404
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["x-frame-options"] == "DENY"
        assert response.headers["referrer-policy"] == "no-referrer"
        assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
        assert response.headers["permissions-policy"] == (
            "camera=(), microphone=(), geolocation=()"
        )


def test_malicious_working_directory_is_never_used_for_frontend(
    monkeypatch,
    tmp_path,
):
    (tmp_path / "index.html").write_text(
        "<script>window.pwned = true</script>",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        server,
        "FRONTEND_DIST",
        server._PROJECT_ROOT / "frontend" / "definitely-missing-dist",
    )

    response = TestClient(server.app).get("/")

    assert response.status_code == 503
    assert "pwned" not in response.text
    assert "default-src 'self'" in response.headers["content-security-policy"]


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


def test_task_accepts_test_command_as_argv(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    command = [
        r"C:\Program Files\Python\python.exe",
        "-m",
        "pytest",
        r"tests\unit tests\test_app.py",
    ]

    created = server.run_task(
        server.TaskRequest(
            root="",
            task="Run explicit argv",
            files="",
            mode="chat",
            test_cmd=command,
        )
    )

    assert _FakeThread.last_args[15] == command
    assert server.get_task(created["task_id"])["settings"]["test_cmd"] == command


def test_legacy_test_command_uses_windows_quoting_rules(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    legacy = (
        r'"C:\Program Files\Python\python.exe" -m pytest '
        r'"tests\unit tests\test_app.py" -k "name with spaces"'
    )

    created = server.run_task(
        server.TaskRequest(
            root="",
            task="Run legacy command",
            files="",
            mode="chat",
            test_cmd=legacy,
        )
    )

    expected = [
        r"C:\Program Files\Python\python.exe",
        "-m",
        "pytest",
        r"tests\unit tests\test_app.py",
        "-k",
        "name with spaces",
    ]
    assert _FakeThread.last_args[15] == expected
    assert server.get_task(created["task_id"])["settings"]["test_cmd"] == expected
    assert server._split_windows_command_line(r'python -c "print(\"ok\")"') == [
        "python",
        "-c",
        'print("ok")',
    ]


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


def test_task_approval_mode_defaults_to_manual():
    request = server.TaskRequest(
        root="",
        task="Manual approval by default",
        mode="chat",
    )

    assert request.approval_mode == "manual"


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
    server.task_manager.finish_task(task_id, status="MAX_TURNS", reason="max_turns_reached")
    stale_runtime = server.runtime_registry.get(task_id)
    assert stale_runtime is not None
    server.runtime_registry.complete(stale_runtime, timeout=0)
    monkeypatch.setattr(
        server.hierarchy_repository,
        "latest_event_sequence",
        lambda value: 37 if value == task_id else 0,
    )

    resumed = server.resume_task(task_id)

    assert resumed["task_id"] == task_id
    assert server.get_task(task_id)["status"] == "RESUMING"
    assert _FakeThread.last_args[16] == 44
    assert _FakeThread.last_args[17] is True
    assert _FakeThread.last_args[18] is True
    assert _FakeThread.last_args[25] == 4  # max logical Managers
    assert _FakeThread.last_args[26] == 4  # parallel Manager slots
    assert _FakeThread.last_args[27] == 5  # max children incl. Tester
    assert server.active_queues[task_id].latest_sequence == 37


def test_duplicate_concurrent_resume_has_one_winner(monkeypatch, tmp_path):
    task_id = "duplicate-resume"
    server.task_manager.create_task(
        task_id,
        name="Resume once",
        mode="orchestrator",
        prompt="Continue",
        root=str(tmp_path),
        files=[],
        settings={"project_mode": "edit"},
    )
    server.task_manager.finish_task(
        task_id,
        status="INTERRUPTED",
        reason="restart",
    )
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    original_reserve = server.runtime_registry.reserve
    rendezvous = threading.Barrier(2)

    def synchronized_reserve(*args, **kwargs):
        rendezvous.wait(timeout=2)
        return original_reserve(*args, **kwargs)

    monkeypatch.setattr(
        server.runtime_registry,
        "reserve",
        synchronized_reserve,
    )

    def attempt_resume():
        try:
            return server.resume_task(task_id)
        except HTTPException as exc:
            return exc.status_code

    results = []

    def collect_result():
        results.append(attempt_resume())

    callers = [server._NATIVE_THREAD(target=collect_result) for _ in range(2)]
    for caller in callers:
        caller.start()
    for caller in callers:
        caller.join(timeout=2)

    assert sum(isinstance(result, dict) for result in results) == 1
    assert results.count(409) == 1
    runtime = server.runtime_registry.get(task_id)
    assert runtime is not None
    server.runtime_registry.complete(runtime, timeout=0)


def test_auto_continue_runs_next_turn_cycle(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    calls = []

    def fake_run_session(root, **kwargs):
        calls.append(kwargs["resume_session"])
        stopped_reason = "max_turns_reached" if len(calls) == 1 else "task_completed"
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
    (tmp_path / "existing.py").write_text("VALUE = 1\n", encoding="utf-8")
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
            max_managers=6,
            max_parallel_managers=3,
            max_workers_per_manager=5,
            max_parallel_workers_per_manager=2,
            max_parallel_workers=9,
        )
    )

    assert captured["task_id"] == created["task_id"]
    assert captured["limits"].manager_cap == 6
    assert captured["limits"].max_parallel_managers == 3
    assert captured["limits"].max_workers_per_manager == 5
    assert captured["limits"].worker_parallel_cap == 2
    assert captured["limits"].max_parallel_workers == 9
    assert server.get_task(created["task_id"])["status"] == "COMPLETED"


def test_hierarchy_rejects_parallel_slots_above_planning_cap(tmp_path):
    with pytest.raises(HTTPException) as exc_info:
        server.run_task(
            server.TaskRequest(
                root=str(tmp_path),
                task="Invalid hierarchy caps",
                mode="orchestrator",
                project_mode="new_project",
                hierarchy_enabled=True,
                max_managers=2,
                max_parallel_managers=3,
            )
        )
    assert exc_info.value.status_code == 400
    assert "max_parallel_managers" in exc_info.value.detail


def test_hierarchy_edit_rejects_empty_project_root(monkeypatch, tmp_path):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)

    with pytest.raises(HTTPException) as exc_info:
        server.run_task(
            server.TaskRequest(
                root=str(tmp_path),
                task="Create an application",
                mode="orchestrator",
                project_mode="edit",
                hierarchy_enabled=True,
            )
        )

    assert exc_info.value.status_code == 400
    assert "Create new project" in exc_info.value.detail


def test_agent_model_override_is_persisted_for_next_call(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    created = server.run_task(server.TaskRequest(root="", task="Agent config", mode="chat"))

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
    assert (
        server.task_manager.get_agent_override(created["task_id"], "worker-1")["model"]
        == "claude-sonnet-4-6"
    )


def test_terminal_task_can_be_deleted_even_if_stream_queue_remains(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    created = server.run_task(server.TaskRequest(root="", task="Hello", files="", mode="chat"))
    task_id = created["task_id"]
    server.task_manager.finish_task(task_id, status="COMPLETED")

    assert server.delete_task(task_id)["status"] == "deleted"
    assert task_id not in server.active_queues
    assert server.task_manager.get_task(task_id) is None


def test_stop_marks_dashboard_task_as_stopping(monkeypatch):
    monkeypatch.setattr(server.threading, "Thread", _FakeThread)
    created = server.run_task(server.TaskRequest(root="", task="Hello", files="", mode="chat"))

    server.stop_task(created["task_id"])

    assert server.task_manager.get_task(created["task_id"])["status"] == "STOPPING"


def test_task_stop_flag_reaches_hierarchy_child_threads():
    import orchestrator.llm_client as llm_client

    llm_client.thread_local.task_id = "child-task"
    server.stop_flags["child-task"] = True
    try:
        assert server.is_current_thread_stopped() is True
    finally:
        server.stop_flags.pop("child-task", None)
        llm_client.thread_local.task_id = None


def test_runtime_shutdown_cancels_joins_closes_and_evicts():
    registry = server.TaskRuntimeRegistry()
    runtime = registry.reserve("active-shutdown")
    entered = threading.Event()

    def work():
        entered.set()
        runtime.cancellation.wait(2)

    worker = threading.Thread(target=work, daemon=False)
    registry.bind_worker(runtime.task_id, worker)
    worker.start()
    assert entered.wait(1)

    timed_out = registry.shutdown(timeout=2)

    assert timed_out == ()
    assert not worker.is_alive()
    assert runtime.cancellation.is_set()
    assert runtime.broker.closed
    assert registry.get(runtime.task_id) is None


def test_runtime_completion_closes_broker_before_eviction():
    registry = server.TaskRuntimeRegistry()
    runtime = registry.reserve("terminal-runtime")
    runtime.broker.put({"type": "done", "sequence": 1})

    registry.complete(runtime)

    assert runtime.broker.closed
    assert runtime.broker.events_after(0)[0]["type"] == "done"
    assert registry.get(runtime.task_id) is None


def test_control_event_commits_before_live_publish(monkeypatch):
    runtime = server.runtime_registry.reserve("atomic-control")
    committed = threading.Event()
    captured = {}

    def record_event(task_id, event, *, envelope):
        captured["task_id"] = task_id
        captured["event"] = event
        captured["envelope"] = envelope
        committed.set()
        return replace(envelope, sequence=1)

    original_put = runtime.broker.put

    def assert_committed(event):
        assert committed.is_set()
        original_put(event)

    monkeypatch.setattr(server.task_manager, "record_event", record_event)
    monkeypatch.setattr(runtime.broker, "put", assert_committed)

    wire = server._record_control_event(
        runtime.task_id,
        "approval_decided",
        {"decision": "approved"},
    )

    assert captured["envelope"].sequence == 0
    assert captured["event"]["type"] == "approval_decided"
    assert wire["sequence"] == 1
    assert runtime.broker.events_after(0)[0]["decision"] == "approved"
    server.runtime_registry.complete(runtime)


def test_runtime_event_boundary_repairs_and_redacts_before_persistence(monkeypatch):
    broker = ReplayEventBroker()
    order = []
    persisted = []

    def no_request_log(_name):
        raise ImportError

    def diagnostic_hook(diagnostic):
        order.append(("diagnostic", diagnostic["action"]))
        assert "super-secret-value" not in str(diagnostic)

    def record_event(task_id, event, *, envelope):
        order.append(("event", event["type"]))
        persisted.append((event, envelope))
        return replace(envelope, sequence=len(persisted))

    monkeypatch.setattr(
        server.hierarchy_repository,
        "record_event_schema_diagnostic",
        diagnostic_hook,
        raising=False,
    )
    monkeypatch.setattr(server.importlib, "import_module", no_request_log)
    monkeypatch.setattr(server.task_manager, "record_event", record_event)
    monkeypatch.setattr(server, "_observe_runtime_event", lambda *args, **kwargs: None)

    emitted = server._emit_persisted_runtime_event(
        "task-schema-repair",
        {"id": "session-schema-repair"},
        broker,
        {
            "type": "model_request_failed",
            "role": "worker",
            "reason": "provider rejected sessionKey=super-secret-value",
        },
    )

    assert order == [
        ("diagnostic", "repaired"),
        ("event", "event_schema_validation_repaired"),
        ("event", "model_request_failed"),
    ]
    repaired = persisted[1][0]
    assert repaired["error"] == repaired["reason"]
    assert "super-secret-value" not in repaired["error"]
    assert [event["type"] for event in emitted] == [
        "event_schema_validation_repaired",
        "model_request_failed",
    ]


def test_runtime_event_boundary_rejects_invalid_body_without_raising(monkeypatch):
    broker = ReplayEventBroker()
    persisted = []

    def no_request_log(_name):
        raise ImportError

    def failing_diagnostic_hook(_diagnostic):
        raise RuntimeError("request ledger unavailable")

    def record_event(task_id, event, *, envelope):
        persisted.append(event)
        return replace(envelope, sequence=len(persisted))

    monkeypatch.setattr(
        server.hierarchy_repository,
        "record_event_schema_diagnostic",
        failing_diagnostic_hook,
        raising=False,
    )
    monkeypatch.setattr(server.importlib, "import_module", no_request_log)
    monkeypatch.setattr(server.task_manager, "record_event", record_event)
    monkeypatch.setattr(server, "_observe_runtime_event", lambda *args, **kwargs: None)

    emitted = server._emit_persisted_runtime_event(
        "task-schema-reject",
        {"id": "session-schema-reject"},
        broker,
        {
            "type": "model_request_failed",
            "summary": "No role was supplied",
            "raw_response": "sessionKey=must-not-survive",
        },
    )

    assert [event["type"] for event in persisted] == [
        "event_schema_validation_failure"
    ]
    assert [event["type"] for event in emitted] == [
        "event_schema_validation_failure"
    ]
    assert "must-not-survive" not in str(persisted[0])


def test_repaired_event_persistence_failure_never_rethrows_original(monkeypatch):
    broker = ReplayEventBroker()

    def no_request_log(_name):
        raise ImportError

    def record_event(task_id, event, *, envelope):
        if event["type"] == "model_request_failed":
            raise RuntimeError("event storage unavailable")
        return replace(envelope, sequence=1)

    monkeypatch.setattr(server.importlib, "import_module", no_request_log)
    monkeypatch.setattr(server.task_manager, "record_event", record_event)
    monkeypatch.setattr(server, "_observe_runtime_event", lambda *args, **kwargs: None)

    emitted = server._emit_persisted_runtime_event(
        "task-schema-storage-failure",
        {"id": "session-schema-storage-failure"},
        broker,
        {
            "type": "model_request_failed",
            "role": "worker",
            "reason": "recoverable missing error",
        },
    )

    assert [event["type"] for event in emitted] == [
        "event_schema_validation_repaired"
    ]


def test_schema_diagnostic_uses_feature_detected_request_log_hook(monkeypatch):
    captured = []

    class RequestLog:
        @staticmethod
        def record_event_schema_diagnostic(diagnostic):
            captured.append(diagnostic)

    monkeypatch.setattr(server.importlib, "import_module", lambda _name: RequestLog)

    server._write_schema_diagnostic_to_request_log(
        {
            "type": "event_schema_validation_failure",
            "action": "rejected",
            "source_event_type": "model_request_failed",
            "summary": "Rejected sessionKey=super-secret-value",
            "validation_error": "missing error",
        }
    )

    assert captured[0]["type"] == "event_schema_validation_failure"
    assert "super-secret-value" not in str(captured[0])


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


def test_timeline_reports_retention_boundary_and_pagination(monkeypatch):
    task_id = "retained-task"
    retained = server.EventEnvelope(
        task_id=task_id,
        session_id="session-retained",
        event_type="agent_completed",
        payload={"agent_instance_id": "worker-retained", "role": "worker"},
        sequence=10,
    )

    class Repository:
        def replay_events(self, value, *, after_sequence=0, limit=1000):
            assert value == task_id
            assert limit == 1
            return [retained] if after_sequence < 10 else []

        def event_cursor(self, value):
            assert value == task_id
            return {
                "latest_sequence": 12,
                "retained_from_sequence": 10,
            }

    monkeypatch.setattr(server, "hierarchy_repository", Repository())
    monkeypatch.setattr(server.task_manager, "get_task", lambda value: {"id": value})

    page = server.get_task_timeline(task_id, after=0, limit=1)

    assert page["events"][0]["sequence"] == 10
    assert page["next_after"] == 10
    assert page["latest_sequence"] == 12
    assert page["retained_from_sequence"] == 10
    assert page["has_more"] is True
    assert page["history_incomplete"] is True


def test_resumed_stream_replays_durable_then_live_sequences(monkeypatch):
    task_id = "resumed-reconnect-task"
    broker = ReplayEventBroker(initial_sequence=40)
    server.active_queues[task_id] = broker
    durable = server.EventEnvelope(
        task_id=task_id,
        session_id="session-old",
        event_type="status",
        payload={"data": "durable"},
        sequence=40,
    )

    def replay_events(value, *, after_sequence=0, limit=1000):
        assert value == task_id
        return [durable] if after_sequence < 40 else []

    monkeypatch.setattr(
        server.hierarchy_repository,
        "replay_events",
        replay_events,
    )
    broker.put({"type": "done", "sequence": 41})

    response = TestClient(server.app).get(f"/api/stream/{task_id}?after=39")

    assert response.status_code == 200
    assert '"sequence": 40' in response.text
    assert '"_seq": 41' in response.text
    assert response.text.index('"sequence": 40') < response.text.index('"_seq": 41')


def test_resumed_stream_surfaces_retention_gap_before_replay(monkeypatch):
    task_id = "retained-reconnect-task"
    broker = ReplayEventBroker(initial_sequence=10)
    server.active_queues[task_id] = broker
    durable = server.EventEnvelope(
        task_id=task_id,
        session_id="session-retained",
        event_type="agent_completed",
        payload={"role": "worker"},
        agent_instance_id="worker-retained",
        sequence=10,
    )
    monkeypatch.setattr(
        server.hierarchy_repository,
        "event_cursor",
        lambda value: {
            "latest_sequence": 11,
            "retained_from_sequence": 10,
        },
    )
    monkeypatch.setattr(
        server.hierarchy_repository,
        "replay_events",
        lambda value, *, after_sequence=0, limit=1000: (
            [durable] if after_sequence < 10 else []
        ),
    )
    broker.put({"type": "done", "sequence": 11})

    response = TestClient(server.app).get(f"/api/stream/{task_id}?after=1")

    assert response.status_code == 200
    assert '"type": "timeline_gap"' in response.text
    assert '"retained_from_sequence": 10' in response.text
    assert response.text.index('"type": "timeline_gap"') < response.text.index(
        '"sequence": 10'
    )


@pytest.mark.parametrize(
    ("mode", "project_mode"),
    [
        ("orchestrator", "edit"),
        ("chat", "new_project"),
    ],
)
def test_staging_auto_is_rejected_outside_new_project_orchestrator(
    mode,
    project_mode,
    tmp_path,
):
    with pytest.raises(HTTPException) as exc_info:
        server.run_task(
            server.TaskRequest(
                root=str(tmp_path),
                task="Unsafe auto approval request",
                mode=mode,
                project_mode=project_mode,
                approval_mode="staging_auto",
            )
        )

    assert exc_info.value.status_code == 400
    assert "staging_auto" in exc_info.value.detail


def test_staging_auto_decides_and_audits_managed_workspace_approval(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(server.threading, "Thread", _ImmediateThread)
    captured = {}

    def fake_run_hierarchy(**kwargs):
        captured["execution_root"] = kwargs["root"]
        captured["approved"] = kwargs["approval_callback"](
            {
                "workstream_id": "stream-auto",
                "work_item_id": "item-auto",
                "attempt_id": "attempt-auto",
                "kind": "patch_apply",
                "target": "src/app.py",
                "reason": "high risk patch",
                "patch_sha256": "a" * 64,
                "additions": 5,
                "deletions": 0,
            }
        )
        return type(
            "Result",
            (),
            {
                "stopped_reason": "max_turns_reached",
                "turns": [],
                "final_state": {
                    "turn_count": 1,
                    "completed_tickets": [],
                    "last_review_verdict": None,
                },
            },
        )()

    monkeypatch.setattr(server, "run_hierarchy", fake_run_hierarchy)
    destination = tmp_path / "staging-auto-destination"
    destination.mkdir()

    created = server.run_task(
        server.TaskRequest(
            root=str(destination),
            task="Build in managed staging",
            mode="orchestrator",
            project_mode="new_project",
            approval_mode="staging_auto",
            hierarchy_enabled=True,
            create_zip=False,
        )
    )

    task_id = created["task_id"]
    assert captured["approved"] is True
    assert server._is_managed_staging_workspace(
        task_id,
        captured["execution_root"],
    )
    approval = server.hierarchy_repository.list_approvals(task_id)[0]
    assert approval["status"] == "approved"
    assert "orchestrator-managed staging workspace" in approval["decision_reason"]
    assert server.get_task(task_id)["settings"]["approval_mode"] == "staging_auto"
    events = _FakeThread.last_args[19].events_after(0)
    approval_events = [
        event
        for event in events
        if str(event.get("type") or "").startswith("approval_")
    ]
    assert [event["type"] for event in approval_events] == [
        "approval_requested",
        "approval_decided",
    ]
    assert approval_events[1]["automated"] is True
    assert approval_events[1]["approval_mode"] == "staging_auto"


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
        assert (destination / "main.py").read_text(encoding="utf-8") == "print('jarvis')\n"
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
