from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from orchestrator import artifact_manager, event_broker, task_manager
from orchestrator.event_schema import (
    TYPESCRIPT_ARTIFACT_PATH,
    generate_typescript,
    validate_event,
)
from orchestrator.lifecycle import LifecycleCoordinator
from orchestrator.models import EventEnvelope
from orchestrator.observability import (
    MetricsRegistry,
    ObservabilityRecord,
    ObservationContext,
    StructuredObserver,
)
from orchestrator.redaction import redact_candidate, scan_secrets
from orchestrator.state_repository import (
    CURRENT_SCHEMA_VERSION,
    RetentionPolicy,
    StateRepository,
)


def test_old_schema_migrates_events_and_exposes_version(tmp_path):
    database = tmp_path / "old.sqlite3"
    event = EventEnvelope(
        task_id="legacy-task",
        session_id="legacy-session",
        event_type="legacy.custom",
        payload={"kept": True},
        sequence=7,
    )
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE events (
            task_id TEXT NOT NULL, session_id TEXT NOT NULL,
            sequence INTEGER NOT NULL, event_id TEXT NOT NULL UNIQUE,
            envelope_json TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY (task_id, sequence))"""
    )
    connection.execute(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)",
        (
            event.task_id,
            event.session_id,
            event.sequence,
            event.event_id,
            json.dumps(
                {
                    "task_id": event.task_id,
                    "session_id": event.session_id,
                    "event_type": event.event_type,
                    "payload": event.payload,
                    "sequence": event.sequence,
                    "version": event.version,
                    "event_id": event.event_id,
                    "timestamp": event.timestamp.isoformat(),
                    "workstream_id": None,
                    "work_item_id": None,
                    "agent_instance_id": None,
                    "call_id": None,
                }
            ),
            event.timestamp.isoformat(),
        ),
    )
    connection.execute("PRAGMA user_version = 2")
    connection.commit()
    connection.close()

    with StateRepository(database) as repository:
        assert repository.current_schema_version == CURRENT_SCHEMA_VERSION
        assert repository.replay_events("legacy-task") == [event]
        assert repository.event_cursor("legacy-task")["latest_sequence"] == 7
        appended = repository.append_event(
            EventEnvelope(
                task_id="legacy-task",
                session_id="legacy-session",
                event_type="future.unknown",
                payload={"additive": "field"},
            )
        )
        assert appended.sequence == 8


def test_task_manager_imports_json_once_then_uses_sqlite_snapshot(
    monkeypatch,
    tmp_path,
):
    legacy_file = tmp_path / "tasks.json"
    legacy_record = {
        "legacy": {
            "id": "legacy",
            "name": "Imported",
            "mode": "orchestrator",
            "prompt": "Migrate me",
            "root": str(tmp_path),
            "files": [],
            "settings": {},
            "status": "COMPLETED",
            "phase": "finished",
            "hierarchy": {},
        }
    }
    legacy_file.write_text(json.dumps(legacy_record), encoding="utf-8")
    monkeypatch.setattr(task_manager, "TASKS_FILE", legacy_file)
    monkeypatch.setattr(task_manager, "_LEGACY_TASKS_FILE", legacy_file)
    monkeypatch.setattr(task_manager, "_repository", None)
    task_manager._records.clear()
    task_manager._records.update(task_manager._load())

    with StateRepository(tmp_path / "canonical.sqlite3") as repository:
        task_manager.configure_repository(repository)
        task_manager.set_status("legacy", "REVISION", "review")

        assert repository.get_task_snapshot("legacy")["status"] == "REVISION"
        legacy_file.write_text("{}", encoding="utf-8")
        task_manager.configure_repository(repository)
        assert task_manager.get_task("legacy")["status"] == "INTERRUPTED"
        assert repository.get_task_snapshot("legacy")["status"] == "INTERRUPTED"

    task_manager.configure_repository(None)
    task_manager._records.clear()


def test_failed_migration_rolls_back_schema_and_version(monkeypatch, tmp_path):
    with StateRepository(tmp_path / "transactional.sqlite3") as repository:
        with repository._lock:
            repository._connection.execute(
                "DROP TABLE task_completion_invariants"
            )
            repository._connection.execute(
                "DELETE FROM schema_migrations WHERE version = 5"
            )
            repository._connection.execute("PRAGMA user_version = 4")

        def fail_migration(connection):
            connection.execute("CREATE TABLE migration_probe(value TEXT)")
            raise RuntimeError("injected migration failure")

        monkeypatch.setattr(
            repository,
            "_migration_5_completion_invariants",
            fail_migration,
        )
        with pytest.raises(RuntimeError, match="injected migration failure"):
            repository._migrate_schema()

        assert repository.current_schema_version == 4
        with repository._lock:
            probe = repository._connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'migration_probe'"
            ).fetchone()
        assert probe is None


def test_event_schema_codegen_is_deterministic_and_legacy_is_additive():
    first = generate_typescript()
    second = generate_typescript()

    assert first == second
    assert "KnownOrchestratorEvent" in first
    assert TYPESCRIPT_ARTIFACT_PATH.as_posix().endswith(
        "orchestrator-events.generated.ts"
    )
    assert (Path.cwd() / TYPESCRIPT_ARTIFACT_PATH).read_text(
        encoding="utf-8"
    ) == first
    legacy = {"type": "legacy.plugin_event", "new_field": {"kept": True}}
    assert validate_event(legacy) == legacy
    with pytest.raises(ValueError, match="missing required"):
        validate_event({"type": "model_request_started"})


def test_human_approval_queue_is_idempotent_and_auditable(tmp_path):
    with StateRepository(tmp_path / "approvals.sqlite3") as repository:
        first = repository.request_approval(
            "task-a",
            idempotency_key="patch:item-a:sha",
            kind="patch_apply",
            target="src/app.py",
            reason="high risk contract",
            workstream_id="stream-a",
            payload={"patch_sha256": "a" * 64},
        )
        replayed = repository.request_approval(
            "task-a",
            idempotency_key="patch:item-a:sha",
            kind="patch_apply",
            target="src/app.py",
            reason="high risk contract",
        )

        assert replayed["approval_id"] == first["approval_id"]
        assert repository.list_approvals("task-a", status="pending") == [first]

        decided = repository.decide_approval(
            first["approval_id"],
            decision="approved",
            reason="reviewed by operator",
        )
        assert decided["status"] == "approved"
        assert decided["decision_reason"] == "reviewed by operator"
        assert repository.list_approvals("task-a", status="pending") == []


def test_lifecycle_drains_broker_and_joins_worker(monkeypatch):
    broker = event_broker.ReplayEventBroker()
    entered = threading.Event()
    release = threading.Event()
    original_deepcopy = event_broker.deepcopy

    def slow_copy(value):
        entered.set()
        assert release.wait(1)
        return original_deepcopy(value)

    monkeypatch.setattr(event_broker, "deepcopy", slow_copy)
    publisher = threading.Thread(
        target=lambda: broker.put({"type": "status", "data": "persisted"}),
        name="publisher",
    )
    publisher.start()
    assert entered.wait(1)
    threading.Timer(0.05, release.set).start()

    lifecycle = LifecycleCoordinator()
    lifecycle.register_component(broker)
    lifecycle.register_worker(publisher)
    report = lifecycle.shutdown(timeout=1)

    assert report.clean
    assert report.drained == 1
    assert report.joined_workers == 1
    assert broker.events_after(0)[0]["data"] == "persisted"
    with pytest.raises(RuntimeError, match="closing"):
        broker.put({"type": "status"})


def test_metrics_keep_correlation_out_of_bounded_labels(tmp_path):
    with StateRepository(tmp_path / "metrics.sqlite3") as repository:
        observer = StructuredObserver(repository)
        metrics = MetricsRegistry(observer)
        context = ObservationContext(
            task_id="task-1",
            session_id="session-1",
            agent_instance_id="worker-1",
            call_id="call-1",
            attempt_id="attempt-2",
        )
        metrics.increment(
            "llm.call.total",
            labels={
                "component": "llm",
                "operation": "call",
                "outcome": "success",
                "role": "worker",
            },
            context=context,
        )

        saved = repository.list_observability("task-1")
        assert len(saved) == 1
        assert {
            key: saved[0][key]
            for key in (
                "task_id",
                "session_id",
                "agent_instance_id",
                "call_id",
                "attempt_id",
            )
        } == {
            "task_id": "task-1",
            "session_id": "session-1",
            "agent_instance_id": "worker-1",
            "call_id": "call-1",
            "attempt_id": "attempt-2",
        }
        assert "task_id" not in saved[0]["labels"]
        with pytest.raises(ValueError, match="not bounded"):
            metrics.increment(
                "llm.call.total",
                labels={"task_id": "unbounded-task-id"},
            )


def test_structured_observer_bounds_memory_and_can_skip_durable_records():
    class Repository:
        def __init__(self):
            self.saved = []

        def record_observability(self, record):
            self.saved.append(record)
            return record["record_id"]

    repository = Repository()
    observer = StructuredObserver(repository, max_records=2)
    for index in range(3):
        observer.emit(
            ObservabilityRecord(
                kind="log",
                name="worker.event",
                attributes={"index": index},
            ),
            persist=index != 1,
        )

    assert [record.attributes["index"] for record in observer.records()] == [1, 2]
    assert [record["attributes"]["index"] for record in repository.saved] == [0, 2]


def test_secret_fixture_is_detected_redacted_and_blocked(monkeypatch, tmp_path):
    secret = "AKIAIOSFODNN7EXAMPLE"
    prompt = f"Deploy with aws key {secret}"
    findings = scan_secrets(prompt, candidate_type="prompt")

    assert findings
    assert findings[0].kind == "aws_access_key"
    assert secret not in redact_candidate(prompt, candidate_type="prompt")

    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    staging = artifact_manager.create_workspace(
        "task-secret",
        tmp_path / "destination",
        auto_apply=False,
        create_zip=False,
    )
    (staging / "config.txt").write_text(prompt, encoding="utf-8")
    with pytest.raises(ValueError, match="potential credentials"):
        artifact_manager.finalize_workspace("task-secret")


def test_retention_preserves_terminal_events_records_and_cursor(tmp_path):
    old = datetime.now(timezone.utc) - timedelta(days=10)
    policy = RetentionPolicy(
        event_days=1,
        log_days=1,
        artifact_days=1,
        observability_days=1,
        max_events_per_task=2,
    )
    with StateRepository(tmp_path / "retention.sqlite3") as repository:
        first = repository.append_event(
            EventEnvelope(
                task_id="task-retention",
                session_id="session-retention",
                event_type="status",
                payload={"data": "old"},
                timestamp=old,
            )
        )
        terminal = repository.append_event(
            EventEnvelope(
                task_id="task-retention",
                session_id="session-retention",
                event_type="hierarchy_completed",
                payload={"verdict": "approved"},
                timestamp=old,
            )
        )
        repository.record_log_metadata(
            "task-retention",
            "old.log",
            created_at=old,
        )
        repository.record_log_metadata(
            "task-retention",
            "evidence.log",
            terminal_evidence=True,
            created_at=old,
        )
        repository.record_artifact(
            "task-retention",
            "old.txt",
            "a" * 64,
            now=old,
        )
        repository.record_artifact(
            "task-retention",
            "evidence.txt",
            "b" * 64,
            terminal_evidence=True,
            now=old,
        )

        result = repository.compact_retention(policy)

        assert result["events"] == 1
        assert repository.replay_events("task-retention") == [terminal]
        cursor = repository.event_cursor("task-retention")
        assert cursor["task_id"] == "task-retention"
        assert cursor["session_id"] == "session-retention"
        assert cursor["latest_sequence"] == 2
        assert cursor["retained_from_sequence"] == 2
        assert cursor["updated_at"]
        assert [item["path"] for item in repository.list_log_records()] == [
            "evidence.log"
        ]
        assert [item["path"] for item in repository.list_artifacts()] == [
            "evidence.txt"
        ]
        continued = repository.append_event(
            EventEnvelope(
                task_id="task-retention",
                session_id="session-retention",
                event_type="future.event",
                payload={},
            )
        )
        assert (first.sequence, terminal.sequence, continued.sequence) == (1, 2, 3)


def test_artifact_reconciliation_detects_missing_tampered_and_extras(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(artifact_manager, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    staging = artifact_manager.create_workspace(
        "task-hashes",
        tmp_path / "destination",
        auto_apply=False,
        create_zip=False,
    )
    (staging / "one.txt").write_text("one\n", encoding="utf-8")
    (staging / "two.txt").write_text("two\n", encoding="utf-8")
    artifact_manager.finalize_workspace("task-hashes")
    assert artifact_manager.reconcile_workspace(
        "task-hashes",
        target="staging",
    )["valid"]

    (staging / "one.txt").write_text("tampered\n", encoding="utf-8")
    (staging / "two.txt").unlink()
    (staging / "extra.txt").write_text("not approved\n", encoding="utf-8")
    result = artifact_manager.reconcile_workspace(
        "task-hashes",
        target="staging",
        record_result=True,
    )

    assert not result["valid"]
    assert result["missing"] == ["two.txt"]
    assert [item["path"] for item in result["tampered"]] == ["one.txt"]
    assert result["unapproved_extras"] == ["extra.txt"]


def test_task_end_rejects_unfinished_call_then_persists_terminal_state(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-invariant",
        name="Invariant",
        mode="orchestrator",
        prompt="Verify calls",
        root=str(tmp_path),
        files=[],
        settings={"hierarchy_enabled": True},
    )
    task_manager.record_event(
        "task-invariant",
        {
            "type": "model_request_started",
            "role": "worker",
            "agent_instance_id": "worker-1",
            "call_id": "call-1",
            "attempt_id": "attempt-1",
        },
    )
    task_manager.finish_task("task-invariant", status="COMPLETED")
    blocked = task_manager.get_task("task-invariant")
    assert blocked["status"] == "FAILED"
    assert blocked["hierarchy"]["completion_invariant"][
        "unresolved_call_ids"
    ] == ["call-1"]

    task_manager.record_event(
        "task-invariant",
        {
            "type": "model_request_completed",
            "role": "worker",
            "agent_instance_id": "worker-1",
            "call_id": "call-1",
            "attempt_id": "attempt-1",
        },
    )
    task_manager.finish_task("task-invariant", status="COMPLETED")
    completed = task_manager.get_task("task-invariant")
    assert completed["status"] == "COMPLETED"
    assert completed["hierarchy"]["completion_invariant"]["balanced"] is True
    assert completed["hierarchy"]["execution"]["calls"]["call-1"][
        "attempt_id"
    ] == "attempt-1"
    task_manager._records.clear()
