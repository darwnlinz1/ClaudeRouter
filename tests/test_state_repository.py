import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.effects import EffectState
from orchestrator.models import (
    Attempt,
    EventEnvelope,
    HandoffEnvelope,
    TaskPlan,
    WorkContract,
    WorkItem,
    Workstream,
)
from orchestrator.project_workspace import ProjectLeaseLostError
from orchestrator.state_repository import CURRENT_SCHEMA_VERSION, StateRepository


def make_plan() -> TaskPlan:
    workstream = Workstream(
        id="stream-1",
        title="Foundation",
        goal="Build typed state",
        acceptance_criteria=("Models validate",),
        work_items=(
            WorkItem(
                id="item-1",
                workstream_id="stream-1",
                title="Implement",
                goal="Add foundation",
                acceptance_criteria=("Unit tests pass",),
                write_scopes=("orchestrator/models.py",),
            ),
        ),
    )
    return TaskPlan(
        task_id="task-1",
        session_id="session-1",
        goal="Build orchestrator",
        workstreams=(workstream,),
    )


@pytest.fixture
def repository(tmp_path):
    with StateRepository(tmp_path / "state.sqlite3") as value:
        yield value


def test_plan_and_normalized_dag_are_durable(repository):
    plan = make_plan()

    repository.save_plan(plan)

    assert repository.get_task(plan.task_id)["goal"] == plan.goal
    assert repository.get_plan(plan.task_id) == plan
    assert [stream.id for stream in repository.list_workstreams(plan.task_id)] == [
        "stream-1"
    ]
    assert [item.id for item in repository.list_work_items(plan.task_id)] == [
        "item-1"
    ]


def test_plan_revision_uses_optimistic_conflict_check(repository):
    first = make_plan()
    repository.save_plan(first)
    second = replace(first, revision=2)

    with pytest.raises(RuntimeError, match="revision conflict"):
        repository.save_plan(second, expected_previous_revision=0)

    repository.save_plan(second, expected_previous_revision=1)
    assert repository.get_plan(first.task_id).revision == 2


def test_plan_revision_is_immutable_and_identical_retry_is_idempotent(repository):
    first = make_plan()
    repository.save_plan(first, expected_previous_revision=0)

    repository.save_plan(first, expected_previous_revision=0)
    changed = replace(first, goal="Different bytes at the same revision")
    with pytest.raises(RuntimeError, match="immutable revision"):
        repository.save_plan(changed)

    assert repository.get_plan(first.task_id) == first
    with repository._lock:
        count = repository._connection.execute(
            "SELECT COUNT(*) FROM plans WHERE task_id = ? AND revision = ?",
            (first.task_id, first.revision),
        ).fetchone()[0]
    assert count == 1


def test_save_plan_persists_stream_and_item_contract_versions(repository):
    plan = make_plan()

    repository.save_plan(plan)

    expected = [
        plan.workstreams[0].work_items[0].contract,
        plan.workstreams[0].contract,
    ]
    assert repository.list_contract_versions(plan.task_id) == sorted(
        expected,
        key=lambda contract: contract.id,
    )


def test_contract_identity_and_handoff_survive_restart(tmp_path):
    database = tmp_path / "coordination.sqlite3"
    contract = WorkContract(
        id="contract-auth",
        version=2,
        input_artifacts=("spec.md",),
        expected_outputs=("src/auth.py",),
        read_scopes=("spec.md",),
        write_scopes=("src/auth.py",),
        acceptance_criteria=("Authentication works",),
        test_requirements=("pytest tests/test_auth.py",),
        evidence_requirements=("passing test report",),
        consumers=("api",),
    )
    handoff = HandoffEnvelope(
        handoff_id="handoff-auth",
        task_id="task-coordination",
        contract_id=contract.id,
        contract_version=contract.version,
        source_agent_id="manager-auth",
        target_agent_id="worker-auth",
        signal_type="delegate_work_item",
        artifacts=("spec.md",),
        evidence={"approved": True},
        workstream_id="api",
        work_item_id="api:auth",
    )

    with StateRepository(database) as first:
        assert first.save_contract_version("task-coordination", contract) == contract
        assert first.save_contract_version("task-coordination", contract) == contract
        logical_id = first.resolve_agent_identity(
            "task-coordination",
            "worker",
            "api:auth",
        )
        assert first.resolve_agent_identity(
            "task-coordination",
            "worker",
            "api:auth",
            logical_id,
        ) == logical_id
        with pytest.raises(RuntimeError, match="agent identity conflict"):
            first.resolve_agent_identity(
                "task-coordination",
                "worker",
                "api:auth",
                "different-worker",
            )
        assert first.append_handoff(handoff) == handoff
        assert first.append_handoff(handoff) == handoff
        with pytest.raises(RuntimeError, match="contract version"):
            first.append_handoff(
                replace(
                    handoff,
                    handoff_id="handoff-missing-contract",
                    contract_version=3,
                )
            )
        with pytest.raises(RuntimeError, match="contract version conflict"):
            first.save_contract_version(
                "task-coordination",
                replace(contract, priority=1),
            )

    with StateRepository(database) as restarted:
        assert restarted.get_contract_version(
            "task-coordination",
            contract.id,
            contract.version,
        ) == contract
        assert restarted.get_contract(
            "task-coordination",
            contract.id,
        ) == contract
        assert restarted.resolve_agent_identity(
            "task-coordination",
            "worker",
            "api:auth",
        ) == logical_id
        assert restarted.list_handoffs("task-coordination") == [handoff]


def test_attempt_round_trip(repository):
    attempt = Attempt(
        id="attempt-1",
        task_id="task-1",
        workstream_id="stream-1",
        work_item_id="item-1",
        number=1,
        worker_agent_id="worker-1",
    )

    repository.save_attempt(attempt)

    assert repository.get_attempt(attempt.id) == attempt
    assert repository.list_attempts("task-1") == [attempt]


def test_events_get_atomic_monotonic_sequences_and_replay(repository):
    first = repository.append_event(
        EventEnvelope(
            task_id="task-1",
            session_id="session-1",
            event_type="task.started",
            payload={},
        )
    )
    second = repository.append_event(
        EventEnvelope(
            task_id="task-1",
            session_id="session-1",
            event_type="plan.created",
            payload={"revision": 1},
        )
    )

    assert (first.sequence, second.sequence) == (1, 2)
    assert repository.latest_event_sequence("task-1") == 2
    assert repository.latest_event_sequence("missing-task") == 0
    assert repository.replay_events("task-1", after_sequence=1) == [second]
    assert repository.append_event(second) == second


def test_append_event_and_projection_are_atomic_and_idempotent(repository):
    repository.save_task_snapshot(
        "task-atomic",
        {
            "id": "task-atomic",
            "status": "QUEUED",
            "mode": "orchestrator",
            "phase": "queued",
            "events": [{"type": "legacy-duplicate"}],
        },
    )
    event = EventEnvelope(
        task_id="task-atomic",
        session_id="session-atomic",
        event_type="status",
        payload={"data": "running"},
    )

    def update(snapshot, stored):
        snapshot["status"] = "RUNNING"
        snapshot["phase"] = "event"
        snapshot["event_sequence"] = stored.sequence
        return snapshot

    stored, projection = repository.append_event_and_update_projection(
        event,
        update,
    )
    retried, retry_projection = repository.append_event_and_update_projection(
        event,
        lambda snapshot, _: {**snapshot, "status": "SHOULD_NOT_APPLY"},
    )

    assert retried == stored
    assert retry_projection == projection
    assert repository.replay_events("task-atomic") == [stored]
    saved = repository.get_task_snapshot("task-atomic")
    assert saved["status"] == "RUNNING"
    assert saved["event_sequence"] == 1
    assert "events" not in saved

    failed_event = replace(event, event_id="event-fails")
    with pytest.raises(RuntimeError, match="projection failed"):
        repository.append_event_and_update_projection(
            failed_event,
            lambda *_: (_ for _ in ()).throw(RuntimeError("projection failed")),
        )
    assert repository.replay_events("task-atomic") == [stored]


def test_delete_task_removes_events(repository):
    repository.append_event(
        EventEnvelope(
            task_id="task-delete",
            session_id="session-delete",
            event_type="plan.created",
            payload={"revision": 1},
        )
    )
    repository.begin_effect(
        "task-delete",
        "effect-key",
        "file_patch",
        "src/app.py",
    )

    counts = repository.delete_task("task-delete")

    assert counts["events"] == 1
    assert counts["effects"] == 1
    assert repository.list_effects("task-delete") == []
    assert repository.replay_events("task-delete") == []


def test_lease_is_exclusive_until_expiry(repository):
    now = datetime.now(timezone.utc)

    assert repository.acquire_lease("item", "item-1", "worker-a", 30, now=now)
    assert not repository.acquire_lease("item", "item-1", "worker-b", 30, now=now)
    assert repository.acquire_lease(
        "item", "item-1", "worker-b", 30, now=now + timedelta(seconds=31)
    )
    assert not repository.release_lease("item", "item-1", "worker-a")
    assert repository.release_lease("item", "item-1", "worker-b")


def test_project_lock_is_owner_checked(repository):
    assert repository.acquire_project_lock("project-a", "manager-a", 30)
    assert not repository.acquire_project_lock("project-a", "manager-b", 30)
    assert not repository.release_project_lock("project-a", "manager-b")
    assert repository.release_project_lock("project-a", "manager-a")


def test_effect_begin_is_idempotent_across_restart(tmp_path):
    database = tmp_path / "effects.sqlite3"
    before_hash = "a" * 64
    with StateRepository(database) as first_repository:
        first = first_repository.begin_effect(
            "task-effects",
            "write:src/app.py:v1",
            "file.replace",
            "src/app.py",
            payload={"content": "v1"},
            before_sha256=before_hash,
        )

    with StateRepository(database) as second_repository:
        duplicate = second_repository.begin_effect(
            "task-effects",
            "write:src/app.py:v1",
            "file.replace",
            "src/app.py",
            payload={"content": "v1"},
            before_sha256=before_hash,
        )
        assert duplicate.effect_id == first.effect_id
        assert duplicate.attempts == 1
        assert len(second_repository.list_effects("task-effects")) == 1

        applied = second_repository.complete_effect(
            first.effect_id,
            result={"bytes": 2},
            after_sha256="b" * 64,
        )
        assert applied.state is EffectState.APPLIED
        assert second_repository.complete_effect(
            first.effect_id,
            result={"bytes": 2},
            after_sha256="b" * 64,
        ) == applied


def test_failed_effect_retries_same_receipt_and_reconciles(repository):
    started = repository.begin_effect(
        "task-effects", "publish:one", "artifact.publish", "dist/one.zip"
    )
    failed = repository.fail_effect(started.effect_id, "injected crash")
    assert failed.state is EffectState.FAILED

    retried = repository.begin_effect(
        "task-effects", "publish:one", "artifact.publish", "dist/one.zip"
    )
    assert retried.effect_id == started.effect_id
    assert retried.attempts == 2
    assert retried.state is EffectState.PENDING

    reconciled = repository.reconcile_effect(
        retried.effect_id,
        result={"verified": True},
        after_sha256="c" * 64,
    )
    assert reconciled.state is EffectState.RECONCILED
    assert reconciled.result == {"verified": True}


def test_additive_migration_preserves_existing_rows(tmp_path):
    database = tmp_path / "legacy.sqlite3"
    now = datetime.now(timezone.utc)
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE tasks (
            task_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            goal TEXT NOT NULL,
            status TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE project_locks (
            project_key TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            purpose TEXT NOT NULL DEFAULT ''
        );
        """
    )
    connection.execute(
        "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "legacy-task",
            "legacy-session",
            "keep me",
            "pending",
            "{}",
            now.isoformat(),
            now.isoformat(),
        ),
    )
    connection.execute(
        "INSERT INTO project_locks VALUES (?, ?, ?, ?)",
        (
            "legacy-project",
            "legacy-owner",
            (now + timedelta(minutes=5)).isoformat(),
            "legacy",
        ),
    )
    connection.commit()
    connection.close()

    with StateRepository(database) as migrated:
        assert migrated.get_task("legacy-task")["goal"] == "keep me"
        lease = migrated.acquire_project_lease(
            "legacy-project", "legacy-owner", 30, now=now
        )
        assert lease is not None
        assert lease.fencing_token == 1
        effect = migrated.begin_effect(
            "legacy-task", "migration-check", "test", "local"
        )
        assert effect.state is EffectState.PENDING


def test_v11_effect_migration_is_additive(tmp_path):
    database = tmp_path / "v11-effects.sqlite3"
    now = datetime.now(timezone.utc).isoformat()
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE effect_receipts (
            effect_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL, kind TEXT NOT NULL,
            target TEXT NOT NULL, before_sha256 TEXT, after_sha256 TEXT,
            payload_json TEXT NOT NULL DEFAULT '{}',
            result_json TEXT NOT NULL DEFAULT '{}',
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            started_at TEXT NOT NULL, completed_at TEXT, error TEXT,
            UNIQUE(task_id, idempotency_key)
        );
        PRAGMA user_version = 11;
        """
    )
    connection.execute(
        """INSERT INTO effect_receipts(
            effect_id, task_id, idempotency_key, kind, target,
            payload_json, result_json, state, attempts,
            created_at, updated_at, started_at
        ) VALUES (?, ?, ?, ?, ?, '{}', '{}', 'pending', 1, ?, ?, ?)""",
        ("legacy-effect", "task-a", "legacy-key", "write", "value.txt", now, now, now),
    )
    connection.commit()
    connection.close()

    with StateRepository(database) as migrated:
        receipt = migrated.get_effect("legacy-effect")
        assert receipt is not None
        assert receipt.state is EffectState.PENDING
        assert receipt.expected_after_sha256 is None
        assert receipt.fencing_token is None
        with migrated._lock:
            columns = {
                row["name"]
                for row in migrated._connection.execute(
                    "PRAGMA table_info(effect_receipts)"
                ).fetchall()
            }
        assert {
            "expected_after_sha256",
            "fencing_token",
            "compensates_effect_id",
            "compensated_by_effect_id",
            "compensated_at",
        } <= columns


def test_v12_coordination_and_retention_migration_is_additive_and_restartable(
    tmp_path,
):
    database = tmp_path / "v12-coordination.sqlite3"
    now = datetime.now(timezone.utc).isoformat()
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE log_records (
            log_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
            session_id TEXT, agent_instance_id TEXT, call_id TEXT,
            attempt_id TEXT, path TEXT NOT NULL, content_sha256 TEXT,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            terminal_evidence INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE TABLE artifact_records (
            record_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
            session_id TEXT, path TEXT NOT NULL, content_sha256 TEXT,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            approved INTEGER NOT NULL DEFAULT 0,
            terminal_evidence INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            UNIQUE(task_id, path)
        );
        PRAGMA user_version = 12;
        """
    )
    connection.execute(
        """INSERT INTO log_records(
            log_id, task_id, path, size_bytes, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)""",
        ("legacy-log", "legacy-task", "legacy.log", 7, '{"managed":true}', now),
    )
    connection.commit()
    connection.close()

    with StateRepository(database) as migrated:
        assert migrated.current_schema_version == CURRENT_SCHEMA_VERSION
        assert migrated.list_log_records("legacy-task")[0]["retention_state"] == (
            "active"
        )
        with migrated._lock:
            tables = {
                row["name"]
                for row in migrated._connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            log_columns = {
                row["name"]
                for row in migrated._connection.execute(
                    "PRAGMA table_info(log_records)"
                ).fetchall()
            }
        assert {
            "contract_versions",
            "agent_identities",
            "handoffs",
            "llm_request_attempts",
        } <= tables
        assert {
            "retention_state",
            "retention_claim_id",
            "retention_claimed_at",
            "retention_error",
            "retention_finalized_at",
        } <= log_columns

    with StateRepository(database) as restarted:
        assert restarted.current_schema_version == CURRENT_SCHEMA_VERSION
        assert restarted.list_log_records("legacy-task")[0]["log_id"] == (
            "legacy-log"
        )


def test_v16_repairs_early_request_ledger_without_agent_role(tmp_path):
    database = tmp_path / "early-v15.sqlite3"
    with StateRepository(database):
        pass

    connection = sqlite3.connect(database)
    connection.executescript(
        """
        ALTER TABLE llm_request_attempts DROP COLUMN agent_role;
        DELETE FROM schema_migrations WHERE version = 16;
        PRAGMA user_version = 15;
        """
    )
    connection.commit()
    connection.close()

    with StateRepository(database) as migrated:
        assert migrated.current_schema_version == CURRENT_SCHEMA_VERSION
        with migrated._lock:
            columns = {
                row["name"]
                for row in migrated._connection.execute(
                    "PRAGMA table_info(llm_request_attempts)"
                ).fetchall()
            }
        assert "agent_role" in columns


def test_v17_execution_recovery_migration_is_restartable(tmp_path):
    database = tmp_path / "v16-execution-recovery.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA user_version = 16")
    connection.commit()
    connection.close()

    with StateRepository(database) as migrated:
        assert migrated.current_schema_version == 17
        with migrated._lock:
            tables = {
                row["name"]
                for row in migrated._connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
        assert {
            "execution_epochs",
            "execution_roster",
            "remediation_attempts",
            "manager_terminal_reports",
            "director_final_reviews",
            "terminal_dispositions",
        } <= tables
        assert migrated.migration_history()[-1]["name"] == (
            "execution_recovery_records"
        )

    with StateRepository(database) as restarted:
        assert restarted.current_schema_version == 17
        assert [
            row["version"]
            for row in restarted.migration_history()
            if row["version"] == 17
        ] == [17]


def test_manager_reports_and_director_review_are_exactly_once(repository):
    epoch = repository.create_execution_epoch(
        "task-reports",
        "epoch-reports",
        plan_revision=2,
    )
    repository.freeze_execution_roster(
        "task-reports",
        epoch["execution_epoch_id"],
        (
            {
                "manager_agent_id": "manager-a",
                "workstream_id": "stream-a",
                "contract_id": "contract-a",
                "contract_version": 2,
            },
            {
                "manager_agent_id": "manager-b",
                "workstream_id": "stream-b",
                "contract_id": "contract-b",
                "contract_version": 2,
            },
        ),
    )
    first_payload = {
        "completed_items": ["item-a"],
        "failed_items": [],
        "reason_code": "completed",
    }
    first = repository.record_manager_terminal_report(
        "task-reports",
        "epoch-reports",
        "manager-a",
        "stream-a",
        "completed",
        report=first_payload,
        terminal_log_refs=("logs/manager-a.jsonl",),
    )
    assert repository.record_manager_terminal_report(
        "task-reports",
        "epoch-reports",
        "manager-a",
        "stream-a",
        "completed",
        report=first_payload,
        terminal_log_refs=("logs/manager-a.jsonl",),
    ) == first
    with pytest.raises(RuntimeError, match="exactly-once"):
        repository.record_manager_terminal_report(
            "task-reports",
            "epoch-reports",
            "manager-a",
            "stream-a",
            "partial",
            report={"failed_items": ["item-a"]},
        )
    assert repository.manager_report_barrier(
        "task-reports",
        "epoch-reports",
    ) == {
        "task_id": "task-reports",
        "execution_epoch_id": "epoch-reports",
        "expected": 2,
        "received": 1,
        "missing_manager_ids": ["manager-b"],
        "disposition_counts": {
            "completed": 1,
            "partial": 0,
            "abandoned": 0,
        },
        "satisfied": False,
        "all_completed": False,
    }
    with pytest.raises(RuntimeError, match="N/N"):
        repository.reserve_director_final_review(
            "task-reports",
            "epoch-reports",
        )

    repository.record_manager_terminal_report(
        {
            "task_id": "task-reports",
            "execution_epoch_id": "epoch-reports",
            "manager_agent_id": "manager-b",
            "workstream_id": "stream-b",
            "disposition": "abandoned",
            "synthesized": True,
            "failed_items": ["item-b"],
            "terminal_log_refs": ["logs/manager-b.jsonl"],
        }
    )
    barrier = repository.manager_report_barrier(
        "task-reports",
        "epoch-reports",
    )
    assert barrier["satisfied"] is True
    assert barrier["all_completed"] is False
    assert barrier["disposition_counts"] == {
        "completed": 1,
        "partial": 0,
        "abandoned": 1,
    }

    review = repository.reserve_director_final_review(
        "task-reports",
        "epoch-reports",
        logical_request_id="director-call",
    )
    assert review is not None
    assert (
        repository.reserve_director_final_review(
            "task-reports",
            "epoch-reports",
        )
        is None
    )
    completed = repository.complete_director_final_review(
        review["review_id"],
        verdict="partial",
        review={"summary": "one stream abandoned"},
        terminal_disposition="partial",
        terminal_log_refs=("logs/director.jsonl",),
    )
    assert repository.complete_director_final_review(
        review["review_id"],
        verdict="partial",
        review={"summary": "one stream abandoned"},
        terminal_disposition="partial",
        terminal_log_refs=("logs/director.jsonl",),
    ) == completed
    with pytest.raises(RuntimeError, match="terminal record differs"):
        repository.complete_director_final_review(
            review["review_id"],
            verdict="completed",
        )


def test_remediation_strategy_uniqueness_and_terminal_log_refs(
    tmp_path,
):
    database = tmp_path / "remediation.sqlite3"
    with StateRepository(database) as repository:
        repository.create_execution_epoch(
            "task-remediation",
            "epoch-remediation",
        )
        first = repository.reserve_remediation_attempt(
            "task-remediation",
            "worker-a",
            "signature-a",
            "switch_account",
            execution_epoch_id="epoch-remediation",
            category="auth_account",
        )
        assert first is not None
        assert (
            repository.reserve_remediation_attempt(
                "task-remediation",
                "worker-a",
                "signature-a",
                "switch_account",
                execution_epoch_id="epoch-remediation",
            )
            is None
        )
        second = repository.reserve_remediation_attempt(
            "task-remediation",
            "worker-a",
            "signature-a",
            "refresh_prompt",
            execution_epoch_id="epoch-remediation",
        )
        reset = repository.reserve_remediation_attempt(
            "task-remediation",
            "worker-a",
            "signature-b",
            "switch_account",
            execution_epoch_id="epoch-remediation",
        )
        assert second is not None
        assert reset is not None
        completed = repository.complete_remediation_attempt(
            first["remediation_attempt_id"],
            status="failed",
            details={"reason": "replacement exhausted"},
            terminal_disposition="abandoned",
            terminal_log_refs=("logs/worker-a.jsonl",),
        )
        assert completed["terminal_log_refs"] == [
            "logs/worker-a.jsonl"
        ]
        disposition = repository.record_terminal_disposition(
            "task-remediation",
            "epoch-remediation",
            "agent",
            "worker-a",
            "abandoned",
            logical_agent_id="worker-a",
            reason_code="remediation_exhausted",
            terminal_log_refs=("logs/worker-a.jsonl",),
        )
        assert disposition["terminal_log_refs"] == [
            "logs/worker-a.jsonl"
        ]

    with StateRepository(database) as restarted:
        assert (
            restarted.reserve_remediation_attempt(
                "task-remediation",
                "worker-a",
                "signature-a",
                "switch_account",
                execution_epoch_id="epoch-remediation",
            )
            is None
        )
        assert len(
            restarted.list_remediation_attempts(
                "task-remediation",
                logical_agent_id="worker-a",
            )
        ) == 3
        assert restarted.list_terminal_dispositions(
            "task-remediation",
            "epoch-remediation",
        )[0]["reason_code"] == "remediation_exhausted"


def test_managed_retention_claim_is_recovered_after_restart(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "managed-retention.sqlite3"
    monkeypatch.setenv("ORCH_LOG_RETENTION_DAYS", "1")
    monkeypatch.setenv("ORCH_ARTIFACT_RETENTION_DAYS", "1")
    now = datetime(2026, 8, 12, tzinfo=timezone.utc)
    old = now - timedelta(days=2)
    artifact_path = str((tmp_path / "stale.zip").resolve())

    with StateRepository(database) as first:
        first.record_artifact(
            "task-retention",
            artifact_path,
            "a" * 64,
            size_bytes=10,
            metadata={"managed": True, "kind": "artifact"},
            now=old,
        )
        first.record_log_metadata(
            "task-retention",
            str((tmp_path / "terminal.log").resolve()),
            content_sha256="b" * 64,
            terminal_evidence=True,
            metadata={"managed": True, "kind": "log"},
            created_at=old,
        )
        original = first.claim_managed_retention(
            now=now,
            stale_before=now - timedelta(minutes=5),
            limit=10,
        )
        assert [(claim["kind"], claim["path"]) for claim in original] == [
            ("artifact", artifact_path)
        ]

    resumed_at = now + timedelta(minutes=10)
    with StateRepository(database) as restarted:
        recovered = restarted.claim_managed_retention(
            now=resumed_at,
            stale_before=now + timedelta(minutes=5),
            limit=10,
        )
        assert len(recovered) == 1
        assert recovered[0]["claim_id"] != original[0]["claim_id"]
        restarted.finalize_managed_retention(
            recovered[0],
            deleted=True,
            size_bytes=10,
            error=None,
            finalized_at=resumed_at,
        )
        assert restarted.list_artifacts("task-retention") == []
        assert len(restarted.list_log_records("task-retention")) == 1


def test_v9_schema_migrates_task_projection_and_retention_indexes(tmp_path):
    database = tmp_path / "v9.sqlite3"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE task_snapshots (
            task_id TEXT PRIMARY KEY,
            snapshot_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE events (
            task_id TEXT NOT NULL, session_id TEXT NOT NULL,
            sequence INTEGER NOT NULL, event_id TEXT NOT NULL UNIQUE,
            envelope_json TEXT NOT NULL, created_at TEXT NOT NULL,
            event_type TEXT NOT NULL DEFAULT '',
            is_terminal INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (task_id, sequence)
        );
        CREATE TABLE observability_records (
            record_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
            name TEXT NOT NULL, task_id TEXT, session_id TEXT,
            agent_instance_id TEXT, call_id TEXT, attempt_id TEXT,
            value REAL, duration_ms REAL, labels_json TEXT NOT NULL,
            attributes_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        PRAGMA user_version = 9;
        """
    )
    snapshot = {
        "id": "legacy-projection",
        "status": "COMPLETED",
        "mode": "orchestrator",
        "phase": "finished",
        "prompt": "not a summary field",
        "settings": {"auto_continue": True},
        "events": [{"type": "duplicated"}],
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    connection.execute(
        "INSERT INTO task_snapshots VALUES (?, ?, ?)",
        (
            "legacy-projection",
            json.dumps(snapshot),
            snapshot["updated_at"],
        ),
    )
    connection.commit()
    connection.close()

    with StateRepository(database) as migrated:
        saved = migrated.get_task_snapshot("legacy-projection")
        assert "events" not in saved
        assert migrated.list_task_summaries() == [
            {
                key: value
                for key, value in saved.items()
                if key not in {"prompt", "settings"}
            }
        ]
        with migrated._lock:
            columns = {
                row["name"]
                for row in migrated._connection.execute(
                    "PRAGMA table_info(task_snapshots)"
                ).fetchall()
            }
            indexes = {
                row["name"]
                for row in migrated._connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                ).fetchall()
            }
        assert {"status", "mode", "phase", "auto_continue", "summary_json"} <= columns
        assert {
            "ix_task_snapshots_updated",
            "ix_task_snapshots_resume",
            "ix_events_global_retention",
            "ix_observability_retention",
        } <= indexes


def test_failed_one_time_import_rolls_back_marker_and_rows(tmp_path):
    database = tmp_path / "import.sqlite3"
    snapshots = {
        "legacy": {
            "id": "legacy",
            "status": "COMPLETED",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
    }
    with StateRepository(database) as repository:
        with repository._lock:
            repository._connection.execute(
                """CREATE TRIGGER fail_legacy_import
                BEFORE INSERT ON task_snapshots
                BEGIN SELECT RAISE(ABORT, 'injected import crash'); END"""
            )
        with pytest.raises(sqlite3.IntegrityError, match="injected import crash"):
            repository.import_task_snapshots_once(
                "legacy_tasks_json_v1",
                "tasks.json",
                snapshots,
            )
        assert repository.data_migration("legacy_tasks_json_v1") is None
        assert repository.get_task_snapshot("legacy") is None

        with repository._lock:
            repository._connection.execute("DROP TRIGGER fail_legacy_import")
        assert repository.import_task_snapshots_once(
            "legacy_tasks_json_v1",
            "tasks.json",
            snapshots,
        )
        assert repository.get_task_snapshot("legacy")["status"] == "COMPLETED"


def test_one_time_import_never_overwrites_existing_canonical_task(repository):
    repository.save_task_snapshot(
        "same-id",
        {
            "id": "same-id",
            "status": "COMPLETED",
            "name": "Canonical",
        },
    )

    assert repository.import_task_snapshots_once(
        "legacy_tasks_json_v1",
        "tasks.json",
        {
            "same-id": {
                "id": "same-id",
                "status": "QUEUED",
                "name": "Stale JSON",
            }
        },
    )

    assert repository.get_task_snapshot("same-id")["name"] == "Canonical"
    assert repository.data_migration("legacy_tasks_json_v1")["details"] == {
        "record_count": 1,
        "imported_count": 0,
    }


def test_project_lease_fencing_rejects_stale_owner(repository):
    now = datetime.now(timezone.utc)
    first = repository.acquire_project_lease(
        "project-fenced", "owner-a", 10, now=now
    )
    assert first is not None
    assert repository.acquire_project_lease(
        "project-fenced", "owner-b", 10, now=now
    ) is None

    second = repository.acquire_project_lease(
        "project-fenced",
        "owner-b",
        10,
        now=now + timedelta(seconds=11),
    )
    assert second is not None
    assert second.fencing_token > first.fencing_token
    assert repository.heartbeat_project_lease(
        first.project_key,
        first.owner_id,
        first.fencing_token,
        10,
        now=now + timedelta(seconds=11),
    ) is None
    assert not repository.release_project_lease(
        first.project_key, first.owner_id, first.fencing_token
    )

    mutated = []
    with pytest.raises(ProjectLeaseLostError):
        repository.run_fenced_project_mutation(
            first.project_key,
            first.owner_id,
            first.fencing_token,
            lambda: mutated.append("stale"),
            now=now + timedelta(seconds=11),
        )
    assert mutated == []
    repository.run_fenced_project_mutation(
        second.project_key,
        second.owner_id,
        second.fencing_token,
        lambda: (
            assert_no_sqlite_transaction(repository),
            mutated.append("current"),
        ),
        now=now + timedelta(seconds=11),
    )
    assert mutated == ["current"]

    with pytest.raises(ProjectLeaseLostError, match="lost during mutation"):
        repository.run_fenced_project_mutation(
            second.project_key,
            second.owner_id,
            second.fencing_token,
            lambda: repository.release_project_lease(
                second.project_key,
                second.owner_id,
                second.fencing_token,
            ),
            now=now + timedelta(seconds=11),
        )


def assert_no_sqlite_transaction(repository):
    assert repository._connection.in_transaction is False
