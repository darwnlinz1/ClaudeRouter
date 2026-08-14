"""Transactional SQLite persistence for orchestrator state."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, TypeVar

from .effects import EffectReceipt, EffectState
from .event_schema import validate_event_payload
from .models import (
    Attempt,
    EventEnvelope,
    HandoffEnvelope,
    TaskPlan,
    WorkContract,
    WorkItem,
    Workstream,
    attempt_from_dict,
    canonical_contract_json,
    event_from_dict,
    handoff_from_dict,
    task_plan_from_dict,
    to_dict,
    work_contract_from_dict,
    work_contract_sha256,
)
from .project_workspace import ProjectLease, ProjectLeaseLostError

DEFAULT_DB_PATH = Path(
    os.environ.get(
        "ORCHESTRATOR_DB_PATH",
        Path.home() / ".ai_orchestrator" / "orchestrator.sqlite3",
    )
)
CURRENT_SCHEMA_VERSION = 14
TERMINAL_EVENT_TYPES = frozenset(
    {
        "completion_reconciliation",
        "effect_applied",
        "finish_chat_turn",
        "hierarchy_completed",
        "hierarchy_failed",
        "model_request_aborted",
        "model_request_completed",
        "model_request_failed",
        "project_lease_lost",
        "test_result",
    }
)

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    event_days: int = 30
    log_days: int = 14
    artifact_days: int = 90
    observability_days: int = 30
    max_events_per_task: int = 50_000

    def __post_init__(self) -> None:
        for name in (
            "event_days",
            "log_days",
            "artifact_days",
            "observability_days",
            "max_events_per_task",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")

    @classmethod
    def from_environment(cls) -> "RetentionPolicy":
        return cls(
            event_days=int(os.environ.get("ORCH_EVENT_RETENTION_DAYS", "30")),
            log_days=int(os.environ.get("ORCH_LOG_RETENTION_DAYS", "14")),
            artifact_days=int(
                os.environ.get("ORCH_ARTIFACT_RETENTION_DAYS", "90")
            ),
            observability_days=int(
                os.environ.get("ORCH_OBSERVABILITY_RETENTION_DAYS", "30")
            ),
            max_events_per_task=int(
                os.environ.get("ORCH_MAX_EVENTS_PER_TASK", "50000")
            ),
        )


def _json_dump(value: Any) -> str:
    return json.dumps(to_dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _json_load(value: str) -> Any:
    return json.loads(value)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _coerce_datetime(value: datetime | None) -> datetime:
    current = value or _utc_now()
    return (
        current
        if current.tzinfo is not None
        else current.replace(tzinfo=timezone.utc)
    )


def _validate_sha256(value: str | None, field_name: str) -> None:
    if value is None:
        return
    if len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise ValueError(f"{field_name} must be a hexadecimal SHA-256 digest")


class StateRepository:
    """Thread-safe repository with explicit, atomic write transactions.

    One repository owns one SQLite connection. Calls are serialized with an
    ``RLock`` while SQLite WAL mode still permits readers in other processes.
    """

    def __init__(self, db_path: str | os.PathLike[str] | None = None) -> None:
        self.db_path = str(db_path or DEFAULT_DB_PATH)
        self.project_lock_root = Path(
            os.environ.get(
                "ORCHESTRATOR_PROJECT_LOCK_ROOT",
                str(Path.home() / ".ai_orchestrator" / "project-locks"),
            )
        ).expanduser().resolve()
        if self.db_path != ":memory:":
            Path(self.db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._lifecycle_condition = threading.Condition(self._lock)
        self._accepting_writes = True
        self._active_transactions = 0
        self._closed = False
        self._connection = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA busy_timeout = 30000")
            if self.db_path != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
        self._create_schema()

    def _create_schema(self) -> None:
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        """Apply every pending numbered migration in its own transaction."""
        migrations: dict[int, tuple[str, Callable[[sqlite3.Connection], None]]] = {
            1: ("core_state", self._migration_1_core_state),
            2: ("project_fencing", self._migration_2_project_fencing),
            3: ("durable_event_cursors", self._migration_3_event_cursors),
            4: ("observability_retention_records", self._migration_4_records),
            5: ("completion_invariants", self._migration_5_completion_invariants),
            6: ("canonical_task_snapshots", self._migration_6_task_snapshots),
            7: ("human_approval_queue", self._migration_7_approval_queue),
            8: ("wave5_platform_core", self._migration_8_wave5_platform_core),
            9: ("audit_sequence", self._migration_9_audit_sequence),
            10: ("canonical_task_projection", self._migration_10_task_projection),
            11: ("projection_retention_indexes", self._migration_11_indexes),
            12: ("durable_effect_fencing", self._migration_12_durable_effects),
            13: ("durable_coordination", self._migration_13_durable_coordination),
            14: ("managed_retention_claims", self._migration_14_managed_retention),
        }
        with self._lock:
            version = int(
                self._connection.execute("PRAGMA user_version").fetchone()[0]
            )
            if version > CURRENT_SCHEMA_VERSION:
                raise RuntimeError(
                    f"database schema {version} is newer than supported "
                    f"schema {CURRENT_SCHEMA_VERSION}"
                )
            for target_version in range(version + 1, CURRENT_SCHEMA_VERSION + 1):
                name, migration = migrations[target_version]
                self._connection.execute("BEGIN IMMEDIATE")
                try:
                    self._connection.execute(
                        """
                        CREATE TABLE IF NOT EXISTS schema_migrations (
                            version INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            applied_at TEXT NOT NULL
                        )
                        """
                    )
                    migration(self._connection)
                    self._connection.execute(
                        "INSERT OR REPLACE INTO schema_migrations"
                        "(version, name, applied_at) VALUES (?, ?, ?)",
                        (target_version, name, _utc_now().isoformat()),
                    )
                    self._connection.execute(
                        f"PRAGMA user_version = {target_version}"
                    )
                except BaseException:
                    self._connection.rollback()
                    raise
                else:
                    self._connection.commit()

    @staticmethod
    def _execute_all(
        connection: sqlite3.Connection,
        statements: tuple[str, ...],
    ) -> None:
        for statement in statements:
            connection.execute(statement)

    @staticmethod
    def _columns(
        connection: sqlite3.Connection,
        table: str,
    ) -> set[str]:
        return {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }

    def _migration_1_core_state(self, connection: sqlite3.Connection) -> None:
        self._execute_all(
            connection,
            (
                """CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    goal TEXT NOT NULL, status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS plans (
                    task_id TEXT NOT NULL, revision INTEGER NOT NULL,
                    plan_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY (task_id, revision))""",
                """CREATE TABLE IF NOT EXISTS workstreams (
                    task_id TEXT NOT NULL, revision INTEGER NOT NULL,
                    workstream_id TEXT NOT NULL, state_json TEXT NOT NULL,
                    PRIMARY KEY (task_id, revision, workstream_id),
                    FOREIGN KEY (task_id, revision)
                    REFERENCES plans(task_id, revision) ON DELETE CASCADE)""",
                """CREATE TABLE IF NOT EXISTS work_items (
                    task_id TEXT NOT NULL, revision INTEGER NOT NULL,
                    workstream_id TEXT NOT NULL, work_item_id TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    PRIMARY KEY (task_id, revision, work_item_id),
                    FOREIGN KEY (task_id, revision, workstream_id)
                    REFERENCES workstreams(task_id, revision, workstream_id)
                    ON DELETE CASCADE)""",
                """CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    workstream_id TEXT NOT NULL, work_item_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL, state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(task_id, work_item_id, attempt_number))""",
                """CREATE TABLE IF NOT EXISTS events (
                    task_id TEXT NOT NULL, session_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL, event_id TEXT NOT NULL UNIQUE,
                    envelope_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY (task_id, sequence))""",
                """CREATE INDEX IF NOT EXISTS events_session_sequence
                    ON events(session_id, sequence)""",
                """CREATE TABLE IF NOT EXISTS leases (
                    resource_type TEXT NOT NULL, resource_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL, expires_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (resource_type, resource_id))""",
                """CREATE INDEX IF NOT EXISTS leases_expiry
                    ON leases(expires_at)""",
                """CREATE TABLE IF NOT EXISTS project_locks (
                    project_key TEXT PRIMARY KEY, owner_id TEXT NOT NULL,
                    expires_at TEXT NOT NULL, purpose TEXT NOT NULL DEFAULT '')""",
                """CREATE TABLE IF NOT EXISTS project_fencing_counters (
                    project_key TEXT PRIMARY KEY, last_token INTEGER NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS effect_receipts (
                    effect_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL, kind TEXT NOT NULL,
                    target TEXT NOT NULL, before_sha256 TEXT, after_sha256 TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    state TEXT NOT NULL CHECK(
                        state IN ('pending','applied','failed','reconciled')),
                    attempts INTEGER NOT NULL DEFAULT 1 CHECK(attempts >= 1),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    started_at TEXT NOT NULL, completed_at TEXT, error TEXT,
                    UNIQUE(task_id, idempotency_key))""",
                """CREATE INDEX IF NOT EXISTS effect_receipts_task_state
                    ON effect_receipts(task_id, state, created_at)""",
                """CREATE INDEX IF NOT EXISTS effect_receipts_state_updated
                    ON effect_receipts(state, updated_at)""",
            ),
        )

    def _migration_2_project_fencing(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        columns = self._columns(connection, "project_locks")
        additions = {
            "fencing_token": (
                "ALTER TABLE project_locks ADD COLUMN "
                "fencing_token INTEGER NOT NULL DEFAULT 0"
            ),
            "heartbeat_at": (
                "ALTER TABLE project_locks ADD COLUMN "
                "heartbeat_at TEXT NOT NULL DEFAULT ''"
            ),
            "metadata_json": (
                "ALTER TABLE project_locks ADD COLUMN "
                "metadata_json TEXT NOT NULL DEFAULT '{}'"
            ),
        }
        for column, statement in additions.items():
            if column not in columns:
                connection.execute(statement)
        now = _utc_now().isoformat()
        connection.execute(
            "UPDATE project_locks SET heartbeat_at = ? "
            "WHERE heartbeat_at IS NULL OR heartbeat_at = ''",
            (now,),
        )
        connection.execute(
            "UPDATE project_locks SET fencing_token = 0 "
            "WHERE fencing_token IS NULL"
        )
        connection.execute(
            "UPDATE project_locks SET metadata_json = '{}' "
            "WHERE metadata_json IS NULL OR metadata_json = ''"
        )
        connection.execute(
            """INSERT INTO project_fencing_counters(project_key, last_token)
            SELECT project_key, fencing_token FROM project_locks WHERE 1
            ON CONFLICT(project_key) DO UPDATE SET
            last_token=MAX(last_token, excluded.last_token)"""
        )

    def _migration_3_event_cursors(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        columns = self._columns(connection, "events")
        if "event_type" not in columns:
            connection.execute(
                "ALTER TABLE events ADD COLUMN event_type TEXT NOT NULL DEFAULT ''"
            )
        if "is_terminal" not in columns:
            connection.execute(
                "ALTER TABLE events ADD COLUMN is_terminal INTEGER NOT NULL DEFAULT 0"
            )
        for row in connection.execute(
            "SELECT event_id, envelope_json FROM events WHERE event_type = ''"
        ).fetchall():
            try:
                event_type = str(_json_load(row["envelope_json"]).get("event_type") or "")
            except (TypeError, ValueError, json.JSONDecodeError):
                event_type = ""
            connection.execute(
                "UPDATE events SET event_type = ?, is_terminal = ? "
                "WHERE event_id = ?",
                (
                    event_type,
                    int(event_type in TERMINAL_EVENT_TYPES),
                    row["event_id"],
                ),
            )
        self._execute_all(
            connection,
            (
                """CREATE INDEX IF NOT EXISTS events_retention
                    ON events(task_id, is_terminal, created_at, sequence)""",
                """CREATE TABLE IF NOT EXISTS event_cursors (
                    task_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    latest_sequence INTEGER NOT NULL,
                    retained_from_sequence INTEGER NOT NULL,
                    updated_at TEXT NOT NULL)""",
            ),
        )
        now = _utc_now().isoformat()
        connection.execute(
            """INSERT OR REPLACE INTO event_cursors(
                task_id, session_id, latest_sequence,
                retained_from_sequence, updated_at)
            SELECT task_id, MAX(session_id), MAX(sequence), MIN(sequence), ?
            FROM events GROUP BY task_id""",
            (now,),
        )

    def _migration_4_records(self, connection: sqlite3.Connection) -> None:
        self._execute_all(
            connection,
            (
                """CREATE TABLE IF NOT EXISTS observability_records (
                    record_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                    name TEXT NOT NULL, task_id TEXT, session_id TEXT,
                    agent_instance_id TEXT, call_id TEXT, attempt_id TEXT,
                    value REAL, duration_ms REAL, labels_json TEXT NOT NULL,
                    attributes_json TEXT NOT NULL, created_at TEXT NOT NULL)""",
                """CREATE INDEX IF NOT EXISTS observability_task_created
                    ON observability_records(task_id, created_at)""",
                """CREATE TABLE IF NOT EXISTS log_records (
                    log_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    session_id TEXT, agent_instance_id TEXT, call_id TEXT,
                    attempt_id TEXT, path TEXT NOT NULL, content_sha256 TEXT,
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    terminal_evidence INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL)""",
                """CREATE INDEX IF NOT EXISTS log_records_retention
                    ON log_records(terminal_evidence, created_at)""",
                """CREATE TABLE IF NOT EXISTS artifact_records (
                    record_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    session_id TEXT, path TEXT NOT NULL, content_sha256 TEXT,
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    approved INTEGER NOT NULL DEFAULT 0,
                    terminal_evidence INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(task_id, path))""",
                """CREATE INDEX IF NOT EXISTS artifact_records_retention
                    ON artifact_records(terminal_evidence, updated_at)""",
            ),
        )

    def _migration_5_completion_invariants(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS task_completion_invariants (
                task_id TEXT PRIMARY KEY, session_id TEXT,
                requested_status TEXT NOT NULL, effective_status TEXT NOT NULL,
                balanced INTEGER NOT NULL, started_calls INTEGER NOT NULL,
                terminal_calls INTEGER NOT NULL,
                unresolved_call_ids_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL, recorded_at TEXT NOT NULL)"""
        )

    def _migration_6_task_snapshots(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS task_snapshots (
                task_id TEXT PRIMARY KEY,
                snapshot_json TEXT NOT NULL,
                updated_at TEXT NOT NULL)"""
        )

    def _migration_7_approval_queue(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        self._execute_all(
            connection,
            (
                """CREATE TABLE IF NOT EXISTS approval_requests (
                    approval_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    workstream_id TEXT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    target TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    decided_at TEXT,
                    decision_reason TEXT)""",
                """CREATE INDEX IF NOT EXISTS ix_approval_task_status
                   ON approval_requests(task_id, status, created_at)""",
            ),
        )

    def _migration_8_wave5_platform_core(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        self._execute_all(
            connection,
            (
                """CREATE TABLE IF NOT EXISTS event_projections (
                    task_id TEXT PRIMARY KEY,
                    sequence INTEGER NOT NULL CHECK(sequence >= 0),
                    checksum TEXT NOT NULL,
                    projection_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL)""",
                """CREATE TABLE IF NOT EXISTS durable_jobs (
                    job_id TEXT PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    task_id TEXT,
                    kind TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN (
                        'queued','running','retry','completed','failed','cancelled'
                    )),
                    priority INTEGER NOT NULL DEFAULT 0 CHECK(priority >= 0),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                    max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
                    available_at TEXT NOT NULL,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    fencing_token INTEGER NOT NULL DEFAULT 0
                        CHECK(fencing_token >= 0),
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    result_json TEXT NOT NULL DEFAULT '{}',
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    UNIQUE(namespace, idempotency_key))""",
                """CREATE INDEX IF NOT EXISTS ix_durable_jobs_claim
                    ON durable_jobs(
                        namespace, status, available_at, priority, created_at
                    )""",
                """CREATE INDEX IF NOT EXISTS ix_durable_jobs_task
                    ON durable_jobs(task_id, status, created_at)""",
                """CREATE TABLE IF NOT EXISTS job_fencing_counters (
                    job_id TEXT PRIMARY KEY,
                    last_token INTEGER NOT NULL CHECK(last_token >= 0))""",
                """CREATE TABLE IF NOT EXISTS audit_records (
                    audit_id TEXT PRIMARY KEY,
                    namespace TEXT NOT NULL,
                    audit_sequence INTEGER NOT NULL CHECK(audit_sequence >= 1),
                    actor_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    reasons_json TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    previous_hash TEXT,
                    record_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    UNIQUE(namespace, audit_sequence))""",
                """CREATE INDEX IF NOT EXISTS ix_audit_namespace_created
                    ON audit_records(namespace, audit_sequence)""",
            ),
        )

    def _migration_9_audit_sequence(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        columns = self._columns(connection, "audit_records")
        if "audit_sequence" not in columns:
            connection.execute(
                "ALTER TABLE audit_records ADD COLUMN audit_sequence INTEGER"
            )
        namespaces = connection.execute(
            "SELECT DISTINCT namespace FROM audit_records"
        ).fetchall()
        for namespace_row in namespaces:
            rows = connection.execute(
                """SELECT audit_id FROM audit_records
                WHERE namespace = ?
                ORDER BY created_at, audit_id""",
                (namespace_row["namespace"],),
            ).fetchall()
            for sequence, row in enumerate(rows, start=1):
                connection.execute(
                    "UPDATE audit_records SET audit_sequence = ? "
                    "WHERE audit_id = ?",
                    (sequence, row["audit_id"]),
                )
        connection.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS
            ux_audit_namespace_sequence
            ON audit_records(namespace, audit_sequence)"""
        )

    def _migration_10_task_projection(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS data_migrations (
                migration_key TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}',
                applied_at TEXT NOT NULL)"""
        )
        columns = self._columns(connection, "task_snapshots")
        additions = {
            "status": (
                "ALTER TABLE task_snapshots ADD COLUMN "
                "status TEXT NOT NULL DEFAULT ''"
            ),
            "mode": (
                "ALTER TABLE task_snapshots ADD COLUMN "
                "mode TEXT NOT NULL DEFAULT ''"
            ),
            "phase": (
                "ALTER TABLE task_snapshots ADD COLUMN "
                "phase TEXT NOT NULL DEFAULT ''"
            ),
            "auto_continue": (
                "ALTER TABLE task_snapshots ADD COLUMN "
                "auto_continue INTEGER NOT NULL DEFAULT 0"
            ),
            "summary_json": (
                "ALTER TABLE task_snapshots ADD COLUMN "
                "summary_json TEXT NOT NULL DEFAULT '{}'"
            ),
        }
        for column, statement in additions.items():
            if column not in columns:
                connection.execute(statement)
        rows = connection.execute(
            "SELECT task_id, snapshot_json, updated_at FROM task_snapshots"
        ).fetchall()
        for row in rows:
            try:
                snapshot = _json_load(row["snapshot_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(snapshot, dict):
                continue
            normalized, summary, values = self._task_snapshot_values(
                str(row["task_id"]),
                snapshot,
                fallback_updated_at=str(row["updated_at"]),
            )
            connection.execute(
                """UPDATE task_snapshots SET
                    snapshot_json = ?, updated_at = ?, status = ?,
                    mode = ?, phase = ?, auto_continue = ?, summary_json = ?
                WHERE task_id = ?""",
                (
                    _json_dump(normalized),
                    values["updated_at"],
                    values["status"],
                    values["mode"],
                    values["phase"],
                    values["auto_continue"],
                    _json_dump(summary),
                    row["task_id"],
                ),
            )

    def _migration_11_indexes(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        self._execute_all(
            connection,
            (
                """CREATE INDEX IF NOT EXISTS ix_task_snapshots_updated
                    ON task_snapshots(updated_at DESC, task_id)""",
                """CREATE INDEX IF NOT EXISTS ix_task_snapshots_resume
                    ON task_snapshots(
                        status, mode, auto_continue, updated_at DESC, task_id
                    )""",
                """CREATE INDEX IF NOT EXISTS ix_events_global_retention
                    ON events(is_terminal, created_at, task_id, sequence)""",
                """CREATE INDEX IF NOT EXISTS ix_observability_retention
                    ON observability_records(created_at)""",
            ),
        )

    def _migration_12_durable_effects(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        """Add effect fencing and compensation without rewriting existing rows."""
        connection.execute(
            """CREATE TABLE IF NOT EXISTS effect_receipts (
                effect_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL, kind TEXT NOT NULL,
                target TEXT NOT NULL, before_sha256 TEXT, after_sha256 TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL CHECK(
                    state IN ('pending','applied','failed','reconciled')),
                attempts INTEGER NOT NULL DEFAULT 1 CHECK(attempts >= 1),
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                started_at TEXT NOT NULL, completed_at TEXT, error TEXT,
                UNIQUE(task_id, idempotency_key))"""
        )
        columns = self._columns(connection, "effect_receipts")
        additions = {
            "expected_after_sha256": (
                "ALTER TABLE effect_receipts ADD COLUMN expected_after_sha256 TEXT"
            ),
            "fencing_token": (
                "ALTER TABLE effect_receipts ADD COLUMN fencing_token INTEGER"
            ),
            "compensates_effect_id": (
                "ALTER TABLE effect_receipts ADD COLUMN compensates_effect_id TEXT"
            ),
            "compensated_by_effect_id": (
                "ALTER TABLE effect_receipts ADD COLUMN compensated_by_effect_id TEXT"
            ),
            "compensated_at": (
                "ALTER TABLE effect_receipts ADD COLUMN compensated_at TEXT"
            ),
        }
        for column, statement in additions.items():
            if column not in columns:
                connection.execute(statement)
        self._execute_all(
            connection,
            (
                """CREATE INDEX IF NOT EXISTS ix_effect_receipts_pending_target
                    ON effect_receipts(state, target, expected_after_sha256)""",
                """CREATE UNIQUE INDEX IF NOT EXISTS
                    ux_effect_receipts_compensates
                    ON effect_receipts(compensates_effect_id)
                    WHERE compensates_effect_id IS NOT NULL""",
                """CREATE UNIQUE INDEX IF NOT EXISTS
                    ux_effect_receipts_compensated_by
                    ON effect_receipts(compensated_by_effect_id)
                    WHERE compensated_by_effect_id IS NOT NULL""",
            ),
        )

    def _migration_13_durable_coordination(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        """Add immutable contracts, stable agent identities, and handoffs."""

        self._execute_all(
            connection,
            (
                """CREATE TABLE IF NOT EXISTS contract_versions (
                    task_id TEXT NOT NULL,
                    contract_id TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version >= 1),
                    canonical_json TEXT NOT NULL,
                    sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (task_id, contract_id, version))""",
                """CREATE INDEX IF NOT EXISTS ix_contract_versions_task
                    ON contract_versions(task_id, contract_id, version)""",
                """CREATE TABLE IF NOT EXISTS agent_identities (
                    task_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    assignment_id TEXT NOT NULL,
                    logical_agent_id TEXT NOT NULL,
                    supersedes_logical_agent_id TEXT,
                    superseded_by_logical_agent_id TEXT,
                    supersession_reason TEXT,
                    superseded_at TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (task_id, role, assignment_id),
                    UNIQUE (task_id, logical_agent_id))""",
                """CREATE INDEX IF NOT EXISTS ix_agent_identities_logical
                    ON agent_identities(task_id, logical_agent_id)""",
                """CREATE TABLE IF NOT EXISTS handoffs (
                    handoff_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    contract_id TEXT NOT NULL,
                    contract_version INTEGER NOT NULL
                        CHECK(contract_version >= 1),
                    source_agent_id TEXT NOT NULL,
                    target_agent_id TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    artifacts_json TEXT NOT NULL DEFAULT '[]',
                    evidence_json TEXT NOT NULL DEFAULT '{}',
                    workstream_id TEXT,
                    work_item_id TEXT,
                    created_at TEXT NOT NULL,
                    sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
                    FOREIGN KEY (task_id, contract_id, contract_version)
                    REFERENCES contract_versions(task_id, contract_id, version)
                    ON DELETE RESTRICT)""",
                """CREATE INDEX IF NOT EXISTS ix_handoffs_task_created
                    ON handoffs(task_id, created_at, handoff_id)""",
                """CREATE INDEX IF NOT EXISTS ix_handoffs_contract
                    ON handoffs(
                        task_id, contract_id, contract_version, created_at
                    )""",
            ),
        )

    def _migration_14_managed_retention(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        """Add short-lived, recoverable claims to managed file records."""

        # Some legacy databases were stamped at a newer user_version after
        # importing only a subset of tables. Reassert the additive v4 tables
        # so this migration is safe for those stores as well as full schemas.
        self._migration_4_records(connection)
        additions = {
            "retention_state": (
                "TEXT NOT NULL DEFAULT 'active'"
            ),
            "retention_claim_id": "TEXT",
            "retention_claimed_at": "TEXT",
            "retention_error": "TEXT",
            "retention_finalized_at": "TEXT",
        }
        for table in ("log_records", "artifact_records"):
            columns = self._columns(connection, table)
            for column, definition in additions.items():
                if column not in columns:
                    connection.execute(
                        f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
                    )
        self._execute_all(
            connection,
            (
                """CREATE INDEX IF NOT EXISTS ix_log_managed_retention
                    ON log_records(
                        retention_state, terminal_evidence, created_at, log_id
                    )""",
                """CREATE INDEX IF NOT EXISTS ix_artifact_managed_retention
                    ON artifact_records(
                        retention_state, terminal_evidence, updated_at, record_id
                    )""",
            ),
        )

    @property
    def current_schema_version(self) -> int:
        with self._lock:
            return int(
                self._connection.execute("PRAGMA user_version").fetchone()[0]
            )

    def migration_history(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT version, name, applied_at FROM schema_migrations "
                "ORDER BY version"
            ).fetchall()
        return [dict(row) for row in rows]

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Open a transaction and roll it back on every exception."""
        with self._lifecycle_condition:
            if not self._accepting_writes or self._closed:
                raise RuntimeError("state repository is closing")
            self._active_transactions += 1
            try:
                self._connection.execute(
                    "BEGIN IMMEDIATE" if immediate else "BEGIN"
                )
                try:
                    yield self._connection
                except BaseException:
                    self._connection.rollback()
                    raise
                else:
                    self._connection.commit()
            finally:
                self._active_transactions -= 1
                self._lifecycle_condition.notify_all()

    def stop_accepting(self) -> None:
        with self._lifecycle_condition:
            self._accepting_writes = False
            self._lifecycle_condition.notify_all()

    def drain(self, timeout: float = 30.0) -> bool:
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        with self._lifecycle_condition:
            return self._lifecycle_condition.wait_for(
                lambda: self._active_transactions == 0,
                timeout=timeout,
            )

    def close(self, timeout: float = 30.0) -> bool:
        self.stop_accepting()
        if not self.drain(timeout):
            return False
        with self._lifecycle_condition:
            if self._closed:
                return True
            self._connection.close()
            self._closed = True
            self._lifecycle_condition.notify_all()
        return True

    def __enter__(self) -> "StateRepository":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def save_task(
        self,
        task_id: str,
        session_id: str,
        goal: str,
        *,
        status: str = "pending",
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not task_id.strip() or not session_id.strip() or not goal.strip():
            raise ValueError("task_id, session_id, and goal are required")
        now = _utc_now().isoformat()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO tasks (
                    task_id, session_id, goal, status, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    session_id=excluded.session_id,
                    goal=excluded.goal,
                    status=excluded.status,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (task_id, session_id, goal, status, _json_dump(dict(metadata or {})), now, now),
            )

    def save_task_snapshot(
        self,
        task_id: str,
        snapshot: Mapping[str, Any],
    ) -> None:
        """Persist one canonical operator-facing task projection atomically."""

        with self.transaction() as connection:
            self._upsert_task_snapshot(connection, task_id, snapshot)

    @staticmethod
    def _task_snapshot_values(
        task_id: str,
        snapshot: Mapping[str, Any],
        *,
        fallback_updated_at: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        if not task_id.strip():
            raise ValueError("task_id is required")
        serialized = deepcopy(dict(snapshot))
        snapshot_id = str(serialized.get("id") or task_id)
        if snapshot_id != task_id:
            raise ValueError("task snapshot id must match task_id")
        serialized["id"] = task_id
        # Events have their own canonical append-only table. Keeping a second
        # bounded copy in every projection created divergent histories.
        serialized.pop("events", None)
        updated_at = str(
            serialized.get("updated_at")
            or fallback_updated_at
            or _utc_now().isoformat()
        )
        serialized["updated_at"] = updated_at
        settings = serialized.get("settings")
        auto_continue = bool(
            isinstance(settings, Mapping) and settings.get("auto_continue") is True
        )
        summary = deepcopy(serialized)
        summary.pop("prompt", None)
        summary.pop("settings", None)
        values = {
            "updated_at": updated_at,
            "status": str(serialized.get("status") or ""),
            "mode": str(serialized.get("mode") or ""),
            "phase": str(serialized.get("phase") or ""),
            "auto_continue": int(auto_continue),
        }
        return serialized, summary, values

    @classmethod
    def _upsert_task_snapshot(
        cls,
        connection: sqlite3.Connection,
        task_id: str,
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        serialized, summary, values = cls._task_snapshot_values(task_id, snapshot)
        connection.execute(
            """
            INSERT INTO task_snapshots(
                task_id, snapshot_json, updated_at, status, mode, phase,
                auto_continue, summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                snapshot_json=excluded.snapshot_json,
                updated_at=excluded.updated_at,
                status=excluded.status,
                mode=excluded.mode,
                phase=excluded.phase,
                auto_continue=excluded.auto_continue,
                summary_json=excluded.summary_json
            """,
            (
                task_id,
                _json_dump(serialized),
                values["updated_at"],
                values["status"],
                values["mode"],
                values["phase"],
                values["auto_continue"],
                _json_dump(summary),
            ),
        )
        return serialized

    def get_task_snapshot(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT snapshot_json FROM task_snapshots WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return deepcopy(_json_load(row["snapshot_json"])) if row else None

    def mutate_task_snapshot(
        self,
        task_id: str,
        mutation: Callable[[dict[str, Any]], Mapping[str, Any] | None],
    ) -> dict[str, Any] | None:
        """Read, mutate, and write one task projection in one transaction."""

        with self.transaction() as connection:
            row = connection.execute(
                "SELECT snapshot_json FROM task_snapshots WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                return None
            current = deepcopy(_json_load(row["snapshot_json"]))
            updated = mutation(current)
            if updated is None:
                updated = current
            return deepcopy(
                self._upsert_task_snapshot(connection, task_id, updated)
            )

    def import_task_snapshots_once(
        self,
        migration_key: str,
        source: str,
        snapshots: Mapping[str, Mapping[str, Any]],
    ) -> bool:
        """Atomically record a durable import marker and import its rows once."""

        if not migration_key.strip() or not source.strip():
            raise ValueError("migration_key and source are required")
        imported_at = _utc_now().isoformat()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT 1 FROM data_migrations WHERE migration_key = ?",
                (migration_key,),
            ).fetchone()
            if existing is not None:
                return False
            # The marker and imported rows commit together. A crash at any point
            # rolls both back, while deleting task rows later leaves the marker.
            connection.execute(
                """INSERT INTO data_migrations(
                    migration_key, source, details_json, applied_at
                ) VALUES (?, ?, ?, ?)""",
                (
                    migration_key,
                    source,
                    _json_dump({"record_count": len(snapshots)}),
                    imported_at,
                ),
            )
            imported_count = 0
            for task_id, snapshot in snapshots.items():
                present = connection.execute(
                    "SELECT 1 FROM task_snapshots WHERE task_id = ?",
                    (str(task_id),),
                ).fetchone()
                if present is not None:
                    continue
                self._upsert_task_snapshot(
                    connection,
                    str(task_id),
                    snapshot,
                )
                imported_count += 1
            connection.execute(
                "UPDATE data_migrations SET details_json = ? "
                "WHERE migration_key = ?",
                (
                    _json_dump(
                        {
                            "record_count": len(snapshots),
                            "imported_count": imported_count,
                        }
                    ),
                    migration_key,
                ),
            )
        return True

    def data_migration(self, migration_key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM data_migrations WHERE migration_key = ?",
                (migration_key,),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["details"] = _json_load(result.pop("details_json"))
        return result

    def save_event_projection(
        self,
        task_id: str,
        projection: Mapping[str, Any],
        *,
        sequence: int,
        checksum: str,
    ) -> None:
        if not task_id.strip() or sequence < 0:
            raise ValueError("task_id is required and sequence must be non-negative")
        _validate_sha256(checksum, "checksum")
        value = deepcopy(dict(projection))
        if str(value.get("task_id") or "") != task_id:
            raise ValueError("projection task_id must match")
        if int(value.get("sequence", -1)) != sequence:
            raise ValueError("projection sequence must match")
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT sequence, checksum FROM event_projections WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if existing is not None and int(existing["sequence"]) > sequence:
                raise RuntimeError("cannot replace a newer event projection")
            connection.execute(
                """INSERT INTO event_projections(
                    task_id, sequence, checksum, projection_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    sequence=excluded.sequence,
                    checksum=excluded.checksum,
                    projection_json=excluded.projection_json,
                    updated_at=excluded.updated_at""",
                (
                    task_id,
                    sequence,
                    checksum.lower(),
                    _json_dump(value),
                    _utc_now().isoformat(),
                ),
            )

    def get_event_projection(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM event_projections WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "task_id": str(row["task_id"]),
            "sequence": int(row["sequence"]),
            "checksum": str(row["checksum"]),
            "projection": deepcopy(_json_load(row["projection_json"])),
            "updated_at": str(row["updated_at"]),
        }

    def list_task_snapshots(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT snapshot_json FROM task_snapshots "
                "ORDER BY updated_at DESC, task_id"
            ).fetchall()
        return [deepcopy(_json_load(row["snapshot_json"])) for row in rows]

    def list_task_summaries(self) -> list[dict[str, Any]]:
        """Return precomputed summaries without decoding complete snapshots."""

        with self._lock:
            rows = self._connection.execute(
                "SELECT summary_json FROM task_snapshots "
                "ORDER BY updated_at DESC, task_id"
            ).fetchall()
        return [deepcopy(_json_load(row["summary_json"])) for row in rows]

    def list_auto_resumable_task_snapshots(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """SELECT snapshot_json FROM task_snapshots
                WHERE status = 'INTERRUPTED'
                  AND mode = 'orchestrator'
                  AND auto_continue = 1
                ORDER BY updated_at DESC, task_id"""
            ).fetchall()
        return [deepcopy(_json_load(row["snapshot_json"])) for row in rows]

    def list_task_snapshots_by_status(
        self,
        statuses: set[str] | frozenset[str],
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        ordered = sorted(str(status) for status in statuses)
        placeholders = ",".join("?" for _ in ordered)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT snapshot_json FROM task_snapshots "
                f"WHERE status IN ({placeholders}) "
                "ORDER BY updated_at DESC, task_id",
                tuple(ordered),
            ).fetchall()
        return [deepcopy(_json_load(row["snapshot_json"])) for row in rows]

    def delete_task_snapshot(self, task_id: str) -> bool:
        with self.transaction() as connection:
            deleted = connection.execute(
                "DELETE FROM task_snapshots WHERE task_id = ?",
                (task_id,),
            ).rowcount
        return bool(deleted)

    @staticmethod
    def _approval_row(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["payload"] = _json_load(item.pop("payload_json"))
        return item

    def request_approval(
        self,
        task_id: str,
        *,
        idempotency_key: str,
        kind: str,
        target: str,
        reason: str,
        payload: Mapping[str, Any] | None = None,
        workstream_id: str | None = None,
    ) -> dict[str, Any]:
        for name, value in {
            "task_id": task_id,
            "idempotency_key": idempotency_key,
            "kind": kind,
            "target": target,
            "reason": reason,
        }.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        approval_id = f"approval_{uuid.uuid4().hex}"
        created_at = _utc_now().isoformat()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO approval_requests(
                    approval_id, task_id, workstream_id, idempotency_key,
                    kind, target, reason, status, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                ON CONFLICT(idempotency_key) DO NOTHING
                """,
                (
                    approval_id,
                    task_id,
                    workstream_id,
                    idempotency_key,
                    kind,
                    target,
                    reason,
                    _json_dump(dict(payload or {})),
                    created_at,
                ),
            )
            row = connection.execute(
                "SELECT * FROM approval_requests WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        if row is None:
            raise RuntimeError("approval request was not persisted")
        return self._approval_row(row)

    def list_approvals(
        self,
        task_id: str | None = None,
        *,
        status: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        clauses: list[str] = []
        params: list[Any] = []
        if task_id is not None:
            clauses.append("task_id = ?")
            params.append(task_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        query = "SELECT * FROM approval_requests"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, approval_id LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._approval_row(row) for row in rows]

    def decide_approval(
        self,
        approval_id: str,
        *,
        decision: str,
        reason: str = "",
    ) -> dict[str, Any]:
        normalized = decision.strip().lower()
        if normalized not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")
        decided_at = _utc_now().isoformat()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM approval_requests WHERE approval_id = ?",
                (approval_id,),
            ).fetchone()
            if row is None:
                raise KeyError(approval_id)
            if row["status"] == "pending":
                connection.execute(
                    """
                    UPDATE approval_requests
                    SET status = ?, decided_at = ?, decision_reason = ?
                    WHERE approval_id = ? AND status = 'pending'
                    """,
                    (normalized, decided_at, reason.strip(), approval_id),
                )
            elif row["status"] != normalized:
                raise RuntimeError("approval was already decided differently")
            row = connection.execute(
                "SELECT * FROM approval_requests WHERE approval_id = ?",
                (approval_id,),
            ).fetchone()
        return self._approval_row(row)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["metadata"] = _json_load(result.pop("metadata_json"))
        return result

    @staticmethod
    def _save_contract_version_in_transaction(
        connection: sqlite3.Connection,
        task_id: str,
        contract: WorkContract,
    ) -> WorkContract:
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("task_id must be a non-empty string")
        if not isinstance(contract, WorkContract):
            raise TypeError("contract must be a WorkContract")
        serialized = canonical_contract_json(contract)
        digest = work_contract_sha256(contract)
        existing = connection.execute(
            """SELECT canonical_json, sha256 FROM contract_versions
            WHERE task_id = ? AND contract_id = ? AND version = ?""",
            (task_id, contract.id, contract.version),
        ).fetchone()
        if existing is not None:
            if (
                str(existing["canonical_json"]) == serialized
                and str(existing["sha256"]) == digest
            ):
                return contract
            raise RuntimeError(
                "contract version conflict: immutable contract content differs"
            )
        connection.execute(
            """INSERT INTO contract_versions(
                task_id, contract_id, version, canonical_json, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                task_id,
                contract.id,
                contract.version,
                serialized,
                digest,
                _utc_now().isoformat(),
            ),
        )
        return contract

    def save_contract_version(
        self,
        task_id: str,
        contract: WorkContract,
        owner_type: str | None = None,
        owner_id: str | None = None,
    ) -> WorkContract:
        """Persist one immutable Work Contract version idempotently."""

        # Ownership stays in the normalized plan DAG. These optional arguments
        # keep the repository hook compatible with hierarchy rollout callers.
        del owner_type, owner_id
        with self.transaction() as connection:
            return self._save_contract_version_in_transaction(
                connection,
                task_id,
                contract,
            )

    @staticmethod
    def _contract_from_row(row: sqlite3.Row) -> WorkContract:
        contract = work_contract_from_dict(_json_load(row["canonical_json"]))
        if (
            contract.id != str(row["contract_id"])
            or contract.version != int(row["version"])
            or canonical_contract_json(contract) != str(row["canonical_json"])
            or work_contract_sha256(contract) != str(row["sha256"])
        ):
            raise RuntimeError("persisted contract version failed integrity check")
        return contract

    def get_contract_version(
        self,
        task_id: str,
        contract_id: str,
        version: int,
    ) -> WorkContract | None:
        with self._lock:
            row = self._connection.execute(
                """SELECT * FROM contract_versions
                WHERE task_id = ? AND contract_id = ? AND version = ?""",
                (task_id, contract_id, version),
            ).fetchone()
        return self._contract_from_row(row) if row else None

    def get_contract(
        self,
        task_id: str,
        contract_id: str,
        version: int | None = None,
    ) -> WorkContract | None:
        """Return an exact contract version, or the latest version."""

        if version is not None:
            return self.get_contract_version(task_id, contract_id, version)
        with self._lock:
            row = self._connection.execute(
                """SELECT * FROM contract_versions
                WHERE task_id = ? AND contract_id = ?
                ORDER BY version DESC LIMIT 1""",
                (task_id, contract_id),
            ).fetchone()
        return self._contract_from_row(row) if row else None

    def list_contract_versions(
        self,
        task_id: str,
        contract_id: str | None = None,
    ) -> list[WorkContract]:
        query = "SELECT * FROM contract_versions WHERE task_id = ?"
        parameters: tuple[Any, ...] = (task_id,)
        if contract_id is not None:
            query += " AND contract_id = ?"
            parameters += (contract_id,)
        query += " ORDER BY contract_id, version"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        return [self._contract_from_row(row) for row in rows]

    def list_contracts(self, task_id: str) -> list[WorkContract]:
        """Compatibility name for listing every persisted task contract."""

        return self.list_contract_versions(task_id)

    def resolve_agent_identity(
        self,
        task_id: str,
        role: str,
        assignment_id: str,
        proposed_id: str | None = None,
        *,
        logical_agent_id: str | None = None,
        supersedes_agent_id: str | None = None,
        supersession_reason: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        """Resolve a deterministic logical identity without remapping a key."""

        values = {
            "task_id": task_id,
            "role": role,
            "assignment_id": assignment_id,
        }
        for name, value in values.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if proposed_id and logical_agent_id and proposed_id != logical_agent_id:
            raise ValueError("proposed identity values disagree")
        candidate = str(proposed_id or logical_agent_id or "").strip()
        if not candidate:
            digest = hashlib.sha256(
                f"{task_id}\0{role}\0{assignment_id}".encode("utf-8")
            ).hexdigest()[:24]
            candidate = f"{role}_{digest}"
        supersedes = (
            str(supersedes_agent_id).strip() if supersedes_agent_id else None
        )
        if supersedes == candidate:
            raise ValueError("an agent identity cannot supersede itself")
        now = _utc_now().isoformat()
        with self.transaction() as connection:
            existing = connection.execute(
                """SELECT * FROM agent_identities
                WHERE task_id = ? AND role = ? AND assignment_id = ?""",
                (task_id, role, assignment_id),
            ).fetchone()
            if existing is not None:
                if str(existing["logical_agent_id"]) != candidate:
                    raise RuntimeError(
                        "agent identity conflict: assignment is already mapped"
                    )
                if (
                    supersedes is not None
                    and existing["supersedes_logical_agent_id"] != supersedes
                ):
                    raise RuntimeError(
                        "agent identity conflict: supersession differs"
                    )
                return candidate
            collision = connection.execute(
                """SELECT role, assignment_id FROM agent_identities
                WHERE task_id = ? AND logical_agent_id = ?""",
                (task_id, candidate),
            ).fetchone()
            if collision is not None:
                raise RuntimeError(
                    "agent identity conflict: logical identity is already "
                    "assigned elsewhere"
                )
            if supersedes is not None:
                previous = connection.execute(
                    """SELECT * FROM agent_identities
                    WHERE task_id = ? AND logical_agent_id = ?""",
                    (task_id, supersedes),
                ).fetchone()
                if previous is None:
                    raise RuntimeError(
                        "agent identity conflict: superseded identity is unknown"
                    )
                superseded_by = previous["superseded_by_logical_agent_id"]
                if superseded_by not in (None, candidate):
                    raise RuntimeError(
                        "agent identity conflict: identity was already superseded"
                    )
                connection.execute(
                    """UPDATE agent_identities SET
                        superseded_by_logical_agent_id = ?,
                        supersession_reason = ?,
                        superseded_at = ?,
                        updated_at = ?
                    WHERE task_id = ? AND logical_agent_id = ?""",
                    (
                        candidate,
                        supersession_reason,
                        now,
                        now,
                        task_id,
                        supersedes,
                    ),
                )
            connection.execute(
                """INSERT INTO agent_identities(
                    task_id, role, assignment_id, logical_agent_id,
                    supersedes_logical_agent_id, supersession_reason,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task_id,
                    role,
                    assignment_id,
                    candidate,
                    supersedes,
                    supersession_reason,
                    _json_dump(dict(metadata or {})),
                    now,
                    now,
                ),
            )
        return candidate

    def list_agent_identities(self, task_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """SELECT * FROM agent_identities WHERE task_id = ?
                ORDER BY role, assignment_id""",
                (task_id,),
            ).fetchall()
        identities: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _json_load(item.pop("metadata_json"))
            identities.append(item)
        return identities

    @staticmethod
    def _handoff_from_row(row: sqlite3.Row) -> HandoffEnvelope:
        return handoff_from_dict(
            {
                "handoff_id": row["handoff_id"],
                "task_id": row["task_id"],
                "contract_id": row["contract_id"],
                "contract_version": int(row["contract_version"]),
                "source_agent_id": row["source_agent_id"],
                "target_agent_id": row["target_agent_id"],
                "signal_type": row["signal_type"],
                "artifacts": _json_load(row["artifacts_json"]),
                "evidence": _json_load(row["evidence_json"]),
                "workstream_id": row["workstream_id"],
                "work_item_id": row["work_item_id"],
                "created_at": row["created_at"],
            }
        )

    def append_handoff(self, handoff: HandoffEnvelope) -> HandoffEnvelope:
        """Append a handoff tied to one exact persisted contract version."""

        if not isinstance(handoff, HandoffEnvelope):
            raise TypeError("handoff must be a HandoffEnvelope")
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM handoffs WHERE handoff_id = ?",
                (handoff.handoff_id,),
            ).fetchone()
            if existing is not None:
                restored = self._handoff_from_row(existing)
                if restored.sha256 == handoff.sha256:
                    return restored
                raise RuntimeError(
                    "handoff conflict: immutable handoff content differs"
                )
            contract = connection.execute(
                """SELECT 1 FROM contract_versions
                WHERE task_id = ? AND contract_id = ? AND version = ?""",
                (
                    handoff.task_id,
                    handoff.contract_id,
                    handoff.contract_version,
                ),
            ).fetchone()
            if contract is None:
                raise RuntimeError(
                    "handoff contract version has not been persisted"
                )
            connection.execute(
                """INSERT INTO handoffs(
                    handoff_id, task_id, contract_id, contract_version,
                    source_agent_id, target_agent_id, signal_type,
                    artifacts_json, evidence_json, workstream_id, work_item_id,
                    created_at, sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    handoff.handoff_id,
                    handoff.task_id,
                    handoff.contract_id,
                    handoff.contract_version,
                    handoff.source_agent_id,
                    handoff.target_agent_id,
                    handoff.signal_type,
                    _json_dump(handoff.artifacts),
                    _json_dump(handoff.evidence),
                    handoff.workstream_id,
                    handoff.work_item_id,
                    handoff.created_at.isoformat(),
                    handoff.sha256,
                ),
            )
        return handoff

    def list_handoffs(
        self,
        task_id: str,
        *,
        contract_id: str | None = None,
        contract_version: int | None = None,
    ) -> list[HandoffEnvelope]:
        clauses = ["task_id = ?"]
        parameters: list[Any] = [task_id]
        if contract_id is not None:
            clauses.append("contract_id = ?")
            parameters.append(contract_id)
        if contract_version is not None:
            clauses.append("contract_version = ?")
            parameters.append(contract_version)
        query = (
            "SELECT * FROM handoffs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at, handoff_id"
        )
        with self._lock:
            rows = self._connection.execute(query, tuple(parameters)).fetchall()
        handoffs = [self._handoff_from_row(row) for row in rows]
        for row, handoff in zip(rows, handoffs):
            if handoff.sha256 != str(row["sha256"]):
                raise RuntimeError("persisted handoff failed integrity check")
        return handoffs

    def save_plan(
        self, plan: TaskPlan, *, expected_previous_revision: int | None = None
    ) -> None:
        """Persist an immutable plan revision and its queryable DAG rows."""
        serialized_plan = _json_dump(plan)
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT plan_json FROM plans WHERE task_id = ? AND revision = ?",
                (plan.task_id, plan.revision),
            ).fetchone()
            if existing is not None:
                if str(existing["plan_json"]) == serialized_plan:
                    for workstream in plan.workstreams:
                        assert workstream.contract is not None
                        self._save_contract_version_in_transaction(
                            connection,
                            plan.task_id,
                            workstream.contract,
                        )
                        for item in workstream.work_items:
                            assert item.contract is not None
                            self._save_contract_version_in_transaction(
                                connection,
                                plan.task_id,
                                item.contract,
                            )
                    return
                raise RuntimeError(
                    "plan revision conflict: an immutable revision already "
                    "contains different content"
                )
            latest_row = connection.execute(
                "SELECT MAX(revision) AS revision FROM plans WHERE task_id = ?",
                (plan.task_id,),
            ).fetchone()
            latest = latest_row["revision"]
            if (
                expected_previous_revision is not None
                and int(latest or 0) != expected_previous_revision
            ):
                raise RuntimeError(
                    f"plan revision conflict: expected "
                    f"{expected_previous_revision}, found {int(latest or 0)}"
                )
            connection.execute(
                """
                INSERT INTO tasks(
                    task_id, session_id, goal, status, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    session_id=excluded.session_id,
                    goal=excluded.goal,
                    status=excluded.status,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    plan.task_id,
                    plan.session_id,
                    plan.goal,
                    plan.status.value,
                    _json_dump(plan.metadata),
                    plan.created_at.isoformat(),
                    plan.updated_at.isoformat(),
                ),
            )
            connection.execute(
                """
                INSERT INTO plans(task_id, revision, plan_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    plan.task_id,
                    plan.revision,
                    serialized_plan,
                    _utc_now().isoformat(),
                ),
            )
            for workstream in plan.workstreams:
                connection.execute(
                    """
                    INSERT INTO workstreams(task_id, revision, workstream_id, state_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (plan.task_id, plan.revision, workstream.id, _json_dump(workstream)),
                )
                for item in workstream.work_items:
                    connection.execute(
                        """
                        INSERT INTO work_items(
                            task_id, revision, workstream_id, work_item_id, state_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (plan.task_id, plan.revision, workstream.id, item.id, _json_dump(item)),
                    )
                assert workstream.contract is not None
                self._save_contract_version_in_transaction(
                    connection,
                    plan.task_id,
                    workstream.contract,
                )
                for item in workstream.work_items:
                    assert item.contract is not None
                    self._save_contract_version_in_transaction(
                        connection,
                        plan.task_id,
                        item.contract,
                    )

    def get_plan(self, task_id: str, revision: int | None = None) -> TaskPlan | None:
        query = "SELECT plan_json FROM plans WHERE task_id = ?"
        params: tuple[Any, ...]
        if revision is None:
            query += " ORDER BY revision DESC LIMIT 1"
            params = (task_id,)
        else:
            query += " AND revision = ?"
            params = (task_id, revision)
        with self._lock:
            row = self._connection.execute(query, params).fetchone()
        return task_plan_from_dict(_json_load(row["plan_json"])) if row else None

    def list_workstreams(
        self, task_id: str, revision: int | None = None
    ) -> list[Workstream]:
        plan = self.get_plan(task_id, revision)
        return list(plan.workstreams) if plan else []

    def list_work_items(
        self,
        task_id: str,
        *,
        workstream_id: str | None = None,
        revision: int | None = None,
    ) -> list[WorkItem]:
        plan = self.get_plan(task_id, revision)
        if plan is None:
            return []
        streams = (
            stream for stream in plan.workstreams
            if workstream_id is None or stream.id == workstream_id
        )
        return [item for stream in streams for item in stream.work_items]

    def save_attempt(self, attempt: Attempt) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO attempts(
                    attempt_id, task_id, workstream_id, work_item_id,
                    attempt_number, state_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(attempt_id) DO UPDATE SET
                    state_json=excluded.state_json,
                    updated_at=excluded.updated_at
                """,
                (
                    attempt.id,
                    attempt.task_id,
                    attempt.workstream_id,
                    attempt.work_item_id,
                    attempt.number,
                    _json_dump(attempt),
                    _utc_now().isoformat(),
                ),
            )

    def get_attempt(self, attempt_id: str) -> Attempt | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT state_json FROM attempts WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
        return attempt_from_dict(_json_load(row["state_json"])) if row else None

    def list_attempts(self, task_id: str, work_item_id: str | None = None) -> list[Attempt]:
        query = "SELECT state_json FROM attempts WHERE task_id = ?"
        params: tuple[Any, ...] = (task_id,)
        if work_item_id is not None:
            query += " AND work_item_id = ?"
            params += (work_item_id,)
        query += " ORDER BY work_item_id, attempt_number"
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return [attempt_from_dict(_json_load(row["state_json"])) for row in rows]

    @staticmethod
    def _validate_event(event: EventEnvelope) -> None:
        validate_event_payload(
            event.event_type,
            event.payload,
            task_id=event.task_id,
            session_id=event.session_id,
            workstream_id=event.workstream_id,
            work_item_id=event.work_item_id,
            agent_instance_id=event.agent_instance_id,
            call_id=event.call_id,
            sequence=event.sequence,
            timestamp=event.timestamp.isoformat(),
        )

    @staticmethod
    def _append_event_in_transaction(
        connection: sqlite3.Connection,
        event: EventEnvelope,
    ) -> tuple[EventEnvelope, bool]:
        duplicate = connection.execute(
            "SELECT envelope_json FROM events WHERE event_id = ?", (event.event_id,)
        ).fetchone()
        if duplicate:
            return (
                event_from_dict(_json_load(duplicate["envelope_json"])),
                False,
            )
        cursor = connection.execute(
            "SELECT latest_sequence, retained_from_sequence "
            "FROM event_cursors WHERE task_id = ?",
            (event.task_id,),
        ).fetchone()
        if cursor is None:
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS latest, "
                "COALESCE(MIN(sequence), 1) AS retained "
                "FROM events WHERE task_id = ?",
                (event.task_id,),
            ).fetchone()
            latest = int(row["latest"])
            retained_from = int(row["retained"])
        else:
            latest = int(cursor["latest_sequence"])
            retained_from = int(cursor["retained_from_sequence"])
        sequence = latest + 1
        if event.sequence not in (0, sequence):
            raise ValueError(
                f"event sequence must be 0 or the next sequence ({sequence})"
            )
        stored = replace(event, sequence=sequence)
        connection.execute(
            """
            INSERT INTO events(
                task_id, session_id, sequence, event_id, envelope_json,
                created_at, event_type, is_terminal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stored.task_id,
                stored.session_id,
                stored.sequence,
                stored.event_id,
                _json_dump(stored),
                stored.timestamp.isoformat(),
                stored.event_type,
                int(stored.event_type in TERMINAL_EVENT_TYPES),
            ),
        )
        connection.execute(
            """INSERT INTO event_cursors(
                task_id, session_id, latest_sequence,
                retained_from_sequence, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                session_id=excluded.session_id,
                latest_sequence=excluded.latest_sequence,
                updated_at=excluded.updated_at""",
            (
                stored.task_id,
                stored.session_id,
                stored.sequence,
                retained_from,
                stored.timestamp.isoformat(),
            ),
        )
        return stored, True

    def append_event(self, event: EventEnvelope) -> EventEnvelope:
        """Atomically assign the next task-local sequence and append an event."""

        self._validate_event(event)
        with self.transaction() as connection:
            stored, _ = self._append_event_in_transaction(connection, event)
        return stored

    def append_event_and_update_projection(
        self,
        event: EventEnvelope,
        update_projection: Callable[
            [dict[str, Any], EventEnvelope],
            Mapping[str, Any] | None,
        ],
    ) -> tuple[EventEnvelope, dict[str, Any]]:
        """Append an event and update its canonical task projection atomically.

        An idempotent event retry returns the already stored event and current
        projection without applying ``update_projection`` a second time.
        """

        self._validate_event(event)
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT snapshot_json FROM task_snapshots WHERE task_id = ?",
                (event.task_id,),
            ).fetchone()
            if row is None:
                raise KeyError(event.task_id)
            current = deepcopy(_json_load(row["snapshot_json"]))
            stored, inserted = self._append_event_in_transaction(connection, event)
            if not inserted:
                return stored, current
            updated = update_projection(deepcopy(current), stored)
            if updated is None:
                updated = current
            persisted = self._upsert_task_snapshot(
                connection,
                event.task_id,
                updated,
            )
        return stored, deepcopy(persisted)

    def replay_events(
        self, task_id: str, *, after_sequence: int = 0, limit: int = 1000
    ) -> list[EventEnvelope]:
        if after_sequence < 0 or limit < 1:
            raise ValueError("after_sequence must be non-negative and limit must be positive")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT envelope_json FROM events
                WHERE task_id = ? AND sequence > ?
                ORDER BY sequence ASC LIMIT ?
                """,
                (task_id, after_sequence, limit),
            ).fetchall()
        return [event_from_dict(_json_load(row["envelope_json"])) for row in rows]

    def latest_event_sequence(self, task_id: str) -> int:
        """Return the durable task-local event cursor."""
        with self._lock:
            row = self._connection.execute(
                "SELECT latest_sequence FROM event_cursors WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return int(row["latest_sequence"]) if row else 0

    def event_cursor(self, task_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM event_cursors WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        if row is None:
            return {
                "task_id": task_id,
                "session_id": None,
                "latest_sequence": 0,
                "retained_from_sequence": 1,
                "updated_at": None,
            }
        return dict(row)

    def record_observability(self, record: Mapping[str, Any]) -> str:
        value = dict(record)
        record_id = str(value.get("record_id") or uuid.uuid4().hex)
        kind = str(value.get("kind") or "")
        name = str(value.get("name") or "")
        if kind not in {"log", "metric", "span"} or not name.strip():
            raise ValueError("observability kind and name are required")
        timestamp = value.get("timestamp") or _utc_now().isoformat()
        if isinstance(timestamp, datetime):
            timestamp = _coerce_datetime(timestamp).isoformat()
        labels = dict(value.get("labels") or {})
        if kind == "metric":
            from .observability import normalize_labels

            labels = normalize_labels(labels)
        attributes = dict(value.get("attributes") or {})
        with self.transaction() as connection:
            connection.execute(
                """INSERT OR REPLACE INTO observability_records(
                    record_id, kind, name, task_id, session_id,
                    agent_instance_id, call_id, attempt_id, value,
                    duration_ms, labels_json, attributes_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record_id,
                    kind,
                    name,
                    value.get("task_id"),
                    value.get("session_id"),
                    value.get("agent_instance_id"),
                    value.get("call_id"),
                    value.get("attempt_id"),
                    value.get("value"),
                    value.get("duration_ms"),
                    _json_dump(labels),
                    _json_dump(attributes),
                    str(timestamp),
                ),
            )
        return record_id

    def append_audit_record(
        self,
        *,
        namespace: str,
        actor_id: str,
        action: str,
        resource: str,
        decision: str,
        reasons: list[str] | tuple[str, ...] = (),
        details: Mapping[str, Any] | None = None,
        audit_id: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        required = {
            "namespace": namespace,
            "actor_id": actor_id,
            "action": action,
            "resource": resource,
            "decision": decision,
        }
        if any(not str(value).strip() for value in required.values()):
            raise ValueError("all audit identity fields are required")
        identifier = audit_id or f"audit_{uuid.uuid4().hex}"
        created_at = _coerce_datetime(now).isoformat()
        reasons_value = tuple(str(reason) for reason in reasons)
        details_value = dict(details or {})
        with self.transaction() as connection:
            previous = connection.execute(
                """SELECT audit_sequence, record_hash FROM audit_records
                WHERE namespace = ?
                ORDER BY audit_sequence DESC LIMIT 1""",
                (namespace,),
            ).fetchone()
            previous_hash = str(previous["record_hash"]) if previous else None
            audit_sequence = int(previous["audit_sequence"]) + 1 if previous else 1
            hash_payload = {
                "audit_id": identifier,
                "namespace": namespace,
                "audit_sequence": audit_sequence,
                "actor_id": actor_id,
                "action": action,
                "resource": resource,
                "decision": decision,
                "reasons": reasons_value,
                "details": details_value,
                "previous_hash": previous_hash,
                "created_at": created_at,
            }
            record_hash = hashlib.sha256(
                _json_dump(hash_payload).encode("utf-8")
            ).hexdigest()
            connection.execute(
                """INSERT INTO audit_records(
                    audit_id, namespace, audit_sequence, actor_id, action,
                    resource, decision, reasons_json, details_json,
                    previous_hash, record_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    identifier,
                    namespace,
                    audit_sequence,
                    actor_id,
                    action,
                    resource,
                    decision,
                    _json_dump(reasons_value),
                    _json_dump(details_value),
                    previous_hash,
                    record_hash,
                    created_at,
                ),
            )
        return {
            **hash_payload,
            "record_hash": record_hash,
        }

    def list_audit_records(
        self,
        *,
        namespace: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = "SELECT * FROM audit_records"
        params: list[Any] = []
        if namespace is not None:
            query += " WHERE namespace = ?"
            params.append(namespace)
        query += " ORDER BY namespace, audit_sequence LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["reasons"] = _json_load(item.pop("reasons_json"))
            item["details"] = _json_load(item.pop("details_json"))
            records.append(item)
        return records

    def list_observability(
        self,
        task_id: str | None = None,
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = "SELECT * FROM observability_records"
        params: list[Any] = []
        if task_id is not None:
            query += " WHERE task_id = ?"
            params.append(task_id)
        query += " ORDER BY created_at, record_id LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["labels"] = _json_load(item.pop("labels_json"))
            item["attributes"] = _json_load(item.pop("attributes_json"))
            result.append(item)
        return result

    def record_log_metadata(
        self,
        task_id: str,
        path: str,
        *,
        session_id: str | None = None,
        agent_instance_id: str | None = None,
        call_id: str | None = None,
        attempt_id: str | None = None,
        content_sha256: str | None = None,
        size_bytes: int = 0,
        terminal_evidence: bool = False,
        metadata: Mapping[str, Any] | None = None,
        log_id: str | None = None,
        created_at: datetime | None = None,
    ) -> str:
        if not task_id.strip() or not path.strip():
            raise ValueError("task_id and path are required")
        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        _validate_sha256(content_sha256, "content_sha256")
        identifier = log_id or uuid.uuid4().hex
        timestamp = _coerce_datetime(created_at).isoformat()
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO log_records(
                    log_id, task_id, session_id, agent_instance_id, call_id,
                    attempt_id, path, content_sha256, size_bytes,
                    terminal_evidence, metadata_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    identifier,
                    task_id,
                    session_id,
                    agent_instance_id,
                    call_id,
                    attempt_id,
                    path,
                    content_sha256.lower() if content_sha256 else None,
                    size_bytes,
                    int(terminal_evidence),
                    _json_dump(dict(metadata or {})),
                    timestamp,
                ),
            )
        return identifier

    def list_log_records(
        self,
        task_id: str | None = None,
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = "SELECT * FROM log_records"
        params: list[Any] = []
        if task_id is not None:
            query += " WHERE task_id = ?"
            params.append(task_id)
        query += " ORDER BY created_at, log_id LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["terminal_evidence"] = bool(item["terminal_evidence"])
            item["metadata"] = _json_load(item.pop("metadata_json"))
            result.append(item)
        return result

    def record_artifact(
        self,
        task_id: str,
        path: str,
        content_sha256: str,
        *,
        session_id: str | None = None,
        size_bytes: int = 0,
        approved: bool = False,
        terminal_evidence: bool = False,
        status: str = "recorded",
        metadata: Mapping[str, Any] | None = None,
        record_id: str | None = None,
        now: datetime | None = None,
    ) -> str:
        if not task_id.strip() or not path.strip() or not status.strip():
            raise ValueError("task_id, path, and status are required")
        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        _validate_sha256(content_sha256, "content_sha256")
        identifier = record_id or uuid.uuid4().hex
        timestamp = _coerce_datetime(now).isoformat()
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO artifact_records(
                    record_id, task_id, session_id, path, content_sha256,
                    size_bytes, approved, terminal_evidence, status,
                    metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id, path) DO UPDATE SET
                    session_id=excluded.session_id,
                    content_sha256=excluded.content_sha256,
                    size_bytes=excluded.size_bytes,
                    approved=excluded.approved,
                    terminal_evidence=excluded.terminal_evidence,
                    status=excluded.status,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at,
                    retention_state='active',
                    retention_claim_id=NULL,
                    retention_claimed_at=NULL,
                    retention_error=NULL,
                    retention_finalized_at=NULL""",
                (
                    identifier,
                    task_id,
                    session_id,
                    path,
                    content_sha256.lower(),
                    size_bytes,
                    int(approved),
                    int(terminal_evidence),
                    status,
                    _json_dump(dict(metadata or {})),
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                "SELECT record_id FROM artifact_records "
                "WHERE task_id = ? AND path = ?",
                (task_id, path),
            ).fetchone()
        return str(row["record_id"])

    def list_artifacts(
        self,
        task_id: str | None = None,
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = "SELECT * FROM artifact_records"
        params: list[Any] = []
        if task_id is not None:
            query += " WHERE task_id = ?"
            params.append(task_id)
        query += " ORDER BY updated_at, record_id LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["approved"] = bool(item["approved"])
            item["terminal_evidence"] = bool(item["terminal_evidence"])
            item["metadata"] = _json_load(item.pop("metadata_json"))
            result.append(item)
        return result

    def claim_managed_retention(
        self,
        *,
        now: datetime,
        stale_before: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Atomically claim expired managed paths and abandoned deletions."""

        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        current = _coerce_datetime(now)
        stale = _coerce_datetime(stale_before)
        if stale > current:
            raise ValueError("stale_before cannot be later than now")
        policy = RetentionPolicy.from_environment()
        log_cutoff = (current - timedelta(days=policy.log_days)).isoformat()
        artifact_cutoff = (
            current - timedelta(days=policy.artifact_days)
        ).isoformat()
        stale_value = stale.isoformat()
        claimed_at = current.isoformat()
        claims: list[dict[str, Any]] = []
        with self.transaction() as connection:
            rows = connection.execute(
                """SELECT
                    'log' AS kind, l.log_id AS record_id, l.task_id, l.path,
                    l.size_bytes, l.terminal_evidence, l.metadata_json,
                    l.created_at AS retention_at, l.retention_state,
                    l.retention_claimed_at
                FROM log_records AS l
                WHERE l.terminal_evidence = 0
                  AND NOT EXISTS (
                    SELECT 1 FROM log_records AS newer
                    WHERE newer.path = l.path
                      AND (
                        newer.created_at > l.created_at
                        OR (
                            newer.created_at = l.created_at
                            AND newer.log_id > l.log_id
                        )
                      )
                  )
                  AND (
                    (
                      l.retention_state = 'active'
                      AND l.created_at < ?
                    )
                    OR (
                      l.retention_state = 'deleting'
                      AND l.retention_claimed_at <= ?
                    )
                  )
                UNION ALL
                SELECT
                    'artifact' AS kind, a.record_id, a.task_id, a.path,
                    a.size_bytes, a.terminal_evidence, a.metadata_json,
                    a.updated_at AS retention_at, a.retention_state,
                    a.retention_claimed_at
                FROM artifact_records AS a
                WHERE a.terminal_evidence = 0
                  AND (
                    (
                      a.retention_state = 'active'
                      AND a.updated_at < ?
                    )
                    OR (
                      a.retention_state = 'deleting'
                      AND a.retention_claimed_at <= ?
                    )
                  )
                ORDER BY retention_at, kind, record_id""",
                (log_cutoff, stale_value, artifact_cutoff, stale_value),
            )
            for row in rows:
                metadata = _json_load(row["metadata_json"])
                if (
                    not isinstance(metadata, dict)
                    or metadata.get("managed") is not True
                    or metadata.get("pinned") is True
                    or metadata.get("terminal_evidence") is True
                ):
                    continue
                if row["kind"] == "log":
                    path_records = connection.execute(
                        """SELECT terminal_evidence, metadata_json
                        FROM log_records WHERE path = ?""",
                        (row["path"],),
                    ).fetchall()
                    path_protected = False
                    for path_record in path_records:
                        path_metadata = _json_load(path_record["metadata_json"])
                        if (
                            bool(path_record["terminal_evidence"])
                            or (
                                isinstance(path_metadata, dict)
                                and (
                                    path_metadata.get("pinned") is True
                                    or path_metadata.get("terminal_evidence") is True
                                )
                            )
                        ):
                            path_protected = True
                            break
                    if path_protected:
                        continue
                claim_id = f"retention_{uuid.uuid4().hex}"
                table = (
                    "log_records"
                    if row["kind"] == "log"
                    else "artifact_records"
                )
                id_column = "log_id" if row["kind"] == "log" else "record_id"
                updated = connection.execute(
                    f"""UPDATE {table} SET
                        retention_state = 'deleting',
                        retention_claim_id = ?,
                        retention_claimed_at = ?,
                        retention_error = NULL,
                        retention_finalized_at = NULL
                    WHERE {id_column} = ?
                      AND (
                        retention_state = 'active'
                        OR (
                          retention_state = 'deleting'
                          AND retention_claimed_at <= ?
                        )
                      )""",
                    (
                        claim_id,
                        claimed_at,
                        row["record_id"],
                        stale_value,
                    ),
                ).rowcount
                if updated != 1:
                    continue
                claims.append(
                    {
                        "claim_id": claim_id,
                        "kind": str(row["kind"]),
                        "record_id": str(row["record_id"]),
                        "task_id": str(row["task_id"]),
                        "path": str(row["path"]),
                        "size_bytes": int(row["size_bytes"]),
                        "terminal_evidence": False,
                        "pinned": False,
                        "metadata": metadata,
                        "state": "deleting",
                        "claimed_at": claimed_at,
                    }
                )
                if len(claims) >= limit:
                    break
        return claims

    def finalize_managed_retention(
        self,
        claim: Mapping[str, Any],
        *,
        deleted: bool,
        size_bytes: int,
        error: str | None,
        finalized_at: datetime,
    ) -> None:
        """Finalize one exact claim without holding a lock during file I/O."""

        kind = str(claim.get("kind") or "")
        record_id = str(claim.get("record_id") or "")
        claim_id = str(claim.get("claim_id") or "")
        if kind not in {"log", "artifact"}:
            raise ValueError("claim kind must be log or artifact")
        if not record_id or not claim_id:
            raise ValueError("claim_id and record_id are required")
        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        table = "log_records" if kind == "log" else "artifact_records"
        id_column = "log_id" if kind == "log" else "record_id"
        timestamp = _coerce_datetime(finalized_at).isoformat()
        with self.transaction() as connection:
            row = connection.execute(
                f"SELECT * FROM {table} WHERE {id_column} = ?",
                (record_id,),
            ).fetchone()
            if row is None:
                if deleted:
                    return
                raise KeyError(record_id)
            if (
                str(row["retention_state"]) != "deleting"
                or str(row["retention_claim_id"] or "") != claim_id
            ):
                raise RuntimeError("managed retention claim is no longer current")
            if str(claim.get("path") or "") != str(row["path"]):
                raise RuntimeError("managed retention claim path changed")
            if deleted:
                if kind == "log":
                    connection.execute(
                        "DELETE FROM log_records WHERE path = ?",
                        (row["path"],),
                    )
                else:
                    connection.execute(
                        "DELETE FROM artifact_records WHERE record_id = ?",
                        (record_id,),
                    )
                return
            connection.execute(
                f"""UPDATE {table} SET
                    retention_state = 'active',
                    retention_claim_id = NULL,
                    retention_claimed_at = NULL,
                    retention_error = ?,
                    retention_finalized_at = ?
                WHERE {id_column} = ?""",
                (str(error or "managed deletion did not complete"), timestamp, record_id),
            )

    def save_completion_invariant(
        self,
        task_id: str,
        *,
        session_id: str | None,
        requested_status: str,
        effective_status: str,
        started_calls: int,
        terminal_calls: int,
        unresolved_call_ids: list[str] | tuple[str, ...],
        evidence: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        unresolved = sorted({str(value) for value in unresolved_call_ids})
        if started_calls < 0 or terminal_calls < 0:
            raise ValueError("call counts must be non-negative")
        balanced = not unresolved and terminal_calls >= started_calls
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO task_completion_invariants(
                    task_id, session_id, requested_status, effective_status,
                    balanced, started_calls, terminal_calls,
                    unresolved_call_ids_json, evidence_json, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    session_id=excluded.session_id,
                    requested_status=excluded.requested_status,
                    effective_status=excluded.effective_status,
                    balanced=excluded.balanced,
                    started_calls=excluded.started_calls,
                    terminal_calls=excluded.terminal_calls,
                    unresolved_call_ids_json=excluded.unresolved_call_ids_json,
                    evidence_json=excluded.evidence_json,
                    recorded_at=excluded.recorded_at""",
                (
                    task_id,
                    session_id,
                    requested_status,
                    effective_status,
                    int(balanced),
                    started_calls,
                    terminal_calls,
                    _json_dump(unresolved),
                    _json_dump(dict(evidence or {})),
                    _coerce_datetime(now).isoformat(),
                ),
            )

    def get_completion_invariant(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM task_completion_invariants WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["balanced"] = bool(result["balanced"])
        result["unresolved_call_ids"] = _json_load(
            result.pop("unresolved_call_ids_json")
        )
        result["evidence"] = _json_load(result.pop("evidence_json"))
        return result

    def compact_events(
        self,
        policy: RetentionPolicy | None = None,
        *,
        task_id: str | None = None,
        now: datetime | None = None,
    ) -> int:
        retention = policy or RetentionPolicy.from_environment()
        current = _coerce_datetime(now)
        cutoff = (current - timedelta(days=retention.event_days)).isoformat()
        deleted = 0
        with self.transaction() as connection:
            query = "DELETE FROM events WHERE is_terminal = 0 AND created_at < ?"
            params: list[Any] = [cutoff]
            if task_id is not None:
                query += " AND task_id = ?"
                params.append(task_id)
            deleted += connection.execute(query, tuple(params)).rowcount
            task_rows = connection.execute(
                "SELECT task_id FROM event_cursors"
                + (" WHERE task_id = ?" if task_id is not None else ""),
                (() if task_id is None else (task_id,)),
            ).fetchall()
            for task_row in task_rows:
                current_task_id = str(task_row["task_id"])
                sequences = connection.execute(
                    "SELECT sequence FROM events "
                    "WHERE task_id = ? AND is_terminal = 0 "
                    "ORDER BY sequence DESC",
                    (current_task_id,),
                ).fetchall()
                stale = [
                    int(row["sequence"])
                    for row in sequences[retention.max_events_per_task :]
                ]
                if stale:
                    placeholders = ",".join("?" for _ in stale)
                    deleted += connection.execute(
                        f"DELETE FROM events WHERE task_id = ? "
                        f"AND sequence IN ({placeholders})",
                        (current_task_id, *stale),
                    ).rowcount
                minimum = connection.execute(
                    "SELECT MIN(sequence) AS retained FROM events "
                    "WHERE task_id = ?",
                    (current_task_id,),
                ).fetchone()["retained"]
                cursor = connection.execute(
                    "SELECT latest_sequence FROM event_cursors "
                    "WHERE task_id = ?",
                    (current_task_id,),
                ).fetchone()
                retained_from = (
                    int(minimum)
                    if minimum is not None
                    else int(cursor["latest_sequence"]) + 1
                )
                connection.execute(
                    "UPDATE event_cursors SET retained_from_sequence = ?, "
                    "updated_at = ? WHERE task_id = ?",
                    (retained_from, current.isoformat(), current_task_id),
                )
        return deleted

    def compact_log_records(
        self,
        policy: RetentionPolicy | None = None,
        *,
        now: datetime | None = None,
    ) -> int:
        retention = policy or RetentionPolicy.from_environment()
        cutoff = (
            _coerce_datetime(now) - timedelta(days=retention.log_days)
        ).isoformat()
        with self.transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM log_records "
                "WHERE terminal_evidence = 0 "
                "AND retention_state != 'deleting' AND created_at < ?",
                (cutoff,),
            )
        return cursor.rowcount

    def compact_artifact_records(
        self,
        policy: RetentionPolicy | None = None,
        *,
        now: datetime | None = None,
    ) -> int:
        retention = policy or RetentionPolicy.from_environment()
        cutoff = (
            _coerce_datetime(now) - timedelta(days=retention.artifact_days)
        ).isoformat()
        with self.transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM artifact_records "
                "WHERE terminal_evidence = 0 "
                "AND retention_state != 'deleting' AND updated_at < ?",
                (cutoff,),
            )
        return cursor.rowcount

    def compact_retention(
        self,
        policy: RetentionPolicy | None = None,
        *,
        now: datetime | None = None,
    ) -> dict[str, int]:
        retention = policy or RetentionPolicy.from_environment()
        current = _coerce_datetime(now)
        observability_cutoff = (
            current - timedelta(days=retention.observability_days)
        ).isoformat()
        with self.transaction() as connection:
            observability_deleted = connection.execute(
                "DELETE FROM observability_records WHERE created_at < ?",
                (observability_cutoff,),
            ).rowcount
        return {
            "events": self.compact_events(retention, now=current),
            "logs": self.compact_log_records(retention, now=current),
            "artifacts": self.compact_artifact_records(retention, now=current),
            "observability": observability_deleted,
        }

    def delete_task(self, task_id: str) -> dict[str, int]:
        """Remove all durable hierarchy rows for a task.

        Deletes children before parents so foreign-key constraints stay happy.
        """
        with self.transaction() as connection:
            counts = {
                "handoffs": connection.execute(
                    "DELETE FROM handoffs WHERE task_id = ?", (task_id,)
                ).rowcount,
                "agent_identities": connection.execute(
                    "DELETE FROM agent_identities WHERE task_id = ?", (task_id,)
                ).rowcount,
                "contract_versions": connection.execute(
                    "DELETE FROM contract_versions WHERE task_id = ?", (task_id,)
                ).rowcount,
                "job_fencing_counters": connection.execute(
                    "DELETE FROM job_fencing_counters WHERE job_id IN "
                    "(SELECT job_id FROM durable_jobs WHERE task_id = ?)",
                    (task_id,),
                ).rowcount,
                "jobs": connection.execute(
                    "DELETE FROM durable_jobs WHERE task_id = ?", (task_id,)
                ).rowcount,
                "event_projections": connection.execute(
                    "DELETE FROM event_projections WHERE task_id = ?", (task_id,)
                ).rowcount,
                "task_snapshots": connection.execute(
                    "DELETE FROM task_snapshots WHERE task_id = ?", (task_id,)
                ).rowcount,
                "approvals": connection.execute(
                    "DELETE FROM approval_requests WHERE task_id = ?", (task_id,)
                ).rowcount,
                "completion_invariants": connection.execute(
                    "DELETE FROM task_completion_invariants WHERE task_id = ?",
                    (task_id,),
                ).rowcount,
                "artifacts": connection.execute(
                    "DELETE FROM artifact_records WHERE task_id = ?", (task_id,)
                ).rowcount,
                "logs": connection.execute(
                    "DELETE FROM log_records WHERE task_id = ?", (task_id,)
                ).rowcount,
                "observability": connection.execute(
                    "DELETE FROM observability_records WHERE task_id = ?",
                    (task_id,),
                ).rowcount,
                "effects": connection.execute(
                    "DELETE FROM effect_receipts WHERE task_id = ?", (task_id,)
                ).rowcount,
                "events": connection.execute(
                    "DELETE FROM events WHERE task_id = ?", (task_id,)
                ).rowcount,
                "attempts": connection.execute(
                    "DELETE FROM attempts WHERE task_id = ?", (task_id,)
                ).rowcount,
                "work_items": connection.execute(
                    "DELETE FROM work_items WHERE task_id = ?", (task_id,)
                ).rowcount,
                "workstreams": connection.execute(
                    "DELETE FROM workstreams WHERE task_id = ?", (task_id,)
                ).rowcount,
                "plans": connection.execute(
                    "DELETE FROM plans WHERE task_id = ?", (task_id,)
                ).rowcount,
                "tasks": connection.execute(
                    "DELETE FROM tasks WHERE task_id = ?", (task_id,)
                ).rowcount,
                "event_cursors": connection.execute(
                    "DELETE FROM event_cursors WHERE task_id = ?", (task_id,)
                ).rowcount,
            }
        return counts

    @staticmethod
    def _effect_from_row(row: sqlite3.Row) -> EffectReceipt:
        return EffectReceipt(
            effect_id=row["effect_id"],
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            kind=row["kind"],
            target=row["target"],
            state=EffectState(row["state"]),
            attempts=int(row["attempts"]),
            payload=_json_load(row["payload_json"]),
            result=_json_load(row["result_json"]),
            before_sha256=row["before_sha256"],
            expected_after_sha256=row["expected_after_sha256"],
            after_sha256=row["after_sha256"],
            fencing_token=(
                int(row["fencing_token"])
                if row["fencing_token"] is not None
                else None
            ),
            compensates_effect_id=row["compensates_effect_id"],
            compensated_by_effect_id=row["compensated_by_effect_id"],
            compensated_at=(
                _parse_datetime(row["compensated_at"])
                if row["compensated_at"]
                else None
            ),
            error=row["error"],
            created_at=_parse_datetime(row["created_at"]),
            updated_at=_parse_datetime(row["updated_at"]),
            started_at=_parse_datetime(row["started_at"]),
            completed_at=(
                _parse_datetime(row["completed_at"])
                if row["completed_at"]
                else None
            ),
        )

    def get_effect(self, effect_id: str) -> EffectReceipt | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
        return self._effect_from_row(row) if row else None

    def get_effect_by_idempotency(
        self,
        task_id: str,
        idempotency_key: str,
    ) -> EffectReceipt | None:
        """Return one durable receipt by its public idempotency identity."""
        with self._lock:
            row = self._connection.execute(
                """SELECT * FROM effect_receipts
                WHERE task_id = ? AND idempotency_key = ?""",
                (task_id, idempotency_key),
            ).fetchone()
        return self._effect_from_row(row) if row else None

    @staticmethod
    def _assert_effect_fencing(
        receipt: EffectReceipt,
        fencing_token: int | None,
    ) -> None:
        if receipt.fencing_token != fencing_token and (
            receipt.fencing_token is not None or fencing_token is not None
        ):
            raise RuntimeError("effect fencing token does not match")

    @staticmethod
    def _link_effect_compensation(
        connection: sqlite3.Connection,
        receipt: EffectReceipt,
        timestamp: str,
    ) -> None:
        if receipt.compensates_effect_id is None:
            return
        original = connection.execute(
            """SELECT compensated_by_effect_id FROM effect_receipts
            WHERE effect_id = ?""",
            (receipt.compensates_effect_id,),
        ).fetchone()
        if original is None:
            raise KeyError(
                f"unknown compensated effect: {receipt.compensates_effect_id}"
            )
        linked = original["compensated_by_effect_id"]
        if linked is not None and linked != receipt.effect_id:
            raise RuntimeError("effect was compensated by another receipt")
        connection.execute(
            """UPDATE effect_receipts SET
                compensated_by_effect_id = ?,
                compensated_at = ?,
                updated_at = ?
            WHERE effect_id = ?""",
            (
                receipt.effect_id,
                timestamp,
                timestamp,
                receipt.compensates_effect_id,
            ),
        )

    def begin_effect(
        self,
        task_id: str,
        idempotency_key: str,
        kind: str,
        target: str,
        *,
        payload: Mapping[str, Any] | None = None,
        before_sha256: str | None = None,
        expected_after_sha256: str | None = None,
        fencing_token: int | None = None,
        compensates_effect_id: str | None = None,
        effect_id: str | None = None,
        now: datetime | None = None,
    ) -> EffectReceipt:
        """Create or resume one idempotent side effect.

        A repeated begin while pending returns the existing receipt. A failed
        effect begins a new attempt on the same durable receipt. Applied and
        reconciled receipts are terminal and are returned unchanged.
        """
        required = {
            "task_id": task_id,
            "idempotency_key": idempotency_key,
            "kind": kind,
            "target": target,
        }
        for name, value in required.items():
            if not str(value).strip():
                raise ValueError(f"{name} is required")
        _validate_sha256(before_sha256, "before_sha256")
        _validate_sha256(expected_after_sha256, "expected_after_sha256")
        if fencing_token is not None and (
            not isinstance(fencing_token, int)
            or isinstance(fencing_token, bool)
            or fencing_token < 1
        ):
            raise ValueError("fencing_token must be a positive integer")
        if compensates_effect_id is not None and not compensates_effect_id.strip():
            raise ValueError("compensates_effect_id cannot be empty")
        normalized_before = before_sha256.lower() if before_sha256 else None
        normalized_expected_after = (
            expected_after_sha256.lower() if expected_after_sha256 else None
        )
        payload_value = dict(payload or {})
        current = _coerce_datetime(now)
        timestamp = current.isoformat()
        receipt_id = effect_id or uuid.uuid4().hex
        if not receipt_id.strip():
            raise ValueError("effect_id is required")

        with self.transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM effect_receipts
                WHERE task_id = ? AND idempotency_key = ?
                """,
                (task_id, idempotency_key),
            ).fetchone()
            if row is not None:
                existing = self._effect_from_row(row)
                if (
                    existing.kind != kind
                    or existing.target != target
                    or dict(existing.payload) != payload_value
                    or existing.before_sha256 != normalized_before
                    or existing.expected_after_sha256 != normalized_expected_after
                    or existing.compensates_effect_id != compensates_effect_id
                ):
                    raise ValueError(
                        "idempotency key already belongs to a different effect"
                    )
                if effect_id is not None and existing.effect_id != effect_id:
                    raise ValueError(
                        "idempotency key already belongs to another effect_id"
                    )
                if existing.state is EffectState.FAILED:
                    connection.execute(
                        """
                        UPDATE effect_receipts SET
                            state = 'pending',
                            attempts = attempts + 1,
                            result_json = '{}',
                            after_sha256 = NULL,
                            fencing_token = ?,
                            error = NULL,
                            updated_at = ?,
                            started_at = ?,
                            completed_at = NULL
                        WHERE effect_id = ?
                        """,
                        (
                            fencing_token,
                            timestamp,
                            timestamp,
                            existing.effect_id,
                        ),
                    )
                    row = connection.execute(
                        "SELECT * FROM effect_receipts WHERE effect_id = ?",
                        (existing.effect_id,),
                    ).fetchone()
                    return self._effect_from_row(row)
                if (
                    existing.state is EffectState.PENDING
                    and fencing_token is not None
                    and existing.fencing_token != fencing_token
                ):
                    connection.execute(
                        """UPDATE effect_receipts SET
                            fencing_token = ?, updated_at = ?
                        WHERE effect_id = ? AND state = 'pending'""",
                        (fencing_token, timestamp, existing.effect_id),
                    )
                    row = connection.execute(
                        "SELECT * FROM effect_receipts WHERE effect_id = ?",
                        (existing.effect_id,),
                    ).fetchone()
                    return self._effect_from_row(row)
                return existing
            if compensates_effect_id is not None:
                compensated = connection.execute(
                    "SELECT * FROM effect_receipts WHERE effect_id = ?",
                    (compensates_effect_id,),
                ).fetchone()
                if compensated is None:
                    raise KeyError(
                        f"unknown compensated effect: {compensates_effect_id}"
                    )
                if compensated["task_id"] != task_id:
                    raise ValueError(
                        "compensation and original effect must belong to one task"
                    )
                if compensated["compensated_by_effect_id"] is not None:
                    raise RuntimeError("effect already has a completed compensation")
                prior_compensation = connection.execute(
                    """SELECT effect_id FROM effect_receipts
                    WHERE compensates_effect_id = ?""",
                    (compensates_effect_id,),
                ).fetchone()
                if prior_compensation is not None:
                    raise RuntimeError("effect already has a durable compensation")
            try:
                connection.execute(
                    """
                    INSERT INTO effect_receipts(
                        effect_id, task_id, idempotency_key, kind, target,
                        before_sha256, expected_after_sha256, after_sha256,
                        fencing_token, compensates_effect_id,
                        payload_json, result_json,
                        state, attempts, created_at, updated_at, started_at,
                        completed_at, error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, '{}', 'pending', 1,
                              ?, ?, ?, NULL, NULL)
                    """,
                    (
                        receipt_id,
                        task_id,
                        idempotency_key,
                        kind,
                        target,
                        normalized_before,
                        normalized_expected_after,
                        fencing_token,
                        compensates_effect_id,
                        _json_dump(payload_value),
                        timestamp,
                        timestamp,
                        timestamp,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"effect_id is already in use: {receipt_id}") from exc
            row = connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (receipt_id,),
            ).fetchone()
            return self._effect_from_row(row)

    def complete_effect(
        self,
        effect_id: str,
        *,
        result: Mapping[str, Any] | None = None,
        after_sha256: str | None = None,
        fencing_token: int | None = None,
        now: datetime | None = None,
    ) -> EffectReceipt:
        _validate_sha256(after_sha256, "after_sha256")
        normalized_after = after_sha256.lower() if after_sha256 else None
        result_value = dict(result or {})
        timestamp = (now or _utc_now()).isoformat()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown effect: {effect_id}")
            existing = self._effect_from_row(row)
            self._assert_effect_fencing(existing, fencing_token)
            if (
                existing.expected_after_sha256 is not None
                and normalized_after != existing.expected_after_sha256
            ):
                raise RuntimeError(
                    "effect completion does not match expected target hash"
                )
            if existing.state is EffectState.RECONCILED:
                return existing
            if existing.state is EffectState.APPLIED:
                if (
                    (result is not None and dict(existing.result) != result_value)
                    or (
                        after_sha256 is not None
                        and existing.after_sha256 != normalized_after
                    )
                ):
                    raise RuntimeError(
                        "applied effect cannot be completed with different evidence"
                    )
                return existing
            if existing.state is not EffectState.PENDING:
                raise RuntimeError(
                    f"cannot complete effect in state {existing.state.value}"
                )
            self._link_effect_compensation(connection, existing, timestamp)
            connection.execute(
                """
                UPDATE effect_receipts SET
                    state = 'applied',
                    result_json = ?,
                    after_sha256 = ?,
                    error = NULL,
                    updated_at = ?,
                    completed_at = ?
                WHERE effect_id = ?
                """,
                (
                    _json_dump(result_value),
                    normalized_after,
                    timestamp,
                    timestamp,
                    effect_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
            return self._effect_from_row(row)

    def fail_effect(
        self,
        effect_id: str,
        error: str,
        *,
        result: Mapping[str, Any] | None = None,
        fencing_token: int | None = None,
        now: datetime | None = None,
    ) -> EffectReceipt:
        if not error.strip():
            raise ValueError("error is required")
        timestamp = (now or _utc_now()).isoformat()
        result_value = dict(result or {})
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown effect: {effect_id}")
            existing = self._effect_from_row(row)
            self._assert_effect_fencing(existing, fencing_token)
            if existing.state is EffectState.FAILED:
                return existing
            if existing.state is not EffectState.PENDING:
                raise RuntimeError(
                    f"cannot fail effect in state {existing.state.value}"
                )
            connection.execute(
                """
                UPDATE effect_receipts SET
                    state = 'failed',
                    result_json = ?,
                    error = ?,
                    updated_at = ?,
                    completed_at = ?
                WHERE effect_id = ?
                """,
                (
                    _json_dump(result_value),
                    error,
                    timestamp,
                    timestamp,
                    effect_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
            return self._effect_from_row(row)

    def list_effects(
        self,
        task_id: str | None = None,
        *,
        state: EffectState | str | None = None,
        kind: str | None = None,
        limit: int = 1000,
    ) -> list[EffectReceipt]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query = "SELECT * FROM effect_receipts WHERE 1"
        params: list[Any] = []
        if task_id is not None:
            query += " AND task_id = ?"
            params.append(task_id)
        if state is not None:
            normalized_state = EffectState(state).value
            query += " AND state = ?"
            params.append(normalized_state)
        if kind is not None:
            query += " AND kind = ?"
            params.append(kind)
        query += " ORDER BY created_at, effect_id LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._effect_from_row(row) for row in rows]

    def reconcile_effect(
        self,
        effect_id: str,
        *,
        result: Mapping[str, Any] | None = None,
        before_sha256: str | None = None,
        after_sha256: str | None = None,
        error: str | None = None,
        fencing_token: int | None = None,
        now: datetime | None = None,
    ) -> EffectReceipt:
        """Record externally verified truth for an uncertain durable effect."""
        _validate_sha256(before_sha256, "before_sha256")
        _validate_sha256(after_sha256, "after_sha256")
        timestamp = (now or _utc_now()).isoformat()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown effect: {effect_id}")
            existing = self._effect_from_row(row)
            self._assert_effect_fencing(existing, fencing_token)
            result_value = (
                dict(existing.result) if result is None else dict(result)
            )
            before_value = (
                existing.before_sha256
                if before_sha256 is None
                else before_sha256.lower()
            )
            after_value = (
                existing.after_sha256
                if after_sha256 is None
                else after_sha256.lower()
            )
            if (
                existing.expected_after_sha256 is not None
                and after_value != existing.expected_after_sha256
            ):
                raise RuntimeError(
                    "effect reconciliation does not match expected target hash"
                )
            self._link_effect_compensation(connection, existing, timestamp)
            connection.execute(
                """
                UPDATE effect_receipts SET
                    state = 'reconciled',
                    result_json = ?,
                    before_sha256 = ?,
                    after_sha256 = ?,
                    error = ?,
                    updated_at = ?,
                    completed_at = ?
                WHERE effect_id = ?
                """,
                (
                    _json_dump(result_value),
                    before_value,
                    after_value,
                    error,
                    timestamp,
                    timestamp,
                    effect_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM effect_receipts WHERE effect_id = ?",
                (effect_id,),
            ).fetchone()
            return self._effect_from_row(row)

    def reconcile_pending_effect_by_target_hash(
        self,
        effect_id: str,
        target_sha256: str | None,
        *,
        result: Mapping[str, Any] | None = None,
        fencing_token: int | None = None,
        now: datetime | None = None,
    ) -> EffectReceipt | None:
        """Reconcile only when an uncertain target equals its prepared hash."""
        _validate_sha256(target_sha256, "target_sha256")
        receipt = self.get_effect(effect_id)
        if receipt is None:
            raise KeyError(f"unknown effect: {effect_id}")
        self._assert_effect_fencing(receipt, fencing_token)
        normalized_target = target_sha256.lower() if target_sha256 else None
        if receipt.expected_after_sha256 is None:
            return None
        if receipt.expected_after_sha256 != normalized_target:
            return None
        if receipt.state is EffectState.PENDING:
            evidence = dict(result or {})
            evidence.setdefault("reconciled_by", "target_sha256")
            return self.reconcile_effect(
                effect_id,
                result=evidence,
                after_sha256=normalized_target,
                fencing_token=fencing_token,
                now=now,
            )
        if receipt.state in {EffectState.APPLIED, EffectState.RECONCILED}:
            return receipt if receipt.after_sha256 == normalized_target else None
        return None

    def acquire_lease(
        self,
        resource_type: str,
        resource_id: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        payload: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        current = _coerce_datetime(now)
        expires = current + timedelta(seconds=ttl_seconds)
        with self.transaction() as connection:
            row = connection.execute(
                """
                SELECT owner_id, expires_at FROM leases
                WHERE resource_type = ? AND resource_id = ?
                """,
                (resource_type, resource_id),
            ).fetchone()
            if row and row["owner_id"] != owner_id:
                if _parse_datetime(row["expires_at"]) > current:
                    return False
            connection.execute(
                """
                INSERT INTO leases(
                    resource_type, resource_id, owner_id, expires_at, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(resource_type, resource_id) DO UPDATE SET
                    owner_id=excluded.owner_id,
                    expires_at=excluded.expires_at,
                    payload_json=excluded.payload_json
                """,
                (
                    resource_type,
                    resource_id,
                    owner_id,
                    expires.isoformat(),
                    _json_dump(dict(payload or {})),
                ),
            )
        return True

    def renew_lease(
        self,
        resource_type: str,
        resource_id: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        now: datetime | None = None,
    ) -> bool:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        current = _coerce_datetime(now)
        expires = current + timedelta(seconds=ttl_seconds)
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE leases SET expires_at = ?
                WHERE resource_type = ? AND resource_id = ? AND owner_id = ?
                    AND expires_at > ?
                """,
                (
                    expires.isoformat(), resource_type, resource_id,
                    owner_id, current.isoformat(),
                ),
            )
        return cursor.rowcount == 1

    def release_lease(self, resource_type: str, resource_id: str, owner_id: str) -> bool:
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                DELETE FROM leases
                WHERE resource_type = ? AND resource_id = ? AND owner_id = ?
                """,
                (resource_type, resource_id, owner_id),
            )
        return cursor.rowcount == 1

    def purge_expired_leases(self, *, now: datetime | None = None) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM leases WHERE expires_at <= ?",
                ((now or _utc_now()).isoformat(),),
            )
        return cursor.rowcount

    @staticmethod
    def _project_lease_from_row(row: sqlite3.Row) -> ProjectLease:
        return ProjectLease(
            project_key=row["project_key"],
            owner_id=row["owner_id"],
            fencing_token=int(row["fencing_token"]),
            expires_at=_parse_datetime(row["expires_at"]),
            heartbeat_at=_parse_datetime(row["heartbeat_at"]),
            purpose=row["purpose"],
            metadata=_json_load(row["metadata_json"]),
        )

    def get_project_lease(self, project_key: str) -> ProjectLease | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM project_locks WHERE project_key = ?",
                (project_key,),
            ).fetchone()
        return self._project_lease_from_row(row) if row else None

    def acquire_project_lease(
        self,
        project_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        purpose: str = "",
        metadata: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> ProjectLease | None:
        if not project_key.strip() or not owner_id.strip():
            raise ValueError("project_key and owner_id are required")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        current = _coerce_datetime(now)
        expires = current + timedelta(seconds=ttl_seconds)
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM project_locks WHERE project_key = ?",
                (project_key,),
            ).fetchone()
            active = row is not None and _parse_datetime(row["expires_at"]) > current
            if active and row["owner_id"] != owner_id:
                return None
            if active and int(row["fencing_token"]) > 0:
                metadata_json = (
                    row["metadata_json"]
                    if metadata is None
                    else _json_dump(dict(metadata))
                )
                connection.execute(
                    """
                    UPDATE project_locks SET
                        expires_at = ?,
                        heartbeat_at = ?,
                        purpose = ?,
                        metadata_json = ?
                    WHERE project_key = ? AND owner_id = ? AND fencing_token = ?
                    """,
                    (
                        expires.isoformat(),
                        current.isoformat(),
                        purpose,
                        metadata_json,
                        project_key,
                        owner_id,
                        int(row["fencing_token"]),
                    ),
                )
                refreshed = connection.execute(
                    "SELECT * FROM project_locks WHERE project_key = ?",
                    (project_key,),
                ).fetchone()
                return self._project_lease_from_row(refreshed)

            counter = connection.execute(
                """
                SELECT last_token FROM project_fencing_counters
                WHERE project_key = ?
                """,
                (project_key,),
            ).fetchone()
            last_token = int(counter["last_token"]) if counter else 0
            if row is not None:
                last_token = max(last_token, int(row["fencing_token"]))
            fencing_token = last_token + 1
            connection.execute(
                """
                INSERT INTO project_fencing_counters(project_key, last_token)
                VALUES (?, ?)
                ON CONFLICT(project_key) DO UPDATE SET
                    last_token=excluded.last_token
                """,
                (project_key, fencing_token),
            )
            connection.execute(
                """
                INSERT INTO project_locks(
                    project_key, owner_id, expires_at, purpose,
                    fencing_token, heartbeat_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_key) DO UPDATE SET
                    owner_id=excluded.owner_id,
                    expires_at=excluded.expires_at,
                    purpose=excluded.purpose,
                    fencing_token=excluded.fencing_token,
                    heartbeat_at=excluded.heartbeat_at,
                    metadata_json=excluded.metadata_json
                """,
                (
                    project_key,
                    owner_id,
                    expires.isoformat(),
                    purpose,
                    fencing_token,
                    current.isoformat(),
                    _json_dump(dict(metadata or {})),
                ),
            )
            acquired = connection.execute(
                "SELECT * FROM project_locks WHERE project_key = ?",
                (project_key,),
            ).fetchone()
            return self._project_lease_from_row(acquired)

    def heartbeat_project_lease(
        self,
        project_key: str,
        owner_id: str,
        fencing_token: int,
        ttl_seconds: float,
        *,
        now: datetime | None = None,
    ) -> ProjectLease | None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        current = _coerce_datetime(now)
        expires = current + timedelta(seconds=ttl_seconds)
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM project_locks WHERE project_key = ?",
                (project_key,),
            ).fetchone()
            if (
                row is None
                or row["owner_id"] != owner_id
                or int(row["fencing_token"]) != fencing_token
                or _parse_datetime(row["expires_at"]) <= current
            ):
                return None
            connection.execute(
                """
                UPDATE project_locks SET expires_at = ?, heartbeat_at = ?
                WHERE project_key = ? AND owner_id = ? AND fencing_token = ?
                """,
                (
                    expires.isoformat(),
                    current.isoformat(),
                    project_key,
                    owner_id,
                    fencing_token,
                ),
            )
            renewed = connection.execute(
                "SELECT * FROM project_locks WHERE project_key = ?",
                (project_key,),
            ).fetchone()
            return self._project_lease_from_row(renewed)

    def validate_project_lease(
        self,
        project_key: str,
        owner_id: str,
        fencing_token: int,
        *,
        now: datetime | None = None,
    ) -> bool:
        current = _coerce_datetime(now)
        with self._lock:
            row = self._connection.execute(
                "SELECT owner_id, fencing_token, expires_at "
                "FROM project_locks WHERE project_key = ?",
                (project_key,),
            ).fetchone()
        return bool(
            row is not None
            and row["owner_id"] == owner_id
            and int(row["fencing_token"]) == fencing_token
            and _parse_datetime(row["expires_at"]) > current
        )

    def release_project_lease(
        self,
        project_key: str,
        owner_id: str,
        fencing_token: int,
    ) -> bool:
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                DELETE FROM project_locks
                WHERE project_key = ? AND owner_id = ? AND fencing_token = ?
                """,
                (project_key, owner_id, fencing_token),
            )
        return cursor.rowcount == 1

    def run_fenced_project_mutation(
        self,
        project_key: str,
        owner_id: str,
        fencing_token: int,
        operation: Callable[[], _T],
        *,
        now: datetime | None = None,
    ) -> _T:
        """Fence before and after an operation without holding SQLite open."""
        if not self.validate_project_lease(
            project_key,
            owner_id,
            fencing_token,
            now=now,
        ):
            raise ProjectLeaseLostError("project lease is no longer current")
        result = operation()
        if not self.validate_project_lease(
            project_key,
            owner_id,
            fencing_token,
            now=now,
        ):
            raise ProjectLeaseLostError("project lease was lost during mutation")
        return result

    def acquire_project_lock(
        self,
        project_key: str,
        owner_id: str,
        ttl_seconds: float,
        *,
        purpose: str = "",
        now: datetime | None = None,
    ) -> bool:
        """Backward-compatible boolean wrapper around fenced project leases."""
        return (
            self.acquire_project_lease(
                project_key,
                owner_id,
                ttl_seconds,
                purpose=purpose,
                now=now,
            )
            is not None
        )

    def release_project_lock(
        self,
        project_key: str,
        owner_id: str,
        fencing_token: int | None = None,
    ) -> bool:
        """Release a legacy lock, optionally enforcing its fencing token."""
        if fencing_token is not None:
            return self.release_project_lease(
                project_key, owner_id, fencing_token
            )
        with self.transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM project_locks WHERE project_key = ? AND owner_id = ?",
                (project_key, owner_id),
            )
        return cursor.rowcount == 1
