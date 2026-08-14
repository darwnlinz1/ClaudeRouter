"""SQLite-backed durable jobs with idempotency, leases, and fencing."""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Mapping, Protocol


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRY = "retry"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobLeaseLostError(RuntimeError):
    pass


class JobRepository(Protocol):
    def transaction(
        self, *, immediate: bool = True
    ) -> AbstractContextManager[sqlite3.Connection]: ...


JobEventSink = Callable[[dict[str, Any]], None]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime:
    current = value or _utc_now()
    return (
        current
        if current.tzinfo is not None
        else current.replace(tzinfo=timezone.utc)
    )


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return (
        parsed
        if parsed.tzinfo is not None
        else parsed.replace(tzinfo=timezone.utc)
    )


def _dump(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@dataclass(frozen=True, slots=True)
class DurableJob:
    job_id: str
    namespace: str
    kind: str
    idempotency_key: str
    status: JobStatus
    payload: Mapping[str, Any]
    task_id: str | None
    priority: int
    attempts: int
    max_attempts: int
    available_at: datetime
    lease_owner: str | None
    lease_expires_at: datetime | None
    fencing_token: int
    cancel_requested: bool
    result: Mapping[str, Any]
    error: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    @property
    def terminal(self) -> bool:
        return self.status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        }


class DurableJobQueue:
    """A multi-process-safe queue using SQLite write transactions."""

    def __init__(
        self,
        repository: JobRepository,
        *,
        event_sink: JobEventSink | None = None,
    ) -> None:
        self.repository = repository
        self.event_sink = event_sink
        self._accepting = True

    @staticmethod
    def _from_row(row: sqlite3.Row) -> DurableJob:
        return DurableJob(
            job_id=str(row["job_id"]),
            namespace=str(row["namespace"]),
            task_id=row["task_id"],
            kind=str(row["kind"]),
            idempotency_key=str(row["idempotency_key"]),
            status=JobStatus(row["status"]),
            payload=json.loads(row["payload_json"]),
            priority=int(row["priority"]),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            available_at=_parse(row["available_at"]) or _utc_now(),
            lease_owner=row["lease_owner"],
            lease_expires_at=_parse(row["lease_expires_at"]),
            fencing_token=int(row["fencing_token"]),
            cancel_requested=bool(row["cancel_requested"]),
            result=json.loads(row["result_json"]),
            error=row["error"],
            created_at=_parse(row["created_at"]) or _utc_now(),
            updated_at=_parse(row["updated_at"]) or _utc_now(),
            completed_at=_parse(row["completed_at"]),
        )

    def _emit(self, event_type: str, job: DurableJob) -> None:
        if self.event_sink is None:
            return
        self.event_sink(
            {
                "type": f"job.{event_type}",
                "job_id": job.job_id,
                "task_id": job.task_id,
                "namespace": job.namespace,
                "kind": job.kind,
                "status": job.status.value,
                "attempt": job.attempts,
                "fencing_token": job.fencing_token,
                "lease_owner": job.lease_owner,
            }
        )

    def enqueue(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        idempotency_key: str,
        task_id: str | None = None,
        namespace: str = "local",
        priority: int = 0,
        max_attempts: int = 3,
        available_at: datetime | None = None,
        job_id: str | None = None,
        now: datetime | None = None,
    ) -> DurableJob:
        if not self._accepting:
            raise RuntimeError("durable job queue is closing")
        required = {
            "kind": kind,
            "idempotency_key": idempotency_key,
            "namespace": namespace,
        }
        if any(not str(value).strip() for value in required.values()):
            raise ValueError("kind, idempotency_key, and namespace are required")
        if priority < 0 or max_attempts < 1:
            raise ValueError("priority must be non-negative and max_attempts positive")
        current = _aware(now)
        available = _aware(available_at or current)
        identifier = job_id or f"job_{uuid.uuid4().hex}"
        payload_json = _dump(payload)
        with self.repository.transaction() as connection:
            existing_row = connection.execute(
                "SELECT * FROM durable_jobs "
                "WHERE namespace = ? AND idempotency_key = ?",
                (namespace, idempotency_key),
            ).fetchone()
            if existing_row is not None:
                existing = self._from_row(existing_row)
                if (
                    existing.kind != kind
                    or existing.task_id != task_id
                    or _dump(existing.payload) != payload_json
                    or existing.max_attempts != max_attempts
                ):
                    raise ValueError(
                        "idempotency key already belongs to a different job"
                    )
                return existing
            connection.execute(
                """INSERT INTO durable_jobs(
                    job_id, namespace, task_id, kind, idempotency_key,
                    payload_json, status, priority, attempts, max_attempts,
                    available_at, lease_owner, lease_expires_at, fencing_token,
                    cancel_requested, result_json, error, created_at, updated_at,
                    completed_at)
                VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, 0, ?, ?, NULL, NULL, 0,
                        0, '{}', NULL, ?, ?, NULL)""",
                (
                    identifier,
                    namespace,
                    task_id,
                    kind,
                    idempotency_key,
                    payload_json,
                    priority,
                    max_attempts,
                    available.isoformat(),
                    current.isoformat(),
                    current.isoformat(),
                ),
            )
            row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (identifier,)
            ).fetchone()
        job = self._from_row(row)
        self._emit("queued", job)
        return job

    def get(self, job_id: str) -> DurableJob | None:
        with self.repository.transaction(immediate=False) as connection:
            row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return self._from_row(row) if row else None

    def list(
        self,
        *,
        namespace: str | None = None,
        task_id: str | None = None,
        status: JobStatus | str | None = None,
        limit: int = 1000,
    ) -> list[DurableJob]:
        if limit < 1:
            raise ValueError("limit must be positive")
        clauses = ["1"]
        params: list[Any] = []
        if namespace is not None:
            clauses.append("namespace = ?")
            params.append(namespace)
        if task_id is not None:
            clauses.append("task_id = ?")
            params.append(task_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(JobStatus(status).value)
        params.append(limit)
        with self.repository.transaction(immediate=False) as connection:
            rows = connection.execute(
                "SELECT * FROM durable_jobs WHERE "
                + " AND ".join(clauses)
                + " ORDER BY created_at, job_id LIMIT ?",
                tuple(params),
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def claim(
        self,
        worker_id: str,
        *,
        namespace: str = "local",
        lease_ttl_seconds: float = 60,
        kinds: tuple[str, ...] = (),
        now: datetime | None = None,
    ) -> DurableJob | None:
        if not worker_id.strip() or not namespace.strip():
            raise ValueError("worker_id and namespace are required")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        current = _aware(now)
        expires = current + timedelta(seconds=lease_ttl_seconds)
        kind_clause = ""
        params: list[Any] = [
            namespace,
            current.isoformat(),
            current.isoformat(),
        ]
        if kinds:
            kind_clause = " AND kind IN (" + ",".join("?" for _ in kinds) + ")"
            params.extend(kinds)
        with self.repository.transaction() as connection:
            row = connection.execute(
                """SELECT * FROM durable_jobs
                WHERE namespace = ?
                  AND cancel_requested = 0
                  AND attempts < max_attempts
                  AND available_at <= ?
                  AND (
                    status IN ('queued', 'retry')
                    OR (status = 'running' AND lease_expires_at <= ?)
                  )"""
                + kind_clause
                + " ORDER BY priority DESC, created_at, job_id LIMIT 1",
                tuple(params),
            ).fetchone()
            if row is None:
                return None
            job_id = str(row["job_id"])
            counter = connection.execute(
                "SELECT last_token FROM job_fencing_counters WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            token = max(
                int(counter["last_token"]) if counter else 0,
                int(row["fencing_token"]),
            ) + 1
            connection.execute(
                """INSERT INTO job_fencing_counters(job_id, last_token)
                VALUES (?, ?)
                ON CONFLICT(job_id) DO UPDATE SET last_token=excluded.last_token""",
                (job_id, token),
            )
            changed = connection.execute(
                """UPDATE durable_jobs
                SET status = 'running', attempts = attempts + 1,
                    lease_owner = ?, lease_expires_at = ?, fencing_token = ?,
                    error = NULL, updated_at = ?
                WHERE job_id = ? AND fencing_token = ?""",
                (
                    worker_id,
                    expires.isoformat(),
                    token,
                    current.isoformat(),
                    job_id,
                    int(row["fencing_token"]),
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("durable job claim lost an atomic update")
            claimed_row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        job = self._from_row(claimed_row)
        self._emit("claimed", job)
        return job

    @staticmethod
    def _require_current(
        connection: sqlite3.Connection,
        job_id: str,
        worker_id: str,
        fencing_token: int,
        current: datetime,
        *,
        allow_cancel_requested: bool = False,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if (
            row is None
            or row["status"] != JobStatus.RUNNING.value
            or row["lease_owner"] != worker_id
            or int(row["fencing_token"]) != fencing_token
            or (_parse(row["lease_expires_at"]) or current) <= current
            or (bool(row["cancel_requested"]) and not allow_cancel_requested)
        ):
            raise JobLeaseLostError("job lease is no longer current")
        return row

    def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        fencing_token: int,
        *,
        lease_ttl_seconds: float = 60,
        now: datetime | None = None,
    ) -> DurableJob:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        current = _aware(now)
        expires = current + timedelta(seconds=lease_ttl_seconds)
        with self.repository.transaction() as connection:
            self._require_current(
                connection, job_id, worker_id, fencing_token, current
            )
            connection.execute(
                """UPDATE durable_jobs
                SET lease_expires_at = ?, updated_at = ?
                WHERE job_id = ? AND lease_owner = ? AND fencing_token = ?""",
                (
                    expires.isoformat(),
                    current.isoformat(),
                    job_id,
                    worker_id,
                    fencing_token,
                ),
            )
            row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        job = self._from_row(row)
        self._emit("heartbeat", job)
        return job

    def complete(
        self,
        job_id: str,
        worker_id: str,
        fencing_token: int,
        *,
        result: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> DurableJob:
        current = _aware(now)
        with self.repository.transaction() as connection:
            self._require_current(
                connection, job_id, worker_id, fencing_token, current
            )
            connection.execute(
                """UPDATE durable_jobs
                SET status = 'completed', result_json = ?, lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?, completed_at = ?
                WHERE job_id = ? AND lease_owner = ? AND fencing_token = ?""",
                (
                    _dump(result or {}),
                    current.isoformat(),
                    current.isoformat(),
                    job_id,
                    worker_id,
                    fencing_token,
                ),
            )
            row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        job = self._from_row(row)
        self._emit("completed", job)
        return job

    def fail(
        self,
        job_id: str,
        worker_id: str,
        fencing_token: int,
        error: str,
        *,
        retryable: bool = True,
        retry_delay_seconds: float = 0,
        now: datetime | None = None,
    ) -> DurableJob:
        if not error.strip() or retry_delay_seconds < 0:
            raise ValueError("error is required and retry delay must be non-negative")
        current = _aware(now)
        with self.repository.transaction() as connection:
            existing = self._require_current(
                connection, job_id, worker_id, fencing_token, current
            )
            should_retry = retryable and int(existing["attempts"]) < int(
                existing["max_attempts"]
            )
            status = JobStatus.RETRY if should_retry else JobStatus.FAILED
            available = current + timedelta(seconds=retry_delay_seconds)
            completed_at = None if should_retry else current.isoformat()
            connection.execute(
                """UPDATE durable_jobs
                SET status = ?, error = ?, available_at = ?,
                    lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = ?, completed_at = ?
                WHERE job_id = ? AND lease_owner = ? AND fencing_token = ?""",
                (
                    status.value,
                    error,
                    available.isoformat(),
                    current.isoformat(),
                    completed_at,
                    job_id,
                    worker_id,
                    fencing_token,
                ),
            )
            row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        job = self._from_row(row)
        self._emit("retry_scheduled" if should_retry else "failed", job)
        return job

    def cancel(
        self,
        job_id: str,
        *,
        now: datetime | None = None,
    ) -> DurableJob:
        current = _aware(now)
        with self.repository.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            existing = self._from_row(row)
            if existing.terminal:
                return existing
            if existing.status is JobStatus.RUNNING:
                connection.execute(
                    "UPDATE durable_jobs SET cancel_requested = 1, updated_at = ? "
                    "WHERE job_id = ?",
                    (current.isoformat(), job_id),
                )
                event_type = "cancel_requested"
            else:
                connection.execute(
                    """UPDATE durable_jobs
                    SET status = 'cancelled', cancel_requested = 1,
                        completed_at = ?, updated_at = ?
                    WHERE job_id = ?""",
                    (current.isoformat(), current.isoformat(), job_id),
                )
                event_type = "cancelled"
            row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        job = self._from_row(row)
        self._emit(event_type, job)
        return job

    def acknowledge_cancel(
        self,
        job_id: str,
        worker_id: str,
        fencing_token: int,
        *,
        now: datetime | None = None,
    ) -> DurableJob:
        current = _aware(now)
        with self.repository.transaction() as connection:
            row = self._require_current(
                connection,
                job_id,
                worker_id,
                fencing_token,
                current,
                allow_cancel_requested=True,
            )
            if not bool(row["cancel_requested"]):
                raise RuntimeError("job cancellation was not requested")
            connection.execute(
                """UPDATE durable_jobs
                SET status = 'cancelled', lease_owner = NULL,
                    lease_expires_at = NULL, completed_at = ?, updated_at = ?
                WHERE job_id = ? AND lease_owner = ? AND fencing_token = ?""",
                (
                    current.isoformat(),
                    current.isoformat(),
                    job_id,
                    worker_id,
                    fencing_token,
                ),
            )
            cancelled_row = connection.execute(
                "SELECT * FROM durable_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        job = self._from_row(cancelled_row)
        self._emit("cancelled", job)
        return job

    def recover_expired(self, *, now: datetime | None = None) -> int:
        """Return expired work to retry state, preserving attempt counters."""

        current = _aware(now)
        changed = 0
        with self.repository.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM durable_jobs "
                "WHERE status = 'running' AND lease_expires_at <= ?",
                (current.isoformat(),),
            ).fetchall()
            for row in rows:
                if bool(row["cancel_requested"]):
                    status = JobStatus.CANCELLED
                elif int(row["attempts"]) < int(row["max_attempts"]):
                    status = JobStatus.RETRY
                else:
                    status = JobStatus.FAILED
                terminal_at = (
                    current.isoformat()
                    if status in {JobStatus.CANCELLED, JobStatus.FAILED}
                    else None
                )
                changed += connection.execute(
                    """UPDATE durable_jobs
                    SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
                        error = CASE WHEN ? = 'failed'
                            THEN 'lease expired after final attempt' ELSE error END,
                        updated_at = ?, completed_at = ?
                    WHERE job_id = ? AND status = 'running'
                        AND fencing_token = ?""",
                    (
                        status.value,
                        status.value,
                        current.isoformat(),
                        terminal_at,
                        row["job_id"],
                        row["fencing_token"],
                    ),
                ).rowcount
        return changed

    def stop_accepting(self) -> None:
        self._accepting = False

    def drain(self, timeout: float = 0) -> bool:
        del timeout
        return True
