"""Deterministic task projections built from the durable event log."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Mapping, Protocol

from .models import EventEnvelope, to_dict


class EventProjectionError(RuntimeError):
    pass


class EventRepository(Protocol):
    def append_event(self, event: EventEnvelope) -> EventEnvelope: ...

    def replay_events(
        self, task_id: str, *, after_sequence: int = 0, limit: int = 1000
    ) -> list[EventEnvelope]: ...

    def save_event_projection(
        self,
        task_id: str,
        projection: Mapping[str, Any],
        *,
        sequence: int,
        checksum: str,
    ) -> None: ...

    def get_event_projection(self, task_id: str) -> dict[str, Any] | None: ...


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@dataclass(frozen=True, slots=True)
class TaskProjection:
    task_id: str
    session_id: str
    sequence: int = 0
    status: str = "pending"
    phase: str = "pending"
    cancellation_requested: bool = False
    event_count: int = 0
    workstreams: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    calls: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    jobs: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    last_error: str | None = None
    last_event_type: str | None = None
    last_event_at: str | None = None

    def __post_init__(self) -> None:
        if not self.task_id.strip() or not self.session_id.strip():
            raise ValueError("task_id and session_id are required")
        if self.sequence < 0 or self.event_count < 0:
            raise ValueError("projection counters must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(to_dict(self))

    @property
    def checksum(self) -> str:
        return hashlib.sha256(
            _canonical_json(self.to_dict()).encode("utf-8")
        ).hexdigest()


def _copy_records(
    records: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {str(key): deepcopy(dict(value)) for key, value in records.items()}


def apply_event(
    projection: TaskProjection,
    event: EventEnvelope,
) -> TaskProjection:
    """Apply one event without reading clocks, environment, or external state."""

    if event.task_id != projection.task_id:
        raise EventProjectionError("event belongs to a different task")
    if event.session_id != projection.session_id:
        raise EventProjectionError("event belongs to a different session")
    if event.sequence != projection.sequence + 1:
        raise EventProjectionError(
            f"expected event sequence {projection.sequence + 1}, "
            f"received {event.sequence}"
        )

    event_type = event.event_type
    payload = deepcopy(event.payload)
    status = projection.status
    phase = projection.phase
    cancellation_requested = projection.cancellation_requested
    last_error = projection.last_error
    workstreams = _copy_records(projection.workstreams)
    calls = _copy_records(projection.calls)
    jobs = _copy_records(projection.jobs)

    if event_type in {"task.started", "resume"}:
        status, phase, cancellation_requested = "running", "running", False
    elif event_type in {"plan.created", "plan_created"}:
        status, phase = "running", "planning"
    elif event_type == "workstream_started":
        stream_id = str(event.workstream_id or payload.get("workstream_id") or "")
        if stream_id:
            workstreams[stream_id] = {
                **workstreams.get(stream_id, {}),
                "status": "running",
                "sequence": event.sequence,
            }
        status, phase = "running", "managing"
    elif event_type in {"workstream_completed", "workstream_failed"}:
        stream_id = str(event.workstream_id or payload.get("workstream_id") or "")
        stream_status = (
            "completed" if event_type.endswith("completed") else "failed"
        )
        if stream_id:
            workstreams[stream_id] = {
                **workstreams.get(stream_id, {}),
                "status": stream_status,
                "summary": payload.get("summary"),
                "sequence": event.sequence,
            }
        if stream_status == "failed":
            last_error = str(payload.get("summary") or "workstream failed")
    elif event_type.startswith("model_request_"):
        call_id = str(
            event.call_id
            or payload.get("logical_request_id")
            or payload.get("call_id")
            or ""
        )
        if call_id:
            call = calls.get(call_id, {"call_id": call_id})
            terminal = event_type.removeprefix("model_request_")
            call.update(
                {
                    "state": terminal,
                    "attempt_id": payload.get("attempt_id"),
                    "agent_instance_id": event.agent_instance_id,
                    "sequence": event.sequence,
                }
            )
            calls[call_id] = call
        phase = "model"
        if event_type == "model_request_failed":
            last_error = str(payload.get("error") or "model request failed")
    elif event_type.startswith("job."):
        job_id = str(payload.get("job_id") or "")
        if job_id:
            job = jobs.get(job_id, {"job_id": job_id})
            job.update(
                {
                    "state": event_type.removeprefix("job."),
                    "attempt": payload.get("attempt"),
                    "fencing_token": payload.get("fencing_token"),
                    "sequence": event.sequence,
                }
            )
            jobs[job_id] = job
        phase = "execution"
    elif event_type in {"task.cancel_requested", "job.cancel_requested"}:
        cancellation_requested = True
        status, phase = "cancelling", "cancelling"
    elif event_type in {"hierarchy_completed", "task.completed"}:
        status, phase = "completed", "finished"
    elif event_type in {
        "hierarchy_failed",
        "task.failed",
        "project_lease_lost",
    }:
        status, phase = "failed", "finished"
        last_error = str(
            payload.get("error")
            or payload.get("summary")
            or event_type
        )
    elif event_type in {"task.cancelled", "job.cancelled"}:
        status, phase, cancellation_requested = "cancelled", "finished", True

    return replace(
        projection,
        sequence=event.sequence,
        status=status,
        phase=phase,
        cancellation_requested=cancellation_requested,
        event_count=projection.event_count + 1,
        workstreams=workstreams,
        calls=calls,
        jobs=jobs,
        last_error=last_error,
        last_event_type=event_type,
        last_event_at=event.timestamp.isoformat(),
    )


def project_events(
    events: Iterable[EventEnvelope],
    *,
    task_id: str | None = None,
    session_id: str | None = None,
) -> TaskProjection:
    ordered = sorted(events, key=lambda item: (item.sequence, item.event_id))
    if not ordered:
        if not task_id or not session_id:
            raise ValueError("empty replay requires task_id and session_id")
        return TaskProjection(task_id=task_id, session_id=session_id)
    first = ordered[0]
    if first.sequence != 1:
        raise EventProjectionError(
            "deterministic replay requires the event stream from sequence 1"
        )
    projection = TaskProjection(
        task_id=task_id or first.task_id,
        session_id=session_id or first.session_id,
    )
    for event in ordered:
        projection = apply_event(projection, event)
    return projection


class TaskEventCore:
    """Append transitions and persist a reproducible projection checkpoint."""

    def __init__(self, repository: EventRepository) -> None:
        self.repository = repository

    def append(self, event: EventEnvelope) -> EventEnvelope:
        stored = self.repository.append_event(event)
        projection = self.rebuild(stored.task_id, session_id=stored.session_id)
        self.repository.save_event_projection(
            stored.task_id,
            projection.to_dict(),
            sequence=projection.sequence,
            checksum=projection.checksum,
        )
        return stored

    def rebuild(
        self,
        task_id: str,
        *,
        session_id: str | None = None,
        page_size: int = 1000,
    ) -> TaskProjection:
        if page_size < 1:
            raise ValueError("page_size must be positive")
        events: list[EventEnvelope] = []
        cursor = 0
        while True:
            page = self.repository.replay_events(
                task_id,
                after_sequence=cursor,
                limit=page_size,
            )
            if not page:
                break
            events.extend(page)
            cursor = page[-1].sequence
            if len(page) < page_size:
                break
        return project_events(events, task_id=task_id, session_id=session_id)

    def verify_checkpoint(self, task_id: str) -> bool:
        checkpoint = self.repository.get_event_projection(task_id)
        if checkpoint is None:
            return False
        projection = self.rebuild(
            task_id,
            session_id=str(checkpoint["projection"]["session_id"]),
        )
        return (
            int(checkpoint["sequence"]) == projection.sequence
            and str(checkpoint["checksum"]) == projection.checksum
            and checkpoint["projection"] == projection.to_dict()
        )
