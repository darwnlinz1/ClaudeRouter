# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import threading
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import EventEnvelope
    from .state_repository import StateRepository


TASKS_FILE = Path(
    os.environ.get(
        "ORCH_TASKS_FILE",
        str(Path.home() / ".ai_orchestrator" / "tasks.json"),
    )
).expanduser()
_LEGACY_TASKS_FILE = TASKS_FILE

_lock = threading.RLock()
_repository: StateRepository | None = None
_LEGACY_IMPORT_KEY = "legacy_tasks_json_v1"
_ACTIVE_STATES = frozenset(
    {
        "QUEUED",
        "RUNNING",
        "PLANNING",
        "MANAGING",
        "CODING",
        "REVIEWING",
        "REVISION",
        "WAITING_INPUT",
        "STOPPING",
        "RESUMING",
    }
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _execution_state(record: dict[str, Any]) -> dict[str, Any]:
    """Return a complete hierarchy execution counter record."""
    hierarchy = record.setdefault("hierarchy", {})
    execution = hierarchy.setdefault("execution", {})
    defaults: dict[str, Any] = {
        "request_attempts": 0,
        "completed_requests": 0,
        "replayed_requests": 0,
        "account_switches": 0,
        "called_agent_ids": [],
        "completed_agent_ids": [],
        "called_by_role": {},
    }
    for key, value in defaults.items():
        if key not in execution:
            execution[key] = value.copy() if isinstance(value, (list, dict)) else value
    return execution


def _track_call(
    record: dict[str, Any],
    event: dict[str, Any],
    event_type: str,
) -> None:
    call_id = event.get("call_id") or event.get("logical_request_id")
    if not call_id:
        return
    execution = _execution_state(record)
    calls = execution.setdefault("calls", {})
    call_key = str(call_id)
    current = dict(calls.get(call_key) or {})
    current.update(
        {
            "call_id": call_key,
            "logical_request_id": event.get("logical_request_id"),
            "agent_instance_id": event.get("agent_instance_id"),
            "attempt_id": event.get("attempt_id"),
            "role": event.get("role"),
        }
    )
    event_at = str(event.get("timestamp") or _now())
    if event_type == "model_request_started":
        current["was_started"] = True
        current["state"] = "started"
        current["started_at"] = current.get("started_at") or event_at
        current["terminal_at"] = None
    else:
        current["state"] = event_type.removeprefix("model_request_")
        current["terminal_at"] = event_at
        if event.get("error"):
            current["error"] = event.get("error")
    calls[call_key] = current


def _persist_completion_invariant(
    record: dict[str, Any],
    requested_status: str,
) -> str:
    hierarchy = record.setdefault("hierarchy", {})
    execution = _execution_state(record)
    calls = execution.get("calls") or {}
    terminal_states = {"completed", "failed", "aborted"}
    started = {
        call_id
        for call_id, call in calls.items()
        if isinstance(call, dict) and call.get("was_started")
    }
    terminal = {
        call_id
        for call_id, call in calls.items()
        if isinstance(call, dict) and call.get("state") in terminal_states
    }
    unresolved = sorted(started - terminal)
    reconciliation_calls = (
        (hierarchy.get("reconciliation") or {}).get("calls") or {}
    )
    summary_balanced = reconciliation_calls.get("balanced")
    balanced = not unresolved and summary_balanced is not False
    effective_status = requested_status
    if requested_status == "COMPLETED" and not balanced:
        effective_status = "FAILED"
    hierarchy["completion_invariant"] = {
        "requested_status": requested_status,
        "effective_status": effective_status,
        "balanced": balanced,
        "started_calls": len(started),
        "terminal_calls": len(terminal),
        "unresolved_call_ids": unresolved,
        "reconciliation_calls": deepcopy(reconciliation_calls),
        "recorded_at": _now(),
    }
    return effective_status


def _load() -> dict[str, dict[str, Any]]:
    """Read legacy JSON only when explicitly requested during migration."""

    try:
        text = TASKS_FILE.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise RuntimeError(f"cannot read legacy task store: {TASKS_FILE}") from exc
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"legacy task store is not valid JSON: {TASKS_FILE}") from exc
    if not isinstance(data, dict):
        raise RuntimeError("legacy task store root must be an object")
    for task_id, record in data.items():
        if not isinstance(record, dict):
            raise RuntimeError(f"legacy task {task_id!r} must be an object")
        if record.get("status") in _ACTIVE_STATES:
            record["status"] = "INTERRUPTED"
            record["phase"] = "finished"
            record["current_agent"] = None
            record["updated_at"] = _now()
    return data


# Compatibility-only in-memory/JSON storage for callers that deliberately run
# without the production repository. Importing this module never reads JSON.
_records: dict[str, dict[str, Any]] = {}


def _uses_repository() -> bool:
    return _repository is not None and TASKS_FILE == _LEGACY_TASKS_FILE


def _get_record(task_id: str) -> dict[str, Any] | None:
    if _uses_repository():
        assert _repository is not None
        return _repository.get_task_snapshot(task_id)
    record = _records.get(task_id)
    return deepcopy(record) if record else None


def _persist_record(task_id: str, record: dict[str, Any]) -> None:
    if _uses_repository():
        assert _repository is not None
        _repository.save_task_snapshot(task_id, record)
    else:
        _records[task_id] = record
        _save()


def _archive_legacy_file(path: Path) -> None:
    if not path.is_file():
        return
    destination = path.with_suffix(path.suffix + ".migrated")
    sequence = 1
    while destination.exists():
        destination = path.with_suffix(path.suffix + f".migrated.{sequence}")
        sequence += 1
    try:
        os.replace(path, destination)
    except OSError:
        # The durable ledger is authoritative. Archival is best-effort and a
        # stale source file can never trigger another import.
        return


def configure_repository(repository: StateRepository | None) -> None:
    """Configure SQLite as the sole production source and migrate JSON once."""

    global _repository
    with _lock:
        _repository = repository
        if repository is None or not _uses_repository():
            return
        legacy_records = _load()
        imported = repository.import_task_snapshots_once(
            _LEGACY_IMPORT_KEY,
            str(TASKS_FILE),
            legacy_records,
        )
        if imported or repository.data_migration(_LEGACY_IMPORT_KEY) is not None:
            _archive_legacy_file(TASKS_FILE)
        for snapshot in repository.list_task_snapshots_by_status(_ACTIVE_STATES):
            task_id = str(snapshot.get("id") or "")
            if not task_id:
                continue
            snapshot["status"] = "INTERRUPTED"
            snapshot["phase"] = "finished"
            snapshot["current_agent"] = None
            snapshot["updated_at"] = _now()
            repository.save_task_snapshot(task_id, snapshot)
        _records.clear()


def _save() -> None:
    """Persist the compatibility JSON store when no repository is configured."""

    if _uses_repository():
        raise RuntimeError("bulk task saves are disabled for the canonical repository")
    TASKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_path = TASKS_FILE.with_suffix(".tmp")
    temp_path.write_text(
        json.dumps(_records, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temp_path, TASKS_FILE)


def create_task(
    task_id: str,
    *,
    name: str,
    mode: str,
    prompt: str,
    root: str,
    files: list[str],
    settings: dict[str, Any],
) -> dict[str, Any]:
    now = _now()
    record = {
        "id": task_id,
        "name": name.strip() or prompt.strip()[:60] or f"Task {task_id[:8]}",
        "mode": mode,
        "project_mode": settings.get("project_mode", "edit"),
        "prompt": prompt,
        "root": root,
        "files": files,
        "settings": settings,
        "status": "QUEUED",
        "phase": "queued",
        "current_agent": None,
        "turn_count": 0,
        "completed_tickets": [],
        "changed_files": {},
        "hierarchy": {
            "enabled": bool(settings.get("hierarchy_enabled", False)),
            "workstreams": {},
            "agents": {},
        },
        "agent_overrides": {},
        "artifact": None,
        "last_worker_feedback": "",
        "last_execution_result": None,
        "last_reviewer_feedback": "",
        "last_review_verdict": None,
        "last_error": None,
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
        "events": [],
    }
    with _lock:
        _persist_record(task_id, record)
        return deepcopy(record)


def record_event(
    task_id: str,
    event: dict[str, Any],
    *,
    envelope: EventEnvelope | None = None,
    _record: dict[str, Any] | None = None,
) -> EventEnvelope | None:
    """Project an event, optionally appending it atomically with the update.

    Existing callers may continue passing only ``task_id`` and ``event``.
    New durable event producers should pass the unpersisted ``envelope`` so the
    event append and task projection commit in the same SQLite transaction.
    """

    with _lock:
        if envelope is not None:
            if _record is not None:
                raise ValueError("envelope and _record cannot be combined")
            if not _uses_repository():
                raise RuntimeError("atomic event projection requires a repository")
            if envelope.task_id != task_id:
                raise ValueError("event envelope task_id must match")
            assert _repository is not None

            def update_projection(
                current: dict[str, Any],
                stored: EventEnvelope,
            ) -> dict[str, Any]:
                projected_event = dict(event)
                projected_event.update(
                    {
                        "event_id": stored.event_id,
                        "sequence": stored.sequence,
                        "timestamp": stored.timestamp.isoformat(),
                    }
                )
                record_event(task_id, projected_event, _record=current)
                return current

            stored, _ = _repository.append_event_and_update_projection(
                envelope,
                update_projection,
            )
            return stored

        record = _record if _record is not None else _get_record(task_id)
        if not record:
            return None
        event_type = event.get("type")
        if event_type == "status":
            if record["status"] in {"QUEUED", "WAITING_INPUT"}:
                record["status"] = "RUNNING"
            record["phase"] = "chat" if record["mode"] == "chat" else record["phase"]
            role = event.get("role")
            if role:
                record["current_agent"] = role
            record["started_at"] = record["started_at"] or _now()
        elif event_type == "finish_chat_turn":
            record["status"] = "WAITING_INPUT"
            record["phase"] = "waiting_input"
            record["current_agent"] = None
        elif event_type == "turn_start":
            record["status"] = "PLANNING"
            record["phase"] = event.get("phase", "supervisor")
            record["current_agent"] = "supervisor"
            record["turn_count"] = event.get("turn", record["turn_count"])
            record["started_at"] = record["started_at"] or _now()
        elif event_type == "turn_phase":
            phase = event.get("phase")
            record["phase"] = phase
            record["current_agent"] = phase
            record["status"] = (
                "REVIEWING"
                if phase == "reviewer"
                else ("CODING" if phase == "worker" else "RUNNING")
            )
        elif event_type == "agent_progress":
            role = event.get("role")
            stage = event.get("stage") or role or "running"
            record["phase"] = stage
            record["current_agent"] = role
            record["status"] = {
                "director": "PLANNING",
                "manager": "MANAGING",
                "supervisor": "PLANNING",
                "worker": "CODING",
                "tester": "REVIEWING",
                "reviewer": "REVIEWING",
            }.get(role, "RUNNING")
        elif event_type == "protocol_retry":
            role = event.get("role")
            record["phase"] = "protocol_retry"
            record["current_agent"] = role
            record["status"] = {
                "director": "PLANNING",
                "manager": "MANAGING",
                "supervisor": "PLANNING",
                "worker": "CODING",
                "tester": "REVIEWING",
                "reviewer": "REVIEWING",
            }.get(role, "RUNNING")
        elif event_type == "plan_created":
            record["status"] = "MANAGING"
            record["phase"] = "director_plan"
            record["current_agent"] = "director"
            hierarchy = record.setdefault("hierarchy", {})
            hierarchy["requested_manager_count"] = event.get(
                "requested_manager_count", 1
            )
            hierarchy["workstreams"] = {
                item.get("id"): item
                for item in event.get("workstreams", [])
                if isinstance(item, dict) and item.get("id")
            }
        elif event_type == "manager_plan_created":
            record["status"] = "MANAGING"
            record["phase"] = "manager_plan"
            record["current_agent"] = "manager"
            stream_id = event.get("workstream_id")
            if stream_id:
                streams = record.setdefault("hierarchy", {}).setdefault(
                    "workstreams", {}
                )
                stream = dict(streams.get(stream_id) or {})
                stream["status"] = "ready"
                stream["requested_worker_count"] = event.get(
                    "requested_worker_count", 1
                )
                stream["work_items"] = event.get("work_items", [])
                streams[stream_id] = stream
        elif event_type == "hierarchy_fanout_planned":
            record["status"] = "MANAGING"
            record["phase"] = "fanout_ready"
            record["current_agent"] = "director"
            hierarchy = record.setdefault("hierarchy", {})
            hierarchy["fanout"] = {
                "manager_count": int(event.get("manager_count") or 0),
                "coder_count": int(event.get("coder_count") or 0),
                "tester_count": int(event.get("tester_count") or 0),
                "child_agent_count": int(event.get("child_agent_count") or 0),
                "max_manager_count": int(
                    event.get("max_manager_count") or 0
                ),
                "unused_manager_capacity": int(
                    event.get("unused_manager_capacity") or 0
                ),
                "max_coders_per_manager": int(
                    event.get("max_coders_per_manager") or 0
                ),
                "max_parallel_managers": int(
                    event.get("max_parallel_managers") or 0
                ),
                "max_parallel_workers_per_manager": int(
                    event.get("max_parallel_workers_per_manager") or 0
                ),
                "max_parallel_workers": int(
                    event.get("max_parallel_workers") or 0
                ),
                "dependency_aware": bool(event.get("dependency_aware", True)),
            }
            _execution_state(record)
        elif event_type == "fanout_selected":
            record["status"] = "MANAGING"
            record["phase"] = "fanout_selected"
            hierarchy = record.setdefault("hierarchy", {})
            selections = hierarchy.setdefault("fanout_selections", {})
            level = str(event.get("level") or "unknown")
            key = (
                str(event.get("workstream_id"))
                if event.get("workstream_id")
                else level
            )
            selections[key] = {
                "level": level,
                "maximum": int(event.get("maximum") or 0),
                "selected": int(event.get("selected") or 0),
                "unused_capacity": int(event.get("unused_capacity") or 0),
                "reason": event.get("reason"),
                "manager_id": event.get("manager_id"),
                "workstream_id": event.get("workstream_id"),
            }
        elif event_type in {
            "model_request_started",
            "model_request_completed",
            "model_request_failed",
            "model_request_aborted",
        }:
            role = str(event.get("role") or "agent")
            record["current_agent"] = role
            record["status"] = {
                "director": "PLANNING",
                "manager": "MANAGING",
                "supervisor": "PLANNING",
                "worker": "CODING",
                "tester": "REVIEWING",
                "reviewer": "REVIEWING",
            }.get(role, "RUNNING")
            record["phase"] = {
                "model_request_started": "model_request",
                "model_request_completed": "model_response",
                "model_request_failed": "model_failed",
                "model_request_aborted": "model_aborted",
            }[event_type]
            execution = _execution_state(record)
            agent_id = event.get("agent_instance_id")
            if event_type == "model_request_started":
                execution["request_attempts"] = int(
                    execution.get("request_attempts") or 0
                ) + 1
                if event.get("replayed"):
                    execution["replayed_requests"] = int(
                        execution.get("replayed_requests") or 0
                    ) + 1
                called_ids = list(execution.get("called_agent_ids") or [])
                if agent_id and agent_id not in called_ids:
                    called_ids.append(agent_id)
                    execution["called_agent_ids"] = called_ids
                    by_role = dict(execution.get("called_by_role") or {})
                    by_role[role] = int(by_role.get(role) or 0) + 1
                    execution["called_by_role"] = by_role
            elif event_type == "model_request_completed":
                execution["completed_requests"] = int(
                    execution.get("completed_requests") or 0
                ) + 1
                completed_ids = list(
                    execution.get("completed_agent_ids") or []
                )
                if agent_id and agent_id not in completed_ids:
                    completed_ids.append(agent_id)
                    execution["completed_agent_ids"] = completed_ids
            else:
                record["status"] = "REVISION"
                key = (
                    "aborted_requests"
                    if event_type == "model_request_aborted"
                    else "failed_requests"
                )
                execution[key] = int(execution.get(key) or 0) + 1
                execution["last_terminal_error"] = {
                    "call_id": event.get("call_id")
                    or event.get("logical_request_id"),
                    "type": event_type,
                    "error": event.get("error"),
                    "role": role,
                    "agent_instance_id": agent_id,
                }
            _track_call(record, event, event_type)
        elif event_type == "account_switch":
            execution = _execution_state(record)
            execution["account_switches"] = int(
                execution.get("account_switches") or 0
            ) + 1
            execution["last_account_switch"] = {
                "agent_instance_id": event.get("agent_instance_id"),
                "from_account": event.get("from_account"),
                "to_account": event.get("to_account"),
                "reason": event.get("reason"),
                "logical_request_id": event.get("logical_request_id"),
            }
        elif event_type in {
            "workstream_started",
            "workstream_completed",
            "workstream_failed",
            "manager_replan_created",
        }:
            record["status"] = (
                "REVISION"
                if event_type in {"workstream_failed", "manager_replan_created"}
                else "MANAGING"
            )
            record["phase"] = event_type
            record["current_agent"] = "manager"
            stream_id = event.get("workstream_id")
            if stream_id:
                streams = record.setdefault("hierarchy", {}).setdefault(
                    "workstreams", {}
                )
                stream = dict(streams.get(stream_id) or {})
                stream["status"] = event.get("status", event_type)
                stream["summary"] = event.get("summary")
                streams[stream_id] = stream
        elif event_type == "director_replan_created":
            record["status"] = "REVISION"
            record["phase"] = "director_replan"
            record["current_agent"] = "director"
        elif event_type in {"hierarchy_completed", "hierarchy_failed"}:
            record["status"] = (
                "COMPLETED" if event_type == "hierarchy_completed" else "FAILED"
            )
            record["phase"] = "director_review"
            record["current_agent"] = "director"
            record.setdefault("hierarchy", {})["verdict"] = event.get("verdict")
            record["last_reviewer_feedback"] = event.get("summary", "")
        elif event_type == "completion_reconciliation":
            balanced = bool(event.get("balanced"))
            record["phase"] = "completion_reconciliation"
            record.setdefault("hierarchy", {})["reconciliation"] = {
                key: event.get(key)
                for key in (
                    "balanced",
                    "errors",
                    "workstreams",
                    "work_items",
                    "agents",
                    "calls",
                )
            }
            if not balanced:
                record["status"] = "FAILED"
                record["last_error"] = "; ".join(event.get("errors") or [])
        elif event_type == "project_lease_acquired":
            record.setdefault("hierarchy", {})["project_lease"] = {
                "status": "active",
                "project_key": event.get("project_key"),
                "fencing_token": event.get("fencing_token"),
                "isolation_level": event.get("isolation_level"),
            }
        elif event_type == "project_lease_lost":
            record["status"] = "FAILED"
            record["phase"] = "project_lease_lost"
            record["last_error"] = event.get("error", "Project lease lost")
            record.setdefault("hierarchy", {}).setdefault(
                "project_lease", {}
            )["status"] = "lost"
        elif event_type == "effect_applied":
            effects = record.setdefault("hierarchy", {}).setdefault(
                "effects", {}
            )
            effects[str(event.get("effect_id"))] = {
                "kind": event.get("effect_kind"),
                "target": event.get("file_path"),
                "before_sha256": event.get("before_sha256"),
                "after_sha256": event.get("after_sha256"),
                "status": "applied",
            }
        elif event_type == "test_result":
            record.setdefault("hierarchy", {})["sandbox"] = {
                "requested_isolation": event.get("requested_isolation"),
                "actual_isolation": event.get("actual_isolation"),
                "isolation_details": event.get("isolation_details"),
                "output_truncated": event.get("output_truncated"),
            }
        elif event_type == "execution_result":
            record["status"] = (
                "CODING"
                if event.get("accepted")
                else "REVISION"
            )
            record["phase"] = "execution"
            record["last_worker_feedback"] = event.get("worker_feedback", "")
            record["last_execution_result"] = event.get("execution_result")
            file_path = event.get("file_path")
            if file_path and event.get("accepted"):
                record["changed_files"][file_path] = {
                    "additions": int(event.get("additions", 0)),
                    "deletions": int(event.get("deletions", 0)),
                }
        elif event_type == "agent_failed":
            role = event.get("role")
            record["status"] = "REVISION"
            record["phase"] = event.get("status", "agent_failed")
            record["current_agent"] = role
            record["last_error"] = event.get("error", "Agent failed")
        elif event_type == "review_result":
            verdict = event.get("verdict")
            record["status"] = "REVIEWING" if verdict == "approved" else "REVISION"
            record["phase"] = "reviewer"
            record["current_agent"] = "reviewer"
            record["last_reviewer_feedback"] = event.get("reviewer_feedback", "")
            record["last_review_verdict"] = verdict
        elif event_type == "error":
            record["status"] = "FAILED"
            record["last_error"] = event.get("data", "Unknown error")

        record["updated_at"] = _now()
        if not _uses_repository():
            record.setdefault("events", []).append(
                {"at": record["updated_at"], **event}
            )
            record["events"] = record["events"][-100:]
        if _record is None:
            _persist_record(task_id, record)
        return None


def finish_task(
    task_id: str,
    *,
    status: str,
    reason: str | None = None,
    final_state: dict[str, Any] | None = None,
) -> None:
    with _lock:
        record = _get_record(task_id)
        if not record:
            return
        effective_status = _persist_completion_invariant(record, status)
        record["status"] = effective_status
        record["phase"] = "finished"
        record["current_agent"] = None
        record["finished_at"] = _now()
        record["updated_at"] = record["finished_at"]
        if reason:
            record["stopped_reason"] = reason
        if status == "COMPLETED" and effective_status != status:
            unresolved = record["hierarchy"]["completion_invariant"][
                "unresolved_call_ids"
            ]
            record["last_error"] = (
                "Task completion rejected because model calls were not "
                f"terminal: {unresolved}"
            )
        if final_state:
            for key in (
                "last_worker_feedback",
                "last_execution_result",
                "last_reviewer_feedback",
                "last_review_verdict",
                "turn_count",
                "completed_tickets",
            ):
                if key in final_state:
                    record[key] = final_state[key]
        _persist_record(task_id, record)


def set_status(task_id: str, status: str, phase: str | None = None) -> None:
    with _lock:
        record = _get_record(task_id)
        if not record:
            return
        record["status"] = status
        if phase is not None:
            record["phase"] = phase
        record["updated_at"] = _now()
        _persist_record(task_id, record)


def resume_task(task_id: str) -> dict[str, Any]:
    with _lock:
        record = _get_record(task_id)
        if not record:
            raise KeyError(task_id)
        record["status"] = "RESUMING"
        record["phase"] = "resuming"
        record["current_agent"] = "supervisor"
        record["finished_at"] = None
        record["last_error"] = None
        record["updated_at"] = _now()
        if not _uses_repository():
            record.setdefault("events", []).append(
                {
                    "at": record["updated_at"],
                    "type": "resume",
                    "turn_count": record.get("turn_count", 0),
                }
            )
            record["events"] = record["events"][-100:]
        _persist_record(task_id, record)
        return deepcopy(record)


def set_artifact(task_id: str, manifest: dict[str, Any] | None) -> None:
    with _lock:
        record = _get_record(task_id)
        if not record:
            return
        record["artifact"] = deepcopy(manifest)
        record["updated_at"] = _now()
        _persist_record(task_id, record)


def set_agent_override(
    task_id: str,
    agent_id: str,
    *,
    model: str,
    effort: str,
) -> dict[str, str]:
    with _lock:
        record = _get_record(task_id)
        if not record:
            raise KeyError(task_id)
        value = {"model": model, "effort": effort, "updated_at": _now()}
        record.setdefault("agent_overrides", {})[agent_id] = value
        record["updated_at"] = value["updated_at"]
        _persist_record(task_id, record)
        return deepcopy(value)


def get_agent_override(task_id: str, agent_id: str) -> dict[str, str] | None:
    with _lock:
        record = _get_record(task_id)
        if not record:
            return None
        value = (record.get("agent_overrides") or {}).get(agent_id)
        return deepcopy(value) if value else None


def list_tasks() -> list[dict[str, Any]]:
    with _lock:
        if _uses_repository():
            assert _repository is not None
            return _repository.list_task_summaries()
        records = []
        for record in _records.values():
            summary = deepcopy(record)
            summary.pop("events", None)
            summary.pop("prompt", None)
            summary.pop("settings", None)
            records.append(summary)
    records.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
    return records


def list_auto_resumable_tasks() -> list[dict[str, Any]]:
    with _lock:
        if _uses_repository():
            assert _repository is not None
            return _repository.list_auto_resumable_task_snapshots()
        return [
            deepcopy(record)
            for record in _records.values()
            if record.get("status") == "INTERRUPTED"
            and (record.get("settings") or {}).get("auto_continue") is True
            and record.get("mode") == "orchestrator"
        ]


def get_task(task_id: str) -> dict[str, Any] | None:
    with _lock:
        return _get_record(task_id)


def delete_task(task_id: str) -> bool:
    with _lock:
        if _uses_repository():
            assert _repository is not None
            return _repository.delete_task_snapshot(task_id)
        removed = _records.pop(task_id, None)
        if removed is not None:
            _save()
        return removed is not None
