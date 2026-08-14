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
_TERMINAL_STATES = frozenset(
    {
        "COMPLETED",
        "PARTIAL",
        "FAILED",
        "STOPPED",
        "CANCELLED",
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
    reconciliation = hierarchy.get("reconciliation") or {}
    reconciliation_calls = reconciliation.get("calls") or {}
    summary_covered = reconciliation.get("covered")
    if summary_covered is None:
        summary_covered = reconciliation_calls.get("balanced")
    report_barrier = hierarchy.get("manager_report_barrier") or {}
    barrier_satisfied = report_barrier.get("satisfied")
    final_review = hierarchy.get("director_final_review") or {}
    final_review_count = int(final_review.get("count") or 0)
    hierarchy_terminal = requested_status in {"COMPLETED", "PARTIAL"} and bool(
        hierarchy.get("execution_epoch") or hierarchy.get("manager_report_barrier")
    )
    balanced = (
        not unresolved
        and summary_covered is not False
        and (not hierarchy_terminal or barrier_satisfied is True)
        and (not hierarchy_terminal or final_review_count == 1)
    )
    effective_status = requested_status
    if requested_status in {"COMPLETED", "PARTIAL"} and not balanced:
        effective_status = "FAILED"
    if requested_status == "COMPLETED" and reconciliation.get("successful") is False:
        effective_status = "FAILED"
    hierarchy["completion_invariant"] = {
        "requested_status": requested_status,
        "effective_status": effective_status,
        "balanced": balanced,
        "started_calls": len(started),
        "terminal_calls": len(terminal),
        "unresolved_call_ids": unresolved,
        "reconciliation_calls": deepcopy(reconciliation_calls),
        "manager_report_barrier": deepcopy(report_barrier),
        "director_final_review_count": final_review_count,
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
    """Configure SQLite as the sole production source and migrate JSON once.

    Runtime recovery is intentionally separate so importing the server from a
    second process cannot mutate active task snapshots.
    """

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
        _records.clear()


def interrupt_active_tasks() -> list[str]:
    """Mark stale active snapshots interrupted during real process startup."""

    with _lock:
        if not _uses_repository():
            return []
        assert _repository is not None
        interrupted: list[str] = []
        for snapshot in _repository.list_task_snapshots_by_status(_ACTIVE_STATES):
            task_id = str(snapshot.get("id") or "")
            if not task_id:
                continue
            snapshot["status"] = "INTERRUPTED"
            snapshot["phase"] = "finished"
            snapshot["current_agent"] = None
            snapshot["updated_at"] = _now()
            _repository.save_task_snapshot(task_id, snapshot)
            interrupted.append(task_id)
        return interrupted


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
        if str(record.get("status") or "") in _TERMINAL_STATES:
            # Late worker/provider events are still durable in the event ledger
            # but may not rewrite a terminal task projection.
            return None
        agent_id = event.get("agent_instance_id")
        if agent_id:
            hierarchy = record.setdefault("hierarchy", {})
            agents = hierarchy.setdefault("agents", {})
            agent = dict(agents.get(str(agent_id)) or {})
            projected_status = event.get("status")
            if event_type == "model_request_started":
                projected_status = "running"
            elif event_type == "agent_completed":
                projected_status = "completed"
            elif event_type in {"agent_cancelled", "agent_skipped"}:
                projected_status = "cancelled" if event_type == "agent_cancelled" else "skipped"
            elif event_type == "agent_abandoned":
                projected_status = "abandoned"
            elif event_type == "agent_blocked":
                projected_status = "blocked"
            elif event_type in {"agent_failed", "preflight_failed"}:
                projected_status = event.get("status") or "failed"
            agent.update(
                {
                    key: value
                    for key, value in {
                        "id": str(agent_id),
                        "role": event.get("role") or agent.get("role"),
                        "manager_id": event.get("manager_id") or agent.get("manager_id"),
                        "workstream_id": event.get("workstream_id") or agent.get("workstream_id"),
                        "work_item_id": event.get("work_item_id") or agent.get("work_item_id"),
                        "status": projected_status or agent.get("status") or "idle",
                        "title": event.get("title") or agent.get("title"),
                        "goal": event.get("goal") or agent.get("goal"),
                        "model": event.get("model") or agent.get("model"),
                        "effort": event.get("effort") or agent.get("effort"),
                        "updated_at": event.get("timestamp") or _now(),
                    }.items()
                    if value is not None
                }
            )
            agents[str(agent_id)] = agent
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
            hierarchy["requested_manager_count"] = event.get("requested_manager_count", 1)
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
                streams = record.setdefault("hierarchy", {}).setdefault("workstreams", {})
                stream = dict(streams.get(stream_id) or {})
                stream["status"] = "ready"
                stream["requested_worker_count"] = event.get("requested_worker_count", 1)
                stream["work_items"] = event.get("work_items", [])
                streams[stream_id] = stream
        elif event_type == "execution_epoch_started":
            record["status"] = "MANAGING"
            record["phase"] = "execution_epoch"
            record["current_agent"] = "director"
            hierarchy = record.setdefault("hierarchy", {})
            hierarchy["execution_epoch"] = event.get("execution_epoch")
            hierarchy["manager_reports"] = {
                "execution_epoch": event.get("execution_epoch"),
                "expected_manager_ids": list(event.get("expected_manager_ids") or []),
                "reports": {},
                "expected_count": int(event.get("expected_manager_count") or 0),
                "reported_count": 0,
            }
            hierarchy.pop("manager_report_barrier", None)
            hierarchy.pop("director_final_review", None)
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
                "max_manager_count": int(event.get("max_manager_count") or 0),
                "unused_manager_capacity": int(event.get("unused_manager_capacity") or 0),
                "max_coders_per_manager": int(event.get("max_coders_per_manager") or 0),
                "max_parallel_managers": int(event.get("max_parallel_managers") or 0),
                "max_parallel_workers_per_manager": int(
                    event.get("max_parallel_workers_per_manager") or 0
                ),
                "max_parallel_workers": int(event.get("max_parallel_workers") or 0),
                "dependency_aware": bool(event.get("dependency_aware", True)),
                "manager_agent_ids": list(event.get("manager_agent_ids") or []),
                "worker_agent_ids": list(event.get("worker_agent_ids") or []),
                "tester_agent_ids": list(event.get("tester_agent_ids") or []),
                "primary_agent_ids": list(event.get("primary_agent_ids") or []),
            }
            _execution_state(record)
        elif event_type == "fanout_selected":
            record["status"] = "MANAGING"
            record["phase"] = "fanout_selected"
            hierarchy = record.setdefault("hierarchy", {})
            selections = hierarchy.setdefault("fanout_selections", {})
            level = str(event.get("level") or "unknown")
            key = str(event.get("workstream_id")) if event.get("workstream_id") else level
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
                execution["request_attempts"] = int(execution.get("request_attempts") or 0) + 1
                if event.get("replayed"):
                    execution["replayed_requests"] = (
                        int(execution.get("replayed_requests") or 0) + 1
                    )
                called_ids = list(execution.get("called_agent_ids") or [])
                if agent_id and agent_id not in called_ids:
                    called_ids.append(agent_id)
                    execution["called_agent_ids"] = called_ids
                    by_role = dict(execution.get("called_by_role") or {})
                    by_role[role] = int(by_role.get(role) or 0) + 1
                    execution["called_by_role"] = by_role
                # "Used" means a request actually went out on this account, so
                # it is counted here and nowhere else: not when a cookie is
                # loaded, not when an agent is merely planned, and not twice
                # when the same account is retried.
                account_ref = event.get("account_ref") or event.get("account")
                if account_ref and str(account_ref) != "[REDACTED]":
                    used_accounts = list(execution.get("used_accounts") or [])
                    if account_ref not in used_accounts:
                        used_accounts.append(str(account_ref))
                        execution["used_accounts"] = used_accounts
                    execution["accounts_used"] = len(execution.get("used_accounts") or [])
            elif event_type == "model_request_completed":
                execution["completed_requests"] = int(execution.get("completed_requests") or 0) + 1
                completed_ids = list(execution.get("completed_agent_ids") or [])
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
                    "call_id": event.get("call_id") or event.get("logical_request_id"),
                    "type": event_type,
                    "error": event.get("error"),
                    "role": role,
                    "agent_instance_id": agent_id,
                }
            _track_call(record, event, event_type)
        elif event_type == "account_switch":
            execution = _execution_state(record)
            execution["account_switches"] = int(execution.get("account_switches") or 0) + 1
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
            "workstream_blocked",
            "workstream_failed",
            "workstream_skipped",
            "manager_replan_created",
        }:
            record["status"] = (
                "REVISION"
                if event_type
                in {
                    "workstream_blocked",
                    "workstream_failed",
                    "workstream_skipped",
                    "manager_replan_created",
                }
                else "MANAGING"
            )
            record["phase"] = event_type
            record["current_agent"] = "manager"
            stream_id = event.get("workstream_id")
            if stream_id:
                streams = record.setdefault("hierarchy", {}).setdefault("workstreams", {})
                stream = dict(streams.get(stream_id) or {})
                stream["status"] = event.get("status", event_type)
                stream["summary"] = event.get("summary")
                streams[stream_id] = stream
        elif event_type == "director_replan_created":
            record["status"] = "REVISION"
            record["phase"] = "director_replan"
            record["current_agent"] = "director"
        elif event_type == "manager_terminal_report":
            hierarchy = record.setdefault("hierarchy", {})
            reports_state = hierarchy.setdefault(
                "manager_reports",
                {
                    "execution_epoch": event.get("execution_epoch"),
                    "expected_manager_ids": [],
                    "reports": {},
                    "expected_count": 0,
                    "reported_count": 0,
                },
            )
            reports = reports_state.setdefault("reports", {})
            manager_id = str(event.get("manager_id") or event.get("agent_instance_id") or "")
            if manager_id and manager_id not in reports:
                reports[manager_id] = {
                    key: deepcopy(event.get(key))
                    for key in (
                        "manager_id",
                        "workstream_id",
                        "status",
                        "completed_item_ids",
                        "abandoned_item_ids",
                        "skipped_item_ids",
                        "artifacts",
                        "reasons",
                        "log_refs",
                        "synthesized",
                    )
                }
            reports_state["reported_count"] = len(reports)
            record["phase"] = "manager_terminal_report"
            record["current_agent"] = "manager"
        elif event_type == "manager_report_barrier":
            hierarchy = record.setdefault("hierarchy", {})
            reports_state = hierarchy.setdefault("manager_reports", {"reports": {}})
            local_reports = reports_state.get("reports") or {}
            expected = sorted(
                str(value)
                for value in (
                    event.get("expected_manager_ids")
                    or reports_state.get("expected_manager_ids")
                    or []
                )
            )
            reported = sorted(str(value) for value in local_reports)
            satisfied = (
                bool(event.get("satisfied"))
                and reported == expected
                and int(event.get("reported_count") or 0) == len(reported)
                and int(event.get("expected_count") or 0) == len(expected)
            )
            hierarchy["manager_report_barrier"] = {
                "execution_epoch": event.get("execution_epoch"),
                "expected_manager_ids": expected,
                "reported_manager_ids": reported,
                "expected_count": len(expected),
                "reported_count": len(reported),
                "satisfied": satisfied,
            }
            record["phase"] = "manager_report_barrier"
            record["current_agent"] = "director"
            if not satisfied:
                record["status"] = "FAILED"
                record["last_error"] = "Manager terminal report barrier is incomplete"
        elif event_type == "director_final_review":
            hierarchy = record.setdefault("hierarchy", {})
            prior = hierarchy.get("director_final_review") or {}
            count = int(prior.get("count") or 0) + 1
            if count == 1:
                hierarchy["director_final_review"] = {
                    "count": 1,
                    "execution_epoch": event.get("execution_epoch"),
                    "verdict": event.get("verdict"),
                    "summary": event.get("summary"),
                    "remaining_risks": list(event.get("remaining_risks") or []),
                    "integration_status": event.get("integration_status"),
                    "manager_reports_expected": event.get("manager_reports_expected"),
                    "manager_reports_reported": event.get("manager_reports_reported"),
                }
            else:
                prior["count"] = count
                prior["invariant_violation"] = "director_final_review_not_exactly_once"
                hierarchy["director_final_review"] = prior
                record["status"] = "FAILED"
                record["last_error"] = "Director final review was emitted more than once"
            record["phase"] = "director_final_review"
            record["current_agent"] = "director"
        elif event_type in {
            "crisis_detected",
            "remediation_started",
            "remediation_applied",
            "remediation_exhausted",
        }:
            record["phase"] = event_type
            record["current_agent"] = event.get("role")
            hierarchy = record.setdefault("hierarchy", {})
            hierarchy["last_crisis"] = {
                "type": event_type,
                "crisis_id": event.get("crisis_id"),
                "scope": event.get("scope"),
                "reason": event.get("reason"),
                "failure_kind": event.get("failure_kind"),
                "affected_manager_ids": list(event.get("affected_manager_ids") or []),
                "affected_work_item_ids": list(event.get("affected_work_item_ids") or []),
            }
        elif event_type in {
            "hierarchy_completed",
            "hierarchy_partial",
            "hierarchy_cancelled",
            "hierarchy_failed",
        }:
            requested_status = (
                "COMPLETED"
                if event_type == "hierarchy_completed"
                else "PARTIAL"
                if event_type == "hierarchy_partial"
                else "STOPPED"
                if event_type == "hierarchy_cancelled"
                else "FAILED"
            )
            hierarchy = record.setdefault("hierarchy", {})
            barrier_ok = bool((hierarchy.get("manager_report_barrier") or {}).get("satisfied"))
            review_count = int(
                (hierarchy.get("director_final_review") or {}).get("count") or 0
            )
            if requested_status in {"COMPLETED", "PARTIAL"} and (
                not barrier_ok or review_count != 1
            ):
                record["status"] = "FAILED"
                record["last_error"] = (
                    "Hierarchy terminal state rejected: N/N Manager reports and "
                    "one Director final review are required"
                )
            else:
                record["status"] = requested_status
            record["phase"] = "director_review"
            record["current_agent"] = "director"
            hierarchy["verdict"] = event.get("verdict")
            record["last_reviewer_feedback"] = event.get("summary", "")
        elif event_type == "completion_reconciliation":
            covered = event.get("covered")
            coverage_failed = (
                not bool(event.get("balanced"))
                if covered is None
                else not bool(covered)
            )
            record["phase"] = "completion_reconciliation"
            record.setdefault("hierarchy", {})["reconciliation"] = {
                key: event.get(key)
                for key in (
                    "balanced",
                    "covered",
                    "successful",
                    "errors",
                    "contract_violations",
                    "workstreams",
                    "work_items",
                    "agents",
                    "agent_calls",
                    "agent_call_purposes",
                    "calls",
                    "manager_reports",
                    "report_barrier",
                    "director_final_review",
                )
            }
            # Coverage/invariant failure fails the task. Contract-proof gaps on
            # a covered PARTIAL must not terminalize here, or hierarchy_partial
            # is dropped by the terminal-status guard.
            if coverage_failed:
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
            record.setdefault("hierarchy", {}).setdefault("project_lease", {})["status"] = "lost"
        elif event_type == "effect_applied":
            effects = record.setdefault("hierarchy", {}).setdefault("effects", {})
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
            record["status"] = "CODING" if event.get("accepted") else "REVISION"
            record["phase"] = "execution"
            record["last_worker_feedback"] = event.get("worker_feedback", "")
            record["last_execution_result"] = event.get("execution_result")
            file_path = event.get("file_path")
            if file_path and event.get("accepted"):
                record["changed_files"][file_path] = {
                    "additions": int(event.get("additions", 0)),
                    "deletions": int(event.get("deletions", 0)),
                }
        elif event_type == "agent_blocked":
            record["phase"] = "agent_blocked"
            record["current_agent"] = event.get("role")
        elif event_type in {"agent_cancelled", "agent_skipped", "agent_abandoned"}:
            record["phase"] = event_type
            record["current_agent"] = event.get("role")
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
            record.setdefault("events", []).append({"at": record["updated_at"], **event})
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
        current_status = str(record.get("status") or "")
        if current_status in _TERMINAL_STATES and current_status != status:
            if not record.get("finished_at"):
                record["finished_at"] = _now()
                record["updated_at"] = record["finished_at"]
                _persist_record(task_id, record)
            return
        effective_status = _persist_completion_invariant(record, status)
        record["status"] = effective_status
        record["phase"] = "finished"
        record["current_agent"] = None
        record["finished_at"] = _now()
        record["updated_at"] = record["finished_at"]
        if reason:
            record["stopped_reason"] = reason
        if status in {"COMPLETED", "PARTIAL"} and effective_status != status:
            invariant = record["hierarchy"]["completion_invariant"]
            record["last_error"] = (
                "Task terminal state rejected because hierarchy completion "
                f"coverage was incomplete: {invariant}"
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
        if str(record.get("status") or "") in _TERMINAL_STATES:
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
        hierarchy = record.setdefault("hierarchy", {})
        hierarchy["execution"] = {"calls": {}}
        for transient_key in (
            "completion_invariant",
            "director_final_review",
            "manager_report_barrier",
            "manager_reports",
            "reconciliation",
        ):
            hierarchy.pop(transient_key, None)
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
