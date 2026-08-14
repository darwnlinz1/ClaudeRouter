"""Versioned event contracts and deterministic TypeScript generation.

This module is the single source of truth for durable and wire event shapes.
Known events are validated without dropping additive fields. Unknown event
types are deliberately accepted so events written by older/newer releases can
still be replayed losslessly.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

SCHEMA_VERSION = 3
TYPESCRIPT_ARTIFACT_PATH = Path("frontend/src/generated/orchestrator-events.generated.ts")


@dataclass(frozen=True, slots=True)
class EventSpec:
    fields: Mapping[str, str]
    required: tuple[str, ...] = ()
    version: int = SCHEMA_VERSION


_CORRELATION_FIELDS: dict[str, str] = {
    "task_id": "string",
    "session_id": "string",
    "execution_epoch": "string",
    "execution_epoch_id": "string",
    "workstream_id": "string",
    "work_item_id": "string",
    "manager_id": "string",
    "agent_instance_id": "string",
    "logical_agent_id": "string",
    "call_id": "string",
    "logical_request_id": "string",
    "attempt_id": "string",
    "execution_attempt_id": "string",
    "provider_attempt_id": "string",
    "call_purpose": "string",
    "sequence": "integer",
    "timestamp": "string",
}


def _spec(
    fields: Mapping[str, str] | None = None,
    *,
    required: tuple[str, ...] = (),
) -> EventSpec:
    return EventSpec(fields={**_CORRELATION_FIELDS, **dict(fields or {})}, required=required)


# Keep entries alphabetic. Generation also sorts them, making output stable if
# declaration order changes during maintenance.
EVENT_SCHEMAS: dict[str, EventSpec] = {
    "agent_abandoned": _spec(
        {
            "role": "string",
            "status": "string",
            "reason": "string",
            "failure_kind": "string",
            "artifacts": "array",
            "log_refs": "array",
        },
        required=("role", "status", "reason"),
    ),
    "approval_decided": _spec(
        {
            "approval_id": "string",
            "approval_mode": "string",
            "automated": "boolean",
            "workstream_id": "string",
            "decision": "string",
            "reason": "string",
        },
        required=("approval_id", "decision"),
    ),
    "approval_requested": _spec(
        {
            "approval_id": "string",
            "approval_mode": "string",
            "workstream_id": "string",
            "kind": "string",
            "target": "string",
            "reason": "string",
            "status": "string",
        },
        required=("approval_id", "kind", "target", "reason", "status"),
    ),
    "account_invalidated": _spec(
        {
            "account_state": "string",
            "attempt": "integer",
            "attempt_terminal": "boolean",
            "credential_deleted": "boolean",
            "from_account": "string",
            "logical_request_id": "string",
            "provider": "string",
            "reason": "string",
            "request_fingerprint": "string",
            "request_revision": "integer",
            "reset_provisional": "boolean",
            "role": "string",
        }
    ),
    "account_rate_limited": _spec(
        {
            "account": "string",
            "attempt": "integer",
            "attempt_terminal": "boolean",
            "cooldown_seconds": "number",
            "from_account": "string",
            "logical_request_id": "string",
            "provider": "string",
            "reason": "string",
            "request_fingerprint": "string",
            "request_revision": "integer",
            "reset_provisional": "boolean",
            "retry_after_seconds": "number",
            "role": "string",
        }
    ),
    "account_switch": _spec(
        {
            "attempt": "integer",
            "from_account": "string",
            "logical_request_id": "string",
            "provider": "string",
            "reason": "string",
            "replayed": "boolean",
            "request_fingerprint": "string",
            "request_revision": "integer",
            "role": "string",
            "to_account": "string",
        },
        required=("reason",),
    ),
    "agent_account_assigned": _spec(
        {
            "account": "string",
            "account_ref": "string",
            "logical_request_id": "string",
            "provider": "string",
            "role": "string",
            "status": "string",
        },
        required=("role", "status"),
    ),
    "agent_failed": _spec(
        {"role": "string", "error": "string", "status": "string"},
        required=("role", "error"),
    ),
    "agent_blocked": _spec(
        {
            "role": "string",
            "status": "string",
            "summary": "string",
            "failure_kind": "string",
            "blocked_by": "array",
        },
        required=("role", "status"),
    ),
    "agent_cancelled": _spec(
        {
            "role": "string",
            "status": "string",
            "error": "string",
            "failure_kind": "string",
        },
        required=("role", "status"),
    ),
    "agent_completed": _spec(
        {
            "role": "string",
            "status": "string",
            "accepted": "boolean",
            "file_path": "string",
        },
        required=("role", "agent_instance_id"),
    ),
    "agent_message": _spec(
        {
            "role": "string",
            "source_agent_id": "string",
            "target_agent_id": "string",
            "signal_type": "string",
            "summary": "string",
        },
        required=("source_agent_id", "target_agent_id"),
    ),
    "agent_planned": _spec(
        {"role": "string", "status": "string"},
        required=("role", "agent_instance_id"),
    ),
    "agent_progress": _spec(
        {"role": "string", "stage": "string", "message": "string"},
        required=("role",),
    ),
    "agent_skipped": _spec(
        {
            "role": "string",
            "status": "string",
            "reason": "string",
        },
        required=("role", "status"),
    ),
    "agent_started": _spec(
        {"role": "string"},
        required=("role", "agent_instance_id"),
    ),
    "agent_waiting_account": _spec(
        {
            "logical_request_id": "string",
            "provider": "string",
            "role": "string",
            "status": "string",
            "summary": "string",
            "waiting_on": "array",
        },
        required=("role", "status", "summary"),
    ),
    "completion_reconciliation": _spec(
        {
            "balanced": "boolean",
            "covered": "boolean",
            "successful": "boolean",
            "errors": "array",
            "workstreams": "object",
            "work_items": "object",
            "agents": "object",
            "agent_calls": "object",
            "agent_call_purposes": "object",
            "calls": "object",
            "manager_reports": "object",
            "report_barrier": "object",
            "director_final_review": "object",
        },
        required=("balanced",),
    ),
    "crisis_detected": _spec(
        {
            "crisis_id": "string",
            "scope": "string",
            "failure_kind": "string",
            "reason": "string",
            "retryable": "boolean",
            "affected_manager_ids": "array",
            "affected_work_item_ids": "array",
        },
        required=("crisis_id", "scope", "failure_kind", "reason", "retryable"),
    ),
    "director_final_review": _spec(
        {
            "verdict": "string",
            "summary": "string",
            "remaining_risks": "array",
            "integration_status": "string",
            "manager_reports_expected": "integer",
            "manager_reports_reported": "integer",
            "final_review_number": "integer",
        },
        required=(
            "verdict",
            "summary",
            "manager_reports_expected",
            "manager_reports_reported",
            "final_review_number",
        ),
    ),
    "director_replan_created": _spec({"reason": "string"}),
    "effect_applied": _spec(
        {
            "effect_id": "string",
            "effect_kind": "string",
            "idempotency_key": "string",
            "before_sha256": "string",
            "after_sha256": "string",
            "file_path": "string",
        },
        required=("effect_id",),
    ),
    "error": _spec({"data": "any"}),
    "event_schema_validation_failure": _spec(
        {
            "action": "string",
            "field_names": "array",
            "repaired_fields": "array",
            "source_event_type": "string",
            "summary": "string",
            "validation_error": "string",
        },
        required=("action", "source_event_type", "summary", "validation_error"),
    ),
    "event_schema_validation_repaired": _spec(
        {
            "action": "string",
            "field_names": "array",
            "repaired_fields": "array",
            "source_event_type": "string",
            "summary": "string",
            "validation_error": "string",
        },
        required=("action", "source_event_type", "summary", "validation_error"),
    ),
    "execution_result": _spec(
        {
            "accepted": "boolean",
            "file_path": "string",
            "additions": "integer",
            "deletions": "integer",
            "worker_feedback": "string",
            "execution_result": "any",
        },
        required=("accepted",),
    ),
    "execution_epoch_started": _spec(
        {
            "expected_manager_ids": "array",
            "expected_manager_count": "integer",
            "plan_revision": "integer",
        },
        required=(
            "execution_epoch",
            "expected_manager_ids",
            "expected_manager_count",
            "plan_revision",
        ),
    ),
    "fanout_selected": _spec(
        {
            "level": "string",
            "maximum": "integer",
            "selected": "integer",
            "unused_capacity": "integer",
            "reason": "string",
            "role": "string",
            "manager_id": "string",
        },
        required=("level", "selected"),
    ),
    "finish_chat_turn": _spec(),
    "hierarchy_completed": _spec({"verdict": "string", "summary": "string"}),
    "hierarchy_cancelled": _spec({"verdict": "string", "summary": "string"}),
    "hierarchy_failed": _spec({"verdict": "string", "summary": "string"}),
    "hierarchy_partial": _spec(
        {
            "verdict": "string",
            "summary": "string",
            "completed_workstream_ids": "array",
            "abandoned_workstream_ids": "array",
            "skipped_workstream_ids": "array",
        },
        required=(
            "verdict",
            "summary",
            "completed_workstream_ids",
            "abandoned_workstream_ids",
            "skipped_workstream_ids",
        ),
    ),
    "hierarchy_fanout_planned": _spec(
        {
            "manager_count": "integer",
            "coder_count": "integer",
            "tester_count": "integer",
            "child_agent_count": "integer",
            "primary_agent_count": "integer",
            "primary_child_request_count": "integer",
            "manager_agent_ids": "array",
            "worker_agent_ids": "array",
            "tester_agent_ids": "array",
            "primary_agent_ids": "array",
            "max_manager_count": "integer",
            "unused_manager_capacity": "integer",
            "max_coders_per_manager": "integer",
            "max_parallel_managers": "integer",
            "max_parallel_workers_per_manager": "integer",
            "max_parallel_workers": "integer",
            "dependency_aware": "boolean",
            "status": "string",
            "summary": "string",
        },
        required=(
            "manager_count",
            "coder_count",
            "tester_count",
            "primary_agent_count",
            "manager_agent_ids",
            "worker_agent_ids",
            "tester_agent_ids",
            "primary_agent_ids",
        ),
    ),
    "integration_result": _spec({"accepted": "boolean", "summary": "string"}),
    "manager_plan_created": _spec(
        {
            "requested_worker_count": "integer",
            "work_items": "array",
        }
    ),
    "manager_report_barrier": _spec(
        {
            "expected_manager_ids": "array",
            "reported_manager_ids": "array",
            "expected_count": "integer",
            "reported_count": "integer",
            "satisfied": "boolean",
        },
        required=(
            "execution_epoch",
            "expected_manager_ids",
            "reported_manager_ids",
            "expected_count",
            "reported_count",
            "satisfied",
        ),
    ),
    "manager_terminal_report": _spec(
        {
            "status": "string",
            "completed_item_ids": "array",
            "abandoned_item_ids": "array",
            "skipped_item_ids": "array",
            "artifacts": "array",
            "reasons": "array",
            "log_refs": "array",
            "synthesized": "boolean",
        },
        required=(
            "execution_epoch",
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
        ),
    ),
    "model_request_aborted": _spec(
        {
            "account": "string",
            "attempt_terminal": "boolean",
            "error": "string",
            "logical_request_id": "string",
            "provider": "string",
            "request_fingerprint": "string",
            "request_revision": "integer",
            "reset_provisional": "boolean",
            "role": "string",
        },
        required=("role",),
    ),
    "model_request_completed": _spec(
        {
            "account": "string",
            "account_ref": "string",
            "attempt": "integer",
            "attempt_terminal": "boolean",
            "latency_ms": "number",
            "logical_request_id": "string",
            "provider": "string",
            "replayed": "boolean",
            "request_fingerprint": "string",
            "request_revision": "integer",
            "reset_provisional": "boolean",
            "role": "string",
            "tool_name": "string",
        },
        required=("role",),
    ),
    "model_request_failed": _spec(
        {
            "account": "string",
            "attempt_terminal": "boolean",
            "error": "string",
            "logical_request_id": "string",
            "provider": "string",
            "reason": "string",
            "request_fingerprint": "string",
            "request_revision": "integer",
            "reset_provisional": "boolean",
            "role": "string",
            "summary": "string",
        },
        required=("role", "error"),
    ),
    "model_request_replayed": _spec(
        {
            "role": "string",
            "logical_request_id": "string",
            "request_revision": "integer",
            "replayed": "boolean",
        },
        required=("role", "logical_request_id"),
    ),
    "model_request_started": _spec(
        {
            "account": "string",
            "account_ref": "string",
            "attempt": "integer",
            "attempt_terminal": "boolean",
            "role": "string",
            "logical_request_id": "string",
            "provider": "string",
            "request_fingerprint": "string",
            "request_revision": "integer",
            "replayed": "boolean",
            "stage": "string",
        },
        required=("role",),
    ),
    "plan.created": _spec({"revision": "integer"}, required=("revision",)),
    "plan_created": _spec({"requested_manager_count": "integer", "workstreams": "array"}),
    "planner_count_rejected": _spec(
        {"requested": "integer", "maximum": "integer", "level": "string"}
    ),
    "preflight_failed": _spec(
        {
            "role": "string",
            "status": "string",
            "error": "string",
            "failure_kind": "string",
            "issues": "array",
        },
        required=("status", "error"),
    ),
    "plan_preflight_failed": _spec(
        {
            "issues": "array",
            "worker_calls_started": "integer",
        },
        required=("issues",),
    ),
    "project_lease_acquired": _spec(
        {
            "project_key": "string",
            "fencing_token": "integer",
            "isolation_level": "string",
        },
        required=("project_key", "fencing_token"),
    ),
    "project_lease_lost": _spec(
        {"project_key": "string", "status": "string", "error": "string"},
        required=("error",),
    ),
    "protocol_retry": _spec(
        {
            "account": "string",
            "attempt": "integer",
            "attempt_terminal": "boolean",
            "error": "string",
            "logical_request_id": "string",
            "max_attempts": "integer",
            "provider": "string",
            "reason": "string",
            "request_revision": "integer",
            "reset_provisional": "boolean",
            "role": "string",
            "switching": "boolean",
        }
    ),
    "remediation_applied": _spec(
        {
            "crisis_id": "string",
            "scope": "string",
            "action": "string",
            "reason": "string",
            "instructions": "string",
            "affected_manager_ids": "array",
            "affected_work_item_ids": "array",
        },
        required=("crisis_id", "scope", "action"),
    ),
    "remediation_exhausted": _spec(
        {
            "crisis_id": "string",
            "scope": "string",
            "reason": "string",
            "affected_manager_ids": "array",
            "affected_work_item_ids": "array",
        },
        required=("crisis_id", "scope", "reason"),
    ),
    "remediation_started": _spec(
        {
            "crisis_id": "string",
            "scope": "string",
            "action": "string",
            "reason": "string",
            "instructions": "string",
            "affected_manager_ids": "array",
            "affected_work_item_ids": "array",
        },
        required=("crisis_id", "scope", "action"),
    ),
    "resume": _spec({"turn_count": "integer"}),
    "resume_checkpoint_loaded": _spec({"checkpoint": "object"}),
    "review_result": _spec(
        {"verdict": "string", "reviewer_feedback": "string"},
        required=("verdict",),
    ),
    "status": _spec(
        {
            "account": "string",
            "account_ref": "string",
            "data": "any",
            "role": "string",
        }
    ),
    "task.started": _spec(),
    "test_result": _spec(
        {
            "passed": "boolean",
            "status": "string",
            "accepted": "boolean",
            "command": "string",
            "detail": "string",
            "requested_isolation": "string",
            "actual_isolation": "string",
            "isolation_details": "string",
            "output_truncated": "boolean",
        }
    ),
    "timeline_gap": _spec(
        {
            "retained_from_sequence": "integer",
            "latest_sequence": "integer",
            "history_incomplete": "boolean",
        },
        required=(
            "retained_from_sequence",
            "latest_sequence",
            "history_incomplete",
        ),
    ),
    "thinking": _spec(
        {
            "attempt": "integer",
            "attempt_terminal": "boolean",
            "chunk_index": "integer",
            "committed": "boolean",
            "logical_request_id": "string",
            "provider": "string",
            "provisional": "boolean",
            "reason": "string",
            "request_revision": "integer",
            "reset": "boolean",
            "role": "string",
            "terminal": "boolean",
            "text": "string",
        }
    ),
    "token": _spec(
        {
            "attempt": "integer",
            "chunk_index": "integer",
            "committed": "boolean",
            "logical_request_id": "string",
            "provider": "string",
            "provisional": "boolean",
            "request_revision": "integer",
            "role": "string",
            "text": "string",
        }
    ),
    "turn_phase": _spec({"phase": "string"}, required=("phase",)),
    "turn_start": _spec({"turn": "integer", "phase": "string"}),
    "workstream_completed": _spec({"status": "string", "summary": "string"}),
    "workstream_blocked": _spec(
        {"status": "string", "summary": "string", "blocked_by": "array"},
        required=("status",),
    ),
    "workstream_failed": _spec({"status": "string", "summary": "string"}),
    "workstream_skipped": _spec(
        {"status": "string", "summary": "string", "reason": "string", "blocked_by": "array"},
        required=("status", "reason"),
    ),
    "workstream_started": _spec({"status": "string"}),
    "work_item_abandoned": _spec(
        {
            "status": "string",
            "reason": "string",
            "failure_kind": "string",
            "artifacts": "array",
            "log_refs": "array",
        },
        required=("status", "reason"),
    ),
    "work_item_skipped": _spec(
        {"status": "string", "reason": "string", "blocked_by": "array"},
        required=("status", "reason"),
    ),
}


def _matches_type(value: Any, type_name: str) -> bool:
    if type_name == "any":
        return True
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "array":
        return isinstance(value, (list, tuple))
    if type_name == "object":
        return isinstance(value, Mapping)
    raise RuntimeError(f"unsupported event field type: {type_name}")


def validate_event(
    event: Mapping[str, Any],
    *,
    preserve_unknown: bool = True,
) -> dict[str, Any]:
    """Validate one discriminated event while retaining additive fields."""
    if not isinstance(event, Mapping):
        raise ValueError("event must be a mapping")
    event_type = event.get("type")
    if not isinstance(event_type, str) or not event_type.strip():
        raise ValueError("event.type must be a non-empty string")
    spec = EVENT_SCHEMAS.get(event_type)
    if spec is None:
        if not preserve_unknown:
            raise ValueError(f"unknown event type: {event_type}")
        return deepcopy(dict(event))
    missing = [name for name in spec.required if name not in event]
    if missing:
        raise ValueError(f"{event_type} event is missing required fields: {', '.join(missing)}")
    for name, value in event.items():
        expected = spec.fields.get(name)
        if name == "type" or expected is None or value is None:
            continue
        if not _matches_type(value, expected):
            raise ValueError(f"{event_type}.{name} must be {expected}, got {type(value).__name__}")
    return deepcopy(dict(event))


def validate_event_payload(
    event_type: str,
    payload: Mapping[str, Any],
    **correlation: Any,
) -> dict[str, Any]:
    # The envelope discriminator is authoritative. A stale or malicious
    # ``payload["type"]`` must never select a different validation contract.
    candidate = {**dict(payload), "type": event_type}
    candidate.update({key: value for key, value in correlation.items() if value is not None})
    validated = validate_event(candidate)
    return {key: value for key, value in validated.items() if key != "type"}


def _typescript_type(type_name: str) -> str:
    return {
        "any": "unknown",
        "array": "unknown[]",
        "boolean": "boolean",
        "integer": "number",
        "number": "number",
        "object": "Record<string, unknown>",
        "string": "string",
    }[type_name]


def generate_typescript() -> str:
    """Return byte-for-byte deterministic TypeScript event declarations."""
    lines = [
        "/* AUTO-GENERATED by orchestrator.event_schema. DO NOT EDIT. */",
        f"export const EVENT_SCHEMA_VERSION = {SCHEMA_VERSION} as const;",
        "",
    ]
    interface_names: list[str] = []
    for index, event_type in enumerate(sorted(EVENT_SCHEMAS)):
        spec = EVENT_SCHEMAS[event_type]
        interface_name = f"KnownEvent{index:03d}"
        interface_names.append(interface_name)
        lines.append(f"export interface {interface_name} {{")
        lines.append(f"  type: {event_type!r};")
        for name in sorted(spec.fields):
            optional = "" if name in spec.required else "?"
            lines.append(f"  {name}{optional}: {_typescript_type(spec.fields[name])};")
        lines.append("  [key: string]: unknown;")
        lines.append("}")
        lines.append("")
    lines.append(f"export type KnownOrchestratorEvent = {' | '.join(interface_names)};")
    lines.extend(
        [
            "export interface LegacyOrchestratorEvent {",
            "  type: string;",
            "  [key: string]: unknown;",
            "}",
            "export type OrchestratorEvent =",
            "  | KnownOrchestratorEvent",
            "  | LegacyOrchestratorEvent;",
            "",
        ]
    )
    return "\n".join(lines)


def write_typescript_artifact(
    project_root: str | Path,
    *,
    relative_path: Path = TYPESCRIPT_ARTIFACT_PATH,
) -> Path:
    """Write the generated artifact when explicitly invoked by tooling."""
    destination = Path(project_root) / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(generate_typescript(), encoding="utf-8", newline="\n")
    return destination
