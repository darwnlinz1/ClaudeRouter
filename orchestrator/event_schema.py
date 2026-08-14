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

SCHEMA_VERSION = 1
TYPESCRIPT_ARTIFACT_PATH = Path(
    "frontend/src/generated/orchestrator-events.generated.ts"
)


@dataclass(frozen=True, slots=True)
class EventSpec:
    fields: Mapping[str, str]
    required: tuple[str, ...] = ()
    version: int = SCHEMA_VERSION


_CORRELATION_FIELDS: dict[str, str] = {
    "task_id": "string",
    "session_id": "string",
    "workstream_id": "string",
    "work_item_id": "string",
    "agent_instance_id": "string",
    "call_id": "string",
    "attempt_id": "string",
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
    "approval_decided": _spec(
        {
            "approval_id": "string",
            "workstream_id": "string",
            "decision": "string",
            "reason": "string",
        },
        required=("approval_id", "decision"),
    ),
    "approval_requested": _spec(
        {
            "approval_id": "string",
            "workstream_id": "string",
            "kind": "string",
            "target": "string",
            "reason": "string",
            "status": "string",
        },
        required=("approval_id", "kind", "target", "reason", "status"),
    ),
    "budget_exceeded": _spec(
        {
            "logical_request_id": "string",
            "role": "string",
            "reason": "string",
            "budget": "object",
        },
        required=("reason", "budget"),
    ),
    "budget_updated": _spec(
        {
            "logical_request_id": "string",
            "role": "string",
            "budget": "object",
        },
        required=("budget",),
    ),
    "account_invalidated": _spec({"reason": "string"}),
    "account_rate_limited": _spec(
        {
            "account": "string",
            "provider": "string",
            "retry_after_seconds": "number",
            "cooldown_seconds": "number",
            "reason": "string",
        }
    ),
    "account_switch": _spec(
        {
            "from_account": "string",
            "to_account": "string",
            "reason": "string",
            "logical_request_id": "string",
        },
        required=("reason",),
    ),
    "agent_failed": _spec(
        {"role": "string", "error": "string", "status": "string"},
        required=("role", "error"),
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
    "agent_progress": _spec(
        {"role": "string", "stage": "string", "message": "string"},
        required=("role",),
    ),
    "agent_started": _spec({"role": "string"}, required=("role",)),
    "completion_reconciliation": _spec(
        {
            "balanced": "boolean",
            "errors": "array",
            "workstreams": "object",
            "work_items": "object",
            "agents": "object",
            "calls": "object",
        },
        required=("balanced",),
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
    "fanout_selected": _spec(
        {
            "level": "string",
            "maximum": "integer",
            "selected": "integer",
            "unused_capacity": "integer",
            "reason": "string",
        },
        required=("level", "selected"),
    ),
    "finish_chat_turn": _spec(),
    "hierarchy_completed": _spec({"verdict": "string", "summary": "string"}),
    "hierarchy_failed": _spec({"verdict": "string", "summary": "string"}),
    "hierarchy_fanout_planned": _spec(
        {
            "manager_count": "integer",
            "coder_count": "integer",
            "tester_count": "integer",
            "child_agent_count": "integer",
        }
    ),
    "integration_result": _spec({"accepted": "boolean", "summary": "string"}),
    "manager_plan_created": _spec(
        {
            "requested_worker_count": "integer",
            "work_items": "array",
        }
    ),
    "model_request_aborted": _spec(
        {"role": "string", "error": "string", "logical_request_id": "string"},
        required=("role",),
    ),
    "model_request_completed": _spec(
        {
            "role": "string",
            "logical_request_id": "string",
            "latency_ms": "number",
        },
        required=("role",),
    ),
    "model_request_failed": _spec(
        {"role": "string", "error": "string", "logical_request_id": "string"},
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
            "role": "string",
            "logical_request_id": "string",
            "replayed": "boolean",
        },
        required=("role",),
    ),
    "plan.created": _spec({"revision": "integer"}, required=("revision",)),
    "plan_created": _spec(
        {"requested_manager_count": "integer", "workstreams": "array"}
    ),
    "planner_count_rejected": _spec(
        {"requested": "integer", "maximum": "integer", "level": "string"}
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
    "protocol_retry": _spec({"role": "string", "error": "string"}),
    "resume": _spec({"turn_count": "integer"}),
    "resume_checkpoint_loaded": _spec({"checkpoint": "object"}),
    "review_result": _spec(
        {"verdict": "string", "reviewer_feedback": "string"},
        required=("verdict",),
    ),
    "status": _spec({"data": "any", "role": "string"}),
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
    "thinking": _spec({"text": "string"}),
    "token": _spec({"text": "string"}),
    "turn_phase": _spec({"phase": "string"}, required=("phase",)),
    "turn_start": _spec({"turn": "integer", "phase": "string"}),
    "workstream_completed": _spec({"status": "string", "summary": "string"}),
    "workstream_failed": _spec({"status": "string", "summary": "string"}),
    "workstream_started": _spec({"status": "string"}),
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
        raise ValueError(
            f"{event_type} event is missing required fields: {', '.join(missing)}"
        )
    for name, value in event.items():
        expected = spec.fields.get(name)
        if name == "type" or expected is None or value is None:
            continue
        if not _matches_type(value, expected):
            raise ValueError(
                f"{event_type}.{name} must be {expected}, "
                f"got {type(value).__name__}"
            )
    return deepcopy(dict(event))


def validate_event_payload(
    event_type: str,
    payload: Mapping[str, Any],
    **correlation: Any,
) -> dict[str, Any]:
    candidate = {"type": event_type, **dict(payload)}
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
            lines.append(
                f"  {name}{optional}: {_typescript_type(spec.fields[name])};"
            )
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
