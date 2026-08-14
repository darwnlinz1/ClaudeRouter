"""Inspect sanitized durable LLM request attempts without mutating the database."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

DEFAULT_DB = Path(
    os.environ.get(
        "ORCHESTRATOR_DB_PATH",
        Path.home() / ".ai_orchestrator" / "orchestrator.sqlite3",
    )
)
JSON_COLUMNS = {
    "logical_request_json": "logical_request",
    "tool_schema_json": "tool_schema",
    "wire_body_json": "wire_body",
    "response_headers_json": "response_headers",
    "response_body_json": "response_body",
    "parser_result_json": "parser_result",
}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_id", nargs="?", help="Task to inspect; defaults to latest")
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--full",
        action="store_true",
        help="Include full redacted prompts, schemas, wire bodies, and responses",
    )
    parser.add_argument(
        "--schema-errors",
        action="store_true",
        help="Show only parser, malformed-input, and conversation-input errors",
    )
    return parser.parse_args()


def _load_json(raw: Any) -> Any:
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _latest_task(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """SELECT task_id FROM llm_request_attempts
        WHERE task_id IS NOT NULL AND task_id != ''
        ORDER BY created_at DESC, attempt_id DESC LIMIT 1"""
    ).fetchone()
    return str(row["task_id"]) if row else None


def _summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "attempt_id": row["attempt_id"],
        "logical_request_id": row["logical_request_id"],
        "request_revision": row["request_revision"],
        "provider_attempt": row["provider_attempt"],
        "status": row["status"],
        "provider": row["provider"],
        "route": row["route"],
        "model": row["model"],
        "effort": row["effort"],
        "account_ref": row["account_ref"],
        "org_ref": row["org_ref"],
        "agent_instance_id": row["agent_instance_id"],
        "agent_role": row["agent_role"],
        "workstream_id": row["workstream_id"],
        "work_item_id": row["work_item_id"],
        "request_fingerprint": row["request_fingerprint"],
        "wire_fingerprint": row["wire_fingerprint"],
        "response_status": row["response_status"],
        "error_stage": row["error_stage"],
        "error_classification": row["error_classification"],
        "error_type": row["error_type"],
        "error_message": row["error_message"],
        "retryable": (
            None if row["retryable"] is None else bool(row["retryable"])
        ),
        "probe_of_attempt_id": row["probe_of_attempt_id"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
        "duration_ms": row["duration_ms"],
        "transport_duration_ms": row["transport_duration_ms"],
    }


def _event_schema_diagnostics(
    connection: sqlite3.Connection,
    task_id: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events'"
    ).fetchone()
    if table is None:
        return []
    rows = connection.execute(
        """SELECT sequence, event_type, envelope_json, created_at
        FROM events
        WHERE task_id = ?
          AND event_type IN (
            'event_schema_validation_failure',
            'event_schema_validation_repaired'
          )
        ORDER BY sequence DESC LIMIT ?""",
        (task_id, limit),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        envelope = _load_json(row["envelope_json"])
        payload = envelope.get("payload") if isinstance(envelope, dict) else {}
        payload = payload if isinstance(payload, dict) else {}
        result.append(
            {
                "sequence": row["sequence"],
                "type": row["event_type"],
                "created_at": row["created_at"],
                **payload,
            }
        )
    return result


def main() -> int:
    args = _arguments()
    if args.limit < 1 or args.limit > 10000:
        raise SystemExit("--limit must be between 1 and 10000")
    database = args.database.expanduser().resolve()
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'llm_request_attempts'"
        ).fetchone()
        if table is None:
            raise SystemExit("llm_request_attempts is unavailable; apply migration 15")
        task_id = args.task_id or _latest_task(connection)
        if not task_id:
            print("[]")
            return 0
        query = "SELECT * FROM llm_request_attempts WHERE task_id = ?"
        params: list[Any] = [task_id]
        if args.schema_errors:
            query += (
                " AND (error_stage = 'parser' OR error_classification IN "
                "('malformed_input','schema_error','protocol_error',"
                "'provider_conversation_input'))"
            )
        query += " ORDER BY created_at DESC, attempt_id DESC LIMIT ?"
        params.append(args.limit)
        rows = connection.execute(query, tuple(params)).fetchall()
        result = []
        for row in rows:
            item = _summary(row)
            if args.full:
                for column, field in JSON_COLUMNS.items():
                    item[field] = _load_json(row[column])
                item["response_headers"] = item.get("response_headers") or {}
            result.append(item)
        output: Any = result
        if args.schema_errors:
            output = {
                "task_id": task_id,
                "request_attempts": result,
                "event_schema_diagnostics": _event_schema_diagnostics(
                    connection,
                    task_id,
                    limit=args.limit,
                ),
            }
        print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
