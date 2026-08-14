"""Durable, sanitized diagnostics for every provider request attempt."""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Sequence

from . import redaction

logger = logging.getLogger(__name__)
REDACTION_VERSION = 1
_repository: object | None = None
_repository_lock = threading.RLock()
_SENSITIVE_KEYS = frozenset(
    {
        "account",
        "authorization",
        "cookie",
        "cookie_file",
        "cookie_path",
        "cookie_string",
        "credential_source",
        "from_account",
        "org_id",
        "organization_id",
        "organization_uuid",
        "raw_cookie",
        "raw_org_id",
        "sessionkey",
        "set-cookie",
        "source_cookie_file",
        "to_account",
    }
)
_SAFE_REFERENCE_KEYS = frozenset({"account_ref", "org_ref"})
_IDENTIFIER_KEYS = frozenset(
    {
        "agent_instance_id",
        "attempt_id",
        "call_id",
        "execution_attempt_id",
        "logical_request_id",
        "manager_id",
        "probe_of_attempt_id",
        "request_fingerprint",
        "session_id",
        "task_id",
        "wire_fingerprint",
        "work_item_id",
        "workstream_id",
        *_SAFE_REFERENCE_KEYS,
    }
)
_RESPONSE_HEADER_ALLOWLIST = frozenset(
    {
        "anthropic-request-id",
        "cf-ray",
        "content-type",
        "date",
        "request-id",
        "retry-after",
        "server",
        "x-request-id",
    }
)


def configure_repository(repository: object | None) -> bool:
    """Inject a feature-detected request-attempt repository."""

    if repository is not None:
        required = (
            "create_llm_request_attempt",
            "update_llm_request_attempt",
            "list_llm_request_attempts",
            "compact_llm_request_attempts",
        )
        if not all(callable(getattr(repository, name, None)) for name in required):
            return False
    global _repository
    with _repository_lock:
        _repository = repository
    return True


def is_configured() -> bool:
    with _repository_lock:
        return _repository is not None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        current = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return current.astimezone(timezone.utc).isoformat()
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_jsonable(item) for item in value]
    if callable(value):
        return "[CALLABLE]"
    return str(value)


def sanitize(
    value: Any,
    *,
    key: str = "",
    sensitive_values: Sequence[str] = (),
) -> Any:
    """Redact recursively without truncating diagnostic request structures."""

    normalized_key = key.casefold().replace("-", "_")
    if normalized_key not in _SAFE_REFERENCE_KEYS and (
        normalized_key in _SENSITIVE_KEYS
        or any(
            marker in normalized_key
            for marker in (
                "authorization",
                "cookie",
                "password",
                "private_key",
                "session_key",
                "sessionkey",
                "access_token",
                "api_token",
                "refresh_token",
            )
        )
    ):
        return "[REDACTED]"
    value = _jsonable(value)
    if isinstance(value, str):
        result = value
        for secret in sensitive_values:
            if secret:
                result = result.replace(secret, "[REDACTED]")
        if normalized_key in _IDENTIFIER_KEYS:
            return result
        return redaction.redact_text(result, max_chars=None)
    if isinstance(value, dict):
        return {
            str(child_key): sanitize(
                child_value,
                key=str(child_key),
                sensitive_values=sensitive_values,
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [
            sanitize(item, key=key, sensitive_values=sensitive_values)
            for item in value
        ]
    return value


def wire_fingerprint(body: Any) -> str:
    """Hash the exact JSON-compatible wire body before redaction."""

    encoded = json.dumps(
        _jsonable(body),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class RequestAttemptLog:
    """Best-effort lifecycle writer that never changes provider outcomes."""

    def __init__(
        self,
        record: Mapping[str, Any],
        *,
        sensitive_values: Sequence[str] = (),
    ) -> None:
        self.attempt_id = str(record.get("attempt_id") or "")
        self._sensitive_values = tuple(
            value for value in (str(item) for item in sensitive_values) if value
        )
        self._started_monotonic = time.monotonic()
        self._transport_started_monotonic: float | None = None
        with _repository_lock:
            self._repository = _repository
        safe_record = sanitize(
            {**dict(record), "redaction_version": REDACTION_VERSION},
            sensitive_values=self._sensitive_values,
        )
        self._call("create_llm_request_attempt", safe_record)

    @property
    def enabled(self) -> bool:
        return self._repository is not None and bool(self.attempt_id)

    def add_sensitive(self, *values: str | None) -> None:
        additions = tuple(str(value) for value in values if value)
        if additions:
            self._sensitive_values = (*self._sensitive_values, *additions)

    def _call(self, method_name: str, *args: Any) -> Any:
        callback = getattr(self._repository, method_name, None)
        if not callable(callback):
            return None
        try:
            return callback(*args)
        except Exception:
            logger.warning(
                "Could not persist LLM request attempt %s",
                self.attempt_id or "<unknown>",
                exc_info=True,
            )
            return None

    def update(self, **updates: Any) -> None:
        if not self.attempt_id:
            return
        safe = sanitize(updates, sensitive_values=self._sensitive_values)
        self._call("update_llm_request_attempt", self.attempt_id, safe)

    def record_wire(
        self,
        *,
        route: str,
        body: Any,
        org_id: str | None = None,
    ) -> None:
        self.add_sensitive(org_id)
        self._transport_started_monotonic = time.monotonic()
        self.update(
            route=route,
            wire_body=body,
            wire_fingerprint=wire_fingerprint(body),
            transport_started_at=_utc_now(),
        )

    def record_response(
        self,
        *,
        status: int | None,
        headers: Mapping[str, Any] | None,
        body: Any,
    ) -> None:
        allowed_headers = {
            str(name).casefold(): value
            for name, value in dict(headers or {}).items()
            if str(name).casefold() in _RESPONSE_HEADER_ALLOWLIST
        }
        updates: dict[str, Any] = {
            "response_status": status,
            "response_headers": allowed_headers,
            "response_body": body,
            "response_received_at": _utc_now(),
        }
        if self._transport_started_monotonic is not None:
            updates["transport_duration_ms"] = (
                time.monotonic() - self._transport_started_monotonic
            ) * 1000
        self.update(**updates)

    def terminal(
        self,
        *,
        status: str,
        parser_result: Any = None,
        error_stage: str | None = None,
        error_classification: str | None = None,
        error: BaseException | str | None = None,
        retryable: bool | None = None,
        **updates: Any,
    ) -> None:
        completed_at = _utc_now()
        if error is not None:
            updates["error_type"] = (
                type(error).__name__ if isinstance(error, BaseException) else "Error"
            )
            updates["error_message"] = str(error)
        updates.update(
            {
                "status": status,
                "parser_result": parser_result,
                "error_stage": error_stage,
                "error_classification": error_classification,
                "retryable": retryable,
                "completed_at": completed_at,
                "duration_ms": (time.monotonic() - self._started_monotonic) * 1000,
            }
        )
        self.update(**updates)


def start_attempt(
    record: Mapping[str, Any],
    *,
    sensitive_values: Sequence[str] = (),
) -> RequestAttemptLog:
    return RequestAttemptLog(record, sensitive_values=sensitive_values)
