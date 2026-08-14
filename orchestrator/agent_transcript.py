"""Persist per-agent model thinking and raw responses for protocol debugging."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import redaction

_lock = threading.Lock()
_registration_callback: Callable[..., object] | None = None
logger = logging.getLogger(__name__)
TRANSCRIPT_SCHEMA_VERSION = 1
TRANSCRIPT_FILENAME = "transcript.jsonl"
_COOKIE_HEADER = re.compile(
    r"(?im)^(\s*(?:set-)?cookie\s*:\s*)[^\r\n]+$"
)


@dataclass(frozen=True)
class TranscriptMessage:
    """Secret-free provider-neutral message restored from a completed turn."""

    role: str
    content: str
    tool_calls: tuple[Mapping[str, Any], ...] = ()
    metadata: Mapping[str, Any] | None = None


def configure_registration_callback(
    callback: Callable[..., object] | None,
) -> None:
    """Configure the optional production ``record_log_metadata`` callback."""

    global _registration_callback
    with _lock:
        _registration_callback = callback


def configure_repository(repository: object | None) -> None:
    """Use a repository's log registration hook when it exposes one."""

    callback = getattr(repository, "record_log_metadata", None)
    configure_registration_callback(callback if callable(callback) else None)


def _root() -> Path:
    configured = os.environ.get("ORCH_AGENT_LOG_DIR", "logs/agents")
    return Path(configured)


def _safe_segment(value: str) -> str:
    cleaned = re.sub(r"[^\w.\-]+", "_", value.strip())[:120]
    return cleaned or "unknown"


def agent_dir(task_id: str | None, agent_id: str | None) -> Path | None:
    if not task_id or not agent_id:
        return None
    path = _root() / _safe_segment(task_id) / _safe_segment(agent_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def append_text(path: Path, text: str) -> None:
    with _lock, path.open("a", encoding="utf-8") as handle:
        handle.write(text)
        if not text.endswith("\n"):
            handle.write("\n")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    safe_payload = redaction.redact_event(payload)
    line = json.dumps(
        safe_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    append_text(path, line)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _register_log_path(
    path: Path,
    *,
    task_id: str | None,
    agent_id: str | None,
    call_id: str | None,
    attempt_id: str | None,
    kind: str,
    terminal_evidence: bool,
    pinned: bool,
    registration_callback: Callable[..., object] | None,
) -> None:
    callback = registration_callback or _registration_callback
    if callback is None or not task_id:
        return
    try:
        callback(
            task_id,
            str(path.resolve()),
            agent_instance_id=agent_id,
            call_id=call_id,
            attempt_id=attempt_id,
            content_sha256=_sha256_file(path),
            size_bytes=path.stat().st_size,
            terminal_evidence=terminal_evidence,
            metadata={
                "kind": kind,
                "managed": True,
                "managed_root": str(_root().resolve()),
                "pinned": bool(pinned),
                "terminal_evidence": bool(terminal_evidence),
            },
        )
    except Exception:
        # Transcript persistence is diagnostic. A repository outage must not
        # turn a completed provider request into a failed request.
        logger.warning(
            "Could not register managed transcript path %s",
            path,
            exc_info=True,
        )


def _timestamp(value: datetime | str | None = None) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    current = value if isinstance(value, datetime) else datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat()


def _redact_transcript_text(value: str, *, max_chars: int) -> str:
    safe = redaction.redact_text(value, max_chars=max_chars)
    return _COOKIE_HEADER.sub(r"\1[REDACTED]", safe)


def record_message(
    *,
    task_id: str | None,
    agent_id: str | None,
    role: str,
    content: str,
    logical_call_id: str | None = None,
    attempt_id: str | None = None,
    provider: str | None = None,
    tool_calls: Sequence[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
    at: datetime | str | None = None,
    terminal_evidence: bool | None = None,
    pinned: bool = False,
    registration_callback: Callable[..., object] | None = None,
) -> Path | None:
    """Append one secret-free provider-neutral message to the local journal."""

    folder = agent_dir(task_id, agent_id)
    if folder is None:
        return None
    safe_content = _redact_transcript_text(content, max_chars=200000)
    safe_tool_calls = redaction.redact_event(list(tool_calls or ()))
    safe_metadata = redaction.redact_event(dict(metadata or {}))
    payload = {
        "schema_version": TRANSCRIPT_SCHEMA_VERSION,
        "timestamp": _timestamp(at),
        "kind": "message",
        "role": str(role),
        "content": safe_content,
        "content_sha256": hashlib.sha256(
            safe_content.encode("utf-8")
        ).hexdigest(),
        "logical_call_id": logical_call_id,
        "attempt_id": attempt_id,
        "provider": provider,
        "tool_calls": safe_tool_calls,
        "metadata": safe_metadata,
    }
    path = folder / TRANSCRIPT_FILENAME
    append_jsonl(path, payload)
    _register_log_path(
        path,
        task_id=task_id,
        agent_id=agent_id,
        call_id=logical_call_id,
        attempt_id=attempt_id,
        kind="provider_transcript",
        terminal_evidence=(
            role == "assistant"
            if terminal_evidence is None
            else bool(terminal_evidence)
        ),
        pinned=pinned,
        registration_callback=registration_callback,
    )
    return path


# More explicit alias for integrations that distinguish chat from debug logs.
record_chat_message = record_message


def load_messages(
    *,
    task_id: str | None,
    agent_id: str | None,
    max_messages: int = 64,
    max_chars: int = 500_000,
) -> tuple[TranscriptMessage, ...]:
    """Load the bounded tail of completed turns, excluding orphaned attempts."""

    if not task_id or not agent_id or max_messages <= 0 or max_chars <= 0:
        return ()
    path = (
        _root()
        / _safe_segment(task_id)
        / _safe_segment(agent_id)
        / TRANSCRIPT_FILENAME
    )
    try:
        with _lock:
            lines = path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, OSError):
        return ()

    pending_users: dict[str, TranscriptMessage] = {}
    turns: list[tuple[TranscriptMessage, TranscriptMessage]] = []
    for line_number, line in enumerate(lines):
        try:
            entry = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(entry, dict) or entry.get("kind") != "message":
            continue
        role = str(entry.get("role") or "")
        if role not in {"user", "assistant"}:
            continue
        content = entry.get("content")
        if not isinstance(content, str):
            continue
        expected_hash = entry.get("content_sha256")
        if expected_hash and expected_hash != hashlib.sha256(
            content.encode("utf-8")
        ).hexdigest():
            continue
        logical_call_id = str(
            entry.get("logical_call_id") or f"legacy:{line_number}"
        )
        raw_tool_calls = entry.get("tool_calls")
        tool_calls = tuple(
            dict(item)
            for item in raw_tool_calls
            if isinstance(item, Mapping)
        ) if isinstance(raw_tool_calls, list) else ()
        metadata = entry.get("metadata")
        message = TranscriptMessage(
            role=role,
            content=content,
            tool_calls=tool_calls,
            metadata=dict(metadata) if isinstance(metadata, Mapping) else {},
        )
        if role == "user":
            pending_users[logical_call_id] = message
            continue
        user = pending_users.pop(logical_call_id, None)
        if user is not None:
            turns.append((user, message))

    selected: list[TranscriptMessage] = []
    selected_chars = 0
    for user, assistant in reversed(turns):
        turn_chars = len(user.content) + len(assistant.content)
        if (
            len(selected) + 2 > max_messages
            or selected_chars + turn_chars > max_chars
        ):
            break
        if turn_chars > max_chars:
            continue
        selected[0:0] = [user, assistant]
        selected_chars += turn_chars
    return tuple(selected)


def record_thinking(
    *,
    task_id: str | None,
    agent_id: str | None,
    role: str | None,
    text: str,
    terminal_evidence: bool = False,
    pinned: bool = False,
    registration_callback: Callable[..., object] | None = None,
) -> None:
    if not text:
        return
    text = redaction.redact_text(text)
    folder = agent_dir(task_id, agent_id)
    if folder is None:
        return
    thinking_path = folder / "thinking.md"
    append_text(thinking_path, text)
    _register_log_path(
        thinking_path,
        task_id=task_id,
        agent_id=agent_id,
        call_id=None,
        attempt_id=None,
        kind="provider_thinking",
        terminal_evidence=terminal_evidence,
        pinned=pinned,
        registration_callback=registration_callback,
    )
    events_path = folder / "events.jsonl"
    append_jsonl(
        events_path,
        {
            "at": datetime.now(timezone.utc).isoformat(),
            "kind": "thinking",
            "role": role,
            "chars": len(text),
        },
    )
    _register_log_path(
        events_path,
        task_id=task_id,
        agent_id=agent_id,
        call_id=None,
        attempt_id=None,
        kind="provider_event",
        terminal_evidence=terminal_evidence,
        pinned=pinned,
        registration_callback=registration_callback,
    )


def record_response(
    *,
    task_id: str | None,
    agent_id: str | None,
    role: str | None,
    account: str | None,
    attempt: int,
    raw_response: str,
    parsed_ok: bool,
    error: str | None = None,
    tool_name: str | None = None,
    provider: str | None = None,
    logical_call_id: str | None = None,
    attempt_id: str | None = None,
    terminal_evidence: bool = True,
    pinned: bool = False,
    registration_callback: Callable[..., object] | None = None,
) -> Path | None:
    folder = agent_dir(task_id, agent_id)
    if folder is None:
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    raw_response = _redact_transcript_text(
        raw_response, max_chars=200000
    )
    safe_account = _redact_transcript_text(
        account or "-", max_chars=500
    )
    safe_error = _redact_transcript_text(error or "-", max_chars=2000)
    status = "ok" if parsed_ok else "parse_error"
    filename = f"{stamp}_attempt{attempt}_{status}.md"
    path = folder / "responses" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        f"# Agent response ({status})",
        f"- at: {datetime.now(timezone.utc).isoformat()}",
        f"- role: {role or '-'}",
        f"- account: {safe_account}",
        f"- provider: {provider or '-'}",
        f"- logical call: {logical_call_id or '-'}",
        f"- attempt id: {attempt_id or '-'}",
        f"- attempt: {attempt}",
        f"- tool: {tool_name or '-'}",
        f"- error: {safe_error}",
        "",
        "## Raw model output",
        "",
        raw_response or "(empty)",
        "",
    ]
    with _lock:
        path.write_text("\n".join(header), encoding="utf-8")
    _register_log_path(
        path,
        task_id=task_id,
        agent_id=agent_id,
        call_id=logical_call_id,
        attempt_id=attempt_id,
        kind="provider_response",
        terminal_evidence=terminal_evidence,
        pinned=pinned,
        registration_callback=registration_callback,
    )
    events_path = folder / "events.jsonl"
    append_jsonl(
        events_path,
        {
            "at": datetime.now(timezone.utc).isoformat(),
            "kind": "response",
            "role": role,
            "account": safe_account,
            "provider": provider,
            "logical_call_id": logical_call_id,
            "attempt_id": attempt_id,
            "attempt": attempt,
            "parsed_ok": parsed_ok,
            "tool_name": tool_name,
            "error": safe_error,
            "file": str(path.as_posix()),
            "chars": len(raw_response or ""),
        },
    )
    _register_log_path(
        events_path,
        task_id=task_id,
        agent_id=agent_id,
        call_id=logical_call_id,
        attempt_id=attempt_id,
        kind="provider_event",
        terminal_evidence=terminal_evidence,
        pinned=pinned,
        registration_callback=registration_callback,
    )
    latest = folder / "latest_response.md"
    with _lock:
        latest.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    _register_log_path(
        latest,
        task_id=task_id,
        agent_id=agent_id,
        call_id=logical_call_id,
        attempt_id=attempt_id,
        kind="provider_latest_response",
        terminal_evidence=terminal_evidence,
        pinned=pinned,
        registration_callback=registration_callback,
    )
    return path
