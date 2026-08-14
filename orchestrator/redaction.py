"""Redact sensitive values before durable event/UI persistence."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

_SENSITIVE_KEY_PARTS = (
    "authorization",
    "cookie",
    "password",
    "passwd",
    "secret",
    "session_key",
    "sessionkey",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "private_key",
)
_SENSITIVE_EXACT_KEYS = frozenset(
    {
        "account",
        "credential_source",
        "from_account",
        "org_id",
        "organization_id",
        "organization_uuid",
        "raw_org_id",
        "source_cookie_file",
        "to_account",
    }
)
# Identifiers the orchestrator mints itself. They are long and random enough to
# trip the high-entropy heuristic, and whether they do depends on nothing more
# than the length of the role prefix: "manager_<24 hex>" is 32 characters and is
# redacted, "worker_<24 hex>" is 31 and is not. Losing them is not a privacy win
# and it corrupts the run: every manager collapses into one "[REDACTED]" agent
# and every worker's parent link points at that same placeholder.
_IDENTIFIER_KEYS = frozenset(
    {
        "account_ref",
        "agent_id",
        "agent_ids",
        "agent_instance_id",
        "assignment_id",
        "attempt_id",
        "blocked_by",
        "call_id",
        "called_agent_ids",
        "completed_agent_ids",
        "contract_id",
        "event_id",
        "execution_attempt_id",
        "lease_id",
        "logical_agent_id",
        "logical_call_id",
        "logical_request_id",
        "affected_manager_ids",
        "expected_manager_ids",
        "manager_agent_id",
        "manager_agent_ids",
        "manager_id",
        "reported_manager_ids",
        "parent_agent_instance_id",
        "primary_agent_ids",
        "provider_attempt_id",
        "request_fingerprint",
        "wire_fingerprint",
        "org_ref",
        "session_id",
        "task_id",
        "tester_agent_id",
        "tester_agent_ids",
        "work_item_id",
        "work_item_ids",
        "abandoned_item_ids",
        "completed_item_ids",
        "skipped_item_ids",
        "worker_agent_id",
        "worker_agent_ids",
        "workstream_id",
        "workstream_ids",
        "abandoned_workstream_ids",
        "completed_workstream_ids",
        "skipped_workstream_ids",
    }
)
_PATTERNS = (
    re.compile(r"(?im)^(\s*(?:set-)?cookie\s*:\s*)[^\r\n]+$"),
    re.compile(r"(?i)(sessionKey\s*[=:]\s*)[^\s;,'\"]+"),
    re.compile(r"(?i)(authorization\s*[=:]\s*bearer\s+)[^\s,'\"]+"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{12,}"),
    re.compile(r"\bsk-(?:ant-)?[A-Za-z0-9_-]{12,}\b"),
    re.compile(
        r"(?i)((?:api[_-]?key|password|secret)\s*[=:]\s*)"
        r"([^\s,;'\"}]{6,})"
    ),
    re.compile(
        r"(?i)https?://[^\s<>'\"]+[?&](?:x-amz-signature|signature|sig|"
        r"token|access_token|x-goog-signature)=[^\s<>'\"]+"
    ),
)
_SCANNER_PATTERNS: tuple[tuple[str, re.Pattern[str], int], ...] = (
    (
        "private_key",
        re.compile(
            r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"
            r"[\s\S]*?-----END (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"
        ),
        0,
    ),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"), 0),
    (
        "github_token",
        re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,255}\b"),
        0,
    ),
    (
        "jwt",
        re.compile(
            r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
            r"\.[A-Za-z0-9_-]{8,}\b"
        ),
        0,
    ),
    ("api_token", re.compile(r"\bsk-(?:ant-)?[A-Za-z0-9_-]{12,}\b"), 0),
    (
        "assigned_secret",
        re.compile(
            r"(?i)(?:api[_-]?key|access[_-]?token|refresh[_-]?token|"
            r"password|passwd|secret|session[_-]?key)"
            r"\s*[=:]\s*[\"']?([^\s,;'\"}]{8,})"
        ),
        1,
    ),
    (
        "authorization",
        re.compile(r"(?i)authorization\s*[=:]\s*bearer\s+([^\s,'\"]{12,})"),
        1,
    ),
)
_HIGH_ENTROPY_TOKEN = re.compile(r"(?<![A-Za-z0-9])[A-Za-z0-9_+/=-]{32,}(?![A-Za-z0-9])")


@dataclass(frozen=True, slots=True)
class SecretFinding:
    kind: str
    start: int
    end: int
    confidence: str


def _entropy(value: str) -> float:
    if not value:
        return 0.0
    counts = {character: value.count(character) for character in set(value)}
    length = len(value)
    return -sum(
        (count / length) * math.log2(count / length)
        for count in counts.values()
    )


def _looks_like_secret_token(value: str) -> bool:
    character_classes = sum(
        bool(pattern.search(value))
        for pattern in (
            re.compile(r"[a-z]"),
            re.compile(r"[A-Z]"),
            re.compile(r"[0-9]"),
            re.compile(r"[_+/=-]"),
        )
    )
    return character_classes >= 3 and _entropy(value) >= 4.0


def scan_secrets(
    value: str | bytes,
    *,
    candidate_type: str = "text",
) -> tuple[SecretFinding, ...]:
    """Find credentials by content in prompts and artifact candidates."""
    del candidate_type  # Reserved for future policy tuning; scanning is uniform.
    if isinstance(value, bytes):
        if b"\x00" in value:
            return ()
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value)
    findings: list[SecretFinding] = []
    occupied: list[tuple[int, int]] = []
    for kind, pattern, secret_group in _SCANNER_PATTERNS:
        for match in pattern.finditer(text):
            start, end = match.span(secret_group)
            if any(start < existing_end and end > existing_start for existing_start, existing_end in occupied):
                continue
            findings.append(SecretFinding(kind, start, end, "high"))
            occupied.append((start, end))
    for match in _HIGH_ENTROPY_TOKEN.finditer(text):
        start, end = match.span()
        if any(start < existing_end and end > existing_start for existing_start, existing_end in occupied):
            continue
        if _looks_like_secret_token(match.group(0)):
            findings.append(SecretFinding("high_entropy_token", start, end, "medium"))
            occupied.append((start, end))
    return tuple(sorted(findings, key=lambda finding: (finding.start, finding.end, finding.kind)))


def redact_candidate(
    value: str | bytes,
    *,
    candidate_type: str = "text",
    replacement: str = "[REDACTED]",
) -> str:
    text = (
        value.decode("utf-8", errors="replace")
        if isinstance(value, bytes)
        else str(value)
    )
    findings = scan_secrets(text, candidate_type=candidate_type)
    for finding in reversed(findings):
        text = text[: finding.start] + replacement + text[finding.end :]
    return text


def redact_text(value: str, *, max_chars: int | None = 60000) -> str:
    text = redact_candidate(value)
    for pattern in _PATTERNS:
        if "https?" in pattern.pattern:
            text = pattern.sub("[REDACTED_SIGNED_URL]", text)
        elif pattern.groups >= 1:
            text = pattern.sub(lambda match: match.group(1) + "[REDACTED]", text)
        else:
            text = pattern.sub("[REDACTED]", text)
    if max_chars is not None and len(text) > max_chars:
        text = text[:max_chars] + f"\n[TRUNCATED {len(text) - max_chars} CHARS]"
    return text


def redact_event(value: Any, *, key: str = "") -> Any:
    lowered = key.casefold()
    if lowered in _IDENTIFIER_KEYS and isinstance(value, str):
        return value
    if lowered in _SENSITIVE_EXACT_KEYS:
        return "[REDACTED]"
    if any(part in lowered for part in _SENSITIVE_KEY_PARTS):
        return "[REDACTED]"
    if isinstance(value, str):
        max_chars = 40000 if lowered in {"prompt", "diff", "patch"} else 60000
        return redact_text(value, max_chars=max_chars)
    if isinstance(value, dict):
        return {
            str(child_key): redact_event(child_value, key=str(child_key))
            for child_key, child_value in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_event(item, key=key) for item in value[:200]]
    return value
