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
_PATTERNS = (
    re.compile(r"(?i)(sessionKey\s*[=:]\s*)[^\s;,'\"]+"),
    re.compile(r"(?i)(authorization\s*[=:]\s*bearer\s+)[^\s,'\"]+"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{12,}"),
    re.compile(r"\bsk-(?:ant-)?[A-Za-z0-9_-]{12,}\b"),
    re.compile(
        r"(?i)((?:api[_-]?key|password|secret)\s*[=:]\s*)"
        r"([^\s,;'\"}]{6,})"
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


def redact_text(value: str, *, max_chars: int = 60000) -> str:
    text = redact_candidate(value)
    for pattern in _PATTERNS:
        if pattern.groups >= 2 or pattern.groups == 1:
            text = pattern.sub(lambda match: match.group(1) + "[REDACTED]", text)
        else:
            text = pattern.sub("[REDACTED]", text)
    if len(text) > max_chars:
        text = text[:max_chars] + f"\n[TRUNCATED {len(text) - max_chars} CHARS]"
    return text


def redact_event(value: Any, *, key: str = "") -> Any:
    lowered = key.casefold()
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
