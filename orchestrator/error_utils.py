# -*- coding: utf-8 -*-
"""Error normalization + hashing for Rule E's consecutive_error_count.

Design note 7 flags a real failure mode: if last_error_hash is computed
from a raw stack trace, two attempts that fail for the *same underlying
reason* can still produce different hashes (different line numbers, temp
variable names, timestamps, memory addresses, tmp file paths) -- silently
resetting consecutive_error_count and preventing Rule E (strategy reset)
from ever firing even though the agent is genuinely stuck in a loop.

This module normalizes common sources of that noise before hashing, and
also exposes a similarity check as a fallback for cases normalization
alone doesn't catch.
"""

from __future__ import annotations

import difflib
import hashlib
import re

# NOTE: order matters. Full ISO timestamps must be consumed *before* the
# generic ":<digits>" line-number pattern below, or a pattern like
# ":\d+:\d+\b" will greedily eat the "MM:SS" portion of a timestamp and
# leave the (differing) hour untouched, defeating normalization.
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # ISO-ish timestamps and common log timestamp formats -- must run
    # before any colon/digit based pattern below.
    (re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?"), "<TIMESTAMP>"),
    # File:line references, e.g. "foo.py:123" or "at line 42"
    (re.compile(r"(?i)\bline\s+\d+\b"), "line <N>"),
    (re.compile(r":\d+:\d+\b"), ":<L>:<C>"),
    (re.compile(r":\d+\b"), ":<L>"),
    # Memory addresses / object ids, e.g. "0x7f9a3c0021b0"
    (re.compile(r"0x[0-9a-fA-F]{4,}"), "0x<ADDR>"),
    # Unix epoch-looking numbers (10-13 digits) often embedded in tmp names
    (re.compile(r"\b\d{10,13}\b"), "<EPOCH>"),
    # Auto-generated tmp identifiers: tmp_ab12cd, /tmp/xyz-9f8e, etc.
    (re.compile(r"(?i)(tmp|temp)[_\-][a-z0-9]{4,}"), "<TMP_ID>"),
    (re.compile(r"/tmp/[A-Za-z0-9_\-./]+"), "/tmp/<PATH>"),
    # Generic hex ids (git-hash-like, uuid-like fragments) of 6+ chars
    (re.compile(r"\b[0-9a-fA-F]{8,}\b"), "<HEXID>"),
    # Collapse runs of whitespace so formatting-only diffs don't matter
    (re.compile(r"\s+"), " "),
]


def normalize_error(raw: str) -> str:
    """Strip line numbers, addresses, timestamps and similar noise so two
    error messages describing the same root cause normalize to the same
    string."""
    text = raw.strip()
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text.strip().lower()


def error_hash(raw: str) -> str:
    """SHA-256 hex digest of the normalized error text."""
    return hashlib.sha256(normalize_error(raw).encode("utf-8")).hexdigest()


def is_similar_error(a: str, b: str, threshold: float = 0.85) -> bool:
    """Fallback fuzzy check for cases where normalization still leaves a
    residual difference (e.g. a changed variable name that isn't caught by
    the regexes above). Not used for the primary hash comparison, but
    available for a stricter orchestrator that wants to avoid Rule E
    silently never triggering on near-duplicate errors."""
    na, nb = normalize_error(a), normalize_error(b)
    if na == nb:
        return True
    ratio = difflib.SequenceMatcher(None, na, nb).ratio()
    return ratio >= threshold
