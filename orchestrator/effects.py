"""Typed durable side-effect receipts.

The repository owns persistence and transitions; these immutable values make
the outbox state explicit to callers without exposing SQLite rows.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


class EffectState(str, Enum):
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"
    RECONCILED = "reconciled"


class ExpectedBefore(str, Enum):
    """Explicit precondition for a filesystem effect."""

    MUST_BE_ABSENT = "must_be_absent"


MUST_BE_ABSENT = ExpectedBefore.MUST_BE_ABSENT
ExpectedBeforeValue = str | ExpectedBefore


@dataclass(frozen=True)
class PreparedFileEffect:
    """A durable temp file plus the target state required for committing it."""

    target: Path
    temporary_path: Path
    expected_before: ExpectedBeforeValue
    after_sha256: str


@dataclass(frozen=True)
class EffectReceipt:
    effect_id: str
    task_id: str
    idempotency_key: str
    kind: str
    target: str
    state: EffectState
    attempts: int
    payload: Mapping[str, Any] = field(default_factory=dict)
    result: Mapping[str, Any] = field(default_factory=dict)
    before_sha256: str | None = None
    expected_after_sha256: str | None = None
    after_sha256: str | None = None
    fencing_token: int | None = None
    compensates_effect_id: str | None = None
    compensated_by_effect_id: str | None = None
    compensated_at: datetime | None = None
    error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def id(self) -> str:
        """Compatibility alias for code that calls receipts by ``id``."""
        return self.effect_id

    @property
    def expected_before(self) -> ExpectedBeforeValue:
        """Translate the persisted nullable hash into an explicit precondition."""
        return self.before_sha256 or MUST_BE_ABSENT

    @property
    def compensated(self) -> bool:
        """Whether a later durable effect explicitly reversed this receipt."""
        return self.compensated_by_effect_id is not None
