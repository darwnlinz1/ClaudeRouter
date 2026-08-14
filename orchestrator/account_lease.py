"""Injectable account leasing and health-transition contracts.

All records use scalar values and epoch timestamps so an implementation can
persist them directly in SQLite without importing the orchestrator's domain
models.  The default LLM path keeps using the in-memory CookieManager until a
store is injected.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Collection, Mapping, Protocol, Sequence, runtime_checkable


@dataclass(frozen=True)
class AccountCandidate:
    """A secret-free account descriptor offered to a lease store."""

    account_id: str
    provider: str
    active_requests: int = 0
    cooldown_until: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AccountLease:
    """Portable lease row suitable for durable SQLite storage."""

    lease_id: str
    account_id: str
    provider: str
    owner_id: str
    acquired_at: float
    expires_at: float
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AccountHealthTransition:
    """Append-only health/cooldown transition."""

    account_id: str
    provider: str
    state: str
    reason: str
    occurred_at: float
    cooldown_until: float | None = None
    retry_after_seconds: int | None = None
    logical_call_id: str | None = None
    attempt_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class AccountLeaseStore(Protocol):
    """Cross-process account lease boundary.

    Implementations should make ``acquire`` atomic (for SQLite, an immediate
    transaction plus a uniqueness constraint on active account leases).
    """

    def acquire(
        self,
        *,
        candidates: Sequence[AccountCandidate],
        owner_id: str,
        lease_ttl_seconds: float,
        exclude_account_ids: Collection[str] = (),
        now: float,
    ) -> AccountLease | None:
        """Atomically select and lease one eligible account."""

    def release(
        self,
        lease: AccountLease,
        *,
        outcome: str,
        now: float,
    ) -> None:
        """Release an active lease after one transport attempt."""

    def renew(
        self,
        lease: AccountLease,
        *,
        lease_ttl_seconds: float,
        now: float,
    ) -> AccountLease | None:
        """Extend an owned, unexpired lease or report that ownership was lost."""

    def record_health_transition(
        self,
        transition: AccountHealthTransition,
    ) -> None:
        """Persist an account health or cooldown transition."""


# Reference DDL only; StateRepository remains the owner's integration point.
SQLITE_ACCOUNT_LEASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS llm_account_leases (
    lease_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    acquired_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    released_at REAL,
    outcome TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_llm_account_active_lease
ON llm_account_leases(account_id, provider)
WHERE released_at IS NULL;
CREATE TABLE IF NOT EXISTS llm_account_health_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    cooldown_until REAL,
    retry_after_seconds INTEGER,
    logical_call_id TEXT,
    attempt_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_llm_account_health_lookup
ON llm_account_health_events(provider, account_id, occurred_at);
""".strip()
