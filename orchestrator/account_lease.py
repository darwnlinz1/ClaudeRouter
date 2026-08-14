"""Injectable account leasing and health-transition contracts.

All records use scalar values and epoch timestamps so an implementation can
persist them directly in SQLite without importing the orchestrator's domain
models.  The default LLM path keeps using the in-memory CookieManager until a
store is injected.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Any,
    Collection,
    Iterator,
    Mapping,
    Protocol,
    Sequence,
    TypeAlias,
    overload,
    runtime_checkable,
)

TASK_ACCOUNT_RESERVATION_API_VERSION = 1


@dataclass(frozen=True)
class AccountCandidate:
    """A secret-free account descriptor offered to a lease store."""

    account_id: str
    provider: str
    active_requests: int = 0
    cooldown_until: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    health_state: str | None = None
    enabled: bool = True


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
class AccountReservationAssignment:
    """Candidate pool for one logical agent in an atomic cohort request."""

    agent_id: str
    candidates: Sequence[AccountCandidate]
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AccountReservation:
    """Task-lifetime account ownership fenced by a per-agent generation."""

    reservation_id: str
    task_id: str
    agent_id: str
    account_id: str
    provider: str
    generation: int
    state: str
    reserved_at: float
    expires_at: float
    activated_at: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def fencing_token(self) -> int:
        """Compatibility name for callers that use lease fencing language."""

        return self.generation


@dataclass(frozen=True)
class AccountReservationCohort:
    """Successful all-or-none reservation result.

    ``reserve_many`` returns ``None`` immediately when a complete matching is
    unavailable.  There is deliberately no waiting or queued state.
    """

    task_id: str
    reservations: tuple[AccountReservation, ...]

    def __iter__(self) -> Iterator[AccountReservation]:
        return iter(self.reservations)

    def __len__(self) -> int:
        return len(self.reservations)

    @overload
    def __getitem__(self, key: int) -> AccountReservation: ...

    @overload
    def __getitem__(self, key: str) -> AccountReservation: ...

    def __getitem__(self, key: int | str) -> AccountReservation:
        if isinstance(key, int):
            return self.reservations[key]
        reservation = self.get(key)
        if reservation is None:
            raise KeyError(key)
        return reservation

    def get(self, agent_id: str) -> AccountReservation | None:
        return next(
            (
                reservation
                for reservation in self.reservations
                if reservation.agent_id == agent_id
            ),
            None,
        )

    @property
    def by_agent(self) -> Mapping[str, AccountReservation]:
        return {
            reservation.agent_id: reservation
            for reservation in self.reservations
        }


AccountReservationAssignments: TypeAlias = (
    Mapping[str, Sequence[AccountCandidate]]
    | Sequence[AccountReservationAssignment]
    | Sequence[str]
)

# Short compatibility names for integration code that speaks in assignments or
# task reservations rather than reservation requests.
AccountAssignment = AccountReservationAssignment
TaskAccountReservation = AccountReservation
TaskAccountReservationCohort = AccountReservationCohort


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

    task_reservation_api_version: int

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

    def reserve_many(
        self,
        task_id: str,
        assignments: AccountReservationAssignments | None = None,
        candidates: Sequence[AccountCandidate] | None = None,
        ttl: float | None = None,
        *,
        agent_ids: Sequence[str] | None = None,
        ttl_seconds: float | None = None,
        lease_ttl_seconds: float | None = None,
        now: float | None = None,
    ) -> AccountReservationCohort | None:
        """Reserve a complete unique cohort, or return immediately with none."""

    def get_reservation(
        self,
        task_id: str,
        agent_id: str,
        *,
        now: float | None = None,
    ) -> AccountReservation | None:
        """Return the current unexpired reservation for one logical agent."""

    def consume_reserved_account(
        self,
        task_id: str,
        agent_id: str,
        *,
        provider: str | None = None,
        candidates: Sequence[AccountCandidate] = (),
        candidate_account_ids: Sequence[str] = (),
        exclude_account_ids: Collection[str] = (),
        now: float | None = None,
    ) -> AccountLease | None:
        """Expose reserved ownership through the transport lease shape."""

    def activate(
        self,
        task_id: str,
        agent_id: str,
        generation: int,
        *,
        now: float | None = None,
    ) -> AccountReservation | None:
        """Fence and mark a reserved account active for transport."""

    def replace(
        self,
        task_id: str,
        agent_id: str,
        failed_account: str | AccountCandidate | AccountReservation,
        candidates: Sequence[AccountCandidate],
        ttl: float | None = None,
        *,
        expected_generation: int | None = None,
        health_transition: AccountHealthTransition | None = None,
        ttl_seconds: float | None = None,
        lease_ttl_seconds: float | None = None,
        now: float | None = None,
    ) -> AccountReservation | None:
        """Atomically retire one failed assignment and reserve its replacement."""

    def replace_account_atomically(
        self,
        task_id: str,
        agent_id: str,
        current_account_id: str,
        candidates: Sequence[AccountCandidate],
        *,
        reason: str = "account_failure",
        logical_call_id: str | None = None,
        current_lease: AccountLease | None = None,
        exclude_account_ids: Collection[str] = (),
        lease_ttl_seconds: float | None = None,
        now: float | None = None,
    ) -> AccountLease | None:
        """Runtime adapter for immediate fenced reservation replacement."""

    def renew_task(
        self,
        task_id: str,
        ttl: float | None = None,
        *,
        ttl_seconds: float | None = None,
        lease_ttl_seconds: float | None = None,
        expected_generations: Mapping[str, int] | None = None,
        now: float | None = None,
    ) -> AccountReservationCohort | None:
        """Atomically extend every current reservation owned by a task."""

    def release_agent(
        self,
        task_id: str,
        agent_id: str,
        *,
        generation: int | None = None,
        reason: str = "released",
        now: float | None = None,
    ) -> bool:
        """Release one logical agent's current fenced reservation."""

    def release_task(
        self,
        task_id: str,
        *,
        reason: str = "released",
        now: float | None = None,
    ) -> int:
        """Release every current reservation for a task."""


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
CREATE TABLE IF NOT EXISTS llm_account_reservations (
    reservation_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation >= 1),
    state TEXT NOT NULL CHECK(state IN ('reserved', 'active', 'released')),
    reserved_at REAL NOT NULL,
    activated_at REAL,
    expires_at REAL NOT NULL,
    released_at REAL,
    release_reason TEXT,
    replaced_by_reservation_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(task_id, agent_id, generation)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_llm_account_reservation_owner
ON llm_account_reservations(provider, account_id)
WHERE state IN ('reserved', 'active');
CREATE UNIQUE INDEX IF NOT EXISTS ux_llm_task_agent_reservation
ON llm_account_reservations(task_id, agent_id)
WHERE state IN ('reserved', 'active');
CREATE INDEX IF NOT EXISTS ix_llm_account_reservation_task
ON llm_account_reservations(task_id, state, expires_at, agent_id);
CREATE INDEX IF NOT EXISTS ix_llm_account_reservation_expiry
ON llm_account_reservations(state, expires_at);
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
ON llm_account_health_events(provider, account_id, occurred_at, id);
""".strip()
