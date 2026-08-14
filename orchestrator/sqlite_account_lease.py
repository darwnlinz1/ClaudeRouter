"""SQLite-backed cross-thread/process LLM account leases and health history."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Collection, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, cast

from .account_lease import (
    SQLITE_ACCOUNT_LEASE_SCHEMA,
    TASK_ACCOUNT_RESERVATION_API_VERSION,
    AccountCandidate,
    AccountHealthTransition,
    AccountLease,
    AccountReservation,
    AccountReservationAssignment,
    AccountReservationAssignments,
    AccountReservationCohort,
)

DEFAULT_ACCOUNT_DB = (
    Path.home() / ".ai_orchestrator" / "account_leases.sqlite3"
)
_OWNED_RESERVATION_STATES = ("reserved", "active")
_BLOCKED_HEALTH_STATES = frozenset(
    {"disabled", "invalid", "quarantined", "revoked"}
)


class SQLiteAccountLeaseStore:
    """Durable account coordination without persisting cookie secrets."""

    task_reservation_api_version = TASK_ACCOUNT_RESERVATION_API_VERSION

    def __init__(self, database: str | Path = DEFAULT_ACCOUNT_DB) -> None:
        self.database = str(database)
        if self.database != ":memory:":
            Path(self.database).expanduser().parent.mkdir(
                parents=True, exist_ok=True
            )
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.database,
            check_same_thread=False,
            isolation_level=None,
            timeout=30,
        )
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA busy_timeout = 30000")
            if self.database != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.executescript(SQLITE_ACCOUNT_LEASE_SCHEMA)
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_llm_account_active_expiry
                ON llm_account_leases(provider, expires_at)
                WHERE released_at IS NULL
                """
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "SQLiteAccountLeaseStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    @staticmethod
    def _clock(now: float | None) -> float:
        return time.time() if now is None else float(now)

    @staticmethod
    def _resolve_ttl(
        ttl: float | None,
        ttl_seconds: float | None = None,
        lease_ttl_seconds: float | None = None,
        *,
        required: bool = True,
    ) -> float | None:
        values = [
            float(value)
            for value in (ttl, ttl_seconds, lease_ttl_seconds)
            if value is not None
        ]
        if not values:
            if required:
                raise ValueError("ttl must be provided")
            return None
        if any(value <= 0 for value in values):
            raise ValueError("ttl must be positive")
        if any(value != values[0] for value in values[1:]):
            raise ValueError("ttl values disagree")
        return values[0]

    @staticmethod
    def _expire_owned(connection: sqlite3.Connection, now: float) -> None:
        connection.execute(
            """
            UPDATE llm_account_leases
            SET released_at = ?, outcome = 'expired'
            WHERE released_at IS NULL AND expires_at <= ?
            """,
            (now, now),
        )
        connection.execute(
            """
            UPDATE llm_account_reservations
            SET state = 'released', released_at = ?,
                release_reason = COALESCE(release_reason, 'expired')
            WHERE state IN ('reserved', 'active') AND expires_at <= ?
            """,
            (now, now),
        )

    @staticmethod
    def _candidate_key(candidate: AccountCandidate) -> tuple[str, str]:
        return (str(candidate.provider), str(candidate.account_id))

    @classmethod
    def _candidate_locally_eligible(
        cls,
        candidate: AccountCandidate,
        now: float,
    ) -> bool:
        if not str(candidate.account_id).strip() or not str(candidate.provider).strip():
            raise ValueError("candidate account_id and provider are required")
        metadata = dict(candidate.metadata)
        metadata_state = str(
            metadata.get("health_state") or metadata.get("state") or ""
        ).lower()
        state = str(candidate.health_state or metadata_state).lower()
        disabled = (
            not candidate.enabled
            or bool(metadata.get("disabled"))
            or bool(metadata.get("quarantined"))
        )
        return (
            not disabled
            and state not in _BLOCKED_HEALTH_STATES
            and (
                candidate.cooldown_until is None
                or float(candidate.cooldown_until) <= now
            )
        )

    @staticmethod
    def _current_health(
        connection: sqlite3.Connection,
        provider: str,
        account_id: str,
    ) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            connection.execute(
                """
                SELECT state, cooldown_until
                FROM llm_account_health_events
                WHERE provider = ? AND account_id = ?
                ORDER BY occurred_at DESC, id DESC
                LIMIT 1
                """,
                (provider, account_id),
            ).fetchone(),
        )

    @classmethod
    def _eligible_candidates(
        cls,
        connection: sqlite3.Connection,
        candidates: Sequence[AccountCandidate],
        now: float,
        *,
        excluded: Collection[tuple[str, str]] = (),
    ) -> list[AccountCandidate]:
        excluded_keys = set(excluded)
        unique: dict[tuple[str, str], AccountCandidate] = {}
        for candidate in candidates:
            key = cls._candidate_key(candidate)
            if key in excluded_keys or not cls._candidate_locally_eligible(
                candidate,
                now,
            ):
                continue
            health = cls._current_health(connection, key[0], key[1])
            if health is not None:
                state = str(health["state"]).lower()
                cooldown_until = health["cooldown_until"]
                if state in _BLOCKED_HEALTH_STATES:
                    continue
                if cooldown_until is not None and float(cooldown_until) > now:
                    continue
                if state == "cooldown" and cooldown_until is None:
                    continue
            incumbent = unique.get(key)
            if incumbent is None or (
                candidate.active_requests,
                candidate.account_id,
            ) < (
                incumbent.active_requests,
                incumbent.account_id,
            ):
                unique[key] = candidate
        return sorted(
            unique.values(),
            key=lambda value: (
                value.active_requests,
                value.provider,
                value.account_id,
            ),
        )

    @staticmethod
    def _occupied_accounts(
        connection: sqlite3.Connection,
    ) -> set[tuple[str, str]]:
        lease_rows = connection.execute(
            """
            SELECT provider, account_id FROM llm_account_leases
            WHERE released_at IS NULL
            """
        ).fetchall()
        reservation_rows = connection.execute(
            """
            SELECT provider, account_id FROM llm_account_reservations
            WHERE state IN ('reserved', 'active')
            """
        ).fetchall()
        return {
            (str(row["provider"]), str(row["account_id"]))
            for row in (*lease_rows, *reservation_rows)
        }

    @staticmethod
    def _reservation_from_row(row: sqlite3.Row) -> AccountReservation:
        return AccountReservation(
            reservation_id=str(row["reservation_id"]),
            task_id=str(row["task_id"]),
            agent_id=str(row["agent_id"]),
            account_id=str(row["account_id"]),
            provider=str(row["provider"]),
            generation=int(row["generation"]),
            state=str(row["state"]),
            reserved_at=float(row["reserved_at"]),
            expires_at=float(row["expires_at"]),
            activated_at=(
                float(row["activated_at"])
                if row["activated_at"] is not None
                else None
            ),
            metadata=json.loads(str(row["metadata_json"] or "{}")),
        )

    @staticmethod
    def _lease_for_reservation(
        reservation: AccountReservation,
    ) -> AccountLease:
        """Expose a task reservation through the legacy transport lease shape."""

        return AccountLease(
            lease_id=reservation.reservation_id,
            account_id=reservation.account_id,
            provider=reservation.provider,
            owner_id=reservation.agent_id,
            acquired_at=reservation.reserved_at,
            expires_at=reservation.expires_at,
            metadata={
                **dict(reservation.metadata),
                "task_id": reservation.task_id,
                "reservation_generation": reservation.generation,
                "task_reservation": True,
            },
        )

    @staticmethod
    def _insert_health_transition(
        connection: sqlite3.Connection,
        transition: AccountHealthTransition,
    ) -> None:
        connection.execute(
            """
            INSERT INTO llm_account_health_events(
                account_id, provider, state, reason, occurred_at,
                cooldown_until, retry_after_seconds, logical_call_id,
                attempt_id, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transition.account_id,
                transition.provider,
                transition.state,
                transition.reason,
                transition.occurred_at,
                transition.cooldown_until,
                transition.retry_after_seconds,
                transition.logical_call_id,
                transition.attempt_id,
                json.dumps(
                    dict(transition.metadata),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ),
        )

    def acquire(
        self,
        *,
        candidates: Sequence[AccountCandidate],
        owner_id: str,
        lease_ttl_seconds: float,
        exclude_account_ids: Collection[str] = (),
        now: float,
    ) -> AccountLease | None:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        excluded = {str(value) for value in exclude_account_ids}
        with self._immediate() as connection:
            self._expire_owned(connection, now)
            ranked = self._eligible_candidates(
                connection,
                candidates,
                now,
            )
            occupied = self._occupied_accounts(connection)
            selected = next(
                (
                    candidate
                    for candidate in ranked
                    if candidate.account_id not in excluded
                    and self._candidate_key(candidate) not in occupied
                ),
                None,
            )
            if selected is None:
                return None

            lease = AccountLease(
                lease_id=f"acctlease_{uuid.uuid4().hex}",
                account_id=selected.account_id,
                provider=selected.provider,
                owner_id=owner_id,
                acquired_at=now,
                expires_at=now + lease_ttl_seconds,
                metadata=dict(selected.metadata),
            )
            connection.execute(
                """
                INSERT INTO llm_account_leases(
                    lease_id, account_id, provider, owner_id,
                    acquired_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    lease.lease_id,
                    lease.account_id,
                    lease.provider,
                    lease.owner_id,
                    lease.acquired_at,
                    lease.expires_at,
                ),
            )
            return lease

    @staticmethod
    def _normalize_assignments(
        assignments: AccountReservationAssignments | None,
        candidates: Sequence[AccountCandidate] | None,
        agent_ids: Sequence[str] | None,
    ) -> list[
        tuple[str, tuple[AccountCandidate, ...], Mapping[str, Any]]
    ]:
        if assignments is not None and agent_ids is not None:
            raise ValueError("provide assignments or agent_ids, not both")
        source: Any = assignments if assignments is not None else agent_ids
        if source is None:
            raise ValueError("assignments are required")

        normalized: list[
            tuple[str, tuple[AccountCandidate, ...], Mapping[str, Any]]
        ] = []
        if isinstance(source, Mapping):
            if candidates is not None:
                raise ValueError(
                    "shared candidates cannot accompany assignment candidate pools"
                )
            for raw_agent_id, raw_candidates in source.items():
                candidate_values = (
                    (raw_candidates,)
                    if isinstance(raw_candidates, AccountCandidate)
                    else tuple(raw_candidates)
                )
                normalized.append(
                    (str(raw_agent_id), candidate_values, {})
                )
        else:
            values = tuple(source)
            if all(
                isinstance(value, AccountReservationAssignment)
                for value in values
            ):
                if candidates is not None:
                    raise ValueError(
                        "shared candidates cannot accompany assignment requests"
                    )
                normalized.extend(
                    (
                        value.agent_id,
                        tuple(value.candidates),
                        dict(value.metadata),
                    )
                    for value in values
                    if isinstance(value, AccountReservationAssignment)
                )
            elif all(isinstance(value, Mapping) for value in values):
                if candidates is not None:
                    raise ValueError(
                        "shared candidates cannot accompany assignment mappings"
                    )
                for value in values:
                    assert isinstance(value, Mapping)
                    raw_agent_id = value.get("agent_id")
                    raw_candidates = value.get("candidates", ())
                    normalized.append(
                        (
                            str(raw_agent_id or ""),
                            tuple(raw_candidates),
                            dict(value.get("metadata") or {}),
                        )
                    )
            else:
                if candidates is None:
                    raise ValueError(
                        "shared candidates are required for agent ID assignments"
                    )
                normalized.extend(
                    (str(value), tuple(candidates), {}) for value in values
                )

        seen: set[str] = set()
        for agent_id, candidate_pool, _ in normalized:
            if not agent_id.strip():
                raise ValueError("agent_id must be a non-empty string")
            if agent_id in seen:
                raise ValueError(f"duplicate agent assignment: {agent_id}")
            seen.add(agent_id)
            if not all(
                isinstance(candidate, AccountCandidate)
                for candidate in candidate_pool
            ):
                raise TypeError("assignment candidates must be AccountCandidate values")
        return normalized

    @staticmethod
    def _match_candidates(
        pools: Mapping[str, Sequence[AccountCandidate]],
    ) -> dict[str, AccountCandidate] | None:
        """Find a complete deterministic bipartite matching."""

        owner_by_account: dict[tuple[str, str], str] = {}
        selected_by_agent: dict[str, AccountCandidate] = {}

        def augment(agent_id: str, seen: set[tuple[str, str]]) -> bool:
            for candidate in pools[agent_id]:
                key = (candidate.provider, candidate.account_id)
                if key in seen:
                    continue
                seen.add(key)
                prior = owner_by_account.get(key)
                if prior is None or augment(prior, seen):
                    owner_by_account[key] = agent_id
                    selected_by_agent[agent_id] = candidate
                    return True
            return False

        for agent_id in sorted(
            pools,
            key=lambda value: (len(pools[value]), value),
        ):
            if not augment(agent_id, set()):
                return None
        return selected_by_agent

    @staticmethod
    def _next_generation(
        connection: sqlite3.Connection,
        task_id: str,
        agent_id: str,
    ) -> int:
        row = connection.execute(
            """
            SELECT MAX(generation) AS generation
            FROM llm_account_reservations
            WHERE task_id = ? AND agent_id = ?
            """,
            (task_id, agent_id),
        ).fetchone()
        return int(row["generation"] or 0) + 1

    @classmethod
    def _insert_reservation(
        cls,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        agent_id: str,
        candidate: AccountCandidate,
        assignment_metadata: Mapping[str, Any],
        reserved_at: float,
        expires_at: float,
    ) -> AccountReservation:
        generation = cls._next_generation(connection, task_id, agent_id)
        reservation_id = f"acctres_{uuid.uuid4().hex}"
        metadata = dict(candidate.metadata)
        if assignment_metadata:
            metadata["assignment"] = dict(assignment_metadata)
        connection.execute(
            """
            INSERT INTO llm_account_reservations(
                reservation_id, task_id, agent_id, account_id, provider,
                generation, state, reserved_at, expires_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, 'reserved', ?, ?, ?)
            """,
            (
                reservation_id,
                task_id,
                agent_id,
                candidate.account_id,
                candidate.provider,
                generation,
                reserved_at,
                expires_at,
                json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            ),
        )
        row = connection.execute(
            "SELECT * FROM llm_account_reservations WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        assert row is not None
        return cls._reservation_from_row(row)

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
        """Atomically reserve a unique account for every requested agent.

        Capacity failure commits only stale-expiry cleanup and returns ``None``;
        no reservation from the failed call is retained.
        """

        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("task_id must be a non-empty string")
        duration = self._resolve_ttl(ttl, ttl_seconds, lease_ttl_seconds)
        assert duration is not None
        current = self._clock(now)
        requested = self._normalize_assignments(
            assignments,
            candidates,
            agent_ids,
        )
        requested_order = [value[0] for value in requested]
        expires_at = current + duration

        with self._immediate() as connection:
            self._expire_owned(connection, current)
            current_by_agent: dict[str, AccountReservation] = {}
            if requested_order:
                placeholders = ",".join("?" for _ in requested_order)
                rows = connection.execute(
                    f"""
                    SELECT * FROM llm_account_reservations
                    WHERE task_id = ?
                      AND agent_id IN ({placeholders})
                      AND state IN ('reserved', 'active')
                    """,
                    (task_id, *requested_order),
                ).fetchall()
                current_by_agent = {
                    str(row["agent_id"]): self._reservation_from_row(row)
                    for row in rows
                }

            occupied = self._occupied_accounts(connection)
            pools: dict[str, list[AccountCandidate]] = {}
            metadata_by_agent: dict[str, Mapping[str, Any]] = {}
            for agent_id, candidate_pool, assignment_metadata in requested:
                metadata_by_agent[agent_id] = assignment_metadata
                if agent_id in current_by_agent:
                    continue
                pools[agent_id] = [
                    candidate
                    for candidate in self._eligible_candidates(
                        connection,
                        candidate_pool,
                        current,
                    )
                    if self._candidate_key(candidate) not in occupied
                ]

            selected = self._match_candidates(pools)
            if selected is None:
                return None

            reservations = dict(current_by_agent)
            for agent_id, candidate in selected.items():
                reservations[agent_id] = self._insert_reservation(
                    connection,
                    task_id=task_id,
                    agent_id=agent_id,
                    candidate=candidate,
                    assignment_metadata=metadata_by_agent[agent_id],
                    reserved_at=current,
                    expires_at=expires_at,
                )
            for agent_id, reservation in tuple(reservations.items()):
                if reservation.expires_at >= expires_at:
                    continue
                connection.execute(
                    """
                    UPDATE llm_account_reservations SET expires_at = ?
                    WHERE reservation_id = ?
                      AND state IN ('reserved', 'active')
                    """,
                    (expires_at, reservation.reservation_id),
                )
                row = connection.execute(
                    """
                    SELECT * FROM llm_account_reservations
                    WHERE reservation_id = ?
                    """,
                    (reservation.reservation_id,),
                ).fetchone()
                assert row is not None
                reservations[agent_id] = self._reservation_from_row(row)
            return AccountReservationCohort(
                task_id=task_id,
                reservations=tuple(
                    reservations[agent_id] for agent_id in requested_order
                ),
            )

    def get_reservation(
        self,
        task_id: str,
        agent_id: str,
        *,
        now: float | None = None,
    ) -> AccountReservation | None:
        current = self._clock(now)
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM llm_account_reservations
                WHERE task_id = ? AND agent_id = ?
                  AND state IN ('reserved', 'active') AND expires_at > ?
                """,
                (task_id, agent_id, current),
            ).fetchone()
        return self._reservation_from_row(row) if row is not None else None

    def list_task_reservations(
        self,
        task_id: str,
        *,
        now: float | None = None,
        include_released: bool = False,
    ) -> list[AccountReservation]:
        query = "SELECT * FROM llm_account_reservations WHERE task_id = ?"
        parameters: list[object] = [task_id]
        if not include_released:
            query += " AND state IN ('reserved', 'active') AND expires_at > ?"
            parameters.append(self._clock(now))
        query += " ORDER BY agent_id, generation"
        with self._lock:
            rows = self._connection.execute(query, tuple(parameters)).fetchall()
        return [self._reservation_from_row(row) for row in rows]

    def activate(
        self,
        task_id: str,
        agent_id: str,
        generation: int,
        *,
        now: float | None = None,
    ) -> AccountReservation | None:
        current = self._clock(now)
        with self._immediate() as connection:
            self._expire_owned(connection, current)
            cursor = connection.execute(
                """
                UPDATE llm_account_reservations
                SET state = 'active', activated_at = COALESCE(activated_at, ?)
                WHERE task_id = ? AND agent_id = ? AND generation = ?
                  AND state IN ('reserved', 'active') AND expires_at > ?
                """,
                (current, task_id, agent_id, generation, current),
            )
            if cursor.rowcount != 1:
                return None
            row = connection.execute(
                """
                SELECT * FROM llm_account_reservations
                WHERE task_id = ? AND agent_id = ? AND generation = ?
                """,
                (task_id, agent_id, generation),
            ).fetchone()
            assert row is not None
            return self._reservation_from_row(row)

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
        """Return a transport-compatible view without releasing task ownership."""

        reservation = self.get_reservation(task_id, agent_id, now=now)
        if reservation is None:
            return None
        allowed_ids = {
            *(str(value) for value in candidate_account_ids),
            *(candidate.account_id for candidate in candidates),
        }
        if (
            (provider is not None and reservation.provider != provider)
            or (
                allowed_ids
                and reservation.account_id not in allowed_ids
            )
            or reservation.account_id
            in {str(value) for value in exclude_account_ids}
        ):
            return None
        activated = self.activate(
            task_id,
            agent_id,
            reservation.generation,
            now=now,
        )
        return (
            self._lease_for_reservation(activated)
            if activated is not None
            else None
        )

    consume_pre_reserved_account = consume_reserved_account
    consume_account_reservation = consume_reserved_account
    consume_reservation = consume_reserved_account

    def release(
        self,
        lease: AccountLease,
        *,
        outcome: str,
        now: float,
    ) -> None:
        with self._lock:
            reservation = self._connection.execute(
                """
                SELECT 1 FROM llm_account_reservations
                WHERE reservation_id = ? AND account_id = ?
                  AND provider = ? AND agent_id = ?
                """,
                (
                    lease.lease_id,
                    lease.account_id,
                    lease.provider,
                    lease.owner_id,
                ),
            ).fetchone()
            if reservation is not None:
                # A transport attempt must not release task-lifetime ownership.
                return
            self._connection.execute(
                """
                UPDATE llm_account_leases
                SET released_at = ?, outcome = ?
                WHERE lease_id = ? AND account_id = ? AND provider = ?
                  AND owner_id = ? AND released_at IS NULL
                """,
                (
                    now,
                    outcome,
                    lease.lease_id,
                    lease.account_id,
                    lease.provider,
                    lease.owner_id,
                ),
            )

    def renew(
        self,
        lease: AccountLease,
        *,
        lease_ttl_seconds: float,
        now: float,
    ) -> AccountLease | None:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        expires_at = now + lease_ttl_seconds
        with self._immediate() as connection:
            self._expire_owned(connection, now)
            reservation_row = connection.execute(
                """
                SELECT * FROM llm_account_reservations
                WHERE reservation_id = ? AND account_id = ?
                  AND provider = ? AND agent_id = ?
                  AND state IN ('reserved', 'active') AND expires_at > ?
                """,
                (
                    lease.lease_id,
                    lease.account_id,
                    lease.provider,
                    lease.owner_id,
                    now,
                ),
            ).fetchone()
            if reservation_row is not None:
                expected_generation = lease.metadata.get(
                    "reservation_generation"
                )
                if (
                    expected_generation is not None
                    and int(expected_generation)
                    != int(reservation_row["generation"])
                ):
                    return None
                connection.execute(
                    """
                    UPDATE llm_account_reservations SET expires_at = ?
                    WHERE reservation_id = ?
                    """,
                    (expires_at, lease.lease_id),
                )
                renewed_row = connection.execute(
                    """
                    SELECT * FROM llm_account_reservations
                    WHERE reservation_id = ?
                    """,
                    (lease.lease_id,),
                ).fetchone()
                assert renewed_row is not None
                return self._lease_for_reservation(
                    self._reservation_from_row(renewed_row)
                )
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE llm_account_leases
                SET expires_at = ?
                WHERE lease_id = ? AND account_id = ? AND provider = ?
                  AND owner_id = ? AND released_at IS NULL
                  AND expires_at > ?
                """,
                (
                    expires_at,
                    lease.lease_id,
                    lease.account_id,
                    lease.provider,
                    lease.owner_id,
                    now,
                ),
            )
        if cursor.rowcount != 1:
            return None
        return AccountLease(
            lease_id=lease.lease_id,
            account_id=lease.account_id,
            provider=lease.provider,
            owner_id=lease.owner_id,
            acquired_at=lease.acquired_at,
            expires_at=expires_at,
            metadata=dict(lease.metadata),
        )

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
        """Atomically retire a failed reservation and claim one replacement."""

        current = self._clock(now)
        duration = self._resolve_ttl(
            ttl,
            ttl_seconds,
            lease_ttl_seconds,
            required=False,
        )
        failed_provider: str | None = None
        if isinstance(failed_account, AccountReservation):
            failed_account_id = failed_account.account_id
            failed_provider = failed_account.provider
            if expected_generation is None:
                expected_generation = failed_account.generation
        elif isinstance(failed_account, AccountCandidate):
            failed_account_id = failed_account.account_id
            failed_provider = failed_account.provider
        else:
            failed_account_id = str(failed_account)
        if not failed_account_id.strip():
            raise ValueError("failed_account must identify an account")

        with self._immediate() as connection:
            self._expire_owned(connection, current)
            row = connection.execute(
                """
                SELECT * FROM llm_account_reservations
                WHERE task_id = ? AND agent_id = ?
                  AND state IN ('reserved', 'active')
                """,
                (task_id, agent_id),
            ).fetchone()
            if row is None:
                return None
            old = self._reservation_from_row(row)
            if (
                old.account_id != failed_account_id
                or (
                    failed_provider is not None
                    and old.provider != failed_provider
                )
                or (
                    expected_generation is not None
                    and old.generation != expected_generation
                )
            ):
                return None
            if health_transition is not None:
                if (
                    health_transition.account_id != old.account_id
                    or health_transition.provider != old.provider
                ):
                    raise ValueError(
                        "health transition must describe the failed reservation"
                    )
                self._insert_health_transition(connection, health_transition)

            failed_key = (old.provider, old.account_id)
            occupied = self._occupied_accounts(connection)
            occupied.discard(failed_key)
            ranked = [
                candidate
                for candidate in self._eligible_candidates(
                    connection,
                    candidates,
                    current,
                    excluded=(failed_key,),
                )
                if self._candidate_key(candidate) not in occupied
            ]
            selected = ranked[0] if ranked else None
            connection.execute(
                """
                UPDATE llm_account_reservations
                SET state = 'released', released_at = ?,
                    release_reason = 'failed_account'
                WHERE reservation_id = ?
                  AND state IN ('reserved', 'active')
                """,
                (current, old.reservation_id),
            )
            if selected is None:
                return None

            expires_at = (
                current + duration
                if duration is not None
                else old.expires_at
            )
            replacement = self._insert_reservation(
                connection,
                task_id=task_id,
                agent_id=agent_id,
                candidate=selected,
                assignment_metadata={},
                reserved_at=current,
                expires_at=expires_at,
            )
            connection.execute(
                """
                UPDATE llm_account_reservations
                SET replaced_by_reservation_id = ?
                WHERE reservation_id = ?
                """,
                (replacement.reservation_id, old.reservation_id),
            )
            return replacement

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
        """LLM-runtime adapter for fenced replacement without cookie storage."""

        current = self._clock(now)
        reservation = self.get_reservation(task_id, agent_id, now=current)
        if (
            reservation is None
            or reservation.account_id != current_account_id
        ):
            return None
        if current_lease is not None:
            generation = current_lease.metadata.get(
                "reservation_generation"
            )
            if (
                generation is not None
                and int(generation) != reservation.generation
            ):
                return None
        excluded = {str(value) for value in exclude_account_ids}
        eligible = tuple(
            candidate
            for candidate in candidates
            if candidate.account_id not in excluded
        )
        normalized_reason = str(reason).lower()
        if any(
            token in normalized_reason
            for token in ("auth", "invalid", "permission", "disabled")
        ):
            health_state = (
                "disabled"
                if "disabled" in normalized_reason
                else "quarantined"
            )
            cooldown_until = None
        elif "rate" in normalized_reason or "429" in normalized_reason:
            health_state = "cooldown"
            cooldown_until = current + float(lease_ttl_seconds or 60.0)
        else:
            health_state = "degraded"
            cooldown_until = None
        transition = AccountHealthTransition(
            account_id=reservation.account_id,
            provider=reservation.provider,
            state=health_state,
            reason=normalized_reason or "account_failure",
            occurred_at=current,
            cooldown_until=cooldown_until,
            logical_call_id=logical_call_id,
            metadata={"source": "atomic_replacement"},
        )
        replacement = self.replace(
            task_id,
            agent_id,
            reservation,
            eligible,
            lease_ttl_seconds=lease_ttl_seconds,
            health_transition=transition,
            now=current,
        )
        return (
            self._lease_for_reservation(replacement)
            if replacement is not None
            else None
        )

    replace_reserved_account = replace_account_atomically
    acquire_replacement_account = replace_account_atomically
    replace_account = replace_account_atomically

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
        """Extend all current task reservations in one transaction."""

        duration = self._resolve_ttl(
            ttl,
            ttl_seconds,
            lease_ttl_seconds,
        )
        assert duration is not None
        current = self._clock(now)
        expires_at = current + duration
        with self._immediate() as connection:
            self._expire_owned(connection, current)
            rows = connection.execute(
                """
                SELECT * FROM llm_account_reservations
                WHERE task_id = ? AND state IN ('reserved', 'active')
                ORDER BY agent_id
                """,
                (task_id,),
            ).fetchall()
            if not rows:
                return None
            generations = {
                str(row["agent_id"]): int(row["generation"]) for row in rows
            }
            if expected_generations is not None:
                expected = {
                    str(agent_id): int(generation)
                    for agent_id, generation in expected_generations.items()
                }
                if expected != generations:
                    return None
            connection.execute(
                """
                UPDATE llm_account_reservations SET expires_at = ?
                WHERE task_id = ? AND state IN ('reserved', 'active')
                """,
                (expires_at, task_id),
            )
            renewed_rows = connection.execute(
                """
                SELECT * FROM llm_account_reservations
                WHERE task_id = ? AND state IN ('reserved', 'active')
                ORDER BY agent_id
                """,
                (task_id,),
            ).fetchall()
            return AccountReservationCohort(
                task_id=task_id,
                reservations=tuple(
                    self._reservation_from_row(row) for row in renewed_rows
                ),
            )

    def release_agent(
        self,
        task_id: str,
        agent_id: str,
        *,
        generation: int | None = None,
        reason: str = "released",
        now: float | None = None,
    ) -> bool:
        current = self._clock(now)
        if not str(reason).strip():
            raise ValueError("release reason must be non-empty")
        with self._immediate() as connection:
            self._expire_owned(connection, current)
            query = """
                UPDATE llm_account_reservations
                SET state = 'released', released_at = ?, release_reason = ?
                WHERE task_id = ? AND agent_id = ?
                  AND state IN ('reserved', 'active')
            """
            parameters: list[object] = [
                current,
                reason,
                task_id,
                agent_id,
            ]
            if generation is not None:
                query += " AND generation = ?"
                parameters.append(generation)
            return connection.execute(query, tuple(parameters)).rowcount == 1

    def release_task(
        self,
        task_id: str,
        *,
        reason: str = "released",
        now: float | None = None,
    ) -> int:
        current = self._clock(now)
        if not str(reason).strip():
            raise ValueError("release reason must be non-empty")
        with self._immediate() as connection:
            self._expire_owned(connection, current)
            return connection.execute(
                """
                UPDATE llm_account_reservations
                SET state = 'released', released_at = ?, release_reason = ?
                WHERE task_id = ? AND state IN ('reserved', 'active')
                """,
                (current, reason, task_id),
            ).rowcount

    # Descriptive aliases let rollout code feature-detect either vocabulary.
    reserve_account_cohort = reserve_many
    get_task_account_reservation = get_reservation
    replace_reservation = replace
    replace_account_reservation = replace
    renew_task_reservations = renew_task
    release_account_reservation = release_agent
    release_task_reservations = release_task

    def record_health_transition(
        self,
        transition: AccountHealthTransition,
    ) -> None:
        with self._immediate() as connection:
            self._insert_health_transition(connection, transition)

    def list_active(self, *, now: float) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT lease_id, account_id, provider, owner_id,
                       acquired_at, expires_at
                FROM llm_account_leases
                WHERE released_at IS NULL AND expires_at > ?
                ORDER BY acquired_at
                """,
                (now,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_health(self, *, now: float) -> list[dict[str, object]]:
        """Return one redaction-safe health row per known account."""

        with self._lock:
            health_rows = self._connection.execute(
                """
                SELECT health.account_id, health.provider, health.state,
                       health.reason, health.occurred_at,
                       health.cooldown_until, health.retry_after_seconds
                FROM llm_account_health_events AS health
                JOIN (
                    SELECT account_id, provider, MAX(id) AS latest_id
                    FROM llm_account_health_events
                    GROUP BY account_id, provider
                ) AS latest ON latest.latest_id = health.id
                ORDER BY health.provider, health.account_id
                """
            ).fetchall()
            active_rows = self._connection.execute(
                """
                SELECT account_id, provider, COUNT(*) AS active_leases,
                       MAX(expires_at) AS lease_expires_at
                FROM llm_account_leases
                WHERE released_at IS NULL AND expires_at > ?
                GROUP BY account_id, provider
                """,
                (now,),
            ).fetchall()
            reservation_rows = self._connection.execute(
                """
                SELECT account_id, provider,
                       SUM(CASE WHEN state = 'reserved' THEN 1 ELSE 0 END)
                           AS reserved_accounts,
                       SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END)
                           AS active_reservations,
                       MAX(expires_at) AS reservation_expires_at
                FROM llm_account_reservations
                WHERE state IN ('reserved', 'active') AND expires_at > ?
                GROUP BY account_id, provider
                """,
                (now,),
            ).fetchall()
        active = {
            (row["provider"], row["account_id"]): dict(row)
            for row in active_rows
        }
        reservations = {
            (row["provider"], row["account_id"]): dict(row)
            for row in reservation_rows
        }
        result: list[dict[str, object]] = []
        known: set[tuple[str, str]] = set()
        for row in health_rows:
            item = dict(row)
            key = (str(item["provider"]), str(item["account_id"]))
            known.add(key)
            lease = active.get(key, {})
            reservation = reservations.get(key, {})
            item["active_leases"] = int(lease.get("active_leases") or 0)
            item["lease_expires_at"] = lease.get("lease_expires_at")
            item["reserved_accounts"] = int(
                reservation.get("reserved_accounts") or 0
            )
            item["active_reservations"] = int(
                reservation.get("active_reservations") or 0
            )
            item["reservation_expires_at"] = reservation.get(
                "reservation_expires_at"
            )
            cooldown_until = item.get("cooldown_until")
            item["cooldown_active"] = bool(
                cooldown_until is not None and float(cooldown_until) > now
            )
            result.append(item)
        for key in sorted(set(active) | set(reservations)):
            if key in known:
                continue
            lease = active.get(key, {})
            reservation = reservations.get(key, {})
            result.append(
                {
                    "provider": key[0],
                    "account_id": key[1],
                    "state": (
                        "active"
                        if reservation.get("active_reservations")
                        else "reserved"
                        if reservation
                        else "leased"
                    ),
                    "reason": (
                        "task_reservation"
                        if reservation
                        else "active_lease"
                    ),
                    "occurred_at": None,
                    "cooldown_until": None,
                    "retry_after_seconds": None,
                    "active_leases": int(lease.get("active_leases") or 0),
                    "lease_expires_at": lease.get("lease_expires_at"),
                    "reserved_accounts": int(
                        reservation.get("reserved_accounts") or 0
                    ),
                    "active_reservations": int(
                        reservation.get("active_reservations") or 0
                    ),
                    "reservation_expires_at": reservation.get(
                        "reservation_expires_at"
                    ),
                    "cooldown_active": False,
                }
            )
        return result
