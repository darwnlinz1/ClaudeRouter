"""SQLite-backed cross-thread/process LLM account leases and health history."""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Collection, Sequence
from pathlib import Path

from .account_lease import (
    SQLITE_ACCOUNT_LEASE_SCHEMA,
    AccountCandidate,
    AccountHealthTransition,
    AccountLease,
)

DEFAULT_ACCOUNT_DB = (
    Path.home() / ".ai_orchestrator" / "account_leases.sqlite3"
)


class SQLiteAccountLeaseStore:
    """Durable account coordination without persisting cookie secrets."""

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
        eligible = [
            candidate
            for candidate in candidates
            if candidate.account_id not in excluded
            and (
                candidate.cooldown_until is None
                or candidate.cooldown_until <= now
            )
        ]
        if not eligible:
            return None

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._connection.execute(
                    """
                    UPDATE llm_account_leases
                    SET released_at = ?, outcome = 'expired'
                    WHERE released_at IS NULL AND expires_at <= ?
                    """,
                    (now, now),
                )
                durable_cooldowns: dict[tuple[str, str], float] = {}
                for candidate in eligible:
                    row = self._connection.execute(
                        """
                        SELECT cooldown_until
                        FROM llm_account_health_events
                        WHERE provider = ? AND account_id = ?
                          AND cooldown_until IS NOT NULL
                        ORDER BY occurred_at DESC, id DESC
                        LIMIT 1
                        """,
                        (candidate.provider, candidate.account_id),
                    ).fetchone()
                    durable_cooldowns[
                        (candidate.provider, candidate.account_id)
                    ] = float(row["cooldown_until"]) if row else 0.0

                ranked = sorted(
                    (
                        candidate
                        for candidate in eligible
                        if durable_cooldowns[
                            (candidate.provider, candidate.account_id)
                        ]
                        <= now
                    ),
                    key=lambda candidate: (
                        candidate.active_requests,
                        candidate.account_id,
                    ),
                )
                selected: AccountCandidate | None = None
                for candidate in ranked:
                    active = self._connection.execute(
                        """
                        SELECT 1 FROM llm_account_leases
                        WHERE provider = ? AND account_id = ?
                          AND released_at IS NULL
                        LIMIT 1
                        """,
                        (candidate.provider, candidate.account_id),
                    ).fetchone()
                    if active is None:
                        selected = candidate
                        break
                if selected is None:
                    self._connection.execute("COMMIT")
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
                self._connection.execute(
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
                self._connection.execute("COMMIT")
                return lease
            except Exception:
                self._connection.execute("ROLLBACK")
                raise

    def release(
        self,
        lease: AccountLease,
        *,
        outcome: str,
        now: float,
    ) -> None:
        with self._lock:
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

    def record_health_transition(
        self,
        transition: AccountHealthTransition,
    ) -> None:
        with self._lock:
            self._connection.execute(
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
        active = {
            (row["provider"], row["account_id"]): dict(row)
            for row in active_rows
        }
        result: list[dict[str, object]] = []
        known: set[tuple[str, str]] = set()
        for row in health_rows:
            item = dict(row)
            key = (str(item["provider"]), str(item["account_id"]))
            known.add(key)
            lease = active.get(key, {})
            item["active_leases"] = int(lease.get("active_leases") or 0)
            item["lease_expires_at"] = lease.get("lease_expires_at")
            cooldown_until = item.get("cooldown_until")
            item["cooldown_active"] = bool(
                cooldown_until is not None and float(cooldown_until) > now
            )
            result.append(item)
        for key, lease in active.items():
            if key in known:
                continue
            result.append(
                {
                    "provider": key[0],
                    "account_id": key[1],
                    "state": "leased",
                    "reason": "active_lease",
                    "occurred_at": None,
                    "cooldown_until": None,
                    "retry_after_seconds": None,
                    "active_leases": int(lease.get("active_leases") or 0),
                    "lease_expires_at": lease.get("lease_expires_at"),
                    "cooldown_active": False,
                }
            )
        return result
