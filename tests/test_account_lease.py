from pathlib import Path

from orchestrator.account_lease import (
    AccountCandidate,
    AccountHealthTransition,
)
from orchestrator.sqlite_account_lease import SQLiteAccountLeaseStore


def test_sqlite_account_lease_is_exclusive_across_connections(tmp_path: Path):
    database = tmp_path / "accounts.sqlite3"
    first = SQLiteAccountLeaseStore(database)
    second = SQLiteAccountLeaseStore(database)
    candidate = AccountCandidate("account-a", "legacy_web")
    try:
        lease = first.acquire(
            candidates=(candidate,),
            owner_id="call-a",
            lease_ttl_seconds=60,
            now=100,
        )
        assert lease is not None
        assert (
            second.acquire(
                candidates=(candidate,),
                owner_id="call-b",
                lease_ttl_seconds=60,
                now=101,
            )
            is None
        )

        first.release(lease, outcome="completed", now=102)
        replacement = second.acquire(
            candidates=(candidate,),
            owner_id="call-b",
            lease_ttl_seconds=60,
            now=103,
        )
        assert replacement is not None
        assert replacement.owner_id == "call-b"
    finally:
        first.close()
        second.close()


def test_sqlite_account_cooldown_survives_store_restart(tmp_path: Path):
    database = tmp_path / "accounts.sqlite3"
    store = SQLiteAccountLeaseStore(database)
    store.record_health_transition(
        AccountHealthTransition(
            account_id="account-a",
            provider="legacy_web",
            state="cooldown",
            reason="rate_limit",
            occurred_at=100,
            cooldown_until=130,
            retry_after_seconds=30,
        )
    )
    store.close()

    with SQLiteAccountLeaseStore(database) as reopened:
        candidate = AccountCandidate("account-a", "legacy_web")
        assert (
            reopened.acquire(
                candidates=(candidate,),
                owner_id="call-a",
                lease_ttl_seconds=60,
                now=120,
            )
            is None
        )
        assert reopened.acquire(
            candidates=(candidate,),
            owner_id="call-a",
            lease_ttl_seconds=60,
            now=131,
        ) is not None


def test_account_health_reports_cooldown_and_active_lease(tmp_path: Path):
    with SQLiteAccountLeaseStore(tmp_path / "accounts.sqlite3") as store:
        store.record_health_transition(
            AccountHealthTransition(
                account_id="account-a",
                provider="legacy_web",
                state="cooldown",
                reason="rate_limit",
                occurred_at=100,
                cooldown_until=130,
                retry_after_seconds=30,
            )
        )
        lease = store.acquire(
            candidates=(AccountCandidate("account-b", "legacy_web"),),
            owner_id="call-b",
            lease_ttl_seconds=60,
            now=110,
        )
        assert lease is not None

        health = {
            row["account_id"]: row for row in store.list_health(now=120)
        }

        assert health["account-a"]["cooldown_active"] is True
        assert health["account-a"]["active_leases"] == 0
        assert health["account-b"]["state"] == "leased"
        assert health["account-b"]["active_leases"] == 1


def test_sqlite_account_lease_renewal_fences_takeover_until_new_expiry(
    tmp_path: Path,
):
    database = tmp_path / "accounts.sqlite3"
    first = SQLiteAccountLeaseStore(database)
    second = SQLiteAccountLeaseStore(database)
    candidate = AccountCandidate("account-a", "legacy_web")
    try:
        lease = first.acquire(
            candidates=(candidate,),
            owner_id="call-a",
            lease_ttl_seconds=10,
            now=100,
        )
        assert lease is not None
        renewed = first.renew(
            lease,
            lease_ttl_seconds=10,
            now=105,
        )
        assert renewed is not None
        assert renewed.expires_at == 115
        assert (
            second.acquire(
                candidates=(candidate,),
                owner_id="call-b",
                lease_ttl_seconds=10,
                now=111,
            )
            is None
        )
        assert second.acquire(
            candidates=(candidate,),
            owner_id="call-b",
            lease_ttl_seconds=10,
            now=116,
        ) is not None
        assert first.renew(
            renewed,
            lease_ttl_seconds=10,
            now=116,
        ) is None
    finally:
        first.close()
        second.close()
