from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

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


def test_reserve_many_is_concurrent_and_all_or_none(tmp_path: Path):
    database = tmp_path / "cohort.sqlite3"
    first = SQLiteAccountLeaseStore(database)
    second = SQLiteAccountLeaseStore(database)
    candidates = tuple(
        AccountCandidate(f"account-{index}", "legacy_web")
        for index in range(3)
    )
    barrier = Barrier(2)

    def reserve(
        store: SQLiteAccountLeaseStore,
        task_id: str,
    ):
        barrier.wait()
        return store.reserve_many(
            task_id,
            ("manager", "worker"),
            candidates,
            60,
            now=100,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (
                pool.submit(reserve, first, "task-a"),
                pool.submit(reserve, second, "task-b"),
            )
            results = [future.result() for future in futures]

        assert sum(result is not None for result in results) == 1
        winner = next(result for result in results if result is not None)
        assert len(winner) == 2
        assert len({reservation.account_id for reservation in winner}) == 2
        loser_task = "task-a" if winner.task_id == "task-b" else "task-b"
        assert first.list_task_reservations(loser_task, now=101) == []
    finally:
        first.close()
        second.close()


def test_atomic_replacement_contention_has_one_winner(tmp_path: Path):
    database = tmp_path / "replacement.sqlite3"
    first = SQLiteAccountLeaseStore(database)
    second = SQLiteAccountLeaseStore(database)
    account_a = AccountCandidate("account-a", "legacy_web")
    account_b = AccountCandidate("account-b", "legacy_web")
    account_c = AccountCandidate("account-c", "legacy_web")
    reserved_a = first.reserve_many(
        "task-a",
        ("agent-a",),
        (account_a,),
        60,
        now=100,
    )
    reserved_b = second.reserve_many(
        "task-b",
        ("agent-b",),
        (account_b,),
        60,
        now=100,
    )
    assert reserved_a is not None
    assert reserved_b is not None
    barrier = Barrier(2)

    def replace(
        store: SQLiteAccountLeaseStore,
        task_id: str,
        agent_id: str,
        failed,
    ):
        barrier.wait()
        return store.replace(
            task_id,
            agent_id,
            failed,
            (account_c,),
            now=101,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (
                pool.submit(
                    replace,
                    first,
                    "task-a",
                    "agent-a",
                    reserved_a["agent-a"],
                ),
                pool.submit(
                    replace,
                    second,
                    "task-b",
                    "agent-b",
                    reserved_b["agent-b"],
                ),
            )
            replacements = [future.result() for future in futures]

        assert sum(value is not None for value in replacements) == 1
        assert {
            reservation.account_id
            for reservation in replacements
            if reservation is not None
        } == {"account-c"}
        active = (
            first.list_task_reservations("task-a", now=102)
            + first.list_task_reservations("task-b", now=102)
        )
        assert [reservation.account_id for reservation in active] == [
            "account-c"
        ]
    finally:
        first.close()
        second.close()


def test_quarantined_and_disabled_accounts_are_not_reserved(tmp_path: Path):
    with SQLiteAccountLeaseStore(tmp_path / "health.sqlite3") as store:
        store.record_health_transition(
            AccountHealthTransition(
                account_id="quarantined",
                provider="legacy_web",
                state="quarantined",
                reason="authentication_failed",
                occurred_at=100,
            )
        )
        store.record_health_transition(
            AccountHealthTransition(
                account_id="disabled",
                provider="legacy_web",
                state="disabled",
                reason="operator_disabled",
                occurred_at=100,
            )
        )
        candidates = (
            AccountCandidate("quarantined", "legacy_web"),
            AccountCandidate("disabled", "legacy_web"),
            AccountCandidate("healthy", "legacy_web"),
        )

        cohort = store.reserve_many(
            "task-health",
            ("agent",),
            candidates,
            60,
            now=101,
        )

        assert cohort is not None
        assert cohort["agent"].account_id == "healthy"
        assert (
            store.reserve_many(
                "task-exhausted",
                ("agent",),
                candidates[:2],
                60,
                now=101,
            )
            is None
        )


def test_task_reservation_renewal_and_fenced_release(tmp_path: Path):
    database = tmp_path / "renew-release.sqlite3"
    with SQLiteAccountLeaseStore(database) as store:
        cohort = store.reserve_many(
            "task-renew",
            ("manager", "worker"),
            (
                AccountCandidate("account-a", "legacy_web"),
                AccountCandidate("account-b", "legacy_web"),
            ),
            10,
            now=100,
        )
        assert cohort is not None
        manager = store.activate(
            "task-renew",
            "manager",
            cohort["manager"].generation,
            now=101,
        )
        assert manager is not None
        assert manager.state == "active"

        assert (
            store.renew_task(
                "task-renew",
                20,
                expected_generations={"manager": manager.generation + 1},
                now=105,
            )
            is None
        )
        renewed = store.renew_task(
            "task-renew",
            20,
            expected_generations={
                reservation.agent_id: reservation.generation
                for reservation in cohort
            },
            now=105,
        )
        assert renewed is not None
        assert {reservation.expires_at for reservation in renewed} == {125}
        assert not store.release_agent(
            "task-renew",
            "manager",
            generation=manager.generation + 1,
            now=106,
        )
        assert store.release_agent(
            "task-renew",
            "manager",
            generation=manager.generation,
            reason="manager_terminal",
            now=106,
        )
        assert store.release_task(
            "task-renew",
            reason="task_terminal",
            now=107,
        ) == 1
        assert store.list_task_reservations("task-renew", now=108) == []


def test_runtime_adapter_keeps_task_ownership_between_transports(tmp_path: Path):
    with SQLiteAccountLeaseStore(tmp_path / "runtime-adapter.sqlite3") as store:
        first = AccountCandidate("account-a", "legacy_web")
        second = AccountCandidate("account-b", "legacy_web")
        cohort = store.reserve_many(
            "task-runtime",
            ("worker",),
            (first, second),
            30,
            now=100,
        )
        assert cohort is not None

        transport_lease = store.consume_reserved_account(
            "task-runtime",
            "worker",
            provider="legacy_web",
            candidates=(first, second),
            now=101,
        )
        assert transport_lease is not None
        assert transport_lease.metadata["task_reservation"] is True
        renewed = store.renew(
            transport_lease,
            lease_ttl_seconds=30,
            now=102,
        )
        assert renewed is not None
        store.release(renewed, outcome="completed", now=103)
        assert store.get_reservation(
            "task-runtime",
            "worker",
            now=104,
        ) is not None

        replacement = store.replace_account_atomically(
            "task-runtime",
            "worker",
            "account-a",
            (second,),
            reason="account_invalid",
            current_lease=renewed,
            lease_ttl_seconds=30,
            now=104,
        )
        assert replacement is not None
        assert replacement.account_id == "account-b"
        assert replacement.metadata["reservation_generation"] == 2
        assert (
            store.reserve_many(
                "another-task",
                ("worker",),
                (first,),
                30,
                now=105,
            )
            is None
        )


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
