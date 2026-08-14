import threading
from dataclasses import replace

import pytest

from orchestrator import scheduler as scheduler_module
from orchestrator.models import TaskPlan, WorkItem, WorkStatus, Workstream
from orchestrator.scheduler import (
    Scheduler,
    SchedulerLimits,
    SchedulingError,
    ScopeClaims,
    canonical_scope,
    ready_work_items,
    scopes_conflict,
    validate_plan,
)


def item(
    item_id: str,
    stream_id: str,
    *,
    dependencies=(),
    scope=None,
    priority=0,
) -> WorkItem:
    return WorkItem(
        id=item_id,
        workstream_id=stream_id,
        title=item_id,
        goal=f"Implement {item_id}",
        acceptance_criteria=("Approved by tester",),
        dependencies=dependencies,
        write_scopes=(scope or f"src/{item_id}.py",),
        priority=priority,
    )


def stream(stream_id: str, *, dependencies=(), items=(), workers=2) -> Workstream:
    return Workstream(
        id=stream_id,
        title=stream_id,
        goal=f"Complete {stream_id}",
        acceptance_criteria=("All items approved",),
        dependencies=dependencies,
        work_items=items,
        requested_worker_count=workers,
    )


def plan_with(*streams: Workstream, managers=2) -> TaskPlan:
    return TaskPlan(
        task_id="task-1",
        session_id="session-1",
        goal="Deliver",
        workstreams=streams,
        requested_manager_count=min(managers, len(streams)),
    )


def test_two_level_dag_validation_rejects_cycles():
    first = stream("one")
    second = stream("two", dependencies=("one",))
    cyclic = plan_with(
        replace(first, dependencies=("two",)),
        second,
    )

    with pytest.raises(SchedulingError, match="cycle"):
        validate_plan(cyclic)

    subdag = stream(
        "one",
        items=(
            item("a", "one", dependencies=("b",)),
            item("b", "one", dependencies=("a",)),
        ),
    )
    with pytest.raises(SchedulingError, match="cycle"):
        validate_plan(plan_with(subdag, managers=1))


def test_manager_selection_respects_dependencies_and_dynamic_limit():
    first = stream("one")
    second = stream("two", dependencies=("one",))
    third = stream("three")
    plan = plan_with(first, second, third, managers=3)
    scheduler = Scheduler(
        SchedulerLimits(max_managers=3, max_parallel_managers=2)
    )

    selected = scheduler.select_workstreams(
        plan,
        active_manager_count=0,
        statuses={"one": WorkStatus.PENDING, "two": WorkStatus.PENDING, "three": WorkStatus.PENDING},
    )

    assert [value.id for value in selected] == ["one", "three"]


def test_planning_caps_are_separate_from_parallel_slots():
    limits = SchedulerLimits(
        max_managers=6,
        max_parallel_managers=2,
        max_workers_per_manager=6,
        max_parallel_workers_per_manager=2,
        max_parallel_workers=8,
    )
    assert limits.manager_cap == 6
    assert limits.coders_per_manager == 5
    assert limits.worker_parallel_cap == 2


def test_worker_selection_respects_both_limits_dependencies_and_scopes():
    workstream = stream(
        "one",
        workers=4,
        items=(
            item("a", "one", scope="src/shared/a.py", priority=2),
            item("b", "one", scope="src/shared", priority=1),
            item("c", "one", dependencies=("a",), priority=3),
            item("d", "one", scope="src/independent.py"),
        ),
    )
    scheduler = Scheduler(
        SchedulerLimits(max_workers_per_manager=3, max_parallel_workers=3)
    )

    selected = scheduler.select_work_items(
        workstream,
        active_for_manager=0,
        active_global=1,
    )

    assert [value.id for value in selected] == ["a", "d"]


def test_ready_items_require_approved_dependencies():
    workstream = stream(
        "one",
        items=(
            item("a", "one"),
            item("b", "one", dependencies=("a",)),
        ),
    )

    assert [value.id for value in ready_work_items(workstream)] == ["a"]
    assert [
        value.id
        for value in ready_work_items(
            workstream,
            statuses={"a": WorkStatus.APPROVED, "b": WorkStatus.PENDING},
        )
    ] == ["b"]


def test_scope_conflicts_are_path_aware_and_claims_are_owner_checked():
    assert scopes_conflict(("src/api",), ("src/api/routes.py",))
    assert not scopes_conflict(("src/api.py",), ("src/api_v2.py",))

    claims = ScopeClaims()
    assert claims.acquire("worker-a", ("src/api",))
    assert not claims.acquire("worker-b", ("src/api/routes.py",))
    assert claims.acquire("worker-c", ("tests",))
    assert claims.release("worker-a")
    assert claims.acquire("worker-b", ("src/api/routes.py",))


def test_windows_scope_aliases_casefold_and_empty_claim_is_project_wide(
    monkeypatch,
):
    monkeypatch.setattr(scheduler_module.os, "name", "nt")

    assert canonical_scope(r"SRC\Api\Routes.py") == "src/api/routes.py"
    assert scopes_conflict(
        (r"SRC\Api",),
        ("src/api/routes.py",),
    )
    claims = ScopeClaims()
    assert claims.acquire("project-owner", ())
    assert not claims.acquire("file-owner", ("src/app.py",))


def test_scope_claim_waits_for_conflicting_owner_instead_of_failing():
    claims = ScopeClaims()
    started = threading.Event()
    acquired = threading.Event()
    assert claims.acquire("worker-a", ("src/api",))

    def wait_for_scope():
        started.set()
        if claims.wait_acquire(
            "worker-b",
            ("src/api/routes.py",),
            timeout=1,
        ):
            acquired.set()

    waiter = threading.Thread(target=wait_for_scope)
    waiter.start()
    assert started.wait(1)
    assert not acquired.wait(0.05)

    assert claims.release("worker-a")
    assert acquired.wait(1)
    waiter.join(timeout=1)
    assert not waiter.is_alive()
    assert claims.release("worker-b")


def test_scope_claim_wait_can_be_cancelled():
    claims = ScopeClaims()
    cancelled = threading.Event()
    assert claims.acquire("worker-a", ("src/api",))

    cancelled.set()
    assert not claims.wait_acquire(
        "worker-b",
        ("src/api/routes.py",),
        timeout=10,
        cancelled=cancelled.is_set,
    )
    assert claims.release("worker-a")


def test_selection_prioritizes_critical_path_then_rotates_waiters():
    independent = stream("independent")
    foundation = stream("foundation")
    dependent = stream("dependent", dependencies=("foundation",))
    plan = plan_with(independent, foundation, dependent, managers=3)
    scheduler = Scheduler(
        SchedulerLimits(max_managers=3, max_parallel_managers=1)
    )
    statuses = {
        value.id: WorkStatus.PENDING for value in plan.workstreams
    }

    first = scheduler.select_workstreams(
        plan,
        active_manager_count=0,
        statuses=statuses,
    )
    second = scheduler.select_workstreams(
        plan,
        active_manager_count=0,
        statuses=statuses,
    )

    assert [value.id for value in first] == ["foundation"]
    assert [value.id for value in second] == ["independent"]


def test_worker_selection_ages_skipped_item_and_honors_cancellation():
    workstream = stream(
        "fair",
        workers=2,
        items=(
            item("high", "fair", priority=10),
            item("low", "fair", priority=0),
        ),
    )
    scheduler = Scheduler(
        SchedulerLimits(max_workers_per_manager=2, max_parallel_workers=1)
    )

    first = scheduler.select_work_items(
        workstream,
        active_for_manager=0,
        active_global=0,
    )
    second = scheduler.select_work_items(
        workstream,
        active_for_manager=0,
        active_global=0,
    )
    cancelled = scheduler.select_work_items(
        workstream,
        active_for_manager=0,
        active_global=0,
        cancelled=lambda: True,
    )

    assert [value.id for value in first] == ["high"]
    assert [value.id for value in second] == ["low"]
    assert cancelled == []
