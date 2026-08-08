from dataclasses import replace

import pytest

from orchestrator.models import TaskPlan, WorkItem, WorkStatus, Workstream
from orchestrator.scheduler import (
    Scheduler,
    SchedulerLimits,
    SchedulingError,
    ScopeClaims,
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
    scheduler = Scheduler(SchedulerLimits(max_parallel_managers=2))

    selected = scheduler.select_workstreams(
        plan,
        active_manager_count=0,
        statuses={"one": WorkStatus.PENDING, "two": WorkStatus.PENDING, "three": WorkStatus.PENDING},
    )

    assert [value.id for value in selected] == ["one", "three"]


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
