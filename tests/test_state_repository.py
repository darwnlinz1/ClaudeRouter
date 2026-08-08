from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.models import Attempt, EventEnvelope, TaskPlan, WorkItem, Workstream
from orchestrator.state_repository import StateRepository


def make_plan() -> TaskPlan:
    workstream = Workstream(
        id="stream-1",
        title="Foundation",
        goal="Build typed state",
        acceptance_criteria=("Models validate",),
        work_items=(
            WorkItem(
                id="item-1",
                workstream_id="stream-1",
                title="Implement",
                goal="Add foundation",
                acceptance_criteria=("Unit tests pass",),
                write_scopes=("orchestrator/models.py",),
            ),
        ),
    )
    return TaskPlan(
        task_id="task-1",
        session_id="session-1",
        goal="Build orchestrator",
        workstreams=(workstream,),
    )


@pytest.fixture
def repository(tmp_path):
    with StateRepository(tmp_path / "state.sqlite3") as value:
        yield value


def test_plan_and_normalized_dag_are_durable(repository):
    plan = make_plan()

    repository.save_plan(plan)

    assert repository.get_task(plan.task_id)["goal"] == plan.goal
    assert repository.get_plan(plan.task_id) == plan
    assert [stream.id for stream in repository.list_workstreams(plan.task_id)] == [
        "stream-1"
    ]
    assert [item.id for item in repository.list_work_items(plan.task_id)] == [
        "item-1"
    ]


def test_plan_revision_uses_optimistic_conflict_check(repository):
    first = make_plan()
    repository.save_plan(first)
    second = replace(first, revision=2)

    with pytest.raises(RuntimeError, match="revision conflict"):
        repository.save_plan(second, expected_previous_revision=0)

    repository.save_plan(second, expected_previous_revision=1)
    assert repository.get_plan(first.task_id).revision == 2


def test_attempt_round_trip(repository):
    attempt = Attempt(
        id="attempt-1",
        task_id="task-1",
        workstream_id="stream-1",
        work_item_id="item-1",
        number=1,
        worker_agent_id="worker-1",
    )

    repository.save_attempt(attempt)

    assert repository.get_attempt(attempt.id) == attempt
    assert repository.list_attempts("task-1") == [attempt]


def test_events_get_atomic_monotonic_sequences_and_replay(repository):
    first = repository.append_event(
        EventEnvelope(
            task_id="task-1",
            session_id="session-1",
            event_type="task.started",
            payload={},
        )
    )
    second = repository.append_event(
        EventEnvelope(
            task_id="task-1",
            session_id="session-1",
            event_type="plan.created",
            payload={"revision": 1},
        )
    )

    assert (first.sequence, second.sequence) == (1, 2)
    assert repository.replay_events("task-1", after_sequence=1) == [second]
    assert repository.append_event(second) == second


def test_delete_task_removes_events(repository):
    repository.append_event(
        EventEnvelope(
            task_id="task-delete",
            session_id="session-delete",
            event_type="plan.created",
            payload={"revision": 1},
        )
    )

    counts = repository.delete_task("task-delete")

    assert counts["events"] == 1
    assert repository.replay_events("task-delete") == []


def test_lease_is_exclusive_until_expiry(repository):
    now = datetime.now(timezone.utc)

    assert repository.acquire_lease("item", "item-1", "worker-a", 30, now=now)
    assert not repository.acquire_lease("item", "item-1", "worker-b", 30, now=now)
    assert repository.acquire_lease(
        "item", "item-1", "worker-b", 30, now=now + timedelta(seconds=31)
    )
    assert not repository.release_lease("item", "item-1", "worker-a")
    assert repository.release_lease("item", "item-1", "worker-b")


def test_project_lock_is_owner_checked(repository):
    assert repository.acquire_project_lock("project-a", "manager-a", 30)
    assert not repository.acquire_project_lock("project-a", "manager-b", 30)
    assert not repository.release_project_lock("project-a", "manager-b")
    assert repository.release_project_lock("project-a", "manager-a")
