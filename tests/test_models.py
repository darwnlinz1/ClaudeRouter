from dataclasses import replace

import pytest

from orchestrator.models import (
    AgentInstance,
    AgentRole,
    EventEnvelope,
    TaskPlan,
    WorkerAssignment,
    WorkItem,
    Workstream,
    agent_instance_from_dict,
    event_from_dict,
    task_plan_from_dict,
    to_dict,
    worker_assignment_from_dict,
)


def make_plan() -> TaskPlan:
    item = WorkItem(
        id="item-1",
        workstream_id="stream-1",
        title="Implement",
        goal="Add foundation",
        acceptance_criteria=("Unit tests pass",),
        write_scopes=("orchestrator/models.py",),
    )
    stream = Workstream(
        id="stream-1",
        title="Foundation",
        goal="Build typed state",
        acceptance_criteria=("Models validate",),
        work_items=(item,),
        requested_worker_count=2,
    )
    return TaskPlan(
        task_id="task-1",
        session_id="session-1",
        goal="Build orchestrator",
        workstreams=(stream,),
    )


def test_task_plan_round_trip_is_lossless():
    plan = make_plan()

    restored = task_plan_from_dict(to_dict(plan))

    assert restored == plan
    assert restored.workstreams[0].work_items[0].write_scopes == (
        "orchestrator/models.py",
    )


def test_model_validation_rejects_unsafe_scope_and_self_dependency():
    with pytest.raises(ValueError, match="project-relative"):
        replace(
            make_plan().workstreams[0].work_items[0],
            write_scopes=("../outside.py",),
        )

    with pytest.raises(ValueError, match="depend on itself"):
        replace(
            make_plan().workstreams[0].work_items[0],
            dependencies=("item-1",),
        )


def test_agent_role_invariants_are_enforced():
    with pytest.raises(ValueError, match="workstream_id"):
        AgentInstance(
            id="manager-1",
            task_id="task-1",
            session_id="session-1",
            role=AgentRole.MANAGER,
        )


def test_event_envelope_round_trip_preserves_version_and_timestamp():
    event = EventEnvelope(
        task_id="task-1",
        session_id="session-1",
        event_type="plan.created",
        payload={"revision": 1},
        version=2,
    )

    restored = event_from_dict(to_dict(event))

    assert restored == event
    assert restored.version == 2


def test_assignment_and_agent_round_trip():
    assignment = WorkerAssignment(
        id="assignment-1",
        task_id="task-1",
        workstream_id="stream-1",
        work_item_id="item-1",
        worker_agent_id="worker-1",
        attempt_id="attempt-1",
        write_scopes=("src",),
    )
    agent = AgentInstance(
        id="worker-1",
        task_id="task-1",
        session_id="session-1",
        role="worker",
        work_item_id="item-1",
    )

    assert worker_assignment_from_dict(to_dict(assignment)) == assignment
    assert agent_instance_from_dict(to_dict(agent)) == agent
