import pytest

from orchestrator.hierarchy import _director_plan_from_result
from orchestrator.llm_client import ToolCallResult
from orchestrator.scheduler import SchedulerLimits


def _result(count: int) -> ToolCallResult:
    return ToolCallResult(
        "submit_workstream_plan",
        {
            "summary": f"{count} streams",
            "requested_manager_count": count,
            "workstreams": [
                {
                    "id": f"w{i}",
                    "title": f"W{i}",
                    "goal": "g",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [f"w{i-1}"] if i > 1 else [],
                    "contract_id": f"w{i}-contract",
                    "contract_version": 1,
                    "input_artifacts": [],
                    "expected_outputs": [f"f{i}.py"],
                    "read_scopes": [],
                    "write_scopes": [f"f{i}.py"],
                    "evidence_requirements": ["tests pass"],
                    "consumers": ["task"],
                    "risk_level": "medium",
                    "priority": i,
                }
                for i in range(1, count + 1)
            ],
        },
        {},
    )


def test_director_accepts_selected_manager_count_below_cap():
    plan = _director_plan_from_result(
        _result(2),
        task_id="t1",
        session_id="s1",
        goal="goal",
        limits=SchedulerLimits(
            max_managers=4,
            max_parallel_managers=2,
            max_workstreams=64,
        ),
        max_manager_count=4,
    )
    assert plan.requested_manager_count == 2
    assert len(plan.workstreams) == 2
    assert plan.workstreams[0].metadata["work_contract"]["id"] == "w1-contract"
    assert plan.workstreams[0].contract.id == "w1-contract"
    assert plan.metadata["fanout"]["manager"] == {
        "selected": 2,
        "max": 4,
        "reason": "2 streams",
        "execution_slots": 2,
    }


def test_director_rejects_plan_above_manager_cap():
    with pytest.raises(RuntimeError, match="exceeds maximum 4"):
        _director_plan_from_result(
            _result(12),
            task_id="t1",
            session_id="s1",
            goal="goal",
            limits=SchedulerLimits(
                max_managers=4,
                max_parallel_managers=2,
                max_workstreams=64,
            ),
            max_manager_count=4,
        )


def test_director_validates_execution_slots_against_effective_cap():
    with pytest.raises(RuntimeError, match="execution slots"):
        _director_plan_from_result(
            _result(2),
            task_id="t1",
            session_id="s1",
            goal="goal",
            limits=SchedulerLimits(
                max_managers=4,
                max_parallel_managers=3,
                max_workstreams=64,
            ),
            max_manager_count=2,
        )
