import pytest

from orchestrator.hierarchy import _director_plan_from_result
from orchestrator.llm_client import ToolCallResult
from orchestrator.scheduler import SchedulerLimits


def test_director_rejects_plan_that_ignores_required_manager_count():
    result = ToolCallResult(
        "submit_workstream_plan",
        {
            "summary": "many streams",
            "requested_manager_count": 12,
            "workstreams": [
                {
                    "id": f"w{i}",
                    "title": f"W{i}",
                    "goal": "g",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [f"w{i-1}"] if i > 1 else [],
                    "write_scopes": [f"f{i}.py"],
                }
                for i in range(1, 13)
            ],
        },
        {},
    )
    with pytest.raises(RuntimeError, match="expected exactly 4"):
        _director_plan_from_result(
            result,
            task_id="t1",
            session_id="s1",
            goal="goal",
            limits=SchedulerLimits(max_parallel_managers=4, max_workstreams=64),
            required_manager_count=4,
        )
