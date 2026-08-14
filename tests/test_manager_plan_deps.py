import pytest

from orchestrator.hierarchy import _manager_stream_from_result, _package_files
from orchestrator.llm_client import ToolCallResult
from orchestrator.models import WorkStatus, Workstream
from orchestrator.scheduler import SchedulerLimits


def test_manager_plan_worker_slots_follow_item_count():
    stream = Workstream(
        id="ws",
        title="WS",
        goal="g",
        acceptance_criteria=("ok",),
        write_scopes=["a.py"],
        status=WorkStatus.PENDING,
    )
    result = ToolCallResult(
        "submit_work_item_plan",
        {
            "summary": "three items",
            "requested_worker_count": 3,
            "work_items": [
                {
                    "id": "a",
                    "title": "A",
                    "goal": "A",
                    "file_path": "a.py",
                    "instructions": "a",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [],
                    "write_scopes": ["a.py"],
                    "test_focus": "",
                },
                {
                    "id": "b",
                    "title": "B",
                    "goal": "B",
                    "file_path": "b.py",
                    "instructions": "b",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [],
                    "write_scopes": ["b.py"],
                    "test_focus": "",
                },
                {
                    "id": "c",
                    "title": "C",
                    "goal": "C",
                    "file_path": "c.py",
                    "instructions": "c",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [],
                    "write_scopes": ["c.py"],
                    "test_focus": "",
                },
            ],
        },
        {},
    )
    planned = _manager_stream_from_result(
        result, stream, SchedulerLimits(max_workers_per_manager=4)
    )
    assert planned.requested_worker_count == 3
    assert len(planned.work_items) == 3
    # One child-agent slot is reserved for the workstream Tester. Plans above
    # the remaining Coder cap are rejected instead of silently trimmed.
    with pytest.raises(RuntimeError, match="exceeds maximum 1"):
        _manager_stream_from_result(
            result,
            stream,
            SchedulerLimits(max_workers_per_manager=2),
            max_coder_count=1,
        )


def test_manager_plan_package_includes_all_write_scope_files():
    stream = Workstream(
        id="ws",
        title="WS",
        goal="g",
        acceptance_criteria=("ok",),
        write_scopes=["backend/"],
        status=WorkStatus.PENDING,
    )
    result = ToolCallResult(
        "submit_work_item_plan",
        {
            "summary": "auth package",
            "requested_worker_count": 1,
            "work_items": [
                {
                    "id": "auth",
                    "title": "Auth stack",
                    "goal": "Build auth",
                    "file_path": "backend/nexus/auth/service.py",
                    "instructions": "Implement auth package",
                    "acceptance_criteria": ["login works"],
                    "dependencies": [],
                    "contract_id": "auth-contract",
                    "contract_version": 2,
                    "input_artifacts": ["docs/auth.md"],
                    "expected_outputs": ["working auth package"],
                    "read_scopes": ["docs/auth.md"],
                    "write_scopes": [
                        "backend/nexus/auth/service.py",
                        "backend/nexus/auth/router.py",
                        "backend/nexus/auth/models.py",
                    ],
                    "evidence_requirements": ["pytest auth passes"],
                    "consumers": ["api-router"],
                    "risk_level": "high",
                    "priority": 7,
                    "test_focus": "pytest auth",
                }
            ],
        },
        {},
    )
    planned = _manager_stream_from_result(result, stream, SchedulerLimits())
    item = planned.work_items[0]
    assert item.metadata["package_files"] == [
        "backend/nexus/auth/service.py",
        "backend/nexus/auth/router.py",
        "backend/nexus/auth/models.py",
    ]
    assert item.contract.id == "auth-contract"
    assert item.contract.version == 2
    assert item.contract.risk_level.value == "high"
    assert item.priority == 7
    assert _package_files(item) == item.metadata["package_files"]
    assert planned.metadata["fanout"] == {
        "selected": 1,
        "max": 3,
        "reason": "auth package",
        "execution_slots": 3,
    }
    with pytest.raises(RuntimeError, match="execution slots"):
        _manager_stream_from_result(
            result,
            stream,
            SchedulerLimits(),
            max_coder_count=2,
        )


def test_manager_plan_drops_unknown_dependencies():
    stream = Workstream(
        id="platform-core",
        title="Core",
        goal="Build core",
        acceptance_criteria=("works",),
        write_scopes=["a.py"],
        status=WorkStatus.PENDING,
    )
    result = ToolCallResult(
        "submit_work_item_plan",
        {
            "summary": "two items",
            "requested_worker_count": 2,
            "work_items": [
                {
                    "id": "a",
                    "title": "A",
                    "goal": "A",
                    "file_path": "a.py",
                    "instructions": "edit a",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [],
                    "write_scopes": ["a.py"],
                    "test_focus": "",
                },
                {
                    "id": "b",
                    "title": "B",
                    "goal": "B",
                    "file_path": "b.py",
                    "instructions": "edit b",
                    "acceptance_criteria": ["ok"],
                    "dependencies": ["a", "missing-id"],
                    "write_scopes": ["b.py"],
                    "test_focus": "",
                },
            ],
        },
        {},
    )
    planned = _manager_stream_from_result(result, stream, SchedulerLimits())
    by_id = {item.id: item for item in planned.work_items}
    assert by_id["platform-core:b"].dependencies == ("platform-core:a",)
    assert planned.metadata["dropped_dependencies"] == ["b->missing-id"]
