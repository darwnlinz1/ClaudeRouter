import pytest

from orchestrator import config
from orchestrator.hierarchy import _manager_stream_from_result, _package_files
from orchestrator.llm_client import ToolCallResult
from orchestrator.models import WorkContract, WorkStatus, Workstream
from orchestrator.scheduler import SchedulerLimits
from orchestrator.tools_schema import SUBMIT_WORK_ITEM_PLAN_SCHEMA


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


def test_manager_tool_schema_matches_runtime_file_package_cap():
    item_schema = SUBMIT_WORK_ITEM_PLAN_SCHEMA["input_schema"]["properties"]["work_items"]["items"]
    assert (
        item_schema["properties"]["write_scopes"]["maxItems"] == config.MAX_FILES_PER_WORK_PACKAGE
    )


def test_manager_plan_uses_frontend_backed_coder_cap_above_twelve():
    stream = Workstream(
        id="large-stream",
        title="Large stream",
        goal="g",
        acceptance_criteria=("ok",),
        write_scopes=["src/"],
        status=WorkStatus.PENDING,
    )
    work_items = [
        {
            "id": f"item-{index}",
            "title": f"Item {index}",
            "goal": f"Implement item {index}",
            "file_path": f"src/item_{index}.py",
            "instructions": f"Implement src/item_{index}.py",
            "acceptance_criteria": ["ok"],
            "dependencies": [],
            "write_scopes": [f"src/item_{index}.py"],
            "test_focus": "",
        }
        for index in range(20)
    ]
    result = ToolCallResult(
        "submit_work_item_plan",
        {
            "summary": "twenty independent packages",
            "requested_worker_count": len(work_items),
            "work_items": work_items,
        },
        {},
    )

    planned = _manager_stream_from_result(
        result,
        stream,
        SchedulerLimits(
            max_workers_per_manager=21,
            max_parallel_workers_per_manager=20,
            max_parallel_workers=32,
        ),
    )

    assert planned.requested_worker_count == 20
    assert len(planned.work_items) == 20
    assert planned.metadata["fanout"]["max"] == 20


def test_manager_plan_rejects_scope_overflow_instead_of_truncating():
    stream = Workstream(
        id="scope-stream",
        title="Scope stream",
        goal="g",
        acceptance_criteria=("ok",),
        write_scopes=("src/",),
        status=WorkStatus.PENDING,
    )
    scopes = [f"src/file_{index}.py" for index in range(13)]
    result = ToolCallResult(
        "submit_work_item_plan",
        {
            "summary": "oversized package",
            "requested_worker_count": 1,
            "work_items": [
                {
                    "id": "oversized",
                    "title": "Oversized",
                    "goal": "Implement too many files",
                    "file_path": scopes[0],
                    "instructions": "Implement the package",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [],
                    "write_scopes": scopes,
                    "test_focus": "",
                }
            ],
        },
        {},
    )

    with pytest.raises(RuntimeError, match="13 write scopes; limit is 12"):
        _manager_stream_from_result(result, stream, SchedulerLimits())


def test_manager_plan_uses_one_canonical_scope_tuple_for_item_and_contract():
    stream = Workstream(
        id="canonical-stream",
        title="Canonical stream",
        goal="g",
        acceptance_criteria=("ok",),
        write_scopes=("src/",),
        status=WorkStatus.PENDING,
    )
    result = ToolCallResult(
        "submit_work_item_plan",
        {
            "summary": "canonical package",
            "requested_worker_count": 1,
            "work_items": [
                {
                    "id": "canonical",
                    "title": "Canonical",
                    "goal": "Normalize scopes",
                    "file_path": "src/main.py",
                    "instructions": "Implement the package",
                    "acceptance_criteria": ["ok"],
                    "dependencies": [],
                    "write_scopes": ["src/helper.py"],
                    "test_focus": "",
                }
            ],
        },
        {},
    )

    planned = _manager_stream_from_result(result, stream, SchedulerLimits())
    item = planned.work_items[0]
    assert item.write_scopes == ("src/main.py", "src/helper.py")
    assert item.contract is not None
    assert item.contract.write_scopes == item.write_scopes
    assert _package_files(item) == ["src/main.py", "src/helper.py"]


def test_manager_namespaces_item_contract_that_reuses_parent_identity():
    parent_contract = WorkContract(
        id="shared-contract",
        expected_outputs=("src/",),
        write_scopes=("src/",),
        acceptance_criteria=("complete",),
        test_requirements=("tests pass",),
        evidence_requirements=("evidence",),
        consumers=("task",),
    )
    stream = Workstream(
        id="ws",
        title="WS",
        goal="g",
        acceptance_criteria=("complete",),
        write_scopes=("src/",),
        status=WorkStatus.PENDING,
        contract=parent_contract,
    )
    result = ToolCallResult(
        "submit_work_item_plan",
        {
            "summary": "one package",
            "requested_worker_count": 1,
            "work_items": [
                {
                    "id": "item",
                    "title": "Item",
                    "goal": "Implement item",
                    "file_path": "src/item.py",
                    "instructions": "Implement src/item.py",
                    "acceptance_criteria": ["complete"],
                    "dependencies": [],
                    "contract_id": "shared-contract",
                    "contract_version": 1,
                    "write_scopes": ["src/item.py"],
                    "test_focus": "tests",
                }
            ],
        },
        {},
    )

    planned = _manager_stream_from_result(
        result,
        stream,
        SchedulerLimits(max_workers_per_manager=2),
    )

    item = planned.work_items[0]
    assert item.contract.id == "ws:item-contract"
    assert item.contract.id != parent_contract.id
    assert item.metadata["planner_contract_id"] == "shared-contract"


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
    assert item.contract.id == "ws:auth-contract"
    assert item.metadata["planner_contract_id"] == "auth-contract"
    assert item.contract.version == 2
    assert item.contract.risk_level.value == "high"
    assert item.priority == 7
    assert _package_files(item) == item.metadata["package_files"]
    assert planned.metadata["fanout"] == {
        "selected": 1,
        "max": 4,
        "reason": "auth package",
        "execution_slots": 4,
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
