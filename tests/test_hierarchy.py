from pathlib import Path

import pytest

from orchestrator import path_utils
from orchestrator.hierarchy import (
    _append_repository_handoff,
    _persist_contract_version,
    _preflight_plan,
    _resolve_repository_agent_id,
    run_hierarchy,
)
from orchestrator.llm_client import ToolCallResult
from orchestrator.models import (
    HandoffEnvelope,
    PlanStatus,
    TaskPlan,
    WorkContract,
    WorkItem,
    Workstream,
)
from orchestrator.scheduler import SchedulerLimits
from orchestrator.state_repository import StateRepository


def test_repository_coordination_hooks_are_feature_detected():
    class HookRepository:
        def __init__(self):
            self.contracts = []
            self.handoffs = []

        def resolve_agent_identity(
            self,
            task_id,
            role,
            assignment_id,
            proposed_id,
        ):
            return f"persisted:{task_id}:{role}:{assignment_id}"

        def save_contract_version(
            self,
            task_id,
            contract,
            owner_type,
            owner_id,
        ):
            self.contracts.append((task_id, contract, owner_type, owner_id))

        def append_handoff(self, envelope):
            self.handoffs.append(envelope)

    repository = HookRepository()
    contract = WorkContract(
        id="contract-1",
        input_artifacts=("spec.md",),
        expected_outputs=("src/result.py",),
        read_scopes=("spec.md",),
        write_scopes=("src/result.py",),
        acceptance_criteria=("Result works",),
        test_requirements=("pytest",),
        evidence_requirements=("test report",),
        consumers=("task",),
    )
    logical_id = _resolve_repository_agent_id(
        repository,
        task_id="task-1",
        role="worker",
        assignment_id="stream:item",
    )
    _persist_contract_version(
        repository,
        task_id="task-1",
        contract=contract,
        workstream_id="stream",
        work_item_id="stream:item",
    )
    handoff = HandoffEnvelope(
        handoff_id="handoff-1",
        task_id="task-1",
        contract_id=contract.id,
        contract_version=contract.version,
        source_agent_id="manager-1",
        target_agent_id=logical_id,
        signal_type="delegate_work_item",
    )
    _append_repository_handoff(repository, handoff)

    assert logical_id == "persisted:task-1:worker:stream:item"
    assert repository.contracts == [
        ("task-1", contract, "work_item", "stream:item")
    ]
    assert repository.handoffs == [handoff]


def test_preflight_rejects_missing_typed_contract_inputs(tmp_path: Path):
    item_contract = WorkContract(
        id="item-contract",
        input_artifacts=("specs/missing.md",),
        expected_outputs=("out.py",),
        read_scopes=("specs/",),
        write_scopes=("out.py",),
        acceptance_criteria=("Output works",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("core",),
    )
    item = WorkItem(
        id="core:output",
        workstream_id="core",
        title="Output",
        goal="Create output",
        acceptance_criteria=item_contract.acceptance_criteria,
        write_scopes=item_contract.write_scopes,
        contract=item_contract,
        metadata={
            "file_path": "out.py",
            "package_files": ["out.py"],
            "contract_mode": "typed",
        },
    )
    stream_contract = WorkContract(
        id="stream-contract",
        expected_outputs=("out.py",),
        write_scopes=("out.py",),
        acceptance_criteria=("Output works",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("task",),
    )
    plan = TaskPlan(
        task_id="task-1",
        session_id="session-1",
        goal="Create output",
        workstreams=(
            Workstream(
                id="core",
                title="Core",
                goal="Create output",
                acceptance_criteria=stream_contract.acceptance_criteria,
                write_scopes=stream_contract.write_scopes,
                work_items=(item,),
                contract=stream_contract,
                metadata={"contract_mode": "typed"},
            ),
        ),
        status=PlanStatus.READY,
    )

    issues = _preflight_plan(
        root=tmp_path,
        plan=plan,
        limits=SchedulerLimits(),
        allow_new_files=True,
    )

    assert "missing_contract_input" in {
        issue["code"] for issue in issues
    }


def test_hierarchy_runs_dynamic_manager_workers_and_tester(tmp_path: Path):
    (tmp_path / "a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B = 1\n", encoding="utf-8")
    events = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Một workstream có hai file độc lập.",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "core",
                            "title": "Core",
                            "goal": "Nâng hai hằng số.",
                            "acceptance_criteria": ["A và B bằng 2"],
                            "dependencies": [],
                            "write_scopes": ["a.py", "b.py"],
                        }
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Hai work item chạy độc lập.",
                    "requested_worker_count": 2,
                    "work_items": [
                        {
                            "id": "a",
                            "title": "Update A",
                            "goal": "A bằng 2",
                            "file_path": "a.py",
                            "instructions": "Đổi A từ 1 thành 2.",
                            "acceptance_criteria": ["A = 2"],
                            "dependencies": [],
                            "write_scopes": ["a.py"],
                            "test_focus": "Parse Python",
                        },
                        {
                            "id": "b",
                            "title": "Update B",
                            "goal": "B bằng 2",
                            "file_path": "b.py",
                            "instructions": "Đổi B từ 1 thành 2.",
                            "acceptance_criteria": ["B = 2"],
                            "dependencies": [],
                            "write_scopes": ["b.py"],
                            "test_focus": "Parse Python",
                        },
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            before, after = ("A = 1", "A = 2") if "a.py" in user_message else ("B = 1", "B = 2")
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Đã cập nhật hằng số.",
                    "patch_content": (
                        f"<<<< SEARCH\n{before}\n====\n{after}\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Patch đúng.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "Workstream hoàn tất.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "Tích hợp hoàn tất.",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Đổi A và B thành 2.",
            task_id="task-hierarchy",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=3,
                max_parallel_workers=2,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        plan = repository.get_plan("task-hierarchy")
        attempts = repository.list_attempts("task-hierarchy")
        effects = repository.list_effects("task-hierarchy")
        contracts = repository.list_contract_versions("task-hierarchy")
        handoffs = repository.list_handoffs("task-hierarchy")
        identities = repository.list_agent_identities("task-hierarchy")

    assert result.stopped_reason == "task_completed", {
        "final_state": result.final_state,
        "events": [
            {
                key: event.get(key)
                for key in (
                    "type",
                    "work_item_id",
                    "accepted",
                    "error",
                    "verdict",
                )
            }
            for event in events
        ],
    }
    assert (tmp_path / "a.py").read_text(encoding="utf-8") == "A = 2\n"
    assert (tmp_path / "b.py").read_text(encoding="utf-8") == "B = 2\n"
    assert plan is not None and plan.revision >= 3
    assert len(attempts) == 2
    patch_effects = [effect for effect in effects if effect.kind == "file_patch"]
    assert len(patch_effects) == 2
    assert all(effect.state.value == "applied" for effect in patch_effects)
    assert all(
        effect.before_sha256 != effect.after_sha256
        for effect in patch_effects
    )
    assert len(
        {
            event["agent_instance_id"]
            for event in events
            if event.get("role") == "worker"
            and event.get("type") == "agent_started"
        }
    ) == 2
    assert len(
        {
            event["agent_instance_id"]
            for event in events
            if event.get("role") == "tester"
            and event.get("type") == "agent_started"
        }
    ) == 1
    fanout = next(
        event for event in events if event.get("type") == "hierarchy_fanout_planned"
    )
    assert {
        "managers": fanout["manager_count"],
        "coders": fanout["coder_count"],
        "testers": fanout["tester_count"],
        "children": fanout["child_agent_count"],
    } == {"managers": 1, "coders": 2, "testers": 1, "children": 3}
    selections = [
        event for event in events if event.get("type") == "fanout_selected"
    ]
    assert [(event["level"], event["selected"]) for event in selections] == [
        ("manager", 1),
        ("worker", 2),
    ]
    reconciliation = next(
        event
        for event in events
        if event.get("type") == "completion_reconciliation"
    )
    assert reconciliation["balanced"] is True
    assert reconciliation["work_items"]["completed"] == 2
    manager_plan = next(
        event for event in events if event.get("type") == "manager_plan_created"
    )
    assert manager_plan["work_items"][0]["contract"]["version"] == 1
    assert all(
        event.get("contract_version") == 1
        for event in events
        if event.get("type") == "execution_result"
    )
    assert len(contracts) == 3
    assert handoffs
    assert {
        (handoff.contract_id, handoff.contract_version)
        for handoff in handoffs
    } <= {(contract.id, contract.version) for contract in contracts}
    assert {
        identity["logical_agent_id"] for identity in identities
    }.issuperset(
        {
            event["agent_instance_id"]
            for event in events
            if event.get("type") == "agent_started"
            and event.get("agent_instance_id")
        }
    )
    assert any(event.get("type") == "hierarchy_completed" for event in events)
    signal_types = {
        event.get("signal_type")
        for event in events
        if event.get("type") == "agent_message"
    }
    assert {
        "delegate_workstream",
        "delegate_work_item",
        "submit_patch",
        "review_result",
        "workstream_result",
    }.issubset(signal_types)


def test_hierarchy_plans_all_managers_eagerly_before_execution(tmp_path: Path):
    (tmp_path / "a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B = 1\n", encoding="utf-8")
    events = []
    plan_order: list[str] = []
    worker_during_planning = {"seen": False}

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Hai workstream tuần tự.",
                    "requested_manager_count": 2,
                    "workstreams": [
                        {
                            "id": "first",
                            "title": "First",
                            "goal": "Sửa a.py",
                            "acceptance_criteria": ["A = 2"],
                            "dependencies": [],
                            "write_scopes": ["a.py"],
                        },
                        {
                            "id": "second",
                            "title": "Second",
                            "goal": "Sửa b.py",
                            "acceptance_criteria": ["B = 2"],
                            "dependencies": ["first"],
                            "write_scopes": ["b.py"],
                        },
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            stream_id = "second" if "WORKSTREAM ID\nsecond" in user_message else "first"
            plan_order.append(stream_id)
            if any(
                event.get("role") == "worker"
                and event.get("type") == "agent_started"
                and event.get("status") == "running"
                for event in events
            ):
                worker_during_planning["seen"] = True
            file_name = "b.py" if stream_id == "second" else "a.py"
            item_id = "b" if stream_id == "second" else "a"
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": f"Một item cho {file_name}",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": item_id,
                            "title": f"Update {item_id.upper()}",
                            "goal": f"{item_id.upper()} bằng 2",
                            "file_path": file_name,
                            "instructions": f"Đổi nội dung {file_name}.",
                            "acceptance_criteria": [f"{item_id.upper()} = 2"],
                            "dependencies": [],
                            "write_scopes": [file_name],
                            "test_focus": "Parse Python",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            before, after = ("A = 1", "A = 2") if "a.py" in user_message else ("B = 1", "B = 2")
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Đã cập nhật.",
                    "patch_content": (
                        f"<<<< SEARCH\n{before}\n====\n{after}\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Patch đúng.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "Workstream hoàn tất.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "Tích hợp hoàn tất.",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Sửa a rồi b.",
            task_id="task-eager",
            limits=SchedulerLimits(
                max_parallel_managers=2,
                max_workers_per_manager=2,
                max_parallel_workers=2,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )

    assert result.stopped_reason == "task_completed"
    assert sorted(plan_order) == ["first", "second"]
    # Both Managers are planned before any Worker begins execution.
    assert worker_during_planning["seen"] is False
    manager_plans = [
        event.get("workstream_id")
        for event in events
        if event.get("type") == "manager_plan_created"
    ]
    assert sorted(manager_plans) == ["first", "second"]
    assert (tmp_path / "a.py").read_text(encoding="utf-8") == "A = 2\n"
    assert (tmp_path / "b.py").read_text(encoding="utf-8") == "B = 2\n"


def test_missing_edit_file_finishes_attempt_and_emits_preflight_failure(
    tmp_path: Path,
):
    events: list[dict] = []
    worker_calls = {"count": 0}

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "One stream",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "foundation",
                            "title": "Foundation",
                            "goal": "Create requirements",
                            "acceptance_criteria": ["requirements exists"],
                            "dependencies": [],
                            "write_scopes": ["requirements.txt"],
                        }
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "One item",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "requirements",
                            "title": "Requirements",
                            "goal": "Create requirements",
                            "file_path": "requirements.txt",
                            "instructions": "Create requirements.txt",
                            "acceptance_criteria": ["file exists"],
                            "dependencies": [],
                            "write_scopes": ["requirements.txt"],
                            "test_focus": "",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            worker_calls["count"] += 1
            raise AssertionError("Worker model must not be called after preflight failure")
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "revise",
                    "summary": "Wrong project mode",
                    "next_instructions": "Use new-project mode",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "revise",
                    "summary": "Preflight failed",
                    "remaining_risks": ["requirements missing"],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Create a project in edit mode",
            task_id="task-preflight",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        attempts = repository.list_attempts("task-preflight")
        final_plan = repository.get_plan("task-preflight")

    assert worker_calls["count"] == 0
    assert result.stopped_reason == "hierarchy_failed"
    assert len(attempts) == 1
    assert attempts[0].status.value == "failed"
    assert attempts[0].evidence["failure_kind"] == "preflight"
    assert any(
        event.get("type") == "agent_failed"
        and event.get("status") == "preflight_failed"
        for event in events
    )
    assert not any(
        event.get("type") == "agent_started"
        and event.get("role") in {"worker", "tester"}
        for event in events
    )
    preflight = next(
        event
        for event in events
        if event.get("type") == "plan_preflight_failed"
    )
    assert preflight["worker_calls_started"] == 0
    assert preflight["issues"][0]["code"] == "new_file_forbidden"
    assert final_plan is not None
    assert final_plan.workstreams[0].work_items[0].status.value == "failed"


def test_resume_skips_work_item_with_approved_evidence(tmp_path: Path):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    worker_calls = {"count": 0}
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Stable plan",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "core",
                            "title": "Core",
                            "goal": "Update value",
                            "acceptance_criteria": ["VALUE = 2"],
                            "dependencies": [],
                            "write_scopes": ["value.py"],
                        }
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Stable item",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "value",
                            "title": "Value",
                            "goal": "Update value",
                            "file_path": "value.py",
                            "instructions": "Set VALUE to 2",
                            "acceptance_criteria": ["VALUE = 2"],
                            "dependencies": [],
                            "write_scopes": ["value.py"],
                            "test_focus": "",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            worker_calls["count"] += 1
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated value",
                    "patch_content": (
                        "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Approved",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "Complete",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "Complete",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        first = run_hierarchy(
            root=tmp_path,
            task_description="Update value",
            task_id="task-resume-approved",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        resumed = run_hierarchy(
            root=tmp_path,
            task_description="Update value",
            task_id="task-resume-approved",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
            resume_session=True,
        )

    assert first.stopped_reason == "task_completed"
    assert resumed.stopped_reason == "task_completed"
    assert worker_calls["count"] == 1
    assert target.read_text(encoding="utf-8") == "VALUE = 2\n"
    for role in ("director", "manager", "worker", "tester"):
        ids = {
            event.get("agent_instance_id")
            for event in events
            if event.get("role") == role and event.get("agent_instance_id")
        }
        assert len(ids) == 1, (role, ids)
    assert len({event.get("session_id") for event in events}) == 1
    assert all(
        event.get("balanced")
        for event in events
        if event.get("type") == "completion_reconciliation"
    )


def test_manager_recovery_retries_only_failed_item(tmp_path: Path):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    worker_calls = {"count": 0}
    manager_reviews = {"count": 0}
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "One stream",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "core",
                            "title": "Core",
                            "goal": "Update value",
                            "acceptance_criteria": ["valid Python"],
                            "dependencies": [],
                            "write_scopes": ["value.py"],
                        }
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "One item",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "value",
                            "title": "Value",
                            "goal": "Update value",
                            "file_path": "value.py",
                            "instructions": "Set a valid value",
                            "acceptance_criteria": ["valid Python"],
                            "dependencies": [],
                            "write_scopes": ["value.py"],
                            "test_focus": "syntax",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            worker_calls["count"] += 1
            replacement = (
                "VALUE = 2" if worker_calls["count"] == 3 else "VALUE = ("
            )
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Attempted update",
                    "patch_content": (
                        "<<<< SEARCH\nVALUE = 1\n====\n"
                        f"{replacement}\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Valid",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            manager_reviews["count"] += 1
            approved = manager_reviews["count"] > 1
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved" if approved else "revise",
                    "summary": "Complete" if approved else "Fix syntax",
                    "next_instructions": "" if approved else "Return valid Python.",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "Complete",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Update value",
            task_id="task-manager-recovery",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )

    assert result.stopped_reason == "task_completed"
    assert worker_calls["count"] == 3
    assert manager_reviews["count"] == 2
    assert target.read_text(encoding="utf-8") == "VALUE = 2\n"
    assert any(event.get("type") == "manager_replan_created" for event in events)


def test_multi_file_retry_resumes_from_first_unfinished_file(tmp_path: Path):
    (tmp_path / "a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B = 1\n", encoding="utf-8")
    calls = {"a": 0, "b": 0}

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Package",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "core",
                            "title": "Core",
                            "goal": "Update package",
                            "acceptance_criteria": ["Both files valid"],
                            "dependencies": [],
                            "write_scopes": ["a.py", "b.py"],
                        }
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "One package",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "package",
                            "title": "Package",
                            "goal": "Update both files",
                            "file_path": "a.py",
                            "instructions": "Update both values",
                            "acceptance_criteria": ["A and B equal 2"],
                            "dependencies": [],
                            "write_scopes": ["a.py", "b.py"],
                            "test_focus": "syntax",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            if "## TARGET FILE: a.py" in user_message:
                calls["a"] += 1
                before, after = "A = 1", "A = 2"
            else:
                calls["b"] += 1
                before = "B = 1"
                after = "B = (" if calls["b"] == 1 else "B = 2"
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated file",
                    "patch_content": (
                        f"<<<< SEARCH\n{before}\n====\n{after}\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Valid",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "Complete",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "Complete",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Update package",
            task_id="task-package-resume",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            repository=repository,
        )

    assert result.stopped_reason == "task_completed"
    assert calls == {"a": 1, "b": 2}
    assert (tmp_path / "a.py").read_text(encoding="utf-8") == "A = 2\n"
    assert (tmp_path / "b.py").read_text(encoding="utf-8") == "B = 2\n"


def test_selected_source_files_ground_director_and_relevant_manager(
    tmp_path: Path,
):
    source = tmp_path / "spec.txt"
    source.write_text(
        "SOURCE-BEGIN\n" + ("grounded-context-" * 3000) + "\nSOURCE-END",
        encoding="utf-8",
    )
    prompts: dict[str, str] = {}

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            prompts["director"] = user_message
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Grounded stream",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "core",
                            "title": "Core",
                            "goal": "Implement the selected specification",
                            "acceptance_criteria": ["implementation exists"],
                            "dependencies": [],
                            "read_scopes": ["spec.txt"],
                            "write_scopes": ["new.py"],
                        }
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            prompts["manager"] = user_message
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "One grounded item",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "implementation",
                            "title": "Implementation",
                            "goal": "Implement the spec",
                            "file_path": "new.py",
                            "instructions": "Implement from spec.txt",
                            "acceptance_criteria": ["implementation exists"],
                            "dependencies": [],
                            "write_scopes": ["new.py"],
                            "test_focus": "",
                        }
                    ],
                },
                {},
            )
        raise AssertionError(f"Unexpected model call after planning: {names}")

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Implement the selected specification.",
            task_id="task-source-grounding",
            source_files=["spec.txt"],
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            repository=repository,
        )

    assert result.stopped_reason == "hierarchy_failed"
    for role in ("director", "manager"):
        assert "### FILE: spec.txt" in prompts[role]
        assert "SOURCE-BEGIN" in prompts[role]
        assert "SOURCE-END" in prompts[role]
        assert "[TRUNCATED]" in prompts[role]


def test_sensitive_selected_source_is_rejected_before_planner_call(
    tmp_path: Path,
):
    (tmp_path / ".env").write_text("TOKEN=secret\n", encoding="utf-8")
    calls = {"count": 0}

    def fake_call(system_prompt, user_message, tools):
        calls["count"] += 1
        raise AssertionError("Planner must not receive sensitive source files")

    with StateRepository(":memory:") as repository:
        with pytest.raises(path_utils.SensitivePathError):
            run_hierarchy(
                root=tmp_path,
                task_description="Inspect configuration.",
                task_id="task-sensitive-source",
                source_files=[".env"],
                llm_call=fake_call,
                repository=repository,
            )

    assert calls["count"] == 0
