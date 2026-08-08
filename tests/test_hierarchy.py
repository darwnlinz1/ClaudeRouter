from pathlib import Path

from orchestrator.hierarchy import run_hierarchy
from orchestrator.llm_client import ToolCallResult
from orchestrator.scheduler import SchedulerLimits
from orchestrator.state_repository import StateRepository


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


def test_hierarchy_plans_managers_lazily_by_workstream_deps(tmp_path: Path):
    (tmp_path / "a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B = 1\n", encoding="utf-8")
    events = []
    plan_order: list[str] = []
    worker_before_second_plan = {"seen": False}

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
            if stream_id == "second":
                worker_before_second_plan["seen"] = any(
                    event.get("role") == "worker" and event.get("type") == "agent_started"
                    for event in events
                )
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
            task_id="task-lazy",
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
    assert plan_order == ["first", "second"]
    assert worker_before_second_plan["seen"] is True
    assert (tmp_path / "a.py").read_text(encoding="utf-8") == "A = 2\n"
    assert (tmp_path / "b.py").read_text(encoding="utf-8") == "B = 2\n"
