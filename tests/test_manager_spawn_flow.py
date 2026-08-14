from pathlib import Path

from orchestrator.hierarchy import run_hierarchy
from orchestrator.llm_client import ToolCallResult
from orchestrator.scheduler import SchedulerLimits
from orchestrator.state_repository import StateRepository


def _patch_for(user: str) -> ToolCallResult:
    for i in range(1, 5):
        token = f"f{i}.py"
        if token in user:
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "ok",
                    "patch_content": (
                        f"<<<< SEARCH\nX={i}\n====\nX=9\n>>>> REPLACE"
                    ),
                },
                {},
            )
    raise AssertionError(user[:200])


def test_independent_workstreams_start_all_managers(tmp_path: Path):
    for i in range(1, 5):
        (tmp_path / f"f{i}.py").write_text(f"X={i}\n", encoding="utf-8")
    events: list[dict] = []
    plans = {"n": 0}

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "4 independent",
                    "requested_manager_count": 4,
                    "workstreams": [
                        {
                            "id": f"w{i}",
                            "title": f"W{i}",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": [],
                            "write_scopes": [f"f{i}.py"],
                        }
                        for i in range(1, 5)
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            plans["n"] += 1
            stream = next(
                f"w{i}" for i in range(1, 5) if f"WORKSTREAM ID\nw{i}" in user_message
            )
            idx = stream[1]
            fp = f"f{idx}.py"
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "pkg",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "i",
                            "title": "t",
                            "goal": "g",
                            "file_path": fp,
                            "instructions": "x",
                            "acceptance_criteria": ["ok"],
                            "dependencies": [],
                            "write_scopes": [fp],
                            "test_focus": "",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            return _patch_for(user_message)
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "ok",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "ok",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {"verdict": "approved", "summary": "ok", "remaining_risks": []},
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        run_hierarchy(
            root=tmp_path,
            task_description="t",
            task_id="t-indep",
            limits=SchedulerLimits(
                max_parallel_managers=4,
                max_workers_per_manager=2,
                max_parallel_workers=8,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        attempts = repository.list_attempts("t-indep")

    mgr = [
        event.get("workstream_id")
        for event in events
        if event.get("type") == "agent_started" and event.get("role") == "manager"
    ]
    assert sorted(mgr) == ["w1", "w2", "w3", "w4"]
    assert plans["n"] == 4
    assert attempts
    assert all(
        attempt.execution_attempt_id != attempt.logical_agent_id
        for attempt in attempts
    )


def test_sequential_workstreams_start_managers_one_by_one(tmp_path: Path):
    for i in range(1, 5):
        (tmp_path / f"f{i}.py").write_text(f"X={i}\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "seq",
                    "requested_manager_count": 4,
                    "workstreams": [
                        {
                            "id": "w1",
                            "title": "W1",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": [],
                            "write_scopes": ["f1.py"],
                        },
                        {
                            "id": "w2",
                            "title": "W2",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": ["w1"],
                            "write_scopes": ["f2.py"],
                        },
                        {
                            "id": "w3",
                            "title": "W3",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": ["w2"],
                            "write_scopes": ["f3.py"],
                        },
                        {
                            "id": "w4",
                            "title": "W4",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": ["w3"],
                            "write_scopes": ["f4.py"],
                        },
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            stream = next(
                f"w{i}" for i in range(1, 5) if f"WORKSTREAM ID\nw{i}" in user_message
            )
            idx = stream[1]
            fp = f"f{idx}.py"
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "pkg",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "i",
                            "title": "t",
                            "goal": "g",
                            "file_path": fp,
                            "instructions": "x",
                            "acceptance_criteria": ["ok"],
                            "dependencies": [],
                            "write_scopes": [fp],
                            "test_focus": "",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            return _patch_for(user_message)
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "ok",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "ok",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {"verdict": "approved", "summary": "ok", "remaining_risks": []},
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        run_hierarchy(
            root=tmp_path,
            task_description="t",
            task_id="t-seq",
            limits=SchedulerLimits(
                max_parallel_managers=4,
                max_workers_per_manager=2,
                max_parallel_workers=8,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )

    timeline = []
    for event in events:
        if event.get("type") == "agent_started" and event.get("role") == "manager":
            if event.get("status") in {None, "queued", "planning"}:
                timeline.append(("start", event.get("workstream_id")))
        if event.get("type") == "workstream_completed":
            timeline.append(("done", event.get("workstream_id")))

    # All managers are announced/planned even with sequential deps.
    started = [item for kind, item in timeline if kind == "start"]
    assert started.count("w1") >= 1
    assert started.count("w2") >= 1
    assert started.count("w3") >= 1
    assert started.count("w4") >= 1
    manager_plans = [
        event.get("workstream_id")
        for event in events
        if event.get("type") == "manager_plan_created"
    ]
    assert sorted(manager_plans) == ["w1", "w2", "w3", "w4"]
    # Execution remains sequential: w2 cannot complete before w1.
    assert ("done", "w1") in timeline
    assert timeline.index(("done", "w1")) < timeline.index(("done", "w2"))
    assert timeline.index(("done", "w2")) < timeline.index(("done", "w3"))
    assert timeline.index(("done", "w3")) < timeline.index(("done", "w4"))
