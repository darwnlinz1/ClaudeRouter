import re
import time
from collections import defaultdict
from pathlib import Path
from threading import Barrier, Lock

from orchestrator import llm_client
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
                    "patch_content": (f"<<<< SEARCH\nX={i}\n====\nX=9\n>>>> REPLACE"),
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
            stream = next(f"w{i}" for i in range(1, 5) if f"WORKSTREAM ID\nw{i}" in user_message)
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

    mgr = {
        event.get("workstream_id")
        for event in events
        if event.get("type") == "agent_started" and event.get("role") == "manager"
    }
    assert sorted(mgr) == ["w1", "w2", "w3", "w4"]
    assert plans["n"] == 4
    assert attempts
    assert all(attempt.execution_attempt_id != attempt.logical_agent_id for attempt in attempts)


def test_dependent_workstreams_all_start_without_waiting_for_upstream(tmp_path: Path):
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
                        # Each stream reads what the one before it writes, so
                        # these edges are load bearing and survive the pass that
                        # releases dependencies no shared file justifies.
                        {
                            "id": "w2",
                            "title": "W2",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": ["w1"],
                            "write_scopes": ["f2.py"],
                            "input_artifacts": ["f1.py"],
                        },
                        {
                            "id": "w3",
                            "title": "W3",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": ["w2"],
                            "write_scopes": ["f3.py"],
                            "input_artifacts": ["f2.py"],
                        },
                        {
                            "id": "w4",
                            "title": "W4",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            "dependencies": ["w3"],
                            "write_scopes": ["f4.py"],
                            "input_artifacts": ["f3.py"],
                        },
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            stream = next(f"w{i}" for i in range(1, 5) if f"WORKSTREAM ID\nw{i}" in user_message)
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
            if event.get("status") in {None, "planned", "planning"}:
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
    # Maximum-parallelism mode treats dependency edges as context, not a gate.
    # Every Manager starts before the first workstream completes.
    first_done = next(index for index, item in enumerate(timeline) if item[0] == "done")
    assert {item for kind, item in timeline if kind == "done"} == {"w1", "w2", "w3", "w4"}
    assert {item for kind, item in timeline[:first_done] if kind == "start"} == {
        "w1",
        "w2",
        "w3",
        "w4",
    }


def test_narrative_chain_runs_in_parallel(tmp_path: Path):
    """A chain nothing reads must not serialise the run.

    A Director described four packages in the order it imagined building them,
    which turned eleven ready workers into waves of 5, 2, 1 and 3. None of the
    streams read a file another produced, so the ordering was decoration.
    """
    for index in range(1, 5):
        (tmp_path / f"f{index}.py").write_text(f"X={index}\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "chain",
                    "requested_manager_count": 4,
                    "workstreams": [
                        {
                            "id": f"w{index}",
                            "title": f"W{index}",
                            "goal": "g",
                            "acceptance_criteria": ["ok"],
                            # Narrative order only: no stream reads another's file.
                            "dependencies": [] if index == 1 else [f"w{index - 1}"],
                            "write_scopes": [f"f{index}.py"],
                        }
                        for index in range(1, 5)
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            stream = next(f"w{i}" for i in range(1, 5) if f"WORKSTREAM ID\nw{i}" in user_message)
            file_path = f"f{stream[1]}.py"
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
                            "file_path": file_path,
                            "instructions": "x",
                            "acceptance_criteria": ["ok"],
                            "dependencies": [],
                            "write_scopes": [file_path],
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
                {"verdict": "approved", "reviewer_feedback": "ok", "next_instructions": ""},
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {"verdict": "approved", "summary": "ok", "next_instructions": ""},
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
            task_id="t-chain",
            limits=SchedulerLimits(
                max_parallel_managers=4,
                max_workers_per_manager=2,
                max_parallel_workers=8,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        plan = repository.get_plan("t-chain")

    released = [event for event in events if event.get("type") == "plan_dependencies_relaxed"]
    assert released, "the decorative chain should have been reported as released"
    assert len(released[0]["dropped"]) == 3

    assert plan is not None
    assert all(stream.dependencies == () for stream in plan.workstreams)

    # Every workstream is free from the first wave rather than queued behind one.
    order = [
        event.get("workstream_id")
        for event in events
        if event.get("type") in {"workstream_started", "workstream_completed"}
    ]
    started_before_first_completion = {
        event.get("workstream_id")
        for event in events[: next(
            index
            for index, candidate in enumerate(events)
            if candidate.get("type") == "workstream_completed"
        )]
        if event.get("type") == "workstream_started"
    }
    assert len(started_before_first_completion) == 4, order


def test_frontend_four_by_four_shape_calls_all_25_logical_agents(tmp_path: Path, monkeypatch):
    # This test measures how many workers the scheduler runs at once. The launch
    # stagger exists to spread real provider connections over a second or two,
    # which is invisible against a ten-minute model call but dwarfs the 5ms fake
    # call below, so it is switched off here to keep the measurement about
    # concurrency rather than pacing.
    monkeypatch.setenv("ORCH_WORKER_LAUNCH_STAGGER_SECONDS", "0")
    manager_count = 4
    coders_per_manager = 4
    for manager_index in range(1, manager_count + 1):
        for worker_index in range(1, coders_per_manager + 1):
            (tmp_path / f"m{manager_index}_w{worker_index}.py").write_text(
                "VALUE = 0\n",
                encoding="utf-8",
            )

    events: list[dict] = []
    calls: list[tuple[str, str, tuple[str, ...]]] = []
    tester_guard = Lock()
    active_testers: dict[str, int] = defaultdict(int)
    peak_testers: dict[str, int] = defaultdict(int)
    worker_guard = Lock()
    active_workers = 0
    peak_workers = 0
    worker_barrier = Barrier(manager_count * coders_per_manager)

    def fake_call(system_prompt, user_message, tools):
        names = tuple(tool["name"] for tool in tools)
        calls.append(
            (
                str(getattr(llm_client.thread_local, "agent_role", "")),
                str(getattr(llm_client.thread_local, "agent_instance_id", "")),
                names,
            )
        )
        if names == ("submit_workstream_plan",):
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Four independent workstreams",
                    "requested_manager_count": manager_count,
                    "workstreams": [
                        {
                            "id": f"stream-{manager_index}",
                            "title": f"Stream {manager_index}",
                            "goal": f"Complete stream {manager_index}",
                            "acceptance_criteria": ["All four items complete"],
                            "dependencies": [],
                            "write_scopes": [
                                f"m{manager_index}_w{worker_index}.py"
                                for worker_index in range(1, coders_per_manager + 1)
                            ],
                        }
                        for manager_index in range(1, manager_count + 1)
                    ],
                },
                {},
            )
        if names == ("submit_work_item_plan",):
            match = re.search(r"## WORKSTREAM ID\nstream-(\d+)", user_message)
            assert match is not None
            manager_index = int(match.group(1))
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Four independent coder items",
                    "requested_worker_count": coders_per_manager,
                    "work_items": [
                        {
                            "id": f"item-{worker_index}",
                            "title": f"Item {worker_index}",
                            "goal": f"Complete item {worker_index}",
                            "file_path": f"m{manager_index}_w{worker_index}.py",
                            "instructions": "Set VALUE to 1",
                            "acceptance_criteria": ["VALUE equals 1"],
                            "dependencies": [],
                            "write_scopes": [f"m{manager_index}_w{worker_index}.py"],
                            "test_focus": "",
                        }
                        for worker_index in range(1, coders_per_manager + 1)
                    ],
                },
                {},
            )
        if names == ("submit_patch",):
            nonlocal active_workers, peak_workers
            with worker_guard:
                active_workers += 1
                peak_workers = max(peak_workers, active_workers)
            worker_barrier.wait(timeout=10)
            with worker_guard:
                active_workers -= 1
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated the assigned item.",
                    "patch_content": "<<<< SEARCH\nVALUE = 0\n====\nVALUE = 1\n>>>> REPLACE",
                },
                {},
            )
        if names == ("review_patch",):
            tester_id = str(getattr(llm_client.thread_local, "agent_instance_id", ""))
            with tester_guard:
                active_testers[tester_id] += 1
                peak_testers[tester_id] = max(
                    peak_testers[tester_id],
                    active_testers[tester_id],
                )
            time.sleep(0.005)
            with tester_guard:
                active_testers[tester_id] -= 1
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Approved.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ("complete_workstream",):
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "All four coder items passed.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ("complete_plan",):
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "All workstreams passed.",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Complete sixteen independent requirements.",
            task_id="task-4x4",
            limits=SchedulerLimits(
                max_managers=manager_count,
                max_parallel_managers=manager_count,
                max_workers_per_manager=coders_per_manager + 1,
                max_parallel_workers_per_manager=coders_per_manager,
                max_parallel_workers=3,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )

    assert result.stopped_reason == "task_completed"
    fanout = next(event for event in events if event.get("type") == "hierarchy_fanout_planned")
    assert fanout["manager_count"] == 4
    assert fanout["coder_count"] == 16
    assert fanout["tester_count"] == 4
    assert fanout["primary_agent_count"] == 25
    assert len(fanout["primary_agent_ids"]) == len(set(fanout["primary_agent_ids"])) == 25
    reconciliation = result.final_state["reconciliation"]
    assert reconciliation["balanced"] is True
    assert reconciliation["agent_calls"]["planned"] == 25
    assert reconciliation["agent_calls"]["completed"] == 25

    def calls_for(tool_name: str) -> list[tuple[str, str, tuple[str, ...]]]:
        return [call for call in calls if call[2] == (tool_name,)]

    assert len({call[1] for call in calls_for("submit_workstream_plan")}) == 1
    assert len(calls_for("submit_work_item_plan")) == 4
    assert len({call[1] for call in calls_for("submit_work_item_plan")}) == 4
    assert len(calls_for("submit_patch")) == 16
    assert len({call[1] for call in calls_for("submit_patch")}) == 16
    assert peak_workers == 16
    assert len(calls_for("review_patch")) == 16
    assert len({call[1] for call in calls_for("review_patch")}) == 4
    assert set(peak_testers) == {call[1] for call in calls_for("review_patch")}
    assert set(peak_testers.values()) == {1}
    assert len(calls_for("complete_workstream")) == 4
    assert len({call[1] for call in calls_for("complete_workstream")}) == 4
    purposes = reconciliation["agent_call_purposes"]
    assert purposes[calls_for("submit_workstream_plan")[0][1]] == ["plan", "review"]
    assert all(purposes[call[1]] == ["execute"] for call in calls_for("submit_patch"))
    assert all(purposes[call[1]] == ["test"] for call in calls_for("review_patch"))

    announced = {
        str(event.get("agent_instance_id"))
        for event in events
        if event.get("type") == "agent_started"
        and event.get("role") in {"director", "manager", "worker", "tester"}
    }
    assert announced == set(fanout["primary_agent_ids"])
    for manager_index in range(1, manager_count + 1):
        for worker_index in range(1, coders_per_manager + 1):
            assert (tmp_path / f"m{manager_index}_w{worker_index}.py").read_text(
                encoding="utf-8"
            ) == "VALUE = 1\n"
