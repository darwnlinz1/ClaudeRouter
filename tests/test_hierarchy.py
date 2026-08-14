import inspect
from dataclasses import replace
from pathlib import Path
from threading import Barrier

import pytest

from orchestrator import hierarchy as hierarchy_module
from orchestrator import path_utils
from orchestrator.hierarchy import (
    _append_repository_handoff,
    _persist_contract_version,
    _preflight_plan,
    _resolve_repository_agent_id,
    _runtime_missing_contract_inputs,
    _strip_path_annotation,
    run_hierarchy,
)
from orchestrator.llm_client import AccountPoolExhaustedError, ToolCallResult
from orchestrator.models import (
    AttemptStatus,
    HandoffEnvelope,
    PlanStatus,
    TaskPlan,
    WorkContract,
    WorkItem,
    WorkStatus,
    Workstream,
)
from orchestrator.scheduler import SchedulerLimits
from orchestrator.state_repository import StateRepository
from orchestrator.ticket_executor import TicketExecutionResult


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
    assert repository.contracts == [("task-1", contract, "work_item", "stream:item")]
    assert repository.handoffs == [handoff]


def test_resume_advances_changed_immutable_contract_version(tmp_path: Path):
    original = WorkContract(
        id="stream-contract",
        version=1,
        write_scopes=("src/value.py",),
        expected_outputs=("src/value.py",),
        acceptance_criteria=("Value is one",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("task",),
    )
    changed = replace(original, acceptance_criteria=("Value is two",))
    plan = TaskPlan(
        task_id="task-contract-revision",
        session_id="session-contract-revision",
        goal="Update value",
        requested_manager_count=1,
        workstreams=(
            Workstream(
                id="stream",
                title="Stream",
                goal="Update value",
                acceptance_criteria=changed.acceptance_criteria,
                write_scopes=changed.write_scopes,
                requested_worker_count=1,
                contract=changed,
            ),
        ),
    )

    with StateRepository(tmp_path / "contracts.sqlite3") as repository:
        repository.save_contract_version(
            plan.task_id,
            original,
            owner_type="workstream",
            owner_id="stream",
        )
        reconciled = hierarchy_module._advance_conflicting_contract_versions(
            repository,
            plan,
        )

    assert reconciled.workstreams[0].contract.version == 2
    assert reconciled.workstreams[0].contract.acceptance_criteria == ("Value is two",)


def test_hierarchy_has_no_global_work_item_attempt_budget():
    assert "max_attempts_per_item" not in inspect.signature(run_hierarchy).parameters
    source = inspect.getsource(hierarchy_module._run_hierarchy_impl)
    assert "attempt_budget_exhausted" not in source
    assert "for attempt_number in range" not in source


def test_typed_behavior_contract_requires_configured_integration_before_worker_identity(
    tmp_path: Path,
):
    worker_calls = []
    events = []

    def fake_call(system_prompt, user_message, tools):
        name = tools[0]["name"]
        if name == "submit_workstream_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One source workstream",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "core",
                            "title": "Core",
                            "goal": "Create source",
                            "acceptance_criteria": ["Feature behaves correctly"],
                            "dependencies": [],
                            "write_scopes": ["generator.py"],
                        }
                    ],
                },
                {},
            )
        if name == "submit_work_item_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One typed source package",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "generator",
                            "title": "Generator",
                            "goal": "Generate a runtime image",
                            "file_path": "generator.py",
                            "instructions": "Write source that creates background.jpg at runtime.",
                            "dependencies": [],
                            "contract_id": "core:generator-contract",
                            "contract_version": 1,
                            "input_artifacts": [],
                            "expected_outputs": ["generator.py", "background.jpg"],
                            "read_scopes": [],
                            "write_scopes": ["generator.py"],
                            "acceptance_criteria": ["Runtime output is correct"],
                            "test_requirements": ["pytest verifies runtime output"],
                            "evidence_requirements": ["patch hash"],
                            "consumers": ["core"],
                            "risk_level": "low",
                            "priority": 0,
                            "test_focus": "runtime behavior",
                        }
                    ],
                },
                {},
            )
        if name == "complete_plan":
            return ToolCallResult(
                name,
                {
                    "verdict": "revise",
                    "summary": "Plan admission prevented execution.",
                    "remaining_risks": ["integration command missing"],
                },
                {},
            )
        worker_calls.append(name)
        raise AssertionError("No Worker/Tester call is allowed after admission rejection")

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Create a runtime image generator.",
            task_id="task-integration-required",
            allow_new_files=True,
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        attempts = repository.list_attempts("task-integration-required")

    assert result.stopped_reason == "task_partial"
    assert attempts == []
    assert worker_calls == []
    assert not any(
        event.get("role") == "worker"
        and event.get("type") in {"agent_planned", "agent_started", "model_request_started"}
        for event in events
    )
    assert any(
        event.get("role") == "worker"
        and event.get("type") == "work_item_skipped"
        for event in events
    )
    admission = next(event for event in events if event["type"] == "plan_admission_rejected")
    assert {issue["code"] for issue in admission["issues"]} == {
        "integration_test_not_configured"
    }


def test_typed_item_materializes_only_after_exact_integration_pass(
    tmp_path: Path,
    monkeypatch,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    events = []
    materialized = []
    integration_seen = {"passed": False}

    def fake_sandbox(root, cmd, *, cancelled=None):
        if not cmd:
            return (
                "not_configured",
                "",
                {
                    "actual_isolation": "none",
                    "sandbox_outcome": None,
                    "cancelled": False,
                },
            )
        return (
            "passed",
            "1 passed",
            {
                "actual_isolation": "strong",
                "sandbox_outcome": "completed",
                "cancelled": False,
            },
        )

    monkeypatch.setattr(hierarchy_module.safety, "run_sandbox_tests_detailed", fake_sandbox)
    monkeypatch.setattr(
        hierarchy_module.safety,
        "SNAPSHOTS_ROOT",
        tmp_path / "snapshots",
    )

    def on_event(event):
        events.append(event)
        if event.get("type") == "integration_result" and event.get("status") == "passed":
            integration_seen["passed"] = True

    def on_file_approved(path):
        assert integration_seen["passed"] is True
        materialized.append(path)

    def fake_call(system_prompt, user_message, tools):
        name = tools[0]["name"]
        if name == "submit_workstream_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One stream",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "core",
                            "title": "Core",
                            "goal": "Update value",
                            "acceptance_criteria": ["Behavior is verified"],
                            "dependencies": [],
                            "write_scopes": ["value.py"],
                        }
                    ],
                },
                {},
            )
        if name == "submit_work_item_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One typed item",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "value",
                            "title": "Value",
                            "goal": "Update value",
                            "file_path": "value.py",
                            "instructions": "Set VALUE to 2.",
                            "dependencies": [],
                            "contract_id": "core:value-contract",
                            "contract_version": 1,
                            "input_artifacts": [],
                            "expected_outputs": ["value.py"],
                            "read_scopes": [],
                            "write_scopes": ["value.py"],
                            "acceptance_criteria": ["Behavior is verified"],
                            "test_requirements": ["pytest verifies behavior"],
                            "evidence_requirements": ["patch hash"],
                            "consumers": ["core"],
                            "risk_level": "low",
                            "priority": 0,
                            "test_focus": "behavior",
                        }
                    ],
                },
                {},
            )
        if name == "submit_patch":
            return ToolCallResult(
                name,
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE",
                },
                {},
            )
        if name == "review_patch":
            return ToolCallResult(
                name,
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Code review complete",
                    "next_instructions": "",
                },
                {},
            )
        if name == "complete_workstream":
            return ToolCallResult(
                name,
                {
                    "verdict": "approved",
                    "summary": "Code review complete",
                    "next_instructions": "",
                },
                {},
            )
        if name == "complete_plan":
            return ToolCallResult(
                name,
                {
                    "verdict": "approved",
                    "summary": "Integration passed",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(name)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Update and verify value.",
            task_id="task-two-phase",
            test_cmd=["pytest", "-q"],
            llm_call=fake_call,
            on_event=on_event,
            on_file_approved=on_file_approved,
            repository=repository,
        )
        attempts = repository.list_attempts("task-two-phase")

    assert result.stopped_reason == "task_completed"
    assert materialized == ["value.py"]
    assert attempts[-1].evidence["test_status"] == "passed"
    assert attempts[-1].evidence["test_scope"] == "integration"
    assert result.final_state["approved_file_paths"] == ["value.py"]


def test_plan_preflight_defers_missing_typed_contract_inputs(tmp_path: Path):
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

    assert "missing_contract_input" not in {issue["code"] for issue in issues}
    assert _runtime_missing_contract_inputs(tmp_path, item_contract) == ("specs/missing.md",)
    (tmp_path / "specs").mkdir()
    (tmp_path / "specs" / "missing.md").write_text("ready", encoding="utf-8")
    assert _runtime_missing_contract_inputs(tmp_path, item_contract) == ()


def test_declared_text_inputs_are_loaded_into_worker_context(tmp_path: Path):
    (tmp_path / "core").mkdir()
    (tmp_path / "core" / "compositor.py").write_text(
        "class Compositor:\n    def compose(self, frame, mask): ...\n",
        encoding="utf-8",
    )
    (tmp_path / "core" / "state.py").write_text(
        "class ApplicationState: ...\n",
        encoding="utf-8",
    )
    contract = WorkContract(
        id="tests-compositor",
        input_artifacts=("core/compositor.py", "assets/background.jpg"),
        expected_outputs=("tests/test_compositor.py",),
        read_scopes=(
            "core/compositor.py",
            "core/state.py",
            "assets/background.jpg",
        ),
        write_scopes=("tests/test_compositor.py",),
        acceptance_criteria=("Tests match the implementation",),
        test_requirements=("pytest",),
        evidence_requirements=("passing test",),
        consumers=("tests",),
    )

    context = hierarchy_module._contract_input_context(tmp_path, contract)

    assert "### core/compositor.py" in context
    assert "def compose(self, frame, mask)" in context
    assert "### core/state.py" in context
    assert "class ApplicationState" in context
    assert "background.jpg" not in context


def test_planner_path_annotations_are_stripped_before_scope_checks():
    # A Manager answered with "data/config.json (schema only, file created at
    # runtime)" in input_artifacts, which preflight then measured against the
    # contract scopes as if the whole sentence were a filename.
    assert _strip_path_annotation("data/config.json (schema only, created later)") == (
        "data/config.json"
    )


def test_manager_plan_conversion_rejects_runtime_binary_worker_target():
    with pytest.raises(ValueError, match="Unsupported Worker target"):
        hierarchy_module._normalize_work_item_scopes(
            {
                "id": "background",
                "file_path": "assets/background.jpg",
                "write_scopes": ["assets/background.jpg"],
            }
        )
    # The commonest shape in practice: a path followed by its description.
    assert _strip_path_annotation("app/capture.py: WebcamCapture class exposing start") == (
        "app/capture.py"
    )
    assert _strip_path_annotation("config.json: default application settings") == "config.json"
    assert _strip_path_annotation("app/foo.py (helper): does a thing") == "app/foo.py"
    assert _strip_path_annotation("core/state.py") == "core/state.py"
    assert _strip_path_annotation("tests/") == "tests/"
    # Prose is untouched, and a drive letter is not mistaken for a description.
    assert _strip_path_annotation("Build the pipeline (fast)") == "Build the pipeline (fast)"
    assert _strip_path_annotation("Returns 200: not a path") == "Returns 200: not a path"
    assert _strip_path_annotation("C:/absolute/path.py") == "C:/absolute/path.py"
    # "./x" and "x" are the same file; a planner writing the former used to lose
    # its work item to an unsafe_target check about spelling.
    assert _strip_path_annotation("./.gitignore") == ".gitignore"
    assert _strip_path_annotation("./scripts/run.py") == "scripts/run.py"


def test_manager_plan_conversion_accepts_directory_fixture_target():
    primary, scopes = hierarchy_module._normalize_work_item_scopes(
        {
            "id": "vault-core",
            "file_path": "tests/fixtures/",
            "write_scopes": ["tests/fixtures/"],
        }
    )
    assert primary == "tests/fixtures/"
    assert scopes == ("tests/fixtures/",)


def _directory_fixture_plan(file_path: str = "tests/fixtures/") -> TaskPlan:
    contract = WorkContract(
        id="vault-core-contract",
        expected_outputs=(file_path,),
        write_scopes=(file_path,),
        acceptance_criteria=("Fixtures stay valid",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("vault",),
    )
    item = WorkItem(
        id="vault-core:fixtures",
        workstream_id="vault-core",
        title="Fixtures",
        goal="Keep fixture files consistent",
        acceptance_criteria=contract.acceptance_criteria,
        write_scopes=contract.write_scopes,
        contract=contract,
        metadata={"file_path": file_path, "package_files": [file_path]},
    )
    stream_contract = WorkContract(
        id="vault-core-stream-contract",
        expected_outputs=(file_path,),
        write_scopes=(file_path,),
        acceptance_criteria=("Fixtures stay valid",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("task",),
    )
    return TaskPlan(
        task_id="task-vault",
        session_id="session-vault",
        goal="Maintain vault fixtures",
        workstreams=(
            Workstream(
                id="vault-core",
                title="Vault core",
                goal="Maintain fixtures",
                acceptance_criteria=stream_contract.acceptance_criteria,
                write_scopes=stream_contract.write_scopes,
                work_items=(item,),
                contract=stream_contract,
            ),
        ),
        status=PlanStatus.READY,
    )


def test_preflight_expands_directory_fixture_target(tmp_path: Path):
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True)
    (fixtures / "note.md").write_text("# note\n", encoding="utf-8")
    (fixtures / "sample.json").write_text("{}\n", encoding="utf-8")
    (fixtures / "image.png").write_bytes(b"\x89PNG\r\n")

    plan = _directory_fixture_plan()
    warnings: list[dict] = []
    issues = _preflight_plan(
        root=tmp_path,
        plan=plan,
        limits=SchedulerLimits(),
        allow_new_files=False,
        warnings=warnings,
    )

    blocking = {issue["code"] for issue in issues}
    assert "missing_concrete_target" not in blocking
    assert "unsupported_worker_target" not in blocking
    assert issues == []
    assert any(warning["code"] == "directory_target_expanded" for warning in warnings)
    item = plan.workstreams[0].work_items[0]
    assert hierarchy_module._package_files(item, root=tmp_path) == [
        "tests/fixtures/note.md",
        "tests/fixtures/sample.json",
    ]


def test_preflight_empty_directory_fixture_is_advisory(tmp_path: Path):
    (tmp_path / "tests" / "fixtures").mkdir(parents=True)

    plan = _directory_fixture_plan()
    warnings: list[dict] = []
    issues = _preflight_plan(
        root=tmp_path,
        plan=plan,
        limits=SchedulerLimits(),
        allow_new_files=False,
        warnings=warnings,
    )

    blocking = {issue["code"] for issue in issues}
    assert "missing_concrete_target" not in blocking
    assert "unsupported_worker_target" not in blocking
    assert issues == []
    assert any(warning["code"] == "directory_target_unexpanded" for warning in warnings)
    assert not any(
        "Unsupported Worker target" in str(warning.get("message") or "") for warning in warnings
    )


def _stream_for_dependency_test(
    stream_id: str,
    *,
    writes: tuple[str, ...],
    reads: tuple[str, ...] = (),
    dependencies: tuple[str, ...] = (),
) -> Workstream:
    contract = WorkContract(
        id=f"{stream_id}-contract",
        input_artifacts=reads,
        expected_outputs=writes,
        read_scopes=reads,
        write_scopes=writes,
        acceptance_criteria=("Works",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("task",),
    )
    return Workstream(
        id=stream_id,
        title=stream_id,
        goal=stream_id,
        acceptance_criteria=contract.acceptance_criteria,
        write_scopes=writes,
        dependencies=dependencies,
        work_items=(),
        contract=contract,
    )


def test_dependencies_no_file_justifies_are_released(tmp_path: Path):
    """A narrative chain must not serialise streams that share nothing.

    Taken from a real plan: core -> backend -> frontend -> tests. Only the first
    edge was real; the rest existed because the Director described the system in
    that order, and they reduced eleven parallel workers to waves of 5, 2, 1, 3.
    """
    streams = [
        _stream_for_dependency_test("core-cv-pipeline", writes=("core/pipeline.py",)),
        _stream_for_dependency_test(
            "backend-api",
            writes=("app.py",),
            # Genuinely reads what core writes: this edge is load bearing.
            reads=("core/pipeline.py",),
            dependencies=("core-cv-pipeline",),
        ),
        _stream_for_dependency_test(
            "frontend-web-ui",
            writes=("web/index.html", "web/app.js"),
            dependencies=("backend-api",),
        ),
        _stream_for_dependency_test(
            "tests-scripts-docs",
            writes=("tests/test_all.py", "README.md"),
            dependencies=("core-cv-pipeline", "backend-api", "frontend-web-ui"),
        ),
    ]

    relaxed, dropped = hierarchy_module._relax_decorative_dependencies(streams)
    by_id = {stream.id: stream for stream in relaxed}

    # The one edge backed by a real file survives.
    assert by_id["backend-api"].dependencies == ("core-cv-pipeline",)
    # The narrative edges are gone, so these start immediately.
    assert by_id["frontend-web-ui"].dependencies == ()
    assert by_id["tests-scripts-docs"].dependencies == ()
    assert len(dropped) == 4

    independent = [stream.id for stream in relaxed if not stream.dependencies]
    assert independent == ["core-cv-pipeline", "frontend-web-ui", "tests-scripts-docs"]


def test_dependency_is_kept_when_write_scopes_would_collide():
    """Two streams writing the same file must stay ordered."""
    streams = [
        _stream_for_dependency_test("first", writes=("shared/config.py",)),
        _stream_for_dependency_test(
            "second",
            writes=("shared/config.py",),
            dependencies=("first",),
        ),
    ]

    relaxed, dropped = hierarchy_module._relax_decorative_dependencies(streams)

    assert relaxed[1].dependencies == ("first",)
    assert dropped == []


def test_a_file_the_contract_creates_is_not_a_missing_input(tmp_path: Path):
    """Listing your own output as an input must not block you from writing it."""
    contract = WorkContract(
        id="self-output",
        input_artifacts=("core/config.py", "specs/absent.md"),
        expected_outputs=("core/config.py",),
        write_scopes=("core/config.py",),
        acceptance_criteria=("Config loads",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("core",),
    )

    # Only the genuinely external file is reported, never the one being created.
    assert _runtime_missing_contract_inputs(tmp_path, contract) == ("specs/absent.md",)


def test_declaring_an_input_also_grants_permission_to_read_it(tmp_path: Path):
    """Naming a file as an input is the declaration that you will read it.

    A work item listing config.example.json as an input but not as a read scope
    used to fail twice over: once against its own scopes, once against the
    workstream's. Reading is not the boundary that matters, writing is.
    """
    contract = hierarchy_module._planner_contract(
        {
            "contract_id": "reads-config",
            "input_artifacts": ["config.example.json"],
            "expected_outputs": ["app/settings.py"],
            "write_scopes": ["app/settings.py"],
            "acceptance_criteria": ["Settings load"],
            "consumers": ["app"],
        },
        fallback_id="reads-config",
        fallback_acceptance=("Settings load",),
        fallback_consumers=("app",),
    )
    assert "config.example.json" in contract.read_scopes

    item = WorkItem(
        id="app:settings",
        workstream_id="app",
        title="Settings",
        goal="Load settings",
        acceptance_criteria=contract.acceptance_criteria,
        write_scopes=contract.write_scopes,
        contract=contract,
        metadata={"file_path": "app/settings.py", "contract_mode": "typed"},
    )
    # The workstream never mentions config.example.json.
    stream_contract = WorkContract(
        id="app-contract",
        expected_outputs=("app/settings.py",),
        write_scopes=("app/settings.py",),
        acceptance_criteria=("Settings load",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("task",),
    )
    plan = TaskPlan(
        task_id="task-reads",
        session_id="session-reads",
        goal="Load settings",
        workstreams=(
            Workstream(
                id="app",
                title="App",
                goal="Load settings",
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

    assert "contract_read_scope_mismatch" not in {issue["code"] for issue in issues}


def test_prose_inputs_are_not_measured_as_file_paths():
    """Prose in a path field must be ignored, not checked against scopes.

    Both of these failed real work items because the slash inside the prose
    made the whole sentence look like a filename.
    """
    prose = (
        "USER GOAL section 5 (camera enumeration, resolution/fps/mirror config, "
        "DSHOW/MSMF backend selection, reconnect and release behavior)"
    )
    assert not hierarchy_module._looks_like_artifact_path(prose)
    assert not hierarchy_module._looks_like_artifact_path(
        "USER GOAL section 6 (countdown, multi-frame median/average capture)"
    )
    # Real paths are still recognised.
    assert hierarchy_module._looks_like_artifact_path("core/camera_manager.py")
    assert hierarchy_module._looks_like_artifact_path("config.json")
    assert hierarchy_module._looks_like_artifact_path("data/")


def test_prose_input_does_not_fail_preflight(tmp_path: Path):
    contract = hierarchy_module._planner_contract(
        {
            "contract_id": "prose-input",
            "input_artifacts": [
                "USER GOAL section 5 (resolution/fps/mirror config, DSHOW/MSMF backend)",
            ],
            "expected_outputs": ["core/camera.py"],
            "write_scopes": ["core/camera.py"],
            "acceptance_criteria": ["Camera opens"],
            "consumers": ["core"],
        },
        fallback_id="prose-input",
        fallback_acceptance=("Camera opens",),
        fallback_consumers=("core",),
    )
    item = WorkItem(
        id="core:camera",
        workstream_id="core",
        title="Camera",
        goal="Open the camera",
        acceptance_criteria=contract.acceptance_criteria,
        write_scopes=contract.write_scopes,
        contract=contract,
        metadata={"file_path": "core/camera.py", "contract_mode": "typed"},
    )
    stream_contract = WorkContract(
        id="core-contract",
        expected_outputs=("core/camera.py",),
        write_scopes=("core/camera.py",),
        acceptance_criteria=("Camera opens",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("task",),
    )
    plan = TaskPlan(
        task_id="task-prose",
        session_id="session-prose",
        goal="Open the camera",
        workstreams=(
            Workstream(
                id="core",
                title="Core",
                goal="Open the camera",
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

    codes = {issue["code"] for issue in issues}
    assert "contract_read_scope_mismatch" not in codes
    assert "contract_file_mismatch" not in codes


def test_annotated_contract_input_stays_inside_scope(tmp_path: Path):
    contract = hierarchy_module._planner_contract(
        {
            "contract_id": "annotated",
            "input_artifacts": ["data/config.json (schema only, file created at runtime)"],
            "expected_outputs": ["core/state.py"],
            "read_scopes": ["data/"],
            "write_scopes": ["core/state.py"],
            "acceptance_criteria": ["State loads config"],
            "consumers": ["core"],
        },
        fallback_id="annotated",
        fallback_acceptance=("State loads config",),
        fallback_consumers=("core",),
    )

    assert contract.input_artifacts == ("data/config.json",)

    item = WorkItem(
        id="core:state",
        workstream_id="core",
        title="State",
        goal="Load config",
        acceptance_criteria=contract.acceptance_criteria,
        write_scopes=contract.write_scopes,
        contract=contract,
        metadata={"file_path": "core/state.py", "contract_mode": "typed"},
    )
    stream_contract = WorkContract(
        id="core-contract",
        expected_outputs=("core/state.py",),
        read_scopes=("data/",),
        write_scopes=("core/state.py",),
        acceptance_criteria=("State loads config",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("task",),
    )
    plan = TaskPlan(
        task_id="task-annotated",
        session_id="session-annotated",
        goal="Load config",
        workstreams=(
            Workstream(
                id="core",
                title="Core",
                goal="Load config",
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

    assert "contract_read_scope_mismatch" not in {issue["code"] for issue in issues}


def test_preflight_accepts_item_inputs_from_parent_workstream_dependencies(
    tmp_path: Path,
):
    foundation_contract = WorkContract(
        id="foundation-contract",
        expected_outputs=("core/application_state.py",),
        write_scopes=("core/application_state.py",),
        acceptance_criteria=("state exists",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("camera",),
    )
    foundation_item = WorkItem(
        id="foundation:state",
        workstream_id="foundation",
        title="State",
        goal="Create state",
        acceptance_criteria=foundation_contract.acceptance_criteria,
        write_scopes=foundation_contract.write_scopes,
        contract=foundation_contract,
        metadata={
            "file_path": "core/application_state.py",
            "package_files": ["core/application_state.py"],
            "contract_mode": "typed",
        },
    )
    camera_contract = WorkContract(
        id="camera-contract",
        input_artifacts=("core/application_state.py",),
        expected_outputs=("core/camera_manager.py",),
        read_scopes=("core/application_state.py",),
        write_scopes=("core/camera_manager.py",),
        acceptance_criteria=("camera exists",),
        test_requirements=("syntax parses",),
        evidence_requirements=("patch hash",),
        consumers=("app",),
    )
    camera_item = WorkItem(
        id="camera:manager",
        workstream_id="camera",
        title="Camera",
        goal="Create camera",
        acceptance_criteria=camera_contract.acceptance_criteria,
        write_scopes=camera_contract.write_scopes,
        contract=camera_contract,
        metadata={
            "file_path": "core/camera_manager.py",
            "package_files": ["core/camera_manager.py"],
            "contract_mode": "typed",
        },
    )
    plan = TaskPlan(
        task_id="task-dependencies",
        session_id="session-dependencies",
        goal="Create dependent modules",
        requested_manager_count=2,
        workstreams=(
            Workstream(
                id="foundation",
                title="Foundation",
                goal="Create state",
                acceptance_criteria=foundation_contract.acceptance_criteria,
                write_scopes=foundation_contract.write_scopes,
                requested_worker_count=1,
                work_items=(foundation_item,),
                contract=foundation_contract,
                metadata={"contract_mode": "typed"},
            ),
            Workstream(
                id="camera",
                title="Camera",
                goal="Create camera",
                acceptance_criteria=camera_contract.acceptance_criteria,
                write_scopes=camera_contract.write_scopes,
                dependencies=("foundation",),
                requested_worker_count=1,
                work_items=(camera_item,),
                contract=camera_contract,
                metadata={"contract_mode": "typed"},
            ),
        ),
        status=PlanStatus.READY,
    )

    issues = _preflight_plan(
        root=tmp_path,
        plan=plan,
        limits=SchedulerLimits(
            max_managers=2,
            max_parallel_managers=2,
        ),
        allow_new_files=True,
    )

    assert "missing_contract_input" not in {issue["code"] for issue in issues}


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
                    "patch_content": (f"<<<< SEARCH\n{before}\n====\n{after}\n>>>> REPLACE"),
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
        epoch = repository.list_execution_epochs("task-hierarchy")[-1]
        durable_reports = repository.list_manager_terminal_reports(
            "task-hierarchy",
            epoch["execution_epoch_id"],
        )
        durable_review = repository.get_director_final_review(
            "task-hierarchy",
            epoch["execution_epoch_id"],
        )

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
    assert all(effect.before_sha256 != effect.after_sha256 for effect in patch_effects)
    assert (
        len(
            {
                event["agent_instance_id"]
                for event in events
                if event.get("role") == "worker" and event.get("type") == "agent_started"
            }
        )
        == 2
    )
    assert (
        len(
            {
                event["agent_instance_id"]
                for event in events
                if event.get("role") == "tester" and event.get("type") == "agent_started"
            }
        )
        == 1
    )
    fanout = next(event for event in events if event.get("type") == "hierarchy_fanout_planned")
    assert {
        "managers": fanout["manager_count"],
        "coders": fanout["coder_count"],
        "testers": fanout["tester_count"],
        "children": fanout["child_agent_count"],
    } == {"managers": 1, "coders": 2, "testers": 1, "children": 3}
    assert fanout["primary_agent_count"] == 5
    assert fanout["primary_child_request_count"] == 4
    selections = [event for event in events if event.get("type") == "fanout_selected"]
    assert [(event["level"], event["selected"]) for event in selections] == [
        ("manager", 1),
        ("worker", 2),
    ]
    reconciliation = next(
        event for event in events if event.get("type") == "completion_reconciliation"
    )
    assert reconciliation["balanced"] is True
    assert reconciliation["covered"] is True
    assert reconciliation["successful"] is True
    assert reconciliation["work_items"]["completed"] == 2
    reports = [
        event for event in events if event.get("type") == "manager_terminal_report"
    ]
    assert len(reports) == 1
    assert reports[0]["status"] == "completed"
    barrier = next(
        event for event in events if event.get("type") == "manager_report_barrier"
    )
    assert (barrier["reported_count"], barrier["expected_count"]) == (1, 1)
    assert barrier["satisfied"] is True
    assert sum(event.get("type") == "director_final_review" for event in events) == 1
    assert len(durable_reports) == 1
    assert durable_reports[0]["disposition"] == "completed"
    assert durable_review is not None
    assert durable_review["status"] == "completed"
    manager_plan = next(event for event in events if event.get("type") == "manager_plan_created")
    assert manager_plan["work_items"][0]["contract"]["version"] == 1
    assert all(
        event.get("contract_version") == 1
        for event in events
        if event.get("type") == "execution_result"
    )
    assert len(contracts) == 3
    assert handoffs
    assert {(handoff.contract_id, handoff.contract_version) for handoff in handoffs} <= {
        (contract.id, contract.version) for contract in contracts
    }
    assert {identity["logical_agent_id"] for identity in identities}.issuperset(
        {
            event["agent_instance_id"]
            for event in events
            if event.get("type") == "agent_started" and event.get("agent_instance_id")
        }
    )
    assert any(event.get("type") == "hierarchy_completed" for event in events)
    signal_types = {
        event.get("signal_type") for event in events if event.get("type") == "agent_message"
    }
    assert {
        "delegate_workstream",
        "delegate_work_item",
        "submit_patch",
        "review_result",
        "workstream_result",
    }.issubset(signal_types)


def test_four_by_four_fanout_creates_25_agents_and_calls_every_worker(tmp_path: Path):
    """The acceptance case: 1 Director + 4 Managers + 16 Workers + 4 Testers.

    Legacy concurrency caps are pinned to one to prove maximum-parallelism mode
    still starts all sixteen Worker inference calls in the same wave.
    """
    for stream_index in range(1, 5):
        for item_index in range(1, 5):
            (tmp_path / f"s{stream_index}_{item_index}.py").write_text("V = 1\n", encoding="utf-8")
    events: list[dict] = []
    worker_barrier = Barrier(16)
    called_worker_ids: set[str] = set()

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Bốn workstream độc lập.",
                    "requested_manager_count": 4,
                    "workstreams": [
                        {
                            "id": f"s{index}",
                            "title": f"Stream {index}",
                            "goal": f"Nâng nhóm {index}.",
                            "acceptance_criteria": [f"Nhóm {index} bằng 2"],
                            "dependencies": [],
                            "write_scopes": [f"s{index}_{item}.py" for item in range(1, 5)],
                        }
                        for index in range(1, 5)
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            stream_index = next(
                (index for index in range(1, 5) if f"Nâng nhóm {index}" in user_message),
                1,
            )
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Bốn work item.",
                    "requested_worker_count": 4,
                    "work_items": [
                        {
                            "id": f"i{item}",
                            "title": f"Update {stream_index}.{item}",
                            "goal": "V bằng 2",
                            "file_path": f"s{stream_index}_{item}.py",
                            "instructions": "Đổi V từ 1 thành 2.",
                            "acceptance_criteria": ["V = 2"],
                            "dependencies": [],
                            "write_scopes": [f"s{stream_index}_{item}.py"],
                            "test_focus": "Parse Python",
                        }
                        for item in range(1, 5)
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            worker_id = str(hierarchy_module.llm_client.thread_local.agent_instance_id)
            called_worker_ids.add(worker_id)
            worker_barrier.wait(timeout=10)
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Đã cập nhật.",
                    "patch_content": "<<<< SEARCH\nV = 1\n====\nV = 2\n>>>> REPLACE",
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {"verdict": "approved", "reviewer_feedback": "Đúng.", "next_instructions": ""},
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {"verdict": "approved", "summary": "Xong.", "next_instructions": ""},
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {"verdict": "approved", "summary": "Xong.", "remaining_risks": []},
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        run_hierarchy(
            root=tmp_path,
            task_description="Nâng toàn bộ hằng số.",
            task_id="task-25",
            limits=SchedulerLimits(
                max_managers=4,
                max_parallel_managers=1,
                max_workers_per_manager=5,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )

    fanout = next(event for event in events if event.get("type") == "hierarchy_fanout_planned")
    assert fanout["manager_count"] == 4
    assert fanout["coder_count"] == 16
    assert fanout["tester_count"] == 4
    assert len(set(fanout["primary_agent_ids"])) == 25

    # Every coder actually reached the model, not just the graph.
    worker_ids = set(fanout["worker_agent_ids"])
    running_workers = {
        event["agent_instance_id"]
        for event in events
        if event.get("type") == "agent_started"
        and event.get("role") == "worker"
        and event.get("status") == "running"
    }
    assert called_worker_ids == worker_ids
    assert running_workers == worker_ids
    for stream_index in range(1, 5):
        for item_index in range(1, 5):
            written = (tmp_path / f"s{stream_index}_{item_index}.py").read_text(encoding="utf-8")
            assert written == "V = 2\n"


def test_agents_are_numbered_by_parent_and_logged_to_the_terminal(tmp_path, caplog):
    """Operators read "Worker 2.1", not "worker_dae85785083184335dd85d35"."""
    (tmp_path / "a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B = 1\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Hai workstream.",
                    "requested_manager_count": 2,
                    "workstreams": [
                        {
                            "id": "alpha",
                            "title": "Alpha",
                            "goal": "Nâng A.",
                            "acceptance_criteria": ["A bằng 2"],
                            "dependencies": [],
                            "write_scopes": ["a.py"],
                        },
                        {
                            "id": "beta",
                            "title": "Beta",
                            "goal": "Nâng B.",
                            "acceptance_criteria": ["B bằng 2"],
                            "dependencies": [],
                            "write_scopes": ["b.py"],
                        },
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            target = "b.py" if ("Nâng B" in user_message or "beta" in user_message) else "a.py"
            letter = "B" if target == "b.py" else "A"
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Một work item.",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": letter.lower(),
                            "title": f"Update {letter}",
                            "goal": f"{letter} bằng 2",
                            "file_path": target,
                            "instructions": f"Đổi {letter} từ 1 thành 2.",
                            "acceptance_criteria": [f"{letter} = 2"],
                            "dependencies": [],
                            "write_scopes": [target],
                            "test_focus": "Parse Python",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            letter = "B" if "b.py" in user_message else "A"
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Đã cập nhật.",
                    "patch_content": (
                        f"<<<< SEARCH\n{letter} = 1\n====\n{letter} = 2\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {"verdict": "approved", "reviewer_feedback": "Đúng.", "next_instructions": ""},
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {"verdict": "approved", "summary": "Xong.", "next_instructions": ""},
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {"verdict": "approved", "summary": "Xong.", "remaining_risks": []},
                {},
            )
        raise AssertionError(names)

    with caplog.at_level("INFO", logger="orchestrator.hierarchy"):
        with StateRepository(":memory:") as repository:
            run_hierarchy(
                root=tmp_path,
                task_description="Đổi A và B thành 2.",
                task_id="task-labels",
                limits=SchedulerLimits(
                    max_parallel_managers=2,
                    max_workers_per_manager=2,
                    max_parallel_workers=2,
                ),
                llm_call=fake_call,
                on_event=events.append,
                repository=repository,
            )

    # Planning events carry display labels without pretending the model started.
    planned_labels = {
        (event.get("role"), event.get("agent_label"))
        for event in events
        if isinstance(event, dict) and event.get("type") == "agent_planned"
    }
    assert ("worker", "Worker 1.1") in planned_labels
    assert ("worker", "Worker 2.1") in planned_labels
    assert ("tester", "Tester 1") in planned_labels
    assert ("tester", "Tester 2") in planned_labels
    assert not [label for _, label in planned_labels if not label]

    labels = {
        event["agent_label"]
        for event in events
        if isinstance(event, dict) and event.get("agent_label")
    }
    assert "Director" in labels
    assert {"Manager 1", "Manager 2"} <= labels
    assert {"Worker 1.1", "Worker 2.1"} <= labels
    assert {"Tester 1", "Tester 2"} <= labels

    # A worker's number names its manager, so 2.1 belongs to Manager 2.
    worker_streams = {
        event["agent_label"]: event.get("workstream_id")
        for event in events
        if isinstance(event, dict) and str(event.get("agent_label") or "").startswith("Worker ")
    }
    assert worker_streams["Worker 1.1"] == "alpha"
    assert worker_streams["Worker 2.1"] == "beta"

    terminal = "\n".join(record.getMessage() for record in caplog.records)
    assert "[AGENT SPAWN] Manager 1" in terminal
    assert "[AGENT SPAWN] Worker 1.1" in terminal
    assert "[AGENT COMPLETE] Worker 1.1" in terminal


def test_preflight_failure_in_one_workstream_still_runs_the_healthy_one(tmp_path: Path):
    """One bad contract must not cancel workers in unrelated workstreams.

    A single annotated input path used to raise one issue, and every work item
    in the plan was then failed with plan_preflight_aborted -- in the observed
    run that killed 15 workers across 5 streams because of one string.
    """
    (tmp_path / "a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B = 1\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Hai workstream độc lập.",
                    "requested_manager_count": 2,
                    "workstreams": [
                        {
                            "id": "good",
                            "title": "Good",
                            "goal": "Nâng A.",
                            "acceptance_criteria": ["A bằng 2"],
                            "dependencies": [],
                            "write_scopes": ["a.py"],
                        },
                        {
                            "id": "bad",
                            "title": "Bad",
                            "goal": "Nâng B.",
                            "acceptance_criteria": ["B bằng 2"],
                            "dependencies": [],
                            "write_scopes": ["b.py"],
                        },
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            if "Nâng B" in user_message or "bad" in user_message:
                return ToolCallResult(
                    "submit_work_item_plan",
                    {
                        "summary": "Một work item với input ngoài scope.",
                        "requested_worker_count": 1,
                        "work_items": [
                            {
                                "id": "b",
                                "title": "Update B",
                                "goal": "B bằng 2",
                                # This run is edit-only and the file does not
                                # exist, so the item cannot execute at all.
                                "file_path": "never_created.py",
                                "instructions": "Đổi B từ 1 thành 2.",
                                "acceptance_criteria": ["B = 2"],
                                "dependencies": [],
                                "write_scopes": ["never_created.py"],
                                "test_focus": "Parse Python",
                            }
                        ],
                    },
                    {},
                )
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Một work item hợp lệ.",
                    "requested_worker_count": 1,
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
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            assert "never_created.py" not in user_message, (
                "Worker của workstream hỏng không được gọi"
            )
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Đã cập nhật hằng số.",
                    "patch_content": "<<<< SEARCH\nA = 1\n====\nA = 2\n>>>> REPLACE",
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {"verdict": "approved", "reviewer_feedback": "Patch đúng.", "next_instructions": ""},
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {"verdict": "approved", "summary": "Xong.", "next_instructions": ""},
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {"verdict": "approved", "summary": "Tích hợp xong.", "remaining_risks": []},
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        run_hierarchy(
            root=tmp_path,
            task_description="Đổi A và B thành 2.",
            task_id="task-partial-preflight",
            limits=SchedulerLimits(
                max_parallel_managers=2,
                max_workers_per_manager=3,
                max_parallel_workers=2,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        final_plan = repository.get_plan("task-partial-preflight")

    # The healthy workstream ran all the way through to a written file.
    assert (tmp_path / "a.py").read_text(encoding="utf-8") == "A = 2\n"
    assert (tmp_path / "b.py").read_text(encoding="utf-8") == "B = 1\n"

    assert final_plan is not None
    statuses = {stream.id: stream.status for stream in final_plan.workstreams}
    assert statuses["good"] == WorkStatus.APPROVED
    assert statuses["bad"] == WorkStatus.ABANDONED
    assert final_plan.status == PlanStatus.PARTIAL
    reports = [
        event for event in events if event.get("type") == "manager_terminal_report"
    ]
    assert len(reports) == 2
    assert {report["workstream_id"] for report in reports} == {"good", "bad"}
    assert {report["status"] for report in reports} == {"completed", "abandoned"}
    barrier = next(
        event for event in events if event.get("type") == "manager_report_barrier"
    )
    assert (barrier["reported_count"], barrier["expected_count"]) == (2, 2)
    assert sum(event.get("type") == "director_final_review" for event in events) == 1
    assert sum(event.get("type") == "hierarchy_partial" for event in events) == 1

    # The healthy stream must never be told the whole plan was aborted.
    aborted = [
        event
        for event in events
        if event.get("type") == "preflight_failed"
        and event.get("workstream_id") == "good"
    ]
    assert aborted == []
    assert any(event.get("type") == "plan_admission_partial" for event in events)
    assert not any(
        event.get("type") in {"agent_planned", "agent_started"}
        and event.get("workstream_id") == "bad"
        and event.get("role") in {"worker", "tester"}
        for event in events
    )


def test_contract_id_collision_still_calls_worker_tester_and_creates_file(
    tmp_path: Path,
):
    events: list[dict] = []
    calls = {"manager": 0, "worker": 0, "tester": 0}

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "One implementation stream",
                    "requested_manager_count": 1,
                    "workstreams": [
                        {
                            "id": "implementation",
                            "title": "Implementation",
                            "goal": "Create new.py",
                            "acceptance_criteria": ["new.py exists"],
                            "dependencies": [],
                            "contract_id": "shared-contract",
                            "contract_version": 1,
                            "input_artifacts": [],
                            "expected_outputs": ["new.py"],
                            "read_scopes": [],
                            "write_scopes": ["new.py"],
                            "test_requirements": ["syntax parses"],
                            "evidence_requirements": ["patch hash"],
                            "consumers": ["task"],
                        }
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            calls["manager"] += 1
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "One file package",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "create-file",
                            "title": "Create file",
                            "goal": "Create new.py",
                            "file_path": "new.py",
                            "instructions": "Create a valid Python module.",
                            "acceptance_criteria": ["new.py exists"],
                            "dependencies": [],
                            # Deliberately reuse the parent identity. The
                            # backend must namespace this child contract.
                            "contract_id": "shared-contract",
                            "contract_version": 1,
                            "input_artifacts": [],
                            "expected_outputs": ["new.py"],
                            "read_scopes": [],
                            "write_scopes": ["new.py"],
                            "test_requirements": ["syntax parses"],
                            "evidence_requirements": ["patch hash"],
                            "consumers": ["implementation"],
                            "test_focus": "syntax",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            calls["worker"] += 1
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Created new.py",
                    "patch_content": ("<<<< SEARCH\n====\nVALUE = 1\n>>>> REPLACE"),
                },
                {},
            )
        if names == ["review_patch"]:
            calls["tester"] += 1
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Valid new module.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved",
                    "summary": "Implementation completed.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "Plan completed.",
                    "remaining_risks": [],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Create a new project file.",
            task_id="task-contract-collision",
            allow_new_files=True,
            limits=SchedulerLimits(
                max_managers=1,
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        contracts = repository.list_contract_versions("task-contract-collision")

    assert result.stopped_reason == "task_completed"
    assert calls == {"manager": 1, "worker": 1, "tester": 1}
    assert (tmp_path / "new.py").read_text(encoding="utf-8") == "VALUE = 1"
    assert len({contract.id for contract in contracts}) == 2
    assert any(
        event.get("type") == "agent_started"
        and event.get("role") == "worker"
        and event.get("status") == "running"
        for event in events
    )
    assert any(
        event.get("type") == "review_result"
        and event.get("role") == "tester"
        and event.get("accepted") is True
        for event in events
    )


def test_hierarchy_plans_all_managers_eagerly_before_execution(tmp_path: Path):
    (tmp_path / "a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B = 1\n", encoding="utf-8")
    events = []
    plan_order: list[str] = []
    worker_during_planning = {"seen": False}
    manager_plan_barrier = Barrier(2)

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
            manager_plan_barrier.wait(timeout=2)
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
                    "patch_content": (f"<<<< SEARCH\n{before}\n====\n{after}\n>>>> REPLACE"),
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


def test_failed_upstream_plan_does_not_block_a_planned_worker(
    tmp_path: Path,
):
    (tmp_path / "core").mkdir()
    (tmp_path / "core" / "camera_manager.py").write_text(
        "CAMERA = 0\n", encoding="utf-8"
    )
    events: list[dict] = []
    worker_calls = {"count": 0}

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Foundation then camera",
                    "requested_manager_count": 2,
                    "workstreams": [
                        {
                            "id": "foundation",
                            "title": "Foundation",
                            "goal": "Create shared state",
                            "acceptance_criteria": ["state exists"],
                            "dependencies": [],
                            "expected_outputs": ["core/application_state.py"],
                            "write_scopes": ["core/application_state.py"],
                        },
                        {
                            "id": "camera",
                            "title": "Camera",
                            "goal": "Create camera",
                            "acceptance_criteria": ["camera exists"],
                            "dependencies": ["foundation"],
                            "input_artifacts": ["core/application_state.py"],
                            "expected_outputs": ["core/camera_manager.py"],
                            "read_scopes": ["core/application_state.py"],
                            "write_scopes": ["core/camera_manager.py"],
                        },
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            if "WORKSTREAM ID\nfoundation" in user_message:
                raise RuntimeError("foundation manager failed")
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Camera package",
                    "requested_worker_count": 1,
                    "work_items": [
                        {
                            "id": "camera-manager",
                            "title": "Camera manager",
                            "goal": "Create camera manager",
                            "file_path": "core/camera_manager.py",
                            "instructions": "Implement camera manager",
                            "acceptance_criteria": ["camera exists"],
                            "dependencies": [],
                            "input_artifacts": ["core/application_state.py"],
                            "expected_outputs": ["core/camera_manager.py"],
                            "read_scopes": ["core/application_state.py"],
                            "write_scopes": ["core/camera_manager.py"],
                            "test_focus": "syntax",
                        }
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            worker_calls["count"] += 1
            assert "INPUTS NOT PRESENT YET" in user_message
            assert "core/application_state.py" in user_message
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Implemented without the missing upstream file.",
                    "patch_content": (
                        "<<<< SEARCH\nCAMERA = 0\n====\nCAMERA = 1\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if names == ["review_patch"]:
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Camera patch is valid.",
                    "next_instructions": "",
                },
                {},
            )
        if names == ["complete_workstream"]:
            return ToolCallResult(
                "complete_workstream",
                {"verdict": "approved", "summary": "Camera done.", "next_instructions": ""},
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "revise",
                    "summary": "Foundation planning failed.",
                    "remaining_risks": ["foundation missing"],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Create a camera project",
            task_id="task-upstream-block",
            allow_new_files=True,
            limits=SchedulerLimits(
                max_managers=2,
                max_parallel_managers=2,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        final_plan = repository.get_plan("task-upstream-block")
        attempts = repository.list_attempts("task-upstream-block")

    assert result.stopped_reason == "task_partial"
    assert worker_calls["count"] == 1
    assert any(attempt.workstream_id == "camera" for attempt in attempts)
    assert (tmp_path / "core" / "camera_manager.py").read_text(encoding="utf-8") == (
        "CAMERA = 1\n"
    )
    assert final_plan is not None
    assert {stream.id: stream.status for stream in final_plan.workstreams} == {
        "foundation": WorkStatus.ABANDONED,
        "camera": WorkStatus.APPROVED,
    }
    assert final_plan.status == PlanStatus.PARTIAL
    assert not any(
        event.get("type") == "agent_blocked"
        and event.get("role") == "worker"
        and event.get("workstream_id") == "camera"
        for event in events
    )


def test_missing_edit_file_rejects_plan_before_worker_is_announced(
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
    assert result.stopped_reason == "task_partial"
    assert attempts == []
    assert not any(
        event.get("type") == "agent_failed" and event.get("role") == "worker"
        for event in events
    )
    planned_children = {
        event.get("role")
        for event in events
        if event.get("type") == "agent_planned"
        and event.get("role") in {"worker", "tester"}
        and event.get("status") == "planned"
    }
    assert planned_children == set()
    assert not any(event.get("type") == "model_request_started" for event in events)
    assert not any(
        event.get("type") == "agent_blocked" and event.get("role") == "tester"
        for event in events
    )
    admission = next(
        event for event in events if event.get("type") == "plan_admission_rejected"
    )
    assert admission["issues"][0]["code"] == "new_file_forbidden"
    assert final_plan is not None
    assert final_plan.status == PlanStatus.PARTIAL
    assert final_plan.workstreams[0].work_items[0].status == WorkStatus.SKIPPED


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
                    "patch_content": ("<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"),
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
    assert sum(
        event.get("type") == "agent_started" and event.get("role") == "worker"
        for event in events
    ) == 1
    assert sum(
        event.get("type") == "agent_planned" and event.get("role") == "worker"
        for event in events
    ) == 2
    assert any(
        event.get("type") == "agent_skipped"
        and event.get("role") == "worker"
        and event.get("reason") == "approved_resume_evidence"
        for event in events
    )
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


def test_repeated_crisis_exhausts_strategy_without_global_attempt_budget(tmp_path: Path):
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
            replacement = "VALUE = ("
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Attempted update",
                    "patch_content": (f"<<<< SEARCH\nVALUE = 1\n====\n{replacement}\n>>>> REPLACE"),
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
        attempts = repository.list_attempts("task-manager-recovery")
        remediations = repository.list_remediation_attempts(
            "task-manager-recovery"
        )

    assert result.stopped_reason == "task_partial"
    assert worker_calls["count"] == 3
    assert manager_reviews["count"] == 3
    assert [attempt.number for attempt in attempts] == [1, 2, 3]
    assert len(remediations) == 2
    assert {item["status"] for item in remediations} == {"applied"}
    assert target.read_text(encoding="utf-8") == "VALUE = 1\n"
    assert any(event.get("type") == "manager_replan_created" for event in events)
    event_types = [event.get("type") for event in events]
    assert event_types.index("remediation_exhausted") < event_types.index(
        "manager_terminal_report"
    )
    assert event_types.index("manager_terminal_report") < event_types.index(
        "manager_report_barrier"
    )
    assert event_types.index("manager_report_barrier") < event_types.index(
        "director_final_review"
    )


def test_partial_workstream_still_schedules_dependent_workstream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(hierarchy_module, "max_parallelism_enabled", lambda: False)
    monkeypatch.setattr(
        "orchestrator.scheduler.max_parallelism_enabled", lambda: False
    )
    (tmp_path / "keep.py").write_text("KEEP = 1\n", encoding="utf-8")
    (tmp_path / "broken.py").write_text("BROKEN = 1\n", encoding="utf-8")
    (tmp_path / "follow.py").write_text("FOLLOW = 1\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        names = [tool["name"] for tool in tools]
        if names == ["submit_workstream_plan"]:
            return ToolCallResult(
                "submit_workstream_plan",
                {
                    "summary": "Foundation then follow-on.",
                    "requested_manager_count": 2,
                    "workstreams": [
                        {
                            "id": "foundation",
                            "title": "Foundation",
                            "goal": "Keep one file and fail the other",
                            "acceptance_criteria": ["KEEP = 2"],
                            "dependencies": [],
                            "write_scopes": ["keep.py", "broken.py"],
                        },
                        {
                            "id": "follow",
                            "title": "Follow",
                            "goal": "Update follow.py",
                            "acceptance_criteria": ["FOLLOW = 2"],
                            "dependencies": ["foundation"],
                            "write_scopes": ["follow.py"],
                        },
                    ],
                },
                {},
            )
        if names == ["submit_work_item_plan"]:
            if "WORKSTREAM ID\nfollow" in user_message:
                return ToolCallResult(
                    "submit_work_item_plan",
                    {
                        "summary": "Follow item",
                        "requested_worker_count": 1,
                        "work_items": [
                            {
                                "id": "follow",
                                "title": "Follow",
                                "goal": "FOLLOW = 2",
                                "file_path": "follow.py",
                                "instructions": "Set FOLLOW to 2",
                                "acceptance_criteria": ["FOLLOW = 2"],
                                "dependencies": [],
                                "write_scopes": ["follow.py"],
                                "test_focus": "syntax",
                            }
                        ],
                    },
                    {},
                )
            return ToolCallResult(
                "submit_work_item_plan",
                {
                    "summary": "Keep and broken",
                    "requested_worker_count": 2,
                    "work_items": [
                        {
                            "id": "keep",
                            "title": "Keep",
                            "goal": "KEEP = 2",
                            "file_path": "keep.py",
                            "instructions": "Set KEEP to 2",
                            "acceptance_criteria": ["KEEP = 2"],
                            "dependencies": [],
                            "write_scopes": ["keep.py"],
                            "test_focus": "syntax",
                        },
                        {
                            "id": "broken",
                            "title": "Broken",
                            "goal": "BROKEN = 2",
                            "file_path": "broken.py",
                            "instructions": "Set BROKEN to 2",
                            "acceptance_criteria": ["BROKEN = 2"],
                            "dependencies": [],
                            "write_scopes": ["broken.py"],
                            "test_focus": "syntax",
                        },
                    ],
                },
                {},
            )
        if names == ["submit_patch"]:
            if "## TARGET FILE: broken.py" in user_message:
                before, after = "BROKEN = 1", "BROKEN = ("
            elif "## TARGET FILE: follow.py" in user_message:
                before, after = "FOLLOW = 1", "FOLLOW = 2"
            else:
                before, after = "KEEP = 1", "KEEP = 2"
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Attempted update",
                    "patch_content": (f"<<<< SEARCH\n{before}\n====\n{after}\n>>>> REPLACE"),
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
            approved = "FOLLOW = 2" in user_message or "KEEP = 2" in user_message
            return ToolCallResult(
                "complete_workstream",
                {
                    "verdict": "approved" if approved else "revise",
                    "summary": "Complete" if approved else "broken.py is invalid",
                    "next_instructions": "" if approved else "Fix broken.py",
                },
                {},
            )
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "approved",
                    "summary": "Partial foundation still unblocked follow-on work.",
                    "remaining_risks": ["broken.py"],
                },
                {},
            )
        raise AssertionError(names)

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Keep one file working even if another fails.",
            task_id="task-partial-dag",
            limits=SchedulerLimits(
                max_parallel_managers=2,
                max_workers_per_manager=3,
                max_parallel_workers=2,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        final_plan = repository.get_plan("task-partial-dag")

    assert result.stopped_reason == "task_partial"
    assert (tmp_path / "keep.py").read_text(encoding="utf-8") == "KEEP = 2\n"
    assert (tmp_path / "follow.py").read_text(encoding="utf-8") == "FOLLOW = 2\n"
    assert final_plan is not None
    item_status = {
        item.id: item.status
        for stream in final_plan.workstreams
        for item in stream.work_items
    }
    assert item_status["foundation:keep"] == WorkStatus.APPROVED
    assert item_status["follow:follow"] == WorkStatus.APPROVED
    assert item_status["foundation:broken"] == WorkStatus.ABANDONED
    assert any(
        event.get("type") == "workstream_completed"
        and event.get("workstream_id") == "foundation"
        and event.get("status") == "partial"
        for event in events
    )
    assert not any(
        event.get("type") == "workstream_skipped"
        and event.get("workstream_id") == "follow"
        for event in events
    )


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
                    "patch_content": (f"<<<< SEARCH\n{before}\n====\n{after}\n>>>> REPLACE"),
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
        if names == ["complete_plan"]:
            return ToolCallResult(
                "complete_plan",
                {
                    "verdict": "revise",
                    "summary": "Plan admission rejected the missing edit target.",
                    "remaining_risks": ["new.py does not exist"],
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

    assert result.stopped_reason == "task_partial"
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


def test_account_pool_exhaustion_escapes_worker_catches_and_fails_whole_task(
    tmp_path: Path,
):
    (tmp_path / "value.py").write_text("VALUE = 1\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        name = tools[0]["name"]
        if name == "submit_workstream_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One stream",
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
        if name == "submit_work_item_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One item",
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
                            "test_focus": "syntax",
                        }
                    ],
                },
                {},
            )
        if name == "submit_patch":
            raise AccountPoolExhaustedError("all accounts exhausted")
        raise AssertionError(f"Fatal account exhaustion must prevent {name}")

    with StateRepository(":memory:") as repository:
        with pytest.raises(AccountPoolExhaustedError, match="all accounts exhausted"):
            run_hierarchy(
                root=tmp_path,
                task_description="Update value",
                task_id="task-account-fatal",
                limits=SchedulerLimits(
                    max_parallel_managers=1,
                    max_workers_per_manager=2,
                    max_parallel_workers=1,
                ),
                llm_call=fake_call,
                on_event=events.append,
                repository=repository,
            )
        plan = repository.get_plan("task-account-fatal")
        attempts = repository.list_attempts("task-account-fatal")
        epoch = repository.list_execution_epochs("task-account-fatal")[-1]
        reports = repository.list_manager_terminal_reports(
            "task-account-fatal",
            epoch["execution_epoch_id"],
        )

    assert plan is not None and plan.status == PlanStatus.FAILED
    assert plan.metadata["failure_kind"] == "account_pool_exhausted"
    assert [attempt.status for attempt in attempts] == [AttemptStatus.FAILED]
    assert plan.workstreams[0].work_items[0].status == WorkStatus.ABANDONED
    assert len(reports) == 1
    assert reports[0]["disposition"] == "abandoned"
    assert sum(event.get("type") == "manager_terminal_report" for event in events) == 1
    assert sum(event.get("type") == "manager_report_barrier" for event in events) == 1
    assert not any(event.get("type") == "director_final_review" for event in events)
    assert any(
        event.get("type") == "hierarchy_failed"
        and event.get("failure_kind") == "account_pool_exhausted"
        for event in events
    )


def test_cancellation_between_report_barrier_and_director_review_wins(
    tmp_path: Path,
):
    (tmp_path / "value.py").write_text("VALUE = 1\n", encoding="utf-8")
    events: list[dict] = []
    cancellation = {"set": False}
    complete_plan_calls = {"count": 0}

    def fake_call(system_prompt, user_message, tools):
        name = tools[0]["name"]
        if name == "submit_workstream_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One stream",
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
        if name == "submit_work_item_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One item",
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
                            "test_focus": "syntax",
                        }
                    ],
                },
                {},
            )
        if name == "submit_patch":
            return ToolCallResult(
                name,
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": (
                        "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
                    ),
                },
                {},
            )
        if name == "review_patch":
            return ToolCallResult(
                name,
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Approved",
                    "next_instructions": "",
                },
                {},
            )
        if name == "complete_workstream":
            return ToolCallResult(
                name,
                {
                    "verdict": "approved",
                    "summary": "Complete",
                    "next_instructions": "",
                },
                {},
            )
        if name == "complete_plan":
            complete_plan_calls["count"] += 1
            raise AssertionError("Cancellation must win before Director review")
        raise AssertionError(name)

    def on_event(event):
        events.append(event)
        if event.get("type") == "manager_report_barrier":
            cancellation["set"] = True

    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Update value",
            task_id="task-cancellation-race",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=on_event,
            repository=repository,
            cancelled=lambda: cancellation["set"],
        )
        plan = repository.get_plan("task-cancellation-race")

    assert result.stopped_reason == "cancelled"
    assert plan is not None and plan.status == PlanStatus.CANCELLED
    assert complete_plan_calls["count"] == 0
    assert sum(event.get("type") == "manager_terminal_report" for event in events) == 1
    assert sum(event.get("type") == "manager_report_barrier" for event in events) == 1
    assert not any(event.get("type") == "director_final_review" for event in events)


def test_cancelled_worker_attempt_is_persisted_without_failure_terminal(
    tmp_path: Path,
    monkeypatch,
):
    (tmp_path / "value.py").write_text("VALUE = 1\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        name = tools[0]["name"]
        if name == "submit_workstream_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One stream",
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
        if name == "submit_work_item_plan":
            return ToolCallResult(
                name,
                {
                    "summary": "One item",
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
        raise AssertionError(f"Cancellation must prevent {name}")

    def cancelled_execution(**kwargs):
        return TicketExecutionResult(
            accepted=False,
            file_path="value.py",
            worker_feedback="",
            execution_result="Cancelled",
            reviewer_feedback="",
            reviewer_verdict="not_run",
            next_instructions="",
            patch_sha256=None,
            additions=0,
            deletions=0,
            syntax_status="not_run",
            test_status="not_run",
            test_output="",
            error="cancelled by operator",
            failure_kind="cancelled",
            retryable=False,
        )

    monkeypatch.setattr(hierarchy_module, "execute_work_item", cancelled_execution)
    with StateRepository(":memory:") as repository:
        result = run_hierarchy(
            root=tmp_path,
            task_description="Update value",
            task_id="task-cancelled-attempt",
            limits=SchedulerLimits(
                max_parallel_managers=1,
                max_workers_per_manager=2,
                max_parallel_workers=1,
            ),
            llm_call=fake_call,
            on_event=events.append,
            repository=repository,
        )
        attempts = repository.list_attempts("task-cancelled-attempt")
        plan = repository.get_plan("task-cancelled-attempt")

    assert result.stopped_reason == "cancelled"
    assert [attempt.status for attempt in attempts] == [AttemptStatus.CANCELLED]
    assert plan is not None and plan.status == PlanStatus.CANCELLED
    assert any(event.get("type") == "hierarchy_cancelled" for event in events)
    assert not any(event.get("type") in {"agent_failed", "hierarchy_failed"} for event in events)
