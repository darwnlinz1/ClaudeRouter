import hashlib
import sys
import threading
from pathlib import Path

import pytest

from orchestrator import llm_client, project_workspace, safety
from orchestrator.effects import EffectState
from orchestrator.llm_client import ToolCallResult
from orchestrator.state_repository import StateRepository
from orchestrator.ticket_executor import (
    build_failure_prompt_inputs,
    execute_work_item,
)
from orchestrator.worker_targets import UnsupportedWorkerTarget


class TrackingLock:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.depth = 0

    @property
    def held(self) -> bool:
        return self.depth > 0

    def acquire(self, *args, **kwargs):
        acquired = self._lock.acquire(*args, **kwargs)
        if acquired:
            self.depth += 1
        return acquired

    def release(self) -> None:
        self.depth -= 1
        self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.release()
        return False


def test_worker_and_tester_inference_do_not_hold_project_mutation_lock(
    tmp_path: Path,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    lock = TrackingLock()
    inference_lock_states: list[tuple[str, bool]] = []

    def fake_call(system_prompt, user_message, tools):
        name = tools[0]["name"]
        inference_lock_states.append((name, lock.held))
        if name == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": ("<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"),
                },
                {},
            )
        if name == "review_patch":
            return ToolCallResult(
                "review_patch",
                {
                    "verdict": "approved",
                    "reviewer_feedback": "Approved",
                    "next_instructions": "",
                },
                {},
            )
        raise AssertionError(name)

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update core value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        llm_call=fake_call,
        execution_lock=lock,
    )

    assert result.accepted is True
    assert inference_lock_states == [
        ("submit_patch", False),
        ("review_patch", False),
    ]
    assert lock.held is False
    assert target.read_text(encoding="utf-8") == "VALUE = 2\n"


def test_tester_transport_failure_is_attributed_to_tester(tmp_path: Path):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        if tools[0]["name"] == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": ("<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"),
                },
                {},
            )
        raise ConnectionError("tester transport unavailable")

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update core value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        llm_call=fake_call,
        on_event=events.append,
        task_id="task-transport",
        session_id="session-transport",
        workstream_id="stream-transport",
        attempt_id="attempt-transport",
        manager_agent_id="manager-stable",
        worker_agent_id="worker-stable",
        tester_agent_id="tester-stable",
    )

    assert result.accepted is False
    assert result.failure_kind == "transport"
    assert result.retryable is True
    assert result.failure_category == "transport_lease"
    assert result.failure_signature
    assert result.failure_signature_components["target_hash"]
    assert result.failure_signature_components["request_hash"]
    assert result.failure_actor == "tester"
    assert result.diagnostic_log_refs["task_id"] == "task-transport"
    assert result.diagnostic_log_refs["session_id"] == "session-transport"
    assert result.diagnostic_log_refs["workstream_id"] == "stream-transport"
    assert result.diagnostic_log_refs["work_item_id"] == "core:value"
    assert result.diagnostic_log_refs["execution_attempt_id"] == "attempt-transport"
    assert result.diagnostic_log_refs["actor_agent_id"] == "tester-stable"
    assert any(
        event.get("type") == "agent_failed"
        and event.get("role") == "tester"
        and event.get("agent_instance_id") == "tester-stable"
        and event.get("telemetry_scope") == "attempt"
        and event.get("attempt_terminal") is True
        for event in events
    )
    assert not any(
        event.get("type") == "agent_failed" and event.get("agent_instance_id") == "worker-stable"
        for event in events
    )
    assert target.read_text(encoding="utf-8") == "VALUE = 1\n"


def test_worker_abort_is_reported_as_cancellation(tmp_path: Path):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        raise llm_client.ModelRequestAborted("cancelled by operator")

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update core value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        llm_call=fake_call,
        on_event=events.append,
        manager_agent_id="manager-stable",
        worker_agent_id="worker-stable",
        tester_agent_id="tester-stable",
    )

    assert result.accepted is False
    assert result.failure_kind == "cancelled"
    assert result.retryable is False
    assert any(
        event.get("type") == "agent_cancelled"
        and event.get("role") == "worker"
        and event.get("agent_instance_id") == "worker-stable"
        for event in events
    )


def test_worker_lease_loss_is_retryable_not_operator_cancellation(tmp_path: Path):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")

    def fake_call(system_prompt, user_message, tools):
        raise llm_client.AccountLeaseLostError("account lease lost")

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update core value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        llm_call=fake_call,
    )

    assert result.failure_kind == "lease_loss"
    assert result.retryable is True


def test_reviewer_rollback_conflicts_instead_of_overwriting_later_edit(
    tmp_path: Path,
    monkeypatch,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")

    def fake_call(system_prompt, user_message, tools):
        if tools[0]["name"] == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": ("<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"),
                },
                {},
            )
        target.write_text("VALUE = 3\n", encoding="utf-8")
        return ToolCallResult(
            "review_patch",
            {
                "verdict": "revise",
                "reviewer_feedback": "Needs revision",
                "next_instructions": "Try again",
            },
            {},
        )

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update core value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        llm_call=fake_call,
    )

    assert result.accepted is False
    assert result.failure_kind == "rollback_conflict"
    assert result.retryable is False
    assert "rollback conflict" in result.error
    assert target.read_text(encoding="utf-8") == "VALUE = 3\n"


def test_high_risk_patch_waits_for_human_approval_before_mutation(tmp_path: Path):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    candidates = []

    def fake_call(system_prompt, user_message, tools):
        return ToolCallResult(
            "submit_patch",
            {
                "task_status": "completed",
                "worker_feedback": "Updated",
                "patch_content": ("<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"),
            },
            {},
        )

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update core value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        llm_call=fake_call,
        task_id="task-a",
        workstream_id="stream-a",
        attempt_id="attempt-a",
        approval_policy="risk_based",
        risk_level="high",
        approval_callback=lambda candidate: candidates.append(candidate) or False,
    )

    assert result.accepted is False
    assert result.failure_kind == "approval_rejected"
    assert candidates[0]["patch_sha256"] == result.patch_sha256
    assert target.read_text(encoding="utf-8") == "VALUE = 1\n"


def test_configured_tests_block_non_retryably_without_strong_sandbox(
    tmp_path: Path,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    events = []

    def fake_call(system_prompt, user_message, tools):
        assert tools[0]["name"] == "submit_patch"
        return ToolCallResult(
            "submit_patch",
            {
                "task_status": "completed",
                "worker_feedback": "Updated",
                "patch_content": ("<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"),
            },
            {},
        )

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update core value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        test_cmd=[sys.executable, "-c", "raise SystemExit(0)"],
        llm_call=fake_call,
        on_event=events.append,
    )

    assert result.accepted is False
    assert result.failure_kind == "sandbox_unavailable"
    assert result.retryable is False
    assert result.test_status == "failed"
    assert target.read_text(encoding="utf-8") == "VALUE = 1\n"
    test_event = next(event for event in events if event["type"] == "test_result")
    assert test_event["status"] == "blocked"
    assert test_event["sandbox_outcome"] == "unavailable"


def test_ticket_prepares_before_durable_begin_then_commits(
    tmp_path: Path,
    monkeypatch,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")
    database = tmp_path / "state.sqlite3"
    patch = "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"

    def fake_call(system_prompt, user_message, tools):
        if tools[0]["name"] == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": patch,
                },
                {},
            )
        return ToolCallResult(
            "review_patch",
            {
                "verdict": "approved",
                "reviewer_feedback": "Approved",
                "next_instructions": "",
            },
            {},
        )

    with StateRepository(database) as repository:
        real_commit = project_workspace.commit_prepared_file
        observed_states = []

        def inspect_commit(prepared, **kwargs):
            if prepared.target == target:
                receipts = repository.list_effects("task-a")
                observed_states.append(receipts[0].state)
                assert target.read_text(encoding="utf-8") == "VALUE = 1\n"
                assert receipts[0].expected_after_sha256 == prepared.after_sha256
            return real_commit(prepared, **kwargs)

        monkeypatch.setattr(
            project_workspace,
            "commit_prepared_file",
            inspect_commit,
        )
        result = execute_work_item(
            root=tmp_path,
            task_goal="Update value",
            workstream_goal="Update core value",
            work_item_id="core:value",
            file_path="value.py",
            instructions="Set VALUE to 2",
            acceptance_criteria=("VALUE = 2",),
            llm_call=fake_call,
            effect_repository=repository,
            task_id="task-a",
            workstream_id="stream-a",
        )

        receipt = repository.get_effect(result.effect_id)
        assert result.accepted is True
        assert observed_states == [EffectState.PENDING]
        assert receipt is not None
        assert receipt.state is EffectState.APPLIED
        assert receipt.after_sha256 == project_workspace.sha256_file(target)


def test_reviewer_rollback_is_linked_as_compensation(
    tmp_path: Path,
    monkeypatch,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")

    def fake_call(system_prompt, user_message, tools):
        if tools[0]["name"] == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": ("<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"),
                },
                {},
            )
        return ToolCallResult(
            "review_patch",
            {
                "verdict": "revise",
                "reviewer_feedback": "Revert this patch",
                "next_instructions": "Try again",
            },
            {},
        )

    with StateRepository(tmp_path / "state.sqlite3") as repository:
        result = execute_work_item(
            root=tmp_path,
            task_goal="Update value",
            workstream_goal="Update core value",
            work_item_id="core:value",
            file_path="value.py",
            instructions="Set VALUE to 2",
            acceptance_criteria=("VALUE = 2",),
            llm_call=fake_call,
            effect_repository=repository,
            task_id="task-a",
            workstream_id="stream-a",
        )

        effects = repository.list_effects("task-a")
        original = next(effect for effect in effects if effect.kind == "file_patch")
        rollback = next(effect for effect in effects if effect.kind == "file_rollback")
        assert result.accepted is False
        assert target.read_text(encoding="utf-8") == "VALUE = 1\n"
        assert rollback.compensates_effect_id == original.effect_id
        assert original.compensated_by_effect_id == rollback.effect_id


def test_ticket_recovers_pending_commit_by_target_hash(
    tmp_path: Path,
    monkeypatch,
):
    target = tmp_path / "value.py"
    target.write_bytes(b"VALUE = 2\n")
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")
    patch = "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
    patch_hash = hashlib.sha256(patch.encode("utf-8")).hexdigest()

    def fake_call(system_prompt, user_message, tools):
        if tools[0]["name"] == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": patch,
                },
                {},
            )
        return ToolCallResult(
            "review_patch",
            {
                "verdict": "approved",
                "reviewer_feedback": "Approved",
                "next_instructions": "",
            },
            {},
        )

    with StateRepository(tmp_path / "state.sqlite3") as repository:
        pending = repository.begin_effect(
            "task-a",
            f"patch:core:value:value.py:{patch_hash}",
            "file_patch",
            "value.py",
            payload={
                "patch_sha256": patch_hash,
                "workstream_id": "stream-a",
            },
            before_sha256=hashlib.sha256(b"VALUE = 1\n").hexdigest(),
            expected_after_sha256=hashlib.sha256(b"VALUE = 2\n").hexdigest(),
        )

        result = execute_work_item(
            root=tmp_path,
            task_goal="Update value",
            workstream_goal="Update core value",
            work_item_id="core:value",
            file_path="value.py",
            instructions="Set VALUE to 2",
            acceptance_criteria=("VALUE = 2",),
            llm_call=fake_call,
            effect_repository=repository,
            task_id="task-a",
            workstream_id="stream-a",
        )

        recovered = repository.get_effect(pending.effect_id)
        assert result.accepted is True
        assert recovered is not None
        assert recovered.state is EffectState.RECONCILED
        assert target.read_text(encoding="utf-8") == "VALUE = 2\n"


@pytest.mark.parametrize(
    ("name", "content"),
    [
        ("background.jpg", b"\xff\xd8\xff\x00"),
        ("value.py", b"VALUE = 1\x00\n"),
        ("value.txt", b"value=\xff\n"),
    ],
)
def test_unsupported_worker_target_is_rejected_before_model_inference(
    tmp_path: Path,
    name: str,
    content: bytes,
):
    target = tmp_path / name
    target.write_bytes(content)
    calls = []

    with pytest.raises(UnsupportedWorkerTarget):
        execute_work_item(
            root=tmp_path,
            task_goal="Update target",
            workstream_goal="Update target",
            work_item_id="core:target",
            file_path=name,
            instructions="Update it",
            acceptance_criteria=("updated",),
            llm_call=lambda *args: calls.append(args),
        )

    assert calls == []


def test_typed_item_can_defer_behavior_tests_without_materializing_file(
    tmp_path: Path,
    monkeypatch,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")
    materialized = []

    def fake_call(system_prompt, user_message, tools):
        if tools[0]["name"] == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE",
                },
                {},
            )
        return ToolCallResult(
            "review_patch",
            {
                "verdict": "approved",
                "reviewer_feedback": "Code review complete",
                "next_instructions": "",
            },
            {},
        )

    with StateRepository(":memory:") as repository:
        result = execute_work_item(
            root=tmp_path,
            task_goal="Update value",
            workstream_goal="Update value",
            work_item_id="core:value",
            file_path="value.py",
            instructions="Set VALUE to 2",
            acceptance_criteria=("Behavior works",),
            llm_call=fake_call,
            effect_repository=repository,
            task_id="task-deferred",
            on_file_approved=materialized.append,
            defer_tests_to_integration=True,
        )
        receipt = repository.get_effect(result.effect_id)

    assert result.accepted is True
    assert result.syntax_status == "passed"
    assert result.test_status == "deferred"
    assert result.test_scope == "integration"
    assert receipt is not None and receipt.state is EffectState.APPLIED
    assert materialized == []


def test_compensated_patch_can_start_a_linked_application_generation(
    tmp_path: Path,
    monkeypatch,
):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr(safety, "SNAPSHOTS_ROOT", tmp_path / "snapshots")
    reviews = {"count": 0}
    events: list[dict] = []

    def fake_call(system_prompt, user_message, tools):
        if tools[0]["name"] == "submit_patch":
            return ToolCallResult(
                "submit_patch",
                {
                    "task_status": "completed",
                    "worker_feedback": "Updated",
                    "patch_content": "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE",
                },
                {},
            )
        reviews["count"] += 1
        approved = reviews["count"] == 2
        return ToolCallResult(
            "review_patch",
            {
                "verdict": "approved" if approved else "revise",
                "reviewer_feedback": "Approved" if approved else "Retry same patch",
                "next_instructions": "" if approved else "Retry",
            },
            {},
        )

    with StateRepository(":memory:") as repository:
        first = execute_work_item(
            root=tmp_path,
            task_goal="Update value",
            workstream_goal="Update value",
            work_item_id="core:value",
            file_path="value.py",
            instructions="Set VALUE to 2",
            acceptance_criteria=("VALUE = 2",),
            llm_call=fake_call,
            effect_repository=repository,
            task_id="task-reapply",
            on_event=events.append,
        )
        second = execute_work_item(
            root=tmp_path,
            task_goal="Update value",
            workstream_goal="Update value",
            work_item_id="core:value",
            file_path="value.py",
            instructions="Set VALUE to 2",
            acceptance_criteria=("VALUE = 2",),
            llm_call=fake_call,
            effect_repository=repository,
            task_id="task-reapply",
            on_event=events.append,
        )
        patches = [
            effect for effect in repository.list_effects("task-reapply") if effect.kind == "file_patch"
        ]

    assert first.accepted is False
    assert first.failure_kind == "reviewer_revise"
    assert second.accepted is True
    failed_event = next(event for event in events if event["type"] == "agent_failed")
    assert failed_event["error"] == "Retry same patch"
    assert failed_event["failure_kind"] == "reviewer_revise"
    assert failed_event["telemetry_scope"] == "attempt"
    assert failed_event["attempt_terminal"] is True
    assert len(patches) == 2
    assert patches[0].compensated is True
    assert patches[1].payload["application_generation"] == 2
    assert patches[1].payload["reapplies_effect_id"] == patches[0].effect_id
    assert target.read_text(encoding="utf-8") == "VALUE = 2\n"


@pytest.mark.parametrize(
    ("failure_kind", "heading"),
    [
        ("authentication", "AUTH ACCOUNT RECOVERY"),
        ("rate_limit", "RATE LIMIT RECOVERY"),
        ("transport", "TRANSPORT LEASE RECOVERY"),
        ("protocol_error", "PROTOCOL RECOVERY"),
        ("provider_payload", "PROVIDER PAYLOAD RECOVERY"),
        ("patch_rejected", "PATCH REJECTION RECOVERY"),
        ("machine_gate", "MACHINE GATE RECOVERY"),
        ("reviewer_revise", "REVIEWER REVISE RECOVERY"),
        ("contract_infeasible", "CONTRACT ISSUE RECOVERY"),
        ("dependency", "DEPENDENCY RECOVERY"),
        ("policy_denied", "APPROVAL POLICY RECOVERY"),
        ("sandbox_unavailable", "SANDBOX RECOVERY"),
        ("unsupported_worker_target", "SAFETY UNSUPPORTED RECOVERY"),
        ("rollback_conflict", "ROLLBACK EFFECT RECOVERY"),
        ("scheduler", "BACKEND SCHEDULER UNKNOWN RECOVERY"),
    ],
)
def test_failure_prompt_inputs_are_category_specific(failure_kind, heading):
    inputs = build_failure_prompt_inputs(
        failure_kind=failure_kind,
        contract={"id": "contract-a"},
        target="value.py",
        source="worker-a",
        patch="patch-a",
        test={"status": "failed"},
        review={"verdict": "revise"},
        request={"id": "request-a"},
        error="precise diagnostic",
        actor="worker",
        diagnostic_log_refs={"attempt_id": "attempt-a"},
    )

    assert heading in inputs["remediation_prompt"]
    assert inputs["failure_signature"]
    assert inputs["remediation_hints"]
    assert "precise diagnostic" in inputs["remediation_prompt"]
    assert "attempt-a" in inputs["remediation_prompt"]


def test_persisted_recovery_context_is_injected_into_worker_prompt(tmp_path: Path):
    target = tmp_path / "value.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    prompts = []
    recovery = build_failure_prompt_inputs(
        failure_kind="machine_gate",
        target="value.py",
        patch="patch-a",
        test={"status": "failed", "output": "SyntaxError line 1"},
        error="SyntaxError line 1",
        actor="worker",
        diagnostic_log_refs={"execution_attempt_id": "attempt-before"},
    )

    def fake_call(system_prompt, user_message, tools):
        prompts.append(user_message)
        return ToolCallResult(
            "submit_patch",
            {
                "task_status": "failed",
                "worker_feedback": "contract needs revision",
                "patch_content": "",
            },
            {},
        )

    result = execute_work_item(
        root=tmp_path,
        task_goal="Update value",
        workstream_goal="Update value",
        work_item_id="core:value",
        file_path="value.py",
        instructions="Set VALUE to 2",
        acceptance_criteria=("VALUE = 2",),
        llm_call=fake_call,
        recovery_context=recovery,
    )

    assert result.accepted is False
    assert "MACHINE GATE RECOVERY" in prompts[0]
    assert recovery["failure_signature"] in prompts[0]
    assert "attempt-before" in prompts[0]
