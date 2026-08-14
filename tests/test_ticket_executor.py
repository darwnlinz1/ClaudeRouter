import hashlib
import sys
import threading
from pathlib import Path

from orchestrator import project_workspace, safety
from orchestrator.effects import EffectState
from orchestrator.llm_client import ToolCallResult
from orchestrator.state_repository import StateRepository
from orchestrator.ticket_executor import execute_work_item


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
                    "patch_content": (
                        "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
                    ),
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
                    "patch_content": (
                        "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
                    ),
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
                "patch_content": (
                    "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
                ),
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
                "patch_content": (
                    "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
                ),
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
                    "patch_content": (
                        "<<<< SEARCH\nVALUE = 1\n====\nVALUE = 2\n>>>> REPLACE"
                    ),
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
        rollback = next(
            effect for effect in effects if effect.kind == "file_rollback"
        )
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
