# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pytest

from orchestrator import state_store
from orchestrator.llm_client import ToolCallResult
from orchestrator.orchestrator import run_session


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "app.py").write_text(
        "def greet(name):\n    return 'hello ' + name\n", encoding="utf-8"
    )
    (tmp_path / "rules.json").write_text('{"style": "pep8"}', encoding="utf-8")
    return tmp_path


def _scripted(calls: list[ToolCallResult]):
    """Adapt legacy test fixtures to the Supervisor -> Worker protocol."""
    calls = list(calls)

    def fake_call(system_prompt: str, user_message: str, tools):
        tool_names = [t["name"] for t in tools]

        if tool_names == ["review_patch"]:
            if calls and calls[0].tool_name == "review_patch":
                return calls.pop(0)
            return ToolCallResult(
                tool_name="review_patch",
                tool_input={
                    "verdict": "approved",
                    "reviewer_feedback": "Patch đúng yêu cầu và machine gate đã Pass.",
                    "next_instructions": "",
                },
                raw_response={},
            )

        scripted = calls[0]
        if "delegate_task" in tool_names:
            assert "request_context" in tool_names
            if scripted.tool_name == "request_context":
                return calls.pop(0)
            old = scripted.tool_input
            return ToolCallResult(
                tool_name="delegate_task",
                tool_input={
                    "file_path": old["file_path"],
                    "instructions": old.get("context_note", "Apply the requested patch."),
                    "is_final_ticket": old.get("is_final_ticket", True),
                    "context_note": old.get("context_note", ""),
                    "decisions_md_entry": old.get("decisions_md_entry"),
                },
                raw_response={},
            )

        assert tool_names == ["submit_patch"]
        scripted = calls.pop(0)
        old = scripted.tool_input
        patch = (
            "<patch>\n<<<< SEARCH\n"
            f"{old['old_string']}\n"
            "====\n"
            f"{old['new_string']}\n"
            ">>>> REPLACE\n</patch>"
        )
        return ToolCallResult(
            tool_name="submit_patch",
            tool_input={
                "task_status": old.get("task_status", "in_progress"),
                "worker_feedback": old.get("context_note", ""),
            },
            raw_response={"content": patch},
        )

    return fake_call


def test_happy_path_single_turn_completion(project: Path):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hi ' + name",
                    "task_status": "completed",
                    "context_note": "Changed greeting from 'hello' to 'hi' per task.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        ]
    )

    result = run_session(
        root=project,
        task_description="Change the greeting to say 'hi' instead of 'hello'.",
        source_files=["src/app.py"],
        llm_call=fake,
    )

    assert result.stopped_reason == "task_completed"
    assert len(result.turns) == 1
    assert result.turns[0].accepted
    assert "hi " in (project / "src" / "app.py").read_text(encoding="utf-8")
    assert result.final_state["last_worker_feedback"] == "Changed greeting from 'hello' to 'hi' per task."
    assert result.final_state["last_execution_result"] == (
        "Partially verified: syntax=passed; tests=not_configured"
    )
    assert result.final_state["session_goal"].startswith("Change the greeting")
    assert result.final_state["active_ticket"] is None
    assert result.final_state["completed_tickets"] == [
        {
            "turn": 1,
            "file_path": "src/app.py",
            "summary": "Changed greeting from 'hello' to 'hi' per task.",
            "verification": "Partially verified: syntax=passed; tests=not_configured",
            "reviewer_feedback": "Patch đúng yêu cầu và machine gate đã Pass.",
            "status": "approved",
        }
    ]


def test_model_tries_to_patch_protected_file_is_blocked_and_loop_continues(project: Path):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "rules.json",
                    "old_string": '{"style": "pep8"}',
                    "new_string": '{"style": "black"}',
                    "task_status": "in_progress",
                    "context_note": "trying to sneak a rules.json edit",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hi ' + name",
                    "task_status": "completed",
                    "context_note": "done after realizing rules.json is off-limits",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
        ]
    )

    result = run_session(
        root=project,
        task_description="Change the greeting.",
        source_files=["src/app.py", "rules.json"],
        llm_call=fake,
    )

    assert len(result.turns) == 2
    assert result.turns[0].accepted is False
    assert "protected" in result.turns[0].detail.lower() or "rules.json" in result.turns[0].detail
    # rules.json content must be untouched
    assert json.loads((project / "rules.json").read_text(encoding="utf-8")) == {"style": "pep8"}
    assert result.turns[1].accepted is True
    assert result.stopped_reason == "task_completed"


def test_request_context_continues_until_turn_limit(project: Path):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="request_context",
                tool_input={
                    "reason": "task_unclear",
                    "files_needed": ["src/config.py"],
                    "context_note": "Need config.py to know the default greeting.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        ]
    )

    result = run_session(
        root=project,
        task_description="Change the default greeting.",
        source_files=["src/app.py"],
        llm_call=fake,
        max_turns=1,
    )

    assert result.stopped_reason == "max_turns_reached"
    assert len(result.turns) == 1
    assert result.turns[0].tool_name == "request_context"


def test_worker_failed_status_never_applies_patch(project: Path):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'unsafe change'",
                    "task_status": "failed",
                    "context_note": "Could not complete the ticket safely.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        ]
    )

    result = run_session(
        root=project,
        task_description="Change the greeting.",
        source_files=["src/app.py"],
        llm_call=fake,
        max_turns=1,
    )

    assert result.turns[0].accepted is False
    assert "unsafe change" not in (project / "src" / "app.py").read_text(
        encoding="utf-8"
    )
    assert result.final_state["last_execution_result"].startswith("Fail:")


def test_edit_mode_request_context_rejects_missing_file(project: Path):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="request_context",
                tool_input={
                    "reason": "need_full_file",
                    "files_needed": ["src/missing.py"],
                    "context_note": "Xin file không tồn tại",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        ]
    )

    result = run_session(
        root=project,
        task_description="Inspect missing path.",
        source_files=[],
        llm_call=fake,
        max_turns=1,
    )

    assert result.turns[0].accepted is False
    assert "chưa tồn tại" in result.turns[0].detail


def test_supervisor_cannot_request_sensitive_context(project: Path):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="request_context",
                tool_input={
                    "reason": "other",
                    "files_needed": [".env"],
                    "context_note": "Try to read credentials.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        ]
    )

    result = run_session(
        root=project,
        task_description="Inspect configuration.",
        source_files=["src/app.py"],
        llm_call=fake,
        max_turns=1,
    )

    assert result.turns[0].accepted is False
    assert "nhạy cảm" in result.turns[0].detail


def test_new_project_delegates_new_file_directly_through_all_agents(
    tmp_path: Path,
):
    events = []

    def fake_call(system_prompt: str, user_message: str, tools):
        action_names = [tool["name"] for tool in tools]
        if "delegate_task" in action_names:
            return ToolCallResult(
                tool_name="delegate_task",
                tool_input={
                    "file_path": "app/config.py",
                    "instructions": "Create a valid Python config module.",
                    "is_final_ticket": True,
                    "context_note": "Create config.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        if action_names == ["submit_patch"]:
            return ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "task_status": "completed",
                    "worker_feedback": "Created config module.",
                },
                raw_response={
                    "content": (
                        "<patch>\n<<<< SEARCH\n====\n"
                        "APP_NAME = 'Jarvis'\n"
                        ">>>> REPLACE\n</patch>"
                    )
                },
            )
        return ToolCallResult(
            tool_name="review_patch",
            tool_input={
                "verdict": "approved",
                "reviewer_feedback": "Valid module.",
                "next_instructions": "",
            },
            raw_response={},
        )

    result = run_session(
        root=tmp_path,
        task_description="Create a new project.",
        source_files=[],
        allow_new_files=True,
        llm_call=fake_call,
        on_event=events.append,
        max_turns=1,
    )

    assert result.stopped_reason == "task_completed"
    assert (tmp_path / "app" / "config.py").read_text(
        encoding="utf-8"
    ) == "APP_NAME = 'Jarvis'"
    assert any(
        event.get("type") == "turn_phase" and event.get("phase") == "worker"
        for event in events
    )
    assert any(
        event.get("type") == "turn_phase" and event.get("phase") == "reviewer"
        for event in events
    )
    assert [
        event["action"]
        for event in events
        if event.get("type") == "agent_action"
    ] == ["delegate_task", "submit_patch", "review_patch"]
    assert [
        event["role"]
        for event in events
        if event.get("type") == "agent_progress"
    ] == ["supervisor", "worker", "reviewer"]


def test_duplicate_context_request_is_rejected_with_delegate_guidance(
    project: Path,
):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="request_context",
                tool_input={
                    "reason": "need_specific_lines",
                    "files_needed": ["src/app.py"],
                    "context_note": "Request the same file again.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        ]
    )

    result = run_session(
        root=project,
        task_description="Change app.py.",
        source_files=["src/app.py"],
        llm_call=fake,
        max_turns=1,
    )

    assert result.turns[0].accepted is False
    assert "Không request_context lại" in result.turns[0].detail
    assert "delegate_task" in result.turns[0].detail


def test_resume_session_preserves_memory_and_continues_turn_numbers(
    project: Path,
):
    paths = state_store.ProjectPaths.for_root(project)
    saved = state_store.load_state(paths)
    saved.update(
        {
            "session_goal": "Original long-running goal",
            "turn_count": 5,
            "completed_tickets": [
                {
                    "turn": 4,
                    "file_path": "src/finished.py",
                    "status": "approved",
                }
            ],
            "context_manifest": [
                {
                    "path": "src/app.py",
                    "modifier": "",
                    "sha256": None,
                }
            ],
        }
    )
    state_store.save_state(paths, saved)
    messages = []
    events = []

    def fake_call(system_prompt: str, user_message: str, tools):
        messages.append(user_message)
        return ToolCallResult(
            tool_name="request_context",
            tool_input={
                "reason": "need_specific_lines",
                "files_needed": ["src/app.py"],
                "context_note": "Already loaded.",
                "decisions_md_entry": None,
            },
            raw_response={},
        )

    result = run_session(
        root=project,
        task_description="Original long-running goal",
        source_files=[],
        resume_session=True,
        max_turns=1,
        llm_call=fake_call,
        on_event=events.append,
    )

    assert result.final_state["session_goal"] == "Original long-running goal"
    assert result.final_state["completed_tickets"][0]["file_path"] == "src/finished.py"
    assert result.final_state["turn_count"] == 6
    assert '"turn_count": 6' in messages[0]
    assert '"file_path": "src/finished.py"' in messages[0]
    assert any(
        event.get("type") == "turn_start" and event.get("turn") == 6
        for event in events
    )


def test_nonunique_anchor_increments_consecutive_error_count_then_resets_on_success(project: Path):
    # Make "return" ambiguous-ish by adding a second function with a similar line.
    (project / "src" / "app.py").write_text(
        "def greet(name):\n    return 'hello ' + name\n\n\ndef farewell(name):\n    return 'hello ' + name\n",
        encoding="utf-8",
    )
    bad_patch = {
        "file_path": "src/app.py",
        "old_string": "return 'hello ' + name",  # appears twice now
        "new_string": "return 'hi ' + name",
        "task_status": "in_progress",
        "context_note": "attempting a patch with a non-unique anchor",
        "decisions_md_entry": None,
    }
    fake = _scripted(
        [
            ToolCallResult(tool_name="submit_patch", tool_input=dict(bad_patch), raw_response={}),
            ToolCallResult(tool_name="submit_patch", tool_input=dict(bad_patch), raw_response={}),
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "def greet(name):\n    return 'hello ' + name",
                    "new_string": "def greet(name):\n    return 'hi ' + name",
                    "task_status": "completed",
                    "context_note": "used a wider, unique anchor this time",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
        ]
    )

    result = run_session(
        root=project,
        task_description="Change greet()'s greeting to 'hi'.",
        source_files=["src/app.py"],
        llm_call=fake,
    )

    assert result.turns[0].accepted is False
    assert result.turns[1].accepted is False
    paths = state_store.ProjectPaths.for_root(project)
    # After the two identical failures, consecutive_error_count should have
    # reached 2 before the successful third turn reset it to 0.
    assert result.turns[2].accepted is True
    assert result.final_state["consecutive_error_count"] == 0
    assert result.stopped_reason == "task_completed"


def test_syntax_gate_rolls_back_a_patch_that_breaks_python_syntax(project: Path):
    fake = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hello ' + name(",  # invalid syntax
                    "task_status": "in_progress",
                    "context_note": "oops, broke syntax",
                    "decisions_md_entry": None,
                },
                raw_response={},
            )
        ]
    )

    result = run_session(
        root=project,
        task_description="Change the greeting.",
        source_files=["src/app.py"],
        llm_call=fake,
        max_turns=1,  # only assert on the single rejected turn we scripted
    )

    assert result.turns[0].accepted is False
    assert "syntaxerror" in result.turns[0].detail.lower()
    content = (project / "src" / "app.py").read_text(encoding="utf-8")
    assert "name(" not in content  # rolled back


def test_decisions_md_is_appended_not_overwritten(project: Path):
    paths = state_store.ProjectPaths.for_root(project)
    state_store.append_decision(paths, "Earlier decision: use snake_case everywhere.")

    fake = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hi ' + name",
                    "task_status": "completed",
                    "context_note": "done",
                    "decisions_md_entry": "New decision: greetings should be casual.",
                },
                raw_response={},
            )
        ]
    )

    run_session(
        root=project,
        task_description="Change the greeting.",
        source_files=["src/app.py"],
        llm_call=fake,
    )

    text = (project / "DECISIONS.md").read_text(encoding="utf-8")
    assert "Earlier decision: use snake_case everywhere." in text
    assert "New decision: greetings should be casual." in text


def test_next_supervisor_turn_receives_worker_feedback_and_execution_result(project: Path):
    scripted = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hello ' + name(",
                    "task_status": "in_progress",
                    "context_note": "Đã sửa lời chào nhưng có thể thiếu dấu đóng ngoặc.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hi ' + name",
                    "task_status": "completed",
                    "context_note": "Đã sửa lại bằng biểu thức hợp lệ.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
        ]
    )
    supervisor_messages: list[str] = []

    def capturing_call(system_prompt: str, user_message: str, tools):
        if "delegate_task" in [tool["name"] for tool in tools]:
            supervisor_messages.append(user_message)
        return scripted(system_prompt, user_message, tools)

    result = run_session(
        root=project,
        task_description="Change the greeting.",
        source_files=["src/app.py"],
        llm_call=capturing_call,
        max_turns=2,
    )

    assert result.stopped_reason == "task_completed"
    assert len(supervisor_messages) == 2
    assert "Đã sửa lời chào nhưng có thể thiếu dấu đóng ngoặc." in supervisor_messages[1]
    assert (
        '"last_execution_result": "Fail: syntax=failed; tests=not_run; '
        "SyntaxError:"
    ) in supervisor_messages[1]
    assert '"status": "machine_gate_failed"' in supervisor_messages[1]
    assert '"file_path": "src/app.py"' in supervisor_messages[1]


def test_reviewer_can_reject_then_supervisor_redelegates(project: Path):
    scripted = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hi ' + name",
                    "task_status": "completed",
                    "context_note": "Đã đổi lời chào.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
            ToolCallResult(
                tool_name="review_patch",
                tool_input={
                    "verdict": "revise",
                    "reviewer_feedback": "Cần giữ tương thích với lời chào cũ.",
                    "next_instructions": "Thêm fallback rõ ràng rồi nộp lại.",
                },
                raw_response={},
            ),
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hi ' + name",
                    "task_status": "completed",
                    "context_note": "Đã kiểm tra lại yêu cầu tương thích.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
        ]
    )
    supervisor_messages: list[str] = []

    def capturing_call(system_prompt: str, user_message: str, tools):
        if "delegate_task" in [tool["name"] for tool in tools]:
            supervisor_messages.append(user_message)
        return scripted(system_prompt, user_message, tools)

    result = run_session(
        root=project,
        task_description="Change the greeting.",
        source_files=["src/app.py"],
        llm_call=capturing_call,
        max_turns=2,
    )

    assert [turn.accepted for turn in result.turns] == [False, True]
    assert result.stopped_reason == "task_completed"
    assert '"last_review_verdict": "revise"' in supervisor_messages[1]
    assert "Thêm fallback rõ ràng rồi nộp lại." in supervisor_messages[1]
    assert result.final_state["last_review_verdict"] == "approved"


def test_nonfinal_ticket_keeps_multi_file_workflow_running(project: Path):
    scripted = _scripted(
        [
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hello ' + name",
                    "new_string": "return 'hey ' + name",
                    "task_status": "completed",
                    "is_final_ticket": False,
                    "context_note": "Hoàn thành ticket đầu, còn ticket sau.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
            ToolCallResult(
                tool_name="submit_patch",
                tool_input={
                    "file_path": "src/app.py",
                    "old_string": "return 'hey ' + name",
                    "new_string": "return 'hi ' + name",
                    "task_status": "completed",
                    "is_final_ticket": True,
                    "context_note": "Hoàn thành ticket cuối.",
                    "decisions_md_entry": None,
                },
                raw_response={},
            ),
        ]
    )

    result = run_session(
        root=project,
        task_description="Complete two sequential tickets.",
        source_files=["src/app.py"],
        llm_call=scripted,
        max_turns=2,
    )

    assert len(result.turns) == 2
    assert result.stopped_reason == "task_completed"
    assert "return 'hi ' + name" in (
        project / "src" / "app.py"
    ).read_text(encoding="utf-8")


def test_folder_only_delegate_auto_fetches_existing_file(project: Path):
    """User chỉ chọn folder; agent delegate path đã có trên đĩa → local file agent nạp."""
    events: list[dict] = []
    seen_tree = {"ok": False}

    calls = [
        ToolCallResult(
            tool_name="delegate_task",
            tool_input={
                "file_path": "src/app.py",
                "instructions": "Đổi greet thành xin chào.",
                "is_final_ticket": True,
                "context_note": "Sửa greet.",
                "decisions_md_entry": None,
            },
            raw_response={},
        ),
        ToolCallResult(
            tool_name="submit_patch",
            tool_input={
                "task_status": "completed",
                "worker_feedback": "Đã đổi chuỗi chào.",
            },
            raw_response={
                "content": (
                    "<patch>\n<<<< SEARCH\n"
                    "def greet(name):\n    return 'hello ' + name\n"
                    "====\n"
                    "def greet(name):\n    return 'xin chao ' + name\n"
                    ">>>> REPLACE\n</patch>"
                )
            },
        ),
        ToolCallResult(
            tool_name="review_patch",
            tool_input={
                "verdict": "approved",
                "reviewer_feedback": "OK",
                "next_instructions": "",
            },
            raw_response={},
        ),
    ]

    def fake_call(system_prompt: str, user_message: str, tools):
        tool_names = [t["name"] for t in tools]
        if "delegate_task" in tool_names:
            if "PROJECT TREE" in user_message and "src/app.py" in user_message:
                seen_tree["ok"] = True
            assert "(chưa nạp nội dung" in user_message or "SOURCE FILES" in user_message
            return calls.pop(0)
        if tool_names == ["submit_patch"]:
            return calls.pop(0)
        return calls.pop(0)

    result = run_session(
        root=project,
        task_description="Sửa greet trong app.",
        source_files=[],
        llm_call=fake_call,
        on_event=events.append,
        max_turns=1,
    )

    assert seen_tree["ok"] is True
    assert result.stopped_reason == "task_completed"
    assert "xin chao" in (project / "src" / "app.py").read_text(encoding="utf-8")
    assert any(
        e.get("type") == "file_fetched" and e.get("file_path") == "src/app.py"
        for e in events
    )


def test_folder_only_request_context_loads_from_disk(project: Path):
    events: list[dict] = []
    second_turn_has_content = {"ok": False}

    calls = [
        ToolCallResult(
            tool_name="request_context",
            tool_input={
                "reason": "need_full_file",
                "files_needed": ["src/app.py"],
                "context_note": "Nạp app.py từ máy",
                "decisions_md_entry": None,
            },
            raw_response={},
        ),
        ToolCallResult(
            tool_name="delegate_task",
            tool_input={
                "file_path": "src/app.py",
                "instructions": "Đổi hello thành hi.",
                "is_final_ticket": True,
                "context_note": "Patch greet.",
                "decisions_md_entry": None,
            },
            raw_response={},
        ),
        ToolCallResult(
            tool_name="submit_patch",
            tool_input={
                "task_status": "completed",
                "worker_feedback": "Done",
            },
            raw_response={
                "content": (
                    "<patch>\n<<<< SEARCH\n"
                    "return 'hello ' + name\n"
                    "====\n"
                    "return 'hi ' + name\n"
                    ">>>> REPLACE\n</patch>"
                )
            },
        ),
        ToolCallResult(
            tool_name="review_patch",
            tool_input={
                "verdict": "approved",
                "reviewer_feedback": "OK",
                "next_instructions": "",
            },
            raw_response={},
        ),
    ]

    def fake_call(system_prompt: str, user_message: str, tools):
        tool_names = [t["name"] for t in tools]
        if "delegate_task" in tool_names:
            if "return 'hello ' + name" in user_message:
                second_turn_has_content["ok"] = True
            return calls.pop(0)
        if tool_names == ["submit_patch"]:
            return calls.pop(0)
        return calls.pop(0)

    result = run_session(
        root=project,
        task_description="Sửa app không seed file.",
        source_files=[],
        llm_call=fake_call,
        on_event=events.append,
        max_turns=2,
    )

    assert second_turn_has_content["ok"] is True
    assert result.stopped_reason == "task_completed"
    assert any(e.get("action") == "request_context" for e in events)
