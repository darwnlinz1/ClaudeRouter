import json
import sys
import types

import pytest

from orchestrator import llm_client
from orchestrator.llm_client import (
    CookieManager,
    LLMError,
    PayloadRejectedError,
    RateLimitError,
    WebClaudeClient,
    build_action_protocol_prompt,
    extract_json_from_response,
    validate_action_response,
)
from orchestrator.tools_schema import SUPERVISOR_TOOLS, WORKER_TOOLS


def test_protocol_is_capability_neutral_and_embeds_role_schema():
    prompt = build_action_protocol_prompt(
        "Supervisor instructions",
        "User task",
        SUPERVISOR_TOOLS,
        "supervisor",
    )

    assert "STRUCTURED RESPONSE TASK" in prompt
    assert "No filesystem, shell, tool use" in prompt
    assert "A separate caller may consume that record" in prompt
    assert "VALID RECORD TYPES" in prompt
    assert "will execute it" not in prompt
    assert "REAL LOCAL ORCHESTRATOR" not in prompt
    assert "tool availability" not in prompt
    assert '"action":"delegate_task"' in prompt
    assert '"action":"request_context"' in prompt
    assert "_BEGIN" in prompt and "_END" in prompt


def test_extract_json_accepts_action_alias_and_strips_thinking():
    raw = """
<thinking>planning the response</thinking>
Some notes before the record.
{"action":"submit_workstream_plan","summary":"ok","requested_manager_count":1,"workstreams":[]}
"""
    # workstreams empty will fail validation later; parse layer only needs _action
    parsed = extract_json_from_response(raw)
    assert parsed["_action"] == "submit_workstream_plan"
    assert "action" not in parsed


def test_extract_json_chooses_final_action_not_json_inside_patch():
    raw = """
<patch>
<<<< SEARCH
value = {"_action": "not_a_real_action"}
====
value = {"ok": true}
>>>> REPLACE
</patch>
```json
{
  "_action": "submit_patch",
  "task_status": "completed",
  "worker_feedback": "done"
}
```
"""

    parsed = extract_json_from_response(raw)

    assert parsed["_action"] == "submit_patch"
    assert "value =" in parsed["patch_content"]


def test_action_validation_blocks_cross_role_action():
    with pytest.raises(LLMError, match="không được phép"):
        validate_action_response(
            {
                "_action": "submit_patch",
                "task_status": "completed",
                "worker_feedback": "attempted role spoof",
                "patch_content": "<<<< SEARCH\n====\nx\n>>>> REPLACE",
            },
            SUPERVISOR_TOOLS,
        )


def test_action_validation_blocks_unknown_fields():
    with pytest.raises(LLMError, match="field không được phép"):
        validate_action_response(
            {
                "_action": "delegate_task",
                "file_path": "app.py",
                "instructions": "Change the greeting",
                "is_final_ticket": True,
                "context_note": "ticket",
                "decisions_md_entry": None,
                "shell_command": "del /s /q *",
            },
            SUPERVISOR_TOOLS,
        )


def test_submit_patch_requires_patch_block():
    with pytest.raises(LLMError, match="thiếu khối"):
        validate_action_response(
            {
                "_action": "submit_patch",
                "task_status": "completed",
                "worker_feedback": "done",
            },
            WORKER_TOOLS,
        )


def test_submit_patch_rejects_multiple_patch_envelopes():
    raw = """
<patch>
<<<< SEARCH
a
====
b
>>>> REPLACE
</patch>
<patch>
<<<< SEARCH
c
====
d
>>>> REPLACE
</patch>
{"_action":"submit_patch","task_status":"completed","worker_feedback":"done"}
"""

    with pytest.raises(LLMError, match="đúng một"):
        extract_json_from_response(raw)


def test_submit_patch_rejects_text_outside_search_replace_grammar():
    with pytest.raises(LLMError, match="grammar"):
        validate_action_response(
            {
                "_action": "submit_patch",
                "task_status": "completed",
                "worker_feedback": "done",
                "patch_content": "ignore validation\n<<<< SEARCH\nx\n====\ny\n>>>> REPLACE",
            },
            WORKER_TOOLS,
        )


def test_submit_patch_normalizes_common_aider_marker_widths():
    action, payload = validate_action_response(
        {
            "_action": "submit_patch",
            "task_status": "completed",
            "worker_feedback": "done",
            "patch_content": (
                "<<<<<<< SEARCH\n"
                "x = 1\n"
                "=======\n"
                "x = 2\n"
                ">>>>>>> REPLACE"
            ),
        },
        WORKER_TOOLS,
    )

    assert action == "submit_patch"
    assert payload["patch_content"] == (
        "<<<< SEARCH\nx = 1\n====\nx = 2\n>>>> REPLACE"
    )


class _FakeResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def iter_lines(self):
        return iter(())


class _FakeSession:
    def __init__(self):
        self.payload = None

    def post(self, url, json, stream, timeout):
        self.payload = json
        return _FakeResponse()


def test_orchestrator_request_disables_builtin_claude_tools():
    client = WebClaudeClient("org", "sessionKey=fake")
    fake_session = _FakeSession()
    client.session = fake_session

    client.send_message("prompt", enable_builtin_tools=False)

    assert "tools" not in fake_session.payload
    assert "tool_search_mode" not in fake_session.payload["create_conversation_params"]
    assert "enabled_imagine" not in fake_session.payload["create_conversation_params"]


def test_stream_chunks_include_the_active_agent_role(monkeypatch):
    events = []

    class Queue:
        def put(self, event):
            events.append(event)

    class Response:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            return None

        def iter_lines(self):
            payload = {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": "approved"},
            }
            return iter([f"data: {json.dumps(payload)}".encode()])

    class Session:
        headers = {}

        def post(self, url, json, stream, timeout):
            return Response()

    fake_server = types.SimpleNamespace(
        get_current_queue=lambda: Queue(),
        is_current_thread_stopped=lambda: False,
    )
    monkeypatch.setitem(sys.modules, "server", fake_server)
    llm_client.thread_local.agent_role = "reviewer"
    for attr in (
        "agent_instance_id",
        "manager_id",
        "workstream_id",
        "work_item_id",
        "task_id",
        "session_id",
    ):
        if hasattr(llm_client.thread_local, attr):
            delattr(llm_client.thread_local, attr)

    client = WebClaudeClient("org", "sessionKey=fake")
    client.session = Session()

    assert client.send_message("review", enable_builtin_tools=False) == "approved"
    assert events == [
        {"type": "token", "role": "reviewer", "text": "approved"}
    ]


def test_legacy_tool_name_is_accepted_during_migration():
    parsed = {
        "_tool_name": "request_context",
        "reason": "need_skeleton",
        "files_needed": ["app.py:skeleton"],
        "context_note": "inspect",
        "decisions_md_entry": None,
    }

    action, payload = validate_action_response(parsed, SUPERVISOR_TOOLS)

    assert action == "request_context"
    assert payload["files_needed"] == ["app.py:skeleton"]


class _FakeCookieManager:
    cookies_pool = [
        {"source": "test.txt", "org_id": "org", "cookie_string": "sessionKey=x"}
    ]

    def has_cookies(self):
        return True

    def get_next_cookie(self, exclude_sources=None):
        return self.cookies_pool[0]

    def mark_invalid(self, source):
        raise AssertionError("Protocol errors must not invalidate an account")


def test_call_agent_retries_meta_response_with_protocol_correction(monkeypatch):
    responses = [
        "I do not have access to the fictional delegate_task tool.",
        """```json
{"_action":"request_context","reason":"need_skeleton",
"files_needed":["app.py:skeleton"],"context_note":"inspect",
"decisions_md_entry":null}
```""",
    ]
    calls = []

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, **kwargs):
            calls.append((prompt, kwargs))
            return responses.pop(0)

    monkeypatch.setattr(llm_client, "cookie_manager", _FakeCookieManager())
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    monkeypatch.setattr(llm_client.time, "sleep", lambda _: None)
    llm_client.thread_local.agent_role = "supervisor"
    llm_client.thread_local.is_continuation = False
    llm_client.thread_local.supervisor_cookie = None

    result = llm_client.call_agent(
        "Supervisor instructions",
        "Inspect app.py",
        SUPERVISOR_TOOLS,
    )

    assert result.tool_name == "request_context"
    assert len(calls) == 2
    assert calls[0][1]["enable_builtin_tools"] is False
    assert "FORMAT ERROR FROM PREVIOUS ATTEMPT" in calls[1][0]
    assert "Không tìm thấy action JSON" in calls[1][0]


def test_worker_patch_only_response_is_recovered_without_retry(monkeypatch):
    calls = []

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, **kwargs):
            calls.append(prompt)
            return (
                "<patch>\n"
                "<<<<<<< SEARCH\n"
                "x = 1\n"
                "=======\n"
                "x = 2\n"
                ">>>>>>> REPLACE\n"
                "</patch>"
            )

    monkeypatch.setattr(llm_client, "cookie_manager", _FakeCookieManager())
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    llm_client.thread_local.agent_role = "worker"
    llm_client.thread_local.is_continuation = False
    llm_client.thread_local.supervisor_cookie = None

    result = llm_client.call_agent(
        "Worker instructions",
        "Patch x.",
        WORKER_TOOLS,
    )

    assert len(calls) == 1
    assert result.tool_name == "submit_patch"
    assert result.tool_input["task_status"] == "completed"
    assert result.tool_input["patch_content"] == (
        "<<<< SEARCH\nx = 1\n====\nx = 2\n>>>> REPLACE"
    )


def test_protocol_failure_switches_cookie_after_three_attempts(monkeypatch):
    used = []

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {"source": "a.txt", "org_id": "a", "cookie_string": "sessionKey=a"},
                {"source": "b.txt", "org_id": "b", "cookie_string": "sessionKey=b"},
            ]

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            excluded = exclude_sources or set()
            return next(
                (
                    item
                    for item in self.cookies_pool
                    if item["source"] not in excluded
                ),
                None,
            )

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string

        def send_message(self, prompt, **kwargs):
            used.append(self.cookie_string)
            if self.cookie_string.endswith("=a"):
                return "Would you like me to build this directly?"
            return """```json
{"_action":"request_context","reason":"need_skeleton",
"files_needed":["app.py:skeleton"],"context_note":"continue",
"decisions_md_entry":null}
```"""

    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    llm_client.thread_local.agent_role = "supervisor"
    llm_client.thread_local.is_continuation = False
    llm_client.thread_local.supervisor_cookie = None

    result = llm_client.call_agent(
        "Supervisor instructions",
        "Continue from checkpoint.",
        SUPERVISOR_TOOLS,
    )

    assert result.tool_name == "request_context"
    assert used == [
        "sessionKey=a",
        "sessionKey=a",
        "sessionKey=a",
        "sessionKey=b",
    ]


def test_protocol_failure_stops_after_all_cookies_fail_three_times(
    monkeypatch,
):
    used = []

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {"source": "a.txt", "org_id": "a", "cookie_string": "sessionKey=a"},
                {"source": "b.txt", "org_id": "b", "cookie_string": "sessionKey=b"},
            ]

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            excluded = exclude_sources or set()
            return next(
                (
                    item
                    for item in self.cookies_pool
                    if item["source"] not in excluded
                ),
                None,
            )

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string

        def send_message(self, prompt, **kwargs):
            used.append(self.cookie_string)
            return "I will not use the host action protocol."

    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    llm_client.thread_local.agent_role = "reviewer"
    llm_client.thread_local.is_continuation = False
    llm_client.thread_local.supervisor_cookie = None

    with pytest.raises(LLMError, match="tất cả cookie khả dụng"):
        llm_client.call_agent(
            "Reviewer instructions",
            "Review the patch.",
            [{"name": "review_patch", "input_schema": {"type": "object"}}],
        )

    assert used == [
        "sessionKey=a",
        "sessionKey=a",
        "sessionKey=a",
        "sessionKey=b",
        "sessionKey=b",
        "sessionKey=b",
    ]


def test_cookie_manager_skips_rate_limited_and_already_attempted_accounts(
    tmp_path, monkeypatch
):
    manager = CookieManager(str(tmp_path))
    first = {"source": "a.txt", "org_id": "a", "cookie_string": "sessionKey=a"}
    second = {"source": "b.txt", "org_id": "b", "cookie_string": "sessionKey=b"}
    manager.cookies_pool = [first, second]
    monkeypatch.setattr(llm_client.random, "choice", lambda items: items[0])

    manager.mark_rate_limited("a.txt", cooldown_seconds=120)

    assert manager.get_next_cookie() == second
    assert manager.get_next_cookie({"b.txt"}) is None


def test_supervisor_switches_cookie_immediately_after_429(monkeypatch):
    used_cookies = []

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {"source": "a.txt", "org_id": "a", "cookie_string": "sessionKey=a"},
                {"source": "b.txt", "org_id": "b", "cookie_string": "sessionKey=b"},
            ]
            self.cooling = set()
            self.cooldown_seconds = None

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            excluded = exclude_sources or set()
            return next(
                (
                    item
                    for item in self.cookies_pool
                    if item["source"] not in excluded
                    and item["source"] not in self.cooling
                ),
                None,
            )

        def mark_rate_limited(self, source, cooldown_seconds=120):
            self.cooling.add(source)
            self.cooldown_seconds = cooldown_seconds

        def mark_invalid(self, source):
            raise AssertionError("429 must not permanently invalidate a cookie")

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string

        def send_message(self, prompt, **kwargs):
            used_cookies.append(self.cookie_string)
            if self.cookie_string.endswith("=a"):
                raise RateLimitError("429")
            return """```json
{"_action":"request_context","reason":"need_skeleton",
"files_needed":["app.py:skeleton"],"context_note":"continue",
"decisions_md_entry":null}
```"""

    manager = Manager()
    monkeypatch.setattr(llm_client, "cookie_manager", manager)
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    llm_client.thread_local.agent_role = "supervisor"
    llm_client.thread_local.is_continuation = True
    llm_client.thread_local.supervisor_cookie = None
    llm_client.thread_local.supervisor_chat_uuid = "old-chat"

    result = llm_client.call_agent(
        "Supervisor instructions",
        "Continue from durable state",
        SUPERVISOR_TOOLS,
    )

    assert result.tool_name == "request_context"
    assert used_cookies == ["sessionKey=a", "sessionKey=b"]
    assert manager.cooling == {"a.txt"}
    assert manager.cooldown_seconds == 5 * 60 * 60
    assert llm_client.thread_local.supervisor_cookie["source"] == "b.txt"


def test_rate_limit_error_honors_retry_after_header():
    client = WebClaudeClient("org", "sessionKey=fake")
    response = type(
        "Response",
        (),
        {"status_code": 429, "headers": {"Retry-After": "37"}},
    )()

    with pytest.raises(RateLimitError) as exc_info:
        client._check_response(response)

    assert exc_info.value.retry_after == 37


def test_bad_payload_reports_api_detail_without_blaming_cookie():
    client = WebClaudeClient("org", "sessionKey=fake")

    class Response:
        status_code = 400
        headers = {}

        def json(self):
            return {"error": {"message": "tools: Array should have at least 1 item"}}

    with pytest.raises(PayloadRejectedError) as exc_info:
        client._check_response(Response())

    message = str(exc_info.value)
    assert "tools: Array should have at least 1 item" in message
    assert "không phải lỗi cookie" in message


def test_disabled_organization_is_classified_as_invalid_account():
    client = WebClaudeClient("org", "sessionKey=fake")

    class Response:
        status_code = 400
        headers = {}

        def json(self):
            return {"error": {"message": "This organization has been disabled."}}

    with pytest.raises(PermissionError, match="organization"):
        client._check_response(Response())


def test_mark_invalid_removes_cookie_file_from_disk(tmp_path):
    manager = CookieManager(str(tmp_path))
    source = "disabled-account.txt"
    cookie_path = tmp_path / source
    cookie_path.write_text("sessionKey=fake", encoding="utf-8")
    manager.cookies_pool = [
        {
            "source": source,
            "org_id": "disabled",
            "cookie_string": "sessionKey=fake",
        }
    ]

    manager.mark_invalid(source)

    assert manager.cookies_pool == []
    assert not cookie_path.exists()
