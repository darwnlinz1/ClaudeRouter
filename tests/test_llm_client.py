import json
import sys
import time
import types

import pytest

from orchestrator import llm_client
from orchestrator.account_lease import AccountLease
from orchestrator.budget import (
    BudgetExceededError,
    BudgetLimits,
    register_task_budget,
    remove_task_budget,
)
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


def test_submit_patch_normalizes_empty_search_for_new_file():
    action, payload = validate_action_response(
        {
            "_action": "submit_patch",
            "task_status": "completed",
            "worker_feedback": "created",
            "patch_content": "<<<< SEARCH\n====\nVALUE = 42\n>>>> REPLACE",
        },
        WORKER_TOOLS,
    )

    assert action == "submit_patch"
    assert payload["patch_content"] == (
        "<<<< SEARCH\n====\nVALUE = 42\n>>>> REPLACE"
    )


class _FakeResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def iter_lines(self):
        return iter([b'data: {"type":"message_stop"}'])


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
            payloads = [
                {
                    "type": "content_block_delta",
                    "delta": {"type": "text_delta", "text": "approved"},
                },
                {"type": "message_stop"},
            ]
            return iter(
                [f"data: {json.dumps(payload)}".encode() for payload in payloads]
            )

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
            "call_id",
            "event_sink",
    ):
        if hasattr(llm_client.thread_local, attr):
            delattr(llm_client.thread_local, attr)

    client = WebClaudeClient("org", "sessionKey=fake")
    client.session = Session()

    assert client.send_message("review", enable_builtin_tools=False) == "approved"
    assert events == [
        {"type": "token", "role": "reviewer", "text": "approved"}
    ]


def test_web_stream_rejects_eof_without_terminal_event():
    class Response:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            return None

        def iter_lines(self):
            return iter(
                [
                    b'data: {"type":"content_block_delta",'
                    b'"delta":{"type":"text_delta","text":"partial"}}'
                ]
            )

        def close(self):
            pass

    class Session:
        headers = {}

        def post(self, *args, **kwargs):
            return Response()

    client = WebClaudeClient("org", "sessionKey=fake")
    client.session = Session()

    with pytest.raises(llm_client.IncompleteStreamError, match="terminal"):
        client.send_message("review", enable_builtin_tools=False)
    assert client.last_stream_chunks == ()


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


def test_provider_prompt_with_secret_is_blocked_before_transport(monkeypatch):
    events = []

    class UnexpectedClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("transport must not start for a secret-bearing prompt")

    monkeypatch.setattr(llm_client, "cookie_manager", _FakeCookieManager())
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "WebClaudeClient", UnexpectedClient)
    monkeypatch.setattr(
        llm_client.thread_local,
        "event_sink",
        events.append,
        raising=False,
    )
    llm_client.thread_local.agent_role = "worker"

    with pytest.raises(
        llm_client.PayloadRejectedError,
        match="potential credentials",
    ):
        llm_client.call_agent(
            "system",
            "Deploy AKIAIOSFODNN7EXAMPLE",
            [],
            require_json=False,
        )

    assert any(event["type"] == "secret_blocked" for event in events)
    assert [event["type"] for event in events if event["type"].startswith("model_request_")] == [
        "model_request_failed"
    ]


def test_task_budget_blocks_new_model_call_before_transport(monkeypatch):
    events = []

    class UnexpectedClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("transport must not start after budget exhaustion")

    monkeypatch.setattr(llm_client, "cookie_manager", _FakeCookieManager())
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "WebClaudeClient", UnexpectedClient)
    monkeypatch.setattr(
        llm_client.thread_local,
        "event_sink",
        events.append,
        raising=False,
    )
    llm_client.thread_local.agent_role = "worker"
    llm_client.thread_local.task_id = "budget-task"
    register_task_budget("budget-task", BudgetLimits(max_model_calls=1))
    try:
        budget = llm_client.get_task_budget("budget-task")
        assert budget is not None
        budget.reserve_model_call("first")
        with pytest.raises(BudgetExceededError, match="model_calls"):
            llm_client.call_agent(
                "system",
                "second request",
                [],
                require_json=False,
            )
    finally:
        remove_task_budget("budget-task")
        del llm_client.thread_local.task_id

    assert any(event["type"] == "budget_exceeded" for event in events)


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
    assert (
        calls[0][1]["human_message_uuid"]
        != calls[1][1]["human_message_uuid"]
    )


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


def test_cookie_manager_prefers_least_busy_account(tmp_path, monkeypatch):
    manager = CookieManager(str(tmp_path))
    first = {"source": "a.txt", "org_id": "a", "cookie_string": "sessionKey=a"}
    second = {"source": "b.txt", "org_id": "b", "cookie_string": "sessionKey=b"}
    manager.cookies_pool = [first, second]
    monkeypatch.setattr(llm_client.random, "choice", lambda items: items[0])

    manager.begin_request("a.txt")
    try:
        assert manager.get_next_cookie() == second
    finally:
        manager.end_request("a.txt")

    assert manager.get_next_cookie() == first


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


def test_worker_429_replays_identical_request_on_new_account(monkeypatch):
    calls = []
    events = []

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {"source": "a.txt", "org_id": "a", "cookie_string": "sessionKey=a"},
                {"source": "b.txt", "org_id": "b", "cookie_string": "sessionKey=b"},
            ]
            self.cooling = set()
            self.active = {}

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

        def begin_request(self, source):
            self.active[source] = self.active.get(source, 0) + 1

        def end_request(self, source):
            self.active[source] -= 1

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string

        def send_message(self, prompt, **kwargs):
            calls.append((self.cookie_string, prompt, kwargs))
            if self.cookie_string.endswith("=a"):
                raise RateLimitError("429", retry_after=17)
            return """
<patch>
<<<< SEARCH
x = 1
====
x = 2
>>>> REPLACE
</patch>
{"_action":"submit_patch","task_status":"completed","worker_feedback":"done"}
"""

    manager = Manager()
    monkeypatch.setattr(llm_client, "cookie_manager", manager)
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    local = llm_client.thread_local
    local.agent_role = "worker"
    local.account_mode = "sticky"
    local.agent_instance_id = "worker-logical-a"
    local.manager_id = "manager-a"
    local.workstream_id = "stream-a"
    local.work_item_id = "item-a"
    local.task_id = "task-a"
    local.event_sink = events.append
    local.is_continuation = False
    for attr in ("supervisor_cookie", "director_cookie", "manager_cookie"):
        setattr(local, attr, None)

    result = llm_client.call_agent(
        "Worker instructions",
        "Solve logical task A",
        WORKER_TOOLS,
    )

    assert result.tool_name == "submit_patch"
    assert [cookie for cookie, _, _ in calls] == ["sessionKey=a", "sessionKey=b"]
    assert calls[0][1] == calls[1][1]
    for key in (
        "chat_uuid",
        "mcp_server_uuid",
        "human_message_uuid",
        "assistant_message_uuid",
    ):
        assert calls[0][2][key] == calls[1][2][key]
    assert manager.cooling == {"a.txt"}
    assert manager.active == {"a.txt": 0, "b.txt": 0}

    started = [event for event in events if event["type"] == "model_request_started"]
    switched = [event for event in events if event["type"] == "account_switch"]
    assert len(started) == 2
    assert started[0]["agent_instance_id"] == started[1]["agent_instance_id"] == "worker-logical-a"
    assert started[0]["logical_request_id"] == started[1]["logical_request_id"]
    assert started[0]["call_id"] == started[1]["call_id"]
    assert started[0]["call_id"] == started[0]["logical_request_id"]
    assert started[0]["request_fingerprint"] == started[1]["request_fingerprint"]
    assert len(switched) == 1
    assert {
        key: switched[0][key]
        for key in (
            "type",
            "role",
            "agent_instance_id",
            "manager_id",
            "workstream_id",
            "work_item_id",
            "task_id",
            "from_account",
            "to_account",
            "reason",
            "attempt",
            "logical_request_id",
            "request_fingerprint",
            "replayed",
        )
    } == {
        "type": "account_switch",
        "role": "worker",
        "agent_instance_id": "worker-logical-a",
        "manager_id": "manager-a",
        "workstream_id": "stream-a",
        "work_item_id": "item-a",
        "task_id": "task-a",
        "from_account": "a.txt",
        "to_account": "b.txt",
        "reason": "rate_limit",
        "attempt": 2,
        "logical_request_id": started[0]["logical_request_id"],
        "request_fingerprint": started[0]["request_fingerprint"],
        "replayed": True,
    }


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


def test_mark_invalid_quarantines_metadata_without_deleting_credential(tmp_path):
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
    assert cookie_path.read_text(encoding="utf-8") == "sessionKey=fake"
    assert manager.quarantined_accounts[source]["enabled"] is False
    assert manager.quarantined_accounts[source]["state"] == "quarantined"
    quarantine_file = tmp_path / manager._QUARANTINE_METADATA_FILE
    assert quarantine_file.is_file()
    assert "sessionKey=fake" not in quarantine_file.read_text(encoding="utf-8")

    reloaded = CookieManager(str(tmp_path))
    assert reloaded.cookies_pool == []
    assert source in reloaded.quarantined_accounts

    assert reloaded.delete_credential(source) is True
    assert not cookie_path.exists()
    assert source not in reloaded.quarantined_accounts


def test_generic_auth_failure_quarantines_and_retains_credential(
    tmp_path,
    monkeypatch,
):
    source = "waf-blocked.txt"
    manager = CookieManager(str(tmp_path))
    cookie_path = tmp_path / source
    cookie_path.write_text("sessionKey=operator-owned", encoding="utf-8")
    manager.cookies_pool = [
        {
            "source": source,
            "org_id": "org",
            "cookie_string": "sessionKey=operator-owned",
        }
    ]
    manager.total_loaded = 1

    class AuthFailingClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, **kwargs):
            raise PermissionError("generic authentication/permission/WAF failure")

    monkeypatch.setattr(llm_client, "cookie_manager", manager)
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "WebClaudeClient", AuthFailingClient)
    monkeypatch.setattr(llm_client.config, "LLM_MAX_RETRIES", 1)
    monkeypatch.setattr(
        llm_client.config,
        "PROTOCOL_ATTEMPTS_PER_COOKIE",
        1,
    )
    llm_client.thread_local.agent_role = "worker"
    for attr in ("supervisor_cookie", "director_cookie", "manager_cookie"):
        setattr(llm_client.thread_local, attr, None)

    with pytest.raises(LLMError, match="LLM thất bại"):
        llm_client.call_agent("system", "message", [], require_json=False)

    assert cookie_path.read_text(encoding="utf-8") == "sessionKey=operator-owned"
    assert manager.cookies_pool == []
    assert manager.quarantined_accounts[source]["enabled"] is False


def test_explicit_credential_deletion_rejects_path_escape(tmp_path):
    manager = CookieManager(str(tmp_path))

    with pytest.raises(ValueError, match="file name"):
        manager.delete_credential("../outside.txt")


def test_director_and_manager_use_sticky_cookies(monkeypatch):
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
            return """```json
{"_action":"submit_workstream_plan","summary":"ok",
"requested_manager_count":1,"workstreams":[{"id":"core","title":"Core",
"goal":"g","acceptance_criteria":["ok"],"dependencies":[],
"write_scopes":["a.py"]}]}
```"""

    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    llm_client.thread_local.agent_role = "director"
    llm_client.thread_local.account_mode = "sticky"
    llm_client.thread_local.director_cookie = None
    llm_client.thread_local.is_continuation = False

    first = llm_client.call_agent("Director", "Plan", [{"name": "submit_workstream_plan", "input_schema": {"type": "object", "properties": {}}}])
    second = llm_client.call_agent("Director", "Plan again", [{"name": "submit_workstream_plan", "input_schema": {"type": "object", "properties": {}}}])

    assert first.tool_name == "submit_workstream_plan"
    assert second.tool_name == "submit_workstream_plan"
    assert used[0] == used[1]
    assert llm_client.thread_local.director_cookie["source"] == "a.txt"


def test_tester_role_uses_reviewer_model_selectors(monkeypatch):
    seen = {}

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {"source": "a.txt", "org_id": "a", "cookie_string": "sessionKey=a"},
            ]

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            return self.cookies_pool[0]

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, **kwargs):
            seen["model"] = kwargs.get("model_name")
            seen["effort"] = kwargs.get("effort")
            return """```json
{"_action":"review_patch","verdict":"approved",
"reviewer_feedback":"ok","next_instructions":""}
```"""

    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    llm_client.thread_local.agent_role = "tester"
    llm_client.thread_local.account_mode = "sticky"
    llm_client.thread_local.reviewer_model = "claude-opus-tester"
    llm_client.thread_local.reviewer_effort = "high"
    llm_client.thread_local.model = "claude-sonnet-worker"
    llm_client.thread_local.effort = "max"

    result = llm_client.call_agent(
        "Tester",
        "Review",
        [{"name": "review_patch", "input_schema": {"type": "object", "properties": {}}}],
    )

    assert result.tool_name == "review_patch"
    assert seen["model"] == "claude-opus-tester"
    assert seen["effort"] == "high"


def test_final_failure_emits_one_terminal_event(monkeypatch):
    events = []

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {
                    "source": "a.txt",
                    "org_id": "a",
                    "cookie_string": "sessionKey=a",
                }
            ]

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            excluded = exclude_sources or set()
            return (
                None
                if "a.txt" in excluded
                else self.cookies_pool[0]
            )

    class FailingClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, **kwargs):
            raise llm_client.requests.ConnectionError("offline")

    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "WebClaudeClient", FailingClient)
    monkeypatch.setattr(llm_client.config, "LLM_MAX_RETRIES", 1)
    monkeypatch.setattr(
        llm_client.config, "PROTOCOL_ATTEMPTS_PER_COOKIE", 1
    )
    monkeypatch.setattr(
        llm_client.thread_local, "event_sink", events.append, raising=False
    )
    llm_client.thread_local.agent_role = "worker"
    llm_client.thread_local.is_continuation = False

    with pytest.raises(LLMError, match="LLM thất bại"):
        llm_client.call_agent("system", "message", [], require_json=False)

    assert len(
        [event for event in events if event["type"] == "model_request_failed"]
    ) == 1
    assert not [
        event
        for event in events
        if event["type"]
        in {"model_request_aborted", "model_request_completed"}
    ]


def test_cancel_emits_one_aborted_terminal_event(monkeypatch):
    events = []

    class Manager:
        cookies_pool = [
            {
                "source": "a.txt",
                "org_id": "a",
                "cookie_string": "sessionKey=a",
            }
        ]

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            return self.cookies_pool[0]

    class AbortingClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, **kwargs):
            raise llm_client.ModelRequestAborted("cancelled")

    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "WebClaudeClient", AbortingClient)
    monkeypatch.setattr(
        llm_client.thread_local, "event_sink", events.append, raising=False
    )
    llm_client.thread_local.agent_role = "worker"
    llm_client.thread_local.is_continuation = False

    with pytest.raises(llm_client.ModelRequestAborted, match="cancelled"):
        llm_client.call_agent("system", "message", [], require_json=False)

    assert len(
        [event for event in events if event["type"] == "model_request_aborted"]
    ) == 1
    assert not [
        event
        for event in events
        if event["type"]
        in {"model_request_failed", "model_request_completed"}
    ]


def test_aborted_web_stream_never_exposes_buffered_partial_chunks(monkeypatch):
    events = []
    payloads = [
        {
            "type": "content_block_delta",
            "delta": {"type": "text_delta", "text": "partial"},
        },
        {
            "type": "content_block_delta",
            "delta": {"type": "text_delta", "text": "ignored"},
        },
    ]

    class Response:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            return None

        def iter_lines(self):
            return iter(
                [
                    f"data: {json.dumps(payload)}".encode()
                    for payload in payloads
                ]
            )

    class Session:
        headers = {}

        def post(self, url, json, stream, timeout):
            return Response()

    checks = iter([False, False, True])
    monkeypatch.setattr(
        llm_client.thread_local, "event_sink", events.append, raising=False
    )
    llm_client.thread_local.agent_role = "worker"
    client = WebClaudeClient("org", "sessionKey=fake")
    client.session = Session()

    with pytest.raises(llm_client.ModelRequestAborted):
        client.send_message(
            "message",
            enable_builtin_tools=False,
            should_abort=lambda: next(checks, True),
        )

    assert client.last_stream_chunks == ()
    assert events == []


def test_rate_limited_partial_attempt_is_not_committed_twice(monkeypatch):
    events = []

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {
                    "source": "a.txt",
                    "org_id": "a",
                    "cookie_string": "sessionKey=a",
                },
                {
                    "source": "b.txt",
                    "org_id": "b",
                    "cookie_string": "sessionKey=b",
                },
            ]
            self.cooling = set()

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

    class FakeClient:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string
            self.last_stream_chunks = ()

        def send_message(self, prompt, **kwargs):
            if self.cookie_string.endswith("=a"):
                self.last_stream_chunks = (
                    llm_client.ProviderChunk(
                        index=0,
                        text="partial-from-aborted-attempt",
                    ),
                )
                raise RateLimitError("429", retry_after=1)
            self.last_stream_chunks = (
                llm_client.ProviderChunk(index=0, text="final"),
            )
            return "final"

    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "WebClaudeClient", FakeClient)
    monkeypatch.setattr(
        llm_client.thread_local, "event_sink", events.append, raising=False
    )
    llm_client.thread_local.agent_role = "worker"
    llm_client.thread_local.is_continuation = False
    for attr in ("supervisor_cookie", "director_cookie", "manager_cookie"):
        setattr(llm_client.thread_local, attr, None)

    result = llm_client.call_agent(
        "system",
        "message",
        [],
        require_json=False,
    )

    assert result.raw_response["content"] == "final"
    committed_text = [
        event["text"]
        for event in events
        if event["type"] == "token"
    ]
    assert committed_text == ["final"]
    assert len(
        [
            event
            for event in events
            if event["type"] == "model_request_completed"
        ]
    ) == 1


def test_credential_fingerprint_is_stable_across_file_renames(monkeypatch):
    monkeypatch.setenv("ORCH_ACCOUNT_FINGERPRINT_SALT", "machine-local-test-salt")
    first = {
        "source": "before.txt",
        "org_id": "org",
        "cookie_string": "sessionKey=credential-content",
    }
    renamed = dict(first, source="after.json")
    different = dict(first, cookie_string="sessionKey=other-content")

    first_id = llm_client._lease_account_id(first)

    assert first_id == llm_client._lease_account_id(renamed)
    assert first_id != llm_client._lease_account_id(different)
    assert "credential-content" not in first_id
    assert "before" not in first_id


def test_long_transport_renews_account_lease(monkeypatch):
    class Manager:
        cookies_pool = [
            {
                "source": "account.txt",
                "org_id": "org",
                "cookie_string": "sessionKey=lease-success",
            }
        ]

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            return self.cookies_pool[0]

    class Store:
        def __init__(self):
            self.renewals = 0
            self.released = []

        def acquire(self, **kwargs):
            now = kwargs["now"]
            return AccountLease(
                "lease-a",
                kwargs["candidates"][0].account_id,
                "legacy_web",
                kwargs["owner_id"],
                now,
                now + kwargs["lease_ttl_seconds"],
            )

        def renew(self, lease, *, lease_ttl_seconds, now):
            self.renewals += 1
            return AccountLease(
                lease.lease_id,
                lease.account_id,
                lease.provider,
                lease.owner_id,
                lease.acquired_at,
                now + lease_ttl_seconds,
            )

        def release(self, lease, *, outcome, now):
            self.released.append(outcome)

        def record_health_transition(self, transition):
            pass

    class SlowClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, *, should_abort, **kwargs):
            deadline = time.monotonic() + 0.22
            while time.monotonic() < deadline:
                assert not should_abort()
                time.sleep(0.01)
            return "completed"

    store = Store()
    monkeypatch.setenv("ORCH_ACCOUNT_LEASE_TTL_SECONDS", "0.15")
    monkeypatch.setenv("ORCH_ACCOUNT_FINGERPRINT_SALT", "machine-local-test-salt")
    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "account_lease_store", store)
    monkeypatch.setattr(llm_client, "WebClaudeClient", SlowClient)
    llm_client.thread_local.agent_role = "worker"

    result = llm_client.call_agent(
        "system",
        "long request",
        [],
        require_json=False,
    )

    assert result.raw_response["content"] == "completed"
    assert store.renewals >= 2
    assert store.released == ["completed"]


def test_lost_account_lease_aborts_long_transport(monkeypatch):
    class Manager:
        cookies_pool = [
            {
                "source": "account.txt",
                "org_id": "org",
                "cookie_string": "sessionKey=lease-loss",
            }
        ]

        def has_cookies(self):
            return True

        def get_next_cookie(self, exclude_sources=None):
            return self.cookies_pool[0]

    class Store:
        def acquire(self, **kwargs):
            now = kwargs["now"]
            return AccountLease(
                "lease-lost",
                kwargs["candidates"][0].account_id,
                "legacy_web",
                kwargs["owner_id"],
                now,
                now + kwargs["lease_ttl_seconds"],
            )

        def renew(self, lease, *, lease_ttl_seconds, now):
            return None

        def release(self, lease, *, outcome, now):
            self.outcome = outcome

        def record_health_transition(self, transition):
            pass

    class BlockingClient:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, *, should_abort, **kwargs):
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                if should_abort():
                    raise llm_client.ModelRequestAborted("transport aborted")
                time.sleep(0.01)
            raise AssertionError("lost lease did not abort transport")

    store = Store()
    monkeypatch.setenv("ORCH_ACCOUNT_LEASE_TTL_SECONDS", "0.15")
    monkeypatch.setenv("ORCH_ACCOUNT_FINGERPRINT_SALT", "machine-local-test-salt")
    monkeypatch.setattr(llm_client, "cookie_manager", Manager())
    monkeypatch.setattr(llm_client, "account_lease_store", store)
    monkeypatch.setattr(llm_client, "WebClaudeClient", BlockingClient)
    llm_client.thread_local.agent_role = "worker"

    with pytest.raises(llm_client.AccountLeaseLostError, match="lease was lost"):
        llm_client.call_agent(
            "system",
            "long request",
            [],
            require_json=False,
        )

    assert store.outcome == "lease_lost"


def test_429_replays_portable_context_and_restart_restores_it(
    monkeypatch, tmp_path
):
    from orchestrator import agent_transcript

    monkeypatch.setenv("ORCH_AGENT_LOG_DIR", str(tmp_path))
    agent_transcript.record_message(
        task_id="task-context",
        agent_id="worker-context",
        role="user",
        content="prior question",
        logical_call_id="prior",
    )
    agent_transcript.record_message(
        task_id="task-context",
        agent_id="worker-context",
        role="assistant",
        content="prior answer",
        logical_call_id="prior",
    )
    calls = []

    class Manager:
        def __init__(self):
            self.cookies_pool = [
                {
                    "source": "a.txt",
                    "org_id": "a",
                    "cookie_string": "sessionKey=context-a-secret",
                },
                {
                    "source": "b.txt",
                    "org_id": "b",
                    "cookie_string": "sessionKey=context-b-secret",
                },
            ]
            self.cooling = set()

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

    class ContextClient:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string

        def send_message(self, prompt, **kwargs):
            calls.append((self.cookie_string, prompt, kwargs))
            if self.cookie_string.endswith("context-a-secret"):
                raise RateLimitError("429", retry_after=1)
            return "answer after switch"

    manager = Manager()
    monkeypatch.setattr(llm_client, "cookie_manager", manager)
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "WebClaudeClient", ContextClient)
    local = llm_client.thread_local
    local.agent_role = "worker"
    local.task_id = "task-context"
    local.agent_instance_id = "worker-context"
    for attr in ("supervisor_cookie", "director_cookie", "manager_cookie"):
        setattr(local, attr, None)

    llm_client.call_agent(
        "system",
        "current question",
        [],
        require_json=False,
    )

    assert calls[0][1] == calls[1][1]
    assert "prior question" in calls[0][1]
    assert "prior answer" in calls[0][1]
    assert calls[0][2]["chat_uuid"] == calls[1][2]["chat_uuid"]
    assert calls[0][2]["is_new_chat"] is calls[1][2]["is_new_chat"] is True

    manager.cooling.add("a.txt")
    calls.clear()
    llm_client.call_agent(
        "system",
        "question after restart",
        [],
        require_json=False,
    )

    assert "answer after switch" in calls[0][1]
    assert "current question" in calls[0][1]
    persisted = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in tmp_path.rglob("*")
        if path.is_file()
    )
    assert "context-a-secret" not in persisted
    assert "context-b-secret" not in persisted