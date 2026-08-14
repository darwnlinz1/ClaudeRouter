from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from orchestrator import llm_client, llm_request_log
from orchestrator.state_repository import RetentionPolicy, StateRepository


@pytest.fixture(autouse=True)
def isolate_request_diagnostics(monkeypatch):
    llm_request_log.configure_repository(None)
    monkeypatch.setattr(llm_client, "account_lease_store", None)
    monkeypatch.setattr(llm_client, "_agent_account_bindings", {})
    monkeypatch.setenv("ORCH_ACCOUNT_FINGERPRINT_SALT", "request-log-test-salt")
    local = llm_client.thread_local
    local.agent_role = "worker"
    local.agent_instance_id = "worker-request-log"
    local.task_id = "task-request-log"
    local.session_id = "session-request-log"
    local.workstream_id = "stream-request-log"
    local.work_item_id = "item-request-log"
    local.is_continuation = False
    for name in ("supervisor_cookie", "director_cookie", "manager_cookie"):
        setattr(local, name, None)
    yield
    llm_request_log.configure_repository(None)


def test_request_attempt_round_trip_redacts_without_truncating(tmp_path):
    database = tmp_path / "attempts.sqlite3"
    secret_cookie = "sessionKey=secret-cookie-value"
    raw_org = "11111111-2222-3333-4444-555555555555"
    long_prompt = "x" * 70_000 + f" {secret_cookie}"
    with StateRepository(database) as repository:
        assert llm_request_log.configure_repository(repository)
        attempt = llm_request_log.start_attempt(
            {
                "attempt_id": "llmreq-a:r1:a1",
                "task_id": "task-a",
                "logical_request_id": "llmreq-a",
                "request_revision": 1,
                "provider_attempt": 1,
                "provider": "legacy_web",
                "account_ref": "acct-aaaa-bbbb-cccc",
                "org_ref": "org-aaaa-bbbb-cccc",
                "logical_request": {"prompt": long_prompt},
                "tool_schema": [{"name": "submit", "cookie": secret_cookie}],
            },
            sensitive_values=("credential-a.txt", raw_org, secret_cookie),
        )
        attempt.record_wire(
            route="POST /organizations/{org_ref}/completion",
            body={
                "prompt": long_prompt,
                "org_id": raw_org,
                "callback": "https://example.test/file?X-Amz-Signature=signed-secret",
            },
            org_id=raw_org,
        )
        attempt.record_response(
            status=400,
            headers={
                "Content-Type": "application/json",
                "Set-Cookie": secret_cookie,
                "Authorization": "Bearer secret",
                "X-Request-ID": "request-1",
            },
            body={"error": f"{raw_org} {secret_cookie}"},
        )
        attempt.terminal(
            status="failed",
            error_stage="response",
            error_classification="malformed_input",
            error=ValueError(f"bad request from credential-a.txt {secret_cookie}"),
            retryable=False,
        )

        saved = repository.list_llm_request_attempts("task-a")[0]

    persisted = database.read_bytes()
    for secret in (
        b"secret-cookie-value",
        raw_org.encode(),
        b"credential-a.txt",
        b"signed-secret",
    ):
        assert secret not in persisted
    assert len(saved["logical_request"]["prompt"]) > 70_000
    assert "TRUNCATED" not in saved["logical_request"]["prompt"]
    assert saved["tool_schema"][0]["cookie"] == "[REDACTED]"
    assert saved["wire_body"]["org_id"] == "[REDACTED]"
    assert saved["wire_body"]["callback"] == "[REDACTED_SIGNED_URL]"
    assert saved["response_headers"] == {
        "content-type": "application/json",
        "x-request-id": "request-1",
    }
    assert saved["error_classification"] == "malformed_input"
    assert saved["retryable"] is False


def test_request_attempt_retention_and_schema_error_filter(tmp_path):
    now = datetime(2026, 8, 13, tzinfo=timezone.utc)
    with StateRepository(tmp_path / "retention.sqlite3") as repository:
        for suffix, classification in (
            ("old", "transport_error"),
            ("schema", "protocol_error"),
        ):
            repository.create_llm_request_attempt(
                {
                    "attempt_id": f"attempt-{suffix}",
                    "task_id": "task-a",
                    "logical_request_id": f"logical-{suffix}",
                    "request_revision": 1,
                    "provider_attempt": 1,
                    "provider": "legacy_web",
                    "status": "failed",
                    "error_stage": (
                        "parser" if classification == "protocol_error" else "transport"
                    ),
                    "error_classification": classification,
                    "created_at": now - timedelta(days=45 if suffix == "old" else 1),
                    "updated_at": now - timedelta(days=45 if suffix == "old" else 1),
                }
            )
        assert [
            row["attempt_id"]
            for row in repository.list_llm_request_attempts(
                "task-a",
                schema_errors_only=True,
            )
        ] == ["attempt-schema"]
        deleted = repository.compact_llm_request_attempts(
            RetentionPolicy(llm_request_days=30),
            now=now,
        )
        assert deleted == 1
        assert [
            row["attempt_id"]
            for row in repository.list_llm_request_attempts("task-a")
        ] == ["attempt-schema"]


def test_web_transport_records_actual_wire_and_allowlisted_response(
    tmp_path,
):
    class Response:
        status_code = 200
        headers = {
            "Content-Type": "text/event-stream",
            "Set-Cookie": "sessionKey=response-secret",
        }

        def iter_lines(self):
            return iter(
                [
                    b'data: {"type":"content_block_delta",'
                    b'"delta":{"type":"text_delta","text":"done"}}',
                    b'data: {"type":"message_stop"}',
                ]
            )

        def raise_for_status(self):
            return None

        def close(self):
            pass

    class Session:
        headers = {}

        def post(self, url, json, stream, timeout):
            return Response()

    with StateRepository(tmp_path / "wire.sqlite3") as repository:
        llm_request_log.configure_repository(repository)
        attempt = llm_request_log.start_attempt(
            {
                "attempt_id": "wire-attempt",
                "task_id": "task-wire",
                "logical_request_id": "logical-wire",
                "request_revision": 1,
                "provider_attempt": 1,
                "provider": "legacy_web",
            }
        )
        client = llm_client.WebClaudeClient(
            "raw-org-wire-secret",
            "sessionKey=request-secret",
        )
        client.session = Session()

        assert client.send_message(
            "wire prompt",
            enable_builtin_tools=False,
            emit_chunks=False,
            request_diagnostics=attempt,
        ) == "done"
        saved = repository.list_llm_request_attempts("task-wire")[0]

    assert saved["wire_body"]["prompt"] == "wire prompt"
    assert saved["wire_body"]["model"] == "claude-sonnet-5"
    assert len(saved["wire_fingerprint"]) == 64
    assert saved["response_status"] == 200
    assert saved["response_headers"] == {"content-type": "text/event-stream"}
    assert saved["response_body"][-1] == 'data: {"type":"message_stop"}'
    persisted = (tmp_path / "wire.sqlite3").read_bytes()
    assert b"raw-org-wire-secret" not in persisted
    assert b"request-secret" not in persisted
    assert b"response-secret" not in persisted


def test_web_400_classification_distinguishes_probe_and_malformed_input():
    client = llm_client.WebClaudeClient("org", "sessionKey=fake")

    class Response:
        status_code = 400
        headers = {}

        def __init__(self, detail):
            self.detail = detail

        def json(self):
            return {"error": {"message": self.detail}}

    with pytest.raises(llm_client.AmbiguousConversationError):
        client._check_response(Response("Conversation could not be created"))
    with pytest.raises(llm_client.PayloadRejectedError) as exc_info:
        client._check_response(Response("tools: Array should have at least 1 item"))
    assert exc_info.value.classification == "malformed_input"


class _AccountManager:
    def __init__(self, count: int = 3):
        self.cookies_pool = [
            {
                "source": f"{letter}.txt",
                "org_id": f"org-{letter}",
                "cookie_string": f"sessionKey={letter}-secret",
            }
            for letter in ("a", "b", "c")[:count]
        ]

    def has_cookies(self):
        return bool(self.cookies_pool)

    def get_next_cookie(self, exclude_sources=None):
        excluded = exclude_sources or set()
        return next(
            (item for item in self.cookies_pool if item["source"] not in excluded),
            None,
        )


def test_ambiguous_conversation_gets_one_same_request_cross_account_probe(
    monkeypatch,
    tmp_path,
):
    calls = []

    class Client:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string
            self.last_stream_chunks = ()

        def send_message(self, prompt, **kwargs):
            calls.append((self.cookie_string, prompt, kwargs))
            if self.cookie_string == "sessionKey=a-secret":
                raise llm_client.AmbiguousConversationError(
                    "Conversation could not be created"
                )
            return "ok"

    with StateRepository(tmp_path / "probe.sqlite3") as repository:
        llm_request_log.configure_repository(repository)
        monkeypatch.setattr(llm_client, "cookie_manager", _AccountManager())
        monkeypatch.setattr(llm_client, "WebClaudeClient", Client)

        result = llm_client.call_agent(
            "system",
            "same logical request",
            [],
            require_json=False,
        )
        rows = list(reversed(repository.list_llm_request_attempts("task-request-log")))

    assert result.raw_response["content"] == "ok"
    assert [call[0] for call in calls] == [
        "sessionKey=a-secret",
        "sessionKey=b-secret",
    ]
    assert calls[0][1] == calls[1][1]
    for key in ("chat_uuid", "human_message_uuid", "assistant_message_uuid"):
        assert calls[0][2][key] != calls[1][2][key]
    assert len(rows) == 2
    assert len({row["logical_request_id"] for row in rows}) == 1
    assert len({row["request_revision"] for row in rows}) == 1
    assert len({row["request_fingerprint"] for row in rows}) == 1
    assert {row["agent_role"] for row in rows} == {"worker"}
    assert rows[0]["error_classification"] == "account_specific"
    assert rows[1]["status"] == "completed"
    assert rows[1]["probe_of_attempt_id"] == rows[0]["attempt_id"]
    assert rows[0]["account_ref"] != rows[1]["account_ref"]


def test_same_ambiguous_400_stops_after_exactly_one_probe(monkeypatch, tmp_path):
    calls = []

    class Client:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string
            self.last_stream_chunks = ()

        def send_message(self, prompt, **kwargs):
            calls.append(self.cookie_string)
            raise llm_client.AmbiguousConversationError(
                "Conversation could not be created"
            )

    with StateRepository(tmp_path / "same-400.sqlite3") as repository:
        llm_request_log.configure_repository(repository)
        monkeypatch.setattr(llm_client, "cookie_manager", _AccountManager(count=3))
        monkeypatch.setattr(llm_client, "WebClaudeClient", Client)

        with pytest.raises(
            llm_client.PayloadRejectedError,
            match="Conversation could not be created",
        ) as exc_info:
            llm_client.call_agent("system", "same request", [], require_json=False)
        rows = repository.list_llm_request_attempts("task-request-log")

    assert exc_info.value.classification == "provider_conversation_input"
    assert calls == ["sessionKey=a-secret", "sessionKey=b-secret"]
    assert len(rows) == 2
    assert {row["error_classification"] for row in rows} == {
        "provider_conversation_input"
    }
    assert all(row["retryable"] is False for row in rows)


def test_explicit_malformed_400_is_not_replayed(monkeypatch, tmp_path):
    calls = []

    class Client:
        def __init__(self, org_id, cookie_string):
            self.cookie_string = cookie_string
            self.last_stream_chunks = ()

        def send_message(self, prompt, **kwargs):
            calls.append(self.cookie_string)
            raise llm_client.PayloadRejectedError(
                "tools: Array should have at least 1 item",
                classification="malformed_input",
            )

    with StateRepository(tmp_path / "malformed.sqlite3") as repository:
        llm_request_log.configure_repository(repository)
        monkeypatch.setattr(llm_client, "cookie_manager", _AccountManager())
        monkeypatch.setattr(llm_client, "WebClaudeClient", Client)

        with pytest.raises(llm_client.PayloadRejectedError) as exc_info:
            llm_client.call_agent("system", "bad request", [], require_json=False)
        rows = repository.list_llm_request_attempts("task-request-log")

    assert exc_info.value.classification == "malformed_input"
    assert calls == ["sessionKey=a-secret"]
    assert len(rows) == 1
    assert rows[0]["error_classification"] == "malformed_input"
    assert rows[0]["retryable"] is False


def test_transport_attempt_log_carries_remediation_metadata(monkeypatch, tmp_path):
    class Client:
        def __init__(self, org_id, cookie_string):
            pass

        def send_message(self, prompt, **kwargs):
            raise llm_client.requests.ConnectionError("offline")

    with StateRepository(tmp_path / "remediation-log.sqlite3") as repository:
        llm_request_log.configure_repository(repository)
        monkeypatch.setattr(llm_client, "cookie_manager", _AccountManager(count=1))
        monkeypatch.setattr(llm_client, "WebClaudeClient", Client)

        with pytest.raises(llm_client.TaskAccountPoolExhaustedError):
            llm_client.call_agent("system", "same request", [], require_json=False)
        row = repository.list_llm_request_attempts("task-request-log")[0]

    remediation = row["parser_result"]["remediation"]
    assert remediation["failure_category"] == "transport_lease"
    assert remediation["failure_signature"]
    assert remediation["remediation_strategy"] == "replace_transport_account"
    assert remediation["remediation_actor"] == "provider_runtime"
