import hashlib
import json

from orchestrator import agent_transcript


def test_provider_neutral_transcript_is_secret_free(monkeypatch, tmp_path):
    monkeypatch.setenv("ORCH_AGENT_LOG_DIR", str(tmp_path))
    session = "sessionKey=super-secret-cookie"
    cookie_header = "Cookie: cf_clearance=another-secret-cookie"

    path = agent_transcript.record_message(
        task_id="task-a",
        agent_id="worker-a",
        role="assistant",
        content=f"{session}\n{cookie_header}",
        provider="legacy_web",
        logical_call_id="llmreq-a",
        attempt_id="llmreq-a:r1:a1",
        tool_calls=[
            {
                "id": "tool-1",
                "name": "example",
                "arguments": {"cookie_string": session, "value": "safe"},
            }
        ],
        metadata={"cookie_string": session, "request_revision": 1},
        at="2026-08-12T15:00:00+00:00",
    )

    assert path is not None
    raw = path.read_text(encoding="utf-8")
    assert "super-secret-cookie" not in raw
    assert "another-secret-cookie" not in raw

    entry = json.loads(raw)
    assert entry["schema_version"] == 1
    assert entry["role"] == "assistant"
    assert entry["provider"] == "legacy_web"
    assert entry["logical_call_id"] == "llmreq-a"
    assert entry["attempt_id"] == "llmreq-a:r1:a1"
    assert entry["timestamp"] == "2026-08-12T15:00:00+00:00"
    assert entry["tool_calls"][0]["arguments"]["cookie_string"] == "[REDACTED]"
    assert entry["metadata"]["cookie_string"] == "[REDACTED]"
    assert entry["content_sha256"] == hashlib.sha256(
        entry["content"].encode("utf-8")
    ).hexdigest()


def test_transcript_loader_restores_only_completed_secret_free_turns(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("ORCH_AGENT_LOG_DIR", str(tmp_path))
    common = {"task_id": "task-a", "agent_id": "worker-a"}
    agent_transcript.record_message(
        **common,
        role="user",
        content="persisted question",
        logical_call_id="complete",
    )
    agent_transcript.record_message(
        **common,
        role="assistant",
        content="persisted answer",
        logical_call_id="complete",
    )
    agent_transcript.record_message(
        **common,
        role="user",
        content="failed attempt partial",
        logical_call_id="orphan",
    )

    restored = agent_transcript.load_messages(**common)

    assert [(message.role, message.content) for message in restored] == [
        ("user", "persisted question"),
        ("assistant", "persisted answer"),
    ]


def test_every_transcript_path_is_registered_with_retention_metadata(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("ORCH_AGENT_LOG_DIR", str(tmp_path))

    class FakeRepository:
        def __init__(self):
            self.records = []

        def record_log_metadata(self, task_id, path, **metadata):
            self.records.append((task_id, path, metadata))
            return str(len(self.records))

    repository = FakeRepository()
    callback = repository.record_log_metadata
    common = {"task_id": "task-a", "agent_id": "worker-a"}
    agent_transcript.record_message(
        **common,
        role="assistant",
        content="completed answer",
        logical_call_id="call-a",
        attempt_id="attempt-a",
        pinned=True,
        registration_callback=callback,
    )
    agent_transcript.record_thinking(
        **common,
        role="worker",
        text="safe thinking",
        registration_callback=callback,
    )
    response = agent_transcript.record_response(
        **common,
        role="worker",
        account="local",
        attempt=1,
        raw_response='{"action":"done"}',
        parsed_ok=True,
        logical_call_id="call-a",
        attempt_id="attempt-a",
        pinned=True,
        registration_callback=callback,
    )

    assert response is not None
    disk_paths = {
        str(path.resolve())
        for path in (tmp_path / "task-a" / "worker-a").rglob("*")
        if path.is_file()
    }
    registered_paths = {record[1] for record in repository.records}
    assert disk_paths == registered_paths
    assert all(record[0] == "task-a" for record in repository.records)
    assert all(record[2]["content_sha256"] for record in repository.records)
    assert all(record[2]["size_bytes"] > 0 for record in repository.records)
    assert any(
        record[2]["terminal_evidence"]
        and record[2]["metadata"]["pinned"] is True
        for record in repository.records
    )
    thinking = next(
        record
        for record in repository.records
        if record[2]["metadata"]["kind"] == "provider_thinking"
    )
    assert thinking[2]["terminal_evidence"] is False
