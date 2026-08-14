from orchestrator import agent_transcript
from orchestrator.redaction import redact_event, redact_text


def test_redacts_nested_sensitive_keys_and_inline_credentials():
    event = {
        "prompt": "Authorization: Bearer abcdefghijklmnop",
        "metadata": {
            "password": "hunter2",
            "safe": "sessionKey=secret-session-value",
        },
    }

    redacted = redact_event(event)

    assert "abcdefghijklmnop" not in redacted["prompt"]
    assert redacted["metadata"]["password"] == "[REDACTED]"
    assert "secret-session-value" not in redacted["metadata"]["safe"]


def test_redaction_truncates_large_text():
    assert "TRUNCATED" in redact_text("x" * 100, max_chars=20)


def test_agent_transcript_redacts_raw_model_output(monkeypatch, tmp_path):
    monkeypatch.setenv("ORCH_AGENT_LOG_DIR", str(tmp_path))

    path = agent_transcript.record_response(
        task_id="task",
        agent_id="worker",
        role="worker",
        account="account.txt",
        attempt=1,
        raw_response="sessionKey=super-secret-value",
        parsed_ok=True,
        tool_name="submit_patch",
    )

    assert path is not None
    saved = path.read_text(encoding="utf-8")
    assert "super-secret-value" not in saved
    assert "[REDACTED]" in saved
