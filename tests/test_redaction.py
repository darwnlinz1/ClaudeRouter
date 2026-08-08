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
