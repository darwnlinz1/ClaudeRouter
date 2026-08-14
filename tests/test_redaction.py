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


def test_agent_identifiers_survive_redaction():
    """Whether an id used to survive depended only on its role prefix length.

    "manager_<24 hex>" is 32 characters and tripped the high-entropy scanner,
    "worker_<24 hex>" is 31 and did not. Every manager then arrived in the UI as
    the same "[REDACTED]" agent and no worker could find its parent.
    """
    event = {
        "agent_instance_id": "manager_cfed21447246ba05798b3037",
        "logical_agent_id": "director_3a7d4ed0f26bd0253850ce71",
        "manager_id": "manager_cfed21447246ba05798b3037",
        "workstream_id": "core-pipeline",
        "logical_request_id": "llmreq_81d51737b887477ab21dd5a6b0d7f673",
        "manager_agent_ids": [
            "manager_cfed21447246ba05798b3037",
            "manager_06b775d269a9542a3345c6f2",
        ],
        "expected_manager_ids": [
            "manager_cfed21447246ba05798b3037",
            "manager_06b775d269a9542a3345c6f2",
        ],
        "reported_manager_ids": [
            "manager_cfed21447246ba05798b3037",
            "manager_06b775d269a9542a3345c6f2",
        ],
        "cookie": "sessionKey=secret-session-value",
    }

    redacted = redact_event(event)

    assert redacted["agent_instance_id"] == "manager_cfed21447246ba05798b3037"
    assert redacted["logical_agent_id"] == "director_3a7d4ed0f26bd0253850ce71"
    assert redacted["manager_id"] == "manager_cfed21447246ba05798b3037"
    assert redacted["logical_request_id"] == "llmreq_81d51737b887477ab21dd5a6b0d7f673"
    assert redacted["manager_agent_ids"] == list(event["manager_agent_ids"])
    assert redacted["expected_manager_ids"] == list(event["expected_manager_ids"])
    assert redacted["reported_manager_ids"] == list(event["reported_manager_ids"])
    # Credentials are still removed.
    assert redacted["cookie"] == "[REDACTED]"


def test_redaction_truncates_large_text():
    assert "TRUNCATED" in redact_text("x" * 100, max_chars=20)


def test_redaction_keeps_refs_but_removes_raw_accounts_orgs_and_signed_urls():
    redacted = redact_event(
        {
            "account": "credential-a.txt",
            "from_account": "credential-b.txt",
            "org_id": "11111111-2222-3333-4444-555555555555",
            "account_ref": "acct-aaaa-bbbb-cccc",
            "org_ref": "org-aaaa-bbbb-cccc",
            "url": "https://example.test/object?X-Amz-Signature=secret-signature",
        }
    )

    assert redacted["account"] == "[REDACTED]"
    assert redacted["from_account"] == "[REDACTED]"
    assert redacted["org_id"] == "[REDACTED]"
    assert redacted["account_ref"] == "acct-aaaa-bbbb-cccc"
    assert redacted["org_ref"] == "org-aaaa-bbbb-cccc"
    assert redacted["url"] == "[REDACTED_SIGNED_URL]"


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
