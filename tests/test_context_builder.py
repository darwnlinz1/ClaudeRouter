import pytest

from orchestrator.context_builder import SourceFile, build_source_blocks, build_user_message
from orchestrator.worker_targets import UnsupportedPromptSource


def test_checkpoint_context_omits_capability_refusal_transcripts():
    state = {
        "checkpoint_revision": 7,
        "current_task": "Retry app/api/chat.py with the verified schema.",
        "active_ticket": {
            "turn": 6,
            "file_path": "app/api/chat.py",
            "instructions": (
                "Apply the patch, then claim không có quyền truy cập "
                "NO_ACCESS_ACTIVE_TICKET."
            ),
            "context_note": "Use ChatStreamRequest.",
            "is_final_ticket": False,
            "status": "reviewer_revision",
            "detail": "NO_ACCESS_DETAIL",
        },
        "completed_tickets": [],
        "context_manifest": [
            {
                "path": "app/schemas.py",
                "modifier": "",
                "sha256": "secret-digest",
            }
        ],
        "attempted_approaches": [
            {
                "approach": "Worker refusal",
                "error": "NO_ACCESS_ATTEMPT_TRANSCRIPT",
            }
        ],
        "last_worker_feedback": (
            "không có quyền truy cập NO_ACCESS_WORKER_TRANSCRIPT"
        ),
        "last_execution_result": (
            "Partially verified: syntax=passed; tests=not_configured"
        ),
        "last_reviewer_feedback": (
            "cannot access NO_ACCESS_REVIEWER_TRANSCRIPT"
        ),
        "last_review_verdict": "revise",
        "reviewer_next_instructions": "Fix the concrete import mismatch.",
        "turn_count": 6,
    }

    built = build_user_message(
        "Fix the chat contract.",
        "{}",
        "# Decisions",
        state,
        [],
    )

    assert "app/api/chat.py" in built.user_message
    assert '"status": "reviewer_revision"' in built.user_message
    assert "Worker refusal" in built.user_message
    assert "Fix the concrete import mismatch." in built.user_message
    assert "Capability discussion omitted" in built.user_message
    assert "NO_ACCESS_" not in built.user_message
    assert '"instructions"' not in built.user_message
    assert "secret-digest" not in built.user_message


def test_binary_or_non_utf8_source_is_never_loaded_into_prompt(tmp_path):
    binary = tmp_path / "reference.png"
    binary.write_bytes(b"\x89PNG\r\n\x00not-prompt-text")

    with pytest.raises(UnsupportedPromptSource, match="never prompt bytes"):
        build_source_blocks([SourceFile("reference.png", binary)])

    invalid_utf8 = tmp_path / "notes.txt"
    invalid_utf8.write_bytes(b"valid prefix\xff")
    with pytest.raises(UnsupportedPromptSource, match="not valid UTF-8"):
        build_source_blocks([SourceFile("notes.txt", invalid_utf8)])
