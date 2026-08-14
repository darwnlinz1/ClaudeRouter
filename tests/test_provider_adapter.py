from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from orchestrator.provider_adapter import (
    LEGACY_WEB_PROVIDER,
    ProviderMessage,
    ProviderRequest,
    ProviderStreamBuffer,
    ProviderStreamLimitError,
    parse_retry_after,
)


def test_provider_request_exposes_cookie_adapter_prompt():
    request = ProviderRequest(
        logical_call_id="call-1",
        attempt_id="call-1:r1:a1",
        request_fingerprint="fingerprint",
        model="claude-test",
        max_tokens=100,
        messages=(ProviderMessage(role="user", content="hello"),),
    )

    assert LEGACY_WEB_PROVIDER == "legacy_web"
    assert request.prompt == "hello"


def test_retry_after_parses_seconds_and_http_date():
    now = datetime(2026, 8, 12, 15, 0, tzinfo=timezone.utc)
    later = format_datetime(now + timedelta(seconds=37), usegmt=True)

    assert parse_retry_after("17", now=now) == 17
    assert parse_retry_after(later, now=now) == 37


def test_retry_after_is_bounded_and_invalid_values_use_default():
    assert parse_retry_after("99999", maximum=300) == 300
    assert parse_retry_after("not-a-date", default=45) == 45
    assert parse_retry_after("-5", default=20) == 20


def test_stream_buffer_coalesces_large_fragment_counts_into_bounded_batches():
    buffer = ProviderStreamBuffer(batch_chars=64, max_chars=10_000)
    for _ in range(5_000):
        buffer.append("x")

    content, chunks = buffer.finish()

    assert content == "x" * 5_000
    assert len(chunks) == 79
    assert all(0 < len(chunk.text) <= 64 for chunk in chunks)
    assert [chunk.index for chunk in chunks] == list(range(len(chunks)))


def test_stream_buffer_rejects_attempts_over_the_total_limit():
    buffer = ProviderStreamBuffer(batch_chars=4, max_chars=8)
    buffer.append("12345678")

    with pytest.raises(ProviderStreamLimitError, match="exceeded"):
        buffer.append("9")


def test_cookie_adapter_prompt_serializes_portable_history_deterministically():
    request = ProviderRequest(
        logical_call_id="call-2",
        attempt_id="call-2:r1:a1",
        request_fingerprint="fingerprint",
        model="claude-test",
        max_tokens=100,
        messages=(
            ProviderMessage(role="user", content="prior question"),
            ProviderMessage(role="assistant", content="prior answer"),
            ProviderMessage(role="user", content="current question"),
        ),
    )

    assert request.prompt == (
        "PORTABLE_CONVERSATION_CONTEXT_V1\n"
        '{"content":"prior question","role":"user","tool_calls":[]}\n'
        '{"content":"prior answer","role":"assistant","tool_calls":[]}\n'
        "END_PORTABLE_CONVERSATION_CONTEXT\n"
        "current question"
    )
