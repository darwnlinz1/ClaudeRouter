"""Provider-neutral contracts for the cookie-backed Web Claude transport.

The orchestrator owns retries and durable events.  Adapters own exactly one
transport attempt and must not publish stream chunks before that attempt has
completed successfully.
"""
from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

LEGACY_WEB_PROVIDER = "legacy_web"
DEFAULT_STREAM_BATCH_CHARS = 16 * 1024
DEFAULT_STREAM_MAX_CHARS = 8 * 1024 * 1024

AbortCheck = Callable[[], bool]


@dataclass(frozen=True)
class ProviderToolCall:
    """Portable tool-call representation independent of provider wire shape."""

    id: str
    name: str
    arguments: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderMessage:
    """One provider-neutral chat message."""

    role: str
    content: str
    tool_calls: tuple[ProviderToolCall, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderRequest:
    """An immutable logical request prepared for one transport attempt."""

    logical_call_id: str
    attempt_id: str
    request_fingerprint: str
    model: str
    max_tokens: int
    messages: tuple[ProviderMessage, ...]
    system: str | None = None
    effort: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def prompt(self) -> str:
        """Return the final text prompt used by legacy single-prompt adapters."""

        if not self.messages:
            return ""
        if len(self.messages) == 1:
            return self.messages[0].content

        current_index = len(self.messages) - 1
        for index in range(len(self.messages) - 1, -1, -1):
            if self.messages[index].role == "user":
                current_index = index
                break
        history = self.messages[:current_index]
        current = self.messages[current_index]
        if not history:
            return current.content

        records = [
            json.dumps(
                {
                    "content": message.content,
                    "role": message.role,
                    "tool_calls": [
                        {
                            "arguments": dict(call.arguments),
                            "id": call.id,
                            "name": call.name,
                        }
                        for call in message.tool_calls
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            for message in history
        ]
        return (
            "PORTABLE_CONVERSATION_CONTEXT_V1\n"
            + "\n".join(records)
            + "\nEND_PORTABLE_CONVERSATION_CONTEXT\n"
            + current.content
        )


@dataclass(frozen=True)
class ProviderChunk:
    """A buffered stream fragment exposed only after attempt completion."""

    index: int
    text: str
    kind: str = "token"
    metadata: Mapping[str, Any] = field(default_factory=dict)


class ProviderStreamLimitError(ValueError):
    """Raised before an attempt can retain an unbounded streamed response."""


class ProviderStreamBuffer:
    """Bounded, coalescing buffer whose content is visible only on finish."""

    def __init__(
        self,
        *,
        batch_chars: int = DEFAULT_STREAM_BATCH_CHARS,
        max_chars: int = DEFAULT_STREAM_MAX_CHARS,
    ) -> None:
        if batch_chars <= 0 or max_chars <= 0 or batch_chars > max_chars:
            raise ValueError("invalid provider stream buffer bounds")
        self.batch_chars = int(batch_chars)
        self.max_chars = int(max_chars)
        self.total_chars = 0
        self._chunks: list[ProviderChunk] = []
        self._pending_kind: str | None = None
        self._pending_parts: list[str] = []
        self._pending_chars = 0
        self._finished = False

    def append(self, text: str, *, kind: str = "token") -> None:
        if self._finished:
            raise RuntimeError("provider stream buffer is already finished")
        if not text:
            return
        if self.total_chars + len(text) > self.max_chars:
            raise ProviderStreamLimitError(
                f"Provider response exceeded {self.max_chars} characters"
            )
        self.total_chars += len(text)
        offset = 0
        while offset < len(text):
            if self._pending_kind is not None and self._pending_kind != kind:
                self._flush()
            self._pending_kind = kind
            available = self.batch_chars - self._pending_chars
            part = text[offset : offset + available]
            offset += len(part)
            self._pending_parts.append(part)
            self._pending_chars += len(part)
            if self._pending_chars >= self.batch_chars:
                self._flush()

    def _flush(self) -> None:
        if not self._pending_parts:
            return
        self._chunks.append(
            ProviderChunk(
                index=len(self._chunks),
                text="".join(self._pending_parts),
                kind=self._pending_kind or "token",
            )
        )
        self._pending_parts = []
        self._pending_chars = 0
        self._pending_kind = None

    def finish(self) -> tuple[str, tuple[ProviderChunk, ...]]:
        if not self._finished:
            self._flush()
            self._finished = True
        chunks = tuple(self._chunks)
        return "".join(chunk.text for chunk in chunks), chunks


@dataclass(frozen=True)
class ProviderResponse:
    """Normalized result of one fully completed provider attempt."""

    provider: str
    content: str
    chunks: tuple[ProviderChunk, ...] = ()
    tool_calls: tuple[ProviderToolCall, ...] = ()
    response_id: str | None = None
    finish_reason: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class ProviderErrorCode(str, Enum):
    AUTHENTICATION = "authentication"
    RATE_LIMIT = "rate_limit"
    PAYLOAD = "payload"
    TRANSPORT = "transport"
    ABORTED = "aborted"
    PROVIDER = "provider"


@dataclass(frozen=True)
class ProviderErrorInfo:
    """Serializable, provider-neutral error metadata."""

    provider: str
    code: ProviderErrorCode
    message: str
    retryable: bool = False
    status_code: int | None = None
    retry_after_seconds: int | None = None


class ProviderError(RuntimeError):
    """Base exception carrying normalized error metadata."""

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        code: ProviderErrorCode,
        retryable: bool = False,
        status_code: int | None = None,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message)
        self.info = ProviderErrorInfo(
            provider=provider,
            code=code,
            message=message,
            retryable=retryable,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
        )


class ProviderAuthenticationError(ProviderError):
    def __init__(
        self, message: str, *, provider: str, status_code: int | None = None
    ) -> None:
        super().__init__(
            message,
            provider=provider,
            code=ProviderErrorCode.AUTHENTICATION,
            status_code=status_code,
        )


class ProviderRateLimitError(ProviderError):
    def __init__(
        self,
        message: str,
        *,
        provider: str,
        retry_after_seconds: int,
        status_code: int | None = 429,
    ) -> None:
        super().__init__(
            message,
            provider=provider,
            code=ProviderErrorCode.RATE_LIMIT,
            retryable=True,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
        )


class ProviderPayloadError(ProviderError):
    def __init__(
        self, message: str, *, provider: str, status_code: int | None = 400
    ) -> None:
        super().__init__(
            message,
            provider=provider,
            code=ProviderErrorCode.PAYLOAD,
            status_code=status_code,
        )


class ProviderTransportError(ProviderError):
    def __init__(
        self,
        message: str,
        *,
        provider: str,
        status_code: int | None = None,
    ) -> None:
        super().__init__(
            message,
            provider=provider,
            code=ProviderErrorCode.TRANSPORT,
            retryable=True,
            status_code=status_code,
        )


class ProviderAbortedError(ProviderError):
    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(
            message,
            provider=provider,
            code=ProviderErrorCode.ABORTED,
        )


@runtime_checkable
class ProviderAdapter(Protocol):
    """One-attempt transport boundary used by the retry orchestrator."""

    name: str

    def complete(
        self,
        request: ProviderRequest,
        *,
        should_abort: AbortCheck | None = None,
    ) -> ProviderResponse:
        """Run one attempt and return only fully committed buffered output."""


def parse_retry_after(
    value: str | int | float | None,
    *,
    now: float | datetime | None = None,
    default: int = 120,
    minimum: int = 1,
    maximum: int = 3600,
) -> int:
    """Parse Retry-After delay-seconds or HTTP-date into a bounded delay."""

    if minimum < 0 or maximum < minimum:
        raise ValueError("invalid Retry-After bounds")
    parsed: int | None = None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isfinite(float(value)):
            parsed = math.ceil(float(value))
    elif isinstance(value, str):
        stripped = value.strip()
        if re.fullmatch(r"\d+(?:\.\d+)?", stripped):
            parsed = math.ceil(float(stripped))
        elif stripped:
            try:
                retry_at = parsedate_to_datetime(stripped)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                if isinstance(now, datetime):
                    current = now
                    if current.tzinfo is None:
                        current = current.replace(tzinfo=timezone.utc)
                    current_timestamp = current.timestamp()
                elif now is None:
                    current_timestamp = time.time()
                else:
                    current_timestamp = float(now)
                parsed = math.ceil(retry_at.timestamp() - current_timestamp)
            except (TypeError, ValueError, OverflowError):
                parsed = None
    if parsed is None:
        parsed = int(default)
    return max(minimum, min(int(parsed), maximum))


def wait_for_retry_after(
    seconds: int | float,
    *,
    should_abort: AbortCheck | None = None,
    sleep: Callable[[float], None] = time.sleep,
    maximum: int = 3600,
    quantum: float = 0.25,
) -> None:
    """Wait for a bounded cooldown while checking cancellation frequently."""

    remaining = float(parse_retry_after(seconds, default=1, maximum=maximum))
    while remaining > 0:
        if should_abort is not None and should_abort():
            raise ProviderAbortedError(
                "Model request was aborted during provider cooldown",
                provider=LEGACY_WEB_PROVIDER,
            )
        interval = min(max(0.01, quantum), remaining)
        sleep(interval)
        remaining -= interval
