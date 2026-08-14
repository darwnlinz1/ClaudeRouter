"""Structured, correlated observability with bounded-cardinality metrics."""
from __future__ import annotations

import os
import re
import threading
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_.]{0,95}$")
LABEL_DOMAINS: dict[str, frozenset[str]] = {
    "component": frozenset(
        {
            "api",
            "artifact",
            "event_broker",
            "lifecycle",
            "llm",
            "repository",
            "scheduler",
            "task",
            "worker",
            "other",
        }
    ),
    "operation": frozenset(
        {
            "append",
            "call",
            "close",
            "compact",
            "drain",
            "generate",
            "persist",
            "reconcile",
            "replay",
            "scan",
            "other",
        }
    ),
    "outcome": frozenset(
        {"aborted", "error", "failure", "success", "timeout", "other"}
    ),
    "provider": frozenset({"web_claude", "local", "other"}),
    "role": frozenset(
        {"director", "manager", "reviewer", "supervisor", "tester", "worker", "other"}
    ),
    "status": frozenset(
        {
            "cancelled",
            "completed",
            "failed",
            "pending",
            "running",
            "stopped",
            "other",
        }
    ),
    "error_class": frozenset(
        {
            "auth",
            "cancelled",
            "conflict",
            "invalid",
            "io",
            "rate_limit",
            "timeout",
            "transport",
            "unknown",
            "other",
        }
    ),
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class ObservationContext:
    task_id: str | None = None
    session_id: str | None = None
    agent_instance_id: str | None = None
    call_id: str | None = None
    attempt_id: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "task_id",
            "session_id",
            "agent_instance_id",
            "call_id",
            "attempt_id",
        ):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be a non-empty string when provided")


@dataclass(frozen=True, slots=True)
class ObservabilityRecord:
    kind: str
    name: str
    context: ObservationContext = field(default_factory=ObservationContext)
    labels: Mapping[str, str] = field(default_factory=dict)
    attributes: Mapping[str, Any] = field(default_factory=dict)
    value: float | None = None
    duration_ms: float | None = None
    record_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if self.kind not in {"log", "metric", "span"}:
            raise ValueError("kind must be log, metric, or span")
        if not _NAME_PATTERN.fullmatch(self.name):
            raise ValueError("name must be a stable lowercase observability name")
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        object.__setattr__(self, "labels", normalize_labels(self.labels))
        object.__setattr__(self, "attributes", dict(self.attributes))
        if self.value is not None:
            object.__setattr__(self, "value", float(self.value))
        if self.duration_ms is not None and self.duration_ms < 0:
            raise ValueError("duration_ms must be non-negative")

    def to_record(self) -> dict[str, Any]:
        value = asdict(self)
        value["timestamp"] = self.timestamp.isoformat()
        value.update(value.pop("context"))
        return value


def normalize_labels(labels: Mapping[str, Any] | None) -> dict[str, str]:
    """Normalize labels into declared finite domains.

    Correlation IDs belong in ``ObservationContext`` and are rejected as
    labels, preventing task/call identifiers from creating unbounded series.
    """
    normalized: dict[str, str] = {}
    for key, raw_value in dict(labels or {}).items():
        if key not in LABEL_DOMAINS:
            raise ValueError(f"metric label is not bounded: {key}")
        value = str(raw_value).strip().casefold().replace("-", "_")
        normalized[key] = value if value in LABEL_DOMAINS[key] else "other"
    return dict(sorted(normalized.items()))


class ObservabilityRepository(Protocol):
    def record_observability(self, record: Mapping[str, Any]) -> str: ...


class StructuredObserver:
    def __init__(
        self,
        repository: ObservabilityRepository | None = None,
        *,
        max_records: int | None = None,
    ) -> None:
        configured_limit = (
            int(os.environ.get("ORCH_OBSERVABILITY_MAX_RECORDS", "2000"))
            if max_records is None
            else max_records
        )
        if configured_limit < 1:
            raise ValueError("max_records must be positive")
        self._repository = repository
        self._lock = threading.RLock()
        self._records: deque[ObservabilityRecord] = deque(maxlen=configured_limit)

    def emit(
        self,
        record: ObservabilityRecord,
        *,
        persist: bool = True,
    ) -> ObservabilityRecord:
        with self._lock:
            self._records.append(record)
        if persist and self._repository is not None:
            self._repository.record_observability(record.to_record())
        return record

    def records(self) -> tuple[ObservabilityRecord, ...]:
        with self._lock:
            return tuple(self._records)


class MetricsRegistry:
    """Small in-process metric registry with a hard series bound."""

    def __init__(
        self,
        observer: StructuredObserver | None = None,
        *,
        max_series: int = 512,
        max_metric_names: int = 64,
    ) -> None:
        if max_series < 1 or max_metric_names < 1:
            raise ValueError("metric bounds must be positive")
        self._observer = observer
        self._max_series = max_series
        self._max_metric_names = max_metric_names
        self._series: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._names: set[str] = set()
        self._lock = threading.RLock()

    def increment(
        self,
        name: str,
        value: float = 1.0,
        *,
        labels: Mapping[str, Any] | None = None,
        context: ObservationContext | None = None,
        attributes: Mapping[str, Any] | None = None,
        persist: bool = True,
    ) -> ObservabilityRecord:
        if not _NAME_PATTERN.fullmatch(name):
            raise ValueError("metric name must be stable lowercase text")
        normalized = normalize_labels(labels)
        key = (name, tuple(normalized.items()))
        numeric_value = float(value)
        with self._lock:
            if name not in self._names and len(self._names) >= self._max_metric_names:
                raise RuntimeError("metric name bound exceeded")
            if key not in self._series and len(self._series) >= self._max_series:
                raise RuntimeError("metric series bound exceeded")
            self._names.add(name)
            self._series[key] = self._series.get(key, 0.0) + numeric_value
        record = ObservabilityRecord(
            kind="metric",
            name=name,
            context=context or ObservationContext(),
            labels=normalized,
            attributes=dict(attributes or {}),
            value=numeric_value,
        )
        if self._observer is not None:
            self._observer.emit(record, persist=persist)
        return record

    def snapshot(self) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
        with self._lock:
            return dict(self._series)
