import importlib
import ipaddress
import json
import logging
import os
import queue
import secrets
import stat
import threading
import time
import tkinter as tk
import uuid
from collections import OrderedDict
from collections.abc import Iterator, MutableMapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from tkinter import filedialog
from typing import Literal
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from orchestrator import (
    __version__,
    agent_transcript,
    artifact_manager,
    config,
    event_broker,
    path_utils,
    redaction,
    task_manager,
)
from orchestrator import (
    llm_client as llm_runtime,
)
from orchestrator.deployment import require_deployment_ready
from orchestrator.event_schema import EVENT_SCHEMAS, validate_event_payload
from orchestrator.event_schema import SCHEMA_VERSION as EVENT_SCHEMA_VERSION
from orchestrator.hierarchy import run_hierarchy
from orchestrator.lifecycle import LifecycleCoordinator
from orchestrator.llm_client import call_agent
from orchestrator.managed_retention import ManagedRetentionService
from orchestrator.models import EventEnvelope, to_dict
from orchestrator.observability import (
    MetricsRegistry,
    ObservabilityRecord,
    ObservationContext,
    StructuredObserver,
)
from orchestrator.orchestrator import run_session
from orchestrator.policy import (
    PolicyAction,
    PolicyEngine,
    PolicyRequest,
    PolicySubject,
)
from orchestrator.project_workspace import (
    ProjectLeaseLostError,
    ProjectLeaseManager,
    sha256_file,
)
from orchestrator.scheduler import SchedulerLimits
from orchestrator.sqlite_account_lease import SQLiteAccountLeaseStore
from orchestrator.state_repository import (
    CURRENT_SCHEMA_VERSION,
    RetentionPolicy,
    StateRepository,
)

_NATIVE_THREAD = threading.Thread
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TaskRuntime:
    task_id: str
    broker: event_broker.ReplayEventBroker
    cancellation: threading.Event = field(default_factory=threading.Event)
    chat_queue: queue.Queue[str] | None = field(default_factory=queue.Queue)
    worker_thread: object | None = None
    heartbeat_threads: list[object] = field(default_factory=list)


class TaskRuntimeRegistry:
    """Own one execution runtime per task and coordinate bounded shutdown."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runtimes: dict[str, TaskRuntime] = {}
        self._accepting = True
        self._compat_stops: dict[object, bool] = {}
        self._compat_chats: dict[object, queue.Queue[str]] = {}

    def reserve(self, task_id: str, *, initial_sequence: int = 0) -> TaskRuntime:
        with self._lock:
            if not self._accepting:
                raise RuntimeError("task admission is closed")
            if task_id in self._runtimes:
                raise RuntimeError(f"task {task_id} already has an active runtime")
            runtime = TaskRuntime(
                task_id=task_id,
                broker=event_broker.ReplayEventBroker(initial_sequence=initial_sequence),
            )
            self._runtimes[task_id] = runtime
            return runtime

    def install_compat(
        self,
        task_id: object,
        broker: event_broker.ReplayEventBroker,
    ) -> TaskRuntime:
        key = str(task_id)
        with self._lock:
            runtime = self._runtimes.get(key)
            if runtime is None:
                runtime = TaskRuntime(task_id=key, broker=broker)
                self._runtimes[key] = runtime
            else:
                runtime.broker = broker
            return runtime

    def get(self, task_id: object) -> TaskRuntime | None:
        with self._lock:
            return self._runtimes.get(str(task_id))

    def runtimes(self) -> tuple[TaskRuntime, ...]:
        with self._lock:
            return tuple(self._runtimes.values())

    def task_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._runtimes)

    def bind_worker(self, task_id: str, worker: object) -> None:
        with self._lock:
            runtime = self._runtimes.get(task_id)
            if runtime is None:
                raise RuntimeError("task runtime was evicted before worker start")
            if runtime.worker_thread is not None:
                raise RuntimeError("task runtime already has a worker")
            runtime.worker_thread = worker

    def add_heartbeat(self, task_id: str, worker: object) -> None:
        with self._lock:
            runtime = self._runtimes.get(task_id)
            if runtime is not None and worker not in runtime.heartbeat_threads:
                runtime.heartbeat_threads.append(worker)

    def cancel(self, task_id: object) -> bool:
        with self._lock:
            runtime = self._runtimes.get(str(task_id))
            if runtime is None:
                self._compat_stops[task_id] = True
                return False
            runtime.cancellation.set()
            return True

    def is_cancelled(self, task_id: object) -> bool:
        with self._lock:
            runtime = self._runtimes.get(str(task_id))
            if runtime is not None:
                return runtime.cancellation.is_set()
            return bool(self._compat_stops.get(task_id, False))

    def evict(
        self,
        task_id: object,
        *,
        expected: TaskRuntime | None = None,
    ) -> TaskRuntime | None:
        key = str(task_id)
        with self._lock:
            runtime = self._runtimes.get(key)
            if runtime is None or (expected is not None and runtime is not expected):
                return None
            self._runtimes.pop(key, None)
            self._compat_stops.pop(task_id, None)
            self._compat_chats.pop(task_id, None)
            return runtime

    def complete(self, runtime: TaskRuntime, *, timeout: float = 5.0) -> None:
        runtime.broker.close(timeout=timeout)
        self.evict(runtime.task_id, expected=runtime)

    def stop_admission(self) -> None:
        with self._lock:
            self._accepting = False

    def clear(self) -> None:
        with self._lock:
            runtimes = tuple(self._runtimes.values())
            self._runtimes.clear()
            self._compat_stops.clear()
            self._compat_chats.clear()
            self._accepting = True
        for runtime in runtimes:
            runtime.cancellation.set()
            runtime.broker.close(timeout=0)

    @staticmethod
    def _join(worker: object | None, deadline: float) -> bool:
        if worker is None or worker is threading.current_thread():
            return True
        join = getattr(worker, "join", None)
        if not callable(join):
            return True
        join(max(0.0, deadline - time.monotonic()))
        alive = getattr(worker, "is_alive", None)
        return not (callable(alive) and alive())

    def shutdown(self, timeout: float = 10.0) -> tuple[str, ...]:
        self.stop_admission()
        deadline = time.monotonic() + max(0.0, timeout)
        runtimes = self.runtimes()
        for runtime in runtimes:
            runtime.cancellation.set()
            if runtime.chat_queue is not None:
                runtime.chat_queue.put("")
        timed_out: list[str] = []
        for runtime in runtimes:
            if not self._join(runtime.worker_thread, deadline):
                timed_out.append(f"task:{runtime.task_id}")
            for heartbeat in tuple(runtime.heartbeat_threads):
                if not self._join(heartbeat, deadline):
                    timed_out.append(str(getattr(heartbeat, "name", None) or runtime.task_id))
        for runtime in runtimes:
            runtime.broker.close(timeout=max(0.0, deadline - time.monotonic()))
            self.evict(runtime.task_id, expected=runtime)
        return tuple(timed_out)


runtime_registry = TaskRuntimeRegistry()


class _ActiveQueueView(MutableMapping[object, event_broker.ReplayEventBroker]):
    """Temporary mapping compatibility for existing local integrations."""

    def __getitem__(self, key: object) -> event_broker.ReplayEventBroker:
        runtime = runtime_registry.get(key)
        if runtime is None:
            raise KeyError(key)
        return runtime.broker

    def __setitem__(
        self,
        key: object,
        value: event_broker.ReplayEventBroker,
    ) -> None:
        runtime_registry.install_compat(key, value)

    def __delitem__(self, key: object) -> None:
        if runtime_registry.evict(key) is None:
            raise KeyError(key)

    def __iter__(self) -> Iterator[object]:
        return iter(runtime_registry.task_ids())

    def __len__(self) -> int:
        return len(runtime_registry.task_ids())

    def clear(self) -> None:
        runtime_registry.clear()


class _StopFlagView(MutableMapping[object, bool]):
    def __getitem__(self, key: object) -> bool:
        if not runtime_registry.is_cancelled(key):
            runtime = runtime_registry.get(key)
            if runtime is None and key not in runtime_registry._compat_stops:
                raise KeyError(key)
        return runtime_registry.is_cancelled(key)

    def __setitem__(self, key: object, value: bool) -> None:
        runtime = runtime_registry.get(key)
        if runtime is not None:
            if value:
                runtime.cancellation.set()
            else:
                runtime.cancellation.clear()
            return
        runtime_registry._compat_stops[key] = bool(value)

    def __delitem__(self, key: object) -> None:
        runtime = runtime_registry.get(key)
        if runtime is not None:
            runtime.cancellation.clear()
            return
        if runtime_registry._compat_stops.pop(key, None) is None:
            raise KeyError(key)

    def __iter__(self) -> Iterator[object]:
        keys: list[object] = list(runtime_registry.task_ids())
        keys.extend(runtime_registry._compat_stops)
        return iter(dict.fromkeys(keys))

    def __len__(self) -> int:
        return len(tuple(iter(self)))

    def clear(self) -> None:
        for runtime in runtime_registry.runtimes():
            runtime.cancellation.clear()
        runtime_registry._compat_stops.clear()


class _ChatQueueView(MutableMapping[object, queue.Queue[str]]):
    def __getitem__(self, key: object) -> queue.Queue[str]:
        runtime = runtime_registry.get(key)
        if runtime is not None and runtime.chat_queue is not None:
            return runtime.chat_queue
        try:
            return runtime_registry._compat_chats[key]
        except KeyError as exc:
            raise KeyError(key) from exc

    def __setitem__(self, key: object, value: queue.Queue[str]) -> None:
        runtime = runtime_registry.get(key)
        if runtime is not None:
            runtime.chat_queue = value
        else:
            runtime_registry._compat_chats[key] = value

    def __delitem__(self, key: object) -> None:
        runtime = runtime_registry.get(key)
        if runtime is not None and runtime.chat_queue is not None:
            runtime.chat_queue = None
            return
        if runtime_registry._compat_chats.pop(key, None) is None:
            raise KeyError(key)

    def __iter__(self) -> Iterator[object]:
        keys: list[object] = [
            runtime.task_id
            for runtime in runtime_registry.runtimes()
            if runtime.chat_queue is not None
        ]
        keys.extend(runtime_registry._compat_chats)
        return iter(dict.fromkeys(keys))

    def __len__(self) -> int:
        return len(tuple(iter(self)))

    def clear(self) -> None:
        for runtime in runtime_registry.runtimes():
            runtime.chat_queue = None
        runtime_registry._compat_chats.clear()


active_queues: MutableMapping[object, event_broker.ReplayEventBroker] = _ActiveQueueView()
stop_flags: MutableMapping[object, bool] = _StopFlagView()
chat_input_queues: MutableMapping[object, queue.Queue[str]] = _ChatQueueView()
hierarchy_repository = StateRepository()
task_manager.configure_repository(hierarchy_repository)
agent_transcript.configure_repository(hierarchy_repository)
artifact_manager.configure_repository(hierarchy_repository)
_configure_request_log = getattr(
    llm_runtime,
    "configure_request_log_repository",
    None,
)
if callable(_configure_request_log):
    try:
        _configure_request_log(hierarchy_repository)
    except Exception:
        logger.warning("Could not configure the optional LLM request log", exc_info=True)
deployment_profile = require_deployment_ready()


_EVENT_CORRELATION_FIELDS = frozenset(
    {
        "type",
        "task_id",
        "session_id",
        "workstream_id",
        "work_item_id",
        "agent_instance_id",
        "call_id",
    }
)
_LARGE_EVENT_FIELDS = frozenset({"prompt", "patch", "diff", "text", "test_output"})


def _event_envelope(task_id: str, session_id: str, raw: dict) -> EventEnvelope:
    event_type = str(raw.get("type") or "event")
    return EventEnvelope(
        task_id=task_id,
        session_id=session_id,
        event_type=event_type,
        version=EVENT_SCHEMA_VERSION,
        payload={
            key: value
            for key, value in raw.items()
            if key not in _EVENT_CORRELATION_FIELDS
        },
        workstream_id=raw.get("workstream_id"),
        work_item_id=raw.get("work_item_id"),
        agent_instance_id=raw.get("agent_instance_id"),
        call_id=raw.get("call_id"),
    )


def _validate_event_envelope(envelope: EventEnvelope) -> None:
    validate_event_payload(
        envelope.event_type,
        envelope.payload,
        task_id=envelope.task_id,
        session_id=envelope.session_id,
        workstream_id=envelope.workstream_id,
        work_item_id=envelope.work_item_id,
        agent_instance_id=envelope.agent_instance_id,
        call_id=envelope.call_id,
        sequence=envelope.sequence,
        timestamp=envelope.timestamp.isoformat(),
    )


def _schema_diagnostic(
    *,
    task_id: str,
    session_id: str,
    raw: dict,
    validation_error: Exception,
    repaired_fields: list[str],
    repaired: bool,
) -> dict:
    source_event_type = str(raw.get("type") or "event")
    action = "repaired" if repaired else "rejected"
    diagnostic = {
        "type": (
            "event_schema_validation_repaired"
            if repaired
            else "event_schema_validation_failure"
        ),
        "task_id": task_id,
        "session_id": session_id,
        "source_event_type": source_event_type,
        "action": action,
        "validation_error": redaction.redact_text(
            str(validation_error),
            max_chars=1000,
        ),
        "repaired_fields": sorted(repaired_fields),
        "field_names": sorted(str(key) for key in raw),
        "summary": (
            f"Event {source_event_type!r} failed schema validation and was {action}; "
            "the original event body was not retained in this diagnostic."
        ),
    }
    for field_name in (
        "workstream_id",
        "work_item_id",
        "agent_instance_id",
        "logical_request_id",
        "execution_attempt_id",
        "provider_attempt_id",
        "attempt_id",
        "call_id",
        "call_purpose",
        "role",
    ):
        value = raw.get(field_name)
        if isinstance(value, (str, int, float, bool)) and value not in {"", None}:
            diagnostic[field_name] = value
    return diagnostic


def _normalize_runtime_event(
    task_id: str,
    session_id: str,
    event: object,
) -> tuple[dict | None, dict | None]:
    """Redact, validate, and narrowly repair one runtime event.

    A rejected event is replaced by a sanitized diagnostic. Returning instead
    of raising is important: the legacy model-runtime fallback queue receives
    the caller's original object whenever its injected sink raises.
    """

    try:
        raw_value = dict(event)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raw = {"type": "event"}
        return None, _schema_diagnostic(
            task_id=task_id,
            session_id=session_id,
            raw=raw,
            validation_error=ValueError(f"event must be a mapping: {type(exc).__name__}"),
            repaired_fields=[],
            repaired=False,
        )
    raw = redaction.redact_event(raw_value)
    if not isinstance(raw, dict):
        raw = {"type": "event"}
    raw["type"] = str(raw.get("type") or "event")
    try:
        _validate_event_envelope(_event_envelope(task_id, session_id, raw))
        return raw, None
    except (TypeError, ValueError) as validation_error:
        repaired = dict(raw)
        repaired_fields: list[str] = []
        spec = EVENT_SCHEMAS.get(str(raw["type"]))
        if spec is not None and "error" in spec.required:
            current_error = repaired.get("error")
            if not isinstance(current_error, str) or not current_error.strip():
                replacement = next(
                    (
                        repaired.get(name)
                        for name in ("reason", "summary", "detail", "message", "data")
                        if isinstance(repaired.get(name), str)
                        and str(repaired.get(name)).strip()
                    ),
                    None,
                )
                repaired["error"] = (
                    replacement
                    if replacement is not None
                    else f"{raw['type']} emitted without error detail"
                )
                repaired_fields.append("error")
        if repaired_fields:
            try:
                _validate_event_envelope(
                    _event_envelope(task_id, session_id, repaired)
                )
            except (TypeError, ValueError):
                pass
            else:
                return repaired, _schema_diagnostic(
                    task_id=task_id,
                    session_id=session_id,
                    raw=raw,
                    validation_error=validation_error,
                    repaired_fields=repaired_fields,
                    repaired=True,
                )
        return None, _schema_diagnostic(
            task_id=task_id,
            session_id=session_id,
            raw=raw,
            validation_error=validation_error,
            repaired_fields=repaired_fields,
            repaired=False,
        )


def _write_schema_diagnostic_to_request_log(diagnostic: dict) -> None:
    """Best-effort bridge to the optional request-attempt ledger."""

    hook = None
    try:
        request_log = importlib.import_module("orchestrator.llm_request_log")
        hook = getattr(request_log, "record_event_schema_diagnostic", None)
    except (ImportError, AttributeError):
        hook = None
    if not callable(hook):
        hook = getattr(hierarchy_repository, "record_event_schema_diagnostic", None)
    if not callable(hook):
        return
    try:
        hook(dict(redaction.redact_event(diagnostic)))
    except Exception:
        # Diagnostics must never turn successfully completed Worker domain work
        # into a failed request.
        logger.warning("Could not write event-schema diagnostic to request log", exc_info=True)


def _projectable_event(raw: dict) -> dict:
    task_event = dict(raw)
    for large_field in _LARGE_EVENT_FIELDS:
        task_event.pop(large_field, None)
    if isinstance(task_event.get("payload"), dict):
        task_event["payload"] = {
            key: value
            for key, value in task_event["payload"].items()
            if key not in _LARGE_EVENT_FIELDS
        }
    return task_event


def _persist_redacted_runtime_event(
    task_id: str,
    session_id: str,
    raw: dict,
    broker: event_broker.ReplayEventBroker,
) -> dict:
    envelope = _event_envelope(task_id, session_id, raw)
    task_event = _projectable_event(raw)
    try:
        stored = task_manager.record_event(
            task_id,
            task_event,
            envelope=envelope,
        )
    except RuntimeError as exc:
        if str(exc) != "atomic event projection requires a repository":
            raise
        # Compatibility-only JSON/in-memory task stores have no shared
        # transaction to join. Project first, then synthesize the live cursor.
        task_manager.record_event(task_id, task_event)
        stored = replace(
            envelope,
            sequence=broker.latest_sequence + 1,
        )
    if stored is None:
        raise RuntimeError("task event projection did not commit")
    wire = {
        **raw,
        **to_dict(stored),
        "type": envelope.event_type,
        "sequence": stored.sequence,
        "timestamp": stored.timestamp.isoformat(),
    }
    _observe_runtime_event(
        task_id,
        session_id,
        wire,
        sequence=stored.sequence,
    )
    broker.put(wire)
    return wire


def _emit_persisted_runtime_event(
    task_id: str,
    session_holder: dict[str, str],
    broker: event_broker.ReplayEventBroker,
    event: object,
) -> list[dict]:
    raw_session_id = (
        event.get("session_id")
        if isinstance(event, dict)
        else None
    )
    session_id = str(raw_session_id or session_holder["id"])
    session_holder["id"] = session_id
    normalized, diagnostic = _normalize_runtime_event(
        task_id,
        session_id,
        event,
    )
    emitted: list[dict] = []
    if diagnostic is not None:
        _write_schema_diagnostic_to_request_log(diagnostic)
        try:
            emitted.append(
                _persist_redacted_runtime_event(
                    task_id,
                    session_id,
                    diagnostic,
                    broker,
                )
            )
        except Exception:
            logger.warning("Could not persist event-schema diagnostic", exc_info=True)
    if normalized is not None:
        try:
            emitted.append(
                _persist_redacted_runtime_event(
                    task_id,
                    session_id,
                    normalized,
                    broker,
                )
            )
        except Exception:
            if diagnostic is None:
                raise
            # The source object was schema-invalid. Do not rethrow into the
            # model runtime, which would enqueue that original unredacted body.
            logger.warning("Could not persist repaired runtime event", exc_info=True)
    return emitted


def _is_managed_staging_workspace(task_id: str, execution_root: Path) -> bool:
    try:
        managed_root = artifact_manager.get_staging_workspace(task_id).resolve()
        candidate = Path(execution_root).resolve()
    except (FileNotFoundError, OSError, RuntimeError, ValueError):
        return False
    return candidate == managed_root


def _audit_policy(request: PolicyRequest, decision) -> None:
    hierarchy_repository.append_audit_record(
        namespace=request.subject.namespace,
        actor_id=request.subject.actor_id,
        action=request.action.value,
        resource=request.resource,
        decision=decision.effect.value,
        reasons=decision.reasons,
        details={
            "method": request.method,
            "matched_rules": decision.matched_rules,
            "task_id": request.task_id,
        },
    )


api_policy = PolicyEngine(
    deployment=deployment_profile,
    audit_sink=_audit_policy,
)
runtime_observer = StructuredObserver(hierarchy_repository)
runtime_metrics = MetricsRegistry(runtime_observer)
_model_call_started_at: OrderedDict[tuple[str, str], float] = OrderedDict()
_model_call_metrics_lock = threading.RLock()
_MODEL_TIMING_MAX = max(
    1,
    int(os.environ.get("ORCH_MODEL_TIMING_MAX_ENTRIES", "2048")),
)
_MODEL_TIMING_TTL_SECONDS = max(
    1.0,
    float(os.environ.get("ORCH_MODEL_TIMING_TTL_SECONDS", "3600")),
)
llm_account_leases: SQLiteAccountLeaseStore | None = None
_llm_account_store_lock = threading.RLock()


def _ensure_account_lease_store() -> SQLiteAccountLeaseStore:
    global llm_account_leases
    with _llm_account_store_lock:
        if llm_account_leases is None:
            llm_account_leases = SQLiteAccountLeaseStore(
                os.environ.get(
                    "ORCH_ACCOUNT_LEASE_DB",
                    str(Path.home() / ".ai_orchestrator" / "account_leases.sqlite3"),
                )
            )
        llm_runtime.configure_account_lease_store(llm_account_leases)
        llm_runtime.configure_account_coordinator(llm_account_leases)
        return llm_account_leases


def _observe_runtime_event(
    task_id: str,
    session_id: str,
    event: dict,
    *,
    sequence: int,
) -> None:
    """Record correlated runtime metrics without putting IDs in labels."""

    try:
        event_type = str(event.get("type") or "event")
        call_id = str(event.get("call_id") or event.get("logical_request_id") or "") or None
        context = ObservationContext(
            task_id=task_id,
            session_id=session_id,
            agent_instance_id=(
                str(event.get("agent_instance_id")) if event.get("agent_instance_id") else None
            ),
            call_id=call_id,
            attempt_id=(str(event.get("attempt_id")) if event.get("attempt_id") else None),
        )
        component = (
            "llm"
            if event_type.startswith("model_request_")
            else "artifact"
            if event_type.startswith(("effect_", "artifact_"))
            else "scheduler"
            if event_type.startswith(("plan", "workstream_", "fanout_"))
            else "task"
            if event_type.startswith(("task", "hierarchy_", "completion_"))
            else "worker"
        )
        outcome = (
            "success"
            if event_type.endswith(("completed", "applied"))
            else "failure"
            if event_type.endswith(("failed", "lost"))
            else "aborted"
            if event_type.endswith("aborted")
            else "other"
        )
        runtime_metrics.increment(
            "orchestrator.event.total",
            labels={
                "component": component,
                "operation": "call" if event_type.startswith("model_request_") else "append",
                "outcome": outcome,
                "role": str(event.get("role") or "other"),
            },
            context=context,
            attributes={"event_type": event_type, "sequence": sequence},
            persist=event_type not in {"token", "thinking"},
        )
        now = time.monotonic()
        started_at = None
        with _model_call_metrics_lock:
            expired_before = now - _MODEL_TIMING_TTL_SECONDS
            while _model_call_started_at:
                _, oldest = next(iter(_model_call_started_at.items()))
                if oldest >= expired_before:
                    break
                _model_call_started_at.popitem(last=False)
            if call_id and event_type == "model_request_started":
                key = (task_id, call_id)
                _model_call_started_at.pop(key, None)
                _model_call_started_at[key] = now
                while len(_model_call_started_at) > _MODEL_TIMING_MAX:
                    _model_call_started_at.popitem(last=False)
            elif call_id and event_type in {
                "model_request_completed",
                "model_request_failed",
                "model_request_aborted",
            }:
                started_at = _model_call_started_at.pop((task_id, call_id), None)
        if call_id and event_type in {
            "model_request_completed",
            "model_request_failed",
            "model_request_aborted",
        }:
            duration_ms = max(0.0, (now - started_at) * 1000) if started_at is not None else None
            runtime_observer.emit(
                ObservabilityRecord(
                    kind="span",
                    name="llm.call",
                    context=context,
                    labels={
                        "component": "llm",
                        "operation": "call",
                        "outcome": outcome,
                        "provider": (
                            "web_claude"
                            if event.get("provider") == "legacy_web"
                            else str(event.get("provider") or "other")
                        ),
                        "role": str(event.get("role") or "other"),
                    },
                    attributes={"terminal_event": event_type},
                    duration_ms=duration_ms,
                )
            )
    except (RuntimeError, TypeError, ValueError):
        # Observability must never break orchestration.
        return


_PROJECT_ROOT = Path(__file__).resolve().parent
_PACKAGED_FRONTEND = _PROJECT_ROOT / "orchestrator" / "frontend_dist"
FRONTEND_DIST = (
    _PACKAGED_FRONTEND if _PACKAGED_FRONTEND.is_dir() else _PROJECT_ROOT / "frontend" / "dist"
)


def _is_symlink_or_reparse(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return True
    return bool(
        path.is_symlink()
        or int(getattr(metadata, "st_file_attributes", 0))
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _safe_frontend_root() -> Path | None:
    candidate = Path(FRONTEND_DIST)
    if not candidate.is_absolute():
        candidate = _PROJECT_ROOT / candidate
    try:
        relative = candidate.relative_to(_PROJECT_ROOT)
    except ValueError:
        return None
    current = _PROJECT_ROOT
    for part in relative.parts:
        current = current / part
        if not current.exists() or _is_symlink_or_reparse(current):
            return None
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(_PROJECT_ROOT)
    except (OSError, ValueError):
        return None
    return resolved if resolved.is_dir() else None


def _safe_frontend_file(relative_path: str) -> Path | None:
    root = _safe_frontend_root()
    if root is None:
        return None
    normalized = Path(relative_path.replace("\\", "/"))
    if normalized.is_absolute() or ".." in normalized.parts:
        return None
    current = root
    for part in normalized.parts:
        current = current / part
        if not current.exists() or _is_symlink_or_reparse(current):
            return None
    try:
        resolved = current.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        return None
    return resolved if resolved.is_file() else None


def _owns_startup_recovery() -> bool:
    return os.environ.get("ORCH_RECOVERY_OWNER_PID") == str(os.getpid())


@asynccontextmanager
async def lifespan(app: FastAPI):
    maintenance_stop = threading.Event()
    lifecycle = LifecycleCoordinator()
    if _owns_startup_recovery():
        task_manager.interrupt_active_tasks()

    def _auto_resume() -> None:
        for task in task_manager.list_auto_resumable_tasks():
            try:
                resume_task(task["id"])
            except Exception:
                # Startup must remain available even if one stale task cannot resume.
                task_manager.finish_task(
                    task["id"],
                    status="FAILED",
                    reason="auto_resume_startup_failed",
                )

    def _retention_maintenance() -> None:
        interval = max(
            300,
            int(os.environ.get("ORCH_RETENTION_INTERVAL_SECONDS", "21600")),
        )
        managed_retention = ManagedRetentionService(
            hierarchy_repository,
            {
                "log": Path(config.AGENT_LOG_DIR).expanduser().resolve(),
                "artifact": artifact_manager.ARTIFACTS_ROOT.resolve(),
            },
        )
        while not maintenance_stop.is_set():
            try:
                hierarchy_repository.compact_retention(RetentionPolicy.from_environment())
                managed_retention.run(limit=200)
            except (OSError, RuntimeError, ValueError):
                pass
            maintenance_stop.wait(interval)

    # Do not block accepting HTTP while stale tasks resume.
    auto_resume_thread = threading.Thread(
        target=_auto_resume,
        name="auto-resume",
        daemon=True,
    )
    retention_thread = threading.Thread(
        target=_retention_maintenance,
        name="retention-maintenance",
        daemon=True,
    )
    lifecycle.register_worker(auto_resume_thread)
    lifecycle.register_worker(retention_thread)
    auto_resume_thread.start()
    retention_thread.start()
    try:
        yield
    finally:
        maintenance_stop.set()
        runtime_registry.shutdown(timeout=10)
        lifecycle.shutdown(timeout=5)
        hierarchy_repository.close(timeout=5)
        global llm_account_leases
        with _llm_account_store_lock:
            if llm_account_leases is not None:
                llm_account_leases.close()
                llm_account_leases = None


def _configure_orchestrator_logging() -> None:
    """Send the run narration to the terminal.

    Uvicorn installs handlers only for its own loggers, so orchestrator INFO
    records were dropped and the terminal showed nothing but HTTP access lines
    while a whole hierarchy was executing.
    """
    level_name = os.environ.get("ORCH_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    orchestrator_logger = logging.getLogger("orchestrator")
    orchestrator_logger.setLevel(level)
    if not any(
        isinstance(handler, logging.StreamHandler) for handler in orchestrator_logger.handlers
    ):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
        orchestrator_logger.addHandler(handler)


_configure_orchestrator_logging()

app = FastAPI(lifespan=lifespan)
_SESSION_COOKIE = "orchestrator_session"
_SESSION_TTL_SECONDS = 12 * 60 * 60
_ALLOWED_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1", "testserver"}
_local_sessions: dict[str, tuple[str, float]] = {}
_local_sessions_lock = threading.RLock()


def _request_allowed_origins(request: Request) -> set[str]:
    host = str(request.headers.get("host") or "").strip()
    return {f"http://{host}", f"https://{host}"} if host else set()


def _request_host(request: Request) -> str:
    raw_host = str(request.headers.get("host") or "").strip()
    if not raw_host or any(character.isspace() for character in raw_host):
        return ""
    try:
        parsed = urlsplit(f"//{raw_host}")
    except ValueError:
        return ""
    if parsed.username is not None or parsed.password is not None:
        return ""
    return str(parsed.hostname or "").casefold()


def _loopback_peer(request: Request) -> bool:
    peer = str(request.client.host if request.client is not None else "").strip()
    # Starlette's in-process test transport has no network peer. It is safe to
    # recognize that exact sentinel while rejecting every routable hostname.
    if peer == "testclient":
        return True
    try:
        return ipaddress.ip_address(peer.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _add_security_headers(response: Response) -> Response:
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "base-uri 'none'; "
        "object-src 'none'; "
        "frame-ancestors 'none'; "
        "form-action 'self'; "
        "script-src 'self'; "
        "style-src 'self'; "
        "img-src 'self' data:; "
        "connect-src 'self'"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


def _denied_response(detail: str) -> Response:
    return _add_security_headers(
        Response(
            content=json.dumps({"detail": detail}),
            status_code=403,
            media_type="application/json",
        )
    )


def _valid_session(session_id: str | None) -> tuple[str, float] | None:
    if not session_id:
        return None
    now = time.time()
    with _local_sessions_lock:
        value = _local_sessions.get(session_id)
        if value is None:
            return None
        if value[1] <= now:
            _local_sessions.pop(session_id, None)
            return None
        return value


@app.middleware("http")
async def protect_local_api(request: Request, call_next):
    host = _request_host(request)
    if not _loopback_peer(request):
        return _denied_response("Request peer must be loopback.")
    if host not in _ALLOWED_LOCAL_HOSTS:
        return _denied_response("Host is not allowed.")

    if request.url.path.startswith("/api/"):
        session = _valid_session(request.cookies.get(_SESSION_COOKIE))
        csrf = str(request.headers.get("x-csrf-token") or "")
        decision = api_policy.evaluate(
            PolicyRequest(
                action=PolicyAction.API_REQUEST,
                resource=request.url.path,
                subject=PolicySubject(
                    actor_id=("local-session" if session is not None else "anonymous"),
                    roles=("operator",) if session is not None else (),
                    namespace=deployment_profile.namespace or "local",
                    authenticated=session is not None,
                ),
                method=request.method,
                host=host,
                origin=str(request.headers.get("origin") or "").rstrip("/"),
                allowed_hosts=frozenset(_ALLOWED_LOCAL_HOSTS),
                allowed_origins=frozenset(_request_allowed_origins(request)),
                session_valid=session is not None,
                csrf_valid=(session is not None and secrets.compare_digest(csrf, session[0])),
            )
        )
        if not decision.allowed:
            reason = decision.reasons[0] if decision.reasons else "policy_denied"
            detail = {
                "host_not_allowed": "Host is not allowed.",
                "origin_not_allowed": "Origin is not allowed.",
                "session_or_csrf_invalid": ("A valid local session and CSRF token are required."),
            }.get(reason, "Request is not allowed by policy.")
            return _denied_response(detail)
    return _add_security_headers(await call_next(request))


@app.get("/api/session")
def create_local_session(response: Response, request: Request):
    session_id = request.cookies.get(_SESSION_COOKIE)
    existing = _valid_session(session_id)
    if existing is None:
        session_id = secrets.token_urlsafe(32)
        csrf = secrets.token_urlsafe(32)
    else:
        csrf = existing[0]
    expires_at = time.time() + _SESSION_TTL_SECONDS
    with _local_sessions_lock:
        _local_sessions[str(session_id)] = (csrf, expires_at)
    response.set_cookie(
        key=_SESSION_COOKIE,
        value=str(session_id),
        max_age=_SESSION_TTL_SECONDS,
        httponly=True,
        secure=False,
        samesite="strict",
        path="/",
    )
    response.headers["Cache-Control"] = "no-store"
    return {
        "csrf_token": csrf,
        "expires_at": expires_at,
        "same_origin": True,
    }


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "version": __version__,
        "schema_version": CURRENT_SCHEMA_VERSION,
        "provider": "web_claude",
        "deployment_mode": deployment_profile.mode.value,
        "execution_boundary": deployment_profile.execution_boundary,
    }


@app.get("/assets/{asset_path:path}")
def get_frontend_asset(asset_path: str):
    if _safe_frontend_root() is None:
        raise HTTPException(status_code=503, detail="React frontend is unavailable.")
    asset = _safe_frontend_file(f"assets/{asset_path}")
    if asset is None:
        raise HTTPException(status_code=404, detail="Frontend asset does not exist.")
    return FileResponse(asset)


@app.get("/favicon.ico")
@app.get("/favicon.svg")
def get_favicon():
    if _safe_frontend_root() is None:
        raise HTTPException(status_code=503, detail="React frontend is unavailable.")
    for name in ("favicon.svg", "favicon.ico"):
        candidate = _safe_frontend_file(name)
        if candidate is not None:
            media = "image/svg+xml" if candidate.suffix == ".svg" else "image/x-icon"
            return FileResponse(candidate, media_type=media)
    raise HTTPException(status_code=404, detail="Frontend favicon does not exist.")


def get_current_queue():
    try:
        import orchestrator.llm_client as llm_client

        task_id = getattr(llm_client.thread_local, "task_id", None)
    except Exception:
        task_id = None
    runtime = runtime_registry.get(task_id) if task_id else None
    return runtime.broker if runtime is not None else None


def is_current_thread_stopped():
    try:
        import orchestrator.llm_client as llm_client

        task_id = getattr(llm_client.thread_local, "task_id", None)
    except Exception:
        task_id = None
    return bool(
        (task_id and runtime_registry.is_cancelled(str(task_id)))
        or runtime_registry.is_cancelled(threading.get_ident())
    )


def _split_windows_command_line(command: str) -> list[str]:
    """Parse a legacy command string using Windows C-runtime quoting rules."""
    arguments: list[str] = []
    length = len(command)
    position = 0
    while position < length:
        while position < length and command[position] in " \t":
            position += 1
        if position >= length:
            break

        argument: list[str] = []
        in_quotes = False
        while position < length:
            character = command[position]
            if character in " \t" and not in_quotes:
                break
            if character == "\\":
                slash_start = position
                while position < length and command[position] == "\\":
                    position += 1
                slash_count = position - slash_start
                if position < length and command[position] == '"':
                    argument.extend("\\" * (slash_count // 2))
                    if slash_count % 2:
                        argument.append('"')
                        position += 1
                    elif in_quotes and position + 1 < length and command[position + 1] == '"':
                        argument.append('"')
                        position += 2
                    else:
                        in_quotes = not in_quotes
                        position += 1
                else:
                    argument.extend("\\" * slash_count)
                continue
            if character == '"':
                if in_quotes and position + 1 < length and command[position + 1] == '"':
                    argument.append('"')
                    position += 2
                else:
                    in_quotes = not in_quotes
                    position += 1
                continue
            argument.append(character)
            position += 1
        arguments.append("".join(argument))
        while position < length and command[position] in " \t":
            position += 1
    return arguments


def _normalize_test_command(
    value: list[str] | str | None,
) -> list[str] | None:
    if value is None:
        return None
    arguments = _split_windows_command_line(value) if isinstance(value, str) else list(value)
    if not arguments:
        return None
    if not arguments[0]:
        raise ValueError("test_cmd executable must not be empty")
    if any("\x00" in argument for argument in arguments):
        raise ValueError("test_cmd arguments must not contain NUL characters")
    return arguments


class StrictRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TaskRequest(StrictRequestModel):
    name: str | None = None
    root: str
    task: str
    files: str = ""
    mode: str
    project_mode: str = "edit"
    approval_mode: Literal["manual", "staging_auto"] = "manual"
    auto_apply: bool = True
    create_zip: bool = True
    model: str = "claude-sonnet-5"
    effort: str = "max"
    supervisor_model: str = "claude-sonnet-5"
    supervisor_effort: str = "max"
    director_model: str | None = None
    director_effort: str | None = None
    manager_model: str | None = None
    manager_effort: str | None = None
    reviewer_model: str = "claude-sonnet-5"
    reviewer_effort: str = "high"
    account_mode: str = "sticky"  # Bổ sung tham số Sticky/Router
    test_cmd: list[str] | str | None = None
    max_turns: int = Field(default=config.MAX_TURNS, ge=1, le=500)
    auto_continue: bool = False
    hierarchy_enabled: bool = False
    # Planning caps are separate from execution concurrency.  Missing values
    # preserve the legacy behavior where the parallel setting was also the cap.
    max_managers: int | None = Field(default=None, ge=1, le=32)
    max_parallel_managers: int = Field(default=4, ge=1, le=32)
    # Total child agents for each Manager: N-1 Coders and one dedicated Tester.
    max_workers_per_manager: int = Field(default=5, ge=2, le=32)
    max_parallel_workers_per_manager: int | None = Field(default=None, ge=1, le=31)
    max_parallel_workers: int = Field(default=8, ge=1, le=64)


class ChatReplyRequest(StrictRequestModel):
    message: str


class AgentConfigRequest(StrictRequestModel):
    model: str
    effort: str


class ApprovalRequest(StrictRequestModel):
    idempotency_key: str = Field(min_length=1, max_length=256)
    kind: str = Field(min_length=1, max_length=80)
    target: str = Field(min_length=1, max_length=2000)
    reason: str = Field(min_length=1, max_length=4000)
    workstream_id: str | None = Field(default=None, max_length=256)
    payload: dict = Field(default_factory=dict)


class ApprovalDecisionRequest(StrictRequestModel):
    decision: str
    reason: str = Field(default="", max_length=4000)


class ArtifactRetentionRequest(StrictRequestModel):
    pinned: bool


ALLOWED_AGENT_MODELS = {"claude-sonnet-5", "claude-sonnet-4-6"}
ALLOWED_AGENT_EFFORTS = {"low", "medium", "high", "max", "xhigh"}


@app.get("/")
def get_ui():
    workspace = _safe_frontend_file("index.html")
    if workspace is None:
        raise HTTPException(status_code=503, detail="React frontend is unavailable.")
    return FileResponse(workspace, media_type="text/html")


@app.get("/api/pick-files")
def pick_files():
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        file_paths = filedialog.askopenfilenames(title="Chọn các file Code cần xử lý")
        root.destroy()
        if file_paths:
            import os

            paths = [Path(p) for p in file_paths]
            common_root = os.path.commonpath([p.parent for p in paths])
            rel_files = [str(p.relative_to(common_root)).replace("\\", "/") for p in paths]
            return {"root": str(common_root).replace("\\", "/"), "files": ",".join(rel_files)}
        return {"root": "", "files": ""}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/pick-folder")
def pick_folder():
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        folder = filedialog.askdirectory(title="Chọn thư mục project")
        root.destroy()
        return {"root": str(Path(folder).resolve()) if folder else ""}
    except Exception as e:
        return {"error": str(e)}


# ENDPOINT ĐỂ NHẬN TIN NHẮN CHAT TIẾP THEO
@app.post("/api/chat/{task_id}")
def chat_reply(task_id: str, req: ChatReplyRequest):
    runtime = runtime_registry.get(task_id)
    if runtime is not None and runtime.chat_queue is not None:
        runtime.chat_queue.put(req.message)
        return {"status": "sent"}
    return {"error": "Luồng chat không tồn tại hoặc đã đóng"}


@app.post("/api/run")
def run_task(req: TaskRequest):
    return _start_task(req)


def _start_task(req: TaskRequest, resume_task_id: str | None = None):
    is_resume = resume_task_id is not None
    if req.approval_mode == "staging_auto" and (
        req.mode != "orchestrator" or req.project_mode != "new_project"
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "approval_mode=staging_auto chỉ được dùng cho "
                "Orchestrator new_project trong staging được quản lý."
            ),
        )
    manager_cap = req.max_managers or req.max_parallel_managers
    if req.max_parallel_managers > manager_cap:
        raise HTTPException(
            status_code=400,
            detail="max_parallel_managers không được vượt max_managers.",
        )
    coder_cap = max(1, req.max_workers_per_manager - 1)
    if (
        req.max_parallel_workers_per_manager is not None
        and req.max_parallel_workers_per_manager > coder_cap
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "max_parallel_workers_per_manager không được vượt số Coder "
                "tối đa (max_workers_per_manager - 1 Tester)."
            ),
        )
    raw_root = req.root.strip()
    if req.mode == "orchestrator" and not raw_root:
        raise HTTPException(
            status_code=400,
            detail="Orchestrator mode yêu cầu project root.",
        )
    root_path = Path(raw_root).resolve() if raw_root else Path.cwd().resolve()
    if req.mode == "orchestrator" and req.project_mode == "new_project" and not is_resume:
        if root_path.exists() and not root_path.is_dir():
            raise HTTPException(status_code=400, detail="Destination phải là thư mục.")
        if root_path.exists() and any(root_path.iterdir()):
            raise HTTPException(
                status_code=400,
                detail="New Project yêu cầu thư mục đích trống.",
            )
        if not root_path.parent.is_dir():
            raise HTTPException(
                status_code=400,
                detail="Thư mục cha của destination không tồn tại.",
            )
    elif not root_path.is_dir() and not (
        is_resume and req.mode == "orchestrator" and req.project_mode == "new_project"
    ):
        raise HTTPException(
            status_code=400, detail="Project root không tồn tại hoặc không phải thư mục."
        )
    if (
        req.mode == "orchestrator"
        and req.hierarchy_enabled
        and req.project_mode == "edit"
        and not is_resume
        and root_path.is_dir()
        and not any(root_path.iterdir())
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Thư mục project đang trống. Hãy chọn 'Create new project' "
                "để hệ thống được phép tạo file mới."
            ),
        )

    files_list = []
    try:
        for item in (f.strip() for f in req.files.split(",") if f.strip()):
            parts = item.split(":", 1)
            path_utils.ensure_context_path_safe(parts[0])
            normalized, _ = path_utils.resolve_under_root(root_path, parts[0])
            files_list.append(f"{normalized}:{parts[1]}" if len(parts) > 1 else normalized)
    except (path_utils.PathEscapeError, path_utils.SensitivePathError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        parsed_test_cmd = _normalize_test_command(req.test_cmd)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    task_id = resume_task_id or str(uuid.uuid4())
    initial_sequence = hierarchy_repository.latest_event_sequence(task_id) if is_resume else 0
    try:
        runtime = runtime_registry.reserve(
            task_id,
            initial_sequence=initial_sequence,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    q = runtime.broker
    try:
        if is_resume:
            task_manager.resume_task(task_id)
        else:
            task_manager.create_task(
                task_id,
                name=req.name or req.task[:60],
                mode=req.mode,
                prompt=req.task,
                root=str(root_path) if raw_root else "",
                files=files_list,
                settings={
                    "worker_model": req.model,
                    "worker_effort": req.effort,
                    "supervisor_model": req.supervisor_model,
                    "supervisor_effort": req.supervisor_effort,
                    "director_model": req.director_model or req.supervisor_model,
                    "director_effort": req.director_effort or req.supervisor_effort,
                    "manager_model": req.manager_model or req.supervisor_model,
                    "manager_effort": req.manager_effort or req.supervisor_effort,
                    "reviewer_model": req.reviewer_model,
                    "reviewer_effort": req.reviewer_effort,
                    "account_mode": req.account_mode,
                    "test_cmd": parsed_test_cmd,
                    "project_mode": req.project_mode,
                    "approval_mode": req.approval_mode,
                    "auto_apply": req.auto_apply,
                    "create_zip": req.create_zip,
                    "max_turns": req.max_turns,
                    "auto_continue": req.auto_continue,
                    "hierarchy_enabled": req.hierarchy_enabled,
                    "max_managers": manager_cap,
                    "max_parallel_managers": req.max_parallel_managers,
                    "max_workers_per_manager": req.max_workers_per_manager,
                    "max_parallel_workers_per_manager": (req.max_parallel_workers_per_manager),
                    "max_parallel_workers": req.max_parallel_workers,
                },
            )
    except Exception:
        runtime_registry.complete(runtime, timeout=0)
        raise

    def background_worker(
        t_id,
        root,
        files,
        task_desc,
        mode,
        model,
        effort,
        supervisor_model,
        supervisor_effort,
        reviewer_model,
        reviewer_effort,
        project_mode,
        auto_apply,
        create_zip,
        account_mode,
        test_cmd,
        max_turns,
        auto_continue,
        resume_session,
        q,
        hierarchy_enabled,
        director_model,
        director_effort,
        manager_model,
        manager_effort,
        max_managers,
        max_parallel_managers,
        max_workers_per_manager,
        max_parallel_workers_per_manager,
        max_parallel_workers,
        approval_mode,
    ):
        task_runtime = runtime_registry.get(t_id)
        if task_runtime is None or task_runtime.broker is not q:
            return

        session_holder = {"id": f"legacy-{t_id}"}
        event_emit_lock = threading.RLock()
        project_lease = None
        lease_heartbeat_stop = threading.Event()
        lease_heartbeat_thread = None

        def emit(event):
            with event_emit_lock:
                _emit_persisted_runtime_event(
                    t_id,
                    session_holder,
                    q,
                    event,
                )

        try:
            import orchestrator.llm_client as llm_client

            llm_client.thread_local.model = model
            llm_client.thread_local.effort = effort
            llm_client.thread_local.worker_model = model
            llm_client.thread_local.worker_effort = effort
            llm_client.thread_local.supervisor_model = supervisor_model
            llm_client.thread_local.supervisor_effort = supervisor_effort
            llm_client.thread_local.reviewer_model = reviewer_model
            llm_client.thread_local.reviewer_effort = reviewer_effort
            llm_client.thread_local.director_model = director_model
            llm_client.thread_local.director_effort = director_effort
            llm_client.thread_local.manager_model = manager_model
            llm_client.thread_local.manager_effort = manager_effort
            llm_client.thread_local.account_mode = account_mode
            llm_client.thread_local.event_sink = emit
            llm_client.thread_local.task_id = t_id
            llm_client.thread_local.abort_check = task_runtime.cancellation.is_set
        except Exception:
            pass

        try:
            _ensure_account_lease_store()
            emit(
                {
                    "type": "status",
                    "data": (
                        (
                            f"🚀 Hierarchy | Director: {director_model}/{director_effort} "
                            f"| Manager: {manager_model}/{manager_effort} "
                            if hierarchy_enabled
                            else f"🚀 Task {t_id} | Supervisor: {supervisor_model}/{supervisor_effort} "
                        )
                        + f"| Worker: {model}/{effort} | Tester: {reviewer_model}/{reviewer_effort}"
                    ),
                }
            )

            if mode == "orchestrator":
                llm_client.thread_local.is_continuation = False
                execution_root = root
                effective_task = task_desc
                if project_mode == "new_project":
                    execution_root = (
                        artifact_manager.get_staging_workspace(t_id)
                        if resume_session
                        else artifact_manager.create_workspace(
                            t_id,
                            root,
                            auto_apply=auto_apply,
                            create_zip=create_zip,
                        )
                    )
                    effective_task = (
                        "NEW PROJECT MODE: Xây dựng project hoàn toàn mới trong staging. "
                        "Hãy tự thiết kế bộ khung và delegate trực tiếp từng file mới; "
                        "backend đã cho phép tạo file trong staging nên KHÔNG request_context "
                        "lặp lại cho file chưa tồn tại. Đặt is_final_ticket=false "
                        "cho đến file cuối cùng. Không sửa state/rules/DECISIONS.\n\n"
                        f"YÊU CẦU USER:\n{task_desc}"
                    )
                else:
                    effective_task = (
                        "EDIT MODE (FOLDER-ONLY): User chỉ chọn thư mục gốc. "
                        "PROJECT TREE liệt kê path trên máy. Khi cần nội dung file, "
                        "dùng request_context; hoặc delegate_task trực tiếp nếu path "
                        "đã tồn tại — local file agent sẽ nạp file từ đĩa. "
                        "Không yêu cầu User chọn từng file trước.\n\n"
                        f"YÊU CẦU USER:\n{task_desc}"
                    )
                project_key = str(
                    Path(root if project_mode == "new_project" else execution_root).resolve()
                ).casefold()
                project_lease = ProjectLeaseManager(hierarchy_repository).acquire(
                    project_key,
                    t_id,
                    90,
                    purpose=f"{project_mode}:{'hierarchy' if hierarchy_enabled else 'legacy'}",
                )
                if project_lease is None:
                    raise RuntimeError("Project is already leased by another active task.")
                emit(
                    {
                        "type": "project_lease_acquired",
                        "project_key": project_key,
                        "fencing_token": project_lease.fencing_token,
                        "isolation_level": "fenced-local-workspace",
                    }
                )

                def heartbeat_project_lease():
                    while not lease_heartbeat_stop.wait(30):
                        try:
                            project_lease.heartbeat(90)
                        except ProjectLeaseLostError as exc:
                            task_runtime.cancellation.set()
                            emit(
                                {
                                    "type": "project_lease_lost",
                                    "status": "failed",
                                    "error": str(exc),
                                }
                            )
                            return

                lease_heartbeat_thread = _NATIVE_THREAD(
                    target=heartbeat_project_lease,
                    name=f"project-lease-{t_id[:8]}",
                    daemon=False,
                )
                runtime_registry.add_heartbeat(
                    t_id,
                    lease_heartbeat_thread,
                )
                lease_heartbeat_thread.start()
                approved_file_callback = None
                if project_mode == "new_project":

                    def approved_file_callback(file_path):
                        manifest_before = artifact_manager.get_manifest(t_id) or {}
                        source = artifact_manager.get_staging_workspace(t_id) / file_path
                        source_hash = sha256_file(source)
                        destination = Path(manifest_before.get("destination") or root).resolve()
                        _, destination_file = path_utils.resolve_under_root(destination, file_path)
                        before_hash = sha256_file(destination_file)
                        effect = hierarchy_repository.begin_effect(
                            t_id,
                            f"artifact:{file_path}:{source_hash or 'missing'}",
                            "artifact_materialize",
                            file_path,
                            payload={
                                "source_sha256": source_hash,
                                "auto_apply": bool(manifest_before.get("auto_apply", True)),
                            },
                            before_sha256=before_hash,
                        )
                        try:
                            progress_manifest = project_lease.mutate(
                                lambda: artifact_manager.materialize_approved_file(t_id, file_path)
                            )
                            file_metadata = next(
                                (
                                    item
                                    for item in progress_manifest.get("files", [])
                                    if item.get("path") == file_path
                                ),
                                {},
                            )
                            after_hash = file_metadata.get("after_sha256") or file_metadata.get(
                                "sha256"
                            )
                            effect = hierarchy_repository.complete_effect(
                                effect.effect_id,
                                result={
                                    "status": progress_manifest.get("status"),
                                    "approved": True,
                                },
                                after_sha256=after_hash,
                            )
                        except Exception as exc:
                            if effect.state.value == "pending":
                                hierarchy_repository.fail_effect(
                                    effect.effect_id,
                                    f"{type(exc).__name__}: {exc}",
                                )
                            raise
                        task_manager.set_artifact(t_id, progress_manifest)
                        emit(
                            {
                                "type": "artifact_progress",
                                "file_path": file_path,
                                "status": progress_manifest["status"],
                                "files": progress_manifest["files"],
                                "effect_id": effect.effect_id,
                                "before_sha256": effect.before_sha256,
                                "after_sha256": effect.after_sha256,
                            }
                        )

                def wait_for_approval(candidate: dict) -> bool:
                    approval = hierarchy_repository.request_approval(
                        t_id,
                        idempotency_key=(
                            f"task:{t_id}:patch:"
                            f"{candidate.get('work_item_id')}:"
                            f"{candidate.get('target')}:"
                            f"{candidate.get('patch_sha256')}"
                        ),
                        kind=str(candidate.get("kind") or "patch_apply"),
                        target=str(candidate.get("target") or "unknown"),
                        reason=str(candidate.get("reason") or "Approval required"),
                        payload={
                            "patch_sha256": candidate.get("patch_sha256"),
                            "additions": candidate.get("additions"),
                            "deletions": candidate.get("deletions"),
                            "work_item_id": candidate.get("work_item_id"),
                            "attempt_id": candidate.get("attempt_id"),
                        },
                        workstream_id=str(candidate.get("workstream_id") or "") or None,
                    )
                    if approval["status"] == "pending":
                        emit(
                            {
                                "type": "approval_requested",
                                "approval_id": approval["approval_id"],
                                "workstream_id": approval["workstream_id"],
                                "kind": approval["kind"],
                                "target": approval["target"],
                                "reason": approval["reason"],
                                "status": approval["status"],
                                "approval_mode": approval_mode,
                            }
                        )
                        if approval_mode == "staging_auto":
                            confined = (
                                project_mode == "new_project"
                                and _is_managed_staging_workspace(t_id, execution_root)
                            )
                            decision = "approved" if confined else "rejected"
                            audit_reason = (
                                "staging_auto approved this request because the task is "
                                "new_project and execution remains confined to the "
                                "orchestrator-managed staging workspace."
                                if confined
                                else (
                                    "staging_auto rejected this request because execution "
                                    "is not confined to the orchestrator-managed staging "
                                    "workspace."
                                )
                            )
                            approval = hierarchy_repository.decide_approval(
                                approval["approval_id"],
                                decision=decision,
                                reason=audit_reason,
                            )
                            emit(
                                {
                                    "type": "approval_decided",
                                    "approval_id": approval["approval_id"],
                                    "workstream_id": approval["workstream_id"],
                                    "decision": approval["status"],
                                    "reason": approval["decision_reason"],
                                    "approval_mode": approval_mode,
                                    "automated": True,
                                }
                            )
                            return approval["status"] == "approved"
                        task_manager.set_status(
                            t_id,
                            "WAITING_INPUT",
                            "approval",
                        )
                    while approval["status"] == "pending":
                        if task_runtime.cancellation.is_set():
                            return False
                        time.sleep(0.5)
                        matches = [
                            item
                            for item in hierarchy_repository.list_approvals(t_id)
                            if item["approval_id"] == approval["approval_id"]
                        ]
                        if not matches:
                            return False
                        approval = matches[0]
                    task_manager.set_status(t_id, "RUNNING", "execution")
                    return approval["status"] == "approved"

                if hierarchy_enabled:
                    result = run_hierarchy(
                        root=execution_root,
                        task_description=effective_task,
                        task_id=t_id,
                        source_files=files,
                        test_cmd=test_cmd,
                        allow_new_files=project_mode == "new_project",
                        limits=SchedulerLimits(
                            max_managers=max_managers,
                            max_parallel_managers=max_parallel_managers,
                            max_workers_per_manager=max_workers_per_manager,
                            max_parallel_workers_per_manager=(max_parallel_workers_per_manager),
                            max_parallel_workers=max_parallel_workers,
                        ),
                        director_model=director_model,
                        director_effort=director_effort,
                        manager_model=manager_model,
                        manager_effort=manager_effort,
                        worker_model=model,
                        worker_effort=effort,
                        reviewer_model=reviewer_model,
                        reviewer_effort=reviewer_effort,
                        on_event=emit,
                        on_file_approved=approved_file_callback,
                        repository=hierarchy_repository,
                        project_lease=project_lease,
                        resume_session=resume_session,
                        agent_config_resolver=(
                            lambda agent_id, role, default_model, default_effort: (
                                (task_manager.get_agent_override(t_id, agent_id) or {}).get(
                                    "model", default_model
                                ),
                                (task_manager.get_agent_override(t_id, agent_id) or {}).get(
                                    "effort", default_effort
                                ),
                            )
                        ),
                        approval_callback=wait_for_approval,
                        cancelled=task_runtime.cancellation.is_set,
                    )
                else:
                    resume_cycle = resume_session
                    auto_cycles = 0
                    while True:
                        result = run_session(
                            root=execution_root,
                            task_description=effective_task,
                            source_files=files,
                            test_cmd=test_cmd,
                            max_turns=max_turns,
                            allow_new_files=project_mode == "new_project",
                            resume_session=resume_cycle,
                            on_event=emit,
                            on_file_approved=approved_file_callback,
                            cancelled=task_runtime.cancellation.is_set,
                        )
                        if (
                            result.stopped_reason == "max_turns_reached"
                            and auto_continue
                            and not task_runtime.cancellation.is_set()
                            and auto_cycles < config.AUTO_CONTINUE_MAX_CYCLES
                        ):
                            auto_cycles += 1
                            resume_cycle = True
                            emit(
                                {
                                    "type": "auto_continue",
                                    "cycle": auto_cycles,
                                    "turn_count": result.final_state.get("turn_count", 0),
                                }
                            )
                            continue
                        break
                if task_runtime.cancellation.is_set():
                    emit({"type": "error", "data": "🛑 Đã hủy thao tác do có lệnh ép dừng!"})
                    task_manager.finish_task(t_id, status="STOPPED", reason="user_stopped")
                else:
                    artifact_manifest = None
                    if project_mode == "new_project" and result.stopped_reason == "task_completed":
                        finalize_effect = hierarchy_repository.begin_effect(
                            t_id,
                            "artifact-finalize:v1",
                            "artifact_finalize",
                            str(root),
                            payload={
                                "create_zip": create_zip,
                                "auto_apply": auto_apply,
                            },
                        )
                        if finalize_effect.state.value in {
                            "applied",
                            "reconciled",
                        }:
                            artifact_manifest = artifact_manager.get_manifest(t_id)
                            if artifact_manifest is None:
                                raise RuntimeError("Finalized artifact receipt has no manifest")
                        else:
                            try:
                                artifact_manifest = project_lease.mutate(
                                    lambda: artifact_manager.finalize_workspace(t_id)
                                )
                                finalize_effect = hierarchy_repository.complete_effect(
                                    finalize_effect.effect_id,
                                    result={
                                        "status": artifact_manifest.get("status"),
                                        "file_count": len(artifact_manifest.get("files", [])),
                                        "zip_created": bool(artifact_manifest.get("zip_path")),
                                    },
                                )
                            except Exception as exc:
                                hierarchy_repository.fail_effect(
                                    finalize_effect.effect_id,
                                    f"{type(exc).__name__}: {exc}",
                                )
                                raise
                        task_manager.set_artifact(t_id, artifact_manifest)
                        emit(
                            {
                                "type": "artifact_ready",
                                "status": artifact_manifest["status"],
                                "files": artifact_manifest["files"],
                                "effect_id": finalize_effect.effect_id,
                                "download_url": (
                                    f"/api/tasks/{t_id}/artifacts/download"
                                    if artifact_manifest.get("zip_path")
                                    else None
                                ),
                            }
                        )
                    finish_payload = {
                        "type": "finish",
                        "reason": result.stopped_reason,
                        "turns": [
                            {
                                "tool": t.tool_name,
                                "detail": t.detail,
                                "accepted": t.accepted,
                            }
                            for t in result.turns
                        ],
                        "final_state": {
                            "last_worker_feedback": result.final_state.get(
                                "last_worker_feedback", ""
                            ),
                            "last_execution_result": result.final_state.get(
                                "last_execution_result"
                            ),
                            "last_reviewer_feedback": result.final_state.get(
                                "last_reviewer_feedback", ""
                            ),
                            "last_review_verdict": result.final_state.get("last_review_verdict"),
                            "reviewer_next_instructions": result.final_state.get(
                                "reviewer_next_instructions", ""
                            ),
                            "turn_count": result.final_state.get("turn_count", 0),
                        },
                        "artifact": artifact_manifest,
                    }
                    emit(finish_payload)
                    task_manager.finish_task(
                        t_id,
                        status=(
                            "COMPLETED"
                            if result.stopped_reason == "task_completed"
                            else (
                                "MAX_TURNS"
                                if result.stopped_reason == "max_turns_reached"
                                else "PARTIAL"
                                if result.stopped_reason == "task_partial"
                                else "STOPPED"
                                if result.stopped_reason == "cancelled"
                                else "FAILED"
                            )
                        ),
                        reason=result.stopped_reason,
                        final_state=result.final_state,
                    )
            else:
                # --- CHẾ ĐỘ CHAT ---
                # Chat dùng cặp model/effort chính và không cần project context.
                llm_client.thread_local.agent_role = "worker"
                llm_client.thread_local.worker_model = model
                llm_client.thread_local.worker_effort = effort
                task_runtime.chat_queue = queue.Queue()
                chat_history = []

                context_blocks = []
                for f in files:
                    rel_path = f.split(":", 1)[0]
                    _, file_path = path_utils.resolve_under_root(root, rel_path)
                    if file_path.exists():
                        content = file_path.read_text(encoding="utf-8", errors="replace")
                        context_blocks.append(f"### FILE: {f}\n```\n{content}\n```")

                if context_blocks:
                    current_msg = f"Yêu cầu ban đầu:\n{task_desc}\n\nNội dung code:\n" + "\n".join(
                        context_blocks
                    )
                else:
                    current_msg = task_desc

                is_first_turn = True

                # Vòng lặp chat vô tận cho đến khi bị ép dừng
                while not task_runtime.cancellation.is_set():
                    # NẾU ROUTER: Bắt buộc nhét tay mớ History vào mỗi lần gọi.
                    if account_mode == "router":
                        if is_first_turn:
                            full_prompt = current_msg
                        else:
                            full_prompt = "\n\n".join(chat_history) + f"\n\nUser: {current_msg}"
                        llm_client.thread_local.is_continuation = False
                    # NẾU STICKY: Chỉ ném tin nhắn mới, Claude API tự nối chat_uuid!
                    else:
                        full_prompt = current_msg
                        llm_client.thread_local.is_continuation = not is_first_turn

                    result = call_agent(
                        "Bạn là một trợ lý AI thông minh.",
                        full_prompt,
                        tools=[],
                        require_json=False,
                    )

                    if task_runtime.cancellation.is_set():
                        emit({"type": "error", "data": "🛑 Luồng chat bị ép dừng!"})
                        break

                    answer = result.raw_response.get("content", "")

                    # Lưu lại lịch sử
                    chat_history.append(f"User: {current_msg}")
                    chat_history.append(f"Assistant: {answer}")
                    is_first_turn = False

                    # Báo cho UI là xong 1 turn, chuẩn bị nhận tiếp
                    emit({"type": "finish_chat_turn"})
                    emit({"type": "status", "data": "⏳ Đang chờ tin nhắn tiếp theo..."})

                    # Treo luồng chờ người dùng gõ tin nhắn mới
                    next_msg = None
                    while not task_runtime.cancellation.is_set():
                        try:
                            assert task_runtime.chat_queue is not None
                            next_msg = task_runtime.chat_queue.get(timeout=1)
                            break
                        except queue.Empty:
                            continue

                    if task_runtime.cancellation.is_set():
                        break

                    if next_msg:
                        current_msg = next_msg

                # Dọn dẹp hàng đợi khi thoát
                task_runtime.chat_queue = None
                task_manager.finish_task(
                    t_id,
                    status="STOPPED",
                    reason="chat_closed",
                )

        except Exception as e:
            emit({"type": "error", "data": str(e)})
            task_manager.finish_task(t_id, status="FAILED", reason=str(e))
        finally:
            lease_heartbeat_stop.set()
            if lease_heartbeat_thread is not None:
                lease_heartbeat_thread.join(timeout=2)
            if project_lease is not None:
                try:
                    project_lease.release()
                except Exception:
                    pass
            try:
                emit({"type": "done"})
            finally:
                task_runtime.chat_queue = None
                runtime_registry.complete(task_runtime, timeout=5)

    worker_thread = threading.Thread(
        target=background_worker,
        args=(
            task_id,
            root_path,
            files_list,
            req.task,
            req.mode,
            req.model,
            req.effort,
            req.supervisor_model,
            req.supervisor_effort,
            req.reviewer_model,
            req.reviewer_effort,
            req.project_mode,
            req.auto_apply,
            req.create_zip,
            req.account_mode,
            parsed_test_cmd,
            req.max_turns,
            req.auto_continue,
            is_resume,
            q,
            req.hierarchy_enabled,
            req.director_model or req.supervisor_model,
            req.director_effort or req.supervisor_effort,
            req.manager_model or req.supervisor_model,
            req.manager_effort or req.supervisor_effort,
            manager_cap,
            req.max_parallel_managers,
            req.max_workers_per_manager,
            req.max_parallel_workers_per_manager,
            req.max_parallel_workers,
            req.approval_mode,
        ),
        daemon=False,
    )
    try:
        runtime_registry.bind_worker(task_id, worker_thread)
        worker_thread.start()
    except Exception:
        runtime_registry.complete(runtime, timeout=0)
        raise
    return {"status": "started", "task_id": task_id}


@app.get("/api/tasks")
def list_tasks():
    return {"tasks": task_manager.list_tasks()}


@app.get("/api/tasks/{task_id}")
def get_task(task_id: str):
    task = task_manager.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return task


def _event_wire(event: EventEnvelope) -> dict:
    data = to_dict(event)
    payload = dict(data.pop("payload", {}))
    return {
        **payload,
        **data,
        "type": event.event_type,
        "sequence": event.sequence,
    }


@app.get("/api/tasks/{task_id}/plan")
def get_task_plan(task_id: str):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    plan = hierarchy_repository.get_plan(task_id)
    return {"plan": to_dict(plan) if plan is not None else None}


@app.get("/api/tasks/{task_id}/timeline")
def get_task_timeline(task_id: str, after: int = 0, limit: int = 1000):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    safe_limit = max(1, min(limit, 5000))
    events = hierarchy_repository.replay_events(
        task_id,
        after_sequence=max(0, after),
        limit=safe_limit,
    )
    cursor = hierarchy_repository.event_cursor(task_id)
    latest_sequence = int(cursor.get("latest_sequence") or 0)
    retained_from_sequence = int(cursor.get("retained_from_sequence") or 1)
    next_after = events[-1].sequence if events else max(0, after)
    return {
        "events": [_event_wire(event) for event in events],
        "next_after": next_after,
        "latest_sequence": latest_sequence,
        "retained_from_sequence": retained_from_sequence,
        "has_more": next_after < latest_sequence,
        "history_incomplete": max(0, after) < retained_from_sequence - 1,
    }


@app.get("/api/tasks/{task_id}/attempts")
def get_task_attempts(task_id: str, work_item_id: str | None = None):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return {
        "attempts": [
            to_dict(item)
            for item in hierarchy_repository.list_attempts(
                task_id,
                work_item_id,
            )
        ]
    }


@app.get("/api/tasks/{task_id}/request-attempts")
def get_task_request_attempts(
    task_id: str,
    limit: int = 100,
    schema_errors_only: bool = False,
    include_content: bool = False,
):
    """Return sanitized provider-attempt evidence; large bodies are opt-in."""
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="limit phải từ 1 đến 1000.")
    attempts = hierarchy_repository.list_llm_request_attempts(
        task_id,
        schema_errors_only=schema_errors_only,
        limit=limit,
    )
    if not include_content:
        content_fields = {
            "logical_request",
            "tool_schema",
            "wire_body",
            "response_body",
            "parser_result",
        }
        attempts = [
            {key: value for key, value in attempt.items() if key not in content_fields}
            for attempt in attempts
        ]
    return {"attempts": attempts}


@app.get("/api/tasks/{task_id}/effects")
def get_task_effects(task_id: str):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return {"effects": [to_dict(item) for item in hierarchy_repository.list_effects(task_id)]}


@app.get("/api/tasks/{task_id}/observability")
def get_task_observability(task_id: str, limit: int = 1000):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    if limit < 1 or limit > 5000:
        raise HTTPException(status_code=400, detail="limit phải từ 1 đến 5000.")
    return {
        "records": hierarchy_repository.list_observability(
            task_id,
            limit=limit,
        )
    }


def _record_control_event(task_id: str, event_type: str, payload: dict) -> dict:
    envelope = EventEnvelope(
        task_id=task_id,
        session_id=f"control-{task_id}",
        event_type=event_type,
        version=EVENT_SCHEMA_VERSION,
        payload=payload,
    )
    stored = task_manager.record_event(
        task_id,
        {"type": event_type, **payload},
        envelope=envelope,
    )
    if stored is None:
        return {"type": event_type, **payload}
    wire = _event_wire(stored)
    runtime = runtime_registry.get(task_id)
    if runtime is not None:
        try:
            runtime.broker.put(wire)
        except RuntimeError:
            if not runtime.broker.closed:
                raise
    return wire


@app.get("/api/tasks/{task_id}/approvals")
def get_task_approvals(task_id: str, status: str | None = None):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return {
        "approvals": hierarchy_repository.list_approvals(
            task_id,
            status=status,
        )
    }


@app.post("/api/tasks/{task_id}/approvals")
def request_task_approval(task_id: str, request: ApprovalRequest):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    approval = hierarchy_repository.request_approval(
        task_id,
        idempotency_key=request.idempotency_key,
        kind=request.kind,
        target=request.target,
        reason=request.reason,
        payload=request.payload,
        workstream_id=request.workstream_id,
    )
    _record_control_event(
        task_id,
        "approval_requested",
        {
            "approval_id": approval["approval_id"],
            "workstream_id": approval["workstream_id"],
            "kind": approval["kind"],
            "target": approval["target"],
            "reason": approval["reason"],
            "status": approval["status"],
        },
    )
    return approval


@app.post("/api/tasks/{task_id}/approvals/{approval_id}/decision")
def decide_task_approval(
    task_id: str,
    approval_id: str,
    request: ApprovalDecisionRequest,
):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    if not any(
        approval["approval_id"] == approval_id
        for approval in hierarchy_repository.list_approvals(task_id)
    ):
        raise HTTPException(status_code=404, detail="Approval không tồn tại.")
    try:
        approval = hierarchy_repository.decide_approval(
            approval_id,
            decision=request.decision,
            reason=request.reason,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Approval không tồn tại.") from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _record_control_event(
        task_id,
        "approval_decided",
        {
            "approval_id": approval["approval_id"],
            "workstream_id": approval["workstream_id"],
            "decision": approval["status"],
            "reason": approval["decision_reason"],
        },
    )
    return approval


@app.get("/api/accounts")
def get_account_health():
    store = _ensure_account_lease_store()
    return {
        "accounts": store.list_health(now=time.time()),
        "active_leases": store.list_active(now=time.time()),
    }


@app.delete("/api/accounts/credentials/{source}")
def delete_account_credential(source: str):
    """Explicit operator-only deletion for a local credential file."""
    manager = llm_runtime.cookie_manager
    if manager is None:
        manager = llm_runtime.CookieManager()
        llm_runtime.cookie_manager = manager
    try:
        deleted = manager.delete_credential(source)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="Credential does not exist.")
    return {"status": "deleted", "source": source}


@app.put("/api/tasks/{task_id}/agents/{agent_id}/config")
def update_agent_config(
    task_id: str,
    agent_id: str,
    request: AgentConfigRequest,
):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    if request.model not in ALLOWED_AGENT_MODELS:
        raise HTTPException(status_code=400, detail="Model không được hỗ trợ.")
    if request.effort not in ALLOWED_AGENT_EFFORTS:
        raise HTTPException(status_code=400, detail="Effort không được hỗ trợ.")
    value = task_manager.set_agent_override(
        task_id,
        agent_id,
        model=request.model,
        effort=request.effort,
    )
    return {
        "status": "saved",
        "agent_id": agent_id,
        "applies_to": "next_model_call",
        **value,
    }


@app.post("/api/tasks/{task_id}/resume")
def resume_task(task_id: str):
    task = task_manager.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    if task.get("mode") != "orchestrator":
        raise HTTPException(
            status_code=400,
            detail="Resume bền vững hiện chỉ hỗ trợ Orchestrator task.",
        )
    resumable_statuses = {
        "STOPPED",
        "FAILED",
        "PARTIAL",
        "MAX_TURNS",
        "INTERRUPTED",
    }
    if task.get("status") not in resumable_statuses:
        raise HTTPException(
            status_code=409,
            detail=f"Task trạng thái {task.get('status')} không thể tiếp tục.",
        )
    settings = task.get("settings") or {}
    request = TaskRequest(
        name=task.get("name"),
        root=task.get("root", ""),
        task=task.get("prompt", ""),
        files=",".join(task.get("files") or []),
        mode=task.get("mode", "orchestrator"),
        project_mode=settings.get("project_mode", task.get("project_mode", "edit")),
        approval_mode=settings.get("approval_mode", "manual"),
        auto_apply=settings.get("auto_apply", True),
        create_zip=settings.get("create_zip", True),
        model=settings.get("worker_model", "claude-sonnet-5"),
        effort=settings.get("worker_effort", "max"),
        supervisor_model=settings.get("supervisor_model", "claude-sonnet-5"),
        supervisor_effort=settings.get("supervisor_effort", "max"),
        reviewer_model=settings.get("reviewer_model", "claude-sonnet-5"),
        reviewer_effort=settings.get("reviewer_effort", "high"),
        director_model=settings.get("director_model"),
        director_effort=settings.get("director_effort"),
        manager_model=settings.get("manager_model"),
        manager_effort=settings.get("manager_effort"),
        account_mode=settings.get("account_mode", "sticky"),
        test_cmd=settings.get("test_cmd"),
        max_turns=settings.get("max_turns", config.MAX_TURNS),
        auto_continue=settings.get("auto_continue", False),
        hierarchy_enabled=settings.get("hierarchy_enabled", False),
        max_managers=settings.get(
            "max_managers",
            settings.get("max_parallel_managers", 4),
        ),
        max_parallel_managers=settings.get("max_parallel_managers", 4),
        max_workers_per_manager=settings.get("max_workers_per_manager", 5),
        max_parallel_workers_per_manager=settings.get("max_parallel_workers_per_manager"),
        max_parallel_workers=settings.get("max_parallel_workers", 8),
    )
    return _start_task(request, resume_task_id=task_id)


@app.get("/api/tasks/{task_id}/artifacts")
def get_task_artifacts(task_id: str):
    manifest = artifact_manager.get_manifest(task_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail="Task chưa có artifact.")
    return manifest


@app.get("/api/tasks/{task_id}/artifacts/download")
def download_task_artifact(task_id: str):
    zip_path = artifact_manager.get_zip_path(task_id)
    if zip_path is None:
        raise HTTPException(status_code=404, detail="Task chưa có ZIP để tải.")
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"{task_id}-project.zip",
    )


@app.get("/api/tasks/{task_id}/artifacts/files/{file_path:path}")
def download_task_artifact_file(task_id: str, file_path: str):
    try:
        path = artifact_manager.get_artifact_file(task_id, file_path)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail="Artifact file không tồn tại.") from exc
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=Path(file_path).name,
    )


@app.patch("/api/tasks/{task_id}/artifacts/retention")
def update_artifact_retention(
    task_id: str,
    request: ArtifactRetentionRequest,
):
    try:
        manifest = artifact_manager.set_retention(
            task_id,
            pinned=request.pinned,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Task chưa có artifact.") from exc
    task_manager.set_artifact(task_id, manifest)
    return manifest


@app.delete("/api/tasks/{task_id}/artifacts")
def delete_task_artifacts(task_id: str, force: bool = False):
    try:
        deleted = artifact_manager.delete_artifacts(task_id, force=force)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="Task chưa có artifact.")
    task_manager.set_artifact(task_id, None)
    return {"status": "deleted"}


@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: str):
    task = task_manager.get_task(task_id)
    active_statuses = {
        "QUEUED",
        "RUNNING",
        "PLANNING",
        "MANAGING",
        "CODING",
        "REVIEWING",
        "REVISION",
        "WAITING_INPUT",
        "STOPPING",
        "RESUMING",
    }
    if task is not None and task.get("status") in active_statuses:
        raise HTTPException(status_code=409, detail="Không thể xóa task đang chạy.")
    runtime = runtime_registry.get(task_id)
    if runtime is not None:
        runtime.cancellation.set()
        runtime.broker.close(timeout=1)
        runtime_registry.evict(task_id, expected=runtime)
    removed = task_manager.delete_task(task_id) if task is not None else False
    purged = hierarchy_repository.delete_task(task_id)
    artifact_manager.delete_artifacts(task_id, force=True)
    if not removed and sum(purged.values()) == 0:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return {"status": "deleted", "purged": purged}


@app.post("/api/stop/{task_id}")
def stop_task(task_id: str):
    runtime = runtime_registry.get(task_id)
    if runtime is not None:
        task_manager.set_status(task_id, "STOPPING", "stopping")
        runtime.cancellation.set()
    return {"status": "stopping"}


@app.get("/api/stream/{task_id}")
def stream_output(task_id: str, after: int = 0):
    def event_stream():
        runtime = runtime_registry.get(task_id)
        broker = runtime.broker if runtime is not None else None
        task = task_manager.get_task(task_id)
        last_sequence = max(0, after)
        replayed_any = False
        cursor_hook = getattr(hierarchy_repository, "event_cursor", None)
        cursor = cursor_hook(task_id) if callable(cursor_hook) else {}
        retained_from_sequence = int(cursor.get("retained_from_sequence") or 1)
        if last_sequence < retained_from_sequence - 1:
            last_sequence = retained_from_sequence - 1
            gap = {
                "type": "timeline_gap",
                "sequence": last_sequence,
                "retained_from_sequence": retained_from_sequence,
                "latest_sequence": int(cursor.get("latest_sequence") or last_sequence),
                "history_incomplete": True,
            }
            yield f"data: {json.dumps(gap)}\n\n"
        while True:
            persisted = hierarchy_repository.replay_events(
                task_id,
                after_sequence=last_sequence,
                limit=5000,
            )
            if not persisted:
                break
            replayed_any = True
            for envelope in persisted:
                wire = _event_wire(envelope)
                last_sequence = envelope.sequence
                yield f"data: {json.dumps(wire)}\n\n"
                if wire["type"] == "done":
                    return
            if len(persisted) < 5000:
                break
        if not broker:
            if task is None:
                yield f"data: {json.dumps({'type': 'error', 'data': 'Task ID đã đóng hoặc không tồn tại', 'fatal': True})}\n\n"
                return
            if replayed_any:
                return
            yield f"data: {json.dumps({'type': 'error', 'data': 'Task ID đã đóng hoặc không tồn tại', 'fatal': True})}\n\n"
            return
        while True:
            if broker.has_gap_after(last_sequence):
                persisted = hierarchy_repository.replay_events(
                    task_id,
                    after_sequence=last_sequence,
                    limit=5000,
                )
                if persisted:
                    for envelope in persisted:
                        wire = _event_wire(envelope)
                        last_sequence = envelope.sequence
                        yield f"data: {json.dumps(wire)}\n\n"
                        if wire["type"] == "done":
                            return
                    continue
            messages = broker.wait_after(last_sequence, timeout=15)
            if not messages:
                if broker.closed:
                    return
                yield ": ping\n\n"
                continue
            for msg in messages:
                last_sequence = int(msg.get("_seq", last_sequence))
                yield f"data: {json.dumps(msg)}\n\n"
                if msg["type"] == "done":
                    return

    return StreamingResponse(event_stream(), media_type="text/event-stream")
