"""Bounded graceful-shutdown coordination for persistence and workers."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class ShutdownReport:
    drained: int
    closed: int
    joined_workers: int
    timed_out_components: tuple[str, ...]
    timed_out_workers: tuple[str, ...]
    elapsed_seconds: float

    @property
    def clean(self) -> bool:
        return not self.timed_out_components and not self.timed_out_workers


def _name(value: object) -> str:
    return str(getattr(value, "name", None) or value.__class__.__name__)


def _bounded_call(
    callback: Callable[..., Any],
    timeout: float,
    *,
    pass_timeout: bool,
) -> tuple[bool, Any]:
    if timeout <= 0:
        return False, None
    result: list[Any] = []
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            result.append(callback(timeout=timeout) if pass_timeout else callback())
        except BaseException as exc:  # surfaced in the shutdown caller
            errors.append(exc)

    thread = threading.Thread(target=invoke, daemon=True, name="lifecycle-component")
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        return False, None
    if errors:
        raise errors[0]
    return True, result[0] if result else None


class LifecycleCoordinator:
    """Stop intake, drain admitted work, join workers, then close resources."""

    def __init__(self) -> None:
        self._drainables: list[object] = []
        self._closeables: list[object] = []
        self._workers: list[object] = []
        self._lock = threading.RLock()
        self._shutdown = False

    def register_drainable(self, component: object) -> object:
        with self._lock:
            if self._shutdown:
                raise RuntimeError("lifecycle is already shut down")
            if component not in self._drainables:
                self._drainables.append(component)
        return component

    def register_closeable(self, component: object) -> object:
        with self._lock:
            if self._shutdown:
                raise RuntimeError("lifecycle is already shut down")
            if component not in self._closeables:
                self._closeables.append(component)
        return component

    def register_worker(self, worker: object) -> object:
        if not callable(getattr(worker, "join", None)):
            raise ValueError("registered worker must expose join(timeout)")
        with self._lock:
            if self._shutdown:
                raise RuntimeError("lifecycle is already shut down")
            if worker not in self._workers:
                self._workers.append(worker)
        return worker

    def register_component(self, component: object) -> object:
        if callable(getattr(component, "drain", None)):
            self.register_drainable(component)
        if callable(getattr(component, "close", None)):
            self.register_closeable(component)
        return component

    def shutdown(self, timeout: float = 10.0) -> ShutdownReport:
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        started = time.monotonic()
        deadline = started + timeout
        with self._lock:
            if self._shutdown:
                return ShutdownReport(0, 0, 0, (), (), time.monotonic() - started)
            self._shutdown = True
            drainables = tuple(self._drainables)
            closeables = tuple(self._closeables)
            workers = tuple(self._workers)

        for component in drainables:
            stop = getattr(component, "stop_accepting", None)
            if callable(stop):
                stop()

        drained = 0
        closed = 0
        timed_out_components: list[str] = []
        for component in drainables:
            remaining = max(0.0, deadline - time.monotonic())
            completed, result = _bounded_call(
                component.drain,
                remaining,
                pass_timeout=True,
            )
            if completed and result is not False:
                drained += 1
            else:
                timed_out_components.append(f"{_name(component)}.drain")

        joined_workers = 0
        timed_out_workers: list[str] = []
        for worker in workers:
            remaining = max(0.0, deadline - time.monotonic())
            worker.join(remaining)
            is_alive = getattr(worker, "is_alive", None)
            alive = bool(is_alive()) if callable(is_alive) else False
            if alive:
                timed_out_workers.append(_name(worker))
            else:
                joined_workers += 1

        for component in reversed(closeables):
            remaining = max(0.0, deadline - time.monotonic())
            completed, _ = _bounded_call(
                component.close,
                remaining,
                pass_timeout=False,
            )
            if completed:
                closed += 1
            else:
                timed_out_components.append(f"{_name(component)}.close")

        return ShutdownReport(
            drained=drained,
            closed=closed,
            joined_workers=joined_workers,
            timed_out_components=tuple(timed_out_components),
            timed_out_workers=tuple(timed_out_workers),
            elapsed_seconds=time.monotonic() - started,
        )
