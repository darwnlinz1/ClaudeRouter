"""Thread-safe per-task execution budgets."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any


class BudgetExceededError(RuntimeError):
    def __init__(self, reason: str, snapshot: dict[str, Any]) -> None:
        super().__init__(f"Execution budget exceeded: {reason}")
        self.reason = reason
        self.snapshot = snapshot


@dataclass(frozen=True, slots=True)
class BudgetLimits:
    max_model_calls: int | None = None
    max_wall_clock_seconds: float | None = None
    max_estimated_input_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in ("max_model_calls", "max_estimated_input_tokens"):
            value = getattr(self, name)
            if value is not None and value < 1:
                raise ValueError(f"{name} must be positive")
        if (
            self.max_wall_clock_seconds is not None
            and self.max_wall_clock_seconds <= 0
        ):
            raise ValueError("max_wall_clock_seconds must be positive")


class ExecutionBudget:
    def __init__(
        self,
        limits: BudgetLimits,
        *,
        started_at: float | None = None,
    ) -> None:
        self.limits = limits
        self.started_at = time.monotonic() if started_at is None else started_at
        self.model_calls = 0
        self.estimated_input_tokens = 0
        self._lock = threading.RLock()

    def snapshot(self, *, now: float | None = None) -> dict[str, Any]:
        current = time.monotonic() if now is None else now
        with self._lock:
            return {
                "model_calls": self.model_calls,
                "estimated_input_tokens": self.estimated_input_tokens,
                "elapsed_seconds": max(0.0, current - self.started_at),
                "limits": {
                    "max_model_calls": self.limits.max_model_calls,
                    "max_wall_clock_seconds": self.limits.max_wall_clock_seconds,
                    "max_estimated_input_tokens": (
                        self.limits.max_estimated_input_tokens
                    ),
                },
            }

    def reserve_model_call(
        self,
        prompt: str,
        *,
        now: float | None = None,
    ) -> dict[str, Any]:
        current = time.monotonic() if now is None else now
        estimated_tokens = max(1, (len(prompt) + 3) // 4)
        with self._lock:
            elapsed = max(0.0, current - self.started_at)
            if (
                self.limits.max_wall_clock_seconds is not None
                and elapsed >= self.limits.max_wall_clock_seconds
            ):
                raise BudgetExceededError(
                    "wall_clock",
                    self.snapshot(now=current),
                )
            if (
                self.limits.max_model_calls is not None
                and self.model_calls + 1 > self.limits.max_model_calls
            ):
                raise BudgetExceededError(
                    "model_calls",
                    self.snapshot(now=current),
                )
            if (
                self.limits.max_estimated_input_tokens is not None
                and self.estimated_input_tokens + estimated_tokens
                > self.limits.max_estimated_input_tokens
            ):
                raise BudgetExceededError(
                    "estimated_input_tokens",
                    self.snapshot(now=current),
                )
            self.model_calls += 1
            self.estimated_input_tokens += estimated_tokens
            return self.snapshot(now=current)


_budgets: dict[str, ExecutionBudget] = {}
_budgets_lock = threading.RLock()


def register_task_budget(task_id: str, limits: BudgetLimits) -> ExecutionBudget:
    budget = ExecutionBudget(limits)
    with _budgets_lock:
        _budgets[task_id] = budget
    return budget


def get_task_budget(task_id: str | None) -> ExecutionBudget | None:
    if not task_id:
        return None
    with _budgets_lock:
        return _budgets.get(task_id)


def remove_task_budget(task_id: str) -> ExecutionBudget | None:
    with _budgets_lock:
        return _budgets.pop(task_id, None)
