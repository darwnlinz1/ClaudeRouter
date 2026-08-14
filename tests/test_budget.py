import pytest

from orchestrator.budget import (
    BudgetExceededError,
    BudgetLimits,
    ExecutionBudget,
    get_task_budget,
    register_task_budget,
    remove_task_budget,
)


def test_budget_reserves_calls_and_input_tokens_atomically():
    budget = ExecutionBudget(
        BudgetLimits(
            max_model_calls=2,
            max_wall_clock_seconds=60,
            max_estimated_input_tokens=10,
        ),
        started_at=100,
    )

    first = budget.reserve_model_call("12345678", now=101)
    second = budget.reserve_model_call("abcdefgh", now=102)

    assert first["model_calls"] == 1
    assert second["model_calls"] == 2
    assert second["estimated_input_tokens"] == 4
    with pytest.raises(BudgetExceededError) as exc_info:
        budget.reserve_model_call("third", now=103)
    assert exc_info.value.reason == "model_calls"


def test_budget_rejects_wall_clock_and_token_overruns():
    wall_clock = ExecutionBudget(
        BudgetLimits(max_wall_clock_seconds=5),
        started_at=100,
    )
    with pytest.raises(BudgetExceededError, match="wall_clock"):
        wall_clock.reserve_model_call("prompt", now=105)

    tokens = ExecutionBudget(
        BudgetLimits(max_estimated_input_tokens=2),
        started_at=100,
    )
    with pytest.raises(BudgetExceededError, match="estimated_input_tokens"):
        tokens.reserve_model_call("x" * 12, now=101)


def test_task_budget_registry_is_removed_after_run():
    budget = register_task_budget("task-a", BudgetLimits(max_model_calls=3))
    assert get_task_budget("task-a") is budget
    assert remove_task_budget("task-a") is budget
    assert get_task_budget("task-a") is None
