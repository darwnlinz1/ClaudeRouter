from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from orchestrator.budget import BudgetExceededError, BudgetLimits, ExecutionBudget
from orchestrator.scheduler import ScopeClaims


def test_model_call_budget_is_atomic_under_contention():
    budget = ExecutionBudget(BudgetLimits(max_model_calls=7))
    barrier = Barrier(24)

    def reserve() -> bool:
        barrier.wait()
        try:
            budget.reserve_model_call("concurrent prompt")
        except BudgetExceededError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=24) as executor:
        accepted = list(executor.map(lambda _: reserve(), range(24)))

    assert sum(accepted) == 7
    assert budget.snapshot()["model_calls"] == 7


def test_conflicting_scope_claim_has_single_winner_under_contention():
    claims = ScopeClaims()
    barrier = Barrier(2)

    def acquire(owner: str) -> bool:
        barrier.wait()
        return claims.acquire(owner, ("src/shared",))

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(acquire, ("worker-a", "worker-b")))

    assert sorted(outcomes) == [False, True]
