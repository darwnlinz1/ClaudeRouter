from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from orchestrator.scheduler import ScopeClaims


def test_conflicting_scope_claim_has_single_winner_under_contention():
    claims = ScopeClaims()
    barrier = Barrier(2)

    def acquire(owner: str) -> bool:
        barrier.wait()
        return claims.acquire(owner, ("src/shared",))

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(acquire, ("worker-a", "worker-b")))

    assert sorted(outcomes) == [False, True]
