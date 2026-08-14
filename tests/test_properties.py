from hypothesis import given
from hypothesis import strategies as st

from orchestrator.models import (
    ApprovalPolicy,
    RiskLevel,
    WorkContract,
    to_dict,
    work_contract_from_dict,
)
from orchestrator.reconciliation import _partition
from orchestrator.scheduler import canonical_scope, scopes_conflict

segment = st.from_regex(r"[a-z][a-z0-9_-]{0,8}", fullmatch=True)
scope = st.lists(segment, min_size=1, max_size=4).map("/".join)


@given(st.lists(scope, max_size=5), st.lists(scope, max_size=5))
def test_scope_conflicts_are_symmetric(left, right):
    assert scopes_conflict(left, right) == scopes_conflict(right, left)


@given(scope)
def test_scope_canonicalization_is_idempotent(value):
    canonical = canonical_scope(value)
    assert canonical_scope(canonical) == canonical


@given(
    st.dictionaries(
        segment,
        st.sampled_from(
            ["completed", "skipped", "blocked", "failed", "preflight_failed"]
        ),
        max_size=30,
    )
)
def test_terminal_partition_accounts_for_every_planned_entity(outcomes):
    result = _partition(outcomes)
    assert result["balanced"] is True
    assert result["terminal"] == result["planned"]
    assert sum(
        result[bucket]
        for bucket in (
            "completed",
            "skipped",
            "blocked",
            "failed",
            "preflight_failed",
        )
    ) == len(outcomes)


@given(
    st.lists(segment, min_size=1, max_size=5, unique=True),
    st.integers(min_value=0, max_value=100),
    st.sampled_from(list(RiskLevel)),
    st.sampled_from(list(ApprovalPolicy)),
)
def test_work_contract_round_trip(criteria, priority, risk, approval):
    contract = WorkContract(
        id="contract-property",
        version=1,
        expected_outputs=("result.json",),
        write_scopes=("src",),
        acceptance_criteria=tuple(criteria),
        test_requirements=("pytest",),
        evidence_requirements=("test-report",),
        consumers=("operator",),
        risk_level=risk,
        approval_policy=approval,
        priority=priority,
    )

    assert work_contract_from_dict(to_dict(contract)) == contract
