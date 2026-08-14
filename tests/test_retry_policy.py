import pytest

from orchestrator.provider_adapter import (
    ProviderPayloadError,
    ProviderRateLimitError,
    ProviderTransportError,
)
from orchestrator.retry_policy import (
    FAILURE_POLICY_REGISTRY,
    PolicyCrisisStrategy,
    RecoveryState,
    advance_remediation,
    build_failure_signature,
    claim_remediation_strategy,
    classify_exception,
    policy_for_failure,
    retryable_for_failure,
)


def test_retry_taxonomy_fails_closed_and_distinguishes_provider_failures():
    payload = classify_exception(ProviderPayloadError("bad payload", provider="test"))
    rate_limit = classify_exception(
        ProviderRateLimitError("limited", provider="test", retry_after_seconds=1)
    )
    transport = classify_exception(ProviderTransportError("offline", provider="test"))

    assert (payload.failure_kind, payload.retryable) == ("provider_payload", False)
    assert (rate_limit.failure_kind, rate_limit.retryable) == ("rate_limit", True)
    assert (transport.failure_kind, transport.retryable) == ("transport", True)
    assert retryable_for_failure(None) is False
    assert retryable_for_failure("unknown") is False
    assert retryable_for_failure("approval_rejected") is False
    assert retryable_for_failure("reviewer_revise") is True
    assert retryable_for_failure("contract_infeasible") is True
    assert retryable_for_failure("worker_declined") is True


@pytest.mark.parametrize(
    ("failure_kind", "category"),
    [
        ("authentication", "auth_account"),
        ("rate_limit", "rate_limit"),
        ("lease_loss", "transport_lease"),
        ("protocol_error", "protocol"),
        ("provider_payload", "provider_payload"),
        ("patch_rejected", "patch_rejection"),
        ("machine_gate", "machine_gate"),
        ("reviewer_revise", "reviewer_revise"),
        ("contract_infeasible", "contract_issue"),
        ("dependency", "dependency"),
        ("policy_denied", "approval_policy"),
        ("sandbox_unavailable", "sandbox"),
        ("unsupported_worker_target", "safety_unsupported"),
        ("effect_replay_conflict", "rollback_effect"),
        ("scheduler", "backend_scheduler_unknown"),
    ],
)
def test_every_crisis_category_has_a_structured_policy(failure_kind, category):
    policy = policy_for_failure(failure_kind)

    assert policy.category == category
    assert policy is FAILURE_POLICY_REGISTRY[category]
    assert policy.strategies
    assert all(strategy.name and strategy.hint and strategy.prompt_variant for strategy in policy.strategies)


def test_failure_signature_hashes_every_deterministic_input():
    values = {
        "contract": {"id": "contract-a", "version": 2},
        "target": "src/value.py",
        "source": "worker-a",
        "patch": "patch-a",
        "test": {"status": "failed", "output": "line 4"},
        "review": {"verdict": "revise", "feedback": "fix it"},
        "request": {"logical_request_id": "request-a"},
    }

    first = build_failure_signature("machine_gate", **values)
    second = build_failure_signature("machine_gate", **values)

    assert first == second
    assert len(first.digest) == 64
    for name in (
        "contract_hash",
        "target_hash",
        "source_hash",
        "patch_hash",
        "test_hash",
        "review_hash",
        "request_hash",
    ):
        assert len(getattr(first, name)) == 64

    for key in values:
        changed = dict(values)
        changed[key] = {"changed": key}
        assert build_failure_signature("machine_gate", **changed).digest != first.digest


def test_each_strategy_runs_once_per_signature_and_changed_signature_resets():
    first_signature = build_failure_signature(
        "protocol_error",
        source="account-a",
        request="logical-request",
        review="same parser error",
    )
    state = RecoveryState()
    selected = []

    while True:
        choice = advance_remediation("protocol_error", first_signature, state)
        if choice is None:
            break
        selected.append(choice.strategy.name)
        state = choice.state

    assert selected == [
        "correct_action_protocol",
        "strict_schema_reprompt",
        "replace_protocol_account",
    ]
    assert len(selected) == len(set(selected))

    changed_signature = build_failure_signature(
        "protocol_error",
        source="account-a",
        request="logical-request",
        review="different parser error",
    )
    reset = advance_remediation("protocol_error", changed_signature, state)
    assert reset is not None
    assert reset.strategy.name == "correct_action_protocol"
    assert reset.state.attempted_strategies == ("correct_action_protocol",)


def test_cancellation_bypasses_all_remediation():
    signature = build_failure_signature("cancelled", request="request-a")

    assert policy_for_failure("cancelled").cancellation_bypass is True
    assert advance_remediation("cancelled", signature) is None


def test_hierarchy_can_atomically_claim_next_strategy_from_its_repository():
    claimed = set()

    class Repository:
        def claim_remediation_strategy(
            self,
            task_id,
            logical_agent_id,
            failure_signature,
            strategy,
            **kwargs,
        ):
            key = (task_id, logical_agent_id, failure_signature, strategy)
            if key in claimed:
                return None
            claimed.add(key)
            return {"strategy": strategy, **kwargs}

    repository = Repository()
    signature = build_failure_signature(
        "patch_rejected",
        target="value.py",
        patch="same rejected patch",
    )

    first = claim_remediation_strategy(
        repository,
        task_id="task-a",
        logical_agent_id="worker-a",
        failure_kind="patch_rejected",
        signature=signature,
    )
    second = claim_remediation_strategy(
        repository,
        task_id="task-a",
        logical_agent_id="worker-a",
        failure_kind="patch_rejected",
        signature=signature,
    )
    exhausted = claim_remediation_strategy(
        repository,
        task_id="task-a",
        logical_agent_id="worker-a",
        failure_kind="patch_rejected",
        signature=signature,
    )

    assert first is not None
    assert first.selection.strategy.name == "refresh_target_context"
    assert second is not None
    assert second.selection.strategy.name == "regenerate_patch"
    assert exhausted is None


def test_policy_crisis_strategy_matches_hierarchy_hook_and_resets_on_new_evidence():
    strategy = PolicyCrisisStrategy()
    context = {
        "crisis_id": "crisis-a",
        "scope": "workstream",
        "task_id": "task-a",
        "session_id": "session-a",
        "execution_epoch": "epoch-a",
        "manager_id": "manager-a",
        "workstream_id": "stream-a",
        "failure_kind": "machine_gate",
        "affected_work_item_ids": ["item-a"],
        "errors": {"item-a": "SyntaxError line 4"},
        "reason": "machine gate failed",
    }

    first = strategy.next_remediation(context)
    second = strategy.next_remediation(
        {
            **context,
            "crisis_id": "crisis-b",
            "reason": "Same gate described with different prose",
            "suggested_instructions": "Try another wording.",
        }
    )
    exhausted = strategy.next_remediation(
        {
            **context,
            "crisis_id": "crisis-c",
            "reason": "Same gate described a third time",
        }
    )
    changed = strategy.next_remediation(
        {
            **context,
            "errors": {"item-a": "AssertionError expected 2"},
        }
    )

    assert first["action"] == "remediate"
    assert first["reason"] == "repair_machine_gate"
    assert first["failure_category"] == "machine_gate"
    assert "MACHINE GATE RECOVERY" in first["instructions"]
    assert second["action"] == "remediate"
    assert second["reason"] == "narrow_machine_gate_repair"
    assert second["failure_signature"] == first["failure_signature"]
    assert exhausted["action"] == "abandon"
    assert exhausted["reason"] == "remediation_strategies_exhausted"
    assert exhausted["failure_signature"] == first["failure_signature"]
    assert changed["action"] == "remediate"
    assert changed["failure_signature"] != first["failure_signature"]


def test_reviewer_revise_signature_ignores_changing_free_form_feedback():
    strategy = PolicyCrisisStrategy()
    context = {
        "scope": "workstream",
        "task_id": "task-a",
        "session_id": "session-a",
        "execution_epoch": "epoch-a",
        "manager_id": "manager-a",
        "workstream_id": "stream-a",
        "failure_kind": "reviewer_revise",
        "affected_work_item_ids": ["item-a"],
        "errors": {"item-a": "First free-form review"},
    }

    first = strategy.next_remediation(context)
    second = strategy.next_remediation(
        {
            **context,
            "crisis_id": "new-occurrence",
            "errors": {"item-a": "Same issue described with different prose"},
        }
    )
    repeated = strategy.next_remediation(
        {
            **context,
            "crisis_id": "third-occurrence",
            "errors": {"item-a": "Still the same logical review finding"},
        }
    )

    assert first["action"] == "remediate"
    assert first["reason"] == "apply_reviewer_feedback"
    assert second["action"] == "remediate"
    assert second["reason"] == "narrow_reviewer_revision"
    assert second["failure_signature"] == first["failure_signature"]
    assert repeated["action"] == "abandon"
    assert repeated["failure_signature"] == first["failure_signature"]


def test_contract_issue_gets_one_automatic_replan_before_abandon():
    strategy = PolicyCrisisStrategy()
    context = {
        "scope": "workstream",
        "task_id": "task-a",
        "session_id": "session-a",
        "execution_epoch": "epoch-a",
        "manager_id": "manager-a",
        "workstream_id": "stream-a",
        "failure_kind": "worker_declined",
        "affected_work_item_ids": ["item-a"],
        "errors": {"item-a": "Worker deferred remaining files"},
    }

    first = strategy.next_remediation(context)
    exhausted = strategy.next_remediation(context)

    assert first["action"] == "replan"
    assert first["reason"] == "revise_contract"
    assert exhausted["action"] == "abandon"
    assert exhausted["reason"] == "remediation_strategies_exhausted"
