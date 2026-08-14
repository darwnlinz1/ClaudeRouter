"""Failure policies and deterministic crisis-remediation selection.

This module deliberately does not persist recovery state.  Callers choose a
strategy with :func:`advance_remediation`, persist ``RecoveryState.as_dict()``
alongside their own attempt evidence, and pass that state back on the next
failure.  A strategy can run once for one deterministic signature; a changed
signature starts a fresh strategy sequence without relying on an attempt count.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from typing import Any, Collection, Mapping

AUTH_ACCOUNT = "auth_account"
RATE_LIMIT = "rate_limit"
TRANSPORT_LEASE = "transport_lease"
PROTOCOL = "protocol"
PROVIDER_PAYLOAD = "provider_payload"
PATCH_REJECTION = "patch_rejection"
MACHINE_GATE = "machine_gate"
REVIEWER_REVISE = "reviewer_revise"
CONTRACT_ISSUE = "contract_issue"
DEPENDENCY = "dependency"
APPROVAL_POLICY = "approval_policy"
SANDBOX = "sandbox"
SAFETY_UNSUPPORTED = "safety_unsupported"
ROLLBACK_EFFECT = "rollback_effect"
BACKEND_SCHEDULER_UNKNOWN = "backend_scheduler_unknown"
CANCELLATION = "cancellation"


@dataclass(frozen=True, slots=True)
class RemediationStrategy:
    """One bounded response to a particular failure signature."""

    name: str
    actor: str
    hint: str
    prompt_variant: str
    automatic: bool = True
    replace_account: bool = False
    quarantine_account: bool = False
    cooldown_account: bool = False

    @property
    def key(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class FailurePolicy:
    """Category policy and its ordered, non-repeating strategies."""

    category: str
    failure_kinds: frozenset[str]
    strategies: tuple[RemediationStrategy, ...]
    retryable: bool
    cancellation_bypass: bool = False

    @property
    def remediation_strategies(self) -> tuple[RemediationStrategy, ...]:
        return self.strategies

    def strategy(self, name: str) -> RemediationStrategy | None:
        normalized = str(name).strip().casefold()
        return next(
            (strategy for strategy in self.strategies if strategy.name.casefold() == normalized),
            None,
        )


def _strategy(
    name: str,
    actor: str,
    hint: str,
    prompt_variant: str,
    *,
    automatic: bool = True,
    replace_account: bool = False,
    quarantine_account: bool = False,
    cooldown_account: bool = False,
) -> RemediationStrategy:
    return RemediationStrategy(
        name=name,
        actor=actor,
        hint=hint,
        prompt_variant=prompt_variant,
        automatic=automatic,
        replace_account=replace_account,
        quarantine_account=quarantine_account,
        cooldown_account=cooldown_account,
    )


FAILURE_POLICY_REGISTRY: Mapping[str, FailurePolicy] = {
    CANCELLATION: FailurePolicy(
        category=CANCELLATION,
        failure_kinds=frozenset({"cancelled", "canceled", "aborted"}),
        strategies=(),
        retryable=False,
        cancellation_bypass=True,
    ),
    AUTH_ACCOUNT: FailurePolicy(
        category=AUTH_ACCOUNT,
        failure_kinds=frozenset(
            {
                "account_invalid",
                "account_unavailable",
                "account_pool_exhausted",
                "authentication",
                "authorization",
            }
        ),
        strategies=(
            _strategy(
                "replace_account",
                "account_coordinator",
                "Quarantine the rejected credential and atomically reserve a healthy replacement.",
                "Replay the unchanged logical request on the coordinator-selected replacement account.",
                replace_account=True,
                quarantine_account=True,
            ),
        ),
        retryable=True,
    ),
    RATE_LIMIT: FailurePolicy(
        category=RATE_LIMIT,
        failure_kinds=frozenset({"rate_limit", "provider_rate_limit"}),
        strategies=(
            _strategy(
                "cooldown_and_replace_account",
                "account_coordinator",
                "Honor Retry-After, record cooldown, and atomically reserve another account.",
                "Replay the unchanged logical request on a non-cooling replacement account.",
                replace_account=True,
                cooldown_account=True,
            ),
        ),
        retryable=True,
    ),
    TRANSPORT_LEASE: FailurePolicy(
        category=TRANSPORT_LEASE,
        failure_kinds=frozenset(
            {
                "account_lease_unavailable",
                "lease_loss",
                "provider_error",
                "transport",
                "transport_error",
            }
        ),
        strategies=(
            _strategy(
                "replace_transport_account",
                "provider_runtime",
                "Release the failed lease and replay the same request through a fresh account lease.",
                "Replay the byte-equivalent logical request with fresh provider identifiers.",
                replace_account=True,
            ),
        ),
        retryable=True,
    ),
    PROTOCOL: FailurePolicy(
        category=PROTOCOL,
        failure_kinds=frozenset(
            {
                "action_protocol",
                "output_protocol",
                "protocol",
                "protocol_error",
            }
        ),
        strategies=(
            _strategy(
                "correct_action_protocol",
                "provider_runtime",
                "Re-prompt with the exact parser diagnostic and allowed action schema.",
                "Return only one valid action record. Correct the cited parser violation exactly.",
            ),
            _strategy(
                "strict_schema_reprompt",
                "provider_runtime",
                "Use the strict schema-only prompt variant with no narrative output.",
                "SCHEMA-ONLY RECOVERY: emit no prose, markdown, or tool simulation outside the record.",
            ),
            _strategy(
                "replace_protocol_account",
                "account_coordinator",
                "Replay the corrected protocol prompt on one fresh account.",
                "Replay the strict schema-only request with fresh provider identifiers.",
                replace_account=True,
            ),
        ),
        retryable=True,
    ),
    PROVIDER_PAYLOAD: FailurePolicy(
        category=PROVIDER_PAYLOAD,
        failure_kinds=frozenset(
            {
                "ambiguous_conversation",
                "malformed_input",
                "provider_conversation_input",
                "provider_payload",
                "provider_request",
            }
        ),
        strategies=(
            _strategy(
                "probe_ambiguous_conversation",
                "account_coordinator",
                "Probe the unchanged conversation request on exactly one alternate account.",
                "Replay the identical conversation request once with fresh provider identifiers.",
                replace_account=True,
            ),
            _strategy(
                "repair_provider_payload",
                "provider_runtime",
                "Repair the provider request shape from logged wire diagnostics; do not blame credentials.",
                "Rebuild the provider payload under the configured provider policy and schema.",
                automatic=False,
            ),
        ),
        retryable=False,
    ),
    PATCH_REJECTION: FailurePolicy(
        category=PATCH_REJECTION,
        failure_kinds=frozenset({"file_changed", "patch_rejected"}),
        strategies=(
            _strategy(
                "refresh_target_context",
                "worker",
                "Reload the authorized target and regenerate anchors against its current hash.",
                "PATCH RECOVERY: use the refreshed target hash and exact, unique SEARCH anchors.",
            ),
            _strategy(
                "regenerate_patch",
                "worker",
                "Generate a materially different patch for the same target and contract.",
                "PATCH RECOVERY: replace the rejected patch; do not replay identical patch bytes.",
            ),
        ),
        retryable=True,
    ),
    MACHINE_GATE: FailurePolicy(
        category=MACHINE_GATE,
        failure_kinds=frozenset({"machine_gate", "syntax_gate", "test_gate"}),
        strategies=(
            _strategy(
                "repair_machine_gate",
                "worker",
                "Repair the concrete syntax or test failure using the exact machine evidence.",
                "MACHINE GATE RECOVERY: address the failing command and diagnostic, then rerun its gate.",
            ),
            _strategy(
                "narrow_machine_gate_repair",
                "worker",
                "Retry with a narrower repair of only the cited diagnostic; do not restyle unrelated code.",
                "MACHINE GATE RECOVERY: change only the failing statement or assertion, then rerun the same gate.",
            ),
        ),
        retryable=True,
    ),
    REVIEWER_REVISE: FailurePolicy(
        category=REVIEWER_REVISE,
        failure_kinds=frozenset({"reviewer_revise", "review_rejected"}),
        strategies=(
            _strategy(
                "apply_reviewer_feedback",
                "worker",
                "Revise the patch against the review hash and every explicit reviewer instruction.",
                "REVIEW RECOVERY: produce a new patch that resolves each cited review finding.",
            ),
            _strategy(
                "narrow_reviewer_revision",
                "worker",
                "Retry a minimal patch that only addresses the cited findings.",
                "REVIEW RECOVERY: touch only the cited findings; leave unrelated code unchanged.",
            ),
        ),
        retryable=True,
    ),
    CONTRACT_ISSUE: FailurePolicy(
        category=CONTRACT_ISSUE,
        failure_kinds=frozenset(
            {
                "contract_approval",
                "contract_infeasible",
                "no_reviewable_output",
                "plan_admission",
                "plan_preflight",
                "preflight",
                "unsafe_or_unexecutable_plan",
                "worker_declined",
            }
        ),
        strategies=(
            _strategy(
                "revise_contract",
                "manager",
                "Revise the typed contract or plan; replaying the same worker assignment cannot repair it.",
                "CONTRACT RECOVERY: change the infeasible inputs, scopes, outputs, or acceptance evidence.",
            ),
        ),
        retryable=True,
    ),
    DEPENDENCY: FailurePolicy(
        category=DEPENDENCY,
        failure_kinds=frozenset({"dependency", "dependency_blocked"}),
        strategies=(
            _strategy(
                "refresh_dependency_evidence",
                "scheduler",
                "Re-evaluate dependency evidence after upstream state changes.",
                "DEPENDENCY RECOVERY: use newly completed upstream evidence before rescheduling.",
            ),
            _strategy(
                "replan_dependency",
                "manager",
                "Change the dependency edge or work contract when the blocker signature is unchanged.",
                "DEPENDENCY RECOVERY: revise the DAG instead of resubmitting the blocked item.",
            ),
        ),
        retryable=True,
    ),
    APPROVAL_POLICY: FailurePolicy(
        category=APPROVAL_POLICY,
        failure_kinds=frozenset(
            {
                "approval_rejected",
                "contract_approval_required",
                "policy_denied",
                "risk_requires_approval",
            }
        ),
        strategies=(
            _strategy(
                "request_policy_resolution",
                "operator",
                "Wait for an explicit approval or policy change; never bypass provider or local policy.",
                "POLICY HOLD: preserve the proposed effect and request an authorized decision.",
                automatic=False,
            ),
        ),
        retryable=False,
    ),
    SANDBOX: FailurePolicy(
        category=SANDBOX,
        failure_kinds=frozenset({"sandbox_blocked", "sandbox_unavailable"}),
        strategies=(
            _strategy(
                "restore_sandbox_capability",
                "operator",
                "Restore the required isolation backend or approve a supported execution path.",
                "SANDBOX HOLD: do not run the command without the required isolation guarantee.",
                automatic=False,
            ),
        ),
        retryable=False,
    ),
    SAFETY_UNSUPPORTED: FailurePolicy(
        category=SAFETY_UNSUPPORTED,
        failure_kinds=frozenset(
            {
                "safety",
                "unsupported_binary",
                "unsupported_prompt_source",
                "unsupported_worker_target",
            }
        ),
        strategies=(
            _strategy(
                "select_supported_safe_target",
                "manager",
                "Choose an authorized text target or revise the plan without weakening safety checks.",
                "SAFETY HOLD: select a supported target and preserve the local security boundary.",
                automatic=False,
            ),
        ),
        retryable=False,
    ),
    ROLLBACK_EFFECT: FailurePolicy(
        category=ROLLBACK_EFFECT,
        failure_kinds=frozenset(
            {
                "effect_replay_conflict",
                "rollback_conflict",
                "rollback_effect",
            }
        ),
        strategies=(
            _strategy(
                "reconcile_effect_manually",
                "operator",
                "Reconcile durable effect hashes before any further mutation or compensation.",
                "EFFECT HOLD: inspect the receipt and target hashes; never overwrite the conflicting state.",
                automatic=False,
            ),
        ),
        retryable=False,
    ),
    BACKEND_SCHEDULER_UNKNOWN: FailurePolicy(
        category=BACKEND_SCHEDULER_UNKNOWN,
        failure_kinds=frozenset(
            {
                "backend_failure",
                "scheduler",
                "scheduler_unknown",
                "unknown",
            }
        ),
        strategies=(
            _strategy(
                "diagnose_backend_scheduler",
                "orchestrator",
                "Use scheduler, request, and effect references to diagnose the unknown backend state.",
                "BACKEND HOLD: classify the failure precisely before selecting another strategy.",
                automatic=False,
            ),
        ),
        retryable=False,
    ),
}

# Friendly alias for integration code.
FAILURE_POLICIES = FAILURE_POLICY_REGISTRY
_POLICY_BY_KIND = {
    failure_kind: policy
    for policy in FAILURE_POLICY_REGISTRY.values()
    for failure_kind in policy.failure_kinds
}
_NONRETRYABLE_KIND_OVERRIDES = frozenset(
    {
        "account_pool_exhausted",
        "account_unavailable",
    }
)

def policy_for_failure(failure_kind: str | None) -> FailurePolicy:
    """Resolve a failure kind, failing closed into backend/scheduler unknown."""

    normalized = str(failure_kind or "").strip().casefold().replace("-", "_")
    return _POLICY_BY_KIND.get(normalized, FAILURE_POLICY_REGISTRY[BACKEND_SCHEDULER_UNKNOWN])


resolve_failure_policy = policy_for_failure


def _json_default(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, os.PathLike):
        return os.fspath(value)
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, BaseException):
        return {"error_type": type(value).__name__, "message": str(value)}
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=lambda item: repr(item))
    return repr(value)


def deterministic_hash(value: Any) -> str:
    """Hash arbitrary diagnostic input with canonical JSON encoding."""

    encoded = json.dumps(
        value,
        default=_json_default,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class FailureSignature:
    """Hashed identity of the facts that determine a remediation sequence."""

    category: str
    contract_hash: str
    target_hash: str
    source_hash: str
    patch_hash: str
    test_hash: str
    review_hash: str
    request_hash: str
    digest: str

    def as_dict(self) -> dict[str, str]:
        return {
            "category": self.category,
            "contract_hash": self.contract_hash,
            "target_hash": self.target_hash,
            "source_hash": self.source_hash,
            "patch_hash": self.patch_hash,
            "test_hash": self.test_hash,
            "review_hash": self.review_hash,
            "request_hash": self.request_hash,
            "digest": self.digest,
        }


def build_failure_signature(
    failure_kind: str | None = None,
    *,
    category: str | None = None,
    contract: Any = None,
    target: Any = None,
    source: Any = None,
    patch: Any = None,
    test: Any = None,
    review: Any = None,
    request: Any = None,
    contract_hash: str | None = None,
    target_hash: str | None = None,
    source_hash: str | None = None,
    patch_hash: str | None = None,
    test_hash: str | None = None,
    review_hash: str | None = None,
    request_hash: str | None = None,
) -> FailureSignature:
    """Build the category + seven-input signature used for crisis recovery."""

    resolved_category = str(category or policy_for_failure(failure_kind).category)
    components = {
        "contract_hash": str(contract_hash or deterministic_hash(contract)),
        "target_hash": str(target_hash or deterministic_hash(target)),
        "source_hash": str(source_hash or deterministic_hash(source)),
        "patch_hash": str(patch_hash or deterministic_hash(patch)),
        "test_hash": str(test_hash or deterministic_hash(test)),
        "review_hash": str(review_hash or deterministic_hash(review)),
        "request_hash": str(request_hash or deterministic_hash(request)),
    }
    digest = deterministic_hash({"category": resolved_category, **components})
    return FailureSignature(
        category=resolved_category,
        digest=digest,
        **components,
    )


signature_for_failure = build_failure_signature


@dataclass(frozen=True, slots=True)
class RecoveryState:
    """Serializable caller-owned state for one active failure signature."""

    signature: str = ""
    attempted_strategies: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | None) -> "RecoveryState":
        if not value:
            return cls()
        attempted = value.get("attempted_strategies") or value.get("strategies") or ()
        return cls(
            signature=str(value.get("signature") or value.get("failure_signature") or ""),
            attempted_strategies=tuple(str(item) for item in attempted),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "signature": self.signature,
            "attempted_strategies": list(self.attempted_strategies),
        }


@dataclass(frozen=True, slots=True)
class RemediationSelection:
    policy: FailurePolicy
    signature: FailureSignature
    strategy: RemediationStrategy
    state: RecoveryState

    def as_dict(self) -> dict[str, Any]:
        return {
            "failure_category": self.policy.category,
            "failure_signature": self.signature.digest,
            "remediation_strategy": self.strategy.name,
            "remediation_actor": self.strategy.actor,
            "remediation_hint": self.strategy.hint,
            "remediation_prompt_variant": self.strategy.prompt_variant,
            "recovery_state": self.state.as_dict(),
        }


def advance_remediation(
    failure_kind: str,
    signature: FailureSignature,
    state: RecoveryState | Mapping[str, Any] | None = None,
    *,
    automatic_only: bool = True,
) -> RemediationSelection | None:
    """Choose and record the next unique strategy for ``signature``.

    If the signature changed, previous strategy names are intentionally reset.
    No numeric attempt count participates in the decision.
    """

    policy = policy_for_failure(failure_kind)
    if policy.cancellation_bypass:
        return None
    current = state if isinstance(state, RecoveryState) else RecoveryState.from_dict(state)
    attempted = current.attempted_strategies if current.signature == signature.digest else ()
    attempted_set = set(attempted)
    strategy = next(
        (
            candidate
            for candidate in policy.strategies
            if candidate.name not in attempted_set
            and (candidate.automatic or not automatic_only)
        ),
        None,
    )
    if strategy is None:
        return None
    next_state = RecoveryState(
        signature=signature.digest,
        attempted_strategies=(*attempted, strategy.name),
    )
    return RemediationSelection(
        policy=policy,
        signature=signature,
        strategy=strategy,
        state=next_state,
    )


def choose_remediation(
    failure_kind: str,
    signature: FailureSignature,
    attempted_strategies: Collection[str] = (),
    *,
    automatic_only: bool = True,
) -> RemediationSelection | None:
    """Stateless convenience hook for hierarchy integrations."""

    return advance_remediation(
        failure_kind,
        signature,
        RecoveryState(signature.digest, tuple(str(item) for item in attempted_strategies)),
        automatic_only=automatic_only,
    )


def recovery_state_from_attempts(
    attempts: Collection[Mapping[str, Any]],
    signature: FailureSignature | str,
) -> RecoveryState:
    """Rehydrate strategy state from caller-owned durable attempt records."""

    digest = signature.digest if isinstance(signature, FailureSignature) else str(signature)
    names = tuple(
        str(attempt.get("strategy"))
        for attempt in attempts
        if str(attempt.get("failure_signature") or "") == digest
        and attempt.get("strategy")
    )
    return RecoveryState(digest, tuple(dict.fromkeys(names)))


@dataclass(frozen=True, slots=True)
class ClaimedRemediation:
    """A selected strategy plus the repository's opaque reservation record."""

    selection: RemediationSelection
    persistence_record: Mapping[str, Any]


def claim_remediation_strategy(
    repository: object,
    *,
    task_id: str,
    logical_agent_id: str,
    failure_kind: str,
    signature: FailureSignature,
    execution_epoch_id: str | None = None,
    attempted_strategies: Collection[str] = (),
    details: Mapping[str, Any] | None = None,
    terminal_log_refs: Collection[Any] = (),
    automatic_only: bool = True,
) -> ClaimedRemediation | None:
    """Choose and atomically claim a strategy through a caller-owned store.

    ``StateRepository.claim_remediation_strategy`` and its
    ``reserve_remediation_attempt`` alias satisfy this feature-detected hook.
    A lost uniqueness race simply advances to the next policy strategy.
    """

    callback = getattr(repository, "claim_remediation_strategy", None)
    if not callable(callback):
        callback = getattr(repository, "reserve_remediation_attempt", None)
    if not callable(callback):
        raise TypeError("repository does not expose a remediation reservation hook")
    attempted = list(dict.fromkeys(str(item) for item in attempted_strategies))
    while True:
        selection = choose_remediation(
            failure_kind,
            signature,
            attempted,
            automatic_only=automatic_only,
        )
        if selection is None:
            return None
        record_details = {
            **selection.as_dict(),
            **dict(details or {}),
        }
        kwargs: dict[str, Any] = {
            "category": selection.policy.category,
            "details": record_details,
            "terminal_log_refs": tuple(terminal_log_refs),
        }
        if execution_epoch_id is not None:
            kwargs["execution_epoch_id"] = execution_epoch_id
        record = callback(
            task_id,
            logical_agent_id,
            signature.digest,
            selection.strategy.name,
            **kwargs,
        )
        if record is not None:
            return ClaimedRemediation(selection, record)
        attempted.append(selection.strategy.name)


@dataclass(slots=True)
class RemediationTracker:
    """In-memory helper for one runtime call; persistence stays with the caller."""

    states: dict[str, RecoveryState] = field(default_factory=dict)

    def choose(
        self,
        failure_kind: str,
        signature: FailureSignature,
        *,
        automatic_only: bool = True,
    ) -> RemediationSelection | None:
        selection = advance_remediation(
            failure_kind,
            signature,
            self.states.get(signature.digest),
            automatic_only=automatic_only,
        )
        if selection is not None:
            self.states[signature.digest] = selection.state
        return selection


@dataclass(slots=True)
class PolicyCrisisStrategy:
    """Hierarchy ``crisis_strategy`` adapter backed by the policy registry.

    Pass a repository to claim strategies durably through its optional
    ``claim_remediation_strategy`` hook.  Without one, this adapter keeps only
    process-local selection state and still never owns task persistence.
    """

    repository: object | None = None
    tracker: RemediationTracker = field(default_factory=RemediationTracker)
    automatic_only: bool = True
    _lock: threading.RLock = field(default_factory=threading.RLock)

    @staticmethod
    def _failure_kind(context: Mapping[str, Any]) -> str:
        declared = str(context.get("failure_kind") or "").strip().casefold()
        aliases = {
            "integration_gate": "machine_gate",
            "manager_review": "reviewer_revise",
            "manager_revise": "reviewer_revise",
        }
        candidates = [
            aliases.get(item.strip(), item.strip())
            for item in declared.split(",")
            if item.strip()
        ]
        return next(
            (
                candidate
                for candidate in candidates
                if policy_for_failure(candidate).category
                != BACKEND_SCHEDULER_UNKNOWN
            ),
            candidates[0] if candidates else "backend_failure",
        )

    def next_remediation(self, context: Mapping[str, Any]) -> dict[str, Any]:
        """Choose one category-specific strategy for hierarchy recovery."""

        failure_kind = self._failure_kind(context)
        policy = policy_for_failure(failure_kind)
        if policy.cancellation_bypass:
            return {
                "action": "abandon",
                "reason": "cancellation_bypasses_remediation",
                "failure_category": policy.category,
            }
        signature = build_failure_signature(
            failure_kind,
            contract=context.get("contract")
            or context.get("contracts")
            or context.get("contract_id"),
            target={
                "scope": context.get("scope"),
                "workstream_id": context.get("workstream_id"),
                "affected_work_item_ids": sorted(
                    context.get("affected_work_item_ids") or ()
                ),
            },
            source=context.get("manager_id")
            or context.get("logical_agent_id")
            or context.get("affected_manager_ids"),
            patch=context.get("patch")
            or context.get("patch_sha256")
            or context.get("patch_hashes"),
            test=(
                None
                if policy.category == REVIEWER_REVISE
                else (
                    context.get("test")
                    or context.get("test_output")
                    or context.get("errors")
                )
            ),
            # Free-form reviewer prose and a per-occurrence crisis id must not
            # turn the same logical failure into an unlimited series of novel
            # signatures. Target identity plus structured test/error evidence
            # is the durable boundary for exhausting category strategies.
            review={"failure_kind": failure_kind},
            request={
                "task_id": context.get("task_id"),
                "session_id": context.get("session_id"),
                "execution_epoch": context.get("execution_epoch")
                or context.get("execution_epoch_id"),
            },
        )
        selection: RemediationSelection | None
        persistence_record: Mapping[str, Any] | None = None
        task_id = str(context.get("task_id") or "")
        logical_agent_id = str(
            context.get("logical_agent_id")
            or context.get("manager_id")
            or next(iter(context.get("affected_manager_ids") or ()), "")
        )
        if self.repository is not None and task_id and logical_agent_id:
            claimed = claim_remediation_strategy(
                self.repository,
                task_id=task_id,
                logical_agent_id=logical_agent_id,
                failure_kind=failure_kind,
                signature=signature,
                execution_epoch_id=(
                    str(
                        context.get("execution_epoch_id")
                        or context.get("execution_epoch")
                    )
                    if (
                        context.get("execution_epoch_id")
                        or context.get("execution_epoch")
                    )
                    else None
                ),
                details={"crisis_context": dict(context)},
                terminal_log_refs=tuple(
                    context.get("terminal_log_refs")
                    or context.get("diagnostic_log_refs")
                    or ()
                ),
                automatic_only=self.automatic_only,
            )
            selection = claimed.selection if claimed is not None else None
            persistence_record = (
                claimed.persistence_record if claimed is not None else None
            )
        else:
            with self._lock:
                selection = self.tracker.choose(
                    failure_kind,
                    signature,
                    automatic_only=self.automatic_only,
                )
        if selection is None:
            return {
                "action": "abandon",
                "reason": "remediation_strategies_exhausted",
                "failure_category": policy.category,
                "failure_signature": signature.digest,
            }
        instructions = remediation_prompt(
            failure_kind,
            signature,
            error=str(context.get("reason") or ""),
            strategy=selection.strategy,
            diagnostic_refs={
                "task_id": context.get("task_id"),
                "session_id": context.get("session_id"),
                "execution_epoch": context.get("execution_epoch")
                or context.get("execution_epoch_id"),
                "crisis_id": context.get("crisis_id"),
            },
        )
        suggested = str(context.get("suggested_instructions") or "").strip()
        if suggested:
            instructions += f"\nManager/reviewer evidence:\n{suggested}"
        action = (
            "replan"
            if selection.strategy.actor in {"manager", "scheduler"}
            else "remediate"
        )
        return {
            "action": action,
            "reason": selection.strategy.name,
            "instructions": instructions,
            **selection.as_dict(),
            "remediation_attempt_id": (
                persistence_record.get("remediation_attempt_id")
                if persistence_record is not None
                else None
            ),
        }

    decide_remediation = next_remediation


@dataclass(frozen=True, slots=True)
class RetryDecision:
    """Backward-compatible decision enriched with structured policy metadata."""

    failure_kind: str
    retryable: bool
    category: str = ""
    remediation_hints: tuple[str, ...] = ()
    policy: FailurePolicy | None = None

    @property
    def failure_category(self) -> str:
        return self.category

    @property
    def remediation_strategies(self) -> tuple[RemediationStrategy, ...]:
        return self.policy.strategies if self.policy is not None else ()


def decision_for_failure(
    failure_kind: str | None,
    *,
    retryable: bool | None = None,
) -> RetryDecision:
    normalized = str(failure_kind or "backend_failure").strip().casefold().replace("-", "_")
    policy = policy_for_failure(normalized)
    resolved_retryable = (
        policy.retryable
        if retryable is None
        else bool(retryable)
    )
    if retryable is None and normalized in _NONRETRYABLE_KIND_OVERRIDES:
        resolved_retryable = False
    return RetryDecision(
        failure_kind=normalized,
        retryable=resolved_retryable,
        category=policy.category,
        remediation_hints=tuple(strategy.hint for strategy in policy.strategies),
        policy=policy,
    )


def retryable_for_failure(failure_kind: str | None) -> bool:
    """Return policy retryability; unknown and missing kinds fail closed."""

    normalized = str(failure_kind or "").strip().casefold().replace("-", "_")
    return bool(
        normalized
        and normalized not in _NONRETRYABLE_KIND_OVERRIDES
        and policy_for_failure(normalized).retryable
    )


def remediation_prompt(
    failure_kind: str,
    signature: FailureSignature,
    *,
    error: str = "",
    strategy: RemediationStrategy | None = None,
    diagnostic_refs: Mapping[str, Any] | None = None,
) -> str:
    """Render deterministic, category-specific recovery input for an agent."""

    policy = policy_for_failure(failure_kind)
    selected = strategy or (policy.strategies[0] if policy.strategies else None)
    lines = [
        f"## {policy.category.replace('_', ' ').upper()} RECOVERY",
        f"Failure signature: {signature.digest}",
    ]
    if selected is not None:
        lines.extend(
            [
                f"Strategy: {selected.name}",
                f"Responsible actor: {selected.actor}",
                f"Required response: {selected.prompt_variant}",
                f"Diagnostic goal: {selected.hint}",
            ]
        )
    if error:
        lines.append(f"Observed diagnostic: {str(error)[:4000]}")
    refs = {
        str(key): str(value)
        for key, value in (diagnostic_refs or {}).items()
        if value not in (None, "")
    }
    if refs:
        lines.append(
            "Diagnostic references: "
            + json.dumps(refs, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
    if policy.category == APPROVAL_POLICY:
        lines.append("Do not bypass provider policy or local approval policy.")
    if policy.cancellation_bypass:
        lines.append("Cancellation bypasses remediation and must terminate immediately.")
    return "\n".join(lines)


def prompt_inputs_for_failure(
    failure_kind: str,
    *,
    contract: Any = None,
    target: Any = None,
    source: Any = None,
    patch: Any = None,
    test: Any = None,
    review: Any = None,
    request: Any = None,
    error: str = "",
    actor: str = "",
    diagnostic_refs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return persistence- and prompt-ready failure metadata for hierarchy."""

    decision = decision_for_failure(failure_kind)
    signature = build_failure_signature(
        failure_kind,
        contract=contract,
        target=target,
        source=source,
        patch=patch,
        test=test,
        review=review,
        request=request,
    )
    return {
        "failure_kind": decision.failure_kind,
        "failure_category": decision.category,
        "failure_signature": signature.digest,
        "failure_signature_components": signature.as_dict(),
        "remediation_hints": list(decision.remediation_hints),
        "failure_actor": actor,
        "diagnostic_log_refs": dict(diagnostic_refs or {}),
        "remediation_prompt": remediation_prompt(
            failure_kind,
            signature,
            error=error,
            diagnostic_refs=diagnostic_refs,
        ),
    }


def _provider_info(exc: BaseException) -> tuple[str, int | None, bool | None, str]:
    info: Any = getattr(exc, "info", None)
    if info is None:
        return "", None, None, str(getattr(exc, "classification", "") or "").casefold()
    code_value = getattr(getattr(info, "code", None), "value", getattr(info, "code", ""))
    status = getattr(info, "status_code", None)
    declared = getattr(info, "retryable", None)
    classification = str(getattr(info, "classification", "") or "").casefold()
    return (
        str(code_value or "").casefold(),
        status,
        declared if isinstance(declared, bool) else None,
        classification,
    )


def classify_exception(exc: BaseException) -> RetryDecision:
    """Normalize runtime exceptions with cancellation taking precedence."""

    class_names = {cls.__name__.casefold() for cls in type(exc).__mro__}
    message = str(exc).casefold()
    provider_code, status_code, provider_retryable, classification = _provider_info(exc)

    # Lease loss is a ModelRequestAborted subclass but is a recoverable resource
    # failure.  Every other abort bypasses remediation.
    if "accountleaselosterror" in class_names:
        return decision_for_failure("lease_loss")
    if (
        "modelrequestaborted" in class_names
        or "providerabortederror" in class_names
        or "cancellederror" in class_names
        or "keyboardinterrupt" in class_names
    ):
        return decision_for_failure("cancelled")

    if any(
        name in class_names
        for name in (
            "taskaccountpoolexhaustederror",
            "accountpoolexhaustederror",
            "accountleaseunavailableerror",
        )
    ):
        return decision_for_failure(
            "account_pool_exhausted",
            retryable=False if "poolexhausted" in " ".join(class_names) else None,
        )
    if (
        provider_code == "authentication"
        or status_code in {401, 403}
        or "providerauthenticationerror" in class_names
        or "permissionerror" in class_names
    ):
        return decision_for_failure("authentication")
    if provider_code == "rate_limit" or status_code == 429 or "ratelimiterror" in class_names:
        return decision_for_failure("rate_limit")
    if classification in {
        "action_protocol",
        "output_protocol",
        "protocol",
        "protocol_error",
    }:
        return decision_for_failure("protocol_error")
    if (
        provider_code == "payload"
        or status_code == 400
        or "payloadrejectederror" in class_names
        or "providerpayloaderror" in class_names
    ):
        return decision_for_failure("provider_payload")
    if provider_code == "transport":
        return decision_for_failure("transport")
    if provider_code and provider_retryable is not None:
        kind = provider_code.replace("-", "_")
        return decision_for_failure(
            kind,
            retryable=bool(provider_retryable and retryable_for_failure(kind)),
        )

    if "unsupportedworkertarget" in class_names:
        return decision_for_failure("unsupported_worker_target")
    if "unsupportedpromptsource" in class_names:
        return decision_for_failure("unsupported_prompt_source")
    if "rollbackconflicterror" in class_names:
        return decision_for_failure("rollback_conflict")
    if "effectreplayconflicterror" in class_names:
        return decision_for_failure("effect_replay_conflict")
    if "lease" in message and any(
        token in message for token in ("lost", "unavailable", "khác")
    ):
        return decision_for_failure("lease_loss")
    if "connectionerror" in class_names or "timeouterror" in class_names:
        return decision_for_failure("transport")
    if any(
        name in class_names
        for name in (
            "anchorerror",
            "anchornotfounderror",
            "anchornotuniqueerror",
            "filechangederror",
            "patchrejected",
        )
    ):
        return decision_for_failure("patch_rejected")
    if any("sandbox" in name for name in class_names):
        return decision_for_failure(
            "sandbox_unavailable" if "unavailable" in message else "sandbox_blocked"
        )
    if any("scheduler" in name or "worklease" in name for name in class_names):
        return decision_for_failure("scheduler")
    if isinstance(exc, (FileNotFoundError, IsADirectoryError, UnicodeError)):
        return decision_for_failure("safety")
    return decision_for_failure("backend_failure")
