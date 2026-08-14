"""Declarative, auditable authorization and execution policy decisions."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from .deployment import (
    DeploymentProfile,
    RemoteActivationError,
    is_encrypted_secret_reference,
)
from .models import ApprovalPolicy, RiskLevel
from .redaction import scan_secrets
from .sandbox import CommandPolicy, IsolationLevel, SandboxPolicyError


class PolicyAction(str, Enum):
    API_REQUEST = "api.request"
    PROVIDER_REQUEST = "provider.request"
    COMMAND_EXECUTION = "command.execute"
    FILE_READ = "file.read"
    FILE_WRITE = "file.write"
    EFFECT_APPLY = "effect.apply"
    REMOTE_CLAIM = "remote.claim"


class PolicyEffect(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_APPROVAL = "require_approval"


@dataclass(frozen=True, slots=True)
class PolicySubject:
    actor_id: str = "local-operator"
    roles: tuple[str, ...] = ("operator",)
    namespace: str = "local"
    authenticated: bool = True


@dataclass(frozen=True, slots=True)
class PolicyRequest:
    action: PolicyAction
    resource: str
    subject: PolicySubject = field(default_factory=PolicySubject)
    task_id: str | None = None
    method: str = ""
    host: str = ""
    origin: str = ""
    allowed_hosts: frozenset[str] = frozenset()
    allowed_origins: frozenset[str] = frozenset()
    session_valid: bool = False
    csrf_valid: bool = False
    content: str | bytes | None = None
    secret_reference: str | None = None
    command: tuple[str, ...] = ()
    command_policy: CommandPolicy | None = None
    requested_isolation: IsolationLevel = IsolationLevel.NONE
    actual_isolation: IsolationLevel = IsolationLevel.NONE
    scopes: tuple[str, ...] = ()
    risk_level: RiskLevel = RiskLevel.LOW
    approval_policy: ApprovalPolicy = ApprovalPolicy.RISK_BASED
    approval_status: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", PolicyAction(self.action))
        object.__setattr__(self, "risk_level", RiskLevel(self.risk_level))
        object.__setattr__(
            self, "approval_policy", ApprovalPolicy(self.approval_policy)
        )
        object.__setattr__(
            self, "requested_isolation", IsolationLevel(self.requested_isolation)
        )
        object.__setattr__(
            self, "actual_isolation", IsolationLevel(self.actual_isolation)
        )


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    effect: PolicyEffect
    reasons: tuple[str, ...]
    matched_rules: tuple[str, ...]

    @property
    def allowed(self) -> bool:
        return self.effect is PolicyEffect.ALLOW


PolicyEvaluator = Callable[
    [PolicyRequest, DeploymentProfile], tuple[PolicyEffect, str] | None
]
AuditSink = Callable[[PolicyRequest, PolicyDecision], None]


@dataclass(frozen=True, slots=True)
class PolicyRule:
    name: str
    actions: frozenset[PolicyAction]
    evaluator: PolicyEvaluator


def _normalize_resource(value: str) -> str | None:
    normalized = value.strip().replace("\\", "/")
    path = PurePosixPath(normalized)
    if (
        not normalized
        or path.is_absolute()
        or ":" in path.parts[0]
        or ".." in path.parts
    ):
        return None
    return path.as_posix()


def _in_scope(resource: str, scopes: Sequence[str]) -> bool:
    normalized = _normalize_resource(resource)
    if normalized is None:
        return False
    for raw_scope in scopes:
        scope = _normalize_resource(raw_scope.removesuffix("/**"))
        if scope is None:
            continue
        if normalized == scope or normalized.startswith(scope.rstrip("/") + "/"):
            return True
    return False


def _api_rule(
    request: PolicyRequest,
    _: DeploymentProfile,
) -> tuple[PolicyEffect, str] | None:
    if request.host.casefold() not in {
        value.casefold() for value in request.allowed_hosts
    }:
        return PolicyEffect.DENY, "host_not_allowed"
    if request.origin and request.origin.rstrip("/") not in request.allowed_origins:
        return PolicyEffect.DENY, "origin_not_allowed"
    if request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"} and (
        not request.session_valid or not request.csrf_valid
    ):
        return PolicyEffect.DENY, "session_or_csrf_invalid"
    return PolicyEffect.ALLOW, "local_api_boundary_satisfied"


def _secret_rule(
    request: PolicyRequest,
    _: DeploymentProfile,
) -> tuple[PolicyEffect, str] | None:
    if request.content is not None and scan_secrets(
        request.content, candidate_type=request.action.value
    ):
        return PolicyEffect.DENY, "raw_secret_detected"
    if request.secret_reference is not None and not is_encrypted_secret_reference(
        request.secret_reference
    ):
        return PolicyEffect.DENY, "unencrypted_secret_reference"
    return PolicyEffect.ALLOW, "secret_boundary_satisfied"


def _command_rule(
    request: PolicyRequest,
    _: DeploymentProfile,
) -> tuple[PolicyEffect, str] | None:
    try:
        (request.command_policy or CommandPolicy()).validate(request.command)
    except (SandboxPolicyError, ValueError) as exc:
        return PolicyEffect.DENY, f"command_policy:{exc}"
    if (
        request.requested_isolation is not IsolationLevel.NONE
        and request.actual_isolation is IsolationLevel.NONE
    ):
        return PolicyEffect.DENY, "requested_isolation_not_achieved"
    return PolicyEffect.ALLOW, "sandbox_command_allowed"


def _scope_rule(
    request: PolicyRequest,
    _: DeploymentProfile,
) -> tuple[PolicyEffect, str] | None:
    if not request.scopes or not _in_scope(request.resource, request.scopes):
        return PolicyEffect.DENY, "resource_outside_declared_scope"
    return PolicyEffect.ALLOW, "resource_within_declared_scope"


def _approval_rule(
    request: PolicyRequest,
    _: DeploymentProfile,
) -> tuple[PolicyEffect, str] | None:
    if request.approval_status == "rejected":
        return PolicyEffect.DENY, "approval_rejected"
    needs_approval = (
        request.approval_policy is ApprovalPolicy.ALWAYS
        or (
            request.approval_policy is ApprovalPolicy.RISK_BASED
            and request.risk_level in {RiskLevel.HIGH, RiskLevel.CRITICAL}
        )
    )
    if needs_approval and request.approval_status != "approved":
        return PolicyEffect.REQUIRE_APPROVAL, "risk_requires_approval"
    return PolicyEffect.ALLOW, "risk_and_approval_satisfied"


def _remote_rule(
    request: PolicyRequest,
    profile: DeploymentProfile,
) -> tuple[PolicyEffect, str] | None:
    if not profile.is_remote:
        return PolicyEffect.DENY, "remote_mode_not_enabled"
    try:
        profile.require_ready()
    except RemoteActivationError as exc:
        return PolicyEffect.DENY, "remote_prerequisites:" + ",".join(exc.missing)
    if request.subject.namespace != profile.namespace:
        return PolicyEffect.DENY, "namespace_mismatch"
    if not request.subject.authenticated:
        return PolicyEffect.DENY, "actor_not_authenticated"
    if not set(request.subject.roles).intersection({"admin", "operator", "worker"}):
        return PolicyEffect.DENY, "rbac_role_not_permitted"
    return PolicyEffect.ALLOW, "remote_claim_control_plane_allowed"


DEFAULT_RULES: tuple[PolicyRule, ...] = (
    PolicyRule(
        "api_session_origin",
        frozenset({PolicyAction.API_REQUEST}),
        _api_rule,
    ),
    PolicyRule(
        "secret_boundary",
        frozenset(
            {
                PolicyAction.PROVIDER_REQUEST,
                PolicyAction.EFFECT_APPLY,
                PolicyAction.REMOTE_CLAIM,
            }
        ),
        _secret_rule,
    ),
    PolicyRule(
        "sandbox_command",
        frozenset({PolicyAction.COMMAND_EXECUTION}),
        _command_rule,
    ),
    PolicyRule(
        "declared_scope",
        frozenset(
            {
                PolicyAction.FILE_READ,
                PolicyAction.FILE_WRITE,
                PolicyAction.EFFECT_APPLY,
            }
        ),
        _scope_rule,
    ),
    PolicyRule(
        "approval_and_risk",
        frozenset({PolicyAction.FILE_WRITE, PolicyAction.EFFECT_APPLY}),
        _approval_rule,
    ),
    PolicyRule(
        "remote_activation",
        frozenset({PolicyAction.REMOTE_CLAIM}),
        _remote_rule,
    ),
)


class PolicyEngine:
    """Evaluate applicable rules; deny beats approval, approval beats allow."""

    def __init__(
        self,
        *,
        deployment: DeploymentProfile | None = None,
        rules: Sequence[PolicyRule] = DEFAULT_RULES,
        audit_sink: AuditSink | None = None,
    ) -> None:
        self.deployment = deployment or DeploymentProfile()
        self.rules = tuple(rules)
        self.audit_sink = audit_sink

    def evaluate(self, request: PolicyRequest) -> PolicyDecision:
        outcomes: list[tuple[str, PolicyEffect, str]] = []
        for rule in self.rules:
            if request.action not in rule.actions:
                continue
            outcome = rule.evaluator(request, self.deployment)
            if outcome is not None:
                outcomes.append((rule.name, outcome[0], outcome[1]))

        if not outcomes:
            decision = PolicyDecision(
                effect=PolicyEffect.DENY,
                reasons=("no_policy_rule_allows_action",),
                matched_rules=(),
            )
        else:
            effects = {item[1] for item in outcomes}
            effect = (
                PolicyEffect.DENY
                if PolicyEffect.DENY in effects
                else PolicyEffect.REQUIRE_APPROVAL
                if PolicyEffect.REQUIRE_APPROVAL in effects
                else PolicyEffect.ALLOW
            )
            decision = PolicyDecision(
                effect=effect,
                reasons=tuple(item[2] for item in outcomes),
                matched_rules=tuple(item[0] for item in outcomes),
            )
        if self.audit_sink is not None:
            self.audit_sink(request, decision)
        return decision

    def require(self, request: PolicyRequest) -> PolicyDecision:
        decision = self.evaluate(request)
        if not decision.allowed:
            raise PermissionError(
                f"policy {decision.effect.value}: {', '.join(decision.reasons)}"
            )
        return decision
