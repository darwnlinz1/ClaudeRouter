"""Fail-closed activation checks for remote and multi-user control planes."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Mapping

_NAMESPACE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")
_SECRET_SCHEMES = ("enc://", "kms://", "keyvault://", "vault://")
_AUTH_MODES = frozenset({"mtls", "oidc", "trusted_proxy"})
_ISOLATION_BACKENDS = frozenset({"container", "kubernetes", "microvm", "vm"})


class DeploymentMode(str, Enum):
    LOCAL = "local"
    REMOTE = "remote"
    MULTI_USER = "multi_user"


class RemoteActivationError(RuntimeError):
    def __init__(self, missing: tuple[str, ...]) -> None:
        self.missing = missing
        super().__init__(
            "remote activation is blocked; missing or unsafe prerequisites: "
            + ", ".join(missing)
        )


def _value(config: Mapping[str, str], name: str) -> str:
    return str(config.get(name, "")).strip()


@dataclass(frozen=True, slots=True)
class DeploymentProfile:
    mode: DeploymentMode = DeploymentMode.LOCAL
    auth_mode: str = ""
    namespace: str = ""
    encrypted_secret_store: str = ""
    rbac_policy: str = ""
    audit_sink: str = ""
    isolation_backend: str = ""
    isolation_attestation: str = ""
    tenant_claim: str = ""

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "DeploymentProfile":
        values = os.environ if environment is None else environment
        raw_mode = _value(values, "ORCH_DEPLOYMENT_MODE").casefold() or "local"
        raw_mode = raw_mode.replace("-", "_")
        try:
            mode = DeploymentMode(raw_mode)
        except ValueError as exc:
            raise RemoteActivationError(("deployment_mode",)) from exc
        return cls(
            mode=mode,
            auth_mode=_value(values, "ORCH_AUTH_MODE").casefold(),
            namespace=_value(values, "ORCH_NAMESPACE").casefold(),
            encrypted_secret_store=_value(values, "ORCH_ENCRYPTED_SECRET_STORE"),
            rbac_policy=_value(values, "ORCH_RBAC_POLICY"),
            audit_sink=_value(values, "ORCH_AUDIT_SINK"),
            isolation_backend=_value(
                values, "ORCH_REMOTE_ISOLATION_BACKEND"
            ).casefold(),
            isolation_attestation=_value(
                values, "ORCH_REMOTE_ISOLATION_ATTESTATION"
            ),
            tenant_claim=_value(values, "ORCH_TENANT_CLAIM"),
        )

    @property
    def is_remote(self) -> bool:
        return self.mode is not DeploymentMode.LOCAL

    @property
    def execution_boundary(self) -> str:
        if not self.is_remote:
            return "local-process"
        return "fenced-claim-control-plane-only"

    def missing_prerequisites(self) -> tuple[str, ...]:
        if not self.is_remote:
            return ()
        # P0 deliberately ships no remote execution boundary.  Configuration
        # values are still parsed for compatibility and diagnostics, but they
        # must never turn a local process into a remotely reachable control
        # plane.
        missing: list[str] = ["remote_deployment_disabled"]
        if self.auth_mode not in _AUTH_MODES:
            missing.append("auth")
        if (
            not _NAMESPACE.fullmatch(self.namespace)
            or self.namespace in {"default", "local"}
        ):
            missing.append("namespace")
        if not self.encrypted_secret_store.startswith(_SECRET_SCHEMES):
            missing.append("encrypted_secret_reference")
        if not self.rbac_policy:
            missing.append("rbac")
        if not self.audit_sink:
            missing.append("audit")
        if self.isolation_backend not in _ISOLATION_BACKENDS:
            missing.append("isolation_backend")
        if not self.isolation_attestation:
            missing.append("isolation_attestation")
        if self.mode is DeploymentMode.MULTI_USER and not self.tenant_claim:
            missing.append("tenant_claim")
        return tuple(missing)

    def require_ready(self) -> "DeploymentProfile":
        missing = self.missing_prerequisites()
        if missing:
            raise RemoteActivationError(missing)
        return self


def require_deployment_ready(
    environment: Mapping[str, str] | None = None,
) -> DeploymentProfile:
    return DeploymentProfile.from_environment(environment).require_ready()


def is_encrypted_secret_reference(value: str | None) -> bool:
    candidate = str(value or "").strip()
    return candidate.startswith(_SECRET_SCHEMES) and len(candidate.split("://", 1)[-1]) >= 3
