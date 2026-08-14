"""Worker execution backends for local work and fenced remote claims."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol

from .deployment import DeploymentProfile, RemoteActivationError
from .jobs import DurableJob, DurableJobQueue, JobStatus
from .policy import (
    PolicyAction,
    PolicyEngine,
    PolicyRequest,
    PolicySubject,
)


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    job_id: str
    worker_id: str
    namespace: str
    fencing_token: int
    attempt: int
    execution_mode: str
    isolation_statement: str


@dataclass(frozen=True, slots=True)
class ExecutionClaim:
    job: DurableJob
    context: ExecutionContext


class JobHandler(Protocol):
    def __call__(
        self,
        payload: Mapping[str, Any],
        context: ExecutionContext,
    ) -> Mapping[str, Any] | None: ...


class LocalWorkerExecutor:
    """Execute one durable job in the current process."""

    def __init__(
        self,
        queue: DurableJobQueue,
        handlers: Mapping[str, JobHandler],
        *,
        worker_id: str,
        namespace: str = "local",
        lease_ttl_seconds: float = 60,
    ) -> None:
        self.queue = queue
        self.handlers = dict(handlers)
        self.worker_id = worker_id
        self.namespace = namespace
        self.lease_ttl_seconds = lease_ttl_seconds

    def execute_once(self, *, now: datetime | None = None) -> DurableJob | None:
        job = self.queue.claim(
            self.worker_id,
            namespace=self.namespace,
            lease_ttl_seconds=self.lease_ttl_seconds,
            kinds=tuple(sorted(self.handlers)),
            now=now,
        )
        if job is None:
            return None
        context = ExecutionContext(
            job_id=job.job_id,
            worker_id=self.worker_id,
            namespace=job.namespace,
            fencing_token=job.fencing_token,
            attempt=job.attempts,
            execution_mode="local",
            isolation_statement=(
                "in-process handler with durable fencing; no network or "
                "filesystem isolation is claimed"
            ),
        )
        handler = self.handlers.get(job.kind)
        if handler is None:
            return self.queue.fail(
                job.job_id,
                self.worker_id,
                job.fencing_token,
                f"no local handler registered for job kind {job.kind}",
                retryable=False,
                now=now,
            )
        try:
            result = handler(job.payload, context)
        except Exception as exc:
            return self.queue.fail(
                job.job_id,
                self.worker_id,
                job.fencing_token,
                str(exc) or type(exc).__name__,
                now=now,
            )
        current = self.queue.get(job.job_id)
        if current is not None and current.cancel_requested:
            return self.queue.acknowledge_cancel(
                job.job_id,
                self.worker_id,
                job.fencing_token,
                now=now,
            )
        return self.queue.complete(
            job.job_id,
            self.worker_id,
            job.fencing_token,
            result=result,
            now=now,
        )


class FencedRemoteClaimBroker:
    """Remote control-plane claims; transport and network isolation are external.

    This class never opens a network listener and never labels a claim as
    isolated.  A configured deployment may hand the returned payload to an
    authenticated external worker, which must present the fencing token on all
    subsequent state changes.
    """

    def __init__(
        self,
        queue: DurableJobQueue,
        deployment: DeploymentProfile,
        *,
        policy: PolicyEngine | None = None,
        lease_ttl_seconds: float = 60,
    ) -> None:
        self.queue = queue
        if not deployment.is_remote:
            raise RemoteActivationError(("deployment_mode",))
        self.deployment = deployment.require_ready()
        self.policy = policy or PolicyEngine(deployment=deployment)
        self.lease_ttl_seconds = lease_ttl_seconds

    def claim(
        self,
        *,
        worker_id: str,
        roles: tuple[str, ...],
        secret_reference: str,
        kinds: tuple[str, ...] = (),
        now: datetime | None = None,
    ) -> ExecutionClaim | None:
        subject = PolicySubject(
            actor_id=worker_id,
            roles=roles,
            namespace=self.deployment.namespace,
            authenticated=True,
        )
        self.policy.require(
            PolicyRequest(
                action=PolicyAction.REMOTE_CLAIM,
                resource=f"jobs/{self.deployment.namespace}",
                subject=subject,
                secret_reference=secret_reference,
            )
        )
        job = self.queue.claim(
            worker_id,
            namespace=self.deployment.namespace,
            lease_ttl_seconds=self.lease_ttl_seconds,
            kinds=kinds,
            now=now,
        )
        if job is None:
            return None
        return ExecutionClaim(
            job=job,
            context=ExecutionContext(
                job_id=job.job_id,
                worker_id=worker_id,
                namespace=job.namespace,
                fencing_token=job.fencing_token,
                attempt=job.attempts,
                execution_mode="remote_claim",
                isolation_statement=(
                    "fenced control-plane claim only; the orchestrator has not "
                    "verified worker network, host, or filesystem isolation"
                ),
            ),
        )

    def heartbeat(
        self,
        claim: ExecutionClaim,
        *,
        now: datetime | None = None,
    ) -> DurableJob:
        return self.queue.heartbeat(
            claim.job.job_id,
            claim.context.worker_id,
            claim.context.fencing_token,
            lease_ttl_seconds=self.lease_ttl_seconds,
            now=now,
        )

    def complete(
        self,
        claim: ExecutionClaim,
        result: Mapping[str, Any] | None = None,
        *,
        now: datetime | None = None,
    ) -> DurableJob:
        return self.queue.complete(
            claim.job.job_id,
            claim.context.worker_id,
            claim.context.fencing_token,
            result=result,
            now=now,
        )

    def fail(
        self,
        claim: ExecutionClaim,
        error: str,
        *,
        retryable: bool = False,
        now: datetime | None = None,
    ) -> DurableJob:
        return self.queue.fail(
            claim.job.job_id,
            claim.context.worker_id,
            claim.context.fencing_token,
            error,
            retryable=retryable,
            now=now,
        )

    def acknowledge_cancel(
        self,
        claim: ExecutionClaim,
        *,
        now: datetime | None = None,
    ) -> DurableJob:
        current = self.queue.get(claim.job.job_id)
        if current is None or current.status is not JobStatus.RUNNING:
            raise RuntimeError("remote claim is not running")
        return self.queue.acknowledge_cancel(
            claim.job.job_id,
            claim.context.worker_id,
            claim.context.fencing_token,
            now=now,
        )
