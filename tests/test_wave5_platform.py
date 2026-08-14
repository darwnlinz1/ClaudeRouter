from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.deployment import (
    DeploymentMode,
    DeploymentProfile,
    RemoteActivationError,
    require_deployment_ready,
)
from orchestrator.event_core import TaskEventCore, project_events
from orchestrator.execution import FencedRemoteClaimBroker, LocalWorkerExecutor
from orchestrator.jobs import DurableJobQueue, JobLeaseLostError, JobStatus
from orchestrator.models import (
    ApprovalPolicy,
    EventEnvelope,
    RiskLevel,
)
from orchestrator.plugin_registry import (
    COOKIE_WEB_TRANSPORT,
    PRODUCTION_PROVIDER,
    ProviderPluginError,
    ProviderPluginMetadata,
    ProviderPluginRegistry,
)
from orchestrator.policy import (
    PolicyAction,
    PolicyEffect,
    PolicyEngine,
    PolicyRequest,
)
from orchestrator.provider_adapter import ProviderResponse
from orchestrator.sandbox import IsolationLevel
from orchestrator.state_repository import StateRepository


def _event(task_id: str, event_type: str, **payload) -> EventEnvelope:
    return EventEnvelope(
        task_id=task_id,
        session_id=f"session-{task_id}",
        event_type=event_type,
        payload=payload,
    )


def test_event_replay_is_deterministic_and_checkpointed(tmp_path):
    with StateRepository(tmp_path / "events.sqlite3") as repository:
        core = TaskEventCore(repository)
        started = core.append(_event("task-replay", "task.started"))
        planned = core.append(
            _event("task-replay", "plan.created", revision=1)
        )
        finished = core.append(
            _event(
                "task-replay",
                "hierarchy_completed",
                verdict="approved",
                summary="done",
            )
        )

        first = core.rebuild(
            "task-replay", session_id="session-task-replay"
        )
        second = project_events(
            repository.replay_events("task-replay")
        )

        assert (started.sequence, planned.sequence, finished.sequence) == (1, 2, 3)
        assert first == second
        assert first.checksum == second.checksum
        assert first.status == "completed"
        assert core.verify_checkpoint("task-replay")

        assert core.append(finished) == finished
        assert core.rebuild(
            "task-replay", session_id="session-task-replay"
        ) == first


def test_durable_job_idempotency_retry_and_cancel(tmp_path):
    now = datetime(2026, 8, 12, 16, 0, tzinfo=timezone.utc)
    with StateRepository(tmp_path / "jobs.sqlite3") as repository:
        queue = DurableJobQueue(repository)
        first = queue.enqueue(
            "build",
            {"target": "one"},
            idempotency_key="build:one",
            task_id="task-jobs",
            now=now,
        )
        duplicate = queue.enqueue(
            "build",
            {"target": "one"},
            idempotency_key="build:one",
            task_id="task-jobs",
            now=now,
        )
        assert duplicate.job_id == first.job_id
        assert len(queue.list(task_id="task-jobs")) == 1
        with pytest.raises(ValueError, match="different job"):
            queue.enqueue(
                "build",
                {"target": "two"},
                idempotency_key="build:one",
                task_id="task-jobs",
                now=now,
            )

        claimed = queue.claim("worker-a", now=now, lease_ttl_seconds=30)
        assert claimed is not None
        retried = queue.fail(
            claimed.job_id,
            "worker-a",
            claimed.fencing_token,
            "temporary",
            retry_delay_seconds=5,
            now=now,
        )
        assert retried.status is JobStatus.RETRY
        assert queue.claim(
            "worker-a", now=now + timedelta(seconds=4)
        ) is None
        claimed_again = queue.claim(
            "worker-a", now=now + timedelta(seconds=5)
        )
        assert claimed_again is not None
        assert claimed_again.attempts == 2
        assert claimed_again.fencing_token > claimed.fencing_token

        pending_cancel = queue.cancel(claimed_again.job_id, now=now)
        assert pending_cancel.cancel_requested
        cancelled = queue.acknowledge_cancel(
            claimed_again.job_id,
            "worker-a",
            claimed_again.fencing_token,
            now=now,
        )
        assert cancelled.status is JobStatus.CANCELLED


def test_expired_job_is_reclaimed_with_fencing_after_restart(tmp_path):
    database = tmp_path / "restart.sqlite3"
    now = datetime(2026, 8, 12, 17, 0, tzinfo=timezone.utc)
    with StateRepository(database) as first_repository:
        first_queue = DurableJobQueue(first_repository)
        job = first_queue.enqueue(
            "work",
            {},
            idempotency_key="work:restart",
            max_attempts=3,
            now=now,
        )
        first_claim = first_queue.claim(
            "worker-old", lease_ttl_seconds=5, now=now
        )
        assert first_claim is not None

        with StateRepository(database) as restarted_repository:
            restarted_queue = DurableJobQueue(restarted_repository)
            second_claim = restarted_queue.claim(
                "worker-new",
                lease_ttl_seconds=30,
                now=now + timedelta(seconds=6),
            )
            assert second_claim is not None
            assert second_claim.job_id == job.job_id
            assert second_claim.fencing_token > first_claim.fencing_token
            with pytest.raises(JobLeaseLostError):
                first_queue.complete(
                    job.job_id,
                    "worker-old",
                    first_claim.fencing_token,
                    now=now + timedelta(seconds=6),
                )
            completed = restarted_queue.complete(
                job.job_id,
                "worker-new",
                second_claim.fencing_token,
                result={"ok": True},
                now=now + timedelta(seconds=6),
            )
            assert completed.status is JobStatus.COMPLETED


def test_local_executor_uses_durable_claim_and_result(tmp_path):
    with StateRepository(tmp_path / "local-executor.sqlite3") as repository:
        queue = DurableJobQueue(repository)
        queue.enqueue(
            "sum",
            {"values": [2, 3]},
            idempotency_key="sum:one",
        )
        executor = LocalWorkerExecutor(
            queue,
            {
                "sum": lambda payload, context: {
                    "total": sum(payload["values"]),
                    "fence": context.fencing_token,
                }
            },
            worker_id="local-worker",
        )

        result = executor.execute_once()

        assert result is not None
        assert result.status is JobStatus.COMPLETED
        assert result.result["total"] == 5


@pytest.mark.parametrize(
    ("policy_request", "effect", "reason"),
    [
        (
            PolicyRequest(
                action=PolicyAction.API_REQUEST,
                resource="/api/run",
                method="POST",
                host="localhost",
                allowed_hosts=frozenset({"localhost"}),
                session_valid=False,
                csrf_valid=False,
            ),
            PolicyEffect.DENY,
            "session_or_csrf_invalid",
        ),
        (
            PolicyRequest(
                action=PolicyAction.PROVIDER_REQUEST,
                resource=PRODUCTION_PROVIDER,
                content="authorization=Bearer abcdefghijklmnopqrstuvwxyz",
            ),
            PolicyEffect.DENY,
            "raw_secret_detected",
        ),
        (
            PolicyRequest(
                action=PolicyAction.FILE_WRITE,
                resource="src/app.py",
                scopes=("src/**",),
                risk_level=RiskLevel.HIGH,
                approval_policy=ApprovalPolicy.RISK_BASED,
            ),
            PolicyEffect.REQUIRE_APPROVAL,
            "risk_requires_approval",
        ),
        (
            PolicyRequest(
                action=PolicyAction.COMMAND_EXECUTION,
                resource="local",
                command=("powershell", "-Command", "Get-ChildItem"),
                requested_isolation=IsolationLevel.PROCESS,
                actual_isolation=IsolationLevel.PROCESS,
            ),
            PolicyEffect.DENY,
            "command_policy",
        ),
    ],
)
def test_declarative_policy_decisions(policy_request, effect, reason):
    decision = PolicyEngine().evaluate(policy_request)
    assert decision.effect is effect
    assert any(item.startswith(reason) for item in decision.reasons)


def test_policy_approval_and_audit_chain(tmp_path):
    with StateRepository(tmp_path / "audit.sqlite3") as repository:
        def audit(request, decision):
            repository.append_audit_record(
                namespace=request.subject.namespace,
                actor_id=request.subject.actor_id,
                action=request.action.value,
                resource=request.resource,
                decision=decision.effect.value,
                reasons=decision.reasons,
            )

        engine = PolicyEngine(audit_sink=audit)
        request = PolicyRequest(
            action=PolicyAction.EFFECT_APPLY,
            resource="src/app.py",
            scopes=("src/**",),
            risk_level=RiskLevel.CRITICAL,
            approval_status="approved",
        )
        assert engine.require(request).allowed
        assert engine.require(request).allowed

        records = repository.list_audit_records(namespace="local")
        assert len(records) == 2
        assert records[0]["previous_hash"] is None
        assert records[1]["previous_hash"] == records[0]["record_hash"]


def test_remote_activation_always_fails_closed(tmp_path):
    with pytest.raises(RemoteActivationError) as error:
        require_deployment_ready({"ORCH_DEPLOYMENT_MODE": "remote"})
    assert {
        "auth",
        "namespace",
        "encrypted_secret_reference",
        "rbac",
        "audit",
        "isolation_backend",
        "isolation_attestation",
        "remote_deployment_disabled",
    }.issubset(set(error.value.missing))

    profile = DeploymentProfile(
        mode=DeploymentMode.REMOTE,
        auth_mode="oidc",
        namespace="team-a",
        encrypted_secret_store="vault://orchestrator",
        rbac_policy="policy:v1",
        audit_sink="sqlite:///audit.sqlite3",
        isolation_backend="container",
        isolation_attestation="signed-policy:v1",
    )
    with StateRepository(tmp_path / "remote.sqlite3") as repository:
        queue = DurableJobQueue(repository)
        queue.enqueue(
            "remote-work",
            {},
            idempotency_key="remote:one",
            namespace="team-a",
        )
        with pytest.raises(
            RemoteActivationError,
            match="remote_deployment_disabled",
        ):
            FencedRemoteClaimBroker(queue, profile)


@dataclass(frozen=True)
class _Plugin:
    metadata: ProviderPluginMetadata

    def create_adapter(self, account):
        class Adapter:
            name = self.metadata.name

            def complete(self, request, *, should_abort=None):
                return ProviderResponse(provider=self.name, content="ok")

        return Adapter()


def test_provider_plugin_registry_locks_production_to_cookie_web():
    registry = ProviderPluginRegistry()
    plugin = _Plugin(
        ProviderPluginMetadata(
            name=PRODUCTION_PROVIDER,
            transport=COOKIE_WEB_TRANSPORT,
        )
    )
    registry.register(plugin)
    adapter = registry.create_adapter(
        {"org_id": "org", "cookie_string": "sessionKey=redacted"}
    )
    assert adapter.name == PRODUCTION_PROVIDER

    with pytest.raises(ProviderPluginError, match="cannot be replaced"):
        registry.register(plugin, replace=True)
    with pytest.raises(ProviderPluginError, match="cookie-backed"):
        ProviderPluginRegistry().register(
            _Plugin(
                ProviderPluginMetadata(
                    name=PRODUCTION_PROVIDER,
                    transport="api_key",
                    production=False,
                )
            )
        )
    with pytest.raises(ProviderPluginError, match="cookie-backed"):
        ProviderPluginRegistry().register(
            _Plugin(
                ProviderPluginMetadata(
                    name="official_api",
                    transport="api_key",
                )
            )
        )
    with pytest.raises(ProviderPluginError, match="switching is disabled"):
        registry.create_adapter({}, provider="official_api")
