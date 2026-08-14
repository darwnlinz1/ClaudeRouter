"""Typed domain models for the hierarchical orchestrator.

The models deliberately contain no persistence or execution logic.  They are
safe to construct from untrusted planner output because every public model
validates its identifiers, dependencies, scopes, and state invariants.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Sequence, TypeVar
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


class PlanStatus(str, Enum):
    DRAFT = "draft"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    TESTING = "testing"
    APPROVED = "approved"
    ABANDONED = "abandoned"
    SKIPPED = "skipped"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ApprovalPolicy(str, Enum):
    NEVER = "never"
    RISK_BASED = "risk_based"
    ALWAYS = "always"


class AttemptStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class AgentRole(str, Enum):
    DIRECTOR = "director"
    MANAGER = "manager"
    WORKER = "worker"
    TESTER = "tester"


class AgentStatus(str, Enum):
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


def _require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _clean_tuple(values: Sequence[str] | str, name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise ValueError(f"{name} must be a sequence, not a string")
    result = tuple(values)
    for value in result:
        _require_text(name, value)
    if len(set(result)) != len(result):
        raise ValueError(f"{name} must not contain duplicates")
    return result


def _validate_scope(scope: str, name: str = "scope") -> None:
    _require_text(name, scope)
    normalized = scope.strip().replace("\\", "/")
    has_drive_prefix = len(normalized) >= 2 and normalized[0].isalpha() and normalized[1] == ":"
    if (
        normalized.startswith("/")
        or has_drive_prefix
        or normalized == "~"
        or normalized.startswith("~/")
        or ".." in normalized.split("/")
    ):
        raise ValueError(f"{name} must be project-relative: {scope!r}")


def _aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _freeze_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("evidence keys must be strings")
            frozen[key] = _freeze_json_value(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json_value(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class WorkContract:
    """Immutable, versioned execution contract for one unit of work."""

    id: str
    version: int = 1
    input_artifacts: tuple[str, ...] = ()
    expected_outputs: tuple[str, ...] = ()
    read_scopes: tuple[str, ...] = ()
    write_scopes: tuple[str, ...] = ()
    acceptance_criteria: tuple[str, ...] = ()
    test_requirements: tuple[str, ...] = ()
    evidence_requirements: tuple[str, ...] = ()
    consumers: tuple[str, ...] = ()
    risk_level: RiskLevel = RiskLevel.LOW
    priority: int = 0
    approval_policy: ApprovalPolicy = ApprovalPolicy.RISK_BASED

    @property
    def canonical_json(self) -> str:
        """Return the byte-stable JSON representation used for persistence."""
        return canonical_json(self)

    @property
    def sha256(self) -> str:
        """Return the immutable content identity for this exact version."""
        return canonical_sha256(self)

    @property
    def canonical_sha256(self) -> str:
        """Alias used by persistence layers that name the canonical digest."""
        return self.sha256

    def __post_init__(self) -> None:
        _require_text("id", self.id)
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise ValueError("version must be an integer of at least 1")

        for name in (
            "input_artifacts",
            "expected_outputs",
            "read_scopes",
            "write_scopes",
            "acceptance_criteria",
            "test_requirements",
            "evidence_requirements",
            "consumers",
        ):
            object.__setattr__(self, name, _clean_tuple(getattr(self, name), name))

        if not self.acceptance_criteria:
            raise ValueError("acceptance_criteria must not be empty")
        for name in (
            "expected_outputs",
            "evidence_requirements",
            "consumers",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
        for scope in self.read_scopes:
            _validate_scope(scope, "read_scope")
        for scope in self.write_scopes:
            _validate_scope(scope, "write_scope")

        try:
            risk_level = RiskLevel(self.risk_level)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(level.value for level in RiskLevel)
            raise ValueError(f"risk_level must be one of: {allowed}") from exc
        object.__setattr__(self, "risk_level", risk_level)
        try:
            approval_policy = ApprovalPolicy(self.approval_policy)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(policy.value for policy in ApprovalPolicy)
            raise ValueError(f"approval_policy must be one of: {allowed}") from exc
        object.__setattr__(self, "approval_policy", approval_policy)

        if (
            not isinstance(self.priority, int)
            or isinstance(self.priority, bool)
            or self.priority < 0
        ):
            raise ValueError("priority must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class WorkItem:
    id: str
    workstream_id: str
    title: str
    goal: str
    acceptance_criteria: tuple[str, ...]
    dependencies: tuple[str, ...] = ()
    write_scopes: tuple[str, ...] = ()
    status: WorkStatus = WorkStatus.PENDING
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    contract: WorkContract | None = None

    def __post_init__(self) -> None:
        for name in ("id", "workstream_id", "title", "goal"):
            _require_text(name, getattr(self, name))
        object.__setattr__(self, "status", WorkStatus(self.status))
        object.__setattr__(
            self,
            "acceptance_criteria",
            _clean_tuple(self.acceptance_criteria, "acceptance_criteria"),
        )
        if not self.acceptance_criteria:
            raise ValueError("acceptance_criteria must not be empty")
        object.__setattr__(self, "dependencies", _clean_tuple(self.dependencies, "dependencies"))
        if self.id in self.dependencies:
            raise ValueError("a work item cannot depend on itself")
        object.__setattr__(self, "write_scopes", _clean_tuple(self.write_scopes, "write_scopes"))
        for scope in self.write_scopes:
            _validate_scope(scope, "write_scope")
        if (
            not isinstance(self.priority, int)
            or isinstance(self.priority, bool)
            or self.priority < 0
        ):
            raise ValueError("priority must be a non-negative integer")
        contract: Any = self.contract
        if contract is None:
            contract = WorkContract(
                id=f"{self.id}-contract",
                version=1,
                input_artifacts=self.dependencies,
                expected_outputs=self.write_scopes or (self.goal,),
                write_scopes=self.write_scopes,
                acceptance_criteria=self.acceptance_criteria,
                test_requirements=self.acceptance_criteria,
                evidence_requirements=self.acceptance_criteria,
                consumers=(self.workstream_id,),
                priority=self.priority,
            )
            object.__setattr__(
                self,
                "contract",
                contract,
            )
        elif not isinstance(contract, WorkContract):
            raise ValueError("contract must be a WorkContract")
        if contract.acceptance_criteria != self.acceptance_criteria:
            raise ValueError("contract.acceptance_criteria must match the work item")
        if contract.write_scopes != self.write_scopes:
            raise ValueError("contract.write_scopes must match the work item")
        if contract.priority != self.priority:
            raise ValueError("contract.priority must match the work item")


@dataclass(frozen=True, slots=True)
class Workstream:
    id: str
    title: str
    goal: str
    acceptance_criteria: tuple[str, ...]
    dependencies: tuple[str, ...] = ()
    write_scopes: tuple[str, ...] = ()
    work_items: tuple[WorkItem, ...] = ()
    requested_worker_count: int = 1
    status: WorkStatus = WorkStatus.PENDING
    manager_agent_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    contract: WorkContract | None = None

    def __post_init__(self) -> None:
        for name in ("id", "title", "goal"):
            _require_text(name, getattr(self, name))
        object.__setattr__(self, "status", WorkStatus(self.status))
        object.__setattr__(
            self,
            "acceptance_criteria",
            _clean_tuple(self.acceptance_criteria, "acceptance_criteria"),
        )
        if not self.acceptance_criteria:
            raise ValueError("acceptance_criteria must not be empty")
        object.__setattr__(self, "dependencies", _clean_tuple(self.dependencies, "dependencies"))
        if self.id in self.dependencies:
            raise ValueError("a workstream cannot depend on itself")
        object.__setattr__(self, "write_scopes", _clean_tuple(self.write_scopes, "write_scopes"))
        for scope in self.write_scopes:
            _validate_scope(scope, "write_scope")
        object.__setattr__(self, "work_items", tuple(self.work_items))
        if self.requested_worker_count < 1:
            raise ValueError("requested_worker_count must be at least 1")
        item_ids = [item.id for item in self.work_items]
        if len(set(item_ids)) != len(item_ids):
            raise ValueError("work item ids must be unique within a workstream")
        if any(item.workstream_id != self.id for item in self.work_items):
            raise ValueError("each work item must reference its containing workstream")
        contract: Any = self.contract
        if contract is None:
            legacy_contract = self.metadata.get("work_contract")
            if isinstance(legacy_contract, Mapping):
                contract = work_contract_from_dict(legacy_contract)
            else:
                contract = WorkContract(
                    id=f"{self.id}-contract",
                    version=1,
                    input_artifacts=self.dependencies,
                    expected_outputs=self.write_scopes or (self.goal,),
                    write_scopes=self.write_scopes,
                    acceptance_criteria=self.acceptance_criteria,
                    test_requirements=self.acceptance_criteria,
                    evidence_requirements=self.acceptance_criteria,
                    consumers=("task",),
                )
            object.__setattr__(self, "contract", contract)
        elif not isinstance(contract, WorkContract):
            raise ValueError("contract must be a WorkContract")
        if contract.acceptance_criteria != self.acceptance_criteria:
            raise ValueError("contract.acceptance_criteria must match the workstream")
        if contract.write_scopes != self.write_scopes:
            raise ValueError("contract.write_scopes must match the workstream")


@dataclass(frozen=True, slots=True)
class HandoffEnvelope:
    """Append-only transfer tied to one exact persisted Work Contract."""

    handoff_id: str
    task_id: str
    contract_id: str
    contract_version: int
    source_agent_id: str
    target_agent_id: str
    signal_type: str
    artifacts: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)
    workstream_id: str | None = None
    work_item_id: str | None = None
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        for name in (
            "handoff_id",
            "task_id",
            "contract_id",
            "source_agent_id",
            "target_agent_id",
            "signal_type",
        ):
            _require_text(name, getattr(self, name))
        if self.source_agent_id == self.target_agent_id:
            raise ValueError("source_agent_id and target_agent_id must be distinct")
        for name in ("workstream_id", "work_item_id"):
            value = getattr(self, name)
            if value is not None:
                _require_text(name, value)
        if (
            not isinstance(self.contract_version, int)
            or isinstance(self.contract_version, bool)
            or self.contract_version < 1
        ):
            raise ValueError("contract_version must be an integer of at least 1")
        object.__setattr__(
            self,
            "artifacts",
            _clean_tuple(self.artifacts, "artifacts"),
        )
        if not isinstance(self.evidence, Mapping):
            raise ValueError("evidence must be a mapping")
        object.__setattr__(self, "evidence", _freeze_json_value(self.evidence))
        _aware("created_at", self.created_at)

    @property
    def producer_agent_id(self) -> str:
        """Compatibility-neutral name for the handoff producer."""
        return self.source_agent_id

    @property
    def consumer_agent_id(self) -> str:
        """Compatibility-neutral name for the handoff consumer."""
        return self.target_agent_id

    @property
    def sha256(self) -> str:
        """Return a canonical content hash for append-only deduplication."""
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class TaskPlan:
    task_id: str
    session_id: str
    goal: str
    workstreams: tuple[Workstream, ...]
    requested_manager_count: int = 1
    revision: int = 1
    status: PlanStatus = PlanStatus.DRAFT
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("task_id", "session_id", "goal"):
            _require_text(name, getattr(self, name))
        object.__setattr__(self, "status", PlanStatus(self.status))
        object.__setattr__(self, "workstreams", tuple(self.workstreams))
        if not self.workstreams:
            raise ValueError("workstreams must not be empty")
        if self.requested_manager_count < 1:
            raise ValueError("requested_manager_count must be at least 1")
        if self.requested_manager_count > len(self.workstreams):
            raise ValueError("requested_manager_count cannot exceed workstream count")
        if self.revision < 1:
            raise ValueError("revision must be at least 1")
        ids = [workstream.id for workstream in self.workstreams]
        if len(set(ids)) != len(ids):
            raise ValueError("workstream ids must be unique")
        for name in ("created_at", "updated_at"):
            _aware(name, getattr(self, name))


@dataclass(frozen=True, slots=True)
class WorkerAssignment:
    id: str
    task_id: str
    workstream_id: str
    work_item_id: str
    worker_agent_id: str
    attempt_id: str
    write_scopes: tuple[str, ...]
    assigned_at: datetime = field(default_factory=utc_now)
    lease_expires_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in (
            "id",
            "task_id",
            "workstream_id",
            "work_item_id",
            "worker_agent_id",
            "attempt_id",
        ):
            _require_text(name, getattr(self, name))
        if self.worker_agent_id == self.attempt_id:
            raise ValueError("attempt_id must be distinct from the logical worker_agent_id")
        object.__setattr__(self, "write_scopes", _clean_tuple(self.write_scopes, "write_scopes"))
        for scope in self.write_scopes:
            _validate_scope(scope, "write_scope")
        _aware("assigned_at", self.assigned_at)
        if self.lease_expires_at is not None:
            _aware("lease_expires_at", self.lease_expires_at)
        if self.lease_expires_at is not None and self.lease_expires_at <= self.assigned_at:
            raise ValueError("lease_expires_at must be after assigned_at")

    @property
    def logical_agent_id(self) -> str:
        return self.worker_agent_id

    @property
    def execution_attempt_id(self) -> str:
        return self.attempt_id


@dataclass(frozen=True, slots=True)
class Attempt:
    id: str
    task_id: str
    workstream_id: str
    work_item_id: str
    number: int
    worker_agent_id: str
    status: AttemptStatus = AttemptStatus.PENDING
    started_at: datetime | None = None
    finished_at: datetime | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def __post_init__(self) -> None:
        for name in ("id", "task_id", "workstream_id", "work_item_id", "worker_agent_id"):
            _require_text(name, getattr(self, name))
        if self.id == self.worker_agent_id:
            raise ValueError("attempt id must be distinct from the logical worker_agent_id")
        object.__setattr__(self, "status", AttemptStatus(self.status))
        if self.number < 1:
            raise ValueError("number must be at least 1")
        if self.started_at is not None:
            _aware("started_at", self.started_at)
        if self.finished_at is not None:
            _aware("finished_at", self.finished_at)
        if self.finished_at is not None and self.started_at is None:
            raise ValueError("finished_at requires started_at")
        if self.started_at and self.finished_at and self.finished_at < self.started_at:
            raise ValueError("finished_at cannot precede started_at")

    @property
    def logical_agent_id(self) -> str:
        return self.worker_agent_id

    @property
    def execution_attempt_id(self) -> str:
        return self.id


@dataclass(frozen=True, slots=True)
class AgentInstance:
    id: str
    task_id: str
    session_id: str
    role: AgentRole
    status: AgentStatus = AgentStatus.STARTING
    parent_agent_id: str | None = None
    workstream_id: str | None = None
    work_item_id: str | None = None
    model: str | None = None
    started_at: datetime = field(default_factory=utc_now)
    stopped_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("id", "task_id", "session_id"):
            _require_text(name, getattr(self, name))
        object.__setattr__(self, "role", AgentRole(self.role))
        object.__setattr__(self, "status", AgentStatus(self.status))
        if self.role == AgentRole.DIRECTOR and self.parent_agent_id is not None:
            raise ValueError("director cannot have a parent agent")
        if self.role == AgentRole.MANAGER and not self.workstream_id:
            raise ValueError("manager requires workstream_id")
        if self.role == AgentRole.WORKER and not self.work_item_id:
            raise ValueError("worker requires work_item_id")
        if self.role == AgentRole.TESTER and not self.workstream_id:
            raise ValueError("tester requires workstream_id")
        _aware("started_at", self.started_at)
        if self.stopped_at is not None:
            _aware("stopped_at", self.stopped_at)
        if self.stopped_at is not None and self.stopped_at < self.started_at:
            raise ValueError("stopped_at cannot precede started_at")


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    task_id: str
    session_id: str
    event_type: str
    payload: dict[str, Any]
    sequence: int = 0
    version: int = 1
    event_id: str = field(default_factory=lambda: new_id("evt"))
    timestamp: datetime = field(default_factory=utc_now)
    workstream_id: str | None = None
    work_item_id: str | None = None
    agent_instance_id: str | None = None
    call_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("event_id", "task_id", "session_id", "event_type"):
            _require_text(name, getattr(self, name))
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")
        if self.version < 1:
            raise ValueError("version must be at least 1")
        if not isinstance(self.payload, dict):
            raise ValueError("payload must be a dictionary")
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")


ModelT = TypeVar("ModelT")


def to_dict(value: Any) -> Any:
    """Convert models, enums, datetimes and nested values to JSON-safe data."""
    if is_dataclass(value):
        return {item.name: to_dict(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [to_dict(item) for item in value]
    if isinstance(value, list):
        return [to_dict(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): to_dict(item) for key, item in value.items()}
    return value


def canonical_json(value: Any) -> str:
    """Serialize supported model data into one deterministic JSON form."""
    return json.dumps(
        to_dict(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_sha256(value: Any) -> str:
    """Hash the UTF-8 bytes of :func:`canonical_json`."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def work_contract_sha256(contract: WorkContract) -> str:
    """Return the canonical hash of a validated Work Contract."""
    if not isinstance(contract, WorkContract):
        raise TypeError("contract must be a WorkContract")
    return canonical_sha256(contract)


def canonical_contract_json(contract: WorkContract) -> str:
    """Return canonical JSON for a validated Work Contract."""
    if not isinstance(contract, WorkContract):
        raise TypeError("contract must be a WorkContract")
    return canonical_json(contract)


def canonical_contract_sha256(contract: WorkContract) -> str:
    """Compatibility alias for :func:`work_contract_sha256`."""
    return work_contract_sha256(contract)


def work_contract_from_dict(data: Mapping[str, Any]) -> WorkContract:
    values = dict(data)
    for name in (
        "input_artifacts",
        "expected_outputs",
        "read_scopes",
        "write_scopes",
        "acceptance_criteria",
        "test_requirements",
        "evidence_requirements",
        "consumers",
    ):
        values[name] = tuple(values.get(name, ()))
    values["risk_level"] = RiskLevel(values.get("risk_level", RiskLevel.LOW))
    values["approval_policy"] = ApprovalPolicy(
        values.get("approval_policy", ApprovalPolicy.RISK_BASED)
    )
    return WorkContract(**values)


def work_item_from_dict(data: Mapping[str, Any]) -> WorkItem:
    values = dict(data)
    values["acceptance_criteria"] = tuple(values["acceptance_criteria"])
    values["dependencies"] = tuple(values.get("dependencies", ()))
    values["write_scopes"] = tuple(values.get("write_scopes", ()))
    values["status"] = WorkStatus(values.get("status", WorkStatus.PENDING))
    raw_contract = values.get("contract")
    if isinstance(raw_contract, Mapping):
        values["contract"] = work_contract_from_dict(raw_contract)
    return WorkItem(**values)


def workstream_from_dict(data: Mapping[str, Any]) -> Workstream:
    values = dict(data)
    values["acceptance_criteria"] = tuple(values["acceptance_criteria"])
    values["dependencies"] = tuple(values.get("dependencies", ()))
    values["write_scopes"] = tuple(values.get("write_scopes", ()))
    values["work_items"] = tuple(work_item_from_dict(item) for item in values.get("work_items", ()))
    values["status"] = WorkStatus(values.get("status", WorkStatus.PENDING))
    raw_contract = values.get("contract")
    if not isinstance(raw_contract, Mapping):
        metadata = values.get("metadata")
        raw_contract = metadata.get("work_contract") if isinstance(metadata, Mapping) else None
    if isinstance(raw_contract, Mapping):
        values["contract"] = work_contract_from_dict(raw_contract)
    return Workstream(**values)


def task_plan_from_dict(data: Mapping[str, Any]) -> TaskPlan:
    values = dict(data)
    values["workstreams"] = tuple(workstream_from_dict(item) for item in values["workstreams"])
    values["status"] = PlanStatus(values.get("status", PlanStatus.DRAFT))
    values["created_at"] = datetime.fromisoformat(values["created_at"])
    values["updated_at"] = datetime.fromisoformat(values["updated_at"])
    return TaskPlan(**values)


def attempt_from_dict(data: Mapping[str, Any]) -> Attempt:
    values = dict(data)
    values["status"] = AttemptStatus(values.get("status", AttemptStatus.PENDING))
    for key in ("started_at", "finished_at"):
        if values.get(key):
            values[key] = datetime.fromisoformat(values[key])
    return Attempt(**values)


def worker_assignment_from_dict(data: Mapping[str, Any]) -> WorkerAssignment:
    values = dict(data)
    values["write_scopes"] = tuple(values.get("write_scopes", ()))
    values["assigned_at"] = datetime.fromisoformat(values["assigned_at"])
    if values.get("lease_expires_at"):
        values["lease_expires_at"] = datetime.fromisoformat(values["lease_expires_at"])
    return WorkerAssignment(**values)


def agent_instance_from_dict(data: Mapping[str, Any]) -> AgentInstance:
    values = dict(data)
    values["role"] = AgentRole(values["role"])
    values["status"] = AgentStatus(values.get("status", AgentStatus.STARTING))
    values["started_at"] = datetime.fromisoformat(values["started_at"])
    if values.get("stopped_at"):
        values["stopped_at"] = datetime.fromisoformat(values["stopped_at"])
    return AgentInstance(**values)


def event_from_dict(data: Mapping[str, Any]) -> EventEnvelope:
    values = dict(data)
    values["timestamp"] = datetime.fromisoformat(values["timestamp"])
    return EventEnvelope(**values)


def handoff_from_dict(data: Mapping[str, Any]) -> HandoffEnvelope:
    values = dict(data)
    values["artifacts"] = tuple(values.get("artifacts", ()))
    values["created_at"] = datetime.fromisoformat(values["created_at"])
    return HandoffEnvelope(**values)
