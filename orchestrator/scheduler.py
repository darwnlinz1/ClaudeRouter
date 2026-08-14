"""Bounded two-level DAG scheduling and conflict-control primitives."""

from __future__ import annotations

import os
import posixpath
import threading
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Protocol, Sequence, TypeVar

from .models import TaskPlan, WorkItem, WorkStatus, Workstream


class SchedulingError(ValueError):
    """Raised when a plan cannot be scheduled safely."""


def max_parallelism_enabled() -> bool:
    """Whether the plan runs at the width it was planned at.

    In this mode every planned Manager and Worker may begin inference
    immediately. Dependency and write-scope metadata still informs prompts and
    diagnostics, but does not prevent a model call; filesystem effects remain
    serialized by the ticket executor's project lock.
    """
    return os.environ.get("ORCH_MAX_PARALLELISM", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


@dataclass(frozen=True, slots=True)
class SchedulerLimits:
    # Planning caps and execution slots are deliberately separate.  ``None``
    # keeps old settings compatible by using the corresponding parallel cap.
    max_managers: int | None = None
    max_parallel_managers: int = 4
    # Four Coders plus one dedicated Tester, matching the API/UI defaults.
    max_workers_per_manager: int = 5
    max_parallel_workers_per_manager: int | None = None
    max_parallel_workers: int = 8
    max_workstreams: int = 64
    max_work_items_per_stream: int = 128

    def __post_init__(self) -> None:
        if self.max_workers_per_manager < 2:
            raise ValueError(
                "max_workers_per_manager must reserve at least one Coder and one Tester"
            )
        for name in (
            "max_parallel_managers",
            "max_parallel_workers",
            "max_workstreams",
            "max_work_items_per_stream",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        for name in ("max_managers", "max_parallel_workers_per_manager"):
            value = getattr(self, name)
            if value is not None and value < 1:
                raise ValueError(f"{name} must be positive when configured")
        if self.max_parallel_managers > self.manager_cap:
            raise ValueError("max_parallel_managers cannot exceed max_managers")
        if self.worker_parallel_cap > self.coders_per_manager:
            raise ValueError("max_parallel_workers_per_manager cannot exceed the coder cap")

    @property
    def coders_per_manager(self) -> int:
        """One child-agent slot is permanently reserved for the Tester."""
        return self.max_workers_per_manager - 1

    @property
    def manager_cap(self) -> int:
        """Maximum logical Managers the Director may select."""
        return self.max_managers or self.max_parallel_managers

    @property
    def worker_parallel_cap(self) -> int:
        """Maximum concurrent coder pipelines for one Manager."""
        return self.max_parallel_workers_per_manager or self.coders_per_manager


class LockRepository(Protocol):
    def acquire_lease(
        self,
        resource_type: str,
        resource_id: str,
        owner_id: str,
        ttl_seconds: float,
        **kwargs: object,
    ) -> bool: ...

    def release_lease(self, resource_type: str, resource_id: str, owner_id: str) -> bool: ...

    def acquire_project_lock(
        self, project_key: str, owner_id: str, ttl_seconds: float, **kwargs: object
    ) -> bool: ...

    def release_project_lock(self, project_key: str, owner_id: str) -> bool: ...


def _validate_dag(
    nodes: Sequence[Workstream] | Sequence[WorkItem],
    *,
    label: str,
) -> None:
    node_ids = {node.id for node in nodes}
    for node in nodes:
        missing = set(node.dependencies) - node_ids
        if missing:
            raise SchedulingError(
                f"{label} {node.id!r} has unknown dependencies: {sorted(missing)}"
            )

    visiting: set[str] = set()
    visited: set[str] = set()
    by_id = {node.id: node for node in nodes}

    def visit(node_id: str, path: list[str]) -> None:
        if node_id in visiting:
            start = path.index(node_id)
            cycle = path[start:] + [node_id]
            raise SchedulingError(f"{label} DAG contains a cycle: {' -> '.join(cycle)}")
        if node_id in visited:
            return
        visiting.add(node_id)
        for dependency in by_id[node_id].dependencies:
            visit(dependency, path + [node_id])
        visiting.remove(node_id)
        visited.add(node_id)

    for current_id in by_id:
        visit(current_id, [])


def validate_plan(plan: TaskPlan, limits: SchedulerLimits | None = None) -> None:
    """Validate both the Director DAG and every Manager sub-DAG."""
    bounds = limits or SchedulerLimits()
    if len(plan.workstreams) > bounds.max_workstreams:
        raise SchedulingError(
            f"plan has {len(plan.workstreams)} workstreams; limit is {bounds.max_workstreams}"
        )
    if len(plan.workstreams) > bounds.manager_cap:
        raise SchedulingError(
            f"plan selected {len(plan.workstreams)} Managers; cap is {bounds.manager_cap}"
        )
    if plan.requested_manager_count != len(plan.workstreams):
        raise SchedulingError("requested_manager_count must equal the number of workstreams")
    if not max_parallelism_enabled():
        _validate_dag(plan.workstreams, label="workstream")
    all_item_ids: set[str] = set()
    for workstream in plan.workstreams:
        if len(workstream.work_items) > bounds.max_work_items_per_stream:
            raise SchedulingError(f"workstream {workstream.id!r} has too many work items")
        duplicates = all_item_ids.intersection(item.id for item in workstream.work_items)
        if duplicates:
            raise SchedulingError(
                f"work item ids must be task-global; duplicated: {sorted(duplicates)}"
            )
        all_item_ids.update(item.id for item in workstream.work_items)
        for item in workstream.work_items:
            if not item.write_scopes:
                raise SchedulingError(
                    f"work item {item.id!r} must declare at least one write scope"
                )
        if not max_parallelism_enabled():
            _validate_dag(workstream.work_items, label=f"work item in {workstream.id}")
        if workstream.work_items:
            if workstream.requested_worker_count != len(workstream.work_items):
                raise SchedulingError(
                    f"workstream {workstream.id!r} requested_worker_count must "
                    "equal its work item count"
                )
            if workstream.requested_worker_count > bounds.coders_per_manager:
                raise SchedulingError(
                    f"workstream {workstream.id!r} selected "
                    f"{workstream.requested_worker_count} Coders; "
                    f"cap is {bounds.coders_per_manager}"
                )


_SUCCESS_STATES = frozenset({WorkStatus.APPROVED})
_ACTIVE_STATES = frozenset({WorkStatus.RUNNING, WorkStatus.TESTING})
_UNSCHEDULED_STATES = frozenset({WorkStatus.PENDING, WorkStatus.READY})
_TERMINAL_STATES = frozenset(
    {
        WorkStatus.APPROVED,
        WorkStatus.ABANDONED,
        WorkStatus.SKIPPED,
        WorkStatus.FAILED,
        WorkStatus.BLOCKED,
        WorkStatus.CANCELLED,
    }
)
_Schedulable = TypeVar("_Schedulable", Workstream, WorkItem)


def is_terminal_work_status(status: WorkStatus | str) -> bool:
    """Return whether a work status can never be scheduled again."""
    return WorkStatus(status) in _TERMINAL_STATES


def is_successful_work_status(status: WorkStatus | str) -> bool:
    """Return whether a status satisfies dependency success."""
    return WorkStatus(status) in _SUCCESS_STATES


def _critical_path_depths(
    nodes: Sequence[_Schedulable],
) -> dict[str, int]:
    """Return deterministic downstream path lengths for DAG prioritization."""
    dependents: dict[str, list[str]] = {node.id: [] for node in nodes}
    for node in nodes:
        for dependency in node.dependencies:
            if dependency in dependents:
                dependents[dependency].append(node.id)
    memo: dict[str, int] = {}

    def depth(node_id: str) -> int:
        if node_id not in memo:
            children = dependents[node_id]
            memo[node_id] = 1 + max((depth(child) for child in children), default=0)
        return memo[node_id]

    return {node.id: depth(node.id) for node in nodes}


def ready_workstreams(
    plan: TaskPlan,
    *,
    statuses: Mapping[str, WorkStatus] | None = None,
) -> list[Workstream]:
    """Return dependency-ready workstreams in stable plan order."""
    state = {stream.id: stream.status for stream in plan.workstreams}
    if statuses:
        state.update(statuses)
    return [
        stream
        for stream in plan.workstreams
        if state.get(stream.id, stream.status) in _UNSCHEDULED_STATES
        and (
            max_parallelism_enabled()
            or all(
                state.get(dependency) in _SUCCESS_STATES for dependency in stream.dependencies
            )
        )
    ]


def ready_work_items(
    workstream: Workstream,
    *,
    statuses: Mapping[str, WorkStatus] | None = None,
) -> list[WorkItem]:
    """Return ready items, prioritizing larger ``priority`` then plan order."""
    state = {item.id: item.status for item in workstream.work_items}
    if statuses:
        state.update(statuses)
    indexed = [
        (index, item)
        for index, item in enumerate(workstream.work_items)
        if state.get(item.id, item.status) in _UNSCHEDULED_STATES
        and (
            max_parallelism_enabled()
            or all(state.get(dependency) in _SUCCESS_STATES for dependency in item.dependencies)
        )
    ]
    return [item for _, item in sorted(indexed, key=lambda pair: (-pair[1].priority, pair[0]))]


def canonical_scope(scope: str) -> str:
    value = scope.strip().replace("\\", "/")
    if value in ("", "."):
        return "."
    wildcard = value.endswith("/**") or value.endswith("/*")
    if wildcard:
        value = value.rsplit("/", 1)[0]
    normalized = posixpath.normpath(value).strip("/")
    canonical = normalized or "."
    return canonical.casefold() if os.name == "nt" else canonical


def scopes_conflict(left: Iterable[str], right: Iterable[str]) -> bool:
    """Return true when either set may write the same path or subtree."""
    left_scopes = [canonical_scope(value) for value in left] or ["."]
    right_scopes = [canonical_scope(value) for value in right] or ["."]
    for first in left_scopes:
        for second in right_scopes:
            if first == "." or second == ".":
                return True
            if first == second:
                return True
            if first.startswith(second + "/") or second.startswith(first + "/"):
                return True
    return False


class ScopeClaims:
    """Thread-safe, owner-based write-scope claims for one scheduler process."""

    def __init__(self) -> None:
        self._claims: dict[str, tuple[str, ...]] = {}
        self._waiters: list[tuple[int, str, tuple[str, ...]]] = []
        self._next_ticket = 0
        self._condition = threading.Condition(threading.RLock())

    def acquire(self, owner_id: str, scopes: Iterable[str]) -> bool:
        requested = tuple(canonical_scope(scope) for scope in scopes) or (".",)
        with self._condition:
            for existing_owner, claimed in self._claims.items():
                if existing_owner != owner_id and scopes_conflict(requested, claimed):
                    return False
            # Do not let repeated non-blocking callers bypass an older,
            # conflicting waiter.
            if any(
                waiting_owner != owner_id and scopes_conflict(requested, waiting_scopes)
                for _, waiting_owner, waiting_scopes in self._waiters
            ):
                return False
            self._claims[owner_id] = requested
            return True

    def _can_grant(
        self,
        ticket: int,
        owner_id: str,
        requested: tuple[str, ...],
    ) -> bool:
        if any(
            existing_owner != owner_id and scopes_conflict(requested, claimed)
            for existing_owner, claimed in self._claims.items()
        ):
            return False
        for waiting_ticket, waiting_owner, waiting_scopes in self._waiters:
            if waiting_ticket == ticket:
                break
            if waiting_owner != owner_id and scopes_conflict(requested, waiting_scopes):
                return False
        return True

    def wait_acquire(
        self,
        owner_id: str,
        scopes: Iterable[str],
        *,
        timeout: float | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> bool:
        """Wait efficiently until conflicting write scopes are released."""
        requested = tuple(canonical_scope(scope) for scope in scopes) or (".",)
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            if cancelled is not None and cancelled():
                return False
            ticket = self._next_ticket
            self._next_ticket += 1
            waiter = (ticket, owner_id, requested)
            self._waiters.append(waiter)
            try:
                while not self._can_grant(ticket, owner_id, requested):
                    if cancelled is not None and cancelled():
                        return False
                    if deadline is None:
                        self._condition.wait(timeout=0.25)
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._condition.wait(timeout=min(remaining, 0.25))
                if cancelled is not None and cancelled():
                    return False
                self._claims[owner_id] = requested
                return True
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
                self._condition.notify_all()

    def release(self, owner_id: str) -> bool:
        with self._condition:
            released = self._claims.pop(owner_id, None) is not None
            if released:
                self._condition.notify_all()
            return released

    def snapshot(self) -> dict[str, tuple[str, ...]]:
        with self._condition:
            return dict(self._claims)


class HierarchicalScheduler:
    """Select ready work while enforcing dynamic Manager and Worker limits."""

    def __init__(
        self,
        limits: SchedulerLimits | None = None,
        *,
        repository: LockRepository | None = None,
    ) -> None:
        self.limits = limits or SchedulerLimits()
        self.repository = repository
        self.scope_claims = ScopeClaims()
        self._selection_lock = threading.RLock()
        self._stream_wait_age: dict[str, int] = {}
        self._item_wait_age: dict[str, int] = {}

    @staticmethod
    def _update_wait_age(
        ages: dict[str, int],
        ready_ids: Sequence[str],
        selected_ids: set[str],
    ) -> None:
        ready_set = set(ready_ids)
        for node_id in list(ages):
            if node_id not in ready_set:
                ages.pop(node_id, None)
        for node_id in ready_ids:
            ages[node_id] = 0 if node_id in selected_ids else ages.get(node_id, 0) + 1

    def validate(self, plan: TaskPlan) -> None:
        validate_plan(plan, self.limits)

    def select_workstreams(
        self,
        plan: TaskPlan,
        *,
        active_manager_count: int,
        statuses: Mapping[str, WorkStatus] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> list[Workstream]:
        if active_manager_count < 0:
            raise ValueError("active_manager_count must be non-negative")
        if cancelled is not None and cancelled():
            return []
        self.validate(plan)
        requested = (
            plan.requested_manager_count
            if max_parallelism_enabled()
            else min(plan.requested_manager_count, self.limits.max_parallel_managers)
        )
        available = max(0, requested - active_manager_count)
        ready = ready_workstreams(plan, statuses=statuses)
        depths = (
            {stream.id: 0 for stream in plan.workstreams}
            if max_parallelism_enabled()
            else _critical_path_depths(plan.workstreams)
        )
        plan_order = {stream.id: index for index, stream in enumerate(plan.workstreams)}
        with self._selection_lock:
            ranked = sorted(
                ready,
                key=lambda stream: (
                    -self._stream_wait_age.get(stream.id, 0),
                    -depths[stream.id],
                    plan_order[stream.id],
                ),
            )
            selected = ranked[:available]
            self._update_wait_age(
                self._stream_wait_age,
                [stream.id for stream in ready],
                {stream.id for stream in selected},
            )
            return selected

    def worker_slots(
        self,
        workstream: Workstream,
        *,
        active_for_manager: int,
        active_global: int,
    ) -> int:
        if active_for_manager < 0 or active_global < 0:
            raise ValueError("active worker counts must be non-negative")
        if max_parallelism_enabled():
            # Every planned coder is meant to be in flight; write-scope
            # conflicts are what still serialise a pair, not a slot count.
            return max(0, workstream.requested_worker_count - active_for_manager)
        manager_limit = min(
            workstream.requested_worker_count,
            self.limits.worker_parallel_cap,
        )
        return max(
            0,
            min(
                manager_limit - active_for_manager,
                self.limits.max_parallel_workers - active_global,
            ),
        )

    def select_work_items(
        self,
        workstream: Workstream,
        *,
        active_for_manager: int,
        active_global: int,
        statuses: Mapping[str, WorkStatus] | None = None,
        active_scopes: Iterable[Iterable[str]] = (),
        cancelled: Callable[[], bool] | None = None,
    ) -> list[WorkItem]:
        if cancelled is not None and cancelled():
            return []
        slots = self.worker_slots(
            workstream,
            active_for_manager=active_for_manager,
            active_global=active_global,
        )
        selected: list[WorkItem] = []
        occupied = [tuple(scopes) for scopes in active_scopes]
        ready = ready_work_items(workstream, statuses=statuses)
        depths = (
            {item.id: 0 for item in workstream.work_items}
            if max_parallelism_enabled()
            else _critical_path_depths(workstream.work_items)
        )
        plan_order = {item.id: index for index, item in enumerate(workstream.work_items)}
        with self._selection_lock:
            ranked = sorted(
                ready,
                key=lambda item: (
                    -self._item_wait_age.get(item.id, 0),
                    -depths[item.id],
                    -item.priority,
                    plan_order[item.id],
                ),
            )
            for item in ranked:
                if cancelled is not None and cancelled():
                    selected = []
                    break
                if len(selected) >= slots:
                    break
                if not max_parallelism_enabled():
                    if any(scopes_conflict(item.write_scopes, scopes) for scopes in occupied):
                        continue
                    if any(
                        scopes_conflict(item.write_scopes, other.write_scopes)
                        for other in selected
                    ):
                        continue
                selected.append(item)
            self._update_wait_age(
                self._item_wait_age,
                [item.id for item in ready],
                {item.id for item in selected},
            )
            return selected

    def acquire_work_lease(
        self, work_item_id: str, owner_id: str, ttl_seconds: float = 60.0
    ) -> bool:
        if self.repository is None:
            raise RuntimeError("a state repository is required for durable leases")
        return self.repository.acquire_lease("work_item", work_item_id, owner_id, ttl_seconds)

    def release_work_lease(self, work_item_id: str, owner_id: str) -> bool:
        if self.repository is None:
            raise RuntimeError("a state repository is required for durable leases")
        return self.repository.release_lease("work_item", work_item_id, owner_id)

    def acquire_project_lock(
        self,
        project_key: str,
        owner_id: str,
        ttl_seconds: float = 300.0,
        *,
        purpose: str = "integration",
    ) -> bool:
        if self.repository is None:
            raise RuntimeError("a state repository is required for project locks")
        return self.repository.acquire_project_lock(
            project_key, owner_id, ttl_seconds, purpose=purpose
        )

    def release_project_lock(self, project_key: str, owner_id: str) -> bool:
        if self.repository is None:
            raise RuntimeError("a state repository is required for project locks")
        return self.repository.release_project_lock(project_key, owner_id)


# Short alias for integration code that does not need the hierarchy in its name.
Scheduler = HierarchicalScheduler
