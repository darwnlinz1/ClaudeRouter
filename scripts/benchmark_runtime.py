"""Run deterministic P0 runtime and bounded-retention performance gates."""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from orchestrator.event_broker import ReplayEventBroker
from orchestrator.file_agent import build_project_tree
from orchestrator.observability import ObservabilityRecord, StructuredObserver
from orchestrator.state_repository import StateRepository

P0_WORKLOAD = {
    "task_projection_count": 1_000,
    "event_batch_count": 50_000,
    "broker_capacity": 256,
    "broker_publish_count": 10_000,
    "traversal_file_count": 512,
    "observability_emit_count": 10_000,
    "observability_capacity": 512,
}

P0_THRESHOLDS: dict[str, dict[str, int | float]] = {
    "task_projection_o1": {
        "max_duration_ms": 250.0,
        "max_sql_statements": 3,
        "max_rows_changed": 1,
    },
    "event_batch_hydration": {
        "max_batch_ms": 15_000.0,
        "max_hydration_ms": 15_000.0,
    },
    "broker_memory_eviction": {
        "max_duration_ms": 5_000.0,
        "max_retained_events": 256,
        "max_retained_json_bytes": 131_072,
    },
    "bounded_traversal": {
        "max_duration_ms": 2_000.0,
        "max_output_entries": 128,
        "max_scanned_entries": 256,
        "max_depth": 3,
    },
    "observability_retention": {
        "max_duration_ms": 5_000.0,
        "max_retained_records": 512,
    },
}

# Compatibility name used by the existing acceptance command.
SLO = P0_THRESHOLDS


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    task_count: int = P0_WORKLOAD["task_projection_count"]
    event_count: int = P0_WORKLOAD["event_batch_count"]
    broker_capacity: int = P0_WORKLOAD["broker_capacity"]
    broker_publish_count: int = P0_WORKLOAD["broker_publish_count"]
    traversal_file_count: int = P0_WORKLOAD["traversal_file_count"]
    observability_emit_count: int = P0_WORKLOAD["observability_emit_count"]
    observability_capacity: int = P0_WORKLOAD["observability_capacity"]

    def validate(self) -> None:
        for name, value in asdict(self).items():
            if value < 1:
                raise ValueError(f"{name} must be positive")
        if self.broker_publish_count <= self.broker_capacity:
            raise ValueError("broker_publish_count must exceed broker_capacity")
        if self.observability_emit_count <= self.observability_capacity:
            raise ValueError(
                "observability_emit_count must exceed observability_capacity"
            )


@dataclass(frozen=True, slots=True)
class CheckResult:
    check_id: str
    description: str
    passed: bool
    metrics: dict[str, Any]
    thresholds: dict[str, int | float]
    invariants: dict[str, bool]


def _milliseconds(started: float) -> float:
    return round((time.perf_counter() - started) * 1_000, 3)


def _projection_check(task_count: int) -> CheckResult:
    thresholds = P0_THRESHOLDS["task_projection_o1"]
    fixed_time = "2026-01-01T00:00:00+00:00"
    with StateRepository(":memory:") as repository:
        for index in range(task_count):
            task_id = f"task-{index:06d}"
            repository.save_task_snapshot(
                task_id,
                {
                    "id": task_id,
                    "status": "QUEUED",
                    "mode": "orchestrator",
                    "phase": "queued",
                    "updated_at": fixed_time,
                },
            )

        target_id = f"task-{task_count - 1:06d}"
        statements: list[str] = []
        with repository._lock:
            before_changes = repository._connection.total_changes
            repository._connection.set_trace_callback(statements.append)
        started = time.perf_counter()
        repository.save_task_snapshot(
            target_id,
            {
                "id": target_id,
                "status": "RUNNING",
                "mode": "orchestrator",
                "phase": "worker",
                "updated_at": fixed_time,
            },
        )
        duration_ms = _milliseconds(started)
        with repository._lock:
            repository._connection.set_trace_callback(None)
            rows_changed = repository._connection.total_changes - before_changes
            stored_count = int(
                repository._connection.execute(
                    "SELECT COUNT(*) FROM task_snapshots"
                ).fetchone()[0]
            )
        updated = repository.get_task_snapshot(target_id)

    statement_kinds = [statement.lstrip().split(None, 1)[0].upper() for statement in statements]
    invariants = {
        "all_tasks_retained": stored_count == task_count,
        "one_row_changed": rows_changed == 1,
        "single_upsert_statement": statement_kinds.count("INSERT") == 1,
        "no_projection_table_scan": not any(kind == "SELECT" for kind in statement_kinds),
        "target_projection_updated": bool(
            updated
            and updated.get("status") == "RUNNING"
            and updated.get("phase") == "worker"
        ),
    }
    metrics = {
        "task_count": task_count,
        "duration_ms": duration_ms,
        "sql_statement_count": len(statements),
        "rows_changed": rows_changed,
        "sql_statement_kinds": statement_kinds,
    }
    passed = (
        all(invariants.values())
        and duration_ms <= thresholds["max_duration_ms"]
        and len(statements) <= thresholds["max_sql_statements"]
        and rows_changed <= thresholds["max_rows_changed"]
    )
    return CheckResult(
        check_id="task_projection_o1",
        description="One indexed projection upsert remains constant with many tasks",
        passed=passed,
        metrics=metrics,
        thresholds=thresholds,
        invariants=invariants,
    )


def _event_batch_hydration_check(event_count: int) -> CheckResult:
    thresholds = P0_THRESHOLDS["event_batch_hydration"]
    broker = ReplayEventBroker(max_events=event_count)
    events = [
        {
            "type": "benchmark.event",
            "index": index,
            "payload": "x" * 32,
        }
        for index in range(event_count)
    ]
    started = time.perf_counter()
    published = broker.put_many(events)
    batch_ms = _milliseconds(started)
    started = time.perf_counter()
    hydrated = broker.events_after(0, require_contiguous=True)
    hydration_ms = _milliseconds(started)
    broker.close()

    expected_checksum = event_count * (event_count - 1) // 2
    hydrated_checksum = sum(int(event["index"]) for event in hydrated)
    sequences = [int(event["_seq"]) for event in hydrated]
    invariants = {
        "batch_published_once": published == event_count,
        "all_events_hydrated": len(hydrated) == event_count,
        "payload_checksum_matches": hydrated_checksum == expected_checksum,
        "sequences_are_contiguous": bool(
            sequences
            and sequences[0] == 1
            and sequences[-1] == event_count
            and sum(sequences) == event_count * (event_count + 1) // 2
        ),
    }
    metrics = {
        "event_count": event_count,
        "batch_ms": batch_ms,
        "hydration_ms": hydration_ms,
        "events_per_second": round(
            event_count / max((batch_ms + hydration_ms) / 1_000, 0.000_001),
            1,
        ),
        "hydrated_checksum": hydrated_checksum,
    }
    passed = (
        all(invariants.values())
        and batch_ms <= thresholds["max_batch_ms"]
        and hydration_ms <= thresholds["max_hydration_ms"]
    )
    return CheckResult(
        check_id="event_batch_hydration",
        description="A fixed event batch hydrates completely with contiguous cursors",
        passed=passed,
        metrics=metrics,
        thresholds=thresholds,
        invariants=invariants,
    )


def _broker_eviction_check(capacity: int, publish_count: int) -> CheckResult:
    thresholds = P0_THRESHOLDS["broker_memory_eviction"]
    broker = ReplayEventBroker(max_events=capacity)
    started = time.perf_counter()
    chunk_size = 512
    for offset in range(0, publish_count, chunk_size):
        broker.put_many(
            [
                {"type": "broker.sample", "index": index, "payload": "y" * 32}
                for index in range(offset, min(offset + chunk_size, publish_count))
            ]
        )
    duration_ms = _milliseconds(started)
    retained = broker.events_after(0)
    broker.close()

    expected_oldest = publish_count - capacity + 1
    retained_json_bytes = sum(
        len(json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        for event in retained
    )
    invariants = {
        "retention_is_capacity_bounded": len(retained) == capacity,
        "oldest_event_was_evicted": bool(
            retained and int(retained[0]["_seq"]) == expected_oldest
        ),
        "latest_event_is_retained": bool(
            retained and int(retained[-1]["_seq"]) == publish_count
        ),
        "retained_payload_is_latest_window": bool(
            retained and int(retained[0]["index"]) == publish_count - capacity
        ),
    }
    metrics = {
        "published_count": publish_count,
        "capacity": capacity,
        "retained_count": len(retained),
        "oldest_retained_sequence": int(retained[0]["_seq"]) if retained else None,
        "latest_sequence": publish_count,
        "retained_json_bytes": retained_json_bytes,
        "duration_ms": duration_ms,
    }
    passed = (
        all(invariants.values())
        and duration_ms <= thresholds["max_duration_ms"]
        and len(retained) <= thresholds["max_retained_events"]
        and retained_json_bytes <= thresholds["max_retained_json_bytes"]
    )
    return CheckResult(
        check_id="broker_memory_eviction",
        description="Replay memory evicts old events at a fixed capacity",
        passed=passed,
        metrics=metrics,
        thresholds=thresholds,
        invariants=invariants,
    )


def _bounded_traversal_check(root: Path, file_count: int) -> CheckResult:
    thresholds = P0_THRESHOLDS["bounded_traversal"]
    wide = root / "wide"
    wide.mkdir(parents=True)
    for index in range(file_count):
        (wide / f"file-{index:06d}.txt").write_text("x", encoding="utf-8")
    deep = root
    for index in range(int(thresholds["max_depth"]) + 2):
        deep = deep / f"depth-{index}"
        deep.mkdir()
    (deep / "must-not-appear.txt").write_text("hidden", encoding="utf-8")

    kwargs = {
        "max_entries": int(thresholds["max_output_entries"]),
        "max_depth": int(thresholds["max_depth"]),
        "max_scanned_entries": int(thresholds["max_scanned_entries"]),
    }
    started = time.perf_counter()
    first = build_project_tree(root, **kwargs)
    duration_ms = _milliseconds(started)
    second = build_project_tree(root, **kwargs)
    paths = [line for line in first.splitlines() if not line.startswith("...")]
    max_observed_depth = max((path.count("/") for path in paths), default=0)
    invariants = {
        "output_is_deterministic": first == second,
        "output_entry_bound_holds": len(paths) <= thresholds["max_output_entries"],
        "depth_bound_holds": max_observed_depth <= thresholds["max_depth"],
        "deep_file_is_excluded": "must-not-appear.txt" not in first,
        "truncation_is_reported": "... (đã cắt bớt danh sách)" in first,
    }
    metrics = {
        "generated_file_count": file_count + 1,
        "configured_max_scanned_entries": thresholds["max_scanned_entries"],
        "output_entry_count": len(paths),
        "max_observed_depth": max_observed_depth,
        "duration_ms": duration_ms,
    }
    passed = all(invariants.values()) and duration_ms <= thresholds["max_duration_ms"]
    return CheckResult(
        check_id="bounded_traversal",
        description="Project discovery obeys entry, scan, and depth bounds",
        passed=passed,
        metrics=metrics,
        thresholds=thresholds,
        invariants=invariants,
    )


def _observability_retention_check(
    emit_count: int,
    capacity: int,
) -> CheckResult:
    thresholds = P0_THRESHOLDS["observability_retention"]
    observer = StructuredObserver(max_records=capacity)
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    started = time.perf_counter()
    for index in range(emit_count):
        observer.emit(
            ObservabilityRecord(
                kind="metric",
                name="benchmark.retention",
                value=float(index),
                record_id=f"record-{index:08d}",
                timestamp=timestamp,
            ),
            persist=False,
        )
    duration_ms = _milliseconds(started)
    retained = observer.records()
    expected_first = f"record-{emit_count - capacity:08d}"
    expected_last = f"record-{emit_count - 1:08d}"
    invariants = {
        "record_count_is_bounded": len(retained) == capacity,
        "old_records_are_evicted": bool(
            retained and retained[0].record_id == expected_first
        ),
        "latest_record_is_retained": bool(
            retained and retained[-1].record_id == expected_last
        ),
    }
    metrics = {
        "emitted_count": emit_count,
        "capacity": capacity,
        "retained_count": len(retained),
        "first_retained_record_id": retained[0].record_id if retained else None,
        "last_retained_record_id": retained[-1].record_id if retained else None,
        "duration_ms": duration_ms,
    }
    passed = (
        all(invariants.values())
        and duration_ms <= thresholds["max_duration_ms"]
        and len(retained) <= thresholds["max_retained_records"]
    )
    return CheckResult(
        check_id="observability_retention",
        description="In-process observability keeps only the configured latest window",
        passed=passed,
        metrics=metrics,
        thresholds=thresholds,
        invariants=invariants,
    )


def run_benchmarks(config: BenchmarkConfig | None = None) -> dict[str, Any]:
    workload = config or BenchmarkConfig()
    workload.validate()
    with tempfile.TemporaryDirectory(prefix="orchestrator-p0-benchmark-") as temporary:
        checks = [
            _projection_check(workload.task_count),
            _event_batch_hydration_check(workload.event_count),
            _broker_eviction_check(
                workload.broker_capacity,
                workload.broker_publish_count,
            ),
            _bounded_traversal_check(
                Path(temporary) / "traversal",
                workload.traversal_file_count,
            ),
            _observability_retention_check(
                workload.observability_emit_count,
                workload.observability_capacity,
            ),
        ]
    return {
        "schema_version": 1,
        "profile": "p0-deterministic",
        "workload": asdict(workload),
        "thresholds": P0_THRESHOLDS,
        "checks": [asdict(check) for check in checks],
        "passed": all(check.passed for check in checks),
    }


def benchmark(
    event_count: int = P0_WORKLOAD["event_batch_count"],
    scope_checks: int | None = None,
) -> dict[str, Any]:
    """Compatibility wrapper for callers of the previous benchmark function."""

    del scope_checks
    return run_benchmarks(BenchmarkConfig(event_count=event_count))


def _write_report(report: dict[str, Any], output: Path | None) -> None:
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8", newline="\n")
    print(rendered, end="")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=P0_WORKLOAD["task_projection_count"])
    parser.add_argument("--events", type=int, default=P0_WORKLOAD["event_batch_count"])
    parser.add_argument("--broker-capacity", type=int, default=P0_WORKLOAD["broker_capacity"])
    parser.add_argument(
        "--broker-events",
        type=int,
        default=P0_WORKLOAD["broker_publish_count"],
    )
    parser.add_argument(
        "--tree-files",
        type=int,
        default=P0_WORKLOAD["traversal_file_count"],
    )
    parser.add_argument(
        "--observability-records",
        type=int,
        default=P0_WORKLOAD["observability_emit_count"],
    )
    parser.add_argument(
        "--observability-capacity",
        type=int,
        default=P0_WORKLOAD["observability_capacity"],
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--assert-slo",
        action="store_true",
        help="return a non-zero exit code when any threshold fails",
    )
    parser.add_argument("--scope-checks", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    config = BenchmarkConfig(
        task_count=args.tasks,
        event_count=args.events,
        broker_capacity=args.broker_capacity,
        broker_publish_count=args.broker_events,
        traversal_file_count=args.tree_files,
        observability_emit_count=args.observability_records,
        observability_capacity=args.observability_capacity,
    )
    try:
        config.validate()
    except ValueError as error:
        parser.error(str(error))
    report = run_benchmarks(config)
    _write_report(report, args.output)
    return int(args.assert_slo and not report["passed"])


if __name__ == "__main__":
    raise SystemExit(main())
