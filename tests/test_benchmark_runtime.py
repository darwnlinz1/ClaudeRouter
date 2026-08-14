from __future__ import annotations

import json

import pytest

from scripts import benchmark_runtime, run_smoke_benchmark


def test_p0_defaults_cover_required_soak_scale():
    config = benchmark_runtime.BenchmarkConfig()

    assert config.task_count == 1_000
    assert config.event_count == 50_000
    assert config.broker_publish_count > config.broker_capacity
    assert config.observability_emit_count > config.observability_capacity
    assert set(benchmark_runtime.P0_THRESHOLDS) == {
        "task_projection_o1",
        "event_batch_hydration",
        "broker_memory_eviction",
        "bounded_traversal",
        "observability_retention",
    }


def test_scaled_benchmark_is_deterministic_and_json_serializable():
    report = benchmark_runtime.run_benchmarks(
        benchmark_runtime.BenchmarkConfig(
            task_count=64,
            event_count=2_000,
            broker_capacity=32,
            broker_publish_count=256,
            traversal_file_count=160,
            observability_emit_count=512,
            observability_capacity=32,
        )
    )

    assert report["passed"] is True
    checks = {check["check_id"]: check for check in report["checks"]}
    assert set(checks) == set(benchmark_runtime.P0_THRESHOLDS)
    projection = checks["task_projection_o1"]
    assert projection["metrics"]["task_count"] == 64
    assert projection["metrics"]["rows_changed"] == 1
    assert projection["metrics"]["sql_statement_kinds"] == [
        "BEGIN",
        "INSERT",
        "COMMIT",
    ]
    hydration = checks["event_batch_hydration"]
    assert hydration["metrics"]["event_count"] == 2_000
    assert hydration["invariants"]["sequences_are_contiguous"] is True
    eviction = checks["broker_memory_eviction"]
    assert eviction["metrics"]["retained_count"] == 32
    assert eviction["metrics"]["oldest_retained_sequence"] == 225
    traversal = checks["bounded_traversal"]
    assert traversal["invariants"]["output_is_deterministic"] is True
    retention = checks["observability_retention"]
    assert retention["metrics"]["first_retained_record_id"] == "record-00000480"
    assert json.loads(json.dumps(report))["schema_version"] == 1


def test_invalid_workload_rejects_non_eviction_shapes():
    with pytest.raises(ValueError, match="broker_publish_count"):
        benchmark_runtime.BenchmarkConfig(
            broker_capacity=10,
            broker_publish_count=10,
        ).validate()


def test_default_smoke_uses_fake_provider_and_removes_workspace(monkeypatch):
    monkeypatch.setenv("ORCH_COOKIES_DIR", "must-not-be-read")

    report = run_smoke_benchmark.run_smoke(timeout_seconds=15)

    assert report["passed"] is True
    assert report["provider_mode"] == "deterministic_fake"
    assert report["timed_out"] is False
    assert report["metrics"]["prompt_count"] == 1
    assert report["metrics"]["provider_call_count"] == 3
    assert report["invariants"]["cookies_require_explicit_flag"] is True
    assert report["invariants"]["workspace_removed"] is True


def test_smoke_hard_timeout_is_a_failed_json_result():
    report = run_smoke_benchmark.run_smoke(
        timeout_seconds=0.05,
        worker_delay_seconds=1.0,
    )

    assert report["passed"] is False
    assert report["timed_out"] is True
    assert report["metrics"]["worker_exit_code"] is None
    assert report["invariants"]["hard_timeout_not_exceeded"] is False
    assert report["invariants"]["workspace_removed"] is True
    assert json.loads(json.dumps(report))["check_id"] == "one_prompt_smoke"
