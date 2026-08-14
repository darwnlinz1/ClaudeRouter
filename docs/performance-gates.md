# P0 performance and soak gates

These gates are deterministic, local checks for the runtime paths most likely
to create an operator-visible outage. They use fixed inputs and write only to
in-memory databases or disposable temporary directories. They do not mutate a
project, production database, or retained task state.

## Runtime benchmark

Run the release gate:

```powershell
python scripts/benchmark_runtime.py --assert-slo --output benchmark-report.json
```

The default `p0-deterministic` workload checks:

- `task_projection_o1`: seed 1,000 canonical task projections, then trace one
  target upsert. The gate permits one changed row, one `INSERT ... ON
  CONFLICT`, three total SQLite statements including transaction boundaries,
  and 250 ms. This detects an accidental all-task rewrite or projection scan.
- `event_batch_hydration`: publish and hydrate 50,000 deterministic events.
  Every payload checksum and sequence must match. Batch and hydration each
  have a 15-second ceiling.
- `broker_memory_eviction`: publish 10,000 events into a 256-event replay
  broker. Exactly the newest 256 records must remain, the serialized retained
  window must stay below 128 KiB, and the check has a 5-second ceiling.
- `bounded_traversal`: traverse a generated wide/deep project with limits of
  128 output entries, 256 scanned entries, and depth 3. Two traversals must be
  byte-for-byte identical, deep content must be excluded, and traversal has a
  2-second ceiling.
- `observability_retention`: emit 10,000 records into a 512-record observer.
  Exactly the newest retention window must remain, with a 5-second ceiling.

The JSON document has `schema_version`, `profile`, `workload`, `thresholds`,
individual `checks`, and an aggregate `passed` field. Each check includes raw
`metrics`, deterministic `invariants`, and the thresholds applied. Elapsed
times are evidence; checksums, cursor ranges, SQL shape, and retention windows
make correctness failures reproducible.

`--assert-slo` returns exit code 1 when any check fails. Without it, the script
still emits the complete report for local baseline collection. Workload flags
exist for diagnosis, but release evidence must use the defaults above.

## One-prompt smoke benchmark

The default smoke run is offline and cannot discover real cookie files:

```powershell
python scripts/run_smoke_benchmark.py --timeout-seconds 30 --output smoke-report.json
```

It starts a child process, creates a disposable project, submits one prompt,
uses a deterministic fake provider for the Supervisor/Worker/Reviewer
exchange, verifies the fixture change, and deletes the project. The parent
enforces a hard process timeout. A timeout, worker error, incomplete task, or
cleanup failure produces `passed: false` and a non-zero exit code.

The report includes provider mode, timeout state, wall and worker durations,
prompt/turn/provider-call counts, process exit code, and cleanup invariants.

Real cookie-backed provider access is opt-in only:

```powershell
python scripts/run_smoke_benchmark.py --use-real-cookies --timeout-seconds 120
```

Do not use that flag in deterministic CI. It intentionally enables the
configured production provider and therefore depends on local credentials,
network behavior, account health, and model output. The project remains
disposable and the same parent-process timeout still applies.

## Focused verification

```powershell
python -m pytest -q tests/test_benchmark_runtime.py
python -m ruff check scripts/benchmark_runtime.py scripts/run_smoke_benchmark.py tests/test_benchmark_runtime.py
```
