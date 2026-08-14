# Acceptance matrix

This matrix is the repeatable release gate for the accepted P0 product:
local-only service, React-only UI, fail-closed repository sandbox, and one
flattened repository.

**Evidence status: CURRENT for the recorded source fingerprint.** The latest
[acceptance evidence](acceptance-evidence.md) records a full 19/19 schema-14 run
on the flattened repository. It becomes stale after any source or dependency
change.

## Release preconditions

Before running the matrix:

- the repository has one Git root; `orchestrator/` and `frontend/` contain no
  nested `.git` metadata;
- root legacy UI files and routes are absent; `frontend/` is the only UI
  source;
- no `.env`, cookies, tokens, keys, databases, logs, backups, build output, or
  acceptance output is staged;
- `ORCH_DEPLOYMENT_MODE` is unset or `local`;
- the React lockfile is installed with `npm ci`;
- Playwright Chromium is installed;
- no other process modifies source during the run.

## Run on Windows

Prerequisites are Python 3.11+, Node.js 22+, the Python development
requirements, frontend lockfile dependencies, and Playwright Chromium:

```powershell
python -m pip install -r requirements-dev.txt
Push-Location frontend
npm ci
npx playwright install chromium
Pop-Location
```

List or run the matrix:

```powershell
.\scripts\run-acceptance.ps1 -List
.\scripts\run-acceptance.ps1 -Profile full -Label final-schema14
```

The runner does not modify application databases. It writes exact stdout and
stderr logs plus JSON and Markdown reports under
`$env:TEMP\ai-orchestrator-acceptance` unless `-ResultsDir` is supplied. Every
report records the commit, dirty-tree state, command, exit code, duration, and
log SHA-256. The accepted provenance boundary is the repository root. Legacy
nested-repository metadata, if detected by the runner, is diagnostic and
disqualifies the topology from P0 release acceptance. A missing dependency is
`BLOCKED`, not `PASSED`.

Useful focused reruns:

```powershell
.\scripts\run-acceptance.ps1 -Profile backend -Label backend-rerun
.\scripts\run-acceptance.ps1 -Profile frontend -Label frontend-rerun
.\scripts\run-acceptance.ps1 -Profile migration -Label migration-rerun
.\scripts\run-acceptance.ps1 -Gate cookie-only-provider -Gate event-schema-drift
```

`-SkipE2E` is only for diagnosis. A release acceptance run must include E2E.
`-AllowBlocked` changes the process exit code but does not turn blocked gates
into passes.

## Gate coverage

- `backend-lint`, `backend-types`, and `backend-full` cover the complete Python
  quality baseline. The complete suite includes the invariant that remote
  activation remains disabled.
- `sqlite-migration` exercises dynamically built legacy version 0 and version
  2 fixtures, ordered forward migration through canonical schema `14`, JSON
  task import, preservation of legacy rows/events, and rollback of a
  deliberately failed migration.
- `backend-full` runs the complete backend suite, including migrations for
  durable effects, contracts/identities/handoffs, and managed retention. Only
  the final report establishes the exact tests and source fingerprint executed.
- `replay-reconciliation` covers durable sequence cursors, reconnect replay,
  resume checkpoints, and the terminal partition invariant.
- `fanout-contracts` covers dynamic manager/worker selection, caps versus
  execution slots, DAG dependencies, scope fairness, and versioned Work
  Contracts, immutable contract-version storage, stable identity resolution,
  and exact-version typed handoffs in canonical schema 13.
- `effects-leases` covers idempotent effect receipts, expected target hashes,
  crash reconciliation, compensation linkage through ticket execution,
  account leases, project leases, heartbeats, and stale-token fencing.
- `sandbox-security` covers fail-closed strong-isolation requests, honest
  process-only reporting, command/environment policy, path containment,
  external snapshot rollback, secret scanning/redaction, and local HTTP
  session/origin/CSRF enforcement.
- `retention-artifacts` covers terminal-event retention, pins, artifact
  manifests and hashes, online backup verification, safe restore, and
  crash/tamper reconciliation.
- `compatibility` rejects removal or narrowing of the reviewed HTTP/event
  surface.
- `event-schema-drift` byte-compares generated TypeScript to the deterministic
  Python event schema.
- `cookie-only-provider` rejects Anthropic Messages endpoints, Anthropic
  API-key environment variables, API-key headers, parameters, and CLI flags in
  production/config files. Provider behavior tests separately cover bounded
  Retry-After, byte-stable 429 replay, complete-attempt streaming, and one
  terminal outcome.
- `frontend-lint`, `frontend-format`, `frontend-types`, `frontend-tests`, and
  `frontend-build` cover the React operator application.
- `frontend-e2e` covers task routing, destructive confirmation, browser runtime
  errors, and horizontal-overflow/operability checks at 1024x768 and 390x844.

The topology and absence of legacy UI entry points are release preconditions
in addition to automated gate results. Focused profiles are diagnostic and
cannot replace the full profile.

## Evidence policy

Only a gate with exit code zero is recorded as passed. This document records no
current PASS result. A historical count, prior agent statement, stale source
fingerprint, or partial focused run is not full-matrix evidence.

A release report is usable only when:

- all selected full-profile gates passed with no blocked gate;
- source did not change during the run;
- the recorded source fingerprint matches the reviewed flattened tree;
- the working-tree state is disclosed and understood;
- the JSON/Markdown reports and individual log hashes are retained;
- the run did not use `-SkipE2E` or reinterpret `-AllowBlocked` as success.

Do not rewrite [acceptance-evidence.md](acceptance-evidence.md) to imply that an
old run covered new source. Add or replace evidence only with a newly executed,
hashed report for the exact source under review.
