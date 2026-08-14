# Migration and rollback handoff

Current canonical SQLite schema: `17`.

The commands below remain required procedures. Any source change requires a new
full acceptance run via the [acceptance matrix](../acceptance-matrix.md).

This procedure separates application rollback from data rollback. Never test a
downgrade against the only copy of operator data, delete migration rows, edit
`PRAGMA user_version`, remove columns, or copy a live SQLite file without its
WAL state.

`orchestrator/state_repository.py` defines
`CURRENT_SCHEMA_VERSION = 17`. It is the source of truth; do not infer the
version from an old runbook or acceptance report.

## Schema and compatibility

Forward migration accepts a new database (`user_version=0`) and applies each
missing version in its own transaction. `schema_migrations` records version,
name, and application time.

The sequence is:

1. core tasks, plans, work, attempts, events, leases, locks, and effects;
2. project fencing metadata;
3. event cursors and retention fields;
4. observability, log, and artifact records;
5. completion invariants;
6. canonical task snapshots;
7. approval requests;
8. projections, durable jobs/job fencing, and audit records;
9. audit sequencing;
10. queryable task snapshot fields and data-migration records;
11. task resume/projection and global retention indexes;
12. additive durable-effect fencing, expected target hashes, compensation
    links, and supporting indexes.
13. immutable contract versions, logical-agent identities, and typed handoffs.
14. crash-safe managed-retention claims and retention indexes.
15. sanitized per-provider LLM request-attempt records.
16. additive repair for request-attempt agent roles.
17. execution epochs and rosters, unique remediation strategies, exactly-once
    Manager reports and Director final review, and terminal dispositions/log
    references.

Migration 12 is named `durable_effect_fencing`. It does not rewrite legacy
effect rows: version-11 receipts retain their state and receive nullable
`expected_after_sha256`, `fencing_token`, `compensates_effect_id`,
`compensated_by_effect_id`, and `compensated_at` fields. It also adds an index
for pending target/hash lookup and unique partial indexes that enforce one
compensation relationship in each direction.

Migration 13 persists typed contracts, deterministic logical-agent identities,
and typed handoff envelopes in dedicated canonical tables. Migration 14 adds
recoverable claim/finalize state for managed log and artifact deletion.
Migration 17 is additive and leaves existing task, attempt, and report data
untouched while adding durable execution-recovery records.

The migration acceptance gate builds legacy version-0 and version-2 fixtures
and checks preservation, event replay, monotonic sequencing, one-time JSON task
import, and transactional rollback. A gate is evidence only when its recorded
run exits successfully against the exact source being released.

Unknown event types and additive event fields remain replayable. Removing or
narrowing a reviewed event/HTTP contract is handled by the compatibility gate.
There are no down migrations.

Treat schema `17` as incompatible with an older application unless that exact
version has been explicitly verified reading schema `17`, effect fields,
coordination/recovery tables, retention claims, and current event/task fields. Otherwise
data rollback means restoring the verified pre-upgrade archive.

Cookie contents are not stored in either database. The separate account
database contains account identity/health, cooldown, and lease state.

## Variables

Use explicit paths so backup, migration, and verification operate on the same
data:

```powershell
$Db = Join-Path $HOME ".ai_orchestrator\orchestrator.sqlite3"
$AccountDb = Join-Path $HOME ".ai_orchestrator\account_leases.sqlite3"
$Artifacts = Join-Path $HOME ".ai_orchestrator\artifacts"
$Logs = Join-Path $PWD "logs"
$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$Backup = Join-Path $PWD "backups\pre-upgrade-$Stamp.zip"
New-Item -ItemType Directory -Force (Split-Path $Backup) | Out-Null
```

## Preflight and backup

1. Record the current application version, target version, database paths, and
   most recent acceptance report applicable to the exact source tree. Confirm
   the repository has one Git root and no nested `orchestrator/.git`.
2. Disable new task creation. Let active model calls, effects, artifact writes,
   and approvals settle, then stop the service cleanly. Confirm no other
   orchestrator process owns these paths.
3. Run the read-only preflight. `status` is not a preflight command because
   opening `StateRepository` applies pending migrations.

```powershell
python scripts/migration_preflight.py `
  --database $Db `
  --account-database $AccountDb `
  --maximum-schema-version 17
```

4. Resolve any `quick_check` error or newer-than-supported schema before
   continuing. WAL/SHM warnings mean the online backup path is mandatory; never
   delete sidecars.
5. Create and verify the pre-upgrade archive:

```powershell
python scripts/orchestrator_admin.py `
  --database $Db `
  --account-database $AccountDb `
  --artifacts-root $Artifacts `
  --logs-root $Logs `
  backup $Backup

python scripts/orchestrator_admin.py verify $Backup
Get-FileHash -Algorithm SHA256 $Backup
```

Keep the archive and its SHA-256 outside the install directory. Do not proceed
unless manifest verification succeeds.

## Forward migration

1. Install the reviewed wheel/dependencies while the service remains stopped.
2. Apply migrations once with explicit data paths:

```powershell
python scripts/orchestrator_admin.py `
  --database $Db `
  --account-database $AccountDb `
  migrate
```

3. Require `schema_version: 12` and migration history 1 through 12 in the JSON
   output.
4. Run non-production fixture and compatibility gates:

```powershell
.\scripts\run-acceptance.ps1 -Profile migration -Label post-migration
.\scripts\run-acceptance.ps1 `
  -Gate compatibility `
  -Gate event-schema-drift `
  -Gate cookie-only-provider `
  -Label post-migration-contracts
```

5. Build or verify the compiled React bundle. Do not restore legacy HTML or
   JavaScript entry points.
6. Start the service through `python -m orchestrator.cli` or
   `ai-orchestrator`, which fixes the host to `127.0.0.1`.
7. Before accepting new work, verify task and terminal-state counts, canonical
   task projections, latest event sequence and reconnect replay, pending
   approvals, account health without credential material, active lease
   ownership, pending/failed effects, expected target hashes/fencing tokens,
   compensation links, one known artifact hash/download, and one balanced
   completion reconciliation.

## Rollback decision

Stop the service and preserve the migrated state first:

```powershell
$FailedUpgrade = Join-Path $PWD "backups\failed-upgrade-$Stamp.zip"
python scripts/orchestrator_admin.py `
  --database $Db `
  --account-database $AccountDb `
  --artifacts-root $Artifacts `
  --logs-root $Logs `
  backup $FailedUpgrade
python scripts/orchestrator_admin.py verify $FailedUpgrade
```

Choose exactly one path:

- Application-only rollback: allowed only when the prior release is explicitly
  certified to read schema `17`, the effect/coordination/recovery/retention fields, and
  current event/task fields. Reinstall that release
  and retain the migrated data.
- Data rollback: required when compatibility is unknown or the prior release
  cannot read schema `17`. Restore the pre-upgrade archive to empty staging paths,
  verify it, then configure the prior release to those paths.

## Staged data rollback

Do not overwrite primary paths during the first restore:

```powershell
$RollbackRoot = Join-Path $PWD "rollback-$Stamp"
$RollbackDb = Join-Path $RollbackRoot "orchestrator.sqlite3"
$RollbackAccountDb = Join-Path $RollbackRoot "account_leases.sqlite3"
$RollbackArtifacts = Join-Path $RollbackRoot "artifacts"
$RollbackLogs = Join-Path $RollbackRoot "logs"
$PriorMaximumSchemaVersion = 11  # Example for a prior schema-11 release only.

python scripts/orchestrator_admin.py verify $Backup
python scripts/orchestrator_admin.py `
  --database $RollbackDb `
  --account-database $RollbackAccountDb `
  --artifacts-root $RollbackArtifacts `
  --logs-root $RollbackLogs `
  restore $Backup

python scripts/migration_preflight.py `
  --database $RollbackDb `
  --account-database $RollbackAccountDb `
  --maximum-schema-version $PriorMaximumSchemaVersion
```

Install the prior application and point its database, account, artifact, and log
configuration at the staged restore. Do not run the newer migrator against that
restore. Verify task/event counts, approvals, account health, artifacts, and
one safe read-only operator flow. Resume interrupted tasks only after operator
review; effects that may have reached external files require hash comparison
against their receipts.

Keep both the failed-upgrade archive and the pre-upgrade archive until the
rollback has passed the full acceptance matrix applicable to the prior release.
Backups are checksummed but not encrypted; protect both archives as sensitive
operator data.
