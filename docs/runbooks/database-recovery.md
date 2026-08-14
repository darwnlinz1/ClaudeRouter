# Database recovery runbook

## Locked database

1. Stop creating new tasks and identify all local orchestrator processes.
2. Keep one intended service process. Do not delete `-wal` or `-shm` files.
3. Allow active transactions to finish. If the owner is dead, stop the service
   cleanly and restart once.
4. Run the read-only check below. Do not use `orchestrator_admin.py status` as
   a preflight against an old schema because opening `StateRepository` applies
   pending migrations.

```powershell
python scripts/migration_preflight.py
```

5. If integrity is still uncertain, stop the service and restore the latest
   verified backup.

## Backup

Run while the service is healthy:

```powershell
python scripts/orchestrator_admin.py backup backups\orchestrator.zip
python scripts/orchestrator_admin.py verify backups\orchestrator.zip
```

The command uses SQLite's online backup API and includes account health,
artifacts, and redacted logs.

## Restore

1. Stop the orchestrator completely.
2. Preserve the current data directory separately.
3. Verify the archive.
4. Restore only to empty targets, or pass `--overwrite` after confirming the
   preserved copy:

```powershell
python scripts/orchestrator_admin.py restore backups\orchestrator.zip --overwrite
python scripts/orchestrator_admin.py migrate
```

5. Start the service and verify task counts, latest event sequences, approvals,
   artifact downloads, and account health before resuming work.

For release upgrade, staged rollback, and explicit data compatibility, follow
the [migration/rollback handoff](migration-rollback.md).
