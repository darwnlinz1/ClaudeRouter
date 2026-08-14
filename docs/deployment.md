# Installation, deployment, and rollback

## Supported deployment

P0 supports one local operator on one machine. Start the service with
`ai-orchestrator` or `python -m orchestrator.cli`; both bind to
`127.0.0.1`. Do not expose the service through a LAN address, reverse proxy,
tunnel, container port publication, or remote desktop gateway that changes the
network trust boundary.

`ORCH_DEPLOYMENT_MODE=local` is the only accepted mode. `remote` and
`multi_user` are deliberately blocked at startup. The remote prerequisite
variables parsed for compatibility cannot enable remote execution.

## Source installation

Requires Python 3.11 or 3.12 and Node.js 22.

```powershell
python -m pip install -r requirements-dev.txt
Push-Location frontend
npm ci
npm run build
Pop-Location
python scripts/orchestrator_admin.py migrate
python -m orchestrator.cli --port 8000
```

Open `http://127.0.0.1:8000`. The React build in `frontend/dist` is required.
There is no legacy HTML/JavaScript UI fallback.

The application reads process environment variables directly; it does not
load `.env` automatically. [.env.example](../.env.example) documents safe
names and defaults. Supply overrides through the launching shell or a local
process supervisor.

## Release package

Build from the single flattened repository root:

```powershell
python scripts/build_release.py
python -m pip install --force-reinstall dist\ai_orchestrator_local-*.whl
ai-orchestrator
```

The build compiles `frontend/`, copies only the compiled React bundle into
`orchestrator/frontend_dist`, builds a wheel, verifies that the wheel contains
`index.html`, and writes `dist/SHA256SUMS`. Do not distribute an artifact whose
checksum or embedded-bundle verification fails.

The package is local software, not a hosted service image. A wheel does not
change the loopback-only or single-operator boundary.

## Credentials and local files

- Place Web Claude cookie files only in `ORCH_COOKIES_DIR` (default
  `cookies/`). That directory is ignored by Git.
- The runtime reads Web Claude session material only from those cookie files;
  there is no configurable provider or alternate model credential source.
- Keep `.env`, cookies, salts, private keys, databases, WAL/SHM files, logs,
  snapshots, backups, and generated artifacts out of commits.
- Store machine-local salts and cookie material outside the repository when
  practical and restrict filesystem permissions to the operator account.
- Treat redacted logs and verified backups as sensitive; redaction is not a
  substitute for access control.

See [SECURITY.md](../SECURITY.md) for reporting and handling guidance.

## Upgrade

The canonical state schema is version `12`. Opening `StateRepository` applies
pending migrations, including through the administrative `status` command.
Migration 12 is the additive `durable_effect_fencing` migration: it extends
effect receipts with expected target hashes, fencing tokens, and one-to-one
compensation links plus indexes while preserving existing version-11 rows.
Schema 13 adds immutable contract versions, logical-agent identities, and typed
handoff records. Schema 14 adds managed-retention claim state and indexes.
Therefore use the explicit
[migration and rollback runbook](runbooks/migration-rollback.md):

1. stop new task admission, let active effects settle, and stop the service;
2. run read-only preflight with `--maximum-schema-version 14`;
3. create and verify a backup of state SQLite, account SQLite, artifacts, and
   logs;
4. install the reviewed source or wheel;
5. run the migrator with explicit paths and require schema/history `1..14`;
6. run migration and contract checks, then start on loopback;
7. verify task projections, approvals, event replay, leases, effects,
   reconciliation, compensation links, and an artifact hash before resuming
   interrupted work.

## Rollback

Application rollback and data rollback are separate. Stop the service before
either operation.

- Reinstalling the prior application while retaining data is allowed only when
  that exact version is known to read schema `14`, nullable migrated effect
  fields, and the current event/task contracts.
- Otherwise preserve the failed-upgrade state, verify the pre-upgrade archive,
  restore it to empty staging targets, and point the prior application at
  those targets.
- Never delete migration-history rows, edit `PRAGMA user_version`, remove
  SQLite columns, copy a live database without WAL state, or restore over the
  only usable data copy.

Interrupted tasks and effects require operator review after rollback.

The complete release gate is in the
[acceptance matrix](acceptance-matrix.md), with the latest source and log hashes
in [acceptance evidence](acceptance-evidence.md). Rerun the full matrix after
any source or dependency change.
