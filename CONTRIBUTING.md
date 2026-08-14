# Contributing

## Scope

Preserve the accepted P0 boundary:

- one local operator and a service fixed to `127.0.0.1`;
- no remote or multi-user activation;
- React/TypeScript/Vite as the only UI;
- one flattened Git repository;
- cookie-backed Web Claude only;
- fail-closed strong-isolation requests;
- forward-only canonical SQLite migrations.

Changes that add remote exposure, a second UI, an alternate model transport,
silent sandbox downgrade, or nested repository metadata require a separately
approved design and are not routine contributions.

## Repository rules

Work from the repository root. `orchestrator/`, `frontend/`, `scripts/`,
`tests/`, and `docs/` share one history. Do not run `git init` below the root,
add a nested `.git`, or treat `orchestrator/` as a submodule.

Do not commit generated/runtime secrets or data, including `.env`, cookies,
salts, keys, databases, sidecars, logs, snapshots, backups, artifacts,
acceptance results, caches, or `frontend/node_modules`.

Use `.env.example` only as a variable reference. The application reads the
process environment and does not load `.env` automatically.

## Setup

Use Python 3.11 or 3.12 and Node.js 22:

```powershell
python -m pip install -r requirements-dev.txt
Push-Location frontend
npm ci
Pop-Location
```

Build and start locally:

```powershell
Push-Location frontend
npm run build
Pop-Location
python scripts/orchestrator_admin.py migrate
python -m orchestrator.cli --port 8000
```

Never change the host to a routable address for development.

## Design constraints

- Validate model output at code boundaries; prompts are not security controls.
- Canonicalize project/artifact paths beneath an explicit root before I/O.
- Preserve hash/fencing preconditions and atomic replacement for mutations.
- Keep cookie values out of events, transcripts, logs, and databases.
- Treat secret scanning and redaction as defense in depth.
- Do not pass parent credentials into sandbox child processes.
- A strong-isolation request must return blocked/unavailable without launching
  when its backend cannot attest the required capabilities.
- Use scoped external snapshots for rollback; do not create commits, stage
  files, reset worktrees, or alter Git configuration as a safety mechanism.

## Persistence and contracts

`orchestrator/state_repository.py` is the canonical schema source. The current
schema is `11`. Add migrations in increasing order, run each transactionally,
preserve existing data, and do not add down migrations or manually edit
`PRAGMA user_version`.

`orchestrator/event_schema.py` is the event source of truth. After an
intentional additive event change, regenerate:

```powershell
python -c "from orchestrator.event_schema import write_typescript_artifact; write_typescript_artifact('.')"
python scripts/acceptance_checks.py event-schema
python scripts/check_compatibility.py
```

Do not hand-edit
`frontend/src/generated/orchestrator-events.generated.ts`. Do not update
`docs/compatibility-baseline.json` only to make a check pass. Breaking contract
changes require the process in `docs/versioning.md`.

## UI changes

Edit only the React sources under `frontend/`. Do not add root `index.html`,
`main.js`, `api.js`, `style.css`, server-rendered task pages, or legacy route
fallbacks. The server must serve compiled assets only from a path contained
beneath the application root.

## Verification

Run checks proportional to the change:

```powershell
python -m ruff check orchestrator server.py scripts tests
python -m mypy --strict orchestrator/budget.py orchestrator/models.py orchestrator/effects.py orchestrator/reconciliation.py
python -m pytest -q
Push-Location frontend
npm run lint
npm run format:check
npm run typecheck
npm test -- --run
npm run build
npm run test:e2e
Pop-Location
```

For a release candidate, run:

```powershell
.\scripts\run-acceptance.ps1 -Profile full -Label post-flatten-p0
```

Report exactly what ran, its exit status, and any blocked dependency. Do not
state that checks passed based on an earlier source fingerprint or another
agent's summary.

## Change submission

Keep changes focused and reviewable. Update architecture, deployment,
operations, security, event, migration, and changelog documentation when their
contracts change. Before submission:

- review the diff for credentials and generated runtime data;
- confirm there is no nested repository metadata;
- explain migration and rollback impact;
- identify requested versus actual sandbox isolation;
- include commands and evidence for checks actually run;
- call out checks not run or blocked.

Report suspected vulnerabilities privately according to [SECURITY.md](SECURITY.md).
