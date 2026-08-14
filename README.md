# Local AI Orchestrator

A single-operator, local-only Director → Managers → Workers → Testers coding
system. The Python service coordinates typed work contracts, durable SQLite
state, account leases, approvals, budgets, effects, and completion
reconciliation. The operator interface is React + TypeScript + Vite.

## Accepted P0 boundary

- Run the HTTP service on `127.0.0.1`. Non-loopback peers and unapproved Host
  values are rejected; API policy also enforces same-origin requests and a
  local session/CSRF token where required.
- Remote and multi-user deployment are not supported. Setting
  `ORCH_DEPLOYMENT_MODE` to a non-local value fails startup even if remote
  prerequisite variables are populated.
- `frontend/` is the only UI source. The server serves its compiled React
  bundle; legacy root HTML/JavaScript routes and files are not supported.
- The project is one flattened Git repository. `orchestrator/` is a Python
  package, not a nested repository.
- Repository test commands request strong isolation and fail closed without an
  attested backend. Process grouping alone is reported as process-only and is
  reserved for trusted internal commands.

This boundary is not a remote-access or multi-tenant security model. Do not
publish the port through a proxy, tunnel, container port mapping, or LAN bind.

## Repository layout

```text
orchestrator/              Python runtime and persistence package
  state_repository.py      canonical SQLite schema and migrations
  sandbox.py               command policy and attested isolation boundary
  safety.py                external snapshots, rollback, and command gates
  event_schema.py          durable/wire event contract source
frontend/                  React/TypeScript/Vite operator application
scripts/                   administration, release, and acceptance tooling
tests/                     backend tests
server.py                  FastAPI, SSE, local policy, and React bundle serving
run.py                     legacy single-session command entry point
```

All paths above belong to the same repository root. Do not initialize a
separate `.git` directory under `orchestrator/` or `frontend/`.

## Requirements

- Python 3.11 or 3.12
- Node.js 22
- Git for project validation and legacy snapshot reads

## Development setup

```powershell
python -m pip install -r requirements-dev.txt
Push-Location frontend
npm ci
npm run build
Pop-Location
python scripts/orchestrator_admin.py migrate
python -m orchestrator.cli --port 8000
```

Open `http://127.0.0.1:8000`. The packaged `ai-orchestrator` command uses the
same fixed loopback host. Do not replace it with a routable address.

The server requires a compiled React bundle at `frontend/dist` during source
development or at `orchestrator/frontend_dist` in a wheel. It returns `503`
instead of falling back to an arbitrary working-directory HTML file.

## Local data and credentials

Defaults are outside target repositories unless noted:

- canonical state: `~/.ai_orchestrator/orchestrator.sqlite3`;
- account health and leases: `~/.ai_orchestrator/account_leases.sqlite3`;
- artifacts: `~/.ai_orchestrator/artifacts`;
- safety snapshots: `~/.ai_orchestrator/snapshots`;
- cookie files: repository-local ignored `cookies/`;
- redacted agent journals: repository-local ignored `logs/agents`.

The canonical state schema is version `12`. Opening `StateRepository` applies
pending forward migrations. Migration 12 (`durable_effect_fencing`) adds
expected target hashes, fencing tokens, and compensation links/indexes to
effect receipts without rewriting existing version-11 rows. Follow the
[migration/rollback runbook](docs/runbooks/migration-rollback.md) before an
upgrade.

Authentication reads Web Claude session material only from local cookie files
under `ORCH_COOKIES_DIR`. There is no selectable provider or alternate model
credential source. Treat cookies, prompts, transcripts, logs, backups, task
descriptions, diffs, and artifacts as sensitive even though content scanning
and redaction provide defense in depth.

The application reads the process environment directly and does not
automatically load `.env`. Use [.env.example](.env.example) as a variable
reference, export values through your shell or supervisor, and never commit a
populated `.env`, cookie file, token, private key, or backup.

## Runtime behavior

Dynamic fan-out selects only the workstreams and workers needed by the plan.
Versioned work contracts carry scopes, dependencies, acceptance criteria, and
evidence. SQLite stores task projections, events, approvals, effects, leases,
jobs, audit records, and completion invariants. Task-local event sequences
support durable replay after reconnect.

Current plans embed typed contracts, logical agent IDs use a deterministic
fallback, and `agent_message` events carry handoff/contract correlation.
The hierarchy can feature-detect repository hooks for separate immutable
contract versions, agent identities, and handoff records, but the canonical
`StateRepository` does not currently implement dedicated tables or migrations
for those three record types. Schema 12 is an effect migration only.

Each model request uses the cookie-backed Web Claude transport. Rate limiting
persists cooldown and can replay the immutable logical request through another
eligible local account. Cookie contents are not stored in task events,
transcripts, or account databases.

New-project work is staged under
`~/.ai_orchestrator/artifacts/<task-id>/staging`. Approved files can be copied
atomically to the selected destination, and an optional ZIP can be produced.
Internal state, Git metadata, caches, and detected credential material are
excluded or blocked.

## Verification

Individual commands:

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

Full Windows acceptance:

```powershell
.\scripts\run-acceptance.ps1 -Profile full -Label post-flatten-p0
```

**Evidence status:** the latest full schema-14 result and source/log hashes are
recorded in [acceptance evidence](docs/acceptance-evidence.md). Rerun the full
matrix after any source or dependency change. See the
[acceptance matrix](docs/acceptance-matrix.md) for evidence rules.

## Documentation

- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Operations](docs/operations.md)
- [Event catalog](docs/event-catalog.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
