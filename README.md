
On first load the runtime may call Claude's organizations endpoint to resolve
an org id. Empty files and files without `sessionKey=` are skipped.
Keep several files if you want rotation and chat handoff. One file is enough
to start; when it is cooling down or quarantined, work waits or fails until
another eligible account exists.
Cookie *values* never belong in events, SQLite, the UI, or chat. Settings →
cookie account health shows opaque ids, lease counts, and cooldown state.
## Using the UI
After the backend is up and the React bundle is built, the shell at
`http://127.0.0.1:8000` is the operator console.
### New code run
1. **New run** (rail `+`, empty state, or command palette).
2. Wizard tab **Create / edit code**.
3. **Folder** — Edit existing project, or Create new project into an empty
   destination. Browse to a local folder.
4. **Goal** — task name, optional test command, Director prompt.
5. **Shape** — max managers and coder tasks per manager, plus concurrency
   slots. Launch.
Director / Manager / Worker / Tester model and effort come from Settings
unless you override an agent later. Hierarchy fan-out from the wizard is what
the planners may select, not a required headcount.
While a run is live you get a task rail, org graph, agent windows (stream,
thinking, response, diff, tools), timeline, and an intervention banner when
Director input or a patch approval is required. New-project
`staging_auto` approvals apply only while execution stays inside the managed
staging workspace; edit-mode stays manual.
### New regular chat
1. **New chat** (rail message icon, empty-state button, command palette, or
   the wizard tab **Regular chat**).
2. Optional display name, model, effort, and the first message. No folder.
3. **Start chat**. Follow-ups go in the chat composer (Enter sends,
   Shift+Enter newline).
Chat stays on one cookie until quota or auth failure. The next cookie receives
the prior User/Assistant log in the prompt because each provider call opens a
new Claude conversation. The transcript shows a system note on account switch.
Chat cannot be resumed after the process is stopped the way a hierarchy task
can; start a new chat if the worker is gone.
### Everyday controls
- Task rail groups code runs by project folder and chats under **Chat**.
- Command palette for new run / new chat / settings.
- Stop and delete are confirmed. Delete does not uninstall cookies.
- Settings: role models, fan-out defaults, density, mock-when-offline, and
  account health.
## Task types
| | Create / edit code | Regular chat |
| --- | --- | --- |
| API `mode` | `orchestrator` | `chat` |
| Project root | Required | Empty; not used |
| Accounts | Planners sticky per role; workers/testers rotate | One sticky cookie until 429/auth, then switch |
| Output | Patches, staged files, tests, approvals | Assistant text only |
| Follow-up | Approvals / Director input | `POST /api/chat/{task_id}` from the composer |
| Resume after restart | Hierarchy resume path | Not supported |
## Runtime data
Defaults (override with absolute paths in the environment; see `.env.example`):
| Path | What |
| --- | --- |
| `~/.ai_orchestrator/orchestrator.sqlite3` | Canonical task/event state (WAL) |
| `~/.ai_orchestrator/account_leases.sqlite3` | Account health and leases; no cookie bodies |
| `~/.ai_orchestrator/artifacts` | Staged / approved files |
| `~/.ai_orchestrator/snapshots` | Scoped pre-mutation snapshots |
| `cookies/` | Credential files (gitignored) |
| `logs/agents` | Redacted agent journals |
Do not copy SQLite files while the service is running, and do not delete
`-wal` / `-shm` sidecars. Use the admin backup command (SQLite backup API).
```powershell
python scripts/orchestrator_admin.py status
python scripts/orchestrator_admin.py migrate
```
## Test
Backup / verify / restore: [Operations](docs/operations.md).
`GET /api/health` on loopback reports version, schema, provider
(`web_claude`), `deployment_mode=local`, and `execution_boundary=local-process`.
## Configuration
Documented names live in [`.env.example`](.env.example). Export them in the
launching shell. Important groups:
- **Boundary** — `ORCH_DEPLOYMENT_MODE=local` is the only accepted value.
- **Cookies** — `ORCH_COOKIES_DIR` (default `cookies`).
- **Model** — `ORCH_MODEL`, token/retry/cooldown, stream stall limits.
- **Planning** — file-size and fan-out caps. The UI also sends per-launch
  manager/worker slot counts.
- **Retention** — event/log/artifact windows; maintenance is periodic.
There is no host flag. `python -m orchestrator.cli --port 8000` always binds
`127.0.0.1`. `--reload` is for local source iteration only.
## Frontend development
Production path: `npm run build` then the Python server serves `frontend/dist`.
Live UI against a running backend:
```powershell
python -m orchestrator.cli --port 8000
Push-Location frontend
npm run dev
```
Vite listens on `5173` and proxies `/api` to `http://127.0.0.1:8000` (override
with `VITE_DEV_API_TARGET`). Dev CSP allows inline styles so Vite HMR works;
the built bundle does not.
```powershell
Push-Location frontend
npm test
npm run typecheck
npm run lint
Pop-Location
```
After an additive event-schema change, regenerate TypeScript from the repo
root (do not hand-edit the artifact):
```powershell
python -c "from orchestrator.event_schema import write_typescript_artifact; write_typescript_artifact('.')"
```
## Tests and acceptance
```powershell
python -m pytest -q
.\scripts\run-acceptance.ps1 -Profile full -Label local
```
Dev Python extras are in `requirements-dev.txt`. Do not point tests at real
cookie files or production databases.
## Layout
```text
server.py           FastAPI app: loopback HTTP, CSRF, SSE, task admission
orchestrator/       Planning, provider transport, leases, SQLite, artifacts
orchestrator/cli.py Binds uvicorn to 127.0.0.1
frontend/           React + TypeScript + Vite workspace (only UI)
scripts/            migrate, backup, launch, acceptance, release
tests/              Backend tests
docs/               Architecture, operations, ADRs, runbooks
cookies/            Local credentials (ignored)
```
## If something is stuck
- **UI blank / unstyled** — rebuild `frontend/` and restart; the server has
  no legacy HTML UI.
- **No model calls** — `cookies/` empty, missing `sessionKey=`, or every
  account quarantined/cooling. Check Settings → account health and the
  [provider runbook](docs/runbooks/provider-incidents.md).
- **Chat died after restart** — expected; start a new chat. Hierarchy tasks
  may resume through the operator control.
- **Task waiting** — approval, dependency, write-scope claim, or account
  cooldown. See [stuck-task](docs/runbooks/stuck-task.md). Do not start a
  second process against the same project root.
- **429 storms** — normal rotation/cooldown. Add another cookie file through
  the usual local procedure; do not paste cookies into chat or issues.
## Docs
Operator and design docs (the README is the map, not a copy of these):
- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Deployment and rollback](docs/deployment.md)
- [Operations](docs/operations.md)
- [Event catalog](docs/event-catalog.md)
- [Versioning](docs/versioning.md)
- [Acceptance matrix](docs/acceptance-matrix.md)
- [Security](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)
Runbooks: [stuck task](docs/runbooks/stuck-task.md),
[provider incidents](docs/runbooks/provider-incidents.md),
[database recovery](docs/runbooks/database-recovery.md),
[migration rollback](docs/runbooks/migration-rollback.md).
