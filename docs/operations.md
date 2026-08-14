# Local operations

## Start and stop

Build the React UI before starting from source:

```powershell
Push-Location frontend
npm run build
Pop-Location
python -m orchestrator.cli --port 8000
```

Use `http://127.0.0.1:8000` only. The service rejects non-loopback peers and
unapproved Host values, but that is not authorization for exposing it through a
proxy or tunnel.

Stop the foreground process normally so task workers, maintenance threads,
account leases, and SQLite connections can drain. Before maintenance, stop new
task admission and wait for active model calls, approvals, artifact writes,
and effects to settle.

## Launch a live new-project task

Pass the complete prompt as a UTF-8 (optionally BOM-prefixed) file, or omit
`--prompt-file` and pipe it through stdin:

```powershell
python scripts/launch_live_task.py C:\work\empty-destination `
  --prompt-file C:\work\full-prompt.md `
  --test-command "python -m pytest" `
  --approval-mode staging_auto `
  --wait
```

`staging_auto` is fail-closed: the API accepts it only for `new_project`, and
each approval is automatically decided only while execution is exactly inside
the orchestrator-managed staging workspace. Edit-mode and existing-project
tasks remain manual. `--wait` captures the durable timeline as JSONL (use
`--timeline-jsonl` to select the path) and prints a terminal diagnostics
summary. The launcher keeps one same-origin cookie session and CSRF token for
the request.

## Health and operator checks

`GET /api/health` reports application version, canonical schema version,
provider, deployment mode, and execution boundary. The expected P0 values are:

- `schema_version`: `12`;
- `provider`: `web_claude`;
- `deployment_mode`: `local`;
- `execution_boundary`: `local-process`.

Also verify:

- the UI is the compiled React workspace, not a legacy route;
- task-local event sequences advance monotonically and reconnect replay starts
  after the client cursor;
- pending approvals and active project/account leases match current work;
- pending or failed effect receipts have an understood outcome, and expected
  target hashes and fencing tokens match the operation being recovered;
- compensation receipts and their original effects have consistent one-to-one
  links and the original is not considered compensated before completion;
- successful tasks have balanced completion reconciliation;
- artifact files match recorded SHA-256 values.

Every event carries task/session correlation and may carry workstream, work
item, agent, attempt, and call identifiers. Metrics deliberately use bounded
label domains rather than identifiers.

## Local data

Default locations:

- `~/.ai_orchestrator/orchestrator.sqlite3` — canonical state schema `17`;
- `~/.ai_orchestrator/account_leases.sqlite3` — account health and leases;
- `~/.ai_orchestrator/artifacts` — staged and approved artifacts;
- `~/.ai_orchestrator/snapshots` — scoped pre-mutation snapshots;
- `logs/agents` — redacted local agent journals.

SQLite uses WAL mode and `synchronous=FULL`. Do not copy database files while
the service is running or delete `-wal`/`-shm` sidecars. Use the administrative
backup command, which uses SQLite's backup API.

Schema 17 remains additive. In addition to durable effects, typed contracts,
logical identities, handoffs, managed retention, and sanitized provider-attempt
records, it stores execution epochs/Manager rosters, remediation strategy
claims, Manager terminal reports, the Director final review, and terminal
disposition/log references. Backups must preserve these as independent records.

## Backup

With explicit paths and the service stopped:

```powershell
$Db = Join-Path $HOME ".ai_orchestrator\orchestrator.sqlite3"
$AccountDb = Join-Path $HOME ".ai_orchestrator\account_leases.sqlite3"
$Artifacts = Join-Path $HOME ".ai_orchestrator\artifacts"
$Backup = Join-Path $PWD "backups\operator-backup.zip"

python scripts/orchestrator_admin.py `
  --database $Db `
  --account-database $AccountDb `
  --artifacts-root $Artifacts `
  --logs-root (Join-Path $PWD "logs") `
  backup $Backup
python scripts/orchestrator_admin.py verify $Backup
Get-FileHash -Algorithm SHA256 $Backup
```

Store the archive and its separately recorded hash outside the installation
directory. The archive is integrity-checked, not encrypted; protect it as
sensitive data. Periodically perform a staged restore to empty paths by
following the [migration/rollback runbook](runbooks/migration-rollback.md).

## Retention

The maintenance loop runs at least every 300 seconds. Defaults are:

- events: 30 days and at most 50,000 per task;
- logs: 14 days;
- artifacts: 90 days;
- observability records: 30 days;
- maintenance interval: 21,600 seconds.

Override these with `ORCH_EVENT_RETENTION_DAYS`,
`ORCH_MAX_EVENTS_PER_TASK`, `ORCH_LOG_RETENTION_DAYS`,
`ORCH_ARTIFACT_RETENTION_DAYS`, `ORCH_OBSERVABILITY_RETENTION_DAYS`, and
`ORCH_RETENTION_INTERVAL_SECONDS`. Values must be positive. Retention preserves
cursor continuity and terminal evidence; pin artifacts that must be retained.

## Sandbox outcomes

Repository test commands request strong isolation. If no backend attests the
required process-tree, filesystem, network, and environment controls, the
command is not launched and the result is `unavailable`. Treat that as a
blocked/failing gate, never as a skipped success.

Process-only execution provides timeout/cancellation cleanup for trusted
internal commands. It is not an OS, filesystem, or network sandbox. Check each
`test_result` event's requested isolation, actual isolation, backend detail,
blocked reason, timeout/cancellation state, and truncation flag.

## Secret handling

Place Web Claude session material only in cookie files under
`ORCH_COOKIES_DIR`. Never put cookie values, API keys, tokens, passwords,
private keys, or account fingerprint salts in task prompts, source files,
`.env.example`, logs, artifacts, or issue reports. Sandbox child processes
receive a minimal environment and do not inherit recognized credential
variables.

Redaction and content scanning are defense in depth and can produce false
negatives. Restrict permissions on cookies, logs, databases, snapshots, and
backups. If exposure is suspected, stop the service, invalidate the affected
credential, preserve redacted evidence, and follow [SECURITY.md](../SECURITY.md).

## Service objectives

The following are local performance targets, not claims about the current
working tree:

- durable event append p95 at or below 25 ms;
- replay of 1,000 events at or below 250 ms;
- scope-conflict checks at or above 50,000/s;
- control API p95 at or below 250 ms, excluding provider and sandbox work;
- zero unbalanced successful task completions.

Run:

```powershell
python scripts/benchmark_runtime.py --assert-slo
```

Record hardware, OS, Python version, endpoint security software, event count,
command, exit code, and result hash with any benchmark report.

## Acceptance

Operator commands in this document are procedures, not independent PASS claims.
Rerun the [acceptance matrix](acceptance-matrix.md) after changes.

## Capacity

Start with two parallel Managers and four global Workers. Increase concurrency
only when local CPU, memory, cookie-account capacity, and non-overlapping
project write scopes permit it. Planning caps are upper bounds, not execution
targets.
