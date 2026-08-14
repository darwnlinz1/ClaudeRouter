# Runtime architecture

## Product boundary

The accepted P0 architecture is a single local process for one operator:

- FastAPI and SSE listen on `127.0.0.1`;
- the only browser application is the compiled React/TypeScript/Vite workspace;
- provider access is cookie-backed Web Claude;
- SQLite and local files provide durable state;
- remote and multi-user activation fail closed;
- the source tree has one Git root, with `orchestrator/` as a normal package.

Remote access, multi-tenancy, distributed execution, and a second legacy UI are
outside this architecture.

## Components

`server.py` owns the local HTTP boundary, security middleware, task admission,
SSE publication, lifecycle coordination, and React bundle serving.

`orchestrator/` contains planning, scheduling, provider transport, account and
project leasing, policy, sandboxing, effects, persistence, artifacts,
redaction, and reconciliation. `frontend/` contains the only UI source.
`scripts/` contains migration, backup, release, compatibility, benchmark, and
acceptance entry points.

These directories are all part of one flattened repository. Tooling and
release provenance use the repository root as the source boundary.

## Control and execution flow

1. The local service creates or resumes a task and durably appends task-local
   events.
2. Director planning selects bounded workstreams. Each Manager selects only the
   Worker packages required by its workstream; planning caps are not quotas.
3. Versioned Work Contracts carry inputs, outputs, read/write scopes,
   dependencies, acceptance criteria, evidence, consumers, risk, and revision.
4. The scheduler respects DAG dependencies, path-aware scope claims, execution
   slots, cancellation, and waiter aging.
5. Workers produce bounded patches or staged files. Reviewers/Testers assess
   the result and evidence independently.
6. Completion reconciliation partitions planned workstreams, work items,
   agents, and logical calls into terminal outcomes before successful
   completion is accepted.

Typed coordination is being introduced through additive runtime compatibility:
plans embed immutable `WorkContract` values and legacy work items/streams are
normalized to compatible contracts; logical agent IDs fall back to a stable
hash of task, role, and assignment; and typed `HandoffEnvelope` values correlate
producer, consumer, contract version, artifacts, and evidence. The hierarchy
feature-detects `save_contract_version`, `resolve_agent_identity`,
`append_handoff`, and `list_handoffs` repository hooks. The canonical
`StateRepository` does not currently implement those hooks or dedicated
contract-version, identity, or handoff tables, so they are not schema-12
migrations. Contracts remain durable as part of immutable plan JSON, and
handoff correlation remains durable through `agent_message` events.

## Durable state

`orchestrator/state_repository.py` is the source of truth for the canonical
state schema. The current `PRAGMA user_version` is `12`. Each numbered
migration runs in its own `BEGIN IMMEDIATE` transaction and records its version,
name, and timestamp in `schema_migrations`.

The migration sequence provides:

1. core tasks, plans, workstreams, work items, attempts, events, leases,
   project locks/fencing counters, and effect receipts;
2. project lock fencing, heartbeat, and metadata;
3. durable event cursors and retention metadata;
4. observability, log, and artifact records;
5. task completion invariants;
6. canonical task snapshots;
7. human approval requests;
8. event projections, durable jobs/job fencing, and hash-chained audit records;
9. deterministic per-namespace audit sequencing;
10. queryable task projection columns and one-time data migration records;
11. task resume and global retention indexes;
12. additive durable-effect expected hashes, fencing tokens, compensation
    links, and pending/compensation indexes.

The canonical state database defaults to
`~/.ai_orchestrator/orchestrator.sqlite3`. Account health and leases are stored
separately in `~/.ai_orchestrator/account_leases.sqlite3`; cookie values are
not stored there.

Events receive monotonic task-local sequences. Reconnect replays durable
history after the client cursor and then follows the live broker. Known
payloads are validated, while unknown event types and additive fields remain
replayable. Event append and the canonical task projection can commit in the
same SQLite transaction.

Effects use deterministic identities, before/expected-after/actual-after
hashes, optional fencing tokens, atomic replacement, and durable receipts.
Restart reconciliation can adopt an already-correct write instead of applying
it twice. A compensation is a separate durable effect linked once to the
original receipt; completing or reconciling it records the reverse link and
timestamp. Migration 12 preserves existing receipts with nullable values for
the new fields.

## Provider and secret boundary

Provider contracts normalize an immutable logical request and withhold
provisional stream fragments until an attempt completes. HTTP 429 persists
cooldown, releases the account lease, and may replay the same logical request
through another eligible local cookie account. HTTP 401/403 invalidates the
account.

Likely credentials are blocked before provider transport or artifact
publication. Sensitive event keys and recognizable secret values are redacted
before durable event/UI persistence. These checks are defense in depth; local
cookie files, logs, transcripts, and backups must still be handled as
sensitive.

## Trust boundaries

- Every HTTP route requires a loopback peer and an allowed local Host. API
  policy enforces same-origin behavior and local session/CSRF requirements.
- Project and artifact paths are canonicalized beneath explicit roots.
  Protected paths are rejected before mutation.
- Patch and artifact writes use staging, hashes, atomic replacement, and
  fencing-token preconditions.
- Account and project leases coordinate across threads and processes through
  SQLite.
- Strong sandbox requests do not launch a child process unless a backend
  attests process-tree, filesystem, network, and environment containment.
  Process-only execution is not described as a security sandbox.
- Sandbox child environments use a minimal allowlist and reject credential,
  proxy, token, cookie, key, and password variables.
- Backup verification rejects traversal, links, duplicate members, undeclared
  files, excessive expansion, and checksum mismatches. Restore refuses
  existing destinations unless overwrite is explicitly requested.

## React operator UI

The server resolves the React bundle only beneath the repository/package root.
Source development uses `frontend/dist`; wheels use
`orchestrator/frontend_dist`. Missing or unsafe bundle paths return an error,
not a legacy fallback.

The UI consumes generated TypeScript event declarations and reconstructs task,
agent, call, contract, test, approval, sandbox, and reconciliation views.
Legacy `/main.js`, `/api.js`, `/style.css`, `/legacy`, and server-rendered task
routes are not part of the product surface.

## Lifecycle and retention

Shutdown stops maintenance and task admission, signals active work, joins
workers, and closes persistence. Retention preserves cursor continuity and
terminal evidence while applying configured age/count limits. Artifact records
and content hashes support missing/tampered-file reconciliation.

See the [event catalog](event-catalog.md), [acceptance matrix](acceptance-matrix.md),
and [migration/rollback runbook](runbooks/migration-rollback.md). Architecture
descriptions are not test results; rerun acceptance after source changes.
