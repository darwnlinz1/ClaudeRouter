# Changelog

Notable changes are recorded here. Acceptance results are tracked separately;
an entry in this file does not prove that a test or release gate passed.

## Unreleased

### Changed

- Advanced canonical SQLite state from schema `11` through schema `14`.
  Migration 12 adds durable effect fencing and compensation; migration 13 adds
  immutable contract versions, logical-agent identities, and typed handoffs;
  migration 14 adds crash-safe managed-retention claims.
- Typed coordination is now persisted independently from plan JSON while plans
  retain their canonical contract snapshots for replay and compatibility.
- Corrected schema references and upgrade/rollback checks throughout operator,
  architecture, event, security, and acceptance documentation.
- At the operator's request, execution is unbounded: there is no model-call,
  wall-clock time, or token budget. This does not change or imply support for
  remote operation; the local-only boundary remains in force.
- Agent windows now display provisional thinking as it streams and provide a
  per-agent Response tab beside Stream, Thinking, Diff, and Tools.
- Frontend fan-out settings now flow directly to Director and Manager planning
  without the former per-Manager package cap; planned Managers and independent
  Workers launch concurrently within the separately configured execution slots.
- Dependency-gated agents now report `blocked` without emitting false failure
  attempts, and provider failures rotate cookies before replaying the same
  logical assignment.
- Provider prompt content is no longer rejected by the internal secret scanner;
  log/artifact redaction and the local API, path, CSRF, and sandbox boundaries
  remain enabled.
- Planner write scopes are rejected when they exceed the configured package
  limit instead of being silently truncated; action-schema numeric bounds are
  now enforced at the transport boundary.
- Completion reconciliation now proves that every planned logical agent was
  called for a named purpose or received an explicit terminal no-call
  disposition. Execution attempts and provider retry attempts have distinct
  correlation IDs.
- Shared workstream Testers are serialized by logical identity, cancellation is
  persisted as `CANCELLED`, and Worker submission is bounded before thread
  creation by the frontend-backed global concurrency setting.
- Event contract version `2` requires stable fan-out identities. Canonical task
  snapshots retain agent topology across event compaction, while timeline
  pages and SSE reconnects expose retained-history gaps.

### Evidence note

The schema-14 release matrix completed 19/19 gates against a stable source
fingerprint; see the acceptance evidence for the exact report and hashes. Any
later source change requires a new run.

## 0.4.0 - 2026-08-12

### Security

- Fixed the accepted deployment boundary to one local operator on
  `127.0.0.1`; remote and multi-user activation fail closed.
- Required repository commands requesting strong isolation to remain
  unlaunched when no attested backend is available.
- Restricted process-only execution to trusted internal commands and exposed
  requested versus actual isolation in test evidence.
- Added local session/origin/CSRF policy, security headers, path containment,
  secret scanning/redaction, sandbox environment filtering, and verified
  backup/restore controls.
- Standardized cookie and environment handling: cookie-backed Web Claude only,
  cookie files as the only model credential source, and no secrets in
  `.env.example`.

### Changed

- Made the React/TypeScript/Vite application under `frontend/` the only
  supported UI and removed legacy browser entry points/routes.
- Flattened source provenance to one Git repository; `orchestrator/` is now a
  normal Python package rather than a nested repository.
- Replaced Git-mutating patch backup behavior with scoped snapshots stored
  outside target repositories and SHA-256-checked rollback.
- Advanced canonical SQLite state to schema `11`, including projections,
  durable jobs, audit sequencing, queryable task fields, data-migration
  records, and retention/resume indexes.
- Clarified forward-only migration, verified backup, staged restore, and
  application-versus-data rollback procedures.

### Documentation

- Added security reporting and contribution guidance.
- Added an environment variable reference with non-secret defaults.
- Updated architecture, deployment, operations, event, migration, and
  acceptance documentation for the accepted local P0 topology.

### Evidence note

The checked-in acceptance evidence records an earlier dirty source with nested
repository provenance. It is not evidence for the flattened working tree; a
new full, hashed acceptance run is required for a release decision.
