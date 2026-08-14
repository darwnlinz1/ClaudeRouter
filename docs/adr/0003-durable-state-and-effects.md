# ADR 0003: SQLite state and durable effects

Status: Accepted

## Context

JSON snapshots and direct filesystem writes could not distinguish an operation
that never started from one that completed before a process crash.

## Decision

SQLite is the canonical store for plans, attempts, event envelopes, approvals,
leases, effects, and operator task snapshots. Schema migrations are ordered,
transactional, and recorded.

External side effects use deterministic IDs and a
`pending -> applied | failed` receipt. File replacement is staged, flushed, and
atomically renamed. Event replay uses task-local monotonic sequence numbers.

## Consequences

Restart and replay can resume idempotently. Operators must back up both SQLite
databases plus artifacts and logs, and must not copy a live database with a
plain filesystem copy.
