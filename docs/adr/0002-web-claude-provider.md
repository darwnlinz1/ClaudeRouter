# ADR 0002: Cookie-backed Web Claude provider

Status: Accepted

## Context

This deployment uses local Claude web sessions. A single credential path keeps
retry, identity, secret handling, and operator expectations unambiguous.

## Decision

Use the provider adapter boundary with the cookie-backed Web Claude transport
only. Local cookie files are the sole provider credential input. Account leases
store opaque account IDs and health state, never cookie contents.

On HTTP 429, preserve the serialized logical request, cool down the affected
account, acquire another eligible account, and replay the same bytes. A stream
without a terminal event is incomplete and must not be treated as success.

## Consequences

Provider behavior remains testable behind an adapter while deployment requires
valid local cookie files. Operators must rotate cookies and investigate account
health through redaction-safe status surfaces.
