# ADR 0004: Local operator security boundary

Status: Accepted

## Context

A loopback service still receives browser requests and executes high-impact
filesystem and command operations. Process grouping is useful for cleanup but
does not provide filesystem or network isolation. The project also previously
carried more than one UI/repository boundary, which made provenance and the
served browser surface harder to reason about.

## Decision

- Support one local operator only. The service command binds to `127.0.0.1`;
  middleware rejects non-loopback peers and unapproved local Host values.
- Require same-origin API access and apply the local session/CSRF policy.
- Fail startup for `remote` or `multi_user` deployment modes. Remote
  prerequisite variables are diagnostic only and cannot activate a remote
  control plane.
- Serve only the compiled React application from a path contained beneath the
  application root. Remove legacy browser routes and do not fall back to files
  from the process working directory.
- Use one flattened Git repository. `orchestrator/` and `frontend/` are normal
  source directories, not independent repositories.
- Resolve all project and artifact paths beneath an approved root.
- Use cookie-backed Web Claude only. Read session material only from local
  cookie files, and do not persist cookie values in events, transcripts, or
  account databases.
- Block likely credentials before provider transport and artifact publication;
  redact sensitive event, transcript, and log values before persistence.
- Require human approval according to each contract's risk and approval policy.
- Request strong isolation for repository test commands. Unless a backend
  attests process-tree, filesystem, network, and environment containment, do
  not launch the child process. Restrict process-only execution to trusted
  internal commands and report it honestly.
- Build sandbox child environments from a small allowlist and reject recognized
  credential, proxy, token, cookie, key, and password variables.
- Store scoped pre-mutation snapshots outside target repositories and restore
  with path and SHA-256 checks instead of mutating Git metadata.

## Consequences

The accepted system has a small, explicit local boundary and one browser
surface. Missing strong isolation blocks repository commands rather than
silently downgrading. Builds and acceptance evidence have one source-provenance
root.

Loopback, CSRF, redaction, and secret scanning are defense in depth; they do not
make untrusted local users safe and do not justify remote exposure. Operators
must protect cookies, logs, databases, snapshots, and backups with local
filesystem controls.

Remote access and multi-tenancy require a new ADR, implementation, threat
model, authentication/isolation design, and acceptance evidence. Configuration
alone is insufficient.
