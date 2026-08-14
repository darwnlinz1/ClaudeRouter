# Security policy

## Supported boundary

Security fixes target the current source and current local package version.
There is no published support window for older snapshots.

The accepted deployment is one trusted operator on one machine, with the
service bound to `127.0.0.1`. Remote access, LAN exposure, reverse proxies,
tunnels, multi-user operation, and untrusted local users are unsupported.
Setting a non-local deployment mode must fail closed.

The React application in `frontend/` is the only supported UI. Legacy root
HTML/JavaScript files and routes are outside the security boundary.

## Report a vulnerability

Do not open a public issue, discussion, pull request, or chat containing an
unpatched vulnerability or credential.

Report privately to the repository maintainer through the private channel by
which you received access. If the repository host offers private vulnerability
reporting, use its Security reporting form. Include:

- affected version, commit, and whether the tree was dirty;
- operating system and Python/Node versions;
- impact and the local trust assumptions required;
- minimal reproduction steps or a redacted proof of concept;
- relevant routes, files, event types, or configuration names;
- whether any cookie, token, prompt, artifact, log, database, snapshot, or
  backup may have been exposed;
- suggested mitigation, if known.

Remove real secrets and personal data. Use synthetic credentials in
reproductions. If a real credential was exposed, invalidate it first and
report only a non-reversible identifier. No acknowledgement or remediation SLA
is currently promised; coordinate disclosure timing with the maintainer.

## Credential handling

Web Claude cookie files are credentials. Keep them in the ignored
`ORCH_COOKIES_DIR` (default `cookies/`) or in a protected location outside the
repository. The runtime reads model session material only from those files.

Never commit or attach:

- `.env` files;
- cookies, session material, tokens, passwords, API keys, or fingerprint salts;
- private keys or certificates;
- SQLite databases and WAL/SHM sidecars;
- prompts, transcripts, logs, snapshots, artifacts, or backups containing
  operator/project data.

`.env.example` must contain names and non-secret defaults only. The application
does not automatically load `.env`; inject process environment values through
the local shell or supervisor. Restrict local file permissions to the operator
account.

Events, transcripts, and artifacts are scanned/redacted as defense in depth,
but scanners can miss novel secret formats. Handle all generated data as
sensitive.

## Command execution

Repository test commands request strong isolation. Without an attested backend
covering process-tree, filesystem, network, and environment containment, the
command must not launch. A process group is not a security sandbox and is
allowed only for trusted internal commands.

Sandbox child environments use an allowlist and reject recognized credential,
proxy, cookie, token, key, and password variables. Do not weaken isolation or
relabel process-only execution as strong.

## Local HTTP controls

The service requires a loopback peer and approved local Host. API policy
enforces same-origin behavior and local session/CSRF controls. Security headers
restrict scripts, framing, referrers, browser capabilities, and content
sniffing.

These controls do not make remote exposure safe. Do not bind Uvicorn directly
to `0.0.0.0`, publish the port, or place the service behind a network proxy.

## Backups and incident handling

Administrative backups are manifest- and SHA-256-verified but are not
encrypted. Store them outside the installation directory with restricted
permissions and a separately recorded hash. Restore first to empty staging
paths.

The canonical database is currently schema 14. Its additive migrations store
effect target hashes/fencing/compensation, immutable contract versions,
logical-agent identities, typed handoffs, and managed-retention claims.
Filesystem writes remain hash-CAS operations outside SQLite transactions; after
a crash, reconcile the target hash against the durable receipt and reject stale
fencing tokens.

Typed contracts, logical identities, and handoff envelopes can contain
sensitive task, artifact, and evidence metadata and have dedicated durable
tables. Protect and redact the database, event logs, and backups accordingly.

If compromise is suspected:

1. stop the local service and prevent new task execution;
2. invalidate affected cookies or other credentials;
3. preserve only the minimum redacted logs and hashes needed for analysis;
4. verify state/account databases and artifact hashes;
5. restore from a known verified backup if integrity is uncertain;
6. rerun the full acceptance matrix on the reviewed, quiescent source.

See [ADR 0004](docs/adr/0004-local-security-boundary.md) and the
[migration/rollback runbook](docs/runbooks/migration-rollback.md).

This policy is not evidence by itself. Rerun the
[acceptance matrix](docs/acceptance-matrix.md) after any source or dependency
change.
