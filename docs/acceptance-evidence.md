# Acceptance evidence

## Final schema-14 handoff

Run label: `graph-visible`

- Time: 2026-08-12 18:57:47Z through 18:58:43Z.
- Source anchor: commit `2cc605b8fce1dfbfeed1459d7ec0a066b0f6629e`,
  branch `main`, flattened repository with no nested Git root.
- Source qualification: the working tree was dirty but quiescent. The source
  fingerprint was
  `fdb71ab75d2287eb58aaaa005535a7d7db196ef6accc1a18f6c879b251d20b6e`
  at both start and finish (`source_changed_during_run: false`).
- Environment: Windows 10.0.19045, Python 3.11.15, Node v24.13.1, npm 11.8.0.
- Complete result: **19 passed, 0 failed, 0 blocked**.
- JSON report:
  `acceptance-results/20260812T185747Z-graph-visible/acceptance-report.json`
- Report SHA-256:
  `00a75429670c9a20f528a8a492abe574ee5843aed809f1f5b2def5b8634cf4e0`.

## Exact gate results

- Backend lint and strict boundary types: passed.
- Backend full: passed, 367 tests.
- SQLite migration: passed, 4 focused tests through schema 14.
- Replay/reconciliation: passed, 11 tests.
- Dynamic fan-out/contracts: passed, 42 tests.
- Effects/account/project leases: passed, 14 tests.
- Sandbox/security: passed, 36 tests.
- Retention/artifacts/backup: passed, 23 tests.
- HTTP/event compatibility: passed.
- Generated event schema drift: passed; artifact SHA-256
  `f4b1d7b02316b58e71cbc60cb499983b53d05b6830435ff38e1279a94bf972e0`.
- Cookie-only provider guard: passed across 50 production/config files.
- Provider retry/replay/stream behavior: passed, 16 tests.
- Frontend lint, formatting, and typecheck: passed.
- Frontend unit/component tests: passed, 7 files and 41 tests.
- Frontend production build: passed with no source maps or external assets.
- Frontend E2E/responsive: passed, 3 Chromium tests at desktop and mobile
  viewports.

Chromium was installed before the final run. npm's local `devdir` deprecation
and Playwright's `NO_COLOR`/`FORCE_COLOR` messages were non-failing warnings.

## Acceptance decision

**PASS.** The ignored timestamped result directory contains the JSON/Markdown
reports and all gate logs. This evidence remains valid only for the source
fingerprint above; rerun the matrix after any source or dependency change.
