# Local Multi-Agent AI Coder Orchestrator

A local Supervisor → Worker → Reviewer/Tester auto-coding system. The
Supervisor plans and delegates; the Worker returns an atomic SEARCH/REPLACE
patch plus `worker_feedback`; Python applies the patch and runs syntax/tests;
the independent Reviewer approves it or requests a scoped revision. Failures
are rolled back and all three viewpoints are recorded for the next turn.

## Layout

```
orchestrator/
  system_prompt_supervisor.py  planning and diagnosis rules
  system_prompt_worker.py      patch and feedback rules
  system_prompt_reviewer.py    independent review rules
  tools_schema.py      role-specific tool JSON schemas
  config.py             protected files, limits, model name, turn cap
  state_store.py        atomic load/save of rules, decisions and state
  context_builder.py    assembles the per-turn user message + [TRUNCATED] markers
  path_utils.py         normalized project-root path containment
  patch_engine.py       code-level guardrails: protected files, allowed-file
                         list, unique anchors and explicit new-file creation
  error_utils.py        error normalization + hashing for consecutive_error_count
  safety.py              git backup/rollback + a syntax gate
  llm_client.py          bounded-retry local Claude cookie client
  orchestrator.py        Supervisor → Worker → Patch/Test → Reviewer loop
run.py                   CLI entry point
server.py                FastAPI + SSE local control server
tests/                    offline core and safety tests
```

## Running it

```bash
pip install -r requirements.txt

python run.py \
  --root /path/to/project \
  --task "Rename compute_total() to compute_grand_total() everywhere" \
  --files src/billing.py src/reports.py \
  --test-cmd "pytest -x" \
  -v
```

Cookie files are loaded from `ORCH_COOKIES_DIR` (default: `cookies/`). This
directory is ignored by Git and must remain local. To start the web UI:

Each agent retries malformed protocol output three times on the same cookie,
then switches cookie. HTTP 429 switches immediately and cools that cookie for
five hours; 401/403 removes it. A task fails only after every usable cookie has
been exhausted for that call.

```bash
python -m uvicorn server:app --host 127.0.0.1 --port 8000
```

Keep the server bound to `127.0.0.1`.

The web UI exposes independent model/effort selectors for Supervisor, Worker
and Reviewer/Tester. The right-hand Tester panel streams its review and shows
the final `approved`/`revise` verdict, feedback and next instructions.

Chat mode is standalone: it only needs a prompt plus its primary model/effort
and account routing. Project selection, test settings and the Supervisor/Tester
panels are hidden, and no project root is required.

The left panel is a persistent task dashboard. Task metadata is stored outside
target repositories in `~/.ai_orchestrator/tasks.json`. Selecting a task shows
its status, phase, turn count, reviewed files and `+/-` line statistics.
Model/effort, routing, project and test configuration live in a task settings
drawer instead of occupying the dashboard.

The default dashboard hides Supervisor, Reviewer and audit panels until a task
is opened. Each task row has explicit Open plus context-sensitive Stop/Delete
actions. The same stop endpoint handles Chat and Orchestrator tasks.

`Create New Project` runs against an isolated staging workspace under
`~/.ai_orchestrator/artifacts/<task-id>/staging`. Supervisor can delegate new
paths directly, Worker creates one file per ticket, and Reviewer gates each
file. With Auto Apply enabled, every approved file is copied atomically into
the destination immediately, so later agent failures do not discard completed
files. The final approved ticket also creates the optional downloadable ZIP.
Internal state, Git metadata and caches are excluded from the output.

Each invocation is one *session* — a loop of turns against a project root
until the Worker reports `task_status: "completed"` after a passing gate, or
the turn cap (`--max-turns`, default 25) is hit. `request_context` adds safely
resolved files and continues the loop. `state.json` on disk carries
forward automatically, so re-running the same command continues where the
last session left off.

## Tests

```bash
python -m pytest -q
```

The tests are offline — the LLM call is injected (`llm_call` parameter of
`run_session`), so the orchestrator's control flow, patch validation, git
rollback, and state bookkeeping are all tested with scripted fake model
responses rather than a live API key.

## How this addresses the spec's own open issues

The spec's design notes (section 3) flag several things as **prompt-only**
mitigations that still need code-level backing. This implementation adds
that backing directly:

- **Note 3 (protected files)**: `patch_engine.validate_file_path` rejects
  `rules.json` / `DECISIONS.md` / `state.json` — by basename, not just
  exact string match, so a path variant like `./state.json` still gets
  caught — *before* any file I/O happens, regardless of what the model's
  output claims. `orchestrator.py` also rejects any `file_path` outside the
  set of files actually shown to the model that turn (Rule I), not just
  files that happen to exist in the repo.

- **Note 4 (truncation)**: `context_builder._read_with_truncation` actually
  cuts oversized files and inserts a literal `[TRUNCATED -- N characters
  omitted]` marker (keeping head + tail so both ends of the file stay
  visible). `apply_patch` re-checks anchor uniqueness against the *full*
  on-disk file regardless of what was shown to the model, so even if the
  model mistakenly treats a truncated snippet as unique, the patch is still
  rejected in code if the real file has more than one match.

- **Explicit file creation**: a missing file can only be created when it was
  explicitly included in context and the Worker submits exactly one empty
  SEARCH block. Existing files still require a unique on-disk anchor.

- **Note 6 (multi-file coordination)**: `task_status` stays `in_progress`
  until the model reports `completed`; the orchestrator does not infer
  completion from a passing syntax/test gate on its own. `context_note` /
  `state.json.current_task` is where the model is expected to name
  remaining files, per Rule B — the loop doesn't second-guess this, since
  true multi-patch transaction/rollback grouping is out of scope for a
  prompt-level mitigation (as the spec itself notes).

- **Note 7 (error hash stability)**: `error_utils.normalize_error` strips
  line numbers, file:column refs, hex addresses, ISO timestamps, epoch-like
  numbers, and generated tmp paths/ids *before* hashing, so two failures of
  the same underlying bug collapse to the same `last_error_hash` and
  `consecutive_error_count` actually climbs instead of resetting on cosmetic
  diffs. A fuzzy fallback (`is_similar_error`, difflib-based) is included
  for residual near-duplicates normalization doesn't catch.

## Structured output

The custom Claude client parses the final JSON block from model text. Schemas
and prompts define the contract, while the backend revalidates paths, patch
anchors, task status and execution results. Model output is never treated as
the security boundary.

## Safety mechanisms (Rule I — off-limits to the model)

`safety.py` (git backup-commit before every patch, hard-reset rollback on
gate failure, and a syntax gate) and `patch_engine.py`'s validation logic
are never exposed to the model as editable files or tools — they aren't in
`ALL_TOOLS`, aren't part of the source-file context, and live in a separate
module the model has no path to reach through `submit_patch`. The only way
this code changes is a human editing the orchestrator's own repo.
