"""Configuration constants for the AI Coder Orchestrator.

These mirror limits stated in the prompt/schema (Module 1 v2 spec) but are
kept here as real Python values so the orchestrator enforces them in code
rather than trusting the model to self-police. See design note 3 in the
spec: "orchestrator validate file_path o lop code (khong chi dua vao prompt)".
"""

import os

# Files the model must NEVER be allowed to patch, regardless of what it
# outputs. Enforced in patch_engine.validate_file_path — this is the
# code-level backstop for Rule H, independent of the prompt text.
PROTECTED_FILES = frozenset({"rules.json", "DECISIONS.md", "state.json"})

# Max characters of a single source file inlined into the per-turn context
# before it gets cut and marked [TRUNCATED] (Rule / design note 4).
MAX_FILE_CHARS = int(os.environ.get("ORCH_MAX_FILE_CHARS", "2000000"))

# Must match the `context_note` field's intended limit in both tool schemas.
CONTEXT_NOTE_MAX_CHARS = 500
WORKER_FEEDBACK_MAX_CHARS = int(
    os.environ.get("ORCH_WORKER_FEEDBACK_MAX_CHARS", "1000")
)
REVIEWER_FEEDBACK_MAX_CHARS = int(
    os.environ.get("ORCH_REVIEWER_FEEDBACK_MAX_CHARS", "1200")
)

# Rule E: number of consecutive identical (normalized) errors that triggers
# strategy-reset mode.
STRATEGY_RESET_THRESHOLD = 3

# Safety valve: stop the whole session after this many turns even if the
# model never reports task_status == "completed" and never calls
# request_context. Prevents a runaway loop from burning API calls forever.
MAX_TURNS = int(os.environ.get("ORCH_MAX_TURNS", "25"))
LLM_MAX_RETRIES = int(os.environ.get("ORCH_LLM_MAX_RETRIES", "8"))
PROTOCOL_ATTEMPTS_PER_COOKIE = int(
    os.environ.get("ORCH_PROTOCOL_ATTEMPTS_PER_COOKIE", "3")
)
AGENT_LOG_DIR = os.environ.get("ORCH_AGENT_LOG_DIR", "logs/agents")
# Max workstreams = parallel_managers * this multiplier (default 1: 4 managers → 4 streams).
MAX_WORKSTREAMS_PER_MANAGER_SLOT = int(
    os.environ.get("ORCH_MAX_WORKSTREAMS_PER_MANAGER_SLOT", "1")
)
RATE_LIMIT_COOLDOWN_SECONDS = int(
    os.environ.get("ORCH_RATE_LIMIT_COOLDOWN_SECONDS", str(5 * 60 * 60))
)
AUTO_CONTINUE_MAX_CYCLES = int(
    os.environ.get("ORCH_AUTO_CONTINUE_MAX_CYCLES", "20")
)

# Manager must plan major capability packages, not one tiny file per item.
MAX_MANAGER_WORK_ITEMS = int(os.environ.get("ORCH_MAX_MANAGER_WORK_ITEMS", "12"))
MAX_FILES_PER_WORK_PACKAGE = int(os.environ.get("ORCH_MAX_FILES_PER_WORK_PACKAGE", "12"))
PLANNER_COUNT_RETRIES = int(os.environ.get("ORCH_PLANNER_COUNT_RETRIES", "2"))
HIERARCHY_MAX_SOURCE_FILES = int(
    os.environ.get("ORCH_HIERARCHY_MAX_SOURCE_FILES", "32")
)
HIERARCHY_SOURCE_FILE_CHARS = int(
    os.environ.get("ORCH_HIERARCHY_SOURCE_FILE_CHARS", "24000")
)
HIERARCHY_DIRECTOR_SOURCE_CHARS = int(
    os.environ.get("ORCH_HIERARCHY_DIRECTOR_SOURCE_CHARS", "80000")
)
HIERARCHY_MANAGER_SOURCE_CHARS = int(
    os.environ.get("ORCH_HIERARCHY_MANAGER_SOURCE_CHARS", "48000")
)
MANAGER_RECOVERY_CYCLES = int(
    os.environ.get("ORCH_MANAGER_RECOVERY_CYCLES", "1")
)
DIRECTOR_RECOVERY_CYCLES = int(
    os.environ.get("ORCH_DIRECTOR_RECOVERY_CYCLES", "1")
)
TEST_TIMEOUT_SECONDS = int(
    os.environ.get("ORCH_TEST_TIMEOUT_SECONDS", "300")
)
SANDBOX_CANCELLATION_POLL_SECONDS = float(
    os.environ.get("ORCH_SANDBOX_CANCELLATION_POLL_SECONDS", "0.05")
)
SCOPE_CLAIM_TIMEOUT_SECONDS = float(
    os.environ.get("ORCH_SCOPE_CLAIM_TIMEOUT_SECONDS", "900")
)

# Model configuration for Web Claude Wrapper
MODEL_NAME = os.environ.get("ORCH_MODEL", "claude-sonnet-5")
MAX_TOKENS = int(os.environ.get("ORCH_MAX_TOKENS", "4096"))

# Cookie Directory for Web API authentication
COOKIES_DIR = os.environ.get("ORCH_COOKIES_DIR", "cookies")

# Filenames on disk (relative to project root) for the three fixed-purpose
# files described in the prompt.
RULES_FILENAME = "rules.json"
DECISIONS_FILENAME = "DECISIONS.md"
STATE_FILENAME = "state.json"
