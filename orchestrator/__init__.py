"""AI Coder Orchestrator — implementation of the v2 spec.

See README.md for the full design rationale. This package wires together:

- system_prompt.py   the static system prompt (Rules A-I)
- tools_schema.py     JSON schemas for submit_patch / request_context
- state_store.py      loading/saving rules.json, DECISIONS.md, state.json
- context_builder.py  assembling the per-turn user message, with [TRUNCATED]
                       markers for oversized files
- patch_engine.py     code-level enforcement of anchor uniqueness + the
                       protected-files list (never trusts the prompt alone)
- error_utils.py      error normalization + hashing for consecutive_error_count
- safety.py           git backup/rollback + a syntax gate before accepting a patch
- llm_client.py       cookie-backed Web Claude transport and account rotation
- orchestrator.py     the main turn loop
"""

__version__ = "0.4.0"
