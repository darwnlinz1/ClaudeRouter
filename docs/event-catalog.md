# Event catalog

Event contract version: `1`.

`orchestrator/event_schema.py` is the source of truth. The frontend declaration
is generated deterministically at
`frontend/src/generated/orchestrator-events.generated.ts`.

## Persisted envelope and wire shape

Every durable `EventEnvelope` has:

- `event_id`, `task_id`, `session_id`, and `event_type`;
- a dictionary `payload`;
- task-local `sequence`;
- envelope `version`;
- a timezone-aware `timestamp`;
- optional `workstream_id`, `work_item_id`, `agent_instance_id`, and `call_id`.

The React/SSE wire event uses `type` as the discriminator and flattens payload
and correlation fields for the generated TypeScript union. Known schema fields
also allow `attempt_id` as event-level correlation.

Known payloads are validated before persistence. Additive fields and unknown
event types are preserved to allow replay compatibility. Removing an event or
field, narrowing a field type, or making an existing optional field required
is breaking.

The following event types are marked terminal in canonical state retention:
`completion_reconciliation`, `effect_applied`, `finish_chat_turn`,
`hierarchy_completed`, `hierarchy_failed`, `model_request_aborted`,
`model_request_completed`, `model_request_failed`, `project_lease_lost`, and
`test_result`.

## Lifecycle events

- Task/turn: `task.started`, `turn_start`, `turn_phase`, `resume`,
  `resume_checkpoint_loaded`, and `finish_chat_turn`.
- Hierarchy: `hierarchy_fanout_planned`, `hierarchy_completed`,
  `hierarchy_failed`.
- Workstreams: `workstream_started`, `workstream_completed`,
  `workstream_failed`, `integration_result`.
- Agents: `agent_started`, `agent_progress`, `agent_message`, `agent_failed`.

## Planning and coordination

- `plan.created` records a durable plan revision.
- `plan_created` records selected Director workstreams and requested manager
  count.
- `manager_plan_created` records selected Worker items/count.
- `fanout_selected` records level, selected count, maximum, unused capacity,
  and reason.
- `planner_count_rejected` records a count outside the accepted bound.
- `plan_preflight_failed` records issues before Worker model calls begin.
- `director_replan_created` records a durable replan reason.
- `completion_reconciliation` records whether workstream, work item, agent, and
  call partitions balance. `balanced` is required.

`agent_message` formally requires source and target agent IDs and may carry
signal/summary fields. Current hierarchy emissions also add `handoff_id`,
`contract_id`, `contract_version`, `contract_sha256`, `artifacts`, and
`evidence`. Those are additive fields under event contract version 1 and are
preserved by replay. A `HandoffEnvelope` is appended as a separate repository
record only when the configured repository implements the optional handoff
hooks; the canonical `StateRepository` currently has no dedicated handoff,
contract-version, or agent-identity tables.

## Model and account lifecycle

- Requests: `model_request_started`, `model_request_replayed`,
  `model_request_completed`, `model_request_failed`,
  `model_request_aborted`.
- Protocol/stream: `protocol_retry`, `thinking`, `token`.
- Accounts: `account_rate_limited`, `account_switch`,
  `account_invalidated`.

`model_request_*` terminal events are mutually exclusive per attempt.
Provisional stream fragments are not committed from an incomplete or aborted
transport attempt.

## Mutation, tests, and review

- `execution_result` records patch/file acceptance, file path, line changes,
  feedback, and execution detail.
- `effect_applied` records effect identity and kind, idempotency key, file
  path, and before/after SHA-256.
- `test_result` records pass/status/acceptance, command, detail, requested
  versus actual isolation, isolation details, and output truncation.
- `review_result` records the independent verdict and feedback.

Canonical state schema 14 includes effect fencing/compensation, immutable
contract versions, stable logical identities, typed handoffs, and recoverable
managed-retention claims. These database migrations do not change event
contract version 1. Consumers must not infer receipt or retention state from an
`effect_applied` event alone.

## Operator policy

- `approval_requested` and `approval_decided` form the auditable human gate.
- `budget_updated` and `budget_exceeded` carry bounded task budget state.
- `project_lease_acquired` carries project key and fencing token.
- `project_lease_lost` is terminal evidence for stale ownership.

## Compatibility events

`status` and `error` retain legacy payload compatibility. New code should
prefer the specific lifecycle event above when one exists.

## Complete known discriminator list

The schema currently defines:

- account: `account_invalidated`, `account_rate_limited`, `account_switch`;
- agents: `agent_failed`, `agent_message`, `agent_progress`, `agent_started`;
- approvals and budgets: `approval_decided`, `approval_requested`,
  `budget_exceeded`, `budget_updated`;
- planning: `director_replan_created`, `fanout_selected`,
  `manager_plan_created`, `plan.created`, `plan_created`,
  `planner_count_rejected`, `plan_preflight_failed`;
- hierarchy/work: `hierarchy_completed`, `hierarchy_failed`,
  `hierarchy_fanout_planned`, `integration_result`, `workstream_completed`,
  `workstream_failed`, `workstream_started`;
- model transport: `model_request_aborted`, `model_request_completed`,
  `model_request_failed`, `model_request_replayed`, `model_request_started`,
  `protocol_retry`, `thinking`, `token`;
- execution/review: `effect_applied`, `execution_result`, `review_result`,
  `test_result`;
- leases/reconciliation: `completion_reconciliation`,
  `project_lease_acquired`, `project_lease_lost`;
- task/turn: `finish_chat_turn`, `resume`, `resume_checkpoint_loaded`,
  `task.started`, `turn_phase`, `turn_start`;
- compatibility: `error`, `status`.

## Regeneration and drift check

Regenerate only after intentionally changing the Python schema:

```powershell
python -c "from orchestrator.event_schema import write_typescript_artifact; write_typescript_artifact('.')"
```

Then run:

```powershell
python scripts/acceptance_checks.py event-schema
python scripts/check_compatibility.py
```

Do not update `docs/compatibility-baseline.json` merely to make a gate pass.
Follow the breaking-change process in `docs/versioning.md`.

These commands describe the required checks; they are not evidence by
themselves. The latest schema-14 result is recorded in
[acceptance evidence](acceptance-evidence.md) and must be replaced after source
changes.
