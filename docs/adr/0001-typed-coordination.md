# ADR 0001: Typed hierarchical coordination

Status: Accepted

## Context

Fixed fan-out created idle agents for small changes and silently dropped work on
large plans. Free-form tickets also made completion impossible to prove.

## Decision

- Director fan-out is `1..maxManagers`; Manager coder fan-out is
  `1..(maxWorkersPerManager - 1)`, reserving one child slot for the Tester.
- Every work item carries a versioned `WorkContract` with inputs, outputs,
  scopes, acceptance criteria, tests, evidence, consumers, risk, approval
  policy, and priority.
- Logical agent IDs are stable across attempts. Physical provider accounts may
  change after a rate limit without changing the logical request.
- Completion is accepted only when each planned workstream, work item, logical
  agent, and model call belongs to exactly one terminal bucket.

## Consequences

The scheduler can safely parallelize non-conflicting scopes and can reconcile
partial failure. Planner output that exceeds a cap or violates a contract is
rejected before execution.
