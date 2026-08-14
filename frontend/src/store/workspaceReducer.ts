import type {
  ActivityEntry,
  AgentFailure,
  AgentInstance,
  AgentRole,
  AgentStatus,
  CallAttempt,
  CompletionReconciliation,
  EventEnvelope,
  ExecutionState,
  FanoutSelection,
  ManagerNode,
  ReconciliationPartition,
  TaskSummary,
  TestResult,
  WorkContract,
  WorkspaceState,
} from '../types';

export const initialWorkspaceState: WorkspaceState = {
  agents: {},
  managers: [],
  graphRevision: 0,
  sequence: 0,
  connected: false,
  connection: 'idle',
  eventCount: 0,
  signals: [],
  usedAccounts: [],
  fanout: {
    plannedManagers: 0,
    plannedCoders: 0,
    plannedTesters: 0,
    plannedChildren: 0,
    maxManagers: 0,
    maxParallelManagers: 0,
    maxWorkersPerManager: 0,
    maxParallelWorkersPerManager: 0,
    maxParallelWorkers: 0,
    requestAttempts: 0,
    completedRequests: 0,
    replayedRequests: 0,
    accountSwitches: 0,
    failedRequests: 0,
    abortedRequests: 0,
    calledAgentIds: [],
    completedAgentIds: [],
    calledByRole: {},
    managerSelections: {},
  },
  effects: [],
};

type WorkspaceAction =
  | { type: 'load-task'; task: TaskSummary }
  | { type: 'reset'; task?: TaskSummary }
  | { type: 'connection'; connection: WorkspaceState['connection'] }
  | { type: 'event'; event: Record<string, unknown>; taskId: string }
  | { type: 'events'; events: Record<string, unknown>[]; taskId: string }
  | { type: 'focus-agent'; agentId: string }
  | { type: 'agent-configured'; agentId: string; model: string; effort: string };

export const WORKSPACE_LIMITS = {
  activity: 200,
  attempts: 50,
  failures: 30,
  outputCharacters: 60_000,
  signals: 100,
  tests: 50,
} as const;

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const stringValue = (...values: unknown[]): string | undefined => {
  const value = values.find((candidate) => typeof candidate === 'string' && candidate.trim());
  return typeof value === 'string' ? value : undefined;
};

const numberValue = (...values: unknown[]): number | undefined => {
  for (const value of values) {
    if (value == null || value === '') continue;
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
};

const booleanValue = (...values: unknown[]): boolean | undefined => {
  const value = values.find((candidate) => typeof candidate === 'boolean');
  return typeof value === 'boolean' ? value : undefined;
};

const stringList = (value: unknown): string[] =>
  Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
    : [];

const workContractValue = (...values: unknown[]): WorkContract | undefined => {
  const value = values.map(asRecord).find((candidate) => Object.keys(candidate).length > 0);
  if (!value) return undefined;
  const id = stringValue(value.id, value.contract_id);
  if (!id) return undefined;
  const risk = String(value.risk_level ?? value.riskLevel ?? 'low').toLowerCase();
  return {
    id,
    version: Math.max(1, numberValue(value.version, value.contract_version) ?? 1),
    inputArtifacts: stringList(value.input_artifacts ?? value.inputArtifacts),
    expectedOutputs: stringList(value.expected_outputs ?? value.expectedOutputs),
    readScopes: stringList(value.read_scopes ?? value.readScopes),
    writeScopes: stringList(value.write_scopes ?? value.writeScopes),
    acceptanceCriteria: stringList(value.acceptance_criteria ?? value.acceptanceCriteria),
    testRequirements: stringList(value.test_requirements ?? value.testRequirements),
    evidenceRequirements: stringList(value.evidence_requirements ?? value.evidenceRequirements),
    consumers: stringList(value.consumers),
    riskLevel: ['low', 'medium', 'high', 'critical'].includes(risk)
      ? (risk as WorkContract['riskLevel'])
      : 'low',
    priority: Math.max(0, numberValue(value.priority) ?? 0),
    approvalPolicy: ['never', 'risk_based', 'always'].includes(
      String(value.approval_policy ?? value.approvalPolicy ?? 'risk_based'),
    )
      ? (String(
          value.approval_policy ?? value.approvalPolicy ?? 'risk_based',
        ) as WorkContract['approvalPolicy'])
      : 'risk_based',
  };
};

const emptyPartition = (): ReconciliationPartition => ({
  planned: 0,
  completed: 0,
  skipped: 0,
  blocked: 0,
  failed: 0,
  preflightFailed: 0,
  terminal: 0,
  balanced: true,
  ids: {},
  invalidIds: [],
});

const partitionValue = (value: unknown): ReconciliationPartition => {
  const partition = asRecord(value);
  if (!Object.keys(partition).length) return emptyPartition();
  const ids = Object.fromEntries(
    Object.entries(asRecord(partition.ids)).map(([key, entries]) => [key, stringList(entries)]),
  );
  return {
    planned: numberValue(partition.planned) ?? 0,
    completed: numberValue(partition.completed) ?? 0,
    skipped: numberValue(partition.skipped) ?? 0,
    blocked: numberValue(partition.blocked) ?? 0,
    failed: numberValue(partition.failed) ?? 0,
    preflightFailed: numberValue(partition.preflight_failed, partition.preflightFailed) ?? 0,
    terminal: numberValue(partition.terminal) ?? 0,
    balanced: booleanValue(partition.balanced) ?? false,
    ids,
    invalidIds: stringList(partition.invalid_ids ?? partition.invalidIds),
  };
};

const reconciliationValue = (
  value: unknown,
  updatedAt?: string,
): CompletionReconciliation | undefined => {
  const reconciliation = asRecord(value);
  if (!Object.keys(reconciliation).length) return undefined;
  return {
    balanced: booleanValue(reconciliation.balanced) ?? false,
    errors: stringList(reconciliation.errors),
    workstreams: partitionValue(reconciliation.workstreams),
    workItems: partitionValue(reconciliation.work_items ?? reconciliation.workItems),
    agents: partitionValue(reconciliation.agents),
    calls: partitionValue(reconciliation.calls),
    updatedAt,
  };
};

const fanoutSelectionValue = (
  value: Record<string, unknown>,
  timestamp?: string,
): FanoutSelection => ({
  level: stringValue(value.level) ?? 'unknown',
  maximum: numberValue(value.maximum, value.max_count, value.maxCount) ?? 0,
  selected: numberValue(value.selected, value.selected_count, value.selectedCount) ?? 0,
  unused:
    numberValue(value.unused_capacity, value.unusedCapacity, value.unused) ??
    Math.max(
      0,
      (numberValue(value.maximum, value.max_count, value.maxCount) ?? 0) -
        (numberValue(value.selected, value.selected_count, value.selectedCount) ?? 0),
    ),
  reason: stringValue(value.reason),
  managerId: stringValue(value.manager_id, value.managerId),
  workstreamId: stringValue(value.workstream_id, value.workstreamId),
  at: timestamp,
});

const roleValue = (value: unknown): AgentRole => {
  const role = String(value ?? 'agent').toLowerCase();
  if (role === 'supervisor') return 'director';
  if (['director', 'manager', 'worker', 'tester', 'reviewer'].includes(role)) {
    return role as AgentRole;
  }
  return 'agent';
};

const statusValue = (value: unknown): AgentStatus => {
  const status = String(value ?? '').toLowerCase();
  if (['complete', 'completed', 'approved', 'success', 'passed', 'done'].includes(status)) {
    return 'passed';
  }
  if (['error', 'failed', 'preflight_failed', 'rejected', 'revise'].includes(status)) {
    return 'failed';
  }
  if (['queued', 'pending', 'planned', 'ready', 'blocked'].includes(status)) return 'queued';
  if (['waiting', 'waiting_input', 'waiting_for_slot'].includes(status)) return 'waiting';
  if (['stopped', 'cancelled', 'canceled'].includes(status)) return 'stopped';
  if (['running', 'planning', 'coding', 'reviewing', 'active'].includes(status)) return 'running';
  return 'idle';
};

const executionStateValue = (value: unknown): ExecutionState => {
  const status = String(value ?? '').toLowerCase();
  if (['planned'].includes(status)) return 'planned';
  if (['queued', 'pending', 'ready'].includes(status)) return 'queued';
  if (['waiting_dependency', 'dependency_wait'].includes(status)) return 'waiting_dependency';
  if (['waiting_scope', 'waiting_for_scope', 'waiting_for_slot'].includes(status)) {
    return 'waiting_scope';
  }
  if (['running', 'active', 'coding', 'calling_model'].includes(status)) return 'in_flight';
  if (['testing', 'reviewing'].includes(status)) return 'testing';
  if (['blocked', 'waiting_input'].includes(status)) return 'blocked';
  if (['skipped'].includes(status)) return 'skipped';
  if (['preflight_failed'].includes(status)) return 'preflight_failed';
  if (['complete', 'completed', 'approved', 'success', 'passed', 'done'].includes(status)) {
    return 'completed';
  }
  if (['error', 'failed', 'rejected', 'revise'].includes(status)) return 'failed';
  if (['stopped', 'cancelled', 'canceled', 'aborted'].includes(status)) return 'aborted';
  return 'idle';
};

export function normalizeEvent(
  raw: Record<string, unknown>,
  taskId: string,
  fallbackSequence = 1,
): EventEnvelope {
  const nestedPayload = asRecord(raw.payload);
  // Keep envelope metadata available to handlers while accepting both current
  // nested payloads and legacy flat event frames.
  const payload = Object.keys(nestedPayload).length ? { ...raw, ...nestedPayload } : raw;
  const metadata = asRecord(raw.metadata);
  const type = stringValue(raw.type, raw.event_type, payload.type) ?? 'unknown';
  const sequence =
    numberValue(raw.sequence, raw.seq, raw._seq, metadata.sequence) ?? fallbackSequence;
  const role = roleValue(stringValue(raw.role, payload.role, metadata.role));
  const managerId = stringValue(
    raw.manager_id,
    payload.manager_id,
    raw.parent_agent_instance_id,
    payload.parent_agent_instance_id,
  );
  const agentInstanceId = stringValue(
    raw.agent_instance_id,
    payload.agent_instance_id,
    raw.agent_id,
    payload.agent_id,
  );

  return {
    version: numberValue(raw.version, raw.schema_version, raw.v) ?? 0,
    sequence,
    type,
    timestamp: stringValue(raw.timestamp, raw.at, payload.timestamp) ?? new Date().toISOString(),
    taskId: stringValue(raw.task_id, payload.task_id) ?? taskId,
    sessionId: stringValue(raw.session_id, payload.session_id),
    workstreamId: stringValue(raw.workstream_id, payload.workstream_id),
    workItemId: stringValue(raw.work_item_id, payload.work_item_id, payload.ticket_id),
    managerId,
    agentInstanceId,
    callId: stringValue(
      raw.call_id,
      payload.call_id,
      raw.logical_request_id,
      payload.logical_request_id,
      raw.logicalRequestId,
      payload.logicalRequestId,
    ),
    attemptId: stringValue(raw.attempt_id, payload.attempt_id, raw.attemptId, payload.attemptId),
    attempt: numberValue(raw.attempt, payload.attempt),
    requestRevision: numberValue(
      raw.request_revision,
      payload.request_revision,
      raw.requestRevision,
      payload.requestRevision,
    ),
    provider: stringValue(raw.provider, payload.provider),
    account: stringValue(raw.account, payload.account, payload.account_id, payload.accountId),
    provisional: booleanValue(raw.provisional, payload.provisional),
    committed: booleanValue(raw.committed, payload.committed),
    role,
    payload,
    raw,
  };
}

const titleFor = (role: AgentRole, id: string): string => {
  const suffix = id
    .split(/[:/_-]/)
    .filter(Boolean)
    .at(-1)
    ?.slice(0, 7);
  const label = role === 'tester' ? 'Tester / Reviewer' : role[0].toUpperCase() + role.slice(1);
  return `${label}${suffix && !['root', role].includes(suffix) ? ` · ${suffix}` : ''}`;
};

const emptyAgent = (event: EventEnvelope): AgentInstance => {
  const payload = event.payload;
  const role = event.role ?? roleValue(payload.role);
  const id = event.agentInstanceId ?? `${role}:root`;
  return {
    id,
    role,
    title: stringValue(payload.name, payload.title) ?? titleFor(role, id),
    managerId: event.managerId,
    workstreamId: event.workstreamId,
    workItemId: event.workItemId,
    status: 'idle',
    executionState: 'idle',
    phase: 'idle',
    model: stringValue(payload.model),
    effort: stringValue(payload.effort),
    account: stringValue(payload.account),
    startedAt: stringValue(payload.started_at),
    updatedAt: event.timestamp,
    durationMs: numberValue(payload.duration_ms),
    tokenCount: 0,
    goal: stringValue(payload.goal, payload.objective) ?? '',
    prompt: stringValue(payload.prompt) ?? '',
    action: '',
    output: '',
    thinking: '',
    contextFiles: [],
    diff: '',
    tests: [],
    activity: [],
    unread: 0,
    requestAttempts: 0,
    replayCount: 0,
    accountSwitchCount: 0,
    committedChunks: 0,
    provisionalChunks: 0,
    callAttempts: [],
    failures: [],
    workContract: workContractValue(payload.contract, payload.work_contract),
  };
};

const activityTone = (type: string, payload: Record<string, unknown>): ActivityEntry['tone'] => {
  if (
    type === 'error' ||
    type.includes('failed') ||
    type.includes('lost') ||
    String(payload.status).toLowerCase() === 'failed'
  )
    return 'error';
  if (type.includes('aborted')) return 'warning';
  if (type.includes('retry') || type.includes('switch')) return 'warning';
  if (type.includes('finish') || type.includes('approved') || payload.accepted === true) {
    return 'success';
  }
  if (type === 'token' || type === 'thinking') return 'neutral';
  return 'info';
};

const eventMessage = (event: EventEnvelope): string => {
  const p = event.payload;
  if (event.type === 'account_switch') {
    const from = stringValue(p.from_account, p.fromAccount) ?? 'previous account';
    const to = stringValue(p.to_account, p.toAccount) ?? 'replacement account';
    const reason = stringValue(p.reason) ?? 'retry';
    return `${reason}: ${from} → ${to}; replaying the same logical request`;
  }
  if (event.type === 'account_rate_limited') {
    const from = stringValue(p.from_account) ?? 'account';
    return `${from} received 429 and entered cooldown`;
  }
  if (event.type === 'model_request_started') {
    return p.replayed === true
      ? 'Replayed the same logical model request'
      : 'Model request started';
  }
  if (event.type === 'model_request_completed') return 'Model request completed';
  if (event.type === 'model_request_failed') {
    return `Model request failed${event.provider ? ` on ${event.provider}` : ''}: ${
      stringValue(p.error) ?? 'unknown provider error'
    }`;
  }
  if (event.type === 'model_request_aborted') {
    return `Model request aborted: ${stringValue(p.error) ?? 'cancelled before completion'}`;
  }
  if (event.type === 'fanout_selected') {
    return `${numberValue(p.selected) ?? 0} selected of ${numberValue(p.maximum) ?? 0} maximum${
      stringValue(p.reason) ? ` — ${stringValue(p.reason)}` : ''
    }`;
  }
  if (event.type === 'completion_reconciliation') {
    return p.balanced === true
      ? 'Completion reconciliation balanced'
      : `Completion reconciliation failed: ${
          stringList(p.errors).join('; ') || 'incomplete terminal partitions'
        }`;
  }
  if (event.type === 'effect_applied') {
    return `Applied ${stringValue(p.effect_kind) ?? 'effect'} to ${
      stringValue(p.file_path) ?? 'workspace'
    }`;
  }
  const files = Array.isArray(p.files_needed) ? p.files_needed.join(', ') : undefined;
  return (
    stringValue(
      p.message,
      p.detail,
      p.data,
      p.action,
      p.reason,
      p.error,
      p.execution_result,
      p.reviewer_feedback,
      files,
    ) ?? event.type.replaceAll('_', ' ')
  );
};

const appendActivity = (agent: AgentInstance, event: EventEnvelope): AgentInstance => {
  const entry: ActivityEntry = {
    id: `${event.sequence}:${event.type}`,
    sequence: event.sequence,
    type: event.type,
    at: event.timestamp,
    message: eventMessage(event),
    tone: activityTone(event.type, event.payload),
  };
  const isStreaming = event.type === 'token' || event.type === 'thinking';
  return {
    ...agent,
    activity: isStreaming
      ? agent.activity
      : [...agent.activity, entry].slice(-WORKSPACE_LIMITS.activity),
    unread: agent.unread + (isStreaming ? 0 : 1),
    updatedAt: event.timestamp,
  };
};

const upsertCallAttempt = (
  agent: AgentInstance,
  event: EventEnvelope,
  status: CallAttempt['status'],
): AgentInstance => {
  const p = event.payload;
  const logicalRequestId = stringValue(p.logical_request_id, p.logicalRequestId, event.callId);
  const attemptNumber =
    event.attempt ?? numberValue(p.attempt) ?? Math.max(1, agent.requestAttempts);
  const id =
    event.attemptId ??
    stringValue(p.attempt_id) ??
    `${logicalRequestId ?? agent.id}:attempt:${attemptNumber}`;
  const existing = agent.callAttempts.find((attempt) => attempt.id === id);
  const attempt: CallAttempt = {
    id,
    logicalRequestId: logicalRequestId ?? existing?.logicalRequestId,
    attempt: attemptNumber || existing?.attempt || 1,
    requestRevision:
      event.requestRevision ??
      numberValue(p.request_revision, p.requestRevision) ??
      existing?.requestRevision,
    provider: event.provider ?? stringValue(p.provider) ?? existing?.provider,
    account:
      event.account ?? stringValue(p.account, p.account_id, p.accountId) ?? existing?.account,
    status,
    replayed:
      booleanValue(p.replayed, p.is_replay, p.isReplay) === true || existing?.replayed === true,
    startedAt:
      status === 'running' ? (existing?.startedAt ?? event.timestamp) : existing?.startedAt,
    finishedAt: status === 'running' ? undefined : event.timestamp,
    error: stringValue(p.error) ?? existing?.error,
    errorType: stringValue(p.error_type, p.errorType) ?? existing?.errorType,
  };
  return {
    ...agent,
    provider: attempt.provider ?? agent.provider,
    account: attempt.account ?? agent.account,
    currentAttemptId: id,
    requestRevision: attempt.requestRevision ?? agent.requestRevision,
    callAttempts: [...agent.callAttempts.filter((candidate) => candidate.id !== id), attempt].slice(
      -WORKSPACE_LIMITS.attempts,
    ),
  };
};

const mergeAgentEvent = (current: AgentInstance, event: EventEnvelope): AgentInstance => {
  const p = event.payload;
  let agent = appendActivity(current, event);
  const phase = stringValue(p.stage, p.phase, p.status);
  const chunk = stringValue(p.text, p.chunk);
  const files = [
    ...(Array.isArray(p.files) ? p.files : []),
    ...(Array.isArray(p.files_needed) ? p.files_needed : []),
    ...(p.file_path ? [p.file_path] : []),
  ].filter((item): item is string => typeof item === 'string');

  if (phase) {
    agent = {
      ...agent,
      phase,
      status: statusValue(phase) || agent.status,
      executionState: executionStateValue(phase),
    };
  }
  if (
    event.type === 'agent_started' ||
    event.type === 'agent_progress' ||
    event.type === 'status'
  ) {
    const startedStatus = statusValue(phase) || statusValue(p.status);
    agent = {
      ...agent,
      status:
        startedStatus && startedStatus !== 'idle'
          ? startedStatus
          : event.type === 'agent_started'
            ? 'running'
            : agent.status,
      phase: phase ?? agent.phase ?? 'running',
      executionState: executionStateValue(phase ?? p.status ?? 'running'),
      startedAt: agent.startedAt ?? event.timestamp,
    };
    if (event.type === 'status' && phase === 'calling_model') {
      const legacyRequest = !stringValue(p.logical_request_id);
      agent = {
        ...agent,
        action: 'Model call started',
        requestAttempts: agent.requestAttempts + (legacyRequest ? 1 : 0),
      };
    }
  }
  if (event.type === 'model_request_started' || event.type === 'model_request_replayed') {
    const replayed =
      event.type === 'model_request_replayed' ||
      booleanValue(p.replayed, p.is_replay, p.isReplay) === true;
    agent = {
      ...agent,
      status: 'running',
      executionState: 'in_flight',
      phase: 'calling_model',
      action: replayed ? 'Replaying model request' : 'Model call started',
      requestAttempts: agent.requestAttempts + 1,
      replayCount: agent.replayCount + (replayed ? 1 : 0),
      logicalRequestId:
        stringValue(p.logical_request_id, p.logicalRequestId, event.callId) ??
        agent.logicalRequestId,
      requestFingerprint:
        stringValue(p.request_fingerprint, p.requestFingerprint) ?? agent.requestFingerprint,
    };
    agent = upsertCallAttempt(
      agent,
      replayed ? { ...event, payload: { ...p, replayed: true } } : event,
      'running',
    );
  }
  if (event.type === 'model_request_completed') {
    agent = {
      ...agent,
      action: 'Model response received',
      logicalRequestId:
        stringValue(p.logical_request_id, p.logicalRequestId, event.callId) ??
        agent.logicalRequestId,
      requestFingerprint:
        stringValue(p.request_fingerprint, p.requestFingerprint) ?? agent.requestFingerprint,
    };
    agent = upsertCallAttempt(agent, event, 'completed');
  }
  if (event.type === 'model_request_failed' || event.type === 'model_request_aborted') {
    const failure: AgentFailure = {
      id: `${event.sequence}:${event.type}:${event.attemptId ?? event.callId ?? 'request'}`,
      type: event.type,
      message: stringValue(p.error, p.message) ?? event.type.replaceAll('_', ' '),
      at: event.timestamp,
      attemptId: event.attemptId,
      provider: event.provider,
    };
    agent = upsertCallAttempt(
      {
        ...agent,
        status: event.type === 'model_request_aborted' ? 'stopped' : 'failed',
        executionState: event.type === 'model_request_aborted' ? 'aborted' : 'failed',
        phase: event.type === 'model_request_aborted' ? 'model_aborted' : 'model_failed',
        action: eventMessage(event),
        error: failure.message,
        failures: [...agent.failures, failure].slice(-WORKSPACE_LIMITS.failures),
      },
      event,
      event.type === 'model_request_aborted' ? 'aborted' : 'failed',
    );
  }
  if (event.type === 'account_switch') {
    agent = {
      ...agent,
      status: 'running',
      executionState: 'in_flight',
      phase: 'account_failover',
      action: eventMessage(event),
      previousAccount: stringValue(p.from_account, p.fromAccount) ?? agent.previousAccount,
      account: stringValue(p.to_account, p.toAccount) ?? agent.account,
      accountSwitchCount: agent.accountSwitchCount + 1,
      logicalRequestId:
        stringValue(p.logical_request_id, p.logicalRequestId, event.callId) ??
        agent.logicalRequestId,
      requestFingerprint:
        stringValue(p.request_fingerprint, p.requestFingerprint) ?? agent.requestFingerprint,
    };
  }
  if (event.type === 'token' && chunk && event.provisional !== true) {
    agent = {
      ...agent,
      output: `${agent.output}${chunk}`.slice(-WORKSPACE_LIMITS.outputCharacters),
      tokenCount: agent.tokenCount + Math.max(1, Math.ceil(chunk.length / 4)),
      committedChunks: agent.committedChunks + 1,
      status: 'running',
      executionState: 'in_flight',
    };
  }
  if (event.type === 'thinking' && chunk && event.provisional !== true) {
    agent = {
      ...agent,
      thinking: `${agent.thinking}${chunk}`.slice(-WORKSPACE_LIMITS.outputCharacters),
      committedChunks: agent.committedChunks + 1,
      status: 'running',
      executionState: 'in_flight',
    };
  }
  if (
    (event.type === 'token' || event.type === 'thinking') &&
    chunk &&
    event.provisional === true
  ) {
    agent = { ...agent, provisionalChunks: agent.provisionalChunks + 1 };
  }
  if (event.type === 'agent_action') {
    agent = {
      ...agent,
      action: eventMessage(event),
      status: 'running',
      executionState: 'in_flight',
    };
  }
  if (files.length) {
    agent = { ...agent, contextFiles: [...new Set([...agent.contextFiles, ...files])] };
  }
  const diff = stringValue(p.diff, p.patch, p.patch_text);
  if (diff) agent = { ...agent, diff };

  if (event.type === 'test_result' || event.type === 'execution_result') {
    const accepted = p.accepted === true || statusValue(p.status) === 'passed';
    const rejected = p.accepted === false || statusValue(p.status) === 'failed';
    const test: TestResult = {
      id: stringValue(p.test_run_id, p.testRunId, p.attempt_id, p.attemptId) ?? `${event.sequence}`,
      name: stringValue(p.name, p.command, p.file_path) ?? 'Machine gate',
      status:
        statusValue(p.status) === 'running'
          ? 'running'
          : accepted
            ? 'passed'
            : rejected
              ? 'failed'
              : 'unknown',
      detail: stringValue(p.detail, p.execution_result),
      durationMs: numberValue(p.duration_ms, p.durationMs),
      requestedIsolation: stringValue(p.requested_isolation, p.requestedIsolation),
      actualIsolation: stringValue(p.actual_isolation, p.actualIsolation),
      isolationDetails: stringValue(p.isolation_details, p.isolationDetails),
      outputTruncated: booleanValue(p.output_truncated, p.outputTruncated),
    };
    agent = {
      ...agent,
      tests: [...agent.tests, test].slice(-WORKSPACE_LIMITS.tests),
      executionState: test.status === 'running' ? 'testing' : agent.executionState,
    };
  }
  if (event.type === 'review_result') {
    agent = {
      ...agent,
      status: String(p.verdict).toLowerCase() === 'approved' ? 'passed' : 'failed',
      executionState: String(p.verdict).toLowerCase() === 'approved' ? 'completed' : 'failed',
      action: eventMessage(event),
    };
  }
  if (event.type.includes('finish') || event.type === 'done' || event.type === 'agent_completed') {
    agent = {
      ...agent,
      status: 'passed',
      executionState: 'completed',
      phase: 'complete',
    };
  }
  if (
    event.type === 'error' ||
    event.type === 'agent_failed' ||
    event.type === 'project_lease_lost'
  ) {
    agent = {
      ...agent,
      status: 'failed',
      executionState: event.type === 'agent_failed' ? executionStateValue(p.status) : 'failed',
      phase: 'error',
      error: eventMessage(event),
    };
  }
  const contract = workContractValue(p.contract, p.work_contract);
  return {
    ...agent,
    role:
      event.role && event.role !== 'agent'
        ? event.role
        : agent.role === 'agent' && event.role
          ? event.role
          : agent.role,
    title: stringValue(p.name, p.title) ?? agent.title,
    model: stringValue(p.model) ?? agent.model,
    effort: stringValue(p.effort) ?? agent.effort,
    account: stringValue(p.account, p.account_id) ?? agent.account,
    provider: event.provider ?? stringValue(p.provider) ?? agent.provider,
    goal: stringValue(p.goal, p.objective) ?? agent.goal,
    prompt: stringValue(p.prompt) ?? agent.prompt,
    managerId: event.managerId ?? agent.managerId,
    workstreamId: event.workstreamId ?? agent.workstreamId,
    workItemId: event.workItemId ?? agent.workItemId,
    durationMs: numberValue(p.duration_ms) ?? agent.durationMs,
    workContract: contract ?? agent.workContract,
  };
};

const resolveManagerNodeId = (
  managers: ManagerNode[],
  event: EventEnvelope,
  agent: AgentInstance,
): string | undefined => {
  if (agent.role === 'manager') return agent.id;

  const explicit = event.managerId ?? agent.managerId;
  if (explicit) {
    const byAgent = managers.find(
      (manager) => manager.id === explicit || manager.agentId === explicit,
    );
    return byAgent?.id ?? explicit;
  }

  const workstream = event.workstreamId ?? agent.workstreamId;
  if (workstream) {
    const byStream = managers.find(
      (manager) =>
        manager.workstreamId === workstream ||
        manager.id === workstream ||
        manager.agentId === workstream,
    );
    if (byStream) return byStream.id;
    return workstream;
  }

  const owning = managers.find((manager) =>
    manager.items.some((item) => item.agentIds.includes(agent.id)),
  );
  return owning?.id;
};

const ensureDag = (
  managers: ManagerNode[],
  event: EventEnvelope,
  agent: AgentInstance,
): ManagerNode[] => {
  // Drop legacy placeholder columns if any still linger in state.
  const clean = managers.some((manager) => manager.id === 'manager:legacy')
    ? managers.filter((manager) => manager.id !== 'manager:legacy')
    : managers;
  if (agent.role === 'director') return clean;

  const managerId = resolveManagerNodeId(clean, event, agent);
  if (!managerId) return clean;

  const existing = clean.find(
    (manager) =>
      manager.id === managerId ||
      manager.agentId === managerId ||
      (event.workstreamId != null && manager.workstreamId === event.workstreamId),
  );
  const resolvedId = existing?.id ?? managerId;
  const manager: ManagerNode = existing ?? {
    id: resolvedId,
    title:
      stringValue(event.payload.workstream_title, event.payload.manager_title) ??
      titleFor('manager', resolvedId),
    status: 'queued',
    dependencies: Array.isArray(event.payload.dependencies)
      ? event.payload.dependencies.filter((item): item is string => typeof item === 'string')
      : [],
    contract: workContractValue(event.payload.contract, event.payload.work_contract),
    items: [],
  };
  let next: ManagerNode = {
    ...manager,
    id: resolvedId,
    status:
      agent.role === 'manager'
        ? agent.status
        : manager.status === 'idle'
          ? 'running'
          : manager.status,
    agentId: agent.role === 'manager' ? agent.id : manager.agentId,
    workstreamId: event.workstreamId ?? agent.workstreamId ?? manager.workstreamId,
  };

  if (event.type === 'manager_plan_created' && Array.isArray(event.payload.work_items)) {
    next = {
      ...next,
      status: 'queued',
      agentId: agent.role === 'manager' ? agent.id : (next.agentId ?? event.managerId),
      workstreamId: event.workstreamId ?? agent.workstreamId ?? next.workstreamId,
      title: stringValue(event.payload.workstream_title, event.payload.manager_title) ?? next.title,
      contract:
        workContractValue(event.payload.contract, event.payload.work_contract) ?? next.contract,
      items: event.payload.work_items
        .map((value) => asRecord(value))
        .filter((value) => typeof value.id === 'string')
        .map((value) => ({
          id: String(value.id),
          managerId: resolvedId,
          title: stringValue(value.title, value.file_path) ?? String(value.id),
          status: 'queued' as AgentStatus,
          dependencies: Array.isArray(value.dependencies)
            ? value.dependencies.filter((item): item is string => typeof item === 'string')
            : [],
          agentIds: [],
          contract: workContractValue(value.contract, value.work_contract),
        })),
    };
  }

  if (agent.role === 'worker' || agent.role === 'tester' || agent.role === 'reviewer') {
    const itemId = event.workItemId ?? agent.workItemId ?? `item:${agent.id}`;
    const item = next.items.find((candidate) => candidate.id === itemId);
    const agentIds = [...new Set([...(item?.agentIds ?? []), agent.id])];
    const nextItem = {
      id: itemId,
      managerId: resolvedId,
      title:
        stringValue(event.payload.work_item_title, event.payload.ticket, event.payload.file_path) ??
        item?.title ??
        `Work item ${next.items.length + 1}`,
      status: agent.status,
      dependencies: Array.isArray(event.payload.dependencies)
        ? event.payload.dependencies.filter((value): value is string => typeof value === 'string')
        : (item?.dependencies ?? []),
      agentIds,
      contract:
        workContractValue(event.payload.contract, event.payload.work_contract) ?? item?.contract,
    };
    next = {
      ...next,
      status: agent.status === 'failed' ? 'failed' : 'running',
      items: item
        ? next.items.map((candidate) => (candidate.id === itemId ? nextItem : candidate))
        : [...next.items, nextItem],
    };
  }

  if (existing) {
    return clean.map((candidate) =>
      candidate.id === existing.id || candidate.id === resolvedId ? next : candidate,
    );
  }
  return [...clean, next];
};

const stateFromTask = (task: TaskSummary): WorkspaceState => {
  const hierarchy = asRecord(task.hierarchy);
  const fanoutSnapshot = asRecord(hierarchy.fanout);
  const execution = asRecord(hierarchy.execution);
  const settings = asRecord(task.settings);
  const legacyWorkstreamEntries = Object.entries(asRecord(hierarchy.workstreams));
  const legacyWorkstreams = legacyWorkstreamEntries.map(([, value]) => asRecord(value));
  const legacyManagerCount = legacyWorkstreams.length;
  const legacyWorkerCount = legacyWorkstreams.reduce(
    (total, stream) => total + (numberValue(stream.requested_worker_count) ?? 0),
    0,
  );
  const legacyMaxWorkers = legacyWorkstreams.reduce(
    (maximum, stream) => Math.max(maximum, numberValue(stream.requested_worker_count) ?? 0),
    0,
  );
  const lease = asRecord(hierarchy.project_lease);
  const sandbox = asRecord(hierarchy.sandbox);
  const maxCodersPerManager = numberValue(fanoutSnapshot.max_coders_per_manager);
  const effects = Object.entries(asRecord(hierarchy.effects))
    .map(([id, raw]) => {
      const value = asRecord(raw);
      return {
        id,
        kind: stringValue(value.kind),
        target: stringValue(value.target),
        beforeSha256: stringValue(value.before_sha256),
        afterSha256: stringValue(value.after_sha256),
        status: 'applied' as const,
        at: task.updated_at ?? task.created_at ?? new Date(0).toISOString(),
      };
    })
    .slice(-200);
  const selections = asRecord(hierarchy.fanout_selections);
  let directorSelection: FanoutSelection | undefined;
  const managerSelections: Record<string, FanoutSelection> = {};
  for (const [key, raw] of Object.entries(selections)) {
    const selection = fanoutSelectionValue(asRecord(raw));
    if (selection.level === 'manager') directorSelection = selection;
    else managerSelections[key] = selection;
  }
  const managers: ManagerNode[] = legacyWorkstreamEntries.map(([entryId, raw]) => {
    const stream = asRecord(raw);
    const managerId = stringValue(stream.id, stream.workstream_id) ?? entryId;
    const rawItems = Array.isArray(stream.work_items) ? stream.work_items : [];
    return {
      id: managerId,
      title: stringValue(stream.title, stream.name) ?? managerId,
      status: statusValue(stream.status),
      dependencies: stringList(stream.dependencies),
      agentId: stringValue(stream.agent_instance_id, stream.manager_agent_id, stream.agent_id),
      workstreamId: stringValue(stream.workstream_id, stream.id) ?? managerId,
      contract: workContractValue(stream.contract, stream.work_contract),
      items: rawItems.map((rawItem, index) => {
        const item = asRecord(rawItem);
        const itemId = stringValue(item.id, item.work_item_id) ?? `${managerId}:item:${index + 1}`;
        return {
          id: itemId,
          managerId,
          title: stringValue(item.title, item.name, item.file_path) ?? itemId,
          status: statusValue(item.status),
          dependencies: stringList(item.dependencies),
          agentIds: stringList(item.agent_ids ?? item.agent_instance_ids),
          contract: workContractValue(item.contract, item.work_contract),
        };
      }),
    };
  });
  return {
    ...initialWorkspaceState,
    task,
    updatedAt: task.updated_at,
    managers,
    graphRevision: managers.length ? 1 : 0,
    effects,
    reconciliation: reconciliationValue(hierarchy.reconciliation, task.updated_at),
    projectLease: Object.keys(lease).length
      ? {
          status:
            lease.status === 'lost' ? 'lost' : lease.status === 'active' ? 'active' : 'unknown',
          projectKey: stringValue(lease.project_key),
          fencingToken: numberValue(lease.fencing_token),
          isolationLevel: stringValue(lease.isolation_level),
          updatedAt: task.updated_at,
        }
      : undefined,
    sandbox: Object.keys(sandbox).length
      ? {
          requestedIsolation: stringValue(sandbox.requested_isolation),
          actualIsolation: stringValue(sandbox.actual_isolation),
          isolationDetails: stringValue(sandbox.isolation_details),
          outputTruncated: booleanValue(sandbox.output_truncated),
          updatedAt: task.updated_at,
        }
      : undefined,
    fanout: {
      ...initialWorkspaceState.fanout,
      plannedManagers:
        numberValue(
          fanoutSnapshot.manager_count,
          hierarchy.requested_manager_count,
          legacyManagerCount,
        ) ?? 0,
      plannedCoders: numberValue(fanoutSnapshot.coder_count, legacyWorkerCount) ?? 0,
      plannedTesters: numberValue(fanoutSnapshot.tester_count, legacyManagerCount) ?? 0,
      plannedChildren:
        numberValue(fanoutSnapshot.child_agent_count, legacyWorkerCount + legacyManagerCount) ?? 0,
      maxManagers:
        numberValue(
          fanoutSnapshot.max_manager_count,
          settings.max_managers,
          settings.max_parallel_managers,
          hierarchy.requested_manager_count,
          legacyManagerCount,
        ) ?? 0,
      maxParallelManagers:
        numberValue(fanoutSnapshot.max_parallel_managers, settings.max_parallel_managers) ?? 0,
      maxWorkersPerManager:
        numberValue(settings.max_workers_per_manager) ??
        (maxCodersPerManager != null ? maxCodersPerManager + 1 : legacyMaxWorkers + 1),
      maxParallelWorkersPerManager:
        numberValue(
          fanoutSnapshot.max_parallel_workers_per_manager,
          settings.max_parallel_workers_per_manager,
        ) ?? 0,
      maxParallelWorkers:
        numberValue(fanoutSnapshot.max_parallel_workers, settings.max_parallel_workers) ?? 0,
      requestAttempts: numberValue(execution.request_attempts) ?? 0,
      completedRequests: numberValue(execution.completed_requests) ?? 0,
      replayedRequests: numberValue(execution.replayed_requests) ?? 0,
      accountSwitches: numberValue(execution.account_switches) ?? 0,
      failedRequests: numberValue(execution.failed_requests) ?? 0,
      abortedRequests: numberValue(execution.aborted_requests) ?? 0,
      calledAgentIds: stringList(execution.called_agent_ids),
      completedAgentIds: stringList(execution.completed_agent_ids),
      calledByRole: asRecord(execution.called_by_role) as Partial<Record<AgentRole, number>>,
      directorSelection,
      managerSelections,
    },
  };
};

const isStreamingEvent = (raw: Record<string, unknown>): boolean => {
  const type = stringValue(raw.type, raw.event_type, asRecord(raw.payload).type);
  return type === 'token' || type === 'thinking';
};

type StreamingAgentBatch = {
  agent: AgentInstance;
  lastEvent: EventEnvelope;
  outputChunks: string[];
  thinkingChunks: string[];
  tokenCount: number;
  committedChunks: number;
  provisionalChunks: number;
  receivedCommittedChunk: boolean;
};

/**
 * Token and thinking frames are the hot path. Collapse a whole frame's worth
 * into one agents-map clone and one clone per changed agent. Graph structures
 * deliberately retain their identities because stream text cannot change DAG
 * topology.
 */
const reduceStreamingBatch = (
  state: WorkspaceState,
  events: Record<string, unknown>[],
  taskId: string,
): WorkspaceState => {
  let sequence = state.sequence;
  let eventCount = state.eventCount;
  let updatedAt = state.updatedAt;
  const batches = new Map<string, StreamingAgentBatch>();
  const accountCandidates = new Set(state.usedAccounts);

  for (const raw of events) {
    const event = normalizeEvent(raw, taskId, sequence + 1);
    if (event.sequence <= sequence) continue;
    sequence = event.sequence;
    eventCount += 1;
    updatedAt = event.timestamp;

    for (const account of [
      stringValue(
        event.payload.account,
        event.payload.account_id,
        event.payload.accountId,
        event.raw.account,
      ),
      stringValue(event.payload.from_account, event.payload.fromAccount, event.raw.from_account),
      stringValue(event.payload.to_account, event.payload.toAccount, event.raw.to_account),
    ]) {
      if (account && account.toLowerCase() !== 'redacted') accountCandidates.add(account);
    }

    const agentId = event.agentInstanceId;
    if (!agentId) continue;
    const existing = batches.get(agentId);
    const current =
      existing?.agent ??
      state.agents[agentId] ??
      emptyAgent({
        ...event,
        agentInstanceId: agentId,
      });
    const batch: StreamingAgentBatch = existing ?? {
      agent: current,
      lastEvent: event,
      outputChunks: [],
      thinkingChunks: [],
      tokenCount: 0,
      committedChunks: 0,
      provisionalChunks: 0,
      receivedCommittedChunk: false,
    };
    const chunk = stringValue(event.payload.text, event.payload.chunk);
    if (chunk && event.provisional === true) {
      batch.provisionalChunks += 1;
    } else if (chunk) {
      if (event.type === 'token') {
        batch.outputChunks.push(chunk);
        batch.tokenCount += Math.max(1, Math.ceil(chunk.length / 4));
      } else {
        batch.thinkingChunks.push(chunk);
      }
      batch.committedChunks += 1;
      batch.receivedCommittedChunk = true;
    }
    batch.lastEvent = {
      ...event,
      managerId: event.managerId ?? batch.lastEvent.managerId ?? current.managerId,
      workstreamId: event.workstreamId ?? batch.lastEvent.workstreamId ?? current.workstreamId,
      workItemId: event.workItemId ?? batch.lastEvent.workItemId ?? current.workItemId,
    };
    batches.set(agentId, batch);
  }

  if (sequence === state.sequence) return state;

  let agents = state.agents;
  let graphChanged = false;
  let directorId = state.directorId;
  if (batches.size) {
    agents = { ...state.agents };
    for (const [agentId, batch] of batches) {
      const { agent, lastEvent } = batch;
      const p = lastEvent.payload;
      const role =
        lastEvent.role && lastEvent.role !== 'agent'
          ? lastEvent.role
          : agent.role === 'agent' && lastEvent.role
            ? lastEvent.role
            : agent.role;
      const previous = state.agents[agentId];
      if (
        !previous ||
        previous.role !== role ||
        previous.managerId !== (lastEvent.managerId ?? agent.managerId) ||
        previous.workstreamId !== (lastEvent.workstreamId ?? agent.workstreamId) ||
        previous.workItemId !== (lastEvent.workItemId ?? agent.workItemId)
      ) {
        graphChanged = true;
      }
      if (role === 'director') directorId = agentId;
      agents[agentId] = {
        ...agent,
        role,
        title: stringValue(p.name, p.title) ?? agent.title,
        model: stringValue(p.model) ?? agent.model,
        effort: stringValue(p.effort) ?? agent.effort,
        account: stringValue(p.account, p.account_id) ?? agent.account,
        provider: lastEvent.provider ?? stringValue(p.provider) ?? agent.provider,
        managerId: lastEvent.managerId ?? agent.managerId,
        workstreamId: lastEvent.workstreamId ?? agent.workstreamId,
        workItemId: lastEvent.workItemId ?? agent.workItemId,
        output: batch.outputChunks.length
          ? `${agent.output}${batch.outputChunks.join('')}`.slice(
              -WORKSPACE_LIMITS.outputCharacters,
            )
          : agent.output,
        thinking: batch.thinkingChunks.length
          ? `${agent.thinking}${batch.thinkingChunks.join('')}`.slice(
              -WORKSPACE_LIMITS.outputCharacters,
            )
          : agent.thinking,
        tokenCount: agent.tokenCount + batch.tokenCount,
        committedChunks: agent.committedChunks + batch.committedChunks,
        provisionalChunks: agent.provisionalChunks + batch.provisionalChunks,
        status: batch.receivedCommittedChunk ? 'running' : agent.status,
        executionState: batch.receivedCommittedChunk ? 'in_flight' : agent.executionState,
        updatedAt: lastEvent.timestamp,
      };
    }
  }

  const usedAccounts =
    accountCandidates.size === state.usedAccounts.length
      ? state.usedAccounts
      : [...accountCandidates];
  return {
    ...state,
    agents,
    graphRevision: state.graphRevision + (graphChanged ? 1 : 0),
    directorId,
    sequence,
    eventCount,
    usedAccounts,
    updatedAt,
  };
};

export function workspaceReducer(state: WorkspaceState, action: WorkspaceAction): WorkspaceState {
  if (action.type === 'reset') {
    return action.task ? stateFromTask(action.task) : initialWorkspaceState;
  }
  if (action.type === 'load-task') {
    const base = stateFromTask(action.task);
    const next = workspaceReducer(base, {
      type: 'events',
      events: action.task.events ?? [],
      taskId: action.task.id,
    });
    return {
      ...next,
      managers: next.managers.some((manager) => manager.id === 'manager:legacy')
        ? next.managers.filter((manager) => manager.id !== 'manager:legacy')
        : next.managers,
    };
  }
  if (action.type === 'connection') {
    return {
      ...state,
      connection: action.connection,
      connected: action.connection === 'live',
    };
  }
  if (action.type === 'focus-agent') {
    const agent = state.agents[action.agentId];
    if (!agent) return state;
    return {
      ...state,
      agents: { ...state.agents, [action.agentId]: { ...agent, unread: 0 } },
    };
  }
  if (action.type === 'agent-configured') {
    const agent = state.agents[action.agentId];
    if (!agent) return state;
    return {
      ...state,
      agents: {
        ...state.agents,
        [action.agentId]: {
          ...agent,
          model: action.model,
          effort: action.effort,
        },
      },
    };
  }
  if (action.type === 'events') {
    if (!action.events.length) return state;
    let next = state;
    let streaming: Record<string, unknown>[] = [];
    const flushStreaming = () => {
      if (!streaming.length) return;
      next = reduceStreamingBatch(next, streaming, action.taskId);
      streaming = [];
    };
    for (const event of action.events) {
      if (isStreamingEvent(event)) {
        streaming.push(event);
        continue;
      }
      flushStreaming();
      next = workspaceReducer(next, { type: 'event', event, taskId: action.taskId });
    }
    flushStreaming();
    return next;
  }
  if (action.type === 'event') {
    const event = normalizeEvent(action.event, action.taskId, state.sequence + 1);
    if (event.sequence <= state.sequence) return state;

    const sourceAgentId = stringValue(event.payload.source_agent_id, event.raw.source_agent_id);
    const targetAgentId = stringValue(event.payload.target_agent_id, event.raw.target_agent_id);
    const signals =
      event.type === 'agent_message' && sourceAgentId && targetAgentId
        ? [
            ...state.signals,
            {
              id: `${event.sequence}:${sourceAgentId}:${targetAgentId}`,
              sequence: event.sequence,
              sourceAgentId,
              targetAgentId,
              signalType:
                stringValue(event.payload.signal_type, event.raw.signal_type) ?? 'message',
              summary: stringValue(event.payload.summary, event.payload.message) ?? 'Agent message',
              workstreamId: event.workstreamId,
              workItemId: event.workItemId,
              timestamp: event.timestamp,
            },
          ].slice(-WORKSPACE_LIMITS.signals)
        : state.signals;

    const accountCandidates = [
      stringValue(
        event.payload.account,
        event.payload.account_id,
        event.payload.accountId,
        event.raw.account,
      ),
      stringValue(event.payload.from_account, event.payload.fromAccount, event.raw.from_account),
      stringValue(event.payload.to_account, event.payload.toAccount, event.raw.to_account),
    ].filter((value): value is string => Boolean(value && value.toLowerCase() !== 'redacted'));
    const accountHit =
      stringValue(event.payload.to_account, event.payload.toAccount, event.raw.to_account) ??
      accountCandidates[0];
    const usedAccounts = [...state.usedAccounts];
    for (const account of accountCandidates) {
      if (!usedAccounts.includes(account)) usedAccounts.push(account);
    }

    let fanout = state.fanout;
    if (event.type === 'hierarchy_fanout_planned') {
      fanout = {
        ...fanout,
        plannedManagers: numberValue(event.payload.manager_count) ?? fanout.plannedManagers,
        plannedCoders: numberValue(event.payload.coder_count) ?? fanout.plannedCoders,
        plannedTesters: numberValue(event.payload.tester_count) ?? fanout.plannedTesters,
        plannedChildren: numberValue(event.payload.child_agent_count) ?? fanout.plannedChildren,
        maxManagers: numberValue(event.payload.max_manager_count) ?? fanout.maxManagers,
        maxParallelManagers:
          numberValue(event.payload.max_parallel_managers) ?? fanout.maxParallelManagers,
        maxWorkersPerManager:
          numberValue(event.payload.max_coders_per_manager) != null
            ? (numberValue(event.payload.max_coders_per_manager) ?? 0) + 1
            : fanout.maxWorkersPerManager,
        maxParallelWorkersPerManager:
          numberValue(event.payload.max_parallel_workers_per_manager) ??
          fanout.maxParallelWorkersPerManager,
        maxParallelWorkers:
          numberValue(event.payload.max_parallel_workers) ?? fanout.maxParallelWorkers,
      };
    } else if (event.type === 'fanout_selected') {
      const selection = fanoutSelectionValue(event.payload, event.timestamp);
      const key = selection.workstreamId ?? selection.managerId ?? `manager:${event.sequence}`;
      fanout =
        selection.level === 'manager'
          ? { ...fanout, directorSelection: selection }
          : {
              ...fanout,
              managerSelections: { ...fanout.managerSelections, [key]: selection },
            };
    } else if (
      event.type === 'model_request_started' ||
      event.type === 'model_request_replayed' ||
      (event.type === 'status' &&
        stringValue(event.payload.stage) === 'calling_model' &&
        !stringValue(event.payload.logical_request_id))
    ) {
      const calledByRole = { ...fanout.calledByRole };
      const role = event.role ?? 'agent';
      calledByRole[role] = (calledByRole[role] ?? 0) + 1;
      const isChild = role === 'worker' || role === 'tester' || role === 'reviewer';
      const calledAgentIds =
        isChild && event.agentInstanceId && !fanout.calledAgentIds.includes(event.agentInstanceId)
          ? [...fanout.calledAgentIds, event.agentInstanceId]
          : fanout.calledAgentIds;
      fanout = {
        ...fanout,
        requestAttempts: fanout.requestAttempts + 1,
        replayedRequests:
          fanout.replayedRequests +
          (event.type === 'model_request_replayed' ||
          booleanValue(event.payload.replayed, event.payload.is_replay, event.payload.isReplay) ===
            true
            ? 1
            : 0),
        calledAgentIds,
        calledByRole,
      };
    } else if (event.type === 'model_request_completed') {
      const role = event.role ?? 'agent';
      const isChild = role === 'worker' || role === 'tester' || role === 'reviewer';
      const completedAgentIds =
        isChild &&
        event.agentInstanceId &&
        !fanout.completedAgentIds.includes(event.agentInstanceId)
          ? [...fanout.completedAgentIds, event.agentInstanceId]
          : fanout.completedAgentIds;
      fanout = {
        ...fanout,
        completedRequests: fanout.completedRequests + 1,
        completedAgentIds,
      };
    } else if (event.type === 'model_request_failed') {
      fanout = { ...fanout, failedRequests: fanout.failedRequests + 1 };
    } else if (event.type === 'model_request_aborted') {
      fanout = { ...fanout, abortedRequests: fanout.abortedRequests + 1 };
    } else if (event.type === 'account_switch') {
      fanout = { ...fanout, accountSwitches: fanout.accountSwitches + 1 };
    }

    let managers = state.managers.some((manager) => manager.id === 'manager:legacy')
      ? state.managers.filter((manager) => manager.id !== 'manager:legacy')
      : state.managers;
    if (event.type === 'plan_created' && Array.isArray(event.payload.workstreams)) {
      for (const rawStream of event.payload.workstreams) {
        const stream = asRecord(rawStream);
        const streamId = stringValue(stream.id);
        if (!streamId) continue;
        const existing = managers.find(
          (manager) => manager.id === streamId || manager.workstreamId === streamId,
        );
        const next: ManagerNode = {
          id: existing?.id ?? streamId,
          title: stringValue(stream.title) ?? existing?.title ?? titleFor('manager', streamId),
          status: existing?.status ?? 'queued',
          dependencies: stringList(stream.dependencies),
          agentId: existing?.agentId,
          workstreamId: streamId,
          contract: workContractValue(stream.contract, stream.work_contract) ?? existing?.contract,
          items: existing?.items ?? [],
        };
        managers = existing
          ? managers.map((manager) => (manager.id === existing.id ? next : manager))
          : [...managers, next];
      }
    }

    let reconciliation = state.reconciliation;
    let projectLease = state.projectLease;
    let effects = state.effects;
    let sandbox = state.sandbox;
    if (event.type === 'completion_reconciliation') {
      reconciliation = reconciliationValue(event.payload, event.timestamp);
    } else if (event.type === 'project_lease_acquired') {
      projectLease = {
        status: 'active',
        projectKey: stringValue(event.payload.project_key),
        fencingToken: numberValue(event.payload.fencing_token),
        isolationLevel: stringValue(event.payload.isolation_level),
        updatedAt: event.timestamp,
      };
    } else if (event.type === 'project_lease_lost') {
      projectLease = {
        ...projectLease,
        status: 'lost',
        error: stringValue(event.payload.error) ?? 'Project lease lost',
        updatedAt: event.timestamp,
      };
    } else if (event.type === 'effect_applied') {
      const id = stringValue(event.payload.effect_id) ?? `effect:${event.sequence}`;
      const effect = {
        id,
        kind: stringValue(event.payload.effect_kind),
        target: stringValue(event.payload.file_path),
        idempotencyKey: stringValue(event.payload.idempotency_key),
        beforeSha256: stringValue(event.payload.before_sha256),
        afterSha256: stringValue(event.payload.after_sha256),
        status: 'applied' as const,
        at: event.timestamp,
      };
      effects = [...effects.filter((candidate) => candidate.id !== id), effect].slice(-200);
    } else if (event.type === 'test_result') {
      sandbox = {
        requestedIsolation:
          stringValue(event.payload.requested_isolation, event.payload.requestedIsolation) ??
          sandbox?.requestedIsolation,
        actualIsolation:
          stringValue(event.payload.actual_isolation, event.payload.actualIsolation) ??
          sandbox?.actualIsolation,
        isolationDetails:
          stringValue(event.payload.isolation_details, event.payload.isolationDetails) ??
          sandbox?.isolationDetails,
        outputTruncated:
          booleanValue(event.payload.output_truncated, event.payload.outputTruncated) ??
          sandbox?.outputTruncated,
        updatedAt: event.timestamp,
      };
    }

    const agentId = event.agentInstanceId;

    // No real agent id → keep signals/accounts/sequence, do not spawn ghost agents.
    if (!agentId) {
      const graphChanged = signals !== state.signals || managers !== state.managers;
      return {
        ...state,
        graphRevision: state.graphRevision + (graphChanged ? 1 : 0),
        sequence: event.sequence,
        eventCount: state.eventCount + 1,
        signals,
        usedAccounts,
        fanout,
        reconciliation,
        projectLease,
        effects,
        sandbox,
        updatedAt: event.timestamp,
        managers,
      };
    }

    const current = state.agents[agentId] ?? emptyAgent({ ...event, agentInstanceId: agentId });
    const inferredManagerId =
      event.managerId ??
      current.managerId ??
      (event.workstreamId
        ? (managers.find(
            (manager) =>
              manager.workstreamId === event.workstreamId ||
              manager.id === event.workstreamId ||
              manager.agentId === event.workstreamId,
          )?.agentId ??
          managers.find(
            (manager) =>
              manager.workstreamId === event.workstreamId || manager.id === event.workstreamId,
          )?.id)
        : undefined);
    const enrichedEvent: EventEnvelope = {
      ...event,
      agentInstanceId: agentId,
      managerId: event.managerId ?? inferredManagerId,
      workstreamId: event.workstreamId ?? current.workstreamId,
      workItemId: event.workItemId ?? current.workItemId,
    };
    let agent = mergeAgentEvent(current, enrichedEvent);
    const owningManager = managers.find(
      (manager) =>
        manager.id === agent.managerId ||
        manager.agentId === agent.managerId ||
        manager.workstreamId === agent.workstreamId,
    );
    const owningItem = owningManager?.items.find(
      (item) => item.id === agent.workItemId || item.agentIds.includes(agent.id),
    );
    if (!agent.workContract) {
      const inheritedContract =
        owningItem?.contract ??
        (agent.role === 'manager' || agent.role === 'tester' || agent.role === 'reviewer'
          ? owningManager?.contract
          : undefined);
      if (inheritedContract) agent = { ...agent, workContract: inheritedContract };
    }
    const accountHits = [agent.account, accountHit].filter((value): value is string =>
      Boolean(value && value.toLowerCase() !== 'redacted'),
    );
    const nextAccounts = [...usedAccounts];
    for (const account of accountHits) {
      if (!nextAccounts.includes(account)) nextAccounts.push(account);
    }
    const streaming = event.type === 'token' || event.type === 'thinking';
    const nextManagers = streaming ? managers : ensureDag(managers, enrichedEvent, agent);
    return {
      ...state,
      agents: { ...state.agents, [agentId]: agent },
      managers: nextManagers,
      graphRevision:
        state.graphRevision +
        (!streaming || signals !== state.signals || nextManagers !== state.managers ? 1 : 0),
      directorId: agent.role === 'director' ? agent.id : state.directorId,
      sequence: event.sequence,
      eventCount: state.eventCount + 1,
      signals,
      usedAccounts: nextAccounts,
      fanout,
      reconciliation,
      projectLease,
      effects,
      sandbox,
      updatedAt: event.timestamp,
    };
  }
  return state;
}

export type { WorkspaceAction };
