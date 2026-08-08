import type {
  ActivityEntry,
  AgentInstance,
  AgentRole,
  AgentStatus,
  EventEnvelope,
  ManagerNode,
  TaskSummary,
  TestResult,
  WorkspaceState,
} from '../types';

export const initialWorkspaceState: WorkspaceState = {
  agents: {},
  managers: [],
  sequence: 0,
  connected: false,
  connection: 'idle',
  eventCount: 0,
  signals: [],
  usedAccounts: [],
};

type WorkspaceAction =
  | { type: 'load-task'; task: TaskSummary }
  | { type: 'reset'; task?: TaskSummary }
  | { type: 'connection'; connection: WorkspaceState['connection'] }
  | { type: 'event'; event: Record<string, unknown>; taskId: string }
  | { type: 'focus-agent'; agentId: string }
  | { type: 'agent-configured'; agentId: string; model: string; effort: string };

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
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
};

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
  if (['error', 'failed', 'rejected', 'revise'].includes(status)) return 'failed';
  if (['queued', 'pending', 'blocked'].includes(status)) return 'queued';
  if (['waiting', 'waiting_input'].includes(status)) return 'waiting';
  if (['stopped', 'cancelled', 'canceled'].includes(status)) return 'stopped';
  if (['running', 'planning', 'coding', 'reviewing', 'active'].includes(status)) return 'running';
  return 'idle';
};

export function normalizeEvent(
  raw: Record<string, unknown>,
  taskId: string,
  fallbackSequence = 1,
): EventEnvelope {
  const payload = Object.keys(asRecord(raw.payload)).length ? asRecord(raw.payload) : raw;
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
    timestamp:
      stringValue(raw.timestamp, raw.at, payload.timestamp) ?? new Date().toISOString(),
    taskId: stringValue(raw.task_id, payload.task_id) ?? taskId,
    sessionId: stringValue(raw.session_id, payload.session_id),
    workstreamId: stringValue(raw.workstream_id, payload.workstream_id),
    workItemId: stringValue(raw.work_item_id, payload.work_item_id, payload.ticket_id),
    managerId,
    agentInstanceId,
    role,
    payload,
    raw,
  };
}

const titleFor = (role: AgentRole, id: string): string => {
  const suffix = id.split(/[:/_-]/).filter(Boolean).at(-1)?.slice(0, 7);
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
  };
};

const activityTone = (type: string, payload: Record<string, unknown>): ActivityEntry['tone'] => {
  if (type === 'error' || String(payload.status).toLowerCase() === 'failed') return 'error';
  if (type.includes('retry') || type.includes('switch')) return 'warning';
  if (type.includes('finish') || type.includes('approved') || payload.accepted === true) {
    return 'success';
  }
  if (type === 'token' || type === 'thinking') return 'neutral';
  return 'info';
};

const eventMessage = (event: EventEnvelope): string => {
  const p = event.payload;
  const files = Array.isArray(p.files_needed) ? p.files_needed.join(', ') : undefined;
  return (
    stringValue(
      p.message,
      p.detail,
      p.data,
      p.action,
      p.reason,
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
    activity: isStreaming ? agent.activity : [...agent.activity, entry].slice(-200),
    unread: agent.unread + (isStreaming ? 0 : 1),
    updatedAt: event.timestamp,
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
    agent = { ...agent, phase, status: statusValue(phase) || agent.status };
  }
  if (event.type === 'agent_started' || event.type === 'agent_progress' || event.type === 'status') {
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
      startedAt: agent.startedAt ?? event.timestamp,
    };
  }
  if (event.type === 'token' && chunk) {
    agent = {
      ...agent,
      output: `${agent.output}${chunk}`.slice(-60_000),
      tokenCount: agent.tokenCount + Math.max(1, Math.ceil(chunk.length / 4)),
      status: 'running',
    };
  }
  if (event.type === 'thinking' && chunk) {
    agent = {
      ...agent,
      thinking: `${agent.thinking}${chunk}`.slice(-60_000),
      status: 'running',
    };
  }
  if (event.type === 'agent_action') {
    agent = { ...agent, action: eventMessage(event), status: 'running' };
  }
  if (files.length) {
    agent = { ...agent, contextFiles: [...new Set([...agent.contextFiles, ...files])] };
  }
  const diff = stringValue(p.diff, p.patch, p.patch_text);
  if (diff) agent = { ...agent, diff };

  if (event.type === 'test_result' || event.type === 'execution_result') {
    const accepted = p.accepted === true || statusValue(p.status) === 'passed';
    const test: TestResult = {
      id: stringValue(p.test_run_id, p.attempt_id) ?? `${event.sequence}`,
      name: stringValue(p.name, p.command, p.file_path) ?? 'Machine gate',
      status: accepted ? 'passed' : p.accepted === false ? 'failed' : 'unknown',
      detail: stringValue(p.detail, p.execution_result),
      durationMs: numberValue(p.duration_ms),
    };
    agent = { ...agent, tests: [...agent.tests, test].slice(-50) };
  }
  if (event.type === 'review_result') {
    agent = {
      ...agent,
      status: String(p.verdict).toLowerCase() === 'approved' ? 'passed' : 'failed',
      action: eventMessage(event),
    };
  }
  if (event.type.includes('finish') || event.type === 'done' || event.type === 'agent_completed') {
    agent = { ...agent, status: 'passed', phase: 'complete' };
  }
  if (event.type === 'error' || event.type === 'agent_failed') {
    agent = { ...agent, status: 'failed', phase: 'error', error: eventMessage(event) };
  }
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
    goal: stringValue(p.goal, p.objective) ?? agent.goal,
    prompt: stringValue(p.prompt) ?? agent.prompt,
    managerId: event.managerId ?? agent.managerId,
    workstreamId: event.workstreamId ?? agent.workstreamId,
    workItemId: event.workItemId ?? agent.workItemId,
    durationMs: numberValue(p.duration_ms) ?? agent.durationMs,
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
  const clean = managers.filter((manager) => manager.id !== 'manager:legacy');
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
    title: stringValue(event.payload.workstream_title, event.payload.manager_title) ??
      titleFor('manager', resolvedId),
    status: 'queued',
    dependencies: Array.isArray(event.payload.dependencies)
      ? event.payload.dependencies.filter((item): item is string => typeof item === 'string')
      : [],
    items: [],
  };
  let next: ManagerNode = {
    ...manager,
    id: resolvedId,
    status: agent.role === 'manager' ? agent.status : manager.status === 'idle' ? 'running' : manager.status,
    agentId: agent.role === 'manager' ? agent.id : manager.agentId,
    workstreamId: event.workstreamId ?? agent.workstreamId ?? manager.workstreamId,
  };

  if (event.type === 'manager_plan_created' && Array.isArray(event.payload.work_items)) {
    next = {
      ...next,
      status: 'queued',
      agentId: agent.role === 'manager' ? agent.id : next.agentId ?? event.managerId,
      workstreamId: event.workstreamId ?? agent.workstreamId ?? next.workstreamId,
      title:
        stringValue(event.payload.workstream_title, event.payload.manager_title) ?? next.title,
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
      title: stringValue(event.payload.work_item_title, event.payload.ticket, event.payload.file_path) ??
        item?.title ??
        `Work item ${next.items.length + 1}`,
      status: agent.status,
      dependencies: Array.isArray(event.payload.dependencies)
        ? event.payload.dependencies.filter((value): value is string => typeof value === 'string')
        : item?.dependencies ?? [],
      agentIds,
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

const taskAgentSeed = (_task: TaskSummary): Record<string, AgentInstance> => ({});

export function workspaceReducer(
  state: WorkspaceState,
  action: WorkspaceAction,
): WorkspaceState {
  if (action.type === 'reset') {
    return action.task
      ? { ...initialWorkspaceState, task: action.task, agents: {} }
      : initialWorkspaceState;
  }
  if (action.type === 'load-task') {
    let next: WorkspaceState = {
      ...initialWorkspaceState,
      task: action.task,
      agents: {},
      updatedAt: action.task.updated_at,
    };
    for (const raw of action.task.events ?? []) {
      next = workspaceReducer(next, { type: 'event', event: raw, taskId: action.task.id });
    }
    return {
      ...next,
      managers: next.managers.filter((manager) => manager.id !== 'manager:legacy'),
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
  if (action.type === 'event') {
    const event = normalizeEvent(action.event, action.taskId, state.sequence + 1);
    if (event.sequence <= state.sequence) return state;

    const sourceAgentId = stringValue(
      event.payload.source_agent_id,
      event.raw.source_agent_id,
    );
    const targetAgentId = stringValue(
      event.payload.target_agent_id,
      event.raw.target_agent_id,
    );
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
              summary:
                stringValue(event.payload.summary, event.payload.message) ?? 'Agent message',
              workstreamId: event.workstreamId,
              workItemId: event.workItemId,
              timestamp: event.timestamp,
            },
          ].slice(-100)
        : state.signals;

    const accountHit = stringValue(
      event.payload.account,
      event.payload.account_id,
      event.payload.from_account,
      event.payload.to_account,
      event.raw.account,
      event.raw.from_account,
      event.raw.to_account,
    );
    const usedAccounts =
      accountHit && accountHit.toLowerCase() !== 'redacted' && !state.usedAccounts.includes(accountHit)
        ? [...state.usedAccounts, accountHit]
        : state.usedAccounts;

    const agentId = event.agentInstanceId;

    // No real agent id → keep signals/accounts/sequence, do not spawn ghost agents.
    if (!agentId) {
      return {
        ...state,
        sequence: event.sequence,
        eventCount: state.eventCount + 1,
        signals,
        usedAccounts,
        updatedAt: event.timestamp,
        managers: state.managers.filter((manager) => manager.id !== 'manager:legacy'),
      };
    }

    const current = state.agents[agentId] ?? emptyAgent({ ...event, agentInstanceId: agentId });
    const inferredManagerId =
      event.managerId ??
      current.managerId ??
      (event.workstreamId
        ? state.managers.find(
            (manager) =>
              manager.workstreamId === event.workstreamId ||
              manager.id === event.workstreamId ||
              manager.agentId === event.workstreamId,
          )?.agentId ??
          state.managers.find(
            (manager) =>
              manager.workstreamId === event.workstreamId ||
              manager.id === event.workstreamId,
          )?.id
        : undefined);
    const enrichedEvent: EventEnvelope = {
      ...event,
      agentInstanceId: agentId,
      managerId: event.managerId ?? inferredManagerId,
      workstreamId: event.workstreamId ?? current.workstreamId,
      workItemId: event.workItemId ?? current.workItemId,
    };
    const agent = mergeAgentEvent(current, enrichedEvent);
    const accountHits = [
      agent.account,
      accountHit,
    ].filter((value): value is string => Boolean(value && value.toLowerCase() !== 'redacted'));
    const nextAccounts = [...usedAccounts];
    for (const account of accountHits) {
      if (!nextAccounts.includes(account)) nextAccounts.push(account);
    }
    return {
      ...state,
      agents: { ...state.agents, [agentId]: agent },
      managers: ensureDag(state.managers, enrichedEvent, agent),
      directorId: agent.role === 'director' ? agent.id : state.directorId,
      sequence: event.sequence,
      eventCount: state.eventCount + 1,
      signals,
      usedAccounts: nextAccounts,
      updatedAt: event.timestamp,
    };
  }
  return state;
}

export type { WorkspaceAction };
