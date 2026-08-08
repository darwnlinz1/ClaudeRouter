export type AgentRole =
  | 'director'
  | 'manager'
  | 'worker'
  | 'tester'
  | 'reviewer'
  | 'supervisor'
  | 'agent';

export type AgentStatus =
  | 'idle'
  | 'queued'
  | 'running'
  | 'waiting'
  | 'passed'
  | 'failed'
  | 'stopped';

export interface ChangedFile {
  additions?: number;
  deletions?: number;
}

export interface TaskSummary {
  id: string;
  name: string;
  mode?: string;
  project_mode?: string;
  prompt?: string;
  root?: string;
  files?: string[];
  status: string;
  phase?: string;
  current_agent?: string | null;
  turn_count?: number;
  changed_files?: Record<string, ChangedFile>;
  settings?: Record<string, unknown>;
  events?: Record<string, unknown>[];
  created_at?: string;
  updated_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
  last_error?: string | null;
  last_worker_feedback?: string;
  last_reviewer_feedback?: string;
  last_review_verdict?: string | null;
  last_execution_result?: string | null;
}

export interface EventEnvelope {
  version: number;
  sequence: number;
  type: string;
  timestamp: string;
  taskId: string;
  sessionId?: string;
  workstreamId?: string;
  workItemId?: string;
  managerId?: string;
  agentInstanceId?: string;
  role?: AgentRole;
  payload: Record<string, unknown>;
  raw: Record<string, unknown>;
}

export interface ActivityEntry {
  id: string;
  sequence: number;
  type: string;
  at: string;
  message: string;
  tone: 'neutral' | 'info' | 'success' | 'warning' | 'error';
}

export interface TestResult {
  id: string;
  name: string;
  status: 'running' | 'passed' | 'failed' | 'unknown';
  detail?: string;
  durationMs?: number;
}

export interface AgentInstance {
  id: string;
  role: AgentRole;
  title: string;
  managerId?: string;
  workstreamId?: string;
  workItemId?: string;
  status: AgentStatus;
  phase: string;
  model?: string;
  effort?: string;
  account?: string;
  startedAt?: string;
  updatedAt?: string;
  durationMs?: number;
  tokenCount: number;
  goal: string;
  prompt: string;
  action: string;
  output: string;
  thinking: string;
  contextFiles: string[];
  diff: string;
  tests: TestResult[];
  activity: ActivityEntry[];
  unread: number;
  error?: string;
}

export interface AgentProfile {
  model: string;
  effort: string;
}

export type ConfigurableRole = 'director' | 'manager' | 'worker' | 'tester';

export interface AgentSignal {
  id: string;
  sequence: number;
  sourceAgentId: string;
  targetAgentId: string;
  signalType: string;
  summary: string;
  workstreamId?: string;
  workItemId?: string;
  timestamp: string;
}

export interface WorkItemNode {
  id: string;
  managerId: string;
  title: string;
  status: AgentStatus;
  dependencies: string[];
  agentIds: string[];
}

export interface ManagerNode {
  id: string;
  title: string;
  status: AgentStatus;
  dependencies: string[];
  agentId?: string;
  workstreamId?: string;
  items: WorkItemNode[];
}

export interface WorkspaceState {
  task?: TaskSummary;
  agents: Record<string, AgentInstance>;
  managers: ManagerNode[];
  directorId?: string;
  sequence: number;
  connected: boolean;
  connection: 'idle' | 'connecting' | 'live' | 'reconnecting' | 'offline';
  eventCount: number;
  signals: AgentSignal[];
  usedAccounts: string[];
  updatedAt?: string;
}

export interface WorkspaceSettings {
  maxParallelManagers: number;
  maxWorkersPerManager: number;
  maxParallelWorkers: number;
  mockWhenUnavailable: boolean;
  roleProfiles: Record<ConfigurableRole, AgentProfile>;
}
