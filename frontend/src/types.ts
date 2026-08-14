export type {
  KnownOrchestratorEvent,
  LegacyOrchestratorEvent,
  OrchestratorEvent,
} from './generated/orchestrator-events.generated';

export type AgentRole =
  'director' | 'manager' | 'worker' | 'tester' | 'reviewer' | 'supervisor' | 'agent';

export type AgentStatus =
  'idle' | 'queued' | 'running' | 'waiting' | 'passed' | 'failed' | 'stopped';

export type ExecutionState =
  | 'planned'
  | 'queued'
  | 'waiting_dependency'
  | 'waiting_scope'
  | 'in_flight'
  | 'testing'
  | 'blocked'
  | 'skipped'
  | 'preflight_failed'
  | 'completed'
  | 'failed'
  | 'aborted'
  | 'idle';

export interface ChangedFile {
  additions?: number;
  deletions?: number;
}

export type RiskLevel = 'low' | 'medium' | 'high' | 'critical';

export interface WorkContract {
  id: string;
  version: number;
  inputArtifacts: string[];
  expectedOutputs: string[];
  readScopes: string[];
  writeScopes: string[];
  acceptanceCriteria: string[];
  testRequirements: string[];
  evidenceRequirements: string[];
  consumers: string[];
  riskLevel: RiskLevel;
  priority: number;
  approvalPolicy?: 'never' | 'risk_based' | 'always';
}

export interface ReconciliationPartition {
  planned: number;
  completed: number;
  skipped: number;
  blocked: number;
  failed: number;
  preflightFailed: number;
  terminal: number;
  balanced: boolean;
  ids: Record<string, string[]>;
  invalidIds: string[];
}

export interface CompletionReconciliation {
  balanced: boolean;
  errors: string[];
  workstreams: ReconciliationPartition;
  workItems: ReconciliationPartition;
  agents: ReconciliationPartition;
  calls: ReconciliationPartition;
  updatedAt?: string;
}

export interface ProjectLease {
  status: 'unknown' | 'active' | 'lost';
  projectKey?: string;
  fencingToken?: number;
  isolationLevel?: string;
  error?: string;
  updatedAt?: string;
}

export interface EffectReceipt {
  id: string;
  kind?: string;
  target?: string;
  idempotencyKey?: string;
  beforeSha256?: string;
  afterSha256?: string;
  status: 'applied';
  at: string;
}

export interface SandboxTrust {
  requestedIsolation?: string;
  actualIsolation?: string;
  isolationDetails?: string;
  outputTruncated?: boolean;
  updatedAt?: string;
}

export interface AccountHealth {
  accountId: string;
  provider: string;
  state: string;
  reason?: string;
  cooldownUntil?: number;
  cooldownActive: boolean;
  activeLeases: number;
  leaseExpiresAt?: number;
}

export interface ApprovalItem {
  approvalId: string;
  taskId: string;
  workstreamId?: string;
  kind: string;
  target: string;
  reason: string;
  status: 'pending' | 'approved' | 'rejected';
  createdAt: string;
  decidedAt?: string;
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
  hierarchy?: Record<string, unknown>;
  artifact?: {
    status?: string;
    files?: Array<Record<string, unknown>>;
    zip_path?: string | null;
    retention?: { pinned?: boolean };
  } | null;
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
  callId?: string;
  attemptId?: string;
  attempt?: number;
  requestRevision?: number;
  provider?: string;
  account?: string;
  provisional?: boolean;
  committed?: boolean;
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
  requestedIsolation?: string;
  actualIsolation?: string;
  isolationDetails?: string;
  outputTruncated?: boolean;
}

export interface CallAttempt {
  id: string;
  logicalRequestId?: string;
  attempt: number;
  requestRevision?: number;
  provider?: string;
  account?: string;
  status: 'running' | 'completed' | 'failed' | 'aborted';
  replayed: boolean;
  startedAt?: string;
  finishedAt?: string;
  error?: string;
  errorType?: string;
}

export interface AgentFailure {
  id: string;
  type: string;
  message: string;
  at: string;
  attemptId?: string;
  provider?: string;
}

export interface AgentInstance {
  id: string;
  role: AgentRole;
  title: string;
  managerId?: string;
  workstreamId?: string;
  workItemId?: string;
  status: AgentStatus;
  executionState?: ExecutionState;
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
  requestAttempts: number;
  replayCount: number;
  accountSwitchCount: number;
  logicalRequestId?: string;
  requestFingerprint?: string;
  previousAccount?: string;
  provider?: string;
  currentAttemptId?: string;
  requestRevision?: number;
  committedChunks: number;
  provisionalChunks: number;
  callAttempts: CallAttempt[];
  failures: AgentFailure[];
  workContract?: WorkContract;
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
  contract?: WorkContract;
}

export interface ManagerNode {
  id: string;
  title: string;
  status: AgentStatus;
  dependencies: string[];
  agentId?: string;
  workstreamId?: string;
  contract?: WorkContract;
  items: WorkItemNode[];
}

export interface FanoutSelection {
  level: 'manager' | 'worker' | string;
  maximum: number;
  selected: number;
  unused: number;
  reason?: string;
  managerId?: string;
  workstreamId?: string;
  at?: string;
}

export interface FanoutStats {
  plannedManagers: number;
  plannedCoders: number;
  plannedTesters: number;
  plannedChildren: number;
  maxManagers: number;
  maxParallelManagers: number;
  maxWorkersPerManager: number;
  maxParallelWorkersPerManager: number;
  maxParallelWorkers: number;
  requestAttempts: number;
  completedRequests: number;
  replayedRequests: number;
  accountSwitches: number;
  failedRequests: number;
  abortedRequests: number;
  calledAgentIds: string[];
  completedAgentIds: string[];
  calledByRole: Partial<Record<AgentRole, number>>;
  directorSelection?: FanoutSelection;
  managerSelections: Record<string, FanoutSelection>;
}

export interface WorkspaceState {
  task?: TaskSummary;
  agents: Record<string, AgentInstance>;
  managers: ManagerNode[];
  directorId?: string;
  graphRevision: number;
  sequence: number;
  connected: boolean;
  connection: 'idle' | 'connecting' | 'live' | 'reconnecting' | 'offline';
  eventCount: number;
  signals: AgentSignal[];
  usedAccounts: string[];
  fanout: FanoutStats;
  reconciliation?: CompletionReconciliation;
  projectLease?: ProjectLease;
  effects: EffectReceipt[];
  sandbox?: SandboxTrust;
  updatedAt?: string;
}

export interface WorkspaceSettings {
  maxManagers: number;
  maxParallelManagers: number;
  maxWorkersPerManager: number;
  maxParallelWorkersPerManager?: number;
  maxParallelWorkers: number;
  maxModelCalls: number;
  maxWallClockSeconds: number;
  maxEstimatedInputTokens: number;
  mockWhenUnavailable: boolean;
  roleProfiles: Record<ConfigurableRole, AgentProfile>;
}
