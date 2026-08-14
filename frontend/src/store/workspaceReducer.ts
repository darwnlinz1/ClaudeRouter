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
  ManagerReportState,
  ManagerNode,
  ManagerTerminalReport,
  ManagerWorkItemCounts,
  ProvisionalThinkingChunk,
  ReconciliationPartition,
  TaskSummary,
  TerminalOutcome,
  TerminalOutcomeCounts,
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
  managerReports: {
    reports: {},
    roster: {
      entries: [],
      expectedManagerIds: [],
      reportedManagerIds: [],
      pendingManagerIds: [],
    },
    counts: {
      expected: 0,
      reported: 0,
      pending: 0,
      completed: 0,
      partial: 0,
      abandoned: 0,
      skipped: 0,
      failed: 0,
    },
    barrierSatisfied: false,
  },
  effects: [],
};

type WorkspaceAction =
  | { type: 'load-task'; task: TaskSummary }
  | { type: 'apply-snapshot'; task: TaskSummary; timeline?: WorkspaceState['timeline'] }
  | { type: 'reset'; task?: TaskSummary }
  | { type: 'connection'; connection: WorkspaceState['connection'] }
  | { type: 'event'; event: Record<string, unknown>; taskId: string }
  | { type: 'events'; events: Record<string, unknown>[]; taskId: string }
  | { type: 'focus-agent'; agentId: string }
  | { type: 'clear-provisional-thinking' }
  | { type: 'agent-configured'; agentId: string; model: string; effort: string };

export const WORKSPACE_LIMITS = {
  activity: 200,
  attempts: 50,
  failures: 30,
  outputCharacters: 60_000,
  provisionalThinkingChunks: 1000,
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

const REDACTED_PLACEHOLDER = /^\[?redacted\]?$/i;

const isUsableAccount = (value: string | undefined): value is string =>
  Boolean(value && !REDACTED_PLACEHOLDER.test(value.trim()));

/* A redaction placeholder is not an identity. Treating it as one merges every
   agent that happens to share it into a single node and leaves their children
   pointing at a parent that does not exist. */
const identityValue = (...values: unknown[]): string | undefined => {
  const value = stringValue(...values);
  return value && !REDACTED_PLACEHOLDER.test(value.trim()) ? value : undefined;
};

/* An account counts as used only once a request actually went out on it. A
   "calling_model" status is emitted before that and would inflate the number,
   so only the request events count. */
const accountUsedByAttempt = (event: EventEnvelope): string | undefined => {
  const isAttempt =
    event.type === 'model_request_started' || event.type === 'model_request_replayed';
  if (!isAttempt) return undefined;
  return stringValue(
    event.payload.account_ref,
    event.payload.accountRef,
    event.raw.account_ref,
    event.raw.accountRef,
    event.payload.account,
    event.payload.account_id,
    event.payload.accountId,
    event.raw.account,
  );
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

const terminalOutcomeValue = (...values: unknown[]): TerminalOutcome | undefined => {
  const value = values.find((candidate) => typeof candidate === 'string' && candidate.trim());
  const outcome = String(value ?? '')
    .trim()
    .toLowerCase()
    .replaceAll('-', '_');
  if (['complete', 'completed', 'approved', 'success', 'passed', 'done'].includes(outcome)) {
    return 'completed';
  }
  if (
    [
      'partial',
      'partially_completed',
      'completed_partial',
      'completed_with_issues',
      'partial_acceptance',
      'accept_partial',
      'approved_partial',
    ].includes(outcome)
  ) {
    return 'partial';
  }
  if (['abandoned', 'aborted', 'cancelled', 'canceled', 'stopped'].includes(outcome)) {
    return 'abandoned';
  }
  if (['skipped', 'not_needed', 'not_applicable'].includes(outcome)) return 'skipped';
  if (['failed', 'error', 'rejected', 'preflight_failed'].includes(outcome)) return 'failed';
  return undefined;
};

const emptyOutcomeCounts = (): TerminalOutcomeCounts => ({
  completed: 0,
  partial: 0,
  abandoned: 0,
  skipped: 0,
  failed: 0,
});

const outcomeCountsValue = (value: unknown): TerminalOutcomeCounts => {
  const counts = asRecord(value);
  return {
    completed:
      numberValue(
        counts.completed,
        counts.complete,
        counts.completed_count,
        counts.completed_managers,
      ) ?? 0,
    partial: numberValue(counts.partial, counts.partial_count, counts.partially_completed) ?? 0,
    abandoned:
      numberValue(counts.abandoned, counts.abandoned_count, counts.abandoned_managers) ?? 0,
    skipped: numberValue(counts.skipped, counts.skipped_count, counts.skipped_managers) ?? 0,
    failed: numberValue(counts.failed, counts.failed_count, counts.failed_managers) ?? 0,
  };
};

const managerWorkItemCountsValue = (value: unknown): ManagerWorkItemCounts => {
  const source = asRecord(value);
  const nested = asRecord(
    source.counts ?? source.work_item_counts ?? source.workItemCounts ?? source.outcomes,
  );
  const counts = { ...source, ...nested };
  const completedIds = stringList(counts.completed_item_ids ?? counts.completedItemIds);
  const partialIds = stringList(counts.partial_item_ids ?? counts.partialItemIds);
  const abandonedIds = stringList(counts.abandoned_item_ids ?? counts.abandonedItemIds);
  const skippedIds = stringList(counts.skipped_item_ids ?? counts.skippedItemIds);
  const failedIds = stringList(counts.failed_item_ids ?? counts.failedItemIds);
  const outcomes = {
    completed:
      numberValue(
        counts.completed,
        counts.complete,
        counts.completed_count,
        counts.completed_managers,
      ) ?? completedIds.length,
    partial:
      numberValue(counts.partial, counts.partial_count, counts.partially_completed) ??
      partialIds.length,
    abandoned:
      numberValue(counts.abandoned, counts.abandoned_count, counts.abandoned_managers) ??
      abandonedIds.length,
    skipped:
      numberValue(counts.skipped, counts.skipped_count, counts.skipped_managers) ??
      skippedIds.length,
    failed:
      numberValue(counts.failed, counts.failed_count, counts.failed_managers) ?? failedIds.length,
  };
  const terminal =
    numberValue(
      counts.terminal,
      counts.terminal_count,
      counts.terminal_work_items,
      counts.reported,
    ) ??
    outcomes.completed + outcomes.partial + outcomes.abandoned + outcomes.skipped + outcomes.failed;
  return {
    planned:
      numberValue(
        counts.planned,
        counts.planned_count,
        counts.planned_work_items,
        counts.total,
        counts.total_work_items,
      ) ?? terminal,
    terminal,
    ...outcomes,
  };
};

const managerWorkItemIdsValue = (value: unknown): ManagerTerminalReport['workItemIds'] => {
  const source = asRecord(value);
  const ids = asRecord(
    source.work_item_ids ?? source.workItemIds ?? source.ids ?? source.outcome_ids,
  );
  const read = (outcome: TerminalOutcome): string[] =>
    stringList(
      ids[outcome] ??
        source[`${outcome}_item_ids`] ??
        source[`${outcome}ItemIds`] ??
        source[`${outcome}_work_item_ids`] ??
        source[`${outcome}WorkItemIds`],
    );
  return {
    completed: read('completed'),
    partial: read('partial'),
    abandoned: read('abandoned'),
    skipped: read('skipped'),
    failed: read('failed'),
  };
};

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
  partial: 0,
  abandoned: 0,
  skipped: 0,
  blocked: 0,
  failed: 0,
  preflightFailed: 0,
  terminal: 0,
  balanced: true,
  covered: true,
  successful: true,
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
    partial: numberValue(partition.partial) ?? 0,
    abandoned: numberValue(partition.abandoned) ?? 0,
    skipped: numberValue(partition.skipped) ?? 0,
    blocked: numberValue(partition.blocked) ?? 0,
    failed: numberValue(partition.failed) ?? 0,
    preflightFailed: numberValue(partition.preflight_failed, partition.preflightFailed) ?? 0,
    terminal: numberValue(partition.terminal) ?? 0,
    balanced: booleanValue(partition.balanced) ?? false,
    covered: booleanValue(partition.covered, partition.balanced) ?? false,
    successful: booleanValue(partition.successful) ?? false,
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
  const reportBarrier = asRecord(reconciliation.report_barrier ?? reconciliation.reportBarrier);
  const directorFinalReview = asRecord(
    reconciliation.director_final_review ?? reconciliation.directorFinalReview,
  );
  return {
    balanced: booleanValue(reconciliation.balanced) ?? false,
    covered: booleanValue(reconciliation.covered, reconciliation.balanced) ?? false,
    successful: booleanValue(reconciliation.successful),
    errors: stringList(reconciliation.errors),
    workstreams: partitionValue(reconciliation.workstreams),
    workItems: partitionValue(reconciliation.work_items ?? reconciliation.workItems),
    agents: partitionValue(reconciliation.agents),
    agentCalls:
      reconciliation.agent_calls || reconciliation.agentCalls
        ? partitionValue(reconciliation.agent_calls ?? reconciliation.agentCalls)
        : undefined,
    agentCallPurposes: asRecord(
      reconciliation.agent_call_purposes ?? reconciliation.agentCallPurposes,
    ) as Record<string, string[]>,
    calls: partitionValue(reconciliation.calls),
    managerReports:
      reconciliation.manager_reports || reconciliation.managerReports
        ? partitionValue(reconciliation.manager_reports ?? reconciliation.managerReports)
        : undefined,
    reportBarrier: Object.keys(reportBarrier).length
      ? {
          expectedCount: numberValue(reportBarrier.expected_count, reportBarrier.expected) ?? 0,
          reportedCount: numberValue(reportBarrier.reported_count, reportBarrier.reported) ?? 0,
          satisfied: booleanValue(reportBarrier.satisfied) ?? false,
          duplicates: stringList(reportBarrier.duplicates),
          unexpectedManagerIds: stringList(
            reportBarrier.unexpected_manager_ids ?? reportBarrier.unexpectedManagerIds,
          ),
        }
      : undefined,
    directorFinalReview: Object.keys(directorFinalReview).length
      ? {
          count: numberValue(directorFinalReview.count) ?? 0,
          exactlyOnce:
            booleanValue(directorFinalReview.exactly_once, directorFinalReview.exactlyOnce) ??
            false,
          enforced: booleanValue(directorFinalReview.enforced) ?? false,
        }
      : undefined,
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

const canonicalManagerId = (
  managers: ManagerNode[],
  managerId?: string,
  workstreamId?: string,
): string | undefined => {
  const manager = managers.find(
    (candidate) =>
      candidate.id === managerId ||
      candidate.agentId === managerId ||
      candidate.workstreamId === managerId ||
      candidate.id === workstreamId ||
      candidate.workstreamId === workstreamId,
  );
  return manager?.agentId ?? manager?.id ?? managerId ?? workstreamId;
};

const managerTerminalReportValue = (
  event: EventEnvelope,
  managers: ManagerNode[],
): ManagerTerminalReport | undefined => {
  const p = event.payload;
  const workstreamId = stringValue(
    event.workstreamId,
    p.workstream_id,
    p.workstreamId,
    p.stream_id,
  );
  const managerId = canonicalManagerId(
    managers,
    identityValue(
      event.managerId,
      event.role === 'manager' ? event.agentInstanceId : undefined,
      p.manager_agent_id,
      p.managerAgentId,
      p.manager_id,
      p.managerId,
    ),
    workstreamId,
  );
  if (!managerId) return undefined;

  const counts = managerWorkItemCountsValue(p);
  const inferredOutcome: TerminalOutcome =
    counts.abandoned > 0 && counts.completed === 0 && counts.partial === 0
      ? 'abandoned'
      : counts.failed > 0 || counts.partial > 0 || counts.abandoned > 0
        ? 'partial'
        : counts.skipped > 0 && counts.completed === 0
          ? 'skipped'
          : 'completed';
  const reasons = stringList(p.reasons);
  return {
    managerId,
    workstreamId,
    outcome:
      terminalOutcomeValue(
        p.outcome,
        p.terminal_outcome,
        p.terminalOutcome,
        p.result,
        p.status,
        p.verdict,
      ) ?? inferredOutcome,
    summary:
      stringValue(p.summary, p.message, p.detail, p.reason) || reasons.join(' · ') || undefined,
    counts,
    workItemIds: managerWorkItemIdsValue(p),
    artifacts: stringList(p.artifacts),
    reasons,
    logRefs: stringList(p.log_refs ?? p.logRefs),
    synthesized: booleanValue(p.synthesized) ?? false,
    executionEpoch: stringValue(p.execution_epoch, p.executionEpoch),
    reportedAt: event.timestamp,
    sequence: event.sequence,
  };
};

const reportOutcomeCounts = (
  reports: Record<string, ManagerTerminalReport>,
): TerminalOutcomeCounts => {
  const counts = emptyOutcomeCounts();
  for (const report of Object.values(reports)) counts[report.outcome] += 1;
  return counts;
};

const managerIdsFrom = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => {
      if (typeof entry === 'string') return entry;
      const record = asRecord(entry);
      return identityValue(
        record.manager_id,
        record.managerId,
        record.manager_agent_id,
        record.managerAgentId,
        record.id,
      );
    })
    .filter((entry): entry is string => Boolean(entry));
};

const updateManagerReportState = ({
  current,
  managers,
  event,
  report,
  barrierSatisfied,
}: {
  current: ManagerReportState;
  managers: ManagerNode[];
  event?: EventEnvelope;
  report?: ManagerTerminalReport;
  barrierSatisfied?: boolean;
}): ManagerReportState => {
  const p = event?.payload ?? {};
  const rosterRecord = asRecord(
    p.roster ?? p.manager_roster ?? p.managerReportRoster ?? p.report_roster,
  );
  const rosterEntriesRaw = Array.isArray(
    p.roster ?? p.manager_roster ?? p.managerReportRoster ?? p.report_roster,
  )
    ? (p.roster ?? p.manager_roster ?? p.managerReportRoster ?? p.report_roster)
    : (rosterRecord.entries ?? rosterRecord.managers);
  const rosterEntries = Array.isArray(rosterEntriesRaw) ? rosterEntriesRaw : [];
  const canonicalize = (id: string) => canonicalManagerId(managers, id) ?? id;
  const managerNodeIds = managers.map((manager) => manager.agentId ?? manager.id);
  const expectedManagerIds = [
    ...new Set(
      [
        ...current.roster.expectedManagerIds,
        ...managerNodeIds,
        ...managerIdsFrom(
          p.expected_manager_ids ??
            p.expectedManagerIds ??
            rosterRecord.expected_manager_ids ??
            rosterRecord.expectedManagerIds,
        ),
        ...managerIdsFrom(rosterEntriesRaw),
        ...(report ? [report.managerId] : []),
      ].map(canonicalize),
    ),
  ];
  const explicitReportedIds = managerIdsFrom(
    p.reported_manager_ids ??
      p.reportedManagerIds ??
      p.received_manager_ids ??
      p.receivedManagerIds ??
      rosterRecord.reported_manager_ids ??
      rosterRecord.reportedManagerIds,
  );
  for (const entry of rosterEntries) {
    const record = asRecord(entry);
    const entryId = managerIdsFrom([entry])[0];
    const entryOutcome = terminalOutcomeValue(record.outcome, record.status);
    if (
      entryId &&
      (booleanValue(record.reported, record.received) === true || entryOutcome != null)
    ) {
      explicitReportedIds.push(entryId);
    }
  }
  const reports = report ? { ...current.reports, [report.managerId]: report } : current.reports;
  const reportedManagerIds = [
    ...new Set(
      [...current.roster.reportedManagerIds, ...Object.keys(reports), ...explicitReportedIds].map(
        canonicalize,
      ),
    ),
  ];
  const explicitPendingIds = managerIdsFrom(
    p.pending_manager_ids ??
      p.pendingManagerIds ??
      p.missing_manager_ids ??
      p.missingManagerIds ??
      rosterRecord.pending_manager_ids ??
      rosterRecord.pendingManagerIds,
  ).map(canonicalize);
  const pendingManagerIds = [
    ...new Set(
      explicitPendingIds.length
        ? explicitPendingIds
        : expectedManagerIds.filter((id) => !reportedManagerIds.includes(id)),
    ),
  ];
  const expected =
    numberValue(
      p.expected,
      p.expected_count,
      p.expected_reports,
      p.expected_manager_count,
      p.manager_count,
      rosterRecord.expected,
      rosterRecord.expected_count,
    ) ?? 0;
  const reported =
    numberValue(
      p.reported,
      p.reported_count,
      p.report_count,
      p.received,
      p.received_count,
      rosterRecord.reported,
      rosterRecord.reported_count,
    ) ?? 0;
  const derivedOutcomes = reportOutcomeCounts(reports);
  const explicitOutcomeSource = asRecord(
    p.manager_outcome_counts ??
      p.managerOutcomeCounts ??
      p.outcome_counts ??
      p.outcomeCounts ??
      p.counts ??
      rosterRecord.counts,
  );
  const explicitOutcomes = report
    ? emptyOutcomeCounts()
    : outcomeCountsValue({ ...p, ...explicitOutcomeSource });
  const counts = {
    expected: Math.max(current.counts.expected, expectedManagerIds.length, expected),
    reported: Math.max(current.counts.reported, reportedManagerIds.length, reported),
    pending: 0,
    completed: Math.max(
      current.counts.completed,
      derivedOutcomes.completed,
      explicitOutcomes.completed,
    ),
    partial: Math.max(current.counts.partial, derivedOutcomes.partial, explicitOutcomes.partial),
    abandoned: Math.max(
      current.counts.abandoned,
      derivedOutcomes.abandoned,
      explicitOutcomes.abandoned,
    ),
    skipped: Math.max(current.counts.skipped, derivedOutcomes.skipped, explicitOutcomes.skipped),
    failed: Math.max(current.counts.failed, derivedOutcomes.failed, explicitOutcomes.failed),
  };
  counts.pending = Math.max(0, counts.expected - counts.reported);

  const priorEntries = new Map(current.roster.entries.map((entry) => [entry.managerId, entry]));
  for (const rawEntry of rosterEntries) {
    const record = asRecord(rawEntry);
    const rawId = managerIdsFrom([rawEntry])[0];
    if (!rawId) continue;
    const managerId = canonicalize(rawId);
    priorEntries.set(managerId, {
      managerId,
      workstreamId: stringValue(record.workstream_id, record.workstreamId),
      reported:
        booleanValue(record.reported, record.received) ?? reportedManagerIds.includes(managerId),
      outcome: terminalOutcomeValue(record.outcome, record.status),
      reportedAt: stringValue(record.reported_at, record.reportedAt, record.timestamp),
    });
  }
  const entries = [...new Set([...expectedManagerIds, ...reportedManagerIds])].map((managerId) => {
    const prior = priorEntries.get(managerId);
    const managerReport = reports[managerId];
    const manager = managers.find((candidate) => (candidate.agentId ?? candidate.id) === managerId);
    return {
      managerId,
      workstreamId: managerReport?.workstreamId ?? prior?.workstreamId ?? manager?.workstreamId,
      reported: reportedManagerIds.includes(managerId),
      outcome: managerReport?.outcome ?? prior?.outcome,
      reportedAt: managerReport?.reportedAt ?? prior?.reportedAt,
    };
  });
  return {
    reports,
    roster: {
      entries,
      expectedManagerIds,
      reportedManagerIds,
      pendingManagerIds,
    },
    counts,
    barrierSatisfied:
      barrierSatisfied ??
      booleanValue(p.satisfied, p.barrier_satisfied, p.barrierSatisfied) ??
      current.barrierSatisfied,
    updatedAt: event?.timestamp ?? current.updatedAt,
  };
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
  if (['partial', 'partially_completed', 'completed_with_issues'].includes(status)) {
    return 'partial';
  }
  if (['abandoned', 'aborted'].includes(status)) return 'abandoned';
  if (status === 'skipped') return 'skipped';
  if (['error', 'failed', 'preflight_failed', 'rejected', 'revise'].includes(status)) {
    return 'failed';
  }
  if (['queued', 'pending', 'planned', 'ready'].includes(status)) return 'queued';
  if (status === 'blocked') return 'blocked';
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
  if (['partial', 'partially_completed', 'completed_with_issues'].includes(status)) {
    return 'partial';
  }
  if (['abandoned'].includes(status)) return 'abandoned';
  if (['error', 'failed', 'rejected', 'revise'].includes(status)) return 'failed';
  if (['stopped', 'cancelled', 'canceled', 'aborted'].includes(status)) return 'aborted';
  return 'idle';
};

const agentStatusForOutcome = (outcome: TerminalOutcome): AgentStatus =>
  outcome === 'completed' ? 'passed' : outcome;

const managerStatusFromItems = (
  items: ManagerNode['items'],
  fallback: AgentStatus,
): AgentStatus => {
  if (!items.length) return fallback;
  const terminal = new Set<AgentStatus>([
    'passed',
    'partial',
    'abandoned',
    'skipped',
    'failed',
    'stopped',
  ]);
  if (!items.every((item) => terminal.has(item.status))) return fallback;
  if (items.some((item) => item.status === 'failed')) return 'failed';
  if (items.every((item) => item.status === 'abandoned')) return 'abandoned';
  if (items.every((item) => item.status === 'skipped')) return 'skipped';
  if (items.some((item) => ['partial', 'abandoned'].includes(item.status))) return 'partial';
  return 'passed';
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
  const managerId = identityValue(
    raw.manager_id,
    payload.manager_id,
    raw.managerId,
    payload.managerId,
    raw.manager_agent_id,
    payload.manager_agent_id,
    raw.managerAgentId,
    payload.managerAgentId,
    raw.parent_agent_instance_id,
    payload.parent_agent_instance_id,
  );
  const agentInstanceId = identityValue(
    raw.agent_instance_id,
    payload.agent_instance_id,
    raw.agent_id,
    payload.agent_id,
  );
  const callId = stringValue(
    raw.call_id,
    payload.call_id,
    raw.logical_request_id,
    payload.logical_request_id,
    raw.logicalRequestId,
    payload.logicalRequestId,
  );
  const rawAttemptId = stringValue(
    raw.attempt_id,
    payload.attempt_id,
    raw.attemptId,
    payload.attemptId,
  );
  const providerAttemptId =
    stringValue(raw.provider_attempt_id, payload.provider_attempt_id) ??
    (callId ? rawAttemptId : undefined);
  const executionAttemptId =
    stringValue(raw.execution_attempt_id, payload.execution_attempt_id) ??
    (!callId ? rawAttemptId : undefined);

  return {
    version: numberValue(raw.version, raw.schema_version, raw.v) ?? 0,
    sequence,
    type,
    timestamp: stringValue(raw.timestamp, raw.at, payload.timestamp) ?? new Date().toISOString(),
    taskId: stringValue(raw.task_id, payload.task_id) ?? taskId,
    sessionId: stringValue(raw.session_id, payload.session_id),
    workstreamId: stringValue(
      raw.workstream_id,
      payload.workstream_id,
      raw.workstreamId,
      payload.workstreamId,
    ),
    workItemId: stringValue(
      raw.work_item_id,
      payload.work_item_id,
      raw.workItemId,
      payload.workItemId,
      payload.ticket_id,
    ),
    managerId,
    agentInstanceId,
    callId,
    attemptId: providerAttemptId ?? executionAttemptId,
    executionAttemptId,
    providerAttemptId,
    callPurpose: stringValue(raw.call_purpose, payload.call_purpose),
    attempt: numberValue(raw.attempt, payload.attempt),
    requestRevision: numberValue(
      raw.request_revision,
      payload.request_revision,
      raw.requestRevision,
      payload.requestRevision,
    ),
    provider: stringValue(raw.provider, payload.provider),
    account: stringValue(
      raw.account_ref,
      payload.account_ref,
      raw.accountRef,
      payload.accountRef,
      raw.account,
      payload.account,
      payload.account_id,
      payload.accountId,
    ),
    provisional: booleanValue(raw.provisional, payload.provisional),
    committed: booleanValue(raw.committed, payload.committed),
    role,
    payload,
    raw,
  };
}

const provisionalChunkIdentity = (
  event: EventEnvelope,
): Pick<ProvisionalThinkingChunk, 'id' | 'attemptId' | 'chunkIndex'> => {
  const attemptId =
    event.attemptId ??
    stringValue(
      event.payload.attempt_id,
      event.payload.attemptId,
      event.payload.logical_request_id,
      event.payload.logicalRequestId,
      event.callId,
    );
  const chunkIndex = numberValue(event.payload.chunk_index, event.payload.chunkIndex);
  return {
    id:
      chunkIndex == null
        ? `${attemptId ?? 'request'}:sequence:${event.sequence}`
        : `${attemptId ?? 'request'}:chunk:${chunkIndex}`,
    attemptId,
    chunkIndex,
  };
};

const boundProvisionalThinking = (
  chunks: ProvisionalThinkingChunk[],
): { chunks: ProvisionalThinkingChunk[]; text: string } => {
  const bounded = chunks.slice(-WORKSPACE_LIMITS.provisionalThinkingChunks);
  let characters = bounded.reduce((total, chunk) => total + chunk.text.length, 0);
  while (bounded.length && characters > WORKSPACE_LIMITS.outputCharacters) {
    const overflow = characters - WORKSPACE_LIMITS.outputCharacters;
    const first = bounded[0];
    if (first.text.length <= overflow) {
      characters -= first.text.length;
      bounded.shift();
      continue;
    }
    bounded[0] = { ...first, text: first.text.slice(overflow) };
    characters -= overflow;
  }
  return { chunks: bounded, text: bounded.map((chunk) => chunk.text).join('') };
};

const withProvisionalThinking = (
  agent: AgentInstance,
  chunks: ProvisionalThinkingChunk[],
): AgentInstance => {
  const bounded = boundProvisionalThinking(chunks);
  return {
    ...agent,
    provisionalThinking: bounded.text,
    provisionalThinkingChunks: bounded.chunks,
  };
};

const appendProvisionalThinking = (
  agent: AgentInstance,
  event: EventEnvelope,
  text: string,
): AgentInstance => {
  const identity = provisionalChunkIdentity(event);
  const chunks = [
    ...agent.provisionalThinkingChunks.filter((chunk) => chunk.id !== identity.id),
    { ...identity, text },
  ];
  return withProvisionalThinking(agent, chunks);
};

const removeCommittedProvisionalThinking = (
  agent: AgentInstance,
  event: EventEnvelope,
  text: string,
): AgentInstance => {
  if (!agent.provisionalThinkingChunks.length) return agent;
  const identity = provisionalChunkIdentity(event);
  const exactIndex =
    identity.chunkIndex == null
      ? -1
      : agent.provisionalThinkingChunks.findIndex((chunk) => chunk.id === identity.id);
  if (exactIndex >= 0) {
    return withProvisionalThinking(
      agent,
      agent.provisionalThinkingChunks.filter((_, index) => index !== exactIndex),
    );
  }

  const compatible = (chunk: ProvisionalThinkingChunk) =>
    !identity.attemptId || !chunk.attemptId || chunk.attemptId === identity.attemptId;
  const compatibleText = agent.provisionalThinkingChunks
    .filter(compatible)
    .map((chunk) => chunk.text)
    .join('');
  if (!compatibleText.startsWith(text) && !text.startsWith(compatibleText)) return agent;

  let remaining = Math.min(text.length, compatibleText.length);
  const chunks: ProvisionalThinkingChunk[] = [];
  for (const chunk of agent.provisionalThinkingChunks) {
    if (!compatible(chunk) || remaining <= 0) {
      chunks.push(chunk);
      continue;
    }
    if (remaining >= chunk.text.length) {
      remaining -= chunk.text.length;
      continue;
    }
    chunks.push({ ...chunk, text: chunk.text.slice(remaining) });
    remaining = 0;
  }
  return withProvisionalThinking(agent, chunks);
};

const clearProvisionalThinking = (agent: AgentInstance): AgentInstance =>
  agent.provisionalThinking || agent.provisionalThinkingChunks.length
    ? { ...agent, provisionalThinking: '', provisionalThinkingChunks: [] }
    : agent;

const clearAllProvisionalThinking = (
  agents: Record<string, AgentInstance>,
): Record<string, AgentInstance> => {
  if (!Object.values(agents).some((agent) => agent.provisionalThinking)) return agents;
  return Object.fromEntries(
    Object.entries(agents).map(([id, agent]) => [id, clearProvisionalThinking(agent)]),
  );
};

const clearsProvisionalThinking = (event: EventEnvelope): boolean => {
  if (
    [
      'model_request_started',
      'model_request_replayed',
      'model_request_completed',
      'model_request_failed',
      'model_request_aborted',
      'account_invalidated',
      'account_rate_limited',
      'account_switch',
      'protocol_retry',
      'agent_failed',
      'agent_cancelled',
      'agent_skipped',
      'agent_abandoned',
      'manager_terminal_report',
      'workstream_failed',
      'hierarchy_failed',
      'hierarchy_cancelled',
      'hierarchy_partial',
      'director_final_review',
      'error',
      'done',
      'agent_completed',
      'finish_chat_turn',
    ].includes(event.type)
  ) {
    return true;
  }
  const phase = String(
    event.payload.stage ?? event.payload.phase ?? event.payload.status ?? '',
  ).toLowerCase();
  return (
    event.type === 'status' &&
    ['calling_model', 'stopped', 'stopping', 'cancelled', 'canceled', 'aborted', 'failed'].includes(
      phase,
    )
  );
};

const clearsAllProvisionalThinking = (event: EventEnvelope): boolean =>
  [
    'hierarchy_completed',
    'hierarchy_cancelled',
    'hierarchy_failed',
    'hierarchy_partial',
    'done',
  ].includes(event.type) ||
  (!event.agentInstanceId && clearsProvisionalThinking(event));

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
    provisionalThinking: '',
    provisionalThinkingChunks: [],
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
  if (
    type.includes('aborted') ||
    type.includes('abandoned') ||
    type.includes('partial') ||
    type.includes('skipped')
  )
    return 'warning';
  if (type.includes('retry') || type.includes('switch') || type.includes('remediation'))
    return 'warning';
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
    const covered = booleanValue(p.covered, p.balanced) === true;
    const successful = booleanValue(p.successful);
    if (!covered) {
      return `Terminal coverage incomplete: ${
        stringList(p.errors).join('; ') || 'incomplete terminal partitions'
      }`;
    }
    return successful === false
      ? 'Terminal coverage complete; outcomes are not all successful'
      : 'Terminal coverage complete and successful';
  }
  if (event.type === 'manager_terminal_report') {
    const outcome =
      terminalOutcomeValue(p.outcome, p.terminal_outcome, p.status, p.verdict) ?? 'completed';
    return `Manager terminal report: ${outcome.replaceAll('_', ' ')}`;
  }
  if (
    event.type === 'manager_report_barrier' ||
    event.type === 'manager_report_barrier_satisfied'
  ) {
    const reported =
      numberValue(p.reported, p.reported_count, p.report_count, p.received_count) ?? 0;
    const expected =
      numberValue(p.expected, p.expected_count, p.expected_reports, p.manager_count) ?? reported;
    return `Manager reports received: ${reported}/${expected}`;
  }
  if (event.type === 'hierarchy_partial') return 'Hierarchy completed with a partial outcome';
  if (event.type === 'director_final_review') {
    return `Director final review: ${stringValue(p.outcome, p.verdict, p.status) ?? 'reported'}`;
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
    executionAttemptId: event.executionAttemptId ?? existing?.executionAttemptId,
    callPurpose: event.callPurpose ?? existing?.callPurpose,
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
  if (clearsProvisionalThinking(event)) agent = clearProvisionalThinking(agent);
  const label = stringValue(p.agent_label, event.raw.agent_label);
  if (label && label !== agent.label) agent = { ...agent, label };
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
  if (event.type === 'agent_planned') {
    agent = {
      ...agent,
      status: 'queued',
      phase: phase ?? 'planned',
      executionState: 'planned',
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
    agent = removeCommittedProvisionalThinking(agent, event, chunk);
  }
  if (event.type === 'thinking' && chunk && event.provisional === true) {
    agent = appendProvisionalThinking(
      {
        ...agent,
        provisionalChunks: agent.provisionalChunks + 1,
        status: 'running',
        executionState: 'in_flight',
      },
      event,
      chunk,
    );
  } else if (event.type === 'token' && chunk && event.provisional === true) {
    agent = {
      ...agent,
      provisionalChunks: agent.provisionalChunks + 1,
      status: 'running',
      executionState: 'in_flight',
    };
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
  const lifecycleOutcome =
    event.type === 'agent_abandoned' || event.type === 'work_item_abandoned'
      ? 'abandoned'
      : event.type === 'agent_skipped' || event.type === 'work_item_skipped'
        ? 'skipped'
        : event.type === 'manager_terminal_report' ||
            event.type === 'director_final_review' ||
            event.type === 'hierarchy_partial'
          ? terminalOutcomeValue(
              p.outcome,
              p.terminal_outcome,
              p.terminalOutcome,
              p.verdict,
              p.status,
              event.type === 'hierarchy_partial' ? 'partial' : undefined,
            )
          : undefined;
  if (lifecycleOutcome) {
    const status: AgentStatus =
      lifecycleOutcome === 'completed'
        ? 'passed'
        : lifecycleOutcome === 'failed'
          ? 'failed'
          : lifecycleOutcome;
    const executionState: ExecutionState =
      lifecycleOutcome === 'completed' ? 'completed' : lifecycleOutcome;
    agent = {
      ...agent,
      status,
      executionState,
      phase:
        event.type === 'manager_terminal_report'
          ? 'terminal_report'
          : event.type === 'director_final_review'
            ? 'final_review'
            : lifecycleOutcome,
      action: eventMessage(event),
      error:
        lifecycleOutcome === 'abandoned' || lifecycleOutcome === 'failed'
          ? (stringValue(p.reason, p.error, p.summary) ?? agent.error)
          : agent.error,
    };
  }
  if (
    event.type === 'manager_report_barrier' ||
    event.type === 'manager_report_barrier_satisfied'
  ) {
    agent = {
      ...agent,
      status: 'running',
      executionState: 'in_flight',
      phase: 'manager_report_barrier',
      action: eventMessage(event),
    };
  }
  if (event.type.startsWith('remediation_')) {
    const exhausted = event.type === 'remediation_exhausted';
    agent = {
      ...agent,
      status: exhausted ? 'abandoned' : 'running',
      executionState: exhausted ? 'abandoned' : 'in_flight',
      phase: event.type,
      action: eventMessage(event),
      error: exhausted ? (stringValue(p.reason, p.error, p.summary) ?? agent.error) : agent.error,
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

  const childHasWorkItem = Boolean(event.workItemId ?? agent.workItemId);
  if (
    agent.role === 'worker' ||
    ((agent.role === 'tester' || agent.role === 'reviewer') && childHasWorkItem)
  ) {
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
      status:
        agent.status === 'failed' ? 'failed' : agent.status === 'blocked' ? 'blocked' : 'running',
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
  const finalReview = asRecord(hierarchy.director_final_review ?? hierarchy.directorFinalReview);
  const crisis = asRecord(hierarchy.crisis);
  const remediation = asRecord(hierarchy.remediation);
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
  const agents: Record<string, AgentInstance> = {};
  for (const [entryId, raw] of Object.entries(asRecord(hierarchy.agents))) {
    const value = asRecord(raw);
    const agentId = stringValue(value.id, value.agent_instance_id) ?? entryId;
    const event = normalizeEvent(
      {
        ...value,
        type: 'agent_snapshot',
        agent_instance_id: agentId,
        timestamp: stringValue(value.updated_at) ?? task.updated_at ?? task.created_at,
      },
      task.id,
      0,
    );
    agents[agentId] = mergeAgentEvent(emptyAgent(event), event);
  }
  let managers: ManagerNode[] = legacyWorkstreamEntries.map(([entryId, raw]) => {
    const stream = asRecord(raw);
    const managerId = stringValue(stream.id, stream.workstream_id) ?? entryId;
    const rawItems = Array.isArray(stream.work_items) ? stream.work_items : [];
    return {
      id: managerId,
      title: stringValue(stream.title, stream.name) ?? managerId,
      status: statusValue(stream.status),
      dependencies: stringList(stream.dependencies),
      agentId: identityValue(stream.agent_instance_id, stream.manager_agent_id, stream.agent_id),
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
  for (const agent of Object.values(agents)) {
    const manager = managers.find(
      (candidate) =>
        candidate.workstreamId === agent.workstreamId ||
        candidate.id === agent.managerId ||
        candidate.agentId === agent.managerId,
    );
    if (!manager) continue;
    if (agent.role === 'manager') {
      manager.agentId = agent.id;
      continue;
    }
    if (agent.role === 'worker' && agent.workItemId) {
      managers = managers.map((candidate) =>
        candidate.id !== manager.id
          ? candidate
          : {
              ...candidate,
              items: candidate.items.map((item) =>
                item.id !== agent.workItemId
                  ? item
                  : { ...item, agentIds: [...new Set([...item.agentIds, agent.id])] },
              ),
            },
      );
    }
  }
  const directorId = Object.values(agents).find((agent) => agent.role === 'director')?.id;
  let managerReports = updateManagerReportState({
    current: initialWorkspaceState.managerReports,
    managers,
  });
  const rawManagerReports =
    hierarchy.manager_terminal_reports ??
    hierarchy.managerTerminalReports ??
    hierarchy.manager_reports ??
    hierarchy.managerReports;
  const managerReportEntries = Array.isArray(rawManagerReports)
    ? rawManagerReports.map((value, index) => [`manager:${index + 1}`, value] as const)
    : Object.entries(asRecord(rawManagerReports));
  for (const [entryId, raw] of managerReportEntries) {
    const reportRecord = asRecord(raw);
    const reportEvent = normalizeEvent(
      {
        ...reportRecord,
        type: 'manager_terminal_report',
        manager_id:
          reportRecord.manager_id ??
          reportRecord.managerId ??
          reportRecord.manager_agent_id ??
          entryId,
        timestamp:
          stringValue(reportRecord.reported_at, reportRecord.reportedAt) ??
          task.updated_at ??
          task.created_at,
      },
      task.id,
      numberValue(reportRecord.sequence) ?? 0,
    );
    const report = managerTerminalReportValue(reportEvent, managers);
    if (report) {
      managerReports = updateManagerReportState({
        current: managerReports,
        managers,
        event: reportEvent,
        report,
      });
    }
  }
  const reportBarrier = asRecord(
    hierarchy.manager_report_barrier ??
      hierarchy.managerReportBarrier ??
      hierarchy.manager_report_summary ??
      hierarchy.managerReportSummary,
  );
  if (Object.keys(reportBarrier).length) {
    const barrierEvent = normalizeEvent(
      {
        ...reportBarrier,
        type: 'manager_report_barrier_satisfied',
        timestamp: task.updated_at ?? task.created_at,
      },
      task.id,
      numberValue(reportBarrier.sequence) ?? 0,
    );
    managerReports = updateManagerReportState({
      current: managerReports,
      managers,
      event: barrierEvent,
      barrierSatisfied:
        booleanValue(reportBarrier.satisfied, reportBarrier.barrier_satisfied) ?? true,
    });
  }
  managers = managers.map((manager) => {
    const report =
      managerReports.reports[manager.agentId ?? manager.id] ??
      Object.values(managerReports.reports).find(
        (candidate) => candidate.workstreamId === manager.workstreamId,
      );
    if (!report) return manager;
    return {
      ...manager,
      status: statusValue(report.outcome),
      terminalReport: report,
    };
  });
  return {
    ...initialWorkspaceState,
    task,
    updatedAt: task.updated_at,
    agents,
    directorId,
    managers,
    managerReports,
    hierarchyOutcome: terminalOutcomeValue(
      hierarchy.outcome,
      hierarchy.terminal_outcome,
      ['PARTIAL', 'ABANDONED', 'SKIPPED'].includes(task.status.toUpperCase())
        ? task.status
        : undefined,
    ),
    directorFinalReview: Object.keys(finalReview).length
      ? {
          outcome:
            terminalOutcomeValue(finalReview.outcome, finalReview.verdict, finalReview.status) ??
            'completed',
          verdict: stringValue(finalReview.verdict),
          summary: stringValue(finalReview.summary, finalReview.message),
          remainingRisks: stringList(finalReview.remaining_risks ?? finalReview.remainingRisks),
          integrationStatus: stringValue(
            finalReview.integration_status,
            finalReview.integrationStatus,
          ),
          managerReportsExpected: numberValue(
            finalReview.manager_reports_expected,
            finalReview.managerReportsExpected,
          ),
          managerReportsReported: numberValue(
            finalReview.manager_reports_reported,
            finalReview.managerReportsReported,
          ),
          finalReviewNumber: numberValue(
            finalReview.final_review_number,
            finalReview.finalReviewNumber,
          ),
          at: stringValue(finalReview.timestamp, finalReview.at, task.updated_at),
        }
      : undefined,
    crisis: Object.keys(crisis).length
      ? {
          crisisId: stringValue(crisis.crisis_id, crisis.crisisId),
          status: stringValue(crisis.status, crisis.state) ?? 'detected',
          scope: stringValue(crisis.scope),
          failureKind: stringValue(crisis.failure_kind, crisis.failureKind),
          reason: stringValue(crisis.reason, crisis.error, crisis.message),
          retryable: booleanValue(crisis.retryable),
          managerId: identityValue(crisis.manager_id, crisis.managerId),
          workstreamId: stringValue(crisis.workstream_id, crisis.workstreamId),
          affectedManagerIds: stringList(crisis.affected_manager_ids ?? crisis.affectedManagerIds),
          affectedWorkItemIds: stringList(
            crisis.affected_work_item_ids ?? crisis.affectedWorkItemIds,
          ),
          at: stringValue(crisis.timestamp, crisis.at, task.updated_at),
        }
      : undefined,
    remediation: Object.keys(remediation).length
      ? {
          crisisId: stringValue(remediation.crisis_id, remediation.crisisId),
          status: stringValue(remediation.status, remediation.state) ?? 'pending',
          action: stringValue(remediation.action, remediation.decision),
          strategy: stringValue(remediation.strategy, remediation.action),
          reason: stringValue(remediation.reason, remediation.error),
          instructions: stringValue(
            remediation.instructions,
            remediation.next_instructions,
            remediation.nextInstructions,
          ),
          summary: stringValue(remediation.summary, remediation.message),
          managerId: identityValue(remediation.manager_id, remediation.managerId),
          workstreamId: stringValue(remediation.workstream_id, remediation.workstreamId),
          affectedManagerIds: stringList(
            remediation.affected_manager_ids ?? remediation.affectedManagerIds,
          ),
          affectedWorkItemIds: stringList(
            remediation.affected_work_item_ids ?? remediation.affectedWorkItemIds,
          ),
          at: stringValue(remediation.timestamp, remediation.at, task.updated_at),
        }
      : undefined,
    graphRevision: managers.length || Object.keys(agents).length ? 1 : 0,
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
  provisionalThinkingChunks: ProvisionalThinkingChunk[];
  tokenCount: number;
  committedChunks: number;
  provisionalChunks: number;
  receivedStreamingChunk: boolean;
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

    const account = accountUsedByAttempt(event);
    if (isUsableAccount(account)) accountCandidates.add(account);

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
      provisionalThinkingChunks: current.provisionalThinkingChunks,
      tokenCount: 0,
      committedChunks: 0,
      provisionalChunks: 0,
      receivedStreamingChunk: false,
    };
    const chunk = stringValue(event.payload.text, event.payload.chunk);
    if (chunk && event.provisional === true) {
      batch.provisionalChunks += 1;
      if (event.type === 'thinking') {
        batch.provisionalThinkingChunks = appendProvisionalThinking(
          {
            ...batch.agent,
            provisionalThinkingChunks: batch.provisionalThinkingChunks,
          },
          event,
          chunk,
        ).provisionalThinkingChunks;
      }
      batch.receivedStreamingChunk = true;
    } else if (chunk) {
      if (event.type === 'token') {
        batch.outputChunks.push(chunk);
        batch.tokenCount += Math.max(1, Math.ceil(chunk.length / 4));
      } else {
        batch.thinkingChunks.push(chunk);
        batch.provisionalThinkingChunks = removeCommittedProvisionalThinking(
          {
            ...batch.agent,
            provisionalThinkingChunks: batch.provisionalThinkingChunks,
          },
          event,
          chunk,
        ).provisionalThinkingChunks;
      }
      batch.committedChunks += 1;
      batch.receivedStreamingChunk = true;
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
      const provisionalThinking = boundProvisionalThinking(batch.provisionalThinkingChunks);
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
        provisionalThinking: provisionalThinking.text,
        provisionalThinkingChunks: provisionalThinking.chunks,
        tokenCount: agent.tokenCount + batch.tokenCount,
        committedChunks: agent.committedChunks + batch.committedChunks,
        provisionalChunks: agent.provisionalChunks + batch.provisionalChunks,
        status: batch.receivedStreamingChunk ? 'running' : agent.status,
        executionState: batch.receivedStreamingChunk ? 'in_flight' : agent.executionState,
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
  if (action.type === 'apply-snapshot') {
    const snapshot = stateFromTask(action.task);
    const execution = asRecord(asRecord(action.task.hierarchy).execution);
    const hasExecutionSnapshot = Object.keys(execution).length > 0;
    const agents = { ...snapshot.agents, ...state.agents };
    const managers = state.managers.length ? state.managers : snapshot.managers;
    const managerReports = updateManagerReportState({
      current: {
        ...snapshot.managerReports,
        reports: {
          ...snapshot.managerReports.reports,
          ...state.managerReports.reports,
        },
        roster: {
          entries: [
            ...snapshot.managerReports.roster.entries,
            ...state.managerReports.roster.entries,
          ],
          expectedManagerIds: [
            ...new Set([
              ...snapshot.managerReports.roster.expectedManagerIds,
              ...state.managerReports.roster.expectedManagerIds,
            ]),
          ],
          reportedManagerIds: [
            ...new Set([
              ...snapshot.managerReports.roster.reportedManagerIds,
              ...state.managerReports.roster.reportedManagerIds,
            ]),
          ],
          pendingManagerIds: [],
        },
        counts: {
          ...snapshot.managerReports.counts,
          expected: Math.max(
            snapshot.managerReports.counts.expected,
            state.managerReports.counts.expected,
          ),
          reported: Math.max(
            snapshot.managerReports.counts.reported,
            state.managerReports.counts.reported,
          ),
          pending: 0,
        },
        barrierSatisfied:
          snapshot.managerReports.barrierSatisfied || state.managerReports.barrierSatisfied,
      },
      managers,
    });
    return {
      ...state,
      task: action.task,
      agents,
      directorId: state.directorId ?? snapshot.directorId,
      managers,
      managerReports,
      hierarchyOutcome: state.hierarchyOutcome ?? snapshot.hierarchyOutcome,
      directorFinalReview: state.directorFinalReview ?? snapshot.directorFinalReview,
      crisis: state.crisis ?? snapshot.crisis,
      remediation: state.remediation ?? snapshot.remediation,
      graphRevision:
        state.graphRevision +
        (Object.keys(agents).length > Object.keys(state.agents).length ? 1 : 0),
      sequence: Math.max(state.sequence, action.timeline?.latestSequence ?? state.sequence),
      fanout: hasExecutionSnapshot
        ? {
            ...state.fanout,
            requestAttempts: snapshot.fanout.requestAttempts,
            completedRequests: snapshot.fanout.completedRequests,
            replayedRequests: snapshot.fanout.replayedRequests,
            accountSwitches: snapshot.fanout.accountSwitches,
            failedRequests: snapshot.fanout.failedRequests,
            abortedRequests: snapshot.fanout.abortedRequests,
            calledAgentIds: snapshot.fanout.calledAgentIds,
            completedAgentIds: snapshot.fanout.completedAgentIds,
            calledByRole: snapshot.fanout.calledByRole,
          }
        : state.fanout,
      timeline: action.timeline ?? state.timeline,
      updatedAt: action.task.updated_at ?? state.updatedAt,
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
  if (action.type === 'clear-provisional-thinking') {
    const agents = clearAllProvisionalThinking(state.agents);
    return agents === state.agents ? state : { ...state, agents };
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
    const baseAgents = clearsAllProvisionalThinking(event)
      ? clearAllProvisionalThinking(state.agents)
      : state.agents;

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

    const accountCandidates = [accountUsedByAttempt(event)].filter(isUsableAccount);
    const accountHit = accountCandidates[0];
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
      const newlyCalled =
        Boolean(event.agentInstanceId) &&
        !fanout.calledAgentIds.includes(event.agentInstanceId as string);
      if (newlyCalled) calledByRole[role] = (calledByRole[role] ?? 0) + 1;
      const calledAgentIds =
        newlyCalled && event.agentInstanceId
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
      const completedAgentIds =
        event.agentInstanceId && !fanout.completedAgentIds.includes(event.agentInstanceId)
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

    const workItemOutcome =
      event.type === 'work_item_abandoned'
        ? 'abandoned'
        : event.type === 'work_item_skipped'
          ? 'skipped'
          : undefined;
    if (workItemOutcome && event.workItemId) {
      const targetIndex = managers.findIndex(
        (manager) =>
          manager.id === event.managerId ||
          manager.agentId === event.managerId ||
          manager.workstreamId === event.workstreamId ||
          manager.items.some((item) => item.id === event.workItemId),
      );
      if (targetIndex >= 0) {
        const manager = managers[targetIndex];
        const existingItem = manager.items.find((item) => item.id === event.workItemId);
        const nextItem = {
          id: event.workItemId,
          managerId: manager.id,
          title:
            stringValue(event.payload.work_item_title, event.payload.title, event.payload.ticket) ??
            existingItem?.title ??
            event.workItemId,
          status: agentStatusForOutcome(workItemOutcome),
          dependencies: existingItem?.dependencies ?? stringList(event.payload.dependencies),
          agentIds: existingItem?.agentIds ?? [],
          contract:
            workContractValue(event.payload.contract, event.payload.work_contract) ??
            existingItem?.contract,
        };
        const items = existingItem
          ? manager.items.map((item) => (item.id === event.workItemId ? nextItem : item))
          : [...manager.items, nextItem];
        managers = managers.map((candidate, index) =>
          index === targetIndex
            ? {
                ...candidate,
                status: managerStatusFromItems(items, candidate.status),
                items,
              }
            : candidate,
        );
      }
    }

    let managerReports = updateManagerReportState({
      current: state.managerReports,
      managers,
    });
    if (event.type === 'manager_terminal_report') {
      const report = managerTerminalReportValue(event, managers);
      if (report) {
        managerReports = updateManagerReportState({
          current: managerReports,
          managers,
          event,
          report,
        });
        managers = managers.map((manager) => {
          const matches =
            manager.id === report.managerId ||
            manager.agentId === report.managerId ||
            (report.workstreamId != null && manager.workstreamId === report.workstreamId);
          return matches
            ? {
                ...manager,
                status: agentStatusForOutcome(report.outcome),
                terminalReport: report,
              }
            : manager;
        });
      }
    } else if (
      event.type === 'manager_report_barrier' ||
      event.type === 'manager_report_barrier_satisfied'
    ) {
      managerReports = updateManagerReportState({
        current: managerReports,
        managers,
        event,
        barrierSatisfied: true,
      });
    }

    let reconciliation = state.reconciliation;
    let projectLease = state.projectLease;
    let effects = state.effects;
    let sandbox = state.sandbox;
    let timeline = state.timeline;
    let task = state.task;
    let hierarchyOutcome = state.hierarchyOutcome;
    let directorFinalReview = state.directorFinalReview;
    let crisis = state.crisis;
    let remediation = state.remediation;
    if (event.type === 'completion_reconciliation') {
      reconciliation = reconciliationValue(event.payload, event.timestamp);
      const reportBarrier = asRecord(event.payload.report_barrier ?? event.payload.reportBarrier);
      if (Object.keys(reportBarrier).length) {
        managerReports = updateManagerReportState({
          current: managerReports,
          managers,
          event: { ...event, payload: reportBarrier },
          barrierSatisfied: booleanValue(reportBarrier.satisfied),
        });
      }
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
    } else if (event.type === 'timeline_gap') {
      timeline = {
        latestSequence:
          numberValue(event.payload.latest_sequence) ?? timeline?.latestSequence ?? event.sequence,
        retainedFromSequence:
          numberValue(event.payload.retained_from_sequence) ??
          timeline?.retainedFromSequence ??
          event.sequence + 1,
        historyIncomplete: true,
      };
    }

    const crisisEvent =
      event.type === 'crisis' ||
      event.type.startsWith('crisis_') ||
      event.type.endsWith('_crisis') ||
      event.type.includes('_crisis_');
    const remediationEvent =
      event.type === 'remediation' ||
      event.type.startsWith('remediation_') ||
      event.type.endsWith('_remediation') ||
      event.type.includes('_remediation_');
    if (crisisEvent) {
      crisis = {
        crisisId: stringValue(event.payload.crisis_id, event.payload.crisisId),
        status:
          stringValue(event.payload.status, event.payload.state) ??
          (event.type.replace(/^.*crisis_?/, '') || 'detected'),
        scope: stringValue(event.payload.scope),
        failureKind: stringValue(event.payload.failure_kind, event.payload.failureKind),
        reason: stringValue(
          event.payload.reason,
          event.payload.error,
          event.payload.message,
          event.payload.summary,
        ),
        retryable: booleanValue(event.payload.retryable),
        managerId: event.managerId,
        workstreamId: event.workstreamId,
        affectedManagerIds: stringList(
          event.payload.affected_manager_ids ?? event.payload.affectedManagerIds,
        ),
        affectedWorkItemIds: stringList(
          event.payload.affected_work_item_ids ?? event.payload.affectedWorkItemIds,
        ),
        at: event.timestamp,
      };
      if (task) {
        task = {
          ...task,
          status: 'REVISION',
          phase: event.type,
          current_agent: event.role === 'manager' || event.managerId ? 'manager' : 'director',
          updated_at: event.timestamp,
          last_error: crisis.reason ?? task.last_error,
        };
      }
    }
    if (remediationEvent) {
      const remediationStatus =
        stringValue(event.payload.status, event.payload.state) ??
        (event.type.replace(/^.*remediation_?/, '') || 'running');
      remediation = {
        crisisId: stringValue(event.payload.crisis_id, event.payload.crisisId),
        status: remediationStatus,
        action: stringValue(event.payload.action, event.payload.decision),
        strategy: stringValue(
          event.payload.strategy,
          event.payload.action,
          event.payload.remediation,
        ),
        reason: stringValue(event.payload.reason, event.payload.error),
        instructions: stringValue(
          event.payload.instructions,
          event.payload.next_instructions,
          event.payload.nextInstructions,
        ),
        summary: stringValue(event.payload.summary, event.payload.message, event.payload.detail),
        managerId: event.managerId,
        workstreamId: event.workstreamId,
        affectedManagerIds: stringList(
          event.payload.affected_manager_ids ?? event.payload.affectedManagerIds,
        ),
        affectedWorkItemIds: stringList(
          event.payload.affected_work_item_ids ?? event.payload.affectedWorkItemIds,
        ),
        at: event.timestamp,
      };
      if (task) {
        task = {
          ...task,
          status: ['failed', 'abandoned'].includes(remediationStatus.toLowerCase())
            ? 'REVISION'
            : 'RUNNING',
          phase: event.type,
          current_agent: event.role === 'manager' || event.managerId ? 'manager' : 'director',
          updated_at: event.timestamp,
        };
      }
    }
    if (event.type === 'hierarchy_partial') {
      hierarchyOutcome = 'partial';
      if (task) {
        task = {
          ...task,
          status: 'PARTIAL',
          phase: 'director_review',
          current_agent: 'director',
          updated_at: event.timestamp,
          finished_at: event.timestamp,
          last_reviewer_feedback:
            stringValue(event.payload.summary, event.payload.message) ??
            task.last_reviewer_feedback,
        };
      }
    }
    if (event.type === 'director_final_review') {
      const explicitOutcome = terminalOutcomeValue(
        event.payload.outcome,
        event.payload.terminal_outcome,
        event.payload.terminalOutcome,
      );
      const verdict = stringValue(event.payload.verdict);
      const outcome =
        explicitOutcome ??
        (['approved', 'complete', 'completed', 'passed'].includes(
          String(verdict ?? '').toLowerCase(),
        )
          ? 'completed'
          : 'failed');
      directorFinalReview = {
        outcome,
        verdict,
        summary: stringValue(event.payload.summary, event.payload.message, event.payload.feedback),
        remainingRisks: stringList(event.payload.remaining_risks ?? event.payload.remainingRisks),
        integrationStatus: stringValue(
          event.payload.integration_status,
          event.payload.integrationStatus,
        ),
        managerReportsExpected: numberValue(
          event.payload.manager_reports_expected,
          event.payload.managerReportsExpected,
        ),
        managerReportsReported: numberValue(
          event.payload.manager_reports_reported,
          event.payload.managerReportsReported,
        ),
        finalReviewNumber: numberValue(
          event.payload.final_review_number,
          event.payload.finalReviewNumber,
        ),
        at: event.timestamp,
      };
      if (task) {
        const taskStatus: Record<TerminalOutcome, string> = {
          completed: 'COMPLETED',
          partial: 'PARTIAL',
          abandoned: 'ABANDONED',
          skipped: 'SKIPPED',
          failed: 'FAILED',
        };
        task = {
          ...task,
          status: explicitOutcome ? taskStatus[outcome] : task.status,
          phase: 'director_review',
          current_agent: 'director',
          updated_at: event.timestamp,
          finished_at: explicitOutcome ? event.timestamp : task.finished_at,
          last_review_verdict:
            stringValue(event.payload.verdict, event.payload.outcome) ?? task.last_review_verdict,
          last_reviewer_feedback: directorFinalReview.summary ?? task.last_reviewer_feedback,
        };
      }
      if (explicitOutcome) hierarchyOutcome = outcome;
    }

    const agentId = event.agentInstanceId;

    // No real agent id → keep signals/accounts/sequence, do not spawn ghost agents.
    if (!agentId) {
      const graphChanged = signals !== state.signals || managers !== state.managers;
      return {
        ...state,
        task,
        agents: baseAgents,
        graphRevision: state.graphRevision + (graphChanged ? 1 : 0),
        sequence: event.sequence,
        eventCount: state.eventCount + 1,
        signals,
        usedAccounts,
        fanout,
        managerReports,
        hierarchyOutcome,
        directorFinalReview,
        crisis,
        remediation,
        reconciliation,
        projectLease,
        effects,
        sandbox,
        timeline,
        updatedAt: event.timestamp,
        managers,
      };
    }

    const current = baseAgents[agentId] ?? emptyAgent({ ...event, agentInstanceId: agentId });
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
    const accountHits = [accountHit].filter(isUsableAccount);
    const nextAccounts = [...usedAccounts];
    for (const account of accountHits) {
      if (!nextAccounts.includes(account)) nextAccounts.push(account);
    }
    const streaming = event.type === 'token' || event.type === 'thinking';
    const nextManagers = streaming ? managers : ensureDag(managers, enrichedEvent, agent);
    const nextManagerReports = updateManagerReportState({
      current: managerReports,
      managers: nextManagers,
    });
    return {
      ...state,
      task,
      agents: { ...baseAgents, [agentId]: agent },
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
      managerReports: nextManagerReports,
      hierarchyOutcome,
      directorFinalReview,
      crisis,
      remediation,
      reconciliation,
      projectLease,
      effects,
      sandbox,
      timeline,
      updatedAt: event.timestamp,
    };
  }
  return state;
}

export type { WorkspaceAction };
