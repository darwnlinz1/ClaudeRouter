import { useMemo, useState, type ReactNode } from 'react';
import {
  Activity,
  Boxes,
  CheckCircle2,
  CircleAlert,
  ClipboardCheck,
  FileDiff,
  GitBranch,
  Layers3,
  LockKeyhole,
  Network,
  Radio,
  ShieldCheck,
  TestTube2,
} from 'lucide-react';
import type {
  AgentInstance,
  CompletionReconciliation,
  TaskSummary,
  WorkContract,
  WorkspaceState,
} from '../types';
import { DagOverview } from './DagOverview';

type DashboardView = 'overview' | 'plan' | 'agents' | 'calls' | 'changes' | 'tests';

interface TaskDashboardProps {
  state: WorkspaceState;
  task: TaskSummary;
  onOpenAgent: (agent: AgentInstance) => void;
  graphExpanded: boolean;
  onToggleGraph: () => void;
}

const views: Array<{ id: DashboardView; label: string; icon: typeof Activity }> = [
  { id: 'overview', label: 'Overview', icon: Activity },
  { id: 'plan', label: 'Plan', icon: Layers3 },
  { id: 'agents', label: 'Agents', icon: Network },
  { id: 'calls', label: 'Calls', icon: Radio },
  { id: 'changes', label: 'Changes', icon: FileDiff },
  { id: 'tests', label: 'Tests', icon: TestTube2 },
];

const riskRank: Record<WorkContract['riskLevel'], number> = {
  low: 0,
  medium: 1,
  high: 2,
  critical: 3,
};

const shortHash = (value?: string) => (value ? value.slice(0, 10) : 'not reported');

const formatTime = (value?: string) => {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? '—'
    : date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};

const sentenceStatus = (value: string) => {
  const normalized = value.replaceAll('_', ' ').trim().toLowerCase();
  return normalized ? normalized[0].toUpperCase() + normalized.slice(1) : 'Unknown';
};

export function TaskDashboard({
  state,
  task,
  onOpenAgent,
  graphExpanded,
  onToggleGraph,
}: TaskDashboardProps) {
  const [view, setView] = useState<DashboardView>('overview');
  const dashboardTask = state.task?.id === task.id ? state.task : task;
  const agents = useMemo(
    () =>
      Object.values(state.agents).filter(
        (agent) => !agent.id.endsWith(':root') && !agent.id.endsWith(':anon'),
      ),
    [state.agents],
  );
  const contracts = useMemo(() => {
    const byId = new Map<string, WorkContract>();
    for (const manager of state.managers) {
      if (manager.contract) byId.set(manager.contract.id, manager.contract);
      for (const item of manager.items) {
        if (item.contract) byId.set(item.contract.id, item.contract);
      }
    }
    for (const agent of agents) {
      if (agent.workContract) byId.set(agent.workContract.id, agent.workContract);
    }
    return [...byId.values()];
  }, [agents, state.managers]);
  const highestRisk = contracts.reduce<WorkContract['riskLevel'] | undefined>(
    (current, contract) =>
      !current || riskRank[contract.riskLevel] > riskRank[current] ? contract.riskLevel : current,
    undefined,
  );
  const completedAgents = agents.filter((agent) =>
    ['passed', 'partial', 'abandoned', 'skipped', 'failed', 'stopped'].includes(agent.status),
  ).length;
  const progress = agents.length
    ? Math.round((completedAgents / agents.length) * 100)
    : [
          'COMPLETED',
          'DONE',
          'PARTIAL',
          'ABANDONED',
          'SKIPPED',
          'FAILED',
          'STOPPED',
          'CANCELLED',
        ].includes(dashboardTask.status.toUpperCase())
      ? 100
      : 0;
  const calls = useMemo(
    () =>
      agents
        .flatMap((agent) => agent.callAttempts.map((attempt) => ({ agent, attempt })))
        .sort((left, right) =>
          (right.attempt.finishedAt ?? right.attempt.startedAt ?? '').localeCompare(
            left.attempt.finishedAt ?? left.attempt.startedAt ?? '',
          ),
        )
        .slice(0, 120),
    [agents],
  );
  const tests = useMemo(
    () =>
      agents
        .flatMap((agent) => agent.tests.map((test) => ({ agent, test })))
        .slice(-100)
        .reverse(),
    [agents],
  );

  const selectView = (next: DashboardView) => {
    if (graphExpanded && next !== 'agents') onToggleGraph();
    setView(next);
  };

  const fullscreenGraph = graphExpanded && view === 'agents';

  return (
    <section
      className={`task-dashboard view-${view}${fullscreenGraph ? ' graph-fullscreen' : ''}`}
      aria-label="Task dashboard"
    >
      <header className="dashboard-header">
        <div>
          <span className="eyebrow">Workspace</span>
          <strong>{views.find((candidate) => candidate.id === view)?.label}</strong>
        </div>
        <nav className="dashboard-tabs" aria-label="Task views" role="tablist">
          {views.map(({ id, label, icon: Icon }, index) => (
            <button
              key={id}
              type="button"
              role="tab"
              className={view === id ? 'active' : ''}
              aria-selected={view === id}
              aria-controls="task-dashboard-panel"
              aria-label={label}
              title={label}
              tabIndex={view === id ? 0 : -1}
              onClick={() => selectView(id)}
              onKeyDown={(event) => {
                let nextIndex: number | undefined;
                if (event.key === 'ArrowRight') nextIndex = (index + 1) % views.length;
                else if (event.key === 'ArrowLeft')
                  nextIndex = (index - 1 + views.length) % views.length;
                else if (event.key === 'Home') nextIndex = 0;
                else if (event.key === 'End') nextIndex = views.length - 1;
                if (nextIndex == null) return;
                event.preventDefault();
                const tabs =
                  event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>(
                    '[role="tab"]',
                  );
                selectView(views[nextIndex].id);
                tabs?.[nextIndex]?.focus();
              }}
            >
              <Icon size={14} />
              <span>{label}</span>
            </button>
          ))}
        </nav>
      </header>

      <div
        className="dashboard-body"
        id="task-dashboard-panel"
        role="tabpanel"
        aria-label={views.find((candidate) => candidate.id === view)?.label}
      >
        {view === 'overview' && (
          <Overview
            state={state}
            task={dashboardTask}
            progress={progress}
            completedAgents={completedAgents}
            agentCount={agents.length}
            highestRisk={highestRisk}
          />
        )}
        {view === 'plan' && <PlanView state={state} contracts={contracts} />}
        {view === 'agents' && (
          <DagOverview
            state={state}
            onOpenAgent={onOpenAgent}
            expanded={graphExpanded}
            onToggleExpand={onToggleGraph}
          />
        )}
        {view === 'calls' && (
          <div className="record-list call-list">
            {calls.map(({ agent, attempt }) => (
              <button
                type="button"
                className={`record-row status-${attempt.status}`}
                key={`${agent.id}:${attempt.id}`}
                onClick={() => onOpenAgent(agent)}
                title={`${agent.title} · Attempt ${attempt.attempt} · ${attempt.status} · ${attempt.error ?? attempt.logicalRequestId ?? attempt.id}`}
                aria-label={`${agent.title}, attempt ${attempt.attempt}, ${attempt.status}`}
              >
                <span className="record-status" />
                <div>
                  <strong>{agent.title}</strong>
                  <small>
                    {attempt.provider ?? 'provider not reported'} ·{' '}
                    {attempt.account ?? 'account redacted'}
                  </small>
                </div>
                <div>
                  <span>
                    Attempt {attempt.attempt}
                    {attempt.replayed ? ' · replay' : ''}
                    {attempt.requestRevision ? ` · revision ${attempt.requestRevision}` : ''}
                  </span>
                  <small>{attempt.logicalRequestId ?? attempt.id}</small>
                </div>
                <div>
                  <strong>{attempt.status}</strong>
                  <small>
                    {attempt.error ?? formatTime(attempt.finishedAt ?? attempt.startedAt)}
                  </small>
                </div>
              </button>
            ))}
            {!calls.length && (
              <EmptyState
                icon={<Radio size={18} />}
                title="No model calls yet"
                detail="Provider attempts, replays, accounts, and failures appear here."
              />
            )}
          </div>
        )}
        {view === 'changes' && <ChangesView state={state} task={dashboardTask} />}
        {view === 'tests' && (
          <div className="record-list test-record-list">
            {tests.map(({ agent, test }) => (
              <button
                type="button"
                className={`record-row status-${test.status}`}
                key={`${agent.id}:${test.id}`}
                onClick={() => onOpenAgent(agent)}
                title={`${test.name} · ${agent.title} · ${test.status} · ${test.detail ?? 'No detail'}`}
                aria-label={`${test.name}, ${test.status}, ${agent.title}`}
              >
                <span className="record-status" />
                <div>
                  <strong>{test.name}</strong>
                  <small>{agent.title}</small>
                </div>
                <div>
                  <span>{test.actualIsolation ?? 'isolation not reported'}</span>
                  <small>
                    {test.isolationDetails ?? test.requestedIsolation ?? 'Legacy test result'}
                  </small>
                </div>
                <div>
                  <strong>{test.status}</strong>
                  <small>{test.detail ?? 'No detail'}</small>
                </div>
              </button>
            ))}
            {!tests.length && (
              <EmptyState
                icon={<TestTube2 size={18} />}
                title="No test evidence yet"
                detail="Machine-gate results and actual sandbox isolation appear here."
              />
            )}
          </div>
        )}
      </div>
    </section>
  );
}

function Overview({
  state,
  task,
  progress,
  completedAgents,
  agentCount,
  highestRisk,
}: {
  state: WorkspaceState;
  task: TaskSummary;
  progress: number;
  completedAgents: number;
  agentCount: number;
  highestRisk?: WorkContract['riskLevel'];
}) {
  const selection = state.fanout.directorSelection;
  const managerSelections = Object.values(state.fanout.managerSelections);
  const selectedWorkers = managerSelections.reduce((total, item) => total + item.selected, 0);
  const maximumWorkers = managerSelections.reduce((total, item) => total + item.maximum, 0);
  const selectedManagers = selection?.selected ?? state.fanout.plannedManagers;
  const fallbackMaximumWorkers =
    Math.max(0, state.fanout.maxWorkersPerManager - 1) * selectedManagers;
  const lease = state.projectLease;
  const sandbox = state.sandbox;
  const reconciliation = state.reconciliation;
  const reportCounts = state.managerReports.counts;
  const expectedReports =
    reportCounts.expected || state.managers.length || state.fanout.plannedManagers;
  const reportedReports = Math.min(expectedReports || reportCounts.reported, reportCounts.reported);
  const reportBarrierComplete =
    state.managerReports.barrierSatisfied ||
    (expectedReports > 0 && reportedReports >= expectedReports);
  const reportTone =
    reportCounts.failed > 0 || reportCounts.abandoned > 0
      ? 'failed'
      : reportCounts.partial > 0 || reportCounts.skipped > 0
        ? 'medium'
        : reportBarrierComplete
          ? 'balanced'
          : 'unknown';
  const reconciliationOutcomes = reconciliation?.workItems;
  const nonSuccessfulOutcomes = reconciliationOutcomes
    ? reconciliationOutcomes.partial +
      reconciliationOutcomes.abandoned +
      reconciliationOutcomes.skipped +
      reconciliationOutcomes.blocked +
      reconciliationOutcomes.failed +
      reconciliationOutcomes.preflightFailed
    : 0;
  const hardFailures = reconciliationOutcomes
    ? reconciliationOutcomes.abandoned +
      reconciliationOutcomes.blocked +
      reconciliationOutcomes.failed +
      reconciliationOutcomes.preflightFailed
    : 0;
  const coverageComplete = reconciliation
    ? (reconciliation.covered ?? reconciliation.balanced)
    : false;
  const reconciliationSuccessful = reconciliation
    ? (reconciliation.successful ??
      (coverageComplete && reconciliation.balanced && nonSuccessfulOutcomes === 0))
    : false;
  return (
    <div className="overview-grid">
      <article className="overview-card overview-progress">
        <CardHeading
          icon={<Activity size={16} />}
          label="Run status"
          tone={task.status.toLowerCase()}
        />
        <strong className="overview-value">{sentenceStatus(task.status)}</strong>
        <p>
          {completedAgents} of {agentCount || '—'} agent outcomes are terminal.
        </p>
        <div
          className="progress-track"
          role="progressbar"
          aria-label="Task completion"
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={progress}
          aria-valuetext={`${progress}% complete`}
        >
          <span style={{ width: `${progress}%` }} />
        </div>
        <small>{progress}% observed progress</small>
      </article>

      <article className="overview-card overview-manager-reports">
        <CardHeading
          icon={<ClipboardCheck size={16} />}
          label="Manager reports"
          tone={reportTone}
        />
        <strong
          className="overview-value"
          aria-label={`${reportedReports} of ${expectedReports} Manager terminal reports received`}
        >
          {reportedReports} / {expectedReports || '—'}
        </strong>
        <p>
          {reportBarrierComplete
            ? 'Terminal report coverage is complete; outcome quality is shown separately.'
            : `${Math.max(0, expectedReports - reportedReports)} Manager terminal report${
                expectedReports - reportedReports === 1 ? '' : 's'
              } still pending.`}
        </p>
        <ManagerOutcomeCounts
          completed={reportCounts.completed}
          partial={reportCounts.partial}
          abandoned={reportCounts.abandoned}
        />
      </article>

      <article className="overview-card">
        <CardHeading
          icon={<CircleAlert size={16} />}
          label="Contract risk"
          tone={highestRisk ?? 'unknown'}
        />
        <strong className="overview-value">{highestRisk ?? 'Not reported'}</strong>
        <p>Highest risk across versioned work contracts.</p>
        <small>
          {state.managers.reduce((total, manager) => total + manager.items.length, 0)} planned work
          items
        </small>
      </article>

      <article className="overview-card">
        <CardHeading
          icon={<LockKeyhole size={16} />}
          label="Project lease"
          tone={lease?.status ?? 'unknown'}
        />
        <strong className="overview-value">{lease?.status ?? 'Not reported'}</strong>
        <p>{lease?.isolationLevel ?? 'Older tasks may not include lease metadata.'}</p>
        <small>
          {lease?.projectKey ?? 'No project key'}
          {lease?.fencingToken != null ? ` · fence ${lease.fencingToken}` : ''}
        </small>
      </article>

      <article className="overview-card">
        <CardHeading
          icon={<ShieldCheck size={16} />}
          label="Sandbox trust"
          tone={sandbox?.actualIsolation ?? 'unknown'}
        />
        <strong className="overview-value">{sandbox?.actualIsolation ?? 'Not tested'}</strong>
        <p>{sandbox?.isolationDetails ?? 'Actual test isolation has not been reported.'}</p>
        <small>Requested: {sandbox?.requestedIsolation ?? '—'}</small>
      </article>

      <article className="overview-card overview-reconciliation">
        <CardHeading
          icon={coverageComplete ? <CheckCircle2 size={16} /> : <ClipboardCheck size={16} />}
          label="Terminal coverage"
          tone={
            reconciliation
              ? !coverageComplete || hardFailures > 0
                ? 'failed'
                : nonSuccessfulOutcomes > 0 || !reconciliationSuccessful
                  ? 'medium'
                  : 'balanced'
              : 'unknown'
          }
        />
        <strong className="overview-value">
          {reconciliation
            ? coverageComplete
              ? 'Coverage complete'
              : 'Coverage incomplete'
            : 'Pending'}
        </strong>
        <p>
          {reconciliation?.errors.join(' · ') ||
            (coverageComplete
              ? reconciliationSuccessful
                ? 'All planned entities are terminal and successful.'
                : 'All planned entities are terminal, but outcomes are not all successful.'
              : 'Waiting for every planned work item, agent, and call to reach a terminal outcome.')}
        </p>
        {reconciliation && <ReconciliationCounts reconciliation={reconciliation} />}
      </article>

      <article className="overview-card overview-capacity">
        <CardHeading icon={<GitBranch size={16} />} label="Planning caps" tone="info" />
        <div className="capacity-grid">
          <Capacity
            label="Managers selected"
            selected={selectedManagers}
            maximum={selection?.maximum ?? state.fanout.maxManagers}
          />
          <Capacity
            label="Workers selected"
            selected={selectedWorkers || state.fanout.plannedCoders}
            maximum={maximumWorkers || fallbackMaximumWorkers || undefined}
          />
        </div>
        <p>
          {selection?.reason ??
            'Planning caps are maxima, never required counts. Planners select only the work needed.'}
        </p>
        <div className="execution-slots">
          <span>Execution slots</span>
          <strong>{state.fanout.maxParallelManagers || '—'} managers</strong>
          <strong>{state.fanout.maxParallelWorkersPerManager || '—'} workers / manager</strong>
          <strong>{state.fanout.maxParallelWorkers || '—'} workers global</strong>
        </div>
      </article>
    </div>
  );
}

function PlanView({ state, contracts }: { state: WorkspaceState; contracts: WorkContract[] }) {
  const reportCounts = state.managerReports.counts;
  const expectedReports =
    reportCounts.expected || state.managers.length || state.fanout.plannedManagers;
  return (
    <div className="plan-view">
      <div className="plan-summary">
        <div>
          <span>Workstreams</span>
          <strong>{state.managers.length}</strong>
        </div>
        <div>
          <span>Contracts</span>
          <strong>{contracts.length}</strong>
        </div>
        <div>
          <span>Manager reports</span>
          <strong>
            {Math.min(expectedReports || reportCounts.reported, reportCounts.reported)} /{' '}
            {expectedReports || '—'}
          </strong>
        </div>
        <div className="outcome-completed">
          <span>Completed</span>
          <strong>{reportCounts.completed}</strong>
        </div>
        <div className="outcome-partial">
          <span>Partial</span>
          <strong>{reportCounts.partial}</strong>
        </div>
        <div className="outcome-abandoned">
          <span>Abandoned</span>
          <strong>{reportCounts.abandoned}</strong>
        </div>
        <div>
          <span>Selected / cap</span>
          <strong>
            {state.fanout.directorSelection?.selected ?? state.fanout.plannedManagers} /{' '}
            {(state.fanout.directorSelection?.maximum ?? state.fanout.maxManagers) || '—'}
          </strong>
        </div>
      </div>
      {state.managers.map((manager) => (
        <article className="plan-workstream" key={manager.id}>
          <header>
            <div>
              <span className={`status-dot status-${manager.status}`} />
              <strong>{manager.title}</strong>
              <small title={manager.workstreamId ?? manager.id}>
                {manager.workstreamId ?? manager.id}
              </small>
            </div>
            <span>
              {manager.terminalReport
                ? `${sentenceStatus(manager.terminalReport.outcome)} report`
                : `${manager.items.length} work item${manager.items.length === 1 ? '' : 's'}`}
            </span>
          </header>
          {manager.contract && <ContractSummary contract={manager.contract} />}
          {manager.items.length ? (
            <div className="plan-items">
              {manager.items.map((item) => (
                <div key={item.id}>
                  <span className={`status-dot status-${item.status}`} />
                  <div>
                    <strong>{item.title}</strong>
                    <small title={item.contract?.id ?? item.id}>
                      {item.contract ? `${item.contract.id} · v${item.contract.version}` : item.id}
                    </small>
                  </div>
                  <span>
                    {item.dependencies.length
                      ? `${item.dependencies.length} dependenc${item.dependencies.length === 1 ? 'y' : 'ies'}`
                      : 'Ready'}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="inline-empty">This Manager has not published work items yet.</p>
          )}
        </article>
      ))}
      {!state.managers.length && (
        <EmptyState
          icon={<Boxes size={18} />}
          title="No plan selected yet"
          detail="Director workstreams and Manager work contracts appear here."
        />
      )}
    </div>
  );
}

function ChangesView({ state, task }: { state: WorkspaceState; task: TaskSummary }) {
  const files = Object.entries(task.changed_files ?? {}).slice(0, 200);
  return (
    <div className="changes-view">
      <section>
        <span className="eyebrow">Changed files</span>
        {files.map(([path, stats]) => (
          <div className="change-row" key={path}>
            <FileDiff size={14} />
            <strong>{path}</strong>
            <span>
              +{stats.additions ?? 0} / −{stats.deletions ?? 0}
            </span>
          </div>
        ))}
        {!files.length && <p className="inline-empty">No changed files reported.</p>}
      </section>
      <section>
        <span className="eyebrow">Applied effects</span>
        {[...state.effects]
          .reverse()
          .slice(0, 100)
          .map((effect) => (
            <div className="effect-row" key={effect.id}>
              <CheckCircle2 size={14} />
              <div>
                <strong>{effect.target ?? effect.kind ?? effect.id}</strong>
                <small>
                  {shortHash(effect.beforeSha256)} → {shortHash(effect.afterSha256)}
                </small>
              </div>
              <span>{effect.kind ?? 'effect'}</span>
            </div>
          ))}
        {!state.effects.length && <p className="inline-empty">No durable effects reported.</p>}
      </section>
    </div>
  );
}

function ReconciliationCounts({ reconciliation }: { reconciliation: CompletionReconciliation }) {
  return (
    <div className="reconciliation-counts">
      {(
        [
          ['Streams', reconciliation.workstreams],
          ['Items', reconciliation.workItems],
          ['Agents', reconciliation.agents],
          ['Calls', reconciliation.calls],
        ] as const
      ).map(([label, partition]) => (
        <span key={label}>
          <small>{label}</small>
          <strong>
            {partition.terminal}/{partition.planned}
          </strong>
        </span>
      ))}
    </div>
  );
}

function ManagerOutcomeCounts({
  completed,
  partial,
  abandoned,
}: {
  completed: number;
  partial: number;
  abandoned: number;
}) {
  return (
    <div className="manager-outcome-counts" aria-label="Manager report outcomes">
      <span className="outcome-completed">
        <small>Completed</small>
        <strong>{completed}</strong>
      </span>
      <span className="outcome-partial">
        <small>Partial</small>
        <strong>{partial}</strong>
      </span>
      <span className="outcome-abandoned">
        <small>Abandoned</small>
        <strong>{abandoned}</strong>
      </span>
    </div>
  );
}

function Capacity({
  label,
  selected,
  maximum,
}: {
  label: string;
  selected: number;
  maximum?: number;
}) {
  return (
    <div>
      <span>{label}</span>
      <strong>
        {selected} <small>/ {maximum || '—'} max</small>
      </strong>
    </div>
  );
}

function ContractSummary({ contract }: { contract: WorkContract }) {
  return (
    <div className="contract-summary">
      <div>
        <strong>
          {contract.id} · revision {contract.version}
        </strong>
        <small>
          {contract.writeScopes.length
            ? `${contract.writeScopes.length} write scopes`
            : 'No write scope reported'}
          {' · '}
          {contract.acceptanceCriteria.length} acceptance criteria
        </small>
      </div>
      <span className={`risk-${contract.riskLevel}`}>{contract.riskLevel} risk</span>
      <span>{(contract.approvalPolicy ?? 'risk_based').replaceAll('_', ' ')} approval</span>
      <span>Priority {contract.priority}</span>
    </div>
  );
}

function CardHeading({ icon, label, tone }: { icon: ReactNode; label: string; tone: string }) {
  return (
    <header className={`card-heading tone-${tone}`}>
      {icon}
      <span>{label}</span>
    </header>
  );
}

function EmptyState({ icon, title, detail }: { icon: ReactNode; title: string; detail: string }) {
  return (
    <div className="panel-empty">
      {icon}
      <strong>{title}</strong>
      <span>{detail}</span>
    </div>
  );
}
