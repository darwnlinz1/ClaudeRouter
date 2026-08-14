import { useEffect, useMemo, useState, type KeyboardEvent } from 'react';
import type { IDockviewPanelProps } from 'dockview-react';
import {
  Activity,
  Braces,
  Brain,
  CheckCircle2,
  CircleAlert,
  Clock3,
  FileCode2,
  Gauge,
  ListChecks,
  Pin,
  Radio,
  Save,
  ScrollText,
  Settings2,
  Target,
  Wrench,
} from 'lucide-react';
import { useWorkspaceContext } from './WorkspaceContext';

type PrimaryTab = 'stream' | 'thinking' | 'diff' | 'tools';
type StreamSub = 'activity' | 'goal' | 'action' | 'status';
const PRIMARY_TABS: PrimaryTab[] = ['stream', 'thinking', 'diff', 'tools'];
const STREAM_TABS: StreamSub[] = ['activity', 'goal', 'action', 'status'];

function moveTabFocus<T extends string>(
  event: KeyboardEvent<HTMLButtonElement>,
  tabs: T[],
  index: number,
  select: (tab: T) => void,
) {
  let nextIndex: number | undefined;
  if (event.key === 'ArrowRight') nextIndex = (index + 1) % tabs.length;
  else if (event.key === 'ArrowLeft') nextIndex = (index - 1 + tabs.length) % tabs.length;
  else if (event.key === 'Home') nextIndex = 0;
  else if (event.key === 'End') nextIndex = tabs.length - 1;
  if (nextIndex == null) return;
  event.preventDefault();
  const controls =
    event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>(':scope > [role="tab"]');
  select(tabs[nextIndex]);
  controls?.[nextIndex]?.focus();
}

const time = (value?: string) => {
  if (!value) return '--:--:--';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '--:--:--' : date.toLocaleTimeString([], { hour12: false });
};

export function AgentWindow({ params }: IDockviewPanelProps<{ agentId: string }>) {
  const { agents, markFocused, configureAgent } = useWorkspaceContext();
  const agent = agents[params.agentId];
  const [tab, setTab] = useState<PrimaryTab>('stream');
  const [streamSub, setStreamSub] = useState<StreamSub>('activity');
  const [follow, setFollow] = useState(true);
  const [configOpen, setConfigOpen] = useState(false);
  const [model, setModel] = useState(agent?.model ?? 'claude-sonnet-5');
  const [effort, setEffort] = useState(agent?.effort ?? 'max');
  const [saving, setSaving] = useState(false);
  const [configError, setConfigError] = useState('');
  const [streamQuery, setStreamQuery] = useState('');

  useEffect(() => {
    markFocused(params.agentId);
  }, [markFocused, params.agentId]);

  useEffect(() => {
    if (!agent) return;
    setModel(agent.model ?? 'claude-sonnet-5');
    setEffort(agent.effort ?? 'max');
  }, [agent]);

  const diffLines = useMemo(() => (agent?.diff ?? '').split('\n'), [agent?.diff]);
  const activity = useMemo(() => {
    if (!agent) return [];
    const rows = [...agent.activity].reverse();
    const needle = streamQuery.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter(
      (entry) =>
        entry.message.toLowerCase().includes(needle) || entry.type.toLowerCase().includes(needle),
    );
  }, [agent, streamQuery]);

  if (!agent) {
    return (
      <div className="agent-missing">
        <Radio size={20} />
        <strong>Waiting for agent</strong>
        <span>{params.agentId}</span>
      </div>
    );
  }

  const shortTitle =
    agent.role === 'worker' && agent.contextFiles[0]
      ? `${agent.role.slice(0, 1).toUpperCase()}${agent.title.replace(/\D/g, '') || ''} · ${agent.contextFiles[0].split(/[/\\]/).pop()}`
      : agent.title;
  const identityMeta = `${agent.role} · ${agent.model ?? 'model n/a'} · ${agent.effort ?? 'effort n/a'}`;
  const stateLabel = (agent.executionState ?? agent.status).replaceAll('_', ' ');

  return (
    <div className={`agent-window role-border-${agent.role}`}>
      <header className="agent-summary">
        <div className={`agent-avatar role-${agent.role}`}>
          {agent.role.slice(0, 2).toUpperCase()}
        </div>
        <div className="agent-identity">
          <span className="eyebrow" title={identityMeta}>
            {identityMeta}
          </span>
          <strong title={agent.title}>{shortTitle}</strong>
        </div>
        <div
          className={`agent-state status-${agent.status}`}
          title={stateLabel}
          aria-label={`Agent status: ${stateLabel}`}
        >
          <span className="agent-state-dot" />
          <span className="agent-state-label">{stateLabel}</span>
        </div>
        {agent.unread > 0 && <em className="agent-unread">{agent.unread}</em>}
        <button
          type="button"
          className={`agent-config-button ${configOpen ? 'active' : ''}`}
          onClick={() => setConfigOpen((value) => !value)}
          title="Configure this agent's next model call"
          aria-label="Configure agent"
          aria-controls={`agent-config-${agent.id}`}
          aria-expanded={configOpen}
        >
          <Settings2 size={13} />
        </button>
      </header>

      {configOpen && (
        <div className="agent-config-panel" id={`agent-config-${agent.id}`}>
          <label>
            <span>Model</span>
            <select
              value={model}
              onChange={(event) => {
                const next = event.target.value;
                setModel(next);
                if (next === 'claude-sonnet-4-6' && effort === 'xhigh') setEffort('max');
              }}
            >
              <option value="claude-sonnet-5">Sonnet 5</option>
              <option value="claude-sonnet-4-6">Sonnet 4.6</option>
            </select>
          </label>
          <label>
            <span>Effort</span>
            <select value={effort} onChange={(event) => setEffort(event.target.value)}>
              <option value="low">Low</option>
              <option value="medium">Medium</option>
              <option value="high">High</option>
              <option value="max">Max</option>
              {model === 'claude-sonnet-5' && <option value="xhigh">X-High</option>}
            </select>
          </label>
          <button
            type="button"
            className="primary-button"
            disabled={saving}
            onClick={async () => {
              setSaving(true);
              setConfigError('');
              try {
                await configureAgent(agent.id, model, effort);
                setConfigOpen(false);
              } catch (error) {
                setConfigError(error instanceof Error ? error.message : 'Could not save override.');
              } finally {
                setSaving(false);
              }
            }}
          >
            <Save size={12} /> {saving ? 'Saving…' : 'Apply next call'}
          </button>
          {configError && <small className="agent-config-error">{configError}</small>}
        </div>
      )}

      <nav className="agent-primary-tabs" aria-label="Agent primary views" role="tablist">
        <button
          type="button"
          role="tab"
          className={tab === 'stream' ? 'active' : ''}
          aria-selected={tab === 'stream'}
          aria-controls={`agent-${agent.id}-stream-${streamSub}-panel`}
          tabIndex={tab === 'stream' ? 0 : -1}
          onClick={() => setTab('stream')}
          onKeyDown={(event) => moveTabFocus(event, PRIMARY_TABS, 0, setTab)}
          title="Stream"
        >
          <Activity size={13} /> <span>Stream</span>
        </button>
        <button
          type="button"
          role="tab"
          className={tab === 'thinking' ? 'active' : ''}
          aria-selected={tab === 'thinking'}
          aria-controls={`agent-${agent.id}-thinking-panel`}
          tabIndex={tab === 'thinking' ? 0 : -1}
          onClick={() => setTab('thinking')}
          onKeyDown={(event) => moveTabFocus(event, PRIMARY_TABS, 1, setTab)}
          title="Thinking"
        >
          <Brain size={13} /> <span>Thinking</span>
        </button>
        <button
          type="button"
          role="tab"
          className={tab === 'diff' ? 'active' : ''}
          aria-selected={tab === 'diff'}
          aria-controls={`agent-${agent.id}-diff-panel`}
          tabIndex={tab === 'diff' ? 0 : -1}
          onClick={() => setTab('diff')}
          onKeyDown={(event) => moveTabFocus(event, PRIMARY_TABS, 2, setTab)}
          title="Diff"
        >
          <ScrollText size={13} /> <span>Diff</span>
        </button>
        <button
          type="button"
          role="tab"
          className={tab === 'tools' ? 'active' : ''}
          aria-selected={tab === 'tools'}
          aria-controls={`agent-${agent.id}-tools-panel`}
          tabIndex={tab === 'tools' ? 0 : -1}
          onClick={() => setTab('tools')}
          onKeyDown={(event) => moveTabFocus(event, PRIMARY_TABS, 3, setTab)}
          title="Tools"
        >
          <Wrench size={13} /> <span>Tools</span>
        </button>
      </nav>

      {tab === 'stream' && (
        <>
          <div className="agent-stream-toolbar">
            <nav className="agent-sections compact" aria-label="Stream sections" role="tablist">
              {(
                [
                  ['activity', 'Activity', Activity],
                  ['goal', 'Goal', Target],
                  ['action', 'Action', Braces],
                  ['status', 'Status', Gauge],
                ] as const
              ).map(([id, label, Icon]) => (
                <button
                  type="button"
                  role="tab"
                  className={streamSub === id ? 'active' : ''}
                  aria-selected={streamSub === id}
                  aria-controls={`agent-${agent.id}-stream-${id}-panel`}
                  tabIndex={streamSub === id ? 0 : -1}
                  onClick={() => setStreamSub(id)}
                  onKeyDown={(event) =>
                    moveTabFocus(event, STREAM_TABS, STREAM_TABS.indexOf(id), setStreamSub)
                  }
                  key={id}
                >
                  <Icon size={12} />
                  <span>{label}</span>
                </button>
              ))}
            </nav>
            {streamSub === 'activity' && (
              <div className="agent-follow-row">
                <input
                  value={streamQuery}
                  onChange={(event) => setStreamQuery(event.target.value)}
                  placeholder="Search stream…"
                  aria-label="Search stream"
                />
                <button
                  type="button"
                  className={follow ? 'active' : ''}
                  onClick={() => setFollow((value) => !value)}
                  title="Follow live activity"
                  aria-pressed={follow}
                >
                  <Pin size={12} /> <span>Follow</span>
                </button>
              </div>
            )}
          </div>
          <div
            className={`agent-section-body ${follow && streamSub === 'activity' ? 'follow-tail' : ''}`}
            id={`agent-${agent.id}-stream-${streamSub}-panel`}
            role="tabpanel"
            aria-label={`${streamSub} stream`}
          >
            {streamSub === 'activity' && (
              <div className="activity-list">
                {activity.length ? (
                  activity.map((entry) => (
                    <div className={`activity-entry tone-${entry.tone}`} key={entry.id}>
                      <time>{time(entry.at)}</time>
                      <span className="activity-marker" />
                      <div>
                        <small>{entry.type.replaceAll('_', ' ')}</small>
                        <p>{entry.message}</p>
                      </div>
                    </div>
                  ))
                ) : (
                  <Empty label="No activity received yet." />
                )}
              </div>
            )}
            {streamSub === 'goal' && (
              <div className="prose-panel">
                <Block label="Goal" value={agent.goal} />
                <Block label="Prompt" value={agent.prompt} />
              </div>
            )}
            {streamSub === 'action' && (
              <div className="prose-panel">
                <Block label="Current action" value={agent.action} />
                <Block label="Stream output" value={agent.output} mono />
              </div>
            )}
            {streamSub === 'status' && (
              <div className="status-grid">
                <Metric label="Phase" value={agent.phase} icon={<Activity size={14} />} />
                <Metric
                  label="Execution state"
                  value={(agent.executionState ?? agent.status).replaceAll('_', ' ')}
                  icon={<Activity size={14} />}
                />
                <Metric
                  label="Model"
                  value={agent.model ?? 'not reported'}
                  icon={<Braces size={14} />}
                />
                <Metric
                  label="Effort"
                  value={agent.effort ?? 'not reported'}
                  icon={<Gauge size={14} />}
                />
                <Metric
                  label="Tokens"
                  value={agent.tokenCount.toLocaleString()}
                  icon={<Gauge size={14} />}
                />
                <Metric
                  label="Requests"
                  value={agent.requestAttempts.toLocaleString()}
                  icon={<Radio size={14} />}
                />
                <Metric
                  label="Request replays"
                  value={agent.replayCount.toLocaleString()}
                  icon={<Activity size={14} />}
                />
                <Metric
                  label="Account switches"
                  value={agent.accountSwitchCount.toLocaleString()}
                  icon={<Radio size={14} />}
                />
                <Metric label="Started" value={time(agent.startedAt)} icon={<Clock3 size={14} />} />
                <Metric
                  label="Account"
                  value={agent.account ?? 'redacted'}
                  icon={<Radio size={14} />}
                />
                {agent.previousAccount && (
                  <Metric
                    label="Previous account"
                    value={agent.previousAccount}
                    icon={<Radio size={14} />}
                  />
                )}
                {agent.logicalRequestId && (
                  <Metric
                    label="Logical request"
                    value={agent.logicalRequestId.slice(-12)}
                    icon={<Braces size={14} />}
                  />
                )}
                {agent.requestFingerprint && (
                  <Metric
                    label="Prompt fingerprint"
                    value={agent.requestFingerprint.slice(0, 12)}
                    icon={<Braces size={14} />}
                  />
                )}
                <Metric
                  label="Duration"
                  value={agent.durationMs != null ? `${agent.durationMs} ms` : 'live'}
                  icon={<Clock3 size={14} />}
                />
                {agent.error && (
                  <div className="agent-error">
                    <CircleAlert size={15} />
                    {agent.error}
                  </div>
                )}
              </div>
            )}
          </div>
        </>
      )}

      {tab === 'thinking' && (
        <div
          className="agent-section-body"
          id={`agent-${agent.id}-thinking-panel`}
          role="tabpanel"
          aria-label="Thinking"
        >
          {agent.thinking ? (
            <div className="prose-panel">
              <section>
                <span className="eyebrow">Thinking</span>
                <p className="mono">{agent.thinking}</p>
              </section>
            </div>
          ) : (
            <Empty label="No thinking stream yet." />
          )}
        </div>
      )}

      {tab === 'diff' && (
        <div
          className="agent-section-body"
          id={`agent-${agent.id}-diff-panel`}
          role="tabpanel"
          aria-label="Diff"
        >
          {agent.diff ? (
            <pre className="diff-view">
              {diffLines.map((line, index) => (
                <span
                  className={line.startsWith('+') ? 'added' : line.startsWith('-') ? 'removed' : ''}
                  key={`${index}:${line}`}
                >
                  <i>{index + 1}</i>
                  {line || ' '}
                </span>
              ))}
            </pre>
          ) : (
            <Empty label="No patch has been submitted." />
          )}
        </div>
      )}

      {tab === 'tools' && (
        <div
          className="agent-section-body tools-pane"
          id={`agent-${agent.id}-tools-panel`}
          role="tabpanel"
          aria-label="Tools"
        >
          <div className="file-list">
            <span className="eyebrow">Context / files</span>
            {agent.contextFiles.length ? (
              agent.contextFiles.map((path) => (
                <div key={path}>
                  <FileCode2 size={14} />
                  <span>{path}</span>
                </div>
              ))
            ) : (
              <Empty label="No context files disclosed." />
            )}
          </div>
          <div className="test-list">
            <span className="eyebrow">Tests</span>
            {agent.tests.length ? (
              agent.tests.map((test) => (
                <div className={`test-row status-${test.status}`} key={test.id}>
                  {test.status === 'passed' ? (
                    <CheckCircle2 size={15} />
                  ) : (
                    <CircleAlert size={15} />
                  )}
                  <div>
                    <strong>{test.name}</strong>
                    <small>{test.detail ?? test.status}</small>
                    {(test.actualIsolation || test.requestedIsolation) && (
                      <small>
                        Isolation: {test.actualIsolation ?? 'not reported'}
                        {test.requestedIsolation ? ` (requested ${test.requestedIsolation})` : ''}
                        {test.outputTruncated ? ' · output truncated' : ''}
                      </small>
                    )}
                  </div>
                  {test.durationMs != null && <time>{test.durationMs} ms</time>}
                </div>
              ))
            ) : (
              <div className="tool-card idle">
                <ListChecks size={14} />
                <span>No test evidence yet — Tester results land here.</span>
              </div>
            )}
          </div>
          {agent.workContract && (
            <section className="contract-panel">
              <div className="contract-panel-heading">
                <ScrollText size={14} />
                <div>
                  <strong>Work contract</strong>
                  <small>
                    {agent.workContract.id} · revision {agent.workContract.version} ·{' '}
                    {agent.workContract.riskLevel} risk · priority {agent.workContract.priority}
                  </small>
                </div>
              </div>
              <ContractList label="Write scopes" values={agent.workContract.writeScopes} />
              <ContractList
                label="Acceptance criteria"
                values={agent.workContract.acceptanceCriteria}
              />
              <ContractList
                label="Test requirements"
                values={agent.workContract.testRequirements}
              />
              <ContractList
                label="Evidence requirements"
                values={agent.workContract.evidenceRequirements}
              />
            </section>
          )}
          {agent.callAttempts.length > 0 && (
            <section className="attempt-list">
              <span className="eyebrow">Logical request attempts</span>
              {agent.callAttempts.length > 1 && (
                <div className="attempt-comparison">
                  <strong>Attempt comparison</strong>
                  <span>
                    {agent.callAttempts[0].account ?? 'redacted'}
                    {' → '}
                    {agent.callAttempts.at(-1)?.account ?? 'redacted'}
                  </span>
                  <span>
                    {agent.callAttempts[0].status}
                    {' → '}
                    {agent.callAttempts.at(-1)?.status}
                  </span>
                  <span>
                    Revision {agent.callAttempts[0].requestRevision ?? 1}
                    {' → '}
                    {agent.callAttempts.at(-1)?.requestRevision ?? 1}
                  </span>
                </div>
              )}
              {[...agent.callAttempts]
                .reverse()
                .slice(0, 12)
                .map((attempt) => (
                  <div className={`attempt-row status-${attempt.status}`} key={attempt.id}>
                    <Radio size={13} />
                    <div>
                      <strong>
                        Attempt {attempt.attempt}
                        {attempt.replayed ? ' · replay' : ''}
                        {attempt.requestRevision ? ` · revision ${attempt.requestRevision}` : ''}
                      </strong>
                      <small>{attempt.logicalRequestId ?? attempt.id}</small>
                    </div>
                    <span>{attempt.account ?? 'Account redacted'}</span>
                    <em>{attempt.status}</em>
                  </div>
                ))}
            </section>
          )}
          {agent.action && (
            <div className="tool-card">
              <Wrench size={14} />
              <div>
                <strong>Current tool / action</strong>
                <p>{agent.action}</p>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Empty({ label }: { label: string }) {
  return (
    <div className="section-empty">
      <Radio size={16} />
      {label}
    </div>
  );
}

function Block({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <section>
      <span className="eyebrow">{label}</span>
      <p className={mono ? 'mono' : ''}>{value || 'Not supplied by this event stream.'}</p>
    </section>
  );
}

function Metric({ label, value, icon }: { label: string; value: string; icon: React.ReactNode }) {
  return (
    <div className="metric">
      {icon}
      <span>{label}</span>
      <strong title={value}>{value}</strong>
    </div>
  );
}

function ContractList({ label, values }: { label: string; values: string[] }) {
  if (!values.length) return null;
  return (
    <div className="contract-list">
      <span>{label}</span>
      <ul>
        {values.slice(0, 8).map((value) => (
          <li key={value}>{value}</li>
        ))}
      </ul>
    </div>
  );
}
