import { useEffect, useMemo, useState } from 'react';
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

const time = (value?: string) => {
  if (!value) return '--:--:--';
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? '--:--:--'
    : date.toLocaleTimeString([], { hour12: false });
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
  }, [agent?.effort, agent?.model]);

  const diffLines = useMemo(() => (agent?.diff ?? '').split('\n'), [agent?.diff]);
  const activity = useMemo(() => {
    if (!agent) return [];
    const rows = [...agent.activity].reverse();
    const needle = streamQuery.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter(
      (entry) =>
        entry.message.toLowerCase().includes(needle) ||
        entry.type.toLowerCase().includes(needle),
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

  return (
    <div className={`agent-window role-border-${agent.role}`}>
      <header className="agent-summary">
        <div className={`agent-avatar role-${agent.role}`}>
          {agent.role.slice(0, 2).toUpperCase()}
        </div>
        <div className="agent-identity">
          <span className="eyebrow">{agent.role} · {agent.model ?? 'model n/a'} · {agent.effort ?? 'effort n/a'}</span>
          <strong title={agent.title}>{shortTitle}</strong>
        </div>
        <div className={`agent-state status-${agent.status}`}>
          <span />
          {agent.status}
        </div>
        {agent.unread > 0 && <em className="agent-unread">{agent.unread}</em>}
        <button
          type="button"
          className={`agent-config-button ${configOpen ? 'active' : ''}`}
          onClick={() => setConfigOpen((value) => !value)}
          title="Configure this agent's next model call"
        >
          <Settings2 size={13} />
        </button>
      </header>

      {configOpen && (
        <div className="agent-config-panel">
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

      <nav className="agent-primary-tabs" aria-label="Agent primary views">
        <button type="button" className={tab === 'stream' ? 'active' : ''} onClick={() => setTab('stream')}>
          <Activity size={13} /> Stream
        </button>
        <button type="button" className={tab === 'thinking' ? 'active' : ''} onClick={() => setTab('thinking')}>
          <Brain size={13} /> Thinking
        </button>
        <button type="button" className={tab === 'diff' ? 'active' : ''} onClick={() => setTab('diff')}>
          <ScrollText size={13} /> Diff
        </button>
        <button type="button" className={tab === 'tools' ? 'active' : ''} onClick={() => setTab('tools')}>
          <Wrench size={13} /> Tools
        </button>
      </nav>

      {tab === 'stream' && (
        <>
          <div className="agent-stream-toolbar">
            <nav className="agent-sections compact" aria-label="Stream sections">
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
                  className={streamSub === id ? 'active' : ''}
                  onClick={() => setStreamSub(id)}
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
                >
                  <Pin size={12} /> Follow
                </button>
              </div>
            )}
          </div>
          <div className={`agent-section-body ${follow && streamSub === 'activity' ? 'follow-tail' : ''}`}>
            {streamSub === 'activity' && (
              <div className="activity-list">
                {activity.length ? activity.map((entry) => (
                  <div className={`activity-entry tone-${entry.tone}`} key={entry.id}>
                    <time>{time(entry.at)}</time>
                    <span className="activity-marker" />
                    <div>
                      <small>{entry.type.replaceAll('_', ' ')}</small>
                      <p>{entry.message}</p>
                    </div>
                  </div>
                )) : <Empty label="No activity received yet." />}
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
                <Metric label="Model" value={agent.model ?? 'not reported'} icon={<Braces size={14} />} />
                <Metric label="Effort" value={agent.effort ?? 'not reported'} icon={<Gauge size={14} />} />
                <Metric label="Tokens" value={agent.tokenCount.toLocaleString()} icon={<Gauge size={14} />} />
                <Metric label="Started" value={time(agent.startedAt)} icon={<Clock3 size={14} />} />
                <Metric label="Account" value={agent.account ?? 'redacted'} icon={<Radio size={14} />} />
                <Metric
                  label="Duration"
                  value={agent.durationMs != null ? `${agent.durationMs} ms` : 'live'}
                  icon={<Clock3 size={14} />}
                />
                {agent.error && <div className="agent-error"><CircleAlert size={15} />{agent.error}</div>}
              </div>
            )}
          </div>
        </>
      )}

      {tab === 'thinking' && (
        <div className="agent-section-body">
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
        <div className="agent-section-body">
          {agent.diff ? (
            <pre className="diff-view">{diffLines.map((line, index) => (
              <span
                className={line.startsWith('+') ? 'added' : line.startsWith('-') ? 'removed' : ''}
                key={`${index}:${line}`}
              >
                <i>{index + 1}</i>{line || ' '}
              </span>
            ))}</pre>
          ) : <Empty label="No patch has been submitted." />}
        </div>
      )}

      {tab === 'tools' && (
        <div className="agent-section-body tools-pane">
          <div className="file-list">
            <span className="eyebrow">Context / files</span>
            {agent.contextFiles.length ? agent.contextFiles.map((path) => (
              <div key={path}><FileCode2 size={14} /><span>{path}</span></div>
            )) : <Empty label="No context files disclosed." />}
          </div>
          <div className="test-list">
            <span className="eyebrow">Tests</span>
            {agent.tests.length ? agent.tests.map((test) => (
              <div className={`test-row status-${test.status}`} key={test.id}>
                {test.status === 'passed'
                  ? <CheckCircle2 size={15} />
                  : <CircleAlert size={15} />}
                <div><strong>{test.name}</strong><small>{test.detail ?? test.status}</small></div>
                {test.durationMs != null && <time>{test.durationMs} ms</time>}
              </div>
            )) : (
              <div className="tool-card idle">
                <ListChecks size={14} />
                <span>No test evidence yet — Tester results land here.</span>
              </div>
            )}
          </div>
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
  return <div className="section-empty"><Radio size={16} />{label}</div>;
}

function Block({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <section>
      <span className="eyebrow">{label}</span>
      <p className={mono ? 'mono' : ''}>{value || 'Not supplied by this event stream.'}</p>
    </section>
  );
}

function Metric({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon: React.ReactNode;
}) {
  return <div className="metric">{icon}<span>{label}</span><strong>{value}</strong></div>;
}
