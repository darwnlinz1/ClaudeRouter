import { useMemo, useState } from 'react';
import { Radio, X } from 'lucide-react';
import type { WorkspaceState } from '../types';

interface EventConsoleProps {
  state: Pick<WorkspaceState, 'agents' | 'signals' | 'eventCount'>;
  open: boolean;
  onClose: () => void;
  onOpenAgent?: (id: string) => void;
}

type ConsoleRow = {
  id: string;
  at: string;
  source: string;
  message: string;
  tone: 'signal' | 'activity';
  agentId?: string;
};

const time = (value: string) => {
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? '--:--:--'
    : date.toLocaleTimeString([], { hour12: false });
};

export function EventConsole({ state, open, onClose, onOpenAgent }: EventConsoleProps) {
  const [filter, setFilter] = useState('');

  const rows = useMemo(() => {
    const items: ConsoleRow[] = state.signals.map((signal) => ({
      id: `signal:${signal.id}`,
      at: signal.timestamp,
      source: signal.signalType,
      message: signal.summary,
      tone: 'signal',
      agentId: signal.sourceAgentId,
    }));

    Object.values(state.agents).forEach((agent) => {
      agent.activity.forEach((entry) => {
        items.push({
          id: `activity:${entry.id}`,
          at: entry.at,
          source: agent.title,
          message: entry.message,
          tone: 'activity',
          agentId: agent.id,
        });
      });
    });

    items.sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime());
    return items.slice(0, 80);
  }, [state.agents, state.signals]);

  const filtered = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter(
      (row) =>
        row.source.toLowerCase().includes(needle) ||
        row.message.toLowerCase().includes(needle),
    );
  }, [filter, rows]);

  if (!open) return null;

  return (
    <aside className="event-console" aria-label="Event console">
      <header>
        <div>
          <Radio size={14} />
          <strong>Event console</strong>
          <span>{state.eventCount} total</span>
        </div>
        <button type="button" aria-label="Close console" onClick={onClose}>
          <X size={14} />
        </button>
      </header>
      <div className="event-console-filter">
        <input
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
          placeholder="Filter signals and activity…"
          aria-label="Filter events"
        />
      </div>
      <ul className="event-console-list">
        {filtered.map((row) => (
          <li key={row.id} className={`event-console-row tone-${row.tone}`}>
            <time>{time(row.at)}</time>
            <button
              type="button"
              className="event-console-source"
              disabled={!row.agentId || !onOpenAgent}
              onClick={() => row.agentId && onOpenAgent?.(row.agentId)}
            >
              {row.source}
            </button>
            <span>{row.message}</span>
          </li>
        ))}
        {!filtered.length && <li className="event-console-empty">No matching events</li>}
      </ul>
    </aside>
  );
}
