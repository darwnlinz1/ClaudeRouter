import { useMemo, useState } from 'react';
import {
  ChevronDown,
  ChevronUp,
  GripHorizontal,
  Maximize2,
  Minimize2,
  Radio,
  X,
} from 'lucide-react';
import type { WorkspaceState } from '../types';

interface EventConsoleProps {
  state: Pick<WorkspaceState, 'agents' | 'signals' | 'eventCount'>;
  open: boolean;
  onClose: () => void;
  collapsed?: boolean;
  height?: number;
  onToggleCollapsed?: () => void;
  onResize?: (height: number) => void;
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
  return Number.isNaN(date.getTime()) ? '--:--:--' : date.toLocaleTimeString([], { hour12: false });
};

export function EventConsole({
  state,
  open,
  onClose,
  collapsed = false,
  height = 132,
  onToggleCollapsed,
  onResize,
  onOpenAgent,
}: EventConsoleProps) {
  const [filter, setFilter] = useState('');
  const [maximized, setMaximized] = useState(false);

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
        row.source.toLowerCase().includes(needle) || row.message.toLowerCase().includes(needle),
    );
  }, [filter, rows]);

  if (!open) return null;

  return (
    <aside
      className={`event-console ${collapsed ? 'collapsed' : ''} ${maximized ? 'maximized' : ''}`}
      aria-label="Event console"
    >
      {!collapsed && !maximized && (
        <button
          type="button"
          className="event-console-resize"
          role="separator"
          aria-label="Resize event console"
          aria-orientation="horizontal"
          aria-valuemin={72}
          aria-valuemax={480}
          aria-valuenow={height}
          onKeyDown={(event) => {
            let next = height;
            if (event.key === 'ArrowUp') next += 10;
            else if (event.key === 'ArrowDown') next -= 10;
            else if (event.key === 'Home') next = 72;
            else if (event.key === 'End') next = 480;
            else return;
            event.preventDefault();
            onResize?.(Math.min(480, Math.max(72, next)));
          }}
          onPointerDown={(event) => {
            event.preventDefault();
            const move = (pointer: PointerEvent) => {
              const max = Math.min(480, window.innerHeight * 0.55);
              onResize?.(
                Math.round(Math.min(max, Math.max(72, window.innerHeight - pointer.clientY))),
              );
            };
            const stop = () => {
              window.removeEventListener('pointermove', move);
              window.removeEventListener('pointerup', stop);
            };
            window.addEventListener('pointermove', move);
            window.addEventListener('pointerup', stop, { once: true });
          }}
        >
          <GripHorizontal size={13} />
        </button>
      )}
      <header>
        <div>
          <Radio size={14} />
          <strong>Event console</strong>
          <span>{state.eventCount} total</span>
        </div>
        <div className="event-console-actions">
          <button
            type="button"
            aria-label={collapsed ? 'Expand console' : 'Collapse console'}
            title={collapsed ? 'Expand console' : 'Collapse console'}
            aria-expanded={!collapsed}
            onClick={onToggleCollapsed}
          >
            {collapsed ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          </button>
          {!collapsed && (
            <button
              type="button"
              aria-label={maximized ? 'Restore console' : 'Maximize console'}
              title={maximized ? 'Restore console' : 'Maximize console'}
              aria-pressed={maximized}
              onClick={() => setMaximized((value) => !value)}
            >
              {maximized ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
            </button>
          )}
          <button type="button" aria-label="Close console" title="Close console" onClick={onClose}>
            <X size={14} />
          </button>
        </div>
      </header>
      {!collapsed && (
        <>
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
                  title={row.source}
                >
                  {row.source}
                </button>
                <span>{row.message}</span>
              </li>
            ))}
            {!filtered.length && <li className="event-console-empty">No matching events</li>}
          </ul>
        </>
      )}
    </aside>
  );
}
