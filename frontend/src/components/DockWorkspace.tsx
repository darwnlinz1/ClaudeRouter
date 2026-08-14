import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from 'react';
import {
  DockviewReact,
  type DockviewApi,
  type IDockviewPanelHeaderProps,
  type DockviewReadyEvent,
  type SerializedDockview,
} from 'dockview-react';
import { Focus, LayoutDashboard, PanelTopOpen, RotateCcw, X } from 'lucide-react';
import type { AgentInstance, WorkspaceState } from '../types';
import { AgentWindow } from './AgentWindow';
import { WorkspaceProvider } from './WorkspaceContext';

const LAYOUT_VERSION = 3;

export interface DockWorkspaceHandle {
  openAgent: (agent: AgentInstance) => void;
}

interface DockWorkspaceProps {
  taskId: string;
  state: WorkspaceState;
  onFocusAgent: (agentId: string) => void;
  onConfigureAgent: (agentId: string, model: string, effort: string) => Promise<void>;
}

interface StoredLayout {
  version: number;
  taskId: string;
  savedAt: string;
  layout: SerializedDockview;
}

const storageKey = (taskId: string) => `orchestrator:dock-layout:v${LAYOUT_VERSION}:${taskId}`;
const panelId = (agentId: string) => `agent:${agentId}`;

function AgentDockTab({ api }: IDockviewPanelHeaderProps) {
  const [title, setTitle] = useState(api.title);

  useEffect(() => {
    setTitle(api.title);
    const disposable = api.onDidTitleChange(() => setTitle(api.title));
    return () => disposable.dispose();
  }, [api]);

  return (
    <div className="agent-dock-tab" title={title} aria-label={title}>
      <span>{title}</span>
      <button
        type="button"
        aria-label={`Close ${title}`}
        title={`Close ${title}`}
        onPointerDown={(event) => {
          event.preventDefault();
          event.stopPropagation();
        }}
        onClick={(event) => {
          event.stopPropagation();
          api.close();
        }}
      >
        <X size={11} />
      </button>
    </div>
  );
}

export const DockWorkspace = forwardRef<DockWorkspaceHandle, DockWorkspaceProps>(
  function DockWorkspace({ taskId, state, onFocusAgent, onConfigureAgent }, ref) {
    const apiRef = useRef<DockviewApi | undefined>(undefined);
    const [focusMode, setFocusMode] = useState(false);
    const [groupMaximized, setGroupMaximized] = useState(false);

    const saveLayout = useCallback(() => {
      const api = apiRef.current;
      if (!api) return;
      const stored: StoredLayout = {
        version: LAYOUT_VERSION,
        taskId,
        savedAt: new Date().toISOString(),
        layout: api.toJSON(),
      };
      localStorage.setItem(storageKey(taskId), JSON.stringify(stored));
    }, [taskId]);

    const openAgent = useCallback(
      (agent: AgentInstance) => {
        const api = apiRef.current;
        if (!api) return;
        const id = panelId(agent.id);
        const existing = api.getPanel(id);
        if (existing) {
          existing.api.setActive();
          return;
        }

        const managerPanel =
          agent.managerId && agent.role !== 'manager'
            ? api.getPanel(panelId(agent.managerId))
            : undefined;
        const directorPanel = state.directorId
          ? api.getPanel(panelId(state.directorId))
          : undefined;
        const referencePanel = managerPanel ?? directorPanel;

        api.addPanel({
          id,
          component: 'agent',
          title: agent.title,
          params: { agentId: agent.id },
          ...(referencePanel
            ? {
                position: {
                  referencePanel,
                  direction: managerPanel ? ('within' as const) : ('right' as const),
                },
              }
            : {}),
        });
      },
      [state.directorId],
    );

    useImperativeHandle(ref, () => ({ openAgent }), [openAgent]);

    const onReady = useCallback(
      (event: DockviewReadyEvent) => {
        apiRef.current = event.api;
        const raw = localStorage.getItem(storageKey(taskId));
        if (raw) {
          try {
            const stored = JSON.parse(raw) as StoredLayout;
            if (stored.version === LAYOUT_VERSION && stored.taskId === taskId) {
              event.api.fromJSON(stored.layout);
            }
          } catch {
            localStorage.removeItem(storageKey(taskId));
          }
        }
        event.api.onDidLayoutChange(saveLayout);
      },
      [saveLayout, taskId],
    );

    useEffect(() => {
      const api = apiRef.current;
      if (!api) return;
      Object.values(state.agents).forEach((agent) => {
        const panel = api.getPanel(panelId(agent.id));
        if (panel) {
          const badge = agent.error ? ' !' : agent.unread ? ` · ${agent.unread}` : '';
          panel.api.setTitle(`${agent.title}${badge}`);
        }
      });
    }, [state.agents]);

    const resetLayout = () => {
      const api = apiRef.current;
      if (!api) return;
      localStorage.removeItem(storageKey(taskId));
      api.clear();
    };

    const toggleMaximize = () => {
      const api = apiRef.current;
      if (!api) return;
      if (api.hasMaximizedGroup()) {
        api.exitMaximizedGroup();
        setGroupMaximized(false);
      } else if (api.activePanel) {
        api.maximizeGroup(api.activePanel);
        setGroupMaximized(true);
      }
    };

    const contextValue = useMemo(
      () => ({
        agents: state.agents,
        markFocused: onFocusAgent,
        configureAgent: onConfigureAgent,
      }),
      [onConfigureAgent, onFocusAgent, state.agents],
    );

    return (
      <WorkspaceProvider value={contextValue}>
        <section className={`dock-shell ${focusMode ? 'dock-focus-mode' : ''}`}>
          <header className="dock-toolbar">
            <div>
              <span className="eyebrow">Agent workspace</span>
              <strong title={`${Object.keys(state.agents).length} live instances`}>
                {Object.keys(state.agents).length} live instances
              </strong>
            </div>
            <div className="compact-agent-picker">
              {Object.values(state.agents).map((agent) => (
                <button
                  type="button"
                  key={agent.id}
                  onClick={() => openAgent(agent)}
                  title={agent.title}
                  aria-label={`Open ${agent.title}`}
                >
                  <span className={`status-dot status-${agent.status}`} />
                  <span className="compact-agent-title">{agent.title}</span>
                </button>
              ))}
            </div>
            <div className="dock-actions" role="toolbar" aria-label="Agent workspace controls">
              <button
                type="button"
                onClick={() => {
                  const director = state.directorId ? state.agents[state.directorId] : undefined;
                  if (director) openAgent(director);
                  Object.values(state.agents)
                    .filter((agent) => agent.role === 'manager')
                    .forEach((agent) => openAgent(agent));
                }}
                title="Director + managers layout"
                aria-label="Open Director and managers layout"
              >
                <LayoutDashboard size={13} /> <span>Director + managers</span>
              </button>
              <button
                type="button"
                onClick={() => {
                  Object.values(state.agents)
                    .filter(
                      (agent) =>
                        agent.role === 'worker' ||
                        agent.role === 'tester' ||
                        agent.role === 'reviewer',
                    )
                    .forEach((agent) => openAgent(agent));
                }}
                title="Open worker/tester grid"
                aria-label="Open worker and tester grid"
              >
                <LayoutDashboard size={13} /> <span>Workers</span>
              </button>
              <button
                type="button"
                onClick={() => setFocusMode((value) => !value)}
                disabled={!apiRef.current?.activePanel}
                title={focusMode ? 'Exit full workspace focus' : 'Focus active workspace'}
                aria-label={focusMode ? 'Exit full workspace focus' : 'Focus active workspace'}
                aria-pressed={focusMode}
              >
                <Focus size={13} /> <span>{focusMode ? 'Exit focus' : 'Focus'}</span>
              </button>
              <button
                type="button"
                onClick={toggleMaximize}
                disabled={!apiRef.current?.activePanel}
                title={
                  groupMaximized
                    ? 'Restore the active dock group'
                    : 'Maximize the active dock group'
                }
                aria-label={
                  groupMaximized
                    ? 'Restore the active dock group'
                    : 'Maximize the active dock group'
                }
                aria-pressed={groupMaximized}
              >
                <PanelTopOpen size={13} /> <span>{groupMaximized ? 'Restore' : 'Maximize'}</span>
              </button>
              <button
                type="button"
                onClick={resetLayout}
                title="Restore the default agent layout"
                aria-label="Restore the default agent layout"
              >
                <RotateCcw size={13} /> <span>Reset</span>
              </button>
            </div>
          </header>
          <div className="dock-host">
            <DockviewReact
              key={taskId}
              className="dockview-theme-abyss"
              components={{ agent: AgentWindow }}
              defaultTabComponent={AgentDockTab}
              onReady={onReady}
              watermarkComponent={() => (
                <div className="dock-watermark">
                  <LayoutDashboard size={24} />
                  <strong>Agent workspace</strong>
                  <span>Select an instance from the execution graph.</span>
                </div>
              )}
            />
          </div>
        </section>
      </WorkspaceProvider>
    );
  },
);
