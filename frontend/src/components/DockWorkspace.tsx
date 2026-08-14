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
import { Focus, Grid2X2, LayoutDashboard, PanelTopOpen, RotateCcw, X } from 'lucide-react';
import type { AgentInstance, AgentRole, WorkspaceState } from '../types';
import { AgentWindow } from './AgentWindow';
import { WorkspaceProvider } from './WorkspaceContext';

const LAYOUT_VERSION = 3;

const ROLE_ORDER: AgentRole[] = [
  'director',
  'manager',
  'supervisor',
  'worker',
  'tester',
  'reviewer',
  'agent',
];

const ROLE_LABEL: Record<AgentRole, string> = {
  director: 'Director',
  manager: 'Managers',
  supervisor: 'Supervisors',
  worker: 'Coders',
  tester: 'Testers',
  reviewer: 'Reviewers',
  agent: 'Other agents',
};

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
    const autoOpenedTaskRef = useRef<string | null>(null);
    const [focusMode, setFocusMode] = useState(false);
    const [groupMaximized, setGroupMaximized] = useState(false);
    const [dockReady, setDockReady] = useState(false);

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
          title: agent.label ?? agent.title,
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
        setDockReady(true);
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
          panel.api.setTitle(`${agent.label ?? agent.title}${badge}`);
        }
      });
    }, [state.agents]);

    useEffect(() => {
      const api = apiRef.current;
      if (!api || autoOpenedTaskRef.current === taskId) return;
      const agents = Object.values(state.agents);
      if (!agents.length) return;
      if (agents.some((agent) => api.getPanel(panelId(agent.id)))) {
        autoOpenedTaskRef.current = taskId;
        return;
      }
      const initial =
        (state.directorId ? state.agents[state.directorId] : undefined) ??
        agents.find((agent) => agent.role === 'manager') ??
        agents[0];
      if (initial) {
        openAgent(initial);
        autoOpenedTaskRef.current = taskId;
      }
    }, [dockReady, openAgent, state.agents, state.directorId, taskId]);

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

    const agentsByRole = useMemo(() => {
      const groups = new Map<AgentRole, AgentInstance[]>();
      for (const agent of Object.values(state.agents)) {
        const role = (ROLE_ORDER.includes(agent.role) ? agent.role : 'agent') as AgentRole;
        const bucket = groups.get(role) ?? [];
        bucket.push(agent);
        groups.set(role, bucket);
      }
      return groups;
    }, [state.agents]);

    const agentCount = Object.keys(state.agents).length;

    const openDirectorAndManagers = useCallback(() => {
      const director = state.directorId ? state.agents[state.directorId] : undefined;
      if (director) openAgent(director);
      Object.values(state.agents)
        .filter((agent) => agent.role === 'manager')
        .forEach((agent) => openAgent(agent));
    }, [openAgent, state.agents, state.directorId]);

    const openWorkers = useCallback(() => {
      Object.values(state.agents)
        .filter(
          (agent) =>
            agent.role === 'worker' || agent.role === 'tester' || agent.role === 'reviewer',
        )
        .forEach((agent) => openAgent(agent));
    }, [openAgent, state.agents]);

    return (
      <WorkspaceProvider value={contextValue}>
        <section className={`dock-shell ${focusMode ? 'dock-focus-mode' : ''}`}>
          <header className="dock-toolbar">
            <div>
              <span className="eyebrow">Agent workspace</span>
              <strong title={`${agentCount} agent instances in this run`}>
                {agentCount} instance{agentCount === 1 ? '' : 's'}
              </strong>
            </div>
            <div className="compact-agent-picker">
              <select
                aria-label="Open an agent window"
                value=""
                onChange={(event) => {
                  const agent = state.agents[event.target.value];
                  if (agent) openAgent(agent);
                }}
              >
                <option value="">Open an agent…</option>
                {ROLE_ORDER.map((role) => {
                  const group = agentsByRole.get(role);
                  if (!group?.length) return null;
                  return (
                    <optgroup key={role} label={ROLE_LABEL[role]}>
                      {group.map((agent) => (
                        <option key={agent.id} value={agent.id}>
                          {agent.label ? `${agent.label} · ${agent.title}` : agent.title}
                          {agent.status === 'running' ? ' · running' : ''}
                          {agent.unread ? ` · ${agent.unread} new` : ''}
                        </option>
                      ))}
                    </optgroup>
                  );
                })}
              </select>
            </div>
            <div className="dock-actions" role="toolbar" aria-label="Agent workspace controls">
              <button
                type="button"
                onClick={openDirectorAndManagers}
                title="Open Director and managers layout"
                aria-label="Open Director and managers layout"
              >
                <LayoutDashboard size={15} />
              </button>
              <button
                type="button"
                onClick={openWorkers}
                title="Open the worker and tester grid"
                aria-label="Open worker and tester grid"
              >
                <Grid2X2 size={15} />
              </button>
              <button
                type="button"
                onClick={() => setFocusMode((value) => !value)}
                disabled={!apiRef.current?.activePanel}
                title={focusMode ? 'Exit full workspace focus' : 'Focus active workspace'}
                aria-label={focusMode ? 'Exit full workspace focus' : 'Focus active workspace'}
                aria-pressed={focusMode}
              >
                <Focus size={15} />
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
                <PanelTopOpen size={15} />
              </button>
              <button
                type="button"
                onClick={resetLayout}
                title="Restore the default agent layout"
                aria-label="Restore the default agent layout"
              >
                <RotateCcw size={15} />
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
                  <strong>No agent window open</strong>
                  <span>
                    {agentCount
                      ? 'Pick an instance from the picker above, or click a node in the execution graph.'
                      : 'Agent windows appear as soon as Director starts the first instance.'}
                  </span>
                  {agentCount > 0 && (
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={openDirectorAndManagers}
                    >
                      <LayoutDashboard size={14} /> Open Director + managers
                    </button>
                  )}
                </div>
              )}
            />
          </div>
        </section>
      </WorkspaceProvider>
    );
  },
);
