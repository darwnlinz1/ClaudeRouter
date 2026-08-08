import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  Bot,
  Boxes,
  ChevronLeft,
  ChevronRight,
  CircleStop,
  CloudOff,
  Command,
  Copy,
  FolderOpen,
  FolderGit2,
  Gauge,
  KeyRound,
  Layers3,
  MoreHorizontal,
  Pin,
  Play,
  Plus,
  RefreshCw,
  Search,
  Settings2,
  SlidersHorizontal,
  Sparkles,
  Terminal,
  Timer,
  Trash2,
  X,
} from 'lucide-react';
import { api } from './api/client';
import { CommandPalette, type CommandItem } from './components/CommandPalette';
import { DagOverview } from './components/DagOverview';
import { DockWorkspace, type DockWorkspaceHandle } from './components/DockWorkspace';
import { EventConsole } from './components/EventConsole';
import { HierarchyPreview } from './components/HierarchyPreview';
import { InterventionBanner } from './components/InterventionBanner';
import { PhaseStepper } from './components/PhaseStepper';
import { ToastStack } from './components/ToastStack';
import { mockTask } from './data/mockWorkspace';
import { useWorkspace } from './hooks/useWorkspace';
import { isActiveStatus, statusTone } from './lib/phases';
import { loadPrefs, savePrefs, type UiPrefs } from './lib/preferences';
import { shortTaskTitle, taskProjectKey } from './lib/taskTitle';
import type {
  AgentInstance,
  AgentProfile,
  ConfigurableRole,
  TaskSummary,
  WorkspaceSettings,
} from './types';

const SETTINGS_KEY = 'orchestrator:workspace-settings:v2';
const DEFAULT_SETTINGS: WorkspaceSettings = {
  maxParallelManagers: 4,
  maxWorkersPerManager: 5,
  maxParallelWorkers: 8,
  mockWhenUnavailable: false,
  roleProfiles: {
    director: { model: 'claude-sonnet-5', effort: 'max' },
    manager: { model: 'claude-sonnet-5', effort: 'max' },
    worker: { model: 'claude-sonnet-5', effort: 'max' },
    tester: { model: 'claude-sonnet-5', effort: 'high' },
  },
};

const TEMPLATES = [
  {
    id: 'bugfix',
    label: 'Bugfix',
    name: 'Bugfix',
    goal: 'Investigate and fix the reported bug. Keep the change minimal, add or update tests, and verify the failing case passes.',
  },
  {
    id: 'feature',
    label: 'Feature slice',
    name: 'Feature slice',
    goal: 'Implement one vertical feature slice end-to-end with clear module boundaries, tests, and a short summary of files touched.',
  },
  {
    id: 'refactor',
    label: 'Refactor + tests',
    name: 'Refactor + tests',
    goal: 'Refactor the targeted area for clarity and safety without behavior changes. Strengthen unit/integration coverage around the risk surface.',
  },
  {
    id: 'greenfield',
    label: 'Greenfield',
    name: 'Greenfield scaffold',
    goal: 'Scaffold a new project structure with baseline config, entrypoints, and a first passing test harness.',
  },
] as const;

type Toast = { id: string; message: string; tone?: 'info' | 'success' | 'error' };

const loadSettings = (): WorkspaceSettings => {
  try {
    const stored = JSON.parse(localStorage.getItem(SETTINGS_KEY) ?? '{}') as Partial<WorkspaceSettings>;
    return {
      ...DEFAULT_SETTINGS,
      ...stored,
      roleProfiles: {
        ...DEFAULT_SETTINGS.roleProfiles,
        ...(stored.roleProfiles ?? {}),
      },
    };
  } catch {
    return DEFAULT_SETTINGS;
  }
};

const relativeTime = (value?: string) => {
  if (!value) return 'just now';
  const delta = Math.max(0, Date.now() - new Date(value).getTime());
  const minutes = Math.floor(delta / 60_000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  if (minutes < 1_440) return `${Math.floor(minutes / 60)}h ago`;
  return `${Math.floor(minutes / 1_440)}d ago`;
};

const formatElapsed = (startedAt?: string | null, finishedAt?: string | null, now = Date.now()) => {
  if (!startedAt) return '—';
  const start = new Date(startedAt).getTime();
  if (Number.isNaN(start)) return '—';
  const endRaw = finishedAt ? new Date(finishedAt).getTime() : now;
  const end = Number.isNaN(endRaw) ? now : endRaw;
  const totalSec = Math.max(0, Math.floor((end - start) / 1000));
  const hours = Math.floor(totalSec / 3600);
  const minutes = Math.floor((totalSec % 3600) / 60);
  const seconds = totalSec % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }
  return `${minutes}:${String(seconds).padStart(2, '0')}`;
};

const matchesRailFilter = (task: TaskSummary, filter: UiPrefs['railFilter']) => {
  const status = task.status.toUpperCase();
  if (filter === 'all') return true;
  if (filter === 'active') return isActiveStatus(status);
  if (filter === 'failed') return status === 'FAILED' || status === 'ERROR';
  return ['COMPLETED', 'DONE', 'STOPPED', 'CANCELLED'].includes(status);
};

export default function App() {
  const [tasks, setTasks] = useState<TaskSummary[]>([]);
  const [selectedTask, setSelectedTask] = useState<TaskSummary>();
  const [settings, setSettings] = useState(loadSettings);
  const [prefs, setPrefs] = useState(loadPrefs);
  const [railCollapsed, setRailCollapsed] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);
  const [offline, setOffline] = useState(false);
  const [menuTaskId, setMenuTaskId] = useState<string | null>(null);
  const [busyAction, setBusyAction] = useState(false);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [graphExpanded, setGraphExpanded] = useState(false);
  const dockRef = useRef<DockWorkspaceHandle>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const { state, dispatch } = useWorkspace(selectedTask);

  const pushToast = useCallback((message: string, tone: Toast['tone'] = 'info') => {
    const id = `${Date.now()}:${Math.random().toString(16).slice(2)}`;
    setToasts((current) => [...current.slice(-4), { id, message, tone }]);
  }, []);

  const handleFocusAgent = useCallback(
    (agentId: string) => dispatch({ type: 'focus-agent', agentId }),
    [],
  );
  const handleConfigureAgent = useCallback(
    async (agentId: string, model: string, effort: string) => {
      if (!selectedTask) throw new Error('No active task.');
      await api.updateAgentConfig(selectedTask.id, agentId, model, effort);
      dispatch({ type: 'agent-configured', agentId, model, effort });
      pushToast('Agent override saved for next call', 'success');
    },
    [pushToast, selectedTask?.id],
  );

  const updatePrefs = useCallback((partial: Partial<UiPrefs>) => {
    setPrefs(savePrefs(partial));
  }, []);

  const refreshTasks = useCallback(async (preserveSelection = true) => {
    try {
      const next = await api.listTasks();
      setOffline(false);
      setTasks(next);
      setSelectedTask((current) => {
        if (preserveSelection && current) {
          return next.find((task) => task.id === current.id);
        }
        return next[0];
      });
    } catch (error) {
      setOffline(true);
      if (!preserveSelection) {
        pushToast(
          error instanceof Error ? error.message : 'The orchestrator API is unavailable.',
          'error',
        );
      }
      if (settings.mockWhenUnavailable) {
        setTasks([mockTask]);
        setSelectedTask((current) => current ?? mockTask);
      } else {
        setTasks([]);
        setSelectedTask((current) =>
          current?.id === mockTask.id ? undefined : current,
        );
      }
    } finally {
      setLoading(false);
    }
  }, [pushToast, settings.mockWhenUnavailable]);

  useEffect(() => {
    if (!toasts.length) return undefined;
    const timer = window.setTimeout(() => {
      setToasts((current) => current.slice(1));
    }, 3200);
    return () => window.clearTimeout(timer);
  }, [toasts]);

  useEffect(() => {
    void refreshTasks(false);
    const timer = window.setInterval(() => void refreshTasks(true), 12_000);
    return () => window.clearInterval(timer);
  }, [refreshTasks]);

  useEffect(() => {
    localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
  }, [settings]);

  useEffect(() => {
    if (!settingsOpen) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setSettingsOpen(false);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [settingsOpen]);

  useEffect(() => {
    if (!menuTaskId) return undefined;
    const onPointerDown = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuTaskId(null);
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setMenuTaskId(null);
    };
    window.addEventListener('mousedown', onPointerDown);
    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('mousedown', onPointerDown);
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [menuTaskId]);

  const openAgent = useCallback((agent: AgentInstance) => {
    setGraphExpanded(false);
    dockRef.current?.openAgent(agent);
  }, []);

  const displayedTask = state.task ?? selectedTask;
  const realAgents = Object.values(state.agents).filter(
    (agent) => !agent.id.endsWith(':root') && !agent.id.endsWith(':anon'),
  );
  const runningAgents = realAgents.filter(
    (agent) => agent.status === 'running' || agent.status === 'waiting',
  ).length;
  const agentCount = realAgents.length;
  const changedFiles = Object.keys(displayedTask?.changed_files ?? {}).length;
  const taskIsActive = displayedTask ? isActiveStatus(displayedTask.status) : false;
  const waitingInput = displayedTask?.status?.toUpperCase() === 'WAITING_INPUT';
  const accountsUsed = useMemo(() => {
    const accounts = new Set(state.usedAccounts);
    for (const agent of Object.values(state.agents)) {
      const account = agent.account?.trim();
      if (account && account.toLowerCase() !== 'redacted') accounts.add(account);
    }
    return accounts.size;
  }, [state.agents, state.usedAccounts]);
  const [elapsedNow, setElapsedNow] = useState(() => Date.now());
  useEffect(() => {
    if (!taskIsActive) return;
    setElapsedNow(Date.now());
    const timer = window.setInterval(() => setElapsedNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [taskIsActive, displayedTask?.id]);
  const totalElapsed = formatElapsed(
    displayedTask?.started_at ?? displayedTask?.created_at,
    displayedTask?.finished_at,
    elapsedNow,
  );

  const filteredTasks = useMemo(() => {
    const needle = search.trim().toLowerCase();
    const pinned = new Set(prefs.pinnedTaskIds);
    return tasks
      .filter((task) => matchesRailFilter(task, prefs.railFilter))
      .filter((task) => {
        if (!needle) return true;
        const title = shortTaskTitle(task).toLowerCase();
        return (
          title.includes(needle) ||
          task.name.toLowerCase().includes(needle) ||
          task.status.toLowerCase().includes(needle) ||
          (task.root ?? '').toLowerCase().includes(needle)
        );
      })
      .sort((a, b) => Number(pinned.has(b.id)) - Number(pinned.has(a.id)));
  }, [prefs.pinnedTaskIds, prefs.railFilter, search, tasks]);

  const groupedTasks = useMemo(() => {
    const groups = new Map<string, TaskSummary[]>();
    for (const task of filteredTasks) {
      const key = taskProjectKey(task.root);
      const bucket = groups.get(key) ?? [];
      bucket.push(task);
      groups.set(key, bucket);
    }
    return [...groups.entries()];
  }, [filteredTasks]);

  const runTaskAction = async (task: TaskSummary) => {
    if (busyAction || task.id === mockTask.id) return;
    if (isActiveStatus(task.status)) {
      if (!window.confirm(`Stop task "${shortTaskTitle(task)}"? Agents will halt after the current step.`)) {
        return;
      }
    }
    setBusyAction(true);
    setMenuTaskId(null);
    try {
      if (isActiveStatus(task.status)) {
        await api.stopTask(task.id);
        pushToast('Stop requested', 'info');
      } else {
        await api.resumeTask(task.id);
        pushToast('Resume requested', 'success');
      }
      await refreshTasks(true);
    } catch (error) {
      pushToast(error instanceof Error ? error.message : 'Action failed', 'error');
    } finally {
      setBusyAction(false);
    }
  };

  const deleteTask = async (task: TaskSummary) => {
    if (busyAction || task.id === mockTask.id) return;
    if (isActiveStatus(task.status)) {
      pushToast('Stop the task before deleting', 'error');
      return;
    }
    const label = shortTaskTitle(task);
    const typed = window.prompt(`Type DELETE to permanently remove "${label}"`);
    if (typed !== 'DELETE') return;
    setBusyAction(true);
    setMenuTaskId(null);
    try {
      await api.deleteTask(task.id);
      if (selectedTask?.id === task.id) setSelectedTask(undefined);
      updatePrefs({ pinnedTaskIds: prefs.pinnedTaskIds.filter((id) => id !== task.id) });
      pushToast('Task deleted', 'success');
      await refreshTasks(false);
    } catch (error) {
      pushToast(error instanceof Error ? error.message : 'Delete failed', 'error');
    } finally {
      setBusyAction(false);
    }
  };

  const copyTaskId = async (taskId: string) => {
    setMenuTaskId(null);
    try {
      await navigator.clipboard.writeText(taskId);
      pushToast('Task ID copied', 'success');
    } catch {
      window.prompt('Copy task ID', taskId);
    }
  };

  const togglePin = (taskId: string) => {
    const pinned = prefs.pinnedTaskIds.includes(taskId)
      ? prefs.pinnedTaskIds.filter((id) => id !== taskId)
      : [...prefs.pinnedTaskIds, taskId];
    updatePrefs({ pinnedTaskIds: pinned });
  };

  const focusDirector = () => {
    if (!state.directorId) return;
    const director = state.agents[state.directorId];
    if (director) openAgent(director);
  };

  const commands: CommandItem[] = useMemo(() => {
    const agentCommands = Object.values(state.agents)
      .slice(0, 12)
      .map((agent, index) => ({
        id: `agent:${agent.id}`,
        label: `Open ${agent.title}`,
        hint: agent.role,
        shortcut: index < 9 ? String(index + 1) : undefined,
        run: () => openAgent(agent),
      }));
    return [
      {
        id: 'new',
        label: 'New run',
        shortcut: 'N',
        run: () => setCreateOpen(true),
      },
      {
        id: 'palette-refresh',
        label: 'Refresh tasks',
        run: () => void refreshTasks(true),
      },
      {
        id: 'stop',
        label: 'Stop selected task',
        shortcut: 'S',
        disabled: !displayedTask || !taskIsActive || displayedTask.id === mockTask.id,
        run: () => displayedTask && void runTaskAction(displayedTask),
      },
      {
        id: 'resume',
        label: 'Resume selected task',
        shortcut: 'R',
        disabled: !displayedTask || taskIsActive || displayedTask.id === mockTask.id,
        run: () => displayedTask && void runTaskAction(displayedTask),
      },
      {
        id: 'director',
        label: 'Focus Director',
        disabled: !state.directorId,
        run: focusDirector,
      },
      {
        id: 'console',
        label: prefs.consoleOpen ? 'Hide event console' : 'Show event console',
        run: () => updatePrefs({ consoleOpen: !prefs.consoleOpen }),
      },
      {
        id: 'settings',
        label: 'Open settings',
        run: () => setSettingsOpen(true),
      },
      {
        id: 'density',
        label: `Density: ${prefs.density === 'compact' ? 'comfortable' : 'compact'}`,
        run: () =>
          updatePrefs({ density: prefs.density === 'compact' ? 'comfortable' : 'compact' }),
      },
      ...agentCommands,
    ];
  }, [
    displayedTask,
    focusDirector,
    openAgent,
    prefs.consoleOpen,
    prefs.density,
    refreshTasks,
    state.agents,
    state.directorId,
    taskIsActive,
    updatePrefs,
  ]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const typing =
        target &&
        (target.tagName === 'INPUT' ||
          target.tagName === 'TEXTAREA' ||
          target.tagName === 'SELECT' ||
          target.isContentEditable);
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setPaletteOpen(true);
        return;
      }
      if (typing || paletteOpen) return;
      if (event.key.toLowerCase() === 'n') {
        event.preventDefault();
        setCreateOpen(true);
      } else if (event.key.toLowerCase() === 's' && displayedTask && taskIsActive) {
        event.preventDefault();
        void runTaskAction(displayedTask);
      } else if (event.key.toLowerCase() === 'r' && displayedTask && !taskIsActive) {
        event.preventDefault();
        void runTaskAction(displayedTask);
      } else if (/^[1-9]$/.test(event.key)) {
        const agent = Object.values(state.agents)[Number(event.key) - 1];
        if (agent) openAgent(agent);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [displayedTask, openAgent, paletteOpen, state.agents, taskIsActive]);

  const profileSummary = `${settings.roleProfiles.worker.model.split('-').slice(-1)[0]}·${settings.roleProfiles.worker.effort}`;

  return (
    <div
      className={`app-shell density-${prefs.density} ${railCollapsed ? 'rail-collapsed' : ''} ${prefs.consoleOpen ? 'console-open' : ''}`}
    >
      <aside className="task-rail">
        <div className="brand">
          <div className="brand-mark"><Command size={17} /></div>
          <div><strong>ORCHESTRATOR</strong><span>MISSION CONTROL</span></div>
          <button type="button" onClick={() => setRailCollapsed((value) => !value)}>
            {railCollapsed ? <ChevronRight size={15} /> : <ChevronLeft size={15} />}
          </button>
        </div>

        <div className="rail-search">
          <Search size={14} />
          <input
            aria-label="Filter tasks"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Filter tasks…"
          />
          <button type="button" className="rail-kbtn" onClick={() => setPaletteOpen(true)} title="Command palette">
            ⌘K
          </button>
        </div>

        <div className="rail-filters" role="tablist" aria-label="Task filters">
          {([
            ['all', 'All'],
            ['active', 'Active'],
            ['failed', 'Failed'],
            ['done', 'Done'],
          ] as const).map(([id, label]) => (
            <button
              key={id}
              type="button"
              role="tab"
              aria-selected={prefs.railFilter === id}
              className={prefs.railFilter === id ? 'active' : ''}
              onClick={() => updatePrefs({ railFilter: id })}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="rail-label">
          <span>Tasks</span>
          <div>
            <button type="button" onClick={() => setCreateOpen(true)} title="New hierarchy task">
              <Plus size={12} />
            </button>
            <button type="button" onClick={() => void refreshTasks(true)} title="Refresh tasks">
              <RefreshCw size={12} />
            </button>
          </div>
        </div>

        <div className="task-list">
          {groupedTasks.map(([project, projectTasks]) => (
            <div className="task-group" key={project}>
              <div className="task-group-label">{project}</div>
              {projectTasks.map((task) => {
                const active = isActiveStatus(task.status);
                const menuOpen = menuTaskId === task.id;
                const pinned = prefs.pinnedTaskIds.includes(task.id);
                const tone = statusTone(task.status);
                return (
                  <div
                    className={`task-card tone-${tone} ${task.id === selectedTask?.id ? 'active' : ''} ${menuOpen ? 'menu-open' : ''} ${pinned ? 'pinned' : ''}`}
                    key={task.id}
                  >
                    <button
                      className="task-card-main"
                      type="button"
                      onClick={() => setSelectedTask(task)}
                    >
                      <span className={`task-status-strip tone-${tone}`} />
                      <span className={`task-icon role-worker`}>
                        <Layers3 size={15} />
                      </span>
                      <span className="task-card-copy">
                        <strong title={task.name}>{shortTaskTitle(task)}</strong>
                        <small>
                          {task.status}
                          {task.turn_count != null ? ` · t${task.turn_count}` : ''}
                          {' · '}
                          {relativeTime(task.updated_at)}
                        </small>
                      </span>
                    </button>
                    <div className="task-menu-wrap" ref={menuOpen ? menuRef : undefined}>
                      <button
                        type="button"
                        className={`task-menu-trigger ${menuOpen ? 'open' : ''}`}
                        aria-label="Task actions"
                        aria-expanded={menuOpen}
                        onClick={(event) => {
                          event.stopPropagation();
                          setMenuTaskId(menuOpen ? null : task.id);
                        }}
                      >
                        <MoreHorizontal size={14} />
                      </button>
                      {menuOpen && (
                        <div className="task-menu" role="menu">
                          <button type="button" role="menuitem" onClick={() => { setMenuTaskId(null); setSelectedTask(task); }}>
                            <FolderOpen size={13} /> Open task
                          </button>
                          <button type="button" role="menuitem" onClick={() => { setMenuTaskId(null); togglePin(task.id); }}>
                            <Pin size={13} /> {pinned ? 'Unpin' : 'Pin'} task
                          </button>
                          <button
                            type="button"
                            role="menuitem"
                            disabled={busyAction || task.id === mockTask.id}
                            onClick={() => void runTaskAction(task)}
                          >
                            {active ? <CircleStop size={13} /> : <Play size={13} />}
                            {active ? 'Stop task' : 'Resume task'}
                          </button>
                          <button type="button" role="menuitem" onClick={() => void copyTaskId(task.id)}>
                            <Copy size={13} /> Copy task ID
                          </button>
                          <div className="task-menu-sep" />
                          <button
                            type="button"
                            role="menuitem"
                            className="danger"
                            disabled={busyAction || active || task.id === mockTask.id}
                            onClick={() => void deleteTask(task)}
                          >
                            <Trash2 size={13} /> Delete task
                          </button>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          ))}
          {!loading && !filteredTasks.length && (
            <div className="rail-empty">
              <Boxes size={18} />
              <strong>No tasks yet</strong>
              <span>Launch a hierarchy run to populate this rail.</span>
              <button type="button" className="primary-button" onClick={() => setCreateOpen(true)}>
                <Plus size={14} /> New run
              </button>
            </div>
          )}
        </div>

        <div className="rail-footer">
          <div className={`backend-state ${offline ? 'offline' : ''}`}>
            {offline ? <CloudOff size={13} /> : <Activity size={13} />}
            <span>{offline ? 'Offline' : 'Backend online'}</span>
          </div>
          <button type="button" onClick={() => setSettingsOpen(true)} title="Workspace settings">
            <Settings2 size={15} />
          </button>
        </div>
      </aside>

      <main className="workspace-main">
        {displayedTask ? (
          <>
            <header className="task-header">
              <div className="task-breadcrumb">
                <span><FolderGit2 size={13} /> {taskProjectKey(displayedTask.root)}</span>
                <i>/</i>
                <strong>{shortTaskTitle(displayedTask)}</strong>
                {displayedTask.phase && (
                  <>
                    <i>/</i>
                    <em>{displayedTask.phase}</em>
                  </>
                )}
                {displayedTask.current_agent && (
                  <>
                    <i>/</i>
                    <em>{displayedTask.current_agent}</em>
                  </>
                )}
              </div>
              <div className="task-title-row">
                <div>
                  <div className="title-kicker">
                    <span className={`pulse-dot ${state.connection}`} />
                    {state.connection === 'live' ? 'LIVE' : displayedTask.status}
                    <span className="live-pill">
                      {runningAgents} agents · {state.signals.length} signals
                    </span>
                    <span className="task-id">#{displayedTask.id.slice(0, 8)}</span>
                    <span className="profile-pill">{profileSummary}</span>
                  </div>
                  <h1 title={displayedTask.name}>{shortTaskTitle(displayedTask)}</h1>
                  <p>{displayedTask.prompt || displayedTask.name}</p>
                </div>
                <div className="task-header-actions">
                  <button type="button" className="ghost-button" onClick={() => setPaletteOpen(true)}>
                    <Command size={14} /> Commands
                  </button>
                  <button
                    type="button"
                    className="ghost-button"
                    onClick={() => updatePrefs({ consoleOpen: !prefs.consoleOpen })}
                  >
                    <Terminal size={14} /> Console
                  </button>
                  <button type="button" className="ghost-button" onClick={() => setSettingsOpen(true)}>
                    <SlidersHorizontal size={14} /> Limits
                  </button>
                  <button
                    type="button"
                    className="danger-button"
                    onClick={() => void deleteTask(displayedTask)}
                    disabled={busyAction || taskIsActive || displayedTask.id === mockTask.id}
                  >
                    <Trash2 size={14} /> Delete
                  </button>
                  <button
                    type="button"
                    className={`primary-button ${taskIsActive ? 'stop' : ''}`}
                    onClick={() => void runTaskAction(displayedTask)}
                    disabled={busyAction || displayedTask.id === mockTask.id}
                  >
                    {taskIsActive
                      ? <><CircleStop size={14} /> Stop</>
                      : <><Play size={14} /> Resume</>}
                  </button>
                </div>
              </div>
              <PhaseStepper status={displayedTask.status} />
              <div className="task-metrics">
                <div><Bot size={14} /><span>Agents</span><strong>{agentCount}</strong></div>
                <div><Sparkles size={14} /><span>Running</span><strong>{runningAgents}</strong></div>
                <div><Gauge size={14} /><span>Events</span><strong>{state.eventCount}</strong></div>
                <div><FolderGit2 size={14} /><span>Files</span><strong>{changedFiles}</strong></div>
                <div><Timer size={14} /><span>Time</span><strong>{totalElapsed}</strong></div>
                <div><KeyRound size={14} /><span>Accounts</span><strong>{accountsUsed}</strong></div>
              </div>
            </header>

            <InterventionBanner
              visible={waitingInput}
              message="Director is waiting for human input before continuing."
              onFocus={() => {
                updatePrefs({ consoleOpen: true });
                focusDirector();
              }}
            />

            <div className={`workspace-content${graphExpanded ? ' graph-expanded' : ''}`}>
              <DagOverview
                state={state}
                onOpenAgent={openAgent}
                expanded={graphExpanded}
                onToggleExpand={() => setGraphExpanded((value) => !value)}
              />
              <DockWorkspace
                ref={dockRef}
                taskId={displayedTask.id}
                state={state}
                onFocusAgent={handleFocusAgent}
                onConfigureAgent={handleConfigureAgent}
              />
            </div>
            <EventConsole
              state={state}
              open={prefs.consoleOpen}
              onClose={() => updatePrefs({ consoleOpen: false })}
              onOpenAgent={(id) => {
                const agent = state.agents[id];
                if (agent) openAgent(agent);
              }}
            />
          </>
        ) : (
          <div className="workspace-empty">
            <div className="workspace-empty-mark"><Bot size={27} /></div>
            <span className="eyebrow">Mission control</span>
            <h1>Standing by</h1>
            <p>
              {offline
                ? 'Backend offline — start the orchestrator on port 8000, then retry.'
                : 'Select a task from the rail or launch a new hierarchy run.'}
            </p>
            <div className="empty-actions">
              <button type="button" className="primary-button" onClick={() => setCreateOpen(true)}>
                <Sparkles size={14} /> New run
              </button>
              <button type="button" className="secondary-button" onClick={() => void refreshTasks(false)}>
                <RefreshCw size={14} /> Retry
              </button>
              <button type="button" className="ghost-button" onClick={() => setPaletteOpen(true)}>
                <Command size={14} /> Commands
              </button>
            </div>
            {!offline && (
              <div className="launch-templates" role="list">
                {TEMPLATES.map((template) => (
                  <button
                    key={template.id}
                    type="button"
                    className="launch-template"
                    role="listitem"
                    onClick={() => setCreateOpen(true)}
                  >
                    <strong>{template.label}</strong>
                    <span>{template.goal.slice(0, 72)}…</span>
                  </button>
                ))}
              </div>
            )}
          </div>
        )}
      </main>

      {settingsOpen && (
        <div className="settings-backdrop" onMouseDown={() => setSettingsOpen(false)}>
          <aside
            className="settings-panel"
            role="dialog"
            aria-modal="true"
            aria-label="Runtime and UI settings"
            onMouseDown={(event) => event.stopPropagation()}
          >
            <header className="settings-panel-header">
              <div>
                <span className="eyebrow">Local preferences</span>
                <h2>Runtime & UI</h2>
              </div>
              <button type="button" className="icon-button" onClick={() => setSettingsOpen(false)} aria-label="Close settings">
                <X size={16} />
              </button>
            </header>

            <div className="settings-panel-body">
              <section className="settings-section">
                <header>
                  <h3>Model defaults</h3>
                  <p>Applied to new hierarchy runs. Live agents can still override per window.</p>
                </header>
                <div className="role-matrix">
                  {(['director', 'manager', 'worker', 'tester'] as ConfigurableRole[]).map((role) => (
                    <RoleProfileEditor
                      key={role}
                      role={role}
                      profile={settings.roleProfiles[role]}
                      onChange={(profile) =>
                        setSettings({
                          ...settings,
                          roleProfiles: { ...settings.roleProfiles, [role]: profile },
                        })
                      }
                    />
                  ))}
                </div>
                <div className="preset-segment" role="group" aria-label="Quality presets">
                  <button
                    type="button"
                    onClick={() =>
                      setSettings({
                        ...settings,
                        roleProfiles: {
                          director: { model: 'claude-sonnet-4-6', effort: 'medium' },
                          manager: { model: 'claude-sonnet-4-6', effort: 'medium' },
                          worker: { model: 'claude-sonnet-4-6', effort: 'low' },
                          tester: { model: 'claude-sonnet-4-6', effort: 'medium' },
                        },
                      })
                    }
                  >
                    Cheap
                  </button>
                  <button
                    type="button"
                    onClick={() =>
                      setSettings({
                        ...settings,
                        roleProfiles: {
                          director: { model: 'claude-sonnet-5', effort: 'high' },
                          manager: { model: 'claude-sonnet-5', effort: 'high' },
                          worker: { model: 'claude-sonnet-5', effort: 'medium' },
                          tester: { model: 'claude-sonnet-5', effort: 'high' },
                        },
                      })
                    }
                  >
                    Balanced
                  </button>
                  <button
                    type="button"
                    onClick={() =>
                      setSettings({
                        ...settings,
                        roleProfiles: DEFAULT_SETTINGS.roleProfiles,
                      })
                    }
                  >
                    Max quality
                  </button>
                </div>
              </section>

              <section className="settings-section">
                <header>
                  <h3>Hierarchy shape</h3>
                  <p>
                    These are exact agent counts: Director creates this many Managers;
                    every Manager creates N-1 Coders and one dedicated Tester.
                  </p>
                </header>
                <div className="concurrency-card">
                  <RangeSetting
                    label="Managers"
                    value={settings.maxParallelManagers}
                    min={1}
                    max={12}
                    onChange={(value) => setSettings({ ...settings, maxParallelManagers: value })}
                  />
                  <RangeSetting
                    label="Child agents per manager"
                    value={settings.maxWorkersPerManager}
                    min={2}
                    max={12}
                    onChange={(value) => setSettings({ ...settings, maxWorkersPerManager: value })}
                  />
                  <RangeSetting
                    label="Global parallel workers"
                    value={settings.maxParallelWorkers}
                    min={1}
                    max={32}
                    onChange={(value) => setSettings({ ...settings, maxParallelWorkers: value })}
                  />
                </div>
              </section>

              <section className="settings-section">
                <header>
                  <h3>Workspace</h3>
                  <p>Local UI density and offline behavior.</p>
                </header>
                <label className="toggle-setting">
                  <span>
                    <strong>Compact density</strong>
                    <small>Tighter rail and headers.</small>
                  </span>
                  <input
                    type="checkbox"
                    checked={prefs.density === 'compact'}
                    onChange={(event) =>
                      updatePrefs({ density: event.target.checked ? 'compact' : 'comfortable' })
                    }
                  />
                </label>
                <label className="toggle-setting">
                  <span>
                    <strong>Offline preview</strong>
                    <small>Use representative data if the API is unavailable.</small>
                  </span>
                  <input
                    type="checkbox"
                    checked={settings.mockWhenUnavailable}
                    onChange={(event) =>
                      setSettings({ ...settings, mockWhenUnavailable: event.target.checked })
                    }
                  />
                </label>
              </section>
            </div>
          </aside>
        </div>
      )}

      {createOpen && (
        <CreateTaskWizard
          settings={settings}
          onClose={() => setCreateOpen(false)}
          onCreated={async () => {
            setCreateOpen(false);
            pushToast('Hierarchy task launched', 'success');
            await refreshTasks(false);
          }}
        />
      )}

      <CommandPalette open={paletteOpen} onClose={() => setPaletteOpen(false)} commands={commands} />
      <ToastStack
        toasts={toasts}
        onDismiss={(id) => setToasts((current) => current.filter((toast) => toast.id !== id))}
      />
    </div>
  );
}

function RangeSetting({
  label,
  value,
  min,
  max,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
}) {
  return (
    <label className="range-setting">
      <span><strong>{label}</strong><output>{value}</output></span>
      <input
        type="range"
        min={min}
        max={max}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
      <small>{min} minimum <i /> {max} maximum</small>
    </label>
  );
}

function RoleProfileEditor({
  role,
  profile,
  onChange,
}: {
  role: ConfigurableRole;
  profile: AgentProfile;
  onChange: (profile: AgentProfile) => void;
}) {
  return (
    <section className={`role-profile role-profile-${role}`}>
      <header><strong>{role}</strong><span>default</span></header>
      <div>
        <label>
          <span>Model</span>
          <select
            value={profile.model}
            onChange={(event) => {
              const model = event.target.value;
              onChange({
                model,
                effort:
                  model === 'claude-sonnet-4-6' && profile.effort === 'xhigh'
                    ? 'max'
                    : profile.effort,
              });
            }}
          >
            <option value="claude-sonnet-5">Sonnet 5</option>
            <option value="claude-sonnet-4-6">Sonnet 4.6</option>
          </select>
        </label>
        <label>
          <span>Effort</span>
          <select
            value={profile.effort}
            onChange={(event) => onChange({ ...profile, effort: event.target.value })}
          >
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
            <option value="max">Max</option>
            {profile.model === 'claude-sonnet-5' && <option value="xhigh">X-High</option>}
          </select>
        </label>
      </div>
    </section>
  );
}

function CreateTaskWizard({
  settings,
  onClose,
  onCreated,
}: {
  settings: WorkspaceSettings;
  onClose: () => void;
  onCreated: (taskId: string) => Promise<void>;
}) {
  const [step, setStep] = useState(0);
  const [name, setName] = useState('');
  const [root, setRoot] = useState('');
  const [task, setTask] = useState('');
  const [testCmd, setTestCmd] = useState('python -m pytest -q');
  const [projectMode, setProjectMode] = useState<'edit' | 'new_project'>('edit');
  const [managers, setManagers] = useState(settings.maxParallelManagers);
  const [workersPerManager, setWorkersPerManager] = useState(settings.maxWorkersPerManager);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  const chooseFolder = async () => {
    setError('');
    try {
      const result = await api.pickFolder();
      if (result.error) throw new Error(result.error);
      if (result.root) setRoot(result.root);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Could not open folder picker.');
    }
  };

  const applyTemplate = (templateId: string) => {
    const template = TEMPLATES.find((item) => item.id === templateId);
    if (!template) return;
    setName(template.name);
    setTask(template.goal);
    if (template.id === 'greenfield') setProjectMode('new_project');
  };

  const submit = async () => {
    if (!root.trim() || !task.trim()) {
      setError('Choose a folder and enter a goal.');
      setStep(1);
      return;
    }
    setBusy(true);
    setError('');
    try {
      const launchSettings: WorkspaceSettings = {
        ...settings,
        maxParallelManagers: managers,
        maxWorkersPerManager: Math.max(2, workersPerManager),
      };
      const created = await api.createHierarchyTask({
        name: name.trim() || shortTaskTitle({ id: 'new', name, prompt: task, root }),
        root,
        task,
        projectMode,
        testCmd,
        settings: launchSettings,
      });
      await onCreated(created.task_id);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Could not create task.');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="settings-backdrop create-backdrop" onMouseDown={onClose}>
      <section className="create-dialog wizard" onMouseDown={(event) => event.stopPropagation()}>
        <header>
          <div>
            <span className="eyebrow">Launch wizard · step {step + 1}/3</span>
            <h2>New run</h2>
          </div>
          <button type="button" onClick={onClose}><X size={16} /></button>
        </header>

        <div className="wizard-steps">
          {['Folder', 'Goal', 'Shape'].map((label, index) => (
            <button
              key={label}
              type="button"
              className={index === step ? 'active' : index < step ? 'done' : ''}
              onClick={() => setStep(index)}
            >
              <i>{index + 1}</i>{label}
            </button>
          ))}
        </div>

          {step === 0 && (
          <div className="create-grid">
            <label>
              <span>Project mode</span>
              <select
                value={projectMode}
                onChange={(event) => setProjectMode(event.target.value as 'edit' | 'new_project')}
              >
                <option value="edit">Edit existing project</option>
                <option value="new_project">Create new project</option>
              </select>
            </label>
            <label className="create-wide">
              <span>Local project folder</span>
              <div className="folder-field">
                <input value={root} readOnly placeholder="Choose a local folder…" />
                <button type="button" className="folder-browse-button" onClick={() => void chooseFolder()}>
                  <FolderOpen size={14} /> Browse
                </button>
              </div>
            </label>
            <div className="launch-templates create-wide" role="list">
              {TEMPLATES.map((template) => (
                <button
                  key={template.id}
                  type="button"
                  className="launch-template"
                  role="listitem"
                  onClick={() => {
                    applyTemplate(template.id);
                    setStep(1);
                  }}
                >
                  <strong>{template.label}</strong>
                  <span>{template.goal.slice(0, 90)}…</span>
                </button>
              ))}
            </div>
          </div>
        )}

        {step === 1 && (
          <div className="create-grid">
            <label>
              <span>Task name</span>
              <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Short display name" />
            </label>
            <label>
              <span>Test command</span>
              <input value={testCmd} onChange={(event) => setTestCmd(event.target.value)} placeholder="python -m pytest -q" />
            </label>
            <label className="create-wide">
              <span>Director goal</span>
              <textarea
                value={task}
                onChange={(event) => setTask(event.target.value)}
                placeholder="Describe the full outcome. Director will split it into workstreams."
                rows={8}
              />
            </label>
          </div>
        )}

        {step === 2 && (
          <div className="create-grid shape-step">
            <RangeSetting label="Managers" value={managers} min={1} max={12} onChange={setManagers} />
            <RangeSetting
              label="Child agents per manager"
              value={workersPerManager}
              min={2}
              max={12}
              onChange={setWorkersPerManager}
            />
            <div className="create-wide">
              <HierarchyPreview managers={managers} workersPerManager={workersPerManager} />
            </div>
            <div className="estimate-panel create-wide">
              <strong>Estimate</strong>
              <span>
                {managers} managers · {managers * Math.max(1, workersPerManager - 1)} coders · {managers} testers · global coder cap{' '}
                {settings.maxParallelWorkers}
              </span>
            </div>
          </div>
        )}

        {error && <p className="create-error">{error}</p>}
        <footer>
          <button type="button" className="ghost-button" onClick={onClose}>Cancel</button>
          {step > 0 && (
            <button type="button" className="ghost-button" onClick={() => setStep((value) => value - 1)}>
              Back
            </button>
          )}
          {step < 2 ? (
            <button
              type="button"
              className="primary-button"
              onClick={() => {
                if (step === 0 && !root.trim()) {
                  setError('Choose a project folder first.');
                  return;
                }
                if (step === 1 && !task.trim()) {
                  setError('Enter a Director goal.');
                  return;
                }
                setError('');
                setStep((value) => value + 1);
              }}
            >
              Continue
            </button>
          ) : (
            <button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>
              <Sparkles size={14} /> {busy ? 'Launching…' : 'Launch'}
            </button>
          )}
        </footer>
      </section>
    </div>
  );
}
