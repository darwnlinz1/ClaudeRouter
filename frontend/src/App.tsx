import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
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
  Maximize2,
  MoreHorizontal,
  Minimize2,
  PanelLeft,
  Pin,
  Play,
  Plus,
  Radio,
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
import { DockWorkspace, type DockWorkspaceHandle } from './components/DockWorkspace';
import { EventConsole } from './components/EventConsole';
import { HierarchyPreview } from './components/HierarchyPreview';
import { InterventionBanner } from './components/InterventionBanner';
import { PhaseStepper } from './components/PhaseStepper';
import { TaskDashboard } from './components/TaskDashboard';
import { ToastStack } from './components/ToastStack';
import { WorkspaceSplitter } from './components/WorkspaceSplitter';
import { mockTask } from './data/mockWorkspace';
import { useWorkspace } from './hooks/useWorkspace';
import { launchManagerSlots, launchWorkerSlots } from './lib/launchSlots';
import { isActiveStatus, statusTone } from './lib/phases';
import { loadPrefs, normalizeWorkspaceSettings, savePrefs, type UiPrefs } from './lib/preferences';
import { shortTaskTitle, taskProjectKey } from './lib/taskTitle';
import type {
  AccountHealth,
  AgentInstance,
  AgentProfile,
  ConfigurableRole,
  TaskSummary,
  WorkspaceSettings,
} from './types';

const SETTINGS_KEY = 'orchestrator:workspace-settings:v2';
const DEFAULT_SETTINGS: WorkspaceSettings = {
  maxManagers: 4,
  maxParallelManagers: 4,
  maxWorkersPerManager: 5,
  maxParallelWorkersPerManager: undefined,
  // Four managers times four coders: wide enough to run the default plan.
  maxParallelWorkers: 16,
  mockWhenUnavailable: false,
  roleProfiles: {
    director: { model: 'claude-sonnet-5', effort: 'max' },
    manager: { model: 'claude-sonnet-5', effort: 'max' },
    worker: { model: 'claude-sonnet-5', effort: 'max' },
    tester: { model: 'claude-sonnet-5', effort: 'high' },
  },
};

const QUALITY_PRESETS: Array<{
  id: 'low-cost' | 'balanced' | 'max-quality';
  label: string;
  profiles: WorkspaceSettings['roleProfiles'];
}> = [
  {
    id: 'low-cost',
    label: 'Low cost',
    profiles: {
      director: { model: 'claude-sonnet-4-6', effort: 'medium' },
      manager: { model: 'claude-sonnet-4-6', effort: 'medium' },
      worker: { model: 'claude-sonnet-4-6', effort: 'low' },
      tester: { model: 'claude-sonnet-4-6', effort: 'medium' },
    },
  },
  {
    id: 'balanced',
    label: 'Balanced',
    profiles: {
      director: { model: 'claude-sonnet-5', effort: 'high' },
      manager: { model: 'claude-sonnet-5', effort: 'high' },
      worker: { model: 'claude-sonnet-5', effort: 'medium' },
      tester: { model: 'claude-sonnet-5', effort: 'high' },
    },
  },
  {
    id: 'max-quality',
    label: 'Max quality',
    profiles: DEFAULT_SETTINGS.roleProfiles,
  },
];

const TEMPLATES = [
  {
    id: 'bugfix',
    label: 'Bugfix',
    name: 'Bugfix',
    goal: 'Investigate and fix the reported bug. Keep the change minimal, add or update tests, and verify the failing case passes.',
    managers: 2,
    workersPerManager: 4,
    testCmd: 'python -m pytest -q',
    projectMode: 'edit',
  },
  {
    id: 'feature',
    label: 'Feature slice',
    name: 'Feature slice',
    goal: 'Implement one vertical feature slice end-to-end with clear module boundaries, tests, and a short summary of files touched.',
    managers: 4,
    workersPerManager: 5,
    testCmd: 'python -m pytest -q',
    projectMode: 'edit',
  },
  {
    id: 'refactor',
    label: 'Refactor + tests',
    name: 'Refactor + tests',
    goal: 'Refactor the targeted area for clarity and safety without behavior changes. Strengthen unit/integration coverage around the risk surface.',
    managers: 3,
    workersPerManager: 4,
    testCmd: 'python -m pytest -q',
    projectMode: 'edit',
  },
  {
    id: 'greenfield',
    label: 'Greenfield',
    name: 'Greenfield scaffold',
    goal: 'Scaffold a new project structure with baseline config, entrypoints, and a first passing test harness.',
    managers: 4,
    workersPerManager: 5,
    testCmd: 'python -m pytest -q',
    projectMode: 'new_project',
  },
] as const;

type Toast = { id: string; message: string; tone?: 'info' | 'success' | 'error' };

const loadSettings = (): WorkspaceSettings => {
  try {
    const stored = JSON.parse(
      localStorage.getItem(SETTINGS_KEY) ?? '{}',
    ) as Partial<WorkspaceSettings>;
    return normalizeWorkspaceSettings(stored, DEFAULT_SETTINGS);
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

const sentenceStatus = (value: string) => {
  const normalized = value.replaceAll('_', ' ').trim().toLowerCase();
  return normalized ? normalized[0].toUpperCase() + normalized.slice(1) : 'Unknown';
};

const matchesRailFilter = (task: TaskSummary, filter: UiPrefs['railFilter']) => {
  const status = task.status.toUpperCase();
  if (filter === 'all') return true;
  if (filter === 'active') return isActiveStatus(status);
  if (filter === 'failed') return status === 'FAILED' || status === 'ERROR';
  return ['COMPLETED', 'DONE', 'PARTIAL', 'ABANDONED', 'SKIPPED', 'STOPPED', 'CANCELLED'].includes(
    status,
  );
};

const routedTaskId = () => {
  const value = new URLSearchParams(window.location.hash.slice(1)).get('task');
  return value?.trim() || undefined;
};

type ConfirmationRequest = {
  kind: 'stop' | 'delete';
  task: TaskSummary;
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
  const [railDrawerOpen, setRailDrawerOpen] = useState(false);
  const openCreate = useCallback(() => {
    setRailDrawerOpen(false);
    setCreateOpen(true);
  }, []);
  const [busyAction, setBusyAction] = useState(false);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [accountHealth, setAccountHealth] = useState<AccountHealth[]>([]);
  const [confirmation, setConfirmation] = useState<ConfirmationRequest>();
  const [graphExpanded, setGraphExpanded] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);
  const dockRef = useRef<DockWorkspaceHandle>(null);
  const appShellRef = useRef<HTMLDivElement>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const taskStatusRef = useRef<Map<string, string>>(new Map());
  const taskRequestRef = useRef<AbortController | null>(null);
  const { state, dispatch, approvals, markApprovalDecision } = useWorkspace(selectedTask);
  const selectedTaskId = selectedTask?.id;
  const selectedQualityPreset = QUALITY_PRESETS.find(({ profiles }) =>
    (Object.keys(profiles) as ConfigurableRole[]).every(
      (role) =>
        profiles[role].model === settings.roleProfiles[role].model &&
        profiles[role].effort === settings.roleProfiles[role].effort,
    ),
  )?.id;

  const pushToast = useCallback((message: string, tone: Toast['tone'] = 'info') => {
    const id = `${Date.now()}:${Math.random().toString(16).slice(2)}`;
    setToasts((current) => [...current.slice(-4), { id, message, tone }]);
  }, []);
  const selectTask = useCallback((task: TaskSummary, pushHistory = true) => {
    setSelectedTask(task);
    setRailDrawerOpen(false);
    const nextHash = `#task=${encodeURIComponent(task.id)}`;
    if (window.location.hash === nextHash) return;
    if (pushHistory) window.history.pushState(null, '', nextHash);
    else window.history.replaceState(null, '', nextHash);
  }, []);

  const handleFocusAgent = useCallback(
    (agentId: string) => dispatch({ type: 'focus-agent', agentId }),
    [dispatch],
  );
  const handleConfigureAgent = useCallback(
    async (agentId: string, model: string, effort: string) => {
      if (!selectedTaskId) throw new Error('No active task.');
      await api.updateAgentConfig(selectedTaskId, agentId, model, effort);
      dispatch({ type: 'agent-configured', agentId, model, effort });
      pushToast('Agent override saved for next call', 'success');
    },
    [dispatch, pushToast, selectedTaskId],
  );

  const updatePrefs = useCallback((partial: Partial<UiPrefs>) => {
    setPrefs(savePrefs(partial));
  }, []);

  const refreshTasks = useCallback(
    async (preserveSelection = true) => {
      taskRequestRef.current?.abort();
      const controller = new AbortController();
      taskRequestRef.current = controller;
      try {
        const next = await api.listTasks(controller.signal);
        if (controller.signal.aborted) return;
        setOffline(false);
        setTasks(next);
        setSelectedTask((current) => {
          if (preserveSelection && current) {
            const preserved = next.find((task) => task.id === current.id);
            if (preserved) return preserved;
          }
          const routeId = routedTaskId();
          if (routeId) {
            const routed = next.find((task) => task.id === routeId);
            if (routed) return routed;
          }
          return next[0];
        });
      } catch (error) {
        if (controller.signal.aborted) return;
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
          setSelectedTask((current) => (current?.id === mockTask.id ? undefined : current));
        }
      } finally {
        if (taskRequestRef.current === controller) {
          taskRequestRef.current = null;
          setLoading(false);
        }
      }
    },
    [pushToast, settings.mockWhenUnavailable],
  );

  useEffect(() => {
    if (!toasts.length) return undefined;
    const timer = window.setTimeout(() => {
      setToasts((current) => current.slice(1));
    }, 3200);
    return () => window.clearTimeout(timer);
  }, [toasts]);

  useEffect(
    () => () => {
      const request = taskRequestRef.current;
      taskRequestRef.current = null;
      request?.abort();
    },
    [],
  );

  useEffect(() => {
    const previous = taskStatusRef.current;
    if (!previous.size) {
      taskStatusRef.current = new Map(tasks.map((task) => [task.id, task.status.toUpperCase()]));
      return;
    }
    for (const task of tasks) {
      const status = task.status.toUpperCase();
      const before = previous.get(task.id);
      if (before && before !== status) {
        if (status === 'WAITING_INPUT') {
          pushToast(`${shortTaskTitle(task)} needs your input`, 'info');
        } else if (['FAILED', 'ERROR'].includes(status)) {
          pushToast(`${shortTaskTitle(task)} failed`, 'error');
        } else if (['COMPLETED', 'DONE'].includes(status)) {
          pushToast(`${shortTaskTitle(task)} completed`, 'success');
        }
      }
      previous.set(task.id, status);
    }
  }, [pushToast, tasks]);

  useEffect(() => {
    void refreshTasks(false);
  }, [refreshTasks]);

  useEffect(() => {
    let timer: number | undefined;
    const schedule = () => {
      if (timer) window.clearInterval(timer);
      timer = undefined;
      if (document.visibilityState === 'hidden') return;
      const hasActiveTask = tasks.some((task) => isActiveStatus(task.status));
      timer = window.setInterval(() => void refreshTasks(true), hasActiveTask ? 8_000 : 30_000);
    };
    const onVisibility = () => {
      if (document.visibilityState === 'visible') void refreshTasks(true);
      schedule();
    };
    schedule();
    document.addEventListener('visibilitychange', onVisibility);
    return () => {
      if (timer) window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [refreshTasks, tasks]);

  useEffect(() => {
    localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
  }, [settings]);

  useEffect(() => {
    const syncRoute = () => {
      const routeId = routedTaskId();
      if (!routeId) return;
      const routed = tasks.find((task) => task.id === routeId);
      if (routed) setSelectedTask(routed);
    };
    window.addEventListener('hashchange', syncRoute);
    window.addEventListener('popstate', syncRoute);
    return () => {
      window.removeEventListener('hashchange', syncRoute);
      window.removeEventListener('popstate', syncRoute);
    };
  }, [tasks]);

  useEffect(() => {
    if (!selectedTaskId || window.location.hash) return;
    window.history.replaceState(null, '', `#task=${encodeURIComponent(selectedTaskId)}`);
  }, [selectedTaskId]);

  useEffect(() => {
    const syncFullscreen = () => setFullscreen(document.fullscreenElement != null);
    document.addEventListener('fullscreenchange', syncFullscreen);
    return () => document.removeEventListener('fullscreenchange', syncFullscreen);
  }, []);

  useEffect(() => {
    if (!settingsOpen) return undefined;
    const controller = new AbortController();
    void api
      .listAccountHealth(controller.signal)
      .then((items) => {
        if (!controller.signal.aborted) setAccountHealth(items);
      })
      .catch(() => {
        if (!controller.signal.aborted) setAccountHealth([]);
      });
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setSettingsOpen(false);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      controller.abort();
      window.removeEventListener('keydown', onKeyDown);
    };
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

  useEffect(() => {
    if (!graphExpanded) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setGraphExpanded(false);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [graphExpanded]);

  useEffect(() => {
    if (!railDrawerOpen) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setRailDrawerOpen(false);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [railDrawerOpen]);

  const openAgent = useCallback((agent: AgentInstance) => {
    setGraphExpanded(false);
    dockRef.current?.openAgent(agent);
  }, []);

  const toggleFullscreen = useCallback(async () => {
    try {
      if (document.fullscreenElement) {
        await document.exitFullscreen();
      } else if (appShellRef.current?.requestFullscreen) {
        await appShellRef.current.requestFullscreen();
      }
    } catch (error) {
      pushToast(error instanceof Error ? error.message : 'Fullscreen is unavailable.', 'error');
    }
  }, [pushToast]);

  const displayedTask = state.task ?? selectedTask;
  const realAgents = Object.values(state.agents).filter(
    (agent) => !agent.id.endsWith(':root') && !agent.id.endsWith(':anon'),
  );
  const runningAgents = realAgents.filter((agent) => agent.status === 'running').length;
  const waitingAgents = realAgents.filter((agent) => agent.status === 'waiting').length;
  const agentCount = realAgents.length;
  const plannedChildren =
    state.fanout.plannedChildren ||
    realAgents.filter((agent) => ['worker', 'tester', 'reviewer'].includes(agent.role)).length;
  const plannedManagers =
    state.fanout.plannedManagers ||
    realAgents.filter((agent) => agent.role === 'manager' || agent.role === 'supervisor').length;
  const calledAgents = state.fanout.calledAgentIds.length;
  const plannedPrimaryAgents =
    plannedManagers || plannedChildren
      ? 1 + plannedManagers + plannedChildren
      : Math.max(1, agentCount);
  const taskIsActive = displayedTask ? isActiveStatus(displayedTask.status) : false;
  const waitingInput = displayedTask?.status?.toUpperCase() === 'WAITING_INPUT';
  const pendingApproval = approvals.find((approval) => approval.status === 'pending');
  const accountsUsed = state.usedAccounts.length;
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

  const runTaskAction = useCallback(
    async (task: TaskSummary, confirmed = false) => {
      if (busyAction || task.id === mockTask.id) return;
      if (isActiveStatus(task.status) && !confirmed) {
        setConfirmation({ kind: 'stop', task });
        return;
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
    },
    [busyAction, pushToast, refreshTasks],
  );

  const deleteTask = async (task: TaskSummary, confirmed = false) => {
    if (busyAction || task.id === mockTask.id) return;
    if (isActiveStatus(task.status)) {
      pushToast('Stop the task before deleting', 'error');
      return;
    }
    if (!confirmed) {
      setConfirmation({ kind: 'delete', task });
      return;
    }
    setBusyAction(true);
    setMenuTaskId(null);
    try {
      await api.deleteTask(task.id);
      if (selectedTask?.id === task.id) {
        setSelectedTask(undefined);
        window.history.replaceState(
          null,
          '',
          `${window.location.pathname}${window.location.search}`,
        );
      }
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
      pushToast(`Copy unavailable. Task ID: ${taskId}`, 'error');
    }
  };

  const decideApproval = async (decision: 'approved' | 'rejected') => {
    if (!displayedTask || !pendingApproval || busyAction) return;
    setBusyAction(true);
    try {
      await api.decideApproval(displayedTask.id, pendingApproval.approvalId, decision);
      markApprovalDecision(pendingApproval.approvalId, decision);
      pushToast(
        decision === 'approved' ? 'Action approved' : 'Action rejected',
        decision === 'approved' ? 'success' : 'info',
      );
    } catch (error) {
      pushToast(error instanceof Error ? error.message : 'Approval decision failed', 'error');
    } finally {
      setBusyAction(false);
    }
  };

  const togglePin = (taskId: string) => {
    const pinned = prefs.pinnedTaskIds.includes(taskId)
      ? prefs.pinnedTaskIds.filter((id) => id !== taskId)
      : [...prefs.pinnedTaskIds, taskId];
    updatePrefs({ pinnedTaskIds: pinned });
  };

  const focusDirector = useCallback(() => {
    if (!state.directorId) return;
    const director = state.agents[state.directorId];
    if (director) openAgent(director);
  }, [openAgent, state.agents, state.directorId]);

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
        run: openCreate,
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
        id: 'fullscreen',
        label: fullscreen ? 'Exit fullscreen' : 'Enter fullscreen',
        run: () => void toggleFullscreen(),
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
    fullscreen,
    openAgent,
    openCreate,
    prefs.consoleOpen,
    prefs.density,
    refreshTasks,
    runTaskAction,
    state.agents,
    state.directorId,
    taskIsActive,
    toggleFullscreen,
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
      if (event.key === 'F11') {
        event.preventDefault();
        void toggleFullscreen();
        return;
      }
      if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key.toLowerCase() === 'f') {
        event.preventDefault();
        void toggleFullscreen();
        return;
      }
      if (typing || paletteOpen) return;
      if (event.key.toLowerCase() === 'n') {
        event.preventDefault();
        openCreate();
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
  }, [
    displayedTask,
    openAgent,
    openCreate,
    paletteOpen,
    runTaskAction,
    state.agents,
    taskIsActive,
    toggleFullscreen,
  ]);

  const profileSummary = `${settings.roleProfiles.worker.model.split('-').slice(-1)[0]}·${settings.roleProfiles.worker.effort}`;

  return (
    <div
      ref={appShellRef}
      className={`app-shell density-${prefs.density} ${railCollapsed ? 'rail-collapsed' : ''} ${railDrawerOpen ? 'rail-drawer-open' : ''} ${prefs.consoleOpen ? 'console-open' : ''} ${prefs.consoleCollapsed ? 'console-collapsed' : ''}`}
      style={
        {
          '--console-height': `${prefs.consoleHeight}px`,
          '--workspace-split-percent': `${prefs.workspaceSplitPercent}%`,
        } as CSSProperties
      }
    >
      {railDrawerOpen && (
        <button
          type="button"
          className="rail-scrim"
          aria-label="Close task list"
          onClick={() => setRailDrawerOpen(false)}
        />
      )}
      <aside className="task-rail" id="task-rail">
        <div className="brand">
          <div className="brand-mark">
            <Command size={17} />
          </div>
          <div>
            <strong>Orchestrator</strong>
            <span>Task workspace</span>
          </div>
          <button
            type="button"
            onClick={() => setRailCollapsed((value) => !value)}
            title={railCollapsed ? 'Expand task rail' : 'Collapse task rail'}
            aria-label={railCollapsed ? 'Expand task rail' : 'Collapse task rail'}
            aria-controls="task-rail"
            aria-expanded={!railCollapsed}
          >
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
          <button
            type="button"
            className="rail-kbtn"
            onClick={() => setPaletteOpen(true)}
            title="Command palette"
            aria-label="Open command palette"
          >
            ⌘K
          </button>
        </div>

        <div className="rail-filters" role="tablist" aria-label="Task filters">
          {(
            [
              ['all', 'All'],
              ['active', 'Active'],
              ['failed', 'Failed'],
              ['done', 'Done'],
            ] as const
          ).map(([id, label], index, filters) => (
            <button
              key={id}
              type="button"
              role="tab"
              aria-selected={prefs.railFilter === id}
              aria-controls="task-list"
              tabIndex={prefs.railFilter === id ? 0 : -1}
              className={prefs.railFilter === id ? 'active' : ''}
              onClick={() => updatePrefs({ railFilter: id })}
              onKeyDown={(event) => {
                let nextIndex: number | undefined;
                if (event.key === 'ArrowRight') nextIndex = (index + 1) % filters.length;
                else if (event.key === 'ArrowLeft')
                  nextIndex = (index - 1 + filters.length) % filters.length;
                else if (event.key === 'Home') nextIndex = 0;
                else if (event.key === 'End') nextIndex = filters.length - 1;
                if (nextIndex == null) return;
                event.preventDefault();
                updatePrefs({ railFilter: filters[nextIndex][0] });
                const tabs =
                  event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>(
                    '[role="tab"]',
                  );
                tabs?.[nextIndex]?.focus();
              }}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="rail-label">
          <span>Tasks</span>
          <div>
            <button
              type="button"
              onClick={openCreate}
              title="New hierarchy task"
              aria-label="New hierarchy task"
            >
              <Plus size={12} />
            </button>
            <button
              type="button"
              onClick={() => void refreshTasks(true)}
              title="Refresh tasks"
              aria-label="Refresh tasks"
            >
              <RefreshCw size={12} />
            </button>
          </div>
        </div>

        <div className="task-list" id="task-list" role="tabpanel">
          {groupedTasks.map(([project, projectTasks]) => (
            <div className="task-group" key={project}>
              <div className="task-group-label" title={project}>
                {project}
              </div>
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
                      onClick={() => selectTask(task)}
                      title={`${task.name} · ${sentenceStatus(task.status)}`}
                      aria-label={`Open ${task.name}, ${sentenceStatus(task.status)}`}
                      aria-pressed={task.id === selectedTask?.id}
                    >
                      <span className={`task-status-strip tone-${tone}`} />
                      <span className={`task-icon role-worker`}>
                        <Layers3 size={15} />
                      </span>
                      <span className="task-card-copy">
                        <strong title={task.name}>{shortTaskTitle(task)}</strong>
                        <small>
                          {sentenceStatus(task.status)}
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
                          <button
                            type="button"
                            role="menuitem"
                            onClick={() => {
                              setMenuTaskId(null);
                              selectTask(task);
                            }}
                          >
                            <FolderOpen size={13} /> Open task
                          </button>
                          <button
                            type="button"
                            role="menuitemcheckbox"
                            aria-checked={pinned}
                            onClick={() => {
                              setMenuTaskId(null);
                              togglePin(task.id);
                            }}
                          >
                            <Pin size={13} /> {pinned ? 'Unpin' : 'Pin'} task
                          </button>
                          <button
                            type="button"
                            role="menuitem"
                            className={active ? 'danger' : undefined}
                            disabled={busyAction || task.id === mockTask.id}
                            onClick={() => void runTaskAction(task)}
                          >
                            {active ? <CircleStop size={13} /> : <Play size={13} />}
                            {active ? 'Stop task' : 'Resume task'}
                          </button>
                          <button
                            type="button"
                            role="menuitem"
                            onClick={() => void copyTaskId(task.id)}
                          >
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
              <button type="button" className="primary-button" onClick={openCreate}>
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
          <button
            type="button"
            onClick={() => setSettingsOpen(true)}
            title="Workspace settings"
            aria-label="Workspace settings"
          >
            <Settings2 size={15} />
          </button>
        </div>
      </aside>

      <main className="workspace-main">
        {displayedTask ? (
          <>
            <header className="task-header">
              <div className="task-header-top">
                <button
                  type="button"
                  className="rail-drawer-toggle"
                  onClick={() => setRailDrawerOpen(true)}
                  aria-label="Open task list"
                  aria-controls="task-rail"
                  aria-expanded={railDrawerOpen}
                >
                  <PanelLeft size={16} />
                </button>
                <nav className="task-breadcrumb" aria-label="Task location">
                  <span title={displayedTask.root}>
                    <FolderGit2 size={13} /> {taskProjectKey(displayedTask.root)}
                  </span>
                  {displayedTask.phase && (
                    <>
                      <i aria-hidden="true">/</i>
                      <em title={`Phase: ${displayedTask.phase}`}>{displayedTask.phase}</em>
                    </>
                  )}
                  {displayedTask.current_agent && (
                    <>
                      <i aria-hidden="true">/</i>
                      <em title={`Current agent: ${displayedTask.current_agent}`}>
                        {displayedTask.current_agent}
                      </em>
                    </>
                  )}
                  <i aria-hidden="true">/</i>
                  <span className="task-id" title={`Task ID: ${displayedTask.id}`}>
                    #{displayedTask.id.slice(0, 8)}
                  </span>
                </nav>
                <span
                  className={`status-pill tone-${statusTone(displayedTask.status)}`}
                  title={`${sentenceStatus(displayedTask.status)} · ${
                    state.connection === 'live' ? 'live stream' : state.connection
                  } · defaults ${profileSummary}`}
                >
                  <span className={`pulse-dot ${state.connection}`} />
                  {sentenceStatus(displayedTask.status)}
                </span>
                <div className="task-header-actions" role="toolbar" aria-label="Task actions">
                  <button
                    type="button"
                    className="task-action-icon"
                    onClick={() => void toggleFullscreen()}
                    title="Toggle fullscreen (F11)"
                    aria-label={fullscreen ? 'Restore window' : 'Fullscreen'}
                    aria-pressed={fullscreen}
                  >
                    {fullscreen ? <Minimize2 size={15} /> : <Maximize2 size={15} />}
                  </button>
                  <button
                    type="button"
                    className="task-action-icon"
                    onClick={() => setPaletteOpen(true)}
                    title="Open command palette (Ctrl+K)"
                    aria-label="Commands"
                  >
                    <Command size={15} />
                  </button>
                  <button
                    type="button"
                    className="task-action-icon"
                    onClick={() => updatePrefs({ consoleOpen: !prefs.consoleOpen })}
                    aria-pressed={prefs.consoleOpen}
                    aria-label={prefs.consoleOpen ? 'Hide console' : 'Show console'}
                    title={prefs.consoleOpen ? 'Hide event console' : 'Show event console'}
                  >
                    <Terminal size={15} />
                  </button>
                  <button
                    type="button"
                    className="task-action-icon"
                    onClick={() => setSettingsOpen(true)}
                    title="Planning caps, concurrency and role defaults"
                    aria-label="Runtime settings"
                  >
                    <SlidersHorizontal size={15} />
                  </button>
                  {displayedTask.artifact?.zip_path && (
                    <a
                      className="task-action-icon"
                      href={`/api/tasks/${encodeURIComponent(displayedTask.id)}/artifacts/download`}
                      download
                      aria-label="Download artifacts"
                      title="Download artifacts"
                    >
                      <FolderOpen size={15} />
                    </a>
                  )}
                  <button
                    type="button"
                    className="task-action-icon danger"
                    onClick={() => void deleteTask(displayedTask)}
                    disabled={busyAction || taskIsActive || displayedTask.id === mockTask.id}
                    aria-label="Delete task"
                    title="Delete task"
                  >
                    <Trash2 size={15} />
                  </button>
                  <span className="action-divider" aria-hidden="true" />
                  <button
                    type="button"
                    className={`run-action ${taskIsActive ? 'danger-button danger-solid' : 'primary-button'}`}
                    onClick={() => void runTaskAction(displayedTask)}
                    disabled={busyAction || displayedTask.id === mockTask.id}
                    title={taskIsActive ? 'Stop this task (S)' : 'Resume this task (R)'}
                  >
                    {taskIsActive ? (
                      <>
                        <CircleStop size={15} /> Stop
                      </>
                    ) : (
                      <>
                        <Play size={15} /> Resume
                      </>
                    )}
                  </button>
                </div>
              </div>
              <div className="task-title-row">
                <h1 title={displayedTask.name}>{shortTaskTitle(displayedTask)}</h1>
                <PhaseStepper status={displayedTask.status} />
              </div>
              <div className="task-metrics">
                <div title={`${agentCount} agent instances have started`}>
                  <span>
                    <Bot size={14} />
                    Agents spawned
                  </span>
                  <strong>{agentCount}</strong>
                </div>
                <div title="Director, Managers and their planned children">
                  <span>
                    <Layers3 size={14} />
                    Primary plan
                  </span>
                  <strong>{plannedPrimaryAgents}</strong>
                </div>
                <div title={`${calledAgents} agents have issued at least one model call`}>
                  <span>
                    <Radio size={14} />
                    Agents called
                  </span>
                  <strong>{calledAgents}</strong>
                </div>
                <div title="Total provider attempts including replays">
                  <span>
                    <Gauge size={14} />
                    Model attempts
                  </span>
                  <strong>{state.fanout.requestAttempts}</strong>
                </div>
                <div title={`${runningAgents} running, ${waitingAgents} waiting`}>
                  <span>
                    <Sparkles size={14} />
                    Running
                  </span>
                  <strong>{runningAgents}</strong>
                </div>
                <div title="Distinct provider accounts actually used">
                  <span>
                    <KeyRound size={14} />
                    Accounts used
                  </span>
                  <strong>{accountsUsed}</strong>
                </div>
                <div title="Elapsed wall-clock time since the run started">
                  <span>
                    <Timer size={14} />
                    Elapsed
                  </span>
                  <strong>{totalElapsed}</strong>
                </div>
              </div>
            </header>

            <InterventionBanner
              visible={waitingInput || pendingApproval != null}
              title={
                pendingApproval
                  ? `Approval required · ${pendingApproval.kind.replaceAll('_', ' ')}${
                      pendingApproval.workstreamId ? ` · ${pendingApproval.workstreamId}` : ''
                    }`
                  : 'Input required'
              }
              message={
                pendingApproval
                  ? `${pendingApproval.reason} Target: ${pendingApproval.target}`
                  : 'Director is waiting for human input before continuing.'
              }
              onFocus={() => {
                updatePrefs({ consoleOpen: true });
                focusDirector();
              }}
              actions={
                pendingApproval ? (
                  <>
                    <button
                      type="button"
                      className="danger-button"
                      disabled={busyAction}
                      onClick={() => void decideApproval('rejected')}
                    >
                      Reject
                    </button>
                    <button
                      type="button"
                      className="primary-button"
                      disabled={busyAction}
                      onClick={() => void decideApproval('approved')}
                    >
                      Approve
                    </button>
                  </>
                ) : undefined
              }
            />

            <div className={`workspace-content${graphExpanded ? ' graph-expanded' : ''}`}>
              <TaskDashboard
                state={state}
                task={displayedTask}
                onOpenAgent={openAgent}
                graphExpanded={graphExpanded}
                onToggleGraph={() => setGraphExpanded((value) => !value)}
              />
              <WorkspaceSplitter
                value={prefs.workspaceSplitPercent}
                onChange={(workspaceSplitPercent) => updatePrefs({ workspaceSplitPercent })}
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
              collapsed={prefs.consoleCollapsed}
              height={prefs.consoleHeight}
              onToggleCollapsed={() => updatePrefs({ consoleCollapsed: !prefs.consoleCollapsed })}
              onResize={(height) => updatePrefs({ consoleHeight: height })}
              onOpenAgent={(id) => {
                const agent = state.agents[id];
                if (agent) openAgent(agent);
              }}
            />
          </>
        ) : (
          <div className="workspace-empty">
            <div className="workspace-empty-mark">
              <Bot size={27} />
            </div>
            <span className="eyebrow">Mission control</span>
            <h1>Standing by</h1>
            <p>
              {offline
                ? 'Backend offline — start the orchestrator on port 8000, then retry.'
                : 'Select a task from the rail or launch a new hierarchy run.'}
            </p>
            <div className="empty-actions">
              <button type="button" className="primary-button" onClick={openCreate}>
                <Sparkles size={14} /> New run
              </button>
              <button
                type="button"
                className="secondary-button"
                onClick={() => void refreshTasks(false)}
              >
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
                    onClick={openCreate}
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

      {confirmation && (
        <div className="confirmation-backdrop" onMouseDown={() => setConfirmation(undefined)}>
          <section
            className="confirmation-dialog"
            role="alertdialog"
            aria-modal="true"
            aria-labelledby="confirmation-title"
            onMouseDown={(event) => event.stopPropagation()}
          >
            <span className={`confirmation-icon ${confirmation.kind}`}>
              {confirmation.kind === 'delete' ? <Trash2 size={20} /> : <CircleStop size={20} />}
            </span>
            <div>
              <h2 id="confirmation-title">
                {confirmation.kind === 'delete' ? 'Delete this task?' : 'Stop this task?'}
              </h2>
              <p>
                {confirmation.kind === 'delete'
                  ? `"${shortTaskTitle(confirmation.task)}" and its persisted history will be removed.`
                  : 'Agents will halt safely after the current operation and the task can be resumed.'}
              </p>
            </div>
            <footer>
              <button
                type="button"
                className="ghost-button"
                onClick={() => setConfirmation(undefined)}
              >
                Cancel
              </button>
              <button
                type="button"
                className={
                  confirmation.kind === 'delete' ? 'danger-button danger-solid' : 'danger-button'
                }
                onClick={() => {
                  const current = confirmation;
                  setConfirmation(undefined);
                  if (current.kind === 'delete') void deleteTask(current.task, true);
                  else void runTaskAction(current.task, true);
                }}
              >
                {confirmation.kind === 'delete' ? 'Delete permanently' : 'Stop task'}
              </button>
            </footer>
          </section>
        </div>
      )}

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
                <h2>Settings</h2>
              </div>
              <button
                type="button"
                className="icon-button"
                onClick={() => setSettingsOpen(false)}
                aria-label="Close settings"
              >
                <X size={16} />
              </button>
            </header>

            <div className="settings-panel-body">
              <section className="settings-section">
                <header>
                  <h3>Planning capacity</h3>
                  <p>
                    Caps are maxima, never required counts. Director and Managers select only the
                    fan-out the plan needs.
                  </p>
                </header>
                <div className="limit-groups">
                  <div className="limit-group">
                    <div className="limit-group-heading">
                      <strong>Director fan-out</strong>
                      <span>How many workstreams the Director may open</span>
                    </div>
                    <RangeSetting
                      label="Maximum managers"
                      value={settings.maxManagers}
                      min={1}
                      max={32}
                      onChange={(value) =>
                        setSettings({
                          ...settings,
                          maxManagers: value,
                          maxParallelManagers: Math.min(settings.maxParallelManagers, value),
                        })
                      }
                    />
                  </div>
                  <div className="limit-group">
                    <div className="limit-group-heading">
                      <strong>Manager fan-out</strong>
                      <span>Independent coder work items per Manager</span>
                    </div>
                    <RangeSetting
                      label="Maximum coder tasks per manager"
                      value={Math.max(1, settings.maxWorkersPerManager - 1)}
                      min={1}
                      max={31}
                      onChange={(value) =>
                        setSettings({
                          ...settings,
                          maxWorkersPerManager: value + 1,
                          maxParallelWorkersPerManager:
                            settings.maxParallelWorkersPerManager == null
                              ? undefined
                              : Math.min(settings.maxParallelWorkersPerManager, value),
                        })
                      }
                    />
                    <p className="setting-hint">
                      The backend adds one dedicated Tester per Manager on top of this value.
                    </p>
                  </div>
                </div>
              </section>

              <section className="settings-section">
                <header>
                  <h3>Execution concurrency</h3>
                  <p>
                    Slots limit how many selected roles may run at the same time. They never change
                    how much work is planned.
                  </p>
                </header>
                <div className="limit-groups">
                  <div className="limit-group">
                    <div className="limit-group-heading">
                      <strong>Manager slots</strong>
                      <span>Workstreams executing in parallel</span>
                    </div>
                    <RangeSetting
                      label="Parallel Manager slots"
                      value={settings.maxParallelManagers}
                      min={1}
                      max={settings.maxManagers}
                      onChange={(value) => setSettings({ ...settings, maxParallelManagers: value })}
                    />
                  </div>
                  <div className="limit-group">
                    <div className="limit-group-heading">
                      <strong>Worker slots</strong>
                      <span>Coders executing in parallel</span>
                    </div>
                    <label className="toggle-setting compact-toggle">
                      <span>
                        <strong>Per-manager worker slots</strong>
                        <small>Optional; otherwise uses that manager’s coder cap.</small>
                      </span>
                      <input
                        type="checkbox"
                        checked={settings.maxParallelWorkersPerManager != null}
                        onChange={(event) =>
                          setSettings({
                            ...settings,
                            maxParallelWorkersPerManager: event.target.checked
                              ? Math.max(1, settings.maxWorkersPerManager - 1)
                              : undefined,
                          })
                        }
                      />
                    </label>
                    {settings.maxParallelWorkersPerManager != null && (
                      <RangeSetting
                        label="Parallel workers per manager"
                        value={settings.maxParallelWorkersPerManager}
                        min={1}
                        max={Math.max(1, settings.maxWorkersPerManager - 1)}
                        onChange={(value) =>
                          setSettings({
                            ...settings,
                            maxParallelWorkersPerManager: value,
                          })
                        }
                      />
                    )}
                    <RangeSetting
                      label="Global parallel worker slots"
                      value={settings.maxParallelWorkers}
                      min={1}
                      max={64}
                      onChange={(value) => setSettings({ ...settings, maxParallelWorkers: value })}
                    />
                  </div>
                </div>
              </section>

              <section className="settings-section">
                <header>
                  <h3>Role profiles</h3>
                  <p>Applied to new hierarchy runs. Live agents can still override per window.</p>
                </header>
                <div className="role-matrix">
                  {(['director', 'manager', 'worker', 'tester'] as ConfigurableRole[]).map(
                    (role) => (
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
                    ),
                  )}
                </div>
                <div className="preset-segment" role="group" aria-label="Quality presets">
                  {QUALITY_PRESETS.map((preset) => (
                    <button
                      key={preset.id}
                      type="button"
                      className={selectedQualityPreset === preset.id ? 'active' : ''}
                      aria-pressed={selectedQualityPreset === preset.id}
                      onClick={() =>
                        setSettings({
                          ...settings,
                          roleProfiles: preset.profiles,
                        })
                      }
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>
              </section>

              <section className="settings-section">
                <header>
                  <h3>Workspace</h3>
                  <p>Local UI density, offline behaviour and provider account health.</p>
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
                <div className="limit-group">
                  <div className="limit-group-heading">
                    <strong>Cookie account health</strong>
                    <span>Durable cooldown and lease state. Cookie values are never exposed.</span>
                  </div>
                  <div className="account-health-list">
                    {accountHealth.length ? (
                      accountHealth.map((account) => (
                        <div key={`${account.provider}:${account.accountId}`}>
                          <span className={`account-health-dot state-${account.state}`} />
                          <strong>{account.accountId}</strong>
                          <span>{account.state.replaceAll('_', ' ')}</span>
                          <span>{account.activeLeases} active</span>
                          <span>
                            {account.cooldownActive && account.cooldownUntil
                              ? `Cooldown until ${new Date(account.cooldownUntil * 1000).toLocaleTimeString()}`
                              : (account.reason ?? 'Available')}
                          </span>
                        </div>
                      ))
                    ) : (
                      <p className="setting-hint">No durable account health events yet.</p>
                    )}
                  </div>
                </div>
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

      <CommandPalette
        open={paletteOpen}
        onClose={() => setPaletteOpen(false)}
        commands={commands}
      />
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
      <span>
        <strong>{label}</strong>
        <output>{value}</output>
      </span>
      <input
        type="range"
        aria-label={label}
        min={min}
        max={max}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
      <small>
        {min} minimum <i /> {max} maximum
      </small>
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
      <header>
        <strong>{role}</strong>
        <span>default</span>
      </header>
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
  const [managers, setManagers] = useState(settings.maxManagers);
  const [workersPerManager, setWorkersPerManager] = useState(settings.maxWorkersPerManager);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  const plannedCoders = managers * Math.max(1, workersPerManager - 1);
  const workerSlots = launchWorkerSlots(managers, workersPerManager, settings.maxParallelWorkers);
  const managerSlots = launchManagerSlots(managers, settings.maxParallelManagers);

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
    setManagers(template.managers);
    setWorkersPerManager(template.workersPerManager);
    setTestCmd(template.testCmd);
    setProjectMode(template.projectMode);
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
        maxManagers: managers,
        maxParallelManagers: managerSlots,
        maxWorkersPerManager: Math.max(2, workersPerManager),
        maxParallelWorkersPerManager:
          settings.maxParallelWorkersPerManager == null
            ? undefined
            : Math.min(settings.maxParallelWorkersPerManager, Math.max(1, workersPerManager - 1)),
        // The wizard configures how wide the plan is; the global slot count has
        // to be at least that wide or the run silently executes a fraction of
        // the coders it just promised.
        maxParallelWorkers: workerSlots,
      };
      const created = await api.createHierarchyTask({
        name: name.trim() || taskProjectKey(root) || 'Untitled task',
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
      <section
        className="create-dialog wizard"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-run-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header>
          <div>
            <span className="eyebrow">Launch wizard · step {step + 1}/3</span>
            <h2 id="create-run-title">New run</h2>
          </div>
          <button type="button" onClick={onClose} aria-label="Close new run wizard">
            <X size={16} />
          </button>
        </header>

        <div className="wizard-steps" role="tablist" aria-label="New run steps">
          {['Folder', 'Goal', 'Shape'].map((label, index) => (
            <button
              key={label}
              type="button"
              role="tab"
              className={index === step ? 'active' : index < step ? 'done' : ''}
              aria-selected={index === step}
              aria-label={label}
              aria-controls={`create-step-${index}`}
              tabIndex={index === step ? 0 : -1}
              onClick={() => setStep(index)}
              onKeyDown={(event) => {
                let nextIndex: number | undefined;
                if (event.key === 'ArrowRight') nextIndex = (index + 1) % 3;
                else if (event.key === 'ArrowLeft') nextIndex = (index + 2) % 3;
                else if (event.key === 'Home') nextIndex = 0;
                else if (event.key === 'End') nextIndex = 2;
                if (nextIndex == null) return;
                event.preventDefault();
                setStep(nextIndex);
                const tabs =
                  event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>(
                    '[role="tab"]',
                  );
                tabs?.[nextIndex]?.focus();
              }}
            >
              <i>{index + 1}</i>
              {label}
            </button>
          ))}
        </div>

        {step === 0 && (
          <div className="create-grid" id="create-step-0" role="tabpanel" aria-label="Folder">
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
                <button
                  type="button"
                  className="folder-browse-button"
                  onClick={() => void chooseFolder()}
                >
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
          <div className="create-grid" id="create-step-1" role="tabpanel" aria-label="Goal">
            <label>
              <span>Task name</span>
              <input
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="Short display name"
              />
            </label>
            <label>
              <span>Test command</span>
              <input
                value={testCmd}
                onChange={(event) => setTestCmd(event.target.value)}
                placeholder="python -m pytest -q"
              />
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
          <div
            className="create-grid shape-step"
            id="create-step-2"
            role="tabpanel"
            aria-label="Shape"
          >
            <div className="limit-group">
              <div className="limit-group-heading">
                <strong>Planning capacity</strong>
                <span>Maxima the planners may select, not required counts</span>
              </div>
              <RangeSetting
                label="Maximum managers"
                value={managers}
                min={1}
                max={32}
                onChange={setManagers}
              />
              <RangeSetting
                label="Maximum coder tasks per manager"
                value={Math.max(1, workersPerManager - 1)}
                min={1}
                max={31}
                onChange={(value) => setWorkersPerManager(value + 1)}
              />
              <p className="setting-hint">
                One dedicated Tester per Manager is added by the backend on top of the coder tasks.
              </p>
            </div>
            <div className="limit-group">
              <div className="limit-group-heading">
                <strong>Execution concurrency</strong>
                <span>Worker slots follow this fan-out</span>
              </div>
              <div className="estimate-panel">
                <strong>Estimate</strong>
                <span>
                  Up to {managers} managers · up to {plannedCoders} coders · up to {managers}{' '}
                  testers. This launch runs {managerSlots} managers and {workerSlots} coders at a
                  time
                  {workerSlots < plannedCoders
                    ? `, so the remaining ${plannedCoders - workerSlots} queue for a slot.`
                    : ', which covers every coder in the plan.'}
                </span>
              </div>
            </div>
            <div className="create-wide">
              <HierarchyPreview managers={managers} workersPerManager={workersPerManager} />
            </div>
            <div className="launch-contract-preview create-wide">
              <strong>Plan contract preview</strong>
              <div>
                <span>Director fan-out</span>
                <b>1–{managers} workstreams</b>
              </div>
              <div>
                <span>Manager fan-out</span>
                <b>1–{Math.max(1, workersPerManager - 1)} coders + 1 tester</b>
              </div>
              <div>
                <span>Required contracts</span>
                <b>Scopes, evidence, risk and approval policy</b>
              </div>
              <div>
                <span>Completion gate</span>
                <b>All work and model calls reconciled</b>
              </div>
            </div>
          </div>
        )}

        {error && <p className="create-error">{error}</p>}
        <footer>
          <button type="button" className="ghost-button" onClick={onClose}>
            Cancel
          </button>
          {step > 0 && (
            <button
              type="button"
              className="ghost-button"
              onClick={() => setStep((value) => value - 1)}
            >
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
            <button
              type="button"
              className="primary-button"
              disabled={busy}
              onClick={() => void submit()}
            >
              <Sparkles size={14} /> {busy ? 'Launching…' : 'Launch'}
            </button>
          )}
        </footer>
      </section>
    </div>
  );
}
