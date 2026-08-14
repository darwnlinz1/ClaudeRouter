import type { ConfigurableRole, WorkspaceSettings } from '../types';

export const PREFS_KEY = 'orchestrator:ui-prefs:v1';
export const WORKSPACE_SPLIT_DEFAULT = 56;
export const WORKSPACE_SPLIT_MIN = 30;
export const WORKSPACE_SPLIT_MAX = 70;
export const WORKSPACE_SPLIT_STEP = 2;

export type UiPrefs = {
  pinnedTaskIds: string[];
  density: 'comfortable' | 'compact';
  railFilter: 'all' | 'active' | 'failed' | 'done';
  consoleOpen: boolean;
  consoleCollapsed: boolean;
  consoleHeight: number;
  workspaceSplitPercent: number;
};

const DEFAULT_PREFS: UiPrefs = {
  pinnedTaskIds: [],
  density: 'comfortable',
  railFilter: 'all',
  consoleOpen: false,
  consoleCollapsed: false,
  consoleHeight: 132,
  workspaceSplitPercent: WORKSPACE_SPLIT_DEFAULT,
};

const normalizePrefs = (stored: Partial<UiPrefs>): UiPrefs => {
  const consoleHeight = Number(stored.consoleHeight);
  const workspaceSplitPercent = Number(stored.workspaceSplitPercent);
  return {
    ...DEFAULT_PREFS,
    ...stored,
    pinnedTaskIds: stored.pinnedTaskIds ?? DEFAULT_PREFS.pinnedTaskIds,
    consoleHeight: Number.isFinite(consoleHeight)
      ? Math.min(480, Math.max(72, consoleHeight))
      : DEFAULT_PREFS.consoleHeight,
    workspaceSplitPercent: Number.isFinite(workspaceSplitPercent)
      ? Math.min(WORKSPACE_SPLIT_MAX, Math.max(WORKSPACE_SPLIT_MIN, workspaceSplitPercent))
      : DEFAULT_PREFS.workspaceSplitPercent,
  };
};

const boundedInteger = (value: unknown, fallback: number, minimum: number, maximum: number) => {
  const parsed = Number(value);
  const selected = Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
  return Math.min(maximum, Math.max(minimum, selected));
};

export function normalizeWorkspaceSettings(
  stored: Partial<WorkspaceSettings>,
  defaults: WorkspaceSettings,
): WorkspaceSettings {
  const maxManagers = boundedInteger(
    stored.maxManagers ?? stored.maxParallelManagers,
    defaults.maxManagers,
    1,
    32,
  );
  const maxWorkersPerManager = boundedInteger(
    stored.maxWorkersPerManager,
    defaults.maxWorkersPerManager,
    2,
    32,
  );
  const coderCap = maxWorkersPerManager - 1;
  const roles: ConfigurableRole[] = ['director', 'manager', 'worker', 'tester'];
  const roleProfiles = Object.fromEntries(
    roles.map((role) => [
      role,
      {
        ...defaults.roleProfiles[role],
        ...(stored.roleProfiles?.[role] ?? {}),
      },
    ]),
  ) as WorkspaceSettings['roleProfiles'];
  const perManagerSlots = stored.maxParallelWorkersPerManager;
  return {
    ...defaults,
    ...stored,
    maxManagers,
    maxParallelManagers: boundedInteger(
      stored.maxParallelManagers,
      Math.min(defaults.maxParallelManagers, maxManagers),
      1,
      maxManagers,
    ),
    maxWorkersPerManager,
    maxParallelWorkersPerManager:
      perManagerSlots == null ? undefined : boundedInteger(perManagerSlots, coderCap, 1, coderCap),
    maxParallelWorkers: boundedInteger(
      stored.maxParallelWorkers,
      defaults.maxParallelWorkers,
      1,
      64,
    ),
    mockWhenUnavailable:
      typeof stored.mockWhenUnavailable === 'boolean'
        ? stored.mockWhenUnavailable
        : defaults.mockWhenUnavailable,
    roleProfiles,
  };
}

export function loadPrefs(): UiPrefs {
  try {
    const stored = JSON.parse(localStorage.getItem(PREFS_KEY) ?? '{}') as Partial<UiPrefs>;
    return normalizePrefs(stored);
  } catch {
    return DEFAULT_PREFS;
  }
}

export function savePrefs(partial: Partial<UiPrefs>): UiPrefs {
  const next = normalizePrefs({ ...loadPrefs(), ...partial });
  localStorage.setItem(PREFS_KEY, JSON.stringify(next));
  return next;
}
