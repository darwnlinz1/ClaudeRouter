export const PREFS_KEY = 'orchestrator:ui-prefs:v1';

export type UiPrefs = {
  pinnedTaskIds: string[];
  density: 'comfortable' | 'compact';
  railFilter: 'all' | 'active' | 'failed' | 'done';
  consoleOpen: boolean;
};

const DEFAULT_PREFS: UiPrefs = {
  pinnedTaskIds: [],
  density: 'comfortable',
  railFilter: 'all',
  consoleOpen: false,
};

export function loadPrefs(): UiPrefs {
  try {
    const stored = JSON.parse(localStorage.getItem(PREFS_KEY) ?? '{}') as Partial<UiPrefs>;
    return {
      ...DEFAULT_PREFS,
      ...stored,
      pinnedTaskIds: stored.pinnedTaskIds ?? DEFAULT_PREFS.pinnedTaskIds,
    };
  } catch {
    return DEFAULT_PREFS;
  }
}

export function savePrefs(partial: Partial<UiPrefs>): UiPrefs {
  const next = { ...loadPrefs(), ...partial };
  localStorage.setItem(PREFS_KEY, JSON.stringify(next));
  return next;
}
