import { beforeEach, describe, expect, it } from 'vitest';
import {
  loadPrefs,
  normalizeWorkspaceSettings,
  PREFS_KEY,
  savePrefs,
  WORKSPACE_SPLIT_DEFAULT,
  WORKSPACE_SPLIT_MAX,
  WORKSPACE_SPLIT_MIN,
} from './preferences';
import type { WorkspaceSettings } from '../types';

const values = new Map<string, string>();
const storage: Storage = {
  get length() {
    return values.size;
  },
  clear: () => values.clear(),
  getItem: (key) => values.get(key) ?? null,
  key: (index) => [...values.keys()][index] ?? null,
  removeItem: (key) => values.delete(key),
  setItem: (key, value) => values.set(key, value),
};

Object.defineProperty(globalThis, 'localStorage', {
  configurable: true,
  value: storage,
});

describe('UI preferences', () => {
  beforeEach(() => storage.clear());

  it('uses a balanced workspace split by default', () => {
    expect(loadPrefs().workspaceSplitPercent).toBe(WORKSPACE_SPLIT_DEFAULT);
  });

  it('clamps stored panel sizes to the supported range', () => {
    storage.setItem(PREFS_KEY, JSON.stringify({ workspaceSplitPercent: 99 }));
    expect(loadPrefs().workspaceSplitPercent).toBe(WORKSPACE_SPLIT_MAX);

    storage.setItem(PREFS_KEY, JSON.stringify({ workspaceSplitPercent: 1 }));
    expect(loadPrefs().workspaceSplitPercent).toBe(WORKSPACE_SPLIT_MIN);
  });

  it('persists adjusted panel sizes', () => {
    const prefs = savePrefs({ workspaceSplitPercent: 52.5 });

    expect(prefs.workspaceSplitPercent).toBe(52.5);
    expect(loadPrefs().workspaceSplitPercent).toBe(52.5);
    expect(JSON.parse(storage.getItem(PREFS_KEY) ?? '{}').workspaceSplitPercent).toBe(52.5);
  });
});

describe('workspace settings', () => {
  const defaults: WorkspaceSettings = {
    maxManagers: 4,
    maxParallelManagers: 4,
    maxWorkersPerManager: 5,
    maxParallelWorkersPerManager: undefined,
    maxParallelWorkers: 8,
    mockWhenUnavailable: false,
    roleProfiles: {
      director: { model: 'claude-sonnet-5', effort: 'max' },
      manager: { model: 'claude-sonnet-5', effort: 'max' },
      worker: { model: 'claude-sonnet-5', effort: 'max' },
      tester: { model: 'claude-sonnet-5', effort: 'high' },
    },
  };

  it('clamps stale local settings to the backend request contract', () => {
    expect(
      normalizeWorkspaceSettings(
        {
          maxManagers: 99,
          maxParallelManagers: -3,
          maxWorkersPerManager: 50,
          maxParallelWorkersPerManager: 99,
          maxParallelWorkers: 0,
        },
        defaults,
      ),
    ).toMatchObject({
      maxManagers: 32,
      maxParallelManagers: 1,
      maxWorkersPerManager: 32,
      maxParallelWorkersPerManager: 31,
      maxParallelWorkers: 1,
    });
  });

  it('falls back from non-numeric persisted limits and preserves all role profiles', () => {
    const malformed = {
      maxManagers: 'many',
      maxParallelWorkers: Number.NaN,
      roleProfiles: {
        worker: { model: 'claude-sonnet-4-6', effort: 'medium' },
      },
    } as unknown as Partial<WorkspaceSettings>;
    const normalized = normalizeWorkspaceSettings(malformed, defaults);

    expect(normalized.maxManagers).toBe(4);
    expect(normalized.maxParallelWorkers).toBe(8);
    expect(normalized.roleProfiles.worker).toEqual({
      model: 'claude-sonnet-4-6',
      effort: 'medium',
    });
    expect(normalized.roleProfiles.director).toEqual(defaults.roleProfiles.director);
  });
});
