import { beforeEach, describe, expect, it } from 'vitest';
import {
  loadPrefs,
  PREFS_KEY,
  savePrefs,
  WORKSPACE_SPLIT_DEFAULT,
  WORKSPACE_SPLIT_MAX,
  WORKSPACE_SPLIT_MIN,
} from './preferences';

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
