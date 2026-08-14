// @vitest-environment jsdom

import { cleanup, fireEvent, render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, test, vi } from 'vitest';
import {
  WORKSPACE_SPLIT_DEFAULT,
  WORKSPACE_SPLIT_MAX,
  WORKSPACE_SPLIT_MIN,
  WORKSPACE_SPLIT_STEP,
} from '../lib/preferences';
import { WorkspaceSplitter } from './WorkspaceSplitter';

afterEach(cleanup);

describe('WorkspaceSplitter', () => {
  test('exposes an accessible horizontal separator', () => {
    const { getByRole } = render(<WorkspaceSplitter value={52} onChange={() => undefined} />);

    const separator = getByRole('separator');
    expect(separator.getAttribute('aria-orientation')).toBe('horizontal');
    expect(separator.getAttribute('aria-valuemin')).toBe(String(WORKSPACE_SPLIT_MIN));
    expect(separator.getAttribute('aria-valuemax')).toBe(String(WORKSPACE_SPLIT_MAX));
    expect(separator.getAttribute('aria-valuenow')).toBe('52');
  });

  test('adjusts with arrow keys and resets on double-click', () => {
    const onChange = vi.fn();
    const { getByRole } = render(<WorkspaceSplitter value={52} onChange={onChange} />);
    const separator = getByRole('separator');

    fireEvent.keyDown(separator, { key: 'ArrowDown' });
    expect(onChange).toHaveBeenLastCalledWith(52 + WORKSPACE_SPLIT_STEP);

    fireEvent.keyDown(separator, { key: 'ArrowUp' });
    expect(onChange).toHaveBeenLastCalledWith(52 - WORKSPACE_SPLIT_STEP);

    fireEvent.doubleClick(separator);
    expect(onChange).toHaveBeenLastCalledWith(WORKSPACE_SPLIT_DEFAULT);
  });

  test('is reachable with keyboard focus', async () => {
    const user = userEvent.setup();
    const { getByRole } = render(<WorkspaceSplitter value={52} onChange={() => undefined} />);

    await user.tab();

    expect(document.activeElement).toBe(getByRole('separator'));
  });
});
