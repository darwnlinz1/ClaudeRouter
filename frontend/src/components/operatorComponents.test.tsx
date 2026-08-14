// @vitest-environment jsdom

import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, test, vi } from 'vitest';
import { mockTask } from '../data/mockWorkspace';
import { initialWorkspaceState } from '../store/workspaceReducer';
import { EventConsole } from './EventConsole';
import { InterventionBanner } from './InterventionBanner';
import { TaskDashboard } from './TaskDashboard';
import { ToastStack } from './ToastStack';

describe('operator interventions', () => {
  test('hidden intervention banner does not announce stale work', () => {
    const { container } = render(<InterventionBanner visible={false} onFocus={() => undefined} />);

    expect(container.innerHTML).toBe('');
  });

  test('visible intervention focuses the waiting agent', async () => {
    const onFocus = vi.fn();
    render(<InterventionBanner visible message="Approval is required." onFocus={onFocus} />);

    expect(screen.getByRole('alert').textContent).toContain('Approval is required.');
    await userEvent.click(screen.getByRole('button', { name: 'Respond' }));
    expect(onFocus).toHaveBeenCalledOnce();
  });

  test('toast stack announces and dismisses a global notification', async () => {
    const onDismiss = vi.fn();
    render(
      <ToastStack
        toasts={[{ id: 'task-failed', message: 'Background task failed', tone: 'error' }]}
        onDismiss={onDismiss}
      />,
    );

    expect(screen.getByRole('status').getAttribute('aria-live')).toBe('polite');
    await userEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(onDismiss).toHaveBeenCalledWith('task-failed');
  });

  test('dashboard tabs expose selection and support arrow-key focus', async () => {
    const user = userEvent.setup();
    render(
      <TaskDashboard
        state={initialWorkspaceState}
        task={mockTask}
        onOpenAgent={() => undefined}
        graphExpanded={false}
        onToggleGraph={() => undefined}
      />,
    );

    const overview = screen.getByRole('tab', { name: 'Overview' });
    const plan = screen.getByRole('tab', { name: 'Plan' });
    expect(overview.getAttribute('aria-selected')).toBe('true');
    expect(plan.getAttribute('aria-selected')).toBe('false');

    overview.focus();
    await user.keyboard('{ArrowRight}');

    expect(document.activeElement).toBe(plan);
    expect(plan.getAttribute('aria-selected')).toBe('true');
    expect(screen.getByRole('tabpanel').getAttribute('aria-label')).toBe('Plan');
  });

  test('console controls expose expanded and pressed states', async () => {
    const user = userEvent.setup();
    render(
      <EventConsole
        state={{
          ...initialWorkspaceState,
          eventCount: 1,
          signals: [
            {
              id: 'signal-1',
              sequence: 1,
              sourceAgentId: 'director-1',
              targetAgentId: 'manager-1',
              signalType: 'approval requested',
              summary: 'Approval is waiting.',
              timestamp: new Date(0).toISOString(),
            },
          ],
        }}
        open
        collapsed={false}
        onClose={() => undefined}
        onToggleCollapsed={() => undefined}
      />,
    );

    expect(
      screen.getByRole('button', { name: 'Collapse console' }).getAttribute('aria-expanded'),
    ).toBe('true');
    const maximize = screen.getByRole('button', { name: 'Maximize console' });
    expect(maximize.getAttribute('aria-pressed')).toBe('false');
    expect(
      (screen.getByRole('button', { name: 'approval requested' }) as HTMLButtonElement).disabled,
    ).toBe(true);
    await user.click(maximize);
    expect(
      screen.getByRole('button', { name: 'Restore console' }).getAttribute('aria-pressed'),
    ).toBe('true');
  });
});
