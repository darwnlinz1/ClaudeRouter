import { expect, test } from '@playwright/test';

test('operator can route to a task and confirm destructive actions', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  let deleted = false;
  const task = {
    id: 'task-e2e',
    name: 'E2E coordination run',
    prompt: 'Verify the operator workflow.',
    root: 'C:/workspace/demo',
    mode: 'orchestrator',
    status: 'COMPLETED',
    phase: 'finished',
    settings: {
      max_managers: 2,
      max_parallel_managers: 2,
      max_workers_per_manager: 3,
      max_parallel_workers: 4,
    },
    events: [],
    hierarchy: {},
  };

  await page.route(
    (url) => url.pathname.startsWith('/api/'),
    async (route) => {
      const url = new URL(route.request().url());
      const method = route.request().method();
      if (url.pathname === '/api/session') {
        await route.fulfill({ json: { csrf_token: 'test-csrf' } });
        return;
      }
      if (url.pathname === '/api/tasks' && method === 'GET') {
        await route.fulfill({ json: { tasks: deleted ? [] : [task] } });
        return;
      }
      if (url.pathname === '/api/tasks/task-e2e/timeline') {
        await route.fulfill({ json: { events: [] } });
        return;
      }
      if (url.pathname === '/api/tasks/task-e2e/approvals') {
        await route.fulfill({ json: { approvals: [] } });
        return;
      }
      if (url.pathname === '/api/tasks/task-e2e' && method === 'GET') {
        await route.fulfill({ json: task });
        return;
      }
      if (url.pathname === '/api/tasks/task-e2e' && method === 'DELETE') {
        deleted = true;
        await route.fulfill({ json: { status: 'deleted' } });
        return;
      }
      if (url.pathname === '/api/accounts') {
        await route.fulfill({ json: { accounts: [], active_leases: [] } });
        return;
      }
      await route.fulfill({ status: 404, json: { detail: 'not mocked' } });
    },
  );

  await page.goto('/#task=task-e2e');
  await expect.poll(() => pageErrors).toEqual([]);
  await expect(page.getByRole('heading', { name: 'E2E coordination run' })).toBeVisible();
  await expect(page).toHaveURL(/#task=task-e2e$/);

  await page.getByRole('button', { name: 'Delete' }).click();
  await expect(page.getByRole('alertdialog')).toContainText('Delete this task?');
  await page.getByRole('button', { name: 'Cancel' }).click();
  await expect(page.getByRole('alertdialog')).toHaveCount(0);

  await page.getByRole('button', { name: 'Delete' }).click();
  await page.getByRole('button', { name: 'Delete permanently' }).click();
  await expect(page.getByText('Task deleted')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'E2E coordination run' })).toHaveCount(0);
});
