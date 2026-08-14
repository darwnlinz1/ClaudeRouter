import { expect, test, type Page } from '@playwright/test';

const task = {
  id: 'task-responsive',
  name: 'Responsive acceptance task',
  prompt: 'Verify the task dashboard at supported viewports.',
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

const timelineEvents = [
  {
    sequence: 1,
    type: 'thinking',
    agent_instance_id: 'director-responsive',
    role: 'director',
    payload: { name: 'Director', text: 'Planning the hierarchy.' },
  },
  {
    sequence: 2,
    type: 'agent_started',
    agent_instance_id: 'manager-responsive',
    role: 'manager',
    workstream_id: 'stream-responsive',
    payload: { name: 'Interface manager', status: 'running' },
  },
  {
    sequence: 3,
    type: 'agent_started',
    agent_instance_id: 'worker-responsive',
    manager_id: 'manager-responsive',
    workstream_id: 'stream-responsive',
    role: 'worker',
    payload: { name: 'Graph worker', status: 'running' },
  },
];

async function installApiRoutes(page: Page) {
  await page.route(
    (url) => url.pathname.startsWith('/api/'),
    async (route) => {
      const url = new URL(route.request().url());
      if (url.pathname === '/api/session') {
        await route.fulfill({ json: { csrf_token: 'responsive-csrf' } });
        return;
      }
      if (url.pathname === '/api/tasks') {
        await route.fulfill({ json: { tasks: [task] } });
        return;
      }
      if (url.pathname === `/api/tasks/${task.id}/timeline`) {
        await route.fulfill({ json: { events: timelineEvents } });
        return;
      }
      if (url.pathname === `/api/tasks/${task.id}/approvals`) {
        await route.fulfill({
          json: {
            approvals: [
              {
                approval_id: 'approval-responsive',
                task_id: task.id,
                kind: 'destructive_action',
                target: 'workspace/demo',
                reason: 'Operator confirmation required.',
                status: 'pending',
                created_at: '2026-08-12T12:00:00Z',
              },
            ],
          },
        });
        return;
      }
      if (url.pathname === `/api/tasks/${task.id}`) {
        await route.fulfill({ json: task });
        return;
      }
      if (url.pathname === '/api/accounts') {
        await route.fulfill({ json: { accounts: [], active_leases: [] } });
        return;
      }
      await route.fulfill({ status: 404, json: { detail: 'not mocked' } });
    },
  );
}

for (const viewport of [
  { name: 'desktop-1024x768', width: 1024, height: 768 },
  { name: 'mobile-390x844', width: 390, height: 844 },
]) {
  test(`task dashboard remains operable at ${viewport.name}`, async ({ page }) => {
    const pageErrors: string[] = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installApiRoutes(page);

    await page.goto(`/#task=${task.id}`);
    await expect(page.getByRole('heading', { name: task.name })).toBeVisible();
    await expect(page.getByRole('tablist', { name: 'Task views' })).toBeVisible();
    await page.getByRole('tab', { name: 'Agents' }).click();
    await expect(page.locator('.flow-agent-label')).toHaveCount(3);
    await expect(
      page.locator('.flow-agent-label').getByText('Director', { exact: true }),
    ).toBeVisible();
    await expect(
      page.locator('.flow-agent-label').getByText('Interface manager', { exact: true }),
    ).toBeVisible();
    await expect(
      page.locator('.flow-agent-label').getByText('Graph worker', { exact: true }),
    ).toBeVisible();
    const graphBounds = await page.locator('.signal-graph .react-flow').boundingBox();
    expect(graphBounds?.width ?? 0).toBeGreaterThan(200);
    expect(graphBounds?.height ?? 0).toBeGreaterThan(120);
    await page.getByRole('tab', { name: 'Tests' }).click();
    await expect(page.getByText('No test evidence yet')).toBeVisible();
    await expect.poll(() => pageErrors).toEqual([]);

    if (viewport.width === 390) {
      await expect(page.getByRole('button', { name: 'Reject' })).toBeVisible();
      await expect(page.getByRole('button', { name: 'Approve' })).toBeVisible();
      await page.getByRole('button', { name: 'Delete', exact: true }).click();
      await expect(page.getByRole('button', { name: 'Cancel' })).toBeVisible();
      await page.getByRole('button', { name: 'Cancel' }).click();
    }

    const overflow = await page.evaluate(() => ({
      body: document.body.scrollWidth - document.body.clientWidth,
      document: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    }));
    expect(overflow.body, 'body horizontal overflow in CSS pixels').toBeLessThanOrEqual(1);
    expect(overflow.document, 'document horizontal overflow in CSS pixels').toBeLessThanOrEqual(1);
  });
}
