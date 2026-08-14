// Screenshot harness: boots the dev server routes through Playwright with a mocked
// orchestrator API so the operator surface can be reviewed at real viewports.
import { mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from '@playwright/test';
import { ACCOUNTS, APPROVALS, buildFixture } from './uiFixture.mjs';

const label = process.argv[2] ?? 'before';
const baseURL = process.env.CAPTURE_URL ?? 'http://127.0.0.1:4319';
const outDir = path.resolve(process.cwd(), '.ui-screens', label);

const VIEWPORTS = [
  { name: 'desktop-1440x900', width: 1440, height: 900 },
  { name: 'laptop-1024x768', width: 1024, height: 768 },
  { name: 'mobile-390x844', width: 390, height: 844 },
];

const fixture = buildFixture({ managerCount: 6, workersPerManager: 3 });
const wide = buildFixture({ managerCount: 16, workersPerManager: 2 });

async function installRoutes(page, data) {
  await page.route(
    (url) => url.pathname.startsWith('/api/'),
    async (route) => {
      const url = new URL(route.request().url());
      if (url.pathname === '/api/session') {
        await route.fulfill({ json: { csrf_token: 'capture' } });
        return;
      }
      if (url.pathname === '/api/tasks') {
        await route.fulfill({ json: { tasks: data.tasks } });
        return;
      }
      if (url.pathname === `/api/tasks/${data.task.id}/timeline`) {
        await route.fulfill({ json: { events: data.events, has_more: false } });
        return;
      }
      if (url.pathname.endsWith('/approvals')) {
        await route.fulfill({ json: APPROVALS });
        return;
      }
      if (url.pathname === '/api/accounts') {
        await route.fulfill({ json: ACCOUNTS });
        return;
      }
      if (url.pathname.startsWith('/api/stream/')) {
        await route.fulfill({ contentType: 'text/event-stream', body: ': idle\n\n' });
        return;
      }
      if (url.pathname.startsWith('/api/tasks/')) {
        const id = url.pathname.split('/')[3];
        const match = data.tasks.find((candidate) => candidate.id === id) ?? data.task;
        await route.fulfill({ json: match });
        return;
      }
      await route.fulfill({ status: 404, json: { detail: 'not mocked' } });
    },
  );
}

async function shoot(page, name) {
  await page.waitForTimeout(700);
  await page.screenshot({ path: path.join(outDir, `${name}.png`) });
  process.stdout.write(`  ${name}.png\n`);
}

const run = async () => {
  await rm(outDir, { recursive: true, force: true });
  await mkdir(outDir, { recursive: true });
  const browser = await chromium.launch();

  for (const viewport of VIEWPORTS) {
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      deviceScaleFactor: 1,
      hasTouch: viewport.width < 600,
    });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await installRoutes(page, fixture);
    await page.goto(`${baseURL}/#task=${fixture.task.id}`, { waitUntil: 'load' });
    await page.waitForSelector('.task-metrics, .workspace-empty');
    process.stdout.write(`${viewport.name}\n`);

    await shoot(page, `${viewport.name}-01-overview`);

    await page.getByRole('tab', { name: 'Plan' }).click();
    await shoot(page, `${viewport.name}-02-plan`);

    await page.getByRole('tab', { name: 'Agents' }).click();
    await shoot(page, `${viewport.name}-03-agents`);

    const fullscreen = page.getByRole('button', { name: /Fullscreen graph/i });
    if (await fullscreen.count()) {
      await fullscreen.first().click();
      await shoot(page, `${viewport.name}-04-graph-fullscreen`);
      await page.keyboard.press('Escape');
      await page.waitForTimeout(200);
    }

    await page.getByRole('tab', { name: 'Calls' }).click();
    await shoot(page, `${viewport.name}-05-calls`);

    await page.getByRole('tab', { name: 'Changes' }).click();
    await shoot(page, `${viewport.name}-06-changes`);

    await page.getByRole('tab', { name: 'Overview' }).click();
    await page.waitForTimeout(200);

    const limits = page.getByRole('button', { name: /Runtime settings|Limits/i });
    if (await limits.count()) {
      await limits.first().click();
      await shoot(page, `${viewport.name}-07-settings`);
      await page.keyboard.press('Escape');
      await page.waitForTimeout(200);
    }

    const drawerToggle = page.getByRole('button', { name: /Open task list/i });
    if (await drawerToggle.count()) {
      await drawerToggle.first().click();
      await page.waitForTimeout(300);
      await shoot(page, `${viewport.name}-10-rail-drawer`);
    }

    await page.getByRole('button', { name: 'New hierarchy task' }).click();
    await page.waitForTimeout(250);
    await shoot(page, `${viewport.name}-08-wizard-folder`);
    const shapeTab = page.getByRole('tab', { name: /Shape/ });
    await shapeTab.evaluate((button) => button.click());
    await shoot(page, `${viewport.name}-09-wizard-shape`);
    await page.keyboard.press('Escape');

    const overflow = await page.evaluate(() => ({
      body: document.body.scrollWidth - document.body.clientWidth,
      doc: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    }));
    process.stdout.write(
      `  overflow body=${overflow.body} doc=${overflow.doc} errors=${errors.length ? errors.join(' | ') : 'none'}\n`,
    );
    await context.close();
  }

  // Fan-out stress: 16 managers must still fit the graph viewport.
  const stressContext = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const stressPage = await stressContext.newPage();
  await installRoutes(stressPage, wide);
  await stressPage.goto(`${baseURL}/#task=${wide.task.id}`, { waitUntil: 'load' });
  await stressPage.waitForSelector('.task-metrics');
  await stressPage.getByRole('tab', { name: 'Agents' }).click();
  process.stdout.write('stress-16-managers\n');
  await shoot(stressPage, 'stress-16-managers-graph');
  await stressContext.close();

  await browser.close();
};

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
