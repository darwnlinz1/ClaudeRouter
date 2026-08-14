// Reports the measured geometry of the graph heading controls at a given
// viewport, so overflow can be diagnosed from real layout instead of guesswork.
import { chromium } from '@playwright/test';
import { ACCOUNTS, APPROVALS, buildFixture } from './uiFixture.mjs';

const baseURL = process.env.CAPTURE_URL ?? 'http://127.0.0.1:4319';
const width = Number(process.argv[2] ?? 390);
const height = Number(process.argv[3] ?? 844);
const fixture = buildFixture({ managerCount: 6, workersPerManager: 3 });

const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width, height }, hasTouch: width < 600 });
const page = await context.newPage();

await page.route(
  (url) => url.pathname.startsWith('/api/'),
  async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === '/api/session') return route.fulfill({ json: { csrf_token: 'probe' } });
    if (url.pathname === '/api/tasks') return route.fulfill({ json: { tasks: fixture.tasks } });
    if (url.pathname === `/api/tasks/${fixture.task.id}/timeline`)
      return route.fulfill({ json: { events: fixture.events, has_more: false } });
    if (url.pathname.endsWith('/approvals')) return route.fulfill({ json: APPROVALS });
    if (url.pathname === '/api/accounts') return route.fulfill({ json: ACCOUNTS });
    if (url.pathname.startsWith('/api/stream/'))
      return route.fulfill({ contentType: 'text/event-stream', body: ': idle\n\n' });
    if (url.pathname.startsWith('/api/tasks/')) return route.fulfill({ json: fixture.task });
    return route.fulfill({ status: 404, json: { detail: 'not mocked' } });
  },
);

await page.goto(`${baseURL}/#task=${fixture.task.id}`, { waitUntil: 'load' });
await page.getByRole('tab', { name: 'Agents' }).click();
await page.waitForTimeout(600);

const report = await page.evaluate(() => {
  const describe = (element) => {
    const box = element.getBoundingClientRect();
    return {
      tag: element.tagName.toLowerCase(),
      cls: element.className?.toString().slice(0, 40),
      text: (element.textContent ?? '').trim().slice(0, 24),
      left: Math.round(box.left),
      right: Math.round(box.right),
      width: Math.round(box.width),
    };
  };
  const heading = document.querySelector('.dag-heading');
  const actions = document.querySelector('.dag-heading-actions');
  return {
    viewport: window.innerWidth,
    heading: heading ? describe(heading) : null,
    actions: actions ? describe(actions) : null,
    children: actions ? [...actions.children].map(describe) : [],
    offscreen: [...document.querySelectorAll('button, a, input, select')]
      .filter((element) => getComputedStyle(element).visibility !== 'hidden')
      .map(describe)
      .filter((item) => item.right > window.innerWidth + 1 || item.left < -1),
  };
});

console.log(JSON.stringify(report, null, 2));
await browser.close();
