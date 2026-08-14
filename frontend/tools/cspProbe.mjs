import { chromium } from '@playwright/test';

const browser = await chromium.launch();
const page = await browser.newPage();
const violations = [];
page.on('console', (message) => {
  if (message.text().toLowerCase().includes('content security policy')) {
    violations.push(message.text().slice(0, 200));
  }
});
await page.route('**/api/**', (route) => route.fulfill({ json: { tasks: [] } }));
await page.goto('http://127.0.0.1:4319/', { waitUntil: 'load' });
const result = await page.evaluate(() => {
  const probe = document.createElement('div');
  probe.setAttribute('style', 'width: 137px');
  document.body.append(probe);
  const applied = getComputedStyle(probe).width;
  probe.remove();
  const sheet = document.createElement('style');
  sheet.textContent = '.csp-probe { color: rgb(1, 2, 3); }';
  document.head.append(sheet);
  const marker = document.createElement('span');
  marker.className = 'csp-probe';
  document.body.append(marker);
  const injected = getComputedStyle(marker).color;
  marker.remove();
  sheet.remove();
  return { inlineAttribute: applied, injectedStyleElement: injected };
});
console.log(JSON.stringify({ ...result, violations }, null, 2));
await browser.close();
