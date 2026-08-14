import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const frontendRoot = fileURLToPath(new URL('../', import.meta.url));
const readFrontendFile = (path: string) =>
  readFileSync(new URL(path, `file:///${frontendRoot.replaceAll('\\', '/')}/`), 'utf8');

describe('CSP-safe production assets', () => {
  it('uses only local HTML assets and the required policy', () => {
    const html = readFrontendFile('index.html');

    expect(html).toContain(
      "default-src 'self'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self'",
    );
    expect(html).not.toMatch(/\b(?:src|href)=["'](?:https?:)?\/\//i);
    expect(html).not.toMatch(/<script(?![^>]*\bsrc=)[^>]*>/i);
    expect(html).not.toMatch(/<style(?:\s|>)/i);
  });

  it('does not import remote styles, fonts, or images', () => {
    const styles = readFrontendFile('src/styles.css');

    expect(styles).not.toMatch(/@import\b/i);
    expect(styles).not.toMatch(/https?:\/\//i);
    expect(styles).not.toMatch(/\burl\s*\(/i);
    expect(styles).toContain("local('Cascadia Mono')");
    expect(styles).toContain('ui-sans-serif');
    expect(styles).toContain('system-ui');
  });

  it('keeps production source maps disabled', () => {
    const viteConfig = readFrontendFile('vite.config.ts');

    expect(viteConfig).toMatch(/\bsourcemap:\s*false\b/);
  });
});
