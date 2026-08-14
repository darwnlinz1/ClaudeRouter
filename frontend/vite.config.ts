import { defineConfig } from 'vitest/config';
import type { Plugin } from 'vite';
import react from '@vitejs/plugin-react';

// The dev server injects styles as inline <style> tags, which the shipped
// policy forbids, so the dev server would otherwise render unstyled.
const devInlineStyles = (): Plugin => ({
  name: 'dev-inline-styles',
  apply: 'serve',
  transformIndexHtml: (html) =>
    html.replace("style-src 'self'", "style-src 'self' 'unsafe-inline'"),
});

export default defineConfig({
  plugins: [react(), devInlineStyles()],
  base: './',
  server: {
    port: 5173,
    proxy: {
      '/api': process.env.VITE_DEV_API_TARGET ?? 'http://127.0.0.1:8000',
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: false,
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            {
              name: 'graph',
              test: /node_modules[\\/]@xyflow[\\/]/,
            },
            {
              name: 'dock',
              test: /node_modules[\\/]dockview/,
            },
            {
              name: 'icons',
              test: /node_modules[\\/]lucide-react/,
            },
            {
              name: 'react-vendor',
              test: /node_modules[\\/](react|react-dom|scheduler)[\\/]/,
            },
          ],
        },
      },
    },
  },
  test: {
    environment: 'node',
    include: ['src/**/*.{test,spec}.{ts,tsx}'],
  },
});
