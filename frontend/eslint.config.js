import js from '@eslint/js';
import { defineConfig, globalIgnores } from 'eslint/config';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default defineConfig(globalIgnores(['dist/**', 'node_modules/**', 'test-results/**']), {
  files: ['**/*.{ts,tsx}'],
  extends: [js.configs.recommended, ...tseslint.configs.recommended],
  languageOptions: {
    ecmaVersion: 2022,
    globals: globals.browser,
  },
  plugins: {
    'react-hooks': reactHooks,
    'react-refresh': reactRefresh,
  },
  rules: {
    ...reactHooks.configs.flat.recommended.rules,
    'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
    'react-hooks/set-state-in-effect': 'off',
    'react-hooks/refs': 'off',
    '@typescript-eslint/no-explicit-any': 'error',
  },
});
