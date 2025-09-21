import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: 'e2e',
  // Cap each test at 20s total
  timeout: 30_000,
  expect: {
    timeout: 30_000
  },
  use: {
    baseURL: 'http://127.0.0.1:5000',
    // Keep UI actions and navigations snappy
    actionTimeout: 15_000,
    navigationTimeout: 15_000,
    trace: 'on-first-retry'
  },
  retries: 0
});

