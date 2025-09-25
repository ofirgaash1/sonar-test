import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: 'e2e',
  // Cap each test at 45s total
  timeout: 45_000,
  expect: {
    timeout: 30_000
  },
  use: {
    baseURL: 'http://127.0.0.1:5000',
    // Allow longer timeouts for save operations that may take time
    actionTimeout: 90_000,
    navigationTimeout: 15_000,
    trace: 'on-first-retry'
  },
  retries: 0,
  // Fail fast on first failure to avoid wasting time on repeated errors
  workers: 1,
  fullyParallel: false,
  // Stop on first failure
  forbidOnly: false,
  // Exit on first failure
  maxFailures: 1
});

