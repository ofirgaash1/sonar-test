import { test, expect } from '@playwright/test';
import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import os from 'os';
import zlib from 'zlib';

async function waitForServer(url: string, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const r = await fetch(url);
      if (r.ok || r.status === 401 || r.status === 404) return;
    } catch {}
    await new Promise(r => setTimeout(r, 300));
  }
  throw new Error('Backend did not start');
}

function writeGzJson(p: string, obj: any) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const buf = Buffer.from(JSON.stringify(obj), 'utf-8');
  const gz = zlib.gzipSync(buf);
  fs.writeFileSync(p, gz);
}

test.describe('Editor E2E', () => {
  let tmpDir: string;
  let proc: any;
  let folder = 'TestFolder';
  let file = 'episode.opus';
  let useDataDir: string;

  function findFirstEpisodeInRepo(): { folder: string, file: string } | null {
    try {
      const root = process.cwd();
      const jsonRoot = path.join(root, 'explore', '..', 'json'); // fallback if repo layout differs
      const jsonAlt = path.join(root, 'json');
      const bases = [jsonAlt, jsonRoot].filter(p => fs.existsSync(p));
      for (const base of bases) {
        // Look for */*/full_transcript.json.gz
        const stack: string[] = [base];
        while (stack.length) {
          const dir = stack.pop()!;
          const ents = fs.readdirSync(dir, { withFileTypes: true });
          for (const e of ents) {
            const p = path.join(dir, e.name);
            if (e.isDirectory()) { stack.push(p); continue; }
            if (e.isFile() && e.name === 'full_transcript.json.gz') {
              // folder is parent of parent dir; file stem is parent dir name
              const parent = path.dirname(p);
              const folderName = path.basename(path.dirname(parent));
              const stem = path.basename(parent);
              return { folder: folderName, file: `${stem}.opus` };
            }
          }
        }
      }
    } catch {}
    return null;
  }

  test.beforeAll(async () => {
    // Prefer real repository data if present; else create a temp dataset
    const found = findFirstEpisodeInRepo();
    if (found) { folder = found.folder; file = found.file; useDataDir = process.cwd(); }
    else {
      tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'explore-e2e-'));
      const tr = {
        segments: [
          { text: 'hello ', start: 0.0, end: 0.6, words: [ { word: 'hello', start: 0.0, end: 0.4 }, { word: ' ', start: 0.4, end: 0.6 } ] },
          { text: 'world', start: 0.6, end: 1.0, words: [ { word: 'world', start: 0.6, end: 1.0 } ] }
        ]
      };
      const p = path.join(tmpDir, 'json', folder, path.parse(file).name, 'full_transcript.json.gz');
      writeGzJson(p, tr);
      useDataDir = tmpDir;
    }

    // Start backend in dev mode (bypass auth)
    proc = spawn('python', ['explore/run.py', '--data-dir', useDataDir, '--dev'], {
      env: {
        ...process.env,
        // Prefer WAL journaling for fewer lock errors during tests
        SQLITE_JOURNAL: 'WAL',
        FLASK_ENV: 'development',
        TS_USER_EMAIL: 'tester@example.com',
        LOG_LEVEL: 'INFO',
        TQDM_DISABLE: '1'
      },
      stdio: ['ignore', fs.openSync('app_stdout.log', 'a'), fs.openSync('app_stderr.log', 'a')]
    });
    await waitForServer('http://127.0.0.1:5000/folders');
  });

  test.afterAll(async () => {
    try { proc?.kill(); } catch {}
    try { if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  });

  test('Load → Edit → Save → Version badge', async ({ page }) => {
    await page.goto('/v2');
    // Click folder and file using precise lists
    await page.locator('#folders .item', { hasText: folder }).first().click();
    await page.locator('#files .item', { hasText: path.parse(file).name }).first().click();
    // Wait transcript to populate with any non-whitespace content
    const transcript = page.locator('#transcript');
    await expect(transcript).toHaveText(/\S/);
    // Edit: add exclamation
    await transcript.click();
    await page.keyboard.type('!');
    // Save
    await page.locator('#submitBtn').click();
    // Version badge shows גרסה 1
    const badge = page.locator('#versionBadge');
    await expect(badge).toContainText('גרסה');
  });

  test('409 conflict → merge modal → auto-merge', async ({ page, request }) => {
    await page.goto('/v2');
    // Open episode
    await page.locator('#folders .item', { hasText: folder }).first().click();
    await page.locator('#files .item', { hasText: path.parse(file).name }).first().click();
    // First save → v1
    await page.locator('#transcript').click();
    await page.keyboard.type('!');
    // Click save (UI will queue if not idle)
    await page.locator('#submitBtn').click();
    await expect(page.locator('#versionBadge')).toContainText('גרסה');

    // Create server v2 with non-overlapping edit via API
    const doc = `${folder}/${file}`;
    const latest = await (await fetch(`http://127.0.0.1:5000/transcripts/latest?doc=${encodeURIComponent(doc)}`)).json();
    const baseHash = latest.base_sha256 || '';
    const newText = latest.text + ' more';
    const resp = await fetch('http://127.0.0.1:5000/transcripts/save', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ doc, parentVersion: latest.version, expected_base_sha256: baseHash, text: newText, words: [{ word: newText }] })
    });
    if (!resp.ok) throw new Error('pre-save v2 failed');

    // Now client edit and save → should conflict
    await page.locator('#transcript').click();
    await page.keyboard.type('!');
    await page.locator('#submitBtn').click();
    // Merge modal visible
    await expect(page.locator('#mergeModal .modal-content')).toBeVisible();
    // Auto-merge
    await page.locator('#mergeTry').click();
    // After merge save, badge updates
    await expect(page.locator('#versionBadge')).toContainText('גרסה');
  });

  test('Confirmations select → save → reload', async ({ page }) => {
    await page.goto('/v2');
    await page.locator('#folders .item', { hasText: folder }).first().click();
    await page.locator('#files .item', { hasText: path.parse(file).name }).first().click();
    // Save to ensure version exists
    await page.locator('#transcript').click();
    await page.locator('#submitBtn').click();
    // Select first word and confirm
    const transcript = page.locator('#transcript');
    await transcript.click();
    // Select whole text to ensure a non-empty selection
    await page.keyboard.press(process.platform === 'win32' ? 'Control+A' : 'Meta+A');
    await page.locator('#markReliable').click();
    // Wait briefly for UI toggle
    await expect(page.locator('#markUnreliable')).toBeVisible();
  });

  test('Spans keep timing/prob attributes after save', async ({ page }) => {
    await page.goto('/v2');
    await page.locator('#folders .item', { hasText: folder }).first().click();
    await page.locator('#files .item', { hasText: path.parse(file).name }).first().click();
    // Ensure words are rendered
    await page.locator('#transcript .word').first().waitFor();

    const statsBefore = await page.evaluate(() => {
      const nodes = Array.from(document.querySelectorAll('#transcript .word')) as HTMLElement[];
      let total = nodes.length;
      let timeCount = 0;
      let probCount = 0;
      let nonZeroStarts = 0;
      let endGTStart = 0;
      for (const el of nodes) {
        const ds = (el as HTMLElement).dataset as any;
        const s = parseFloat(ds.start ?? 'NaN');
        const e = parseFloat(ds.end ?? 'NaN');
        const p = parseFloat(ds.prob ?? 'NaN');
        if (Number.isFinite(s) && Number.isFinite(e)) {
          timeCount++;
          if (s > 0) nonZeroStarts++;
          if (e > s) endGTStart++;
        }
        if (Number.isFinite(p)) probCount++;
      }
      return { total, timeCount, probCount, nonZeroStarts, endGTStart };
    });

    // Basic sanity: we expect some timing attributes on initial render
    expect(statsBefore.timeCount).toBeGreaterThan(0);

    // Edit minimally and save
    const transcript = page.locator('#transcript');
    await transcript.click();
    await page.keyboard.type('!');
    await page.locator('#submitBtn').click();
    await expect(page.locator('#versionBadge')).toContainText('גרסה');

    // Wait for alignment completion toast (signals background align finished)
    await page.waitForFunction(() => {
      const list = Array.from(document.querySelectorAll('.toast')).map(e => (e.textContent || '').trim());
      return list.some(t => t.includes('מיישר תזמונים:') && (t.includes('עודכנו') || t.includes('ללא שינוי') || t.includes('שגיאה')));
    }, { timeout: 30000 });

    // After alignment completion, force scroll to top and give virtualizer time to render
    await page.evaluate(() => { try { const el = document.querySelector('#transcriptCard .body') as HTMLElement; if (el) el.scrollTop = 0; } catch {} });
    await page.waitForTimeout(150);
    // Ensure the window actually shows some non-zero timings; wait a bit for the refresh
    await page.waitForFunction(() => {
      const nodes = Array.from(document.querySelectorAll('#transcript .word')) as HTMLElement[];
      for (const el of nodes) {
        const s = parseFloat((el as HTMLElement).dataset.start || 'NaN');
        const e = parseFloat((el as HTMLElement).dataset.end || 'NaN');
        if (Number.isFinite(s) && Number.isFinite(e) && (s > 0 || e > s)) return true;
      }
      return false;
    }, { timeout: 5000 });
    // After alignment completion, check attributes still present in the visible window
    const statsAfter = await page.evaluate(() => {
      const nodes = Array.from(document.querySelectorAll('#transcript .word')) as HTMLElement[];
      let total = nodes.length;
      let timeCount = 0;
      let probCount = 0;
      let nonZeroStarts = 0;
      let endGTStart = 0;
      for (const el of nodes) {
        const ds = (el as HTMLElement).dataset as any;
        const s = parseFloat(ds.start ?? 'NaN');
        const e = parseFloat(ds.end ?? 'NaN');
        const p = parseFloat(ds.prob ?? 'NaN');
        if (Number.isFinite(s) && Number.isFinite(e)) {
          timeCount++;
          if (s > 0) nonZeroStarts++;
          if (e > s) endGTStart++;
        }
        if (Number.isFinite(p)) probCount++;
      }
      return { total, timeCount, probCount, nonZeroStarts, endGTStart };
    });

    // Timings should not regress after save
    expect(statsAfter.timeCount).toBeGreaterThan(0);
    expect(statsAfter.timeCount).toBeGreaterThanOrEqual(statsBefore.timeCount);
    // Strengthened: in the visible window, at least one start>0 and one end>start
    expect(statsAfter.nonZeroStarts).toBeGreaterThan(0);
    expect(statsAfter.endGTStart).toBeGreaterThan(0);
    // If any probabilities existed before, they should not regress either
    if (statsBefore.probCount > 0) {
      expect(statsAfter.probCount).toBeGreaterThan(0);
    }

    // Optional check 1: backend logs show persisted timings and alignment activity
    const logPath = path.join(process.cwd(), 'app.log');
    const logText = fs.existsSync(logPath) ? fs.readFileSync(logPath, 'utf-8') : '';
    expect(logText).toMatch(/\[SAVE\] persisted tokens: .* with_timings=\d+/);
    const alignLogged = /\[ALIGN\] timings updated: \d+ tokens/.test(logText)
      || /\[ALIGN\] align_segment elapsed_ms=/.test(logText)
      || /\[ALIGN\] prealign mapping:/.test(logText);
    expect(alignLogged).toBeTruthy();
  });

  test('No zeroed timings after alignment (not all data-start=0)', async ({ page }) => {
    await page.goto('/v2');
    await page.locator('#folders .item', { hasText: folder }).first().click();
    await page.locator('#files .item', { hasText: path.parse(file).name }).first().click();
    // Ensure initial render present
    await page.locator('#transcript .word').first().waitFor();

    // Make a minimal edit and save (to trigger save + alignment)
    const transcript = page.locator('#transcript');
    await transcript.click();
    await page.keyboard.type('!');
    await page.locator('#submitBtn').click();
    await expect(page.locator('#versionBadge')).toContainText('גרסה');

    // Wait for alignment completion toast
    await page.waitForFunction(() => {
      const list = Array.from(document.querySelectorAll('.toast')).map(e => (e.textContent || '').trim());
      return list.some(t => t.includes('מיישר תזמונים:') && (t.includes('עודכנו') || t.includes('ללא שינוי') || t.includes('שגיאה')));
    }, { timeout: 30000 });

    // Validate not all spans have data-start=0 (guard against a regression that zeroes timings)
    const { total, zeroStarts, nonZeroStarts } = await page.evaluate(() => {
      const nodes = Array.from(document.querySelectorAll('#transcript .word')) as HTMLElement[];
      let total = nodes.length;
      let zeroStarts = 0;
      let nonZeroStarts = 0;
      for (const el of nodes) {
        const s = parseFloat((el as HTMLElement).dataset.start ?? 'NaN');
        if (!Number.isFinite(s)) continue; // skip non-timed tokens
        if (s === 0) zeroStarts++; else nonZeroStarts++;
      }
      return { total, zeroStarts, nonZeroStarts };
    });
    expect(total).toBeGreaterThan(0);
    // the key check: not all timed tokens are zero-start
    expect(nonZeroStarts).toBeGreaterThan(0);
  });
});

