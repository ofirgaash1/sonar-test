// v2/data/api.js
// Data access + normalization (HF baseline + optional Supabase corrections).
// Zero UI here—just fetch, normalize, and hand back clean structures.

// ---- Optional Supabase client (pass from your app) ----
let supa = null;
let correctionsCache = new Set();
// Optional backend base URL detection (for local dev over file://)
let RUNTIME_BASE = '';
function setRuntimeBase(b) { try { if (b) { RUNTIME_BASE = String(b); if (typeof window !== 'undefined') window.EXPLORE_API_BASE = RUNTIME_BASE; } } catch {}
}
function getBackendBase() {
  try {
    if (RUNTIME_BASE) return RUNTIME_BASE;
    if (typeof window !== 'undefined' && window.EXPLORE_API_BASE) return String(window.EXPLORE_API_BASE);
    if (typeof location !== 'undefined' && /^https?:$/.test(location.protocol)) return location.origin;
    if (typeof location !== 'undefined' && location.protocol === 'file:') return 'http://localhost:5000';
  } catch {}
  return '';
}

// ---- Backend fetch helper (always include credentials/cookies) ----
async function fetchBackend(url, init = {}) {
  const opts = { ...init };
  if (!('credentials' in opts)) opts.credentials = 'include';
  const r = await fetch(url, opts);
  try {
    if (r && r.status === 401 && typeof window !== 'undefined') {
      const base = getBackendBase();
      try { window.dispatchEvent(new CustomEvent('v2:unauthorized', { detail: { url, base } })); } catch {}
      try {
        const loginUrl = (base || '') + '/login';
        if (typeof location !== 'undefined' && !/\/login(\/|$)/.test(location.pathname)) {
          setTimeout(() => { try { location.assign(loginUrl); } catch {} }, 600);
        }
      } catch {}
    }
  } catch {}
  return r;
}

/**
 * Configure Supabase. Call once from app bootstrap:
 *   import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
 *   api.configureSupabase(createClient(SUPABASE_URL, ANON_KEY));
 */
export function configureSupabase(client) {
  supa = client || null;
  if (supa) {
    loadAllCorrections();
  }
}

/**
 * Load all corrections from Supabase to cache
 */
async function loadAllCorrections() {
  if (!supa) return;
  
  try {
    const [corr, ver] = await Promise.all([
      supa.from('corrections').select('file_path'),
      // PostgREST null filter syntax
      supa.from('transcripts').select('file_path').not('version', 'is', null)
    ]);
    if (corr.error && corr.status !== 406) {
      console.error('Corrections query failed:', { status: corr.status, error: corr.error });
      throw corr.error;
    }
    if (ver.error && ver.status !== 406) {
      console.error('Transcripts query failed:', { status: ver.status, error: ver.error });
      throw ver.error;
    }
    const set = new Set();
    (corr.data || []).forEach(r => set.add(r.file_path));
    (ver.data || []).forEach(r => set.add(r.file_path));
    correctionsCache = set;
    console.log('✅ Corrections loaded (union corrections+transcripts):', correctionsCache.size);
  } catch (e) {
    console.error('❌ Failed to load corrections (union):', e?.message || e);
  }
}

/**
 * Check if a file has corrections
 */
export function hasCorrection(filePath) {
  return correctionsCache.has(filePath);
}

// ---- Local fallback cache for corrections (per-file) ----
const LS_PREFIX = 'corr:';
function getLocalCorrection(filePath) {
  try {
    const raw = localStorage.getItem(LS_PREFIX + filePath);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch { return null; }
}
function setLocalCorrection(filePath, jsonObj) {
  try { localStorage.setItem(LS_PREFIX + filePath, JSON.stringify(jsonObj)); } catch {}
}

/** Manually mark a correction in the local cache (e.g., after save) */
export function markCorrection(filePath) {
  if (!filePath) return;
  try { correctionsCache.add(filePath); } catch {}
}

// ---- Path helpers (dataset path normalization only) ----
function normPaths(folder, file) {
  const audioPath = `${folder}/${file}`;
  const trPath = `${folder}/${file.replace(/\.opus$/i, '')}/full_transcript.json.gz`;
  return { audioPath, trPath };
}

// ---- Crypto helpers (SHA-256 hex) ----
export async function sha256Hex(text) {
  const s = String(text || '');
  try {
    const subtle = (globalThis.crypto && globalThis.crypto.subtle)
      ? globalThis.crypto.subtle
      : (globalThis.msCrypto && globalThis.msCrypto.subtle) || null;
    if (subtle) {
      const enc = new TextEncoder();
      const buf = await subtle.digest('SHA-256', enc.encode(s));
      const bytes = new Uint8Array(buf);
      return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
    }
  } catch (e) {
    // fall through to JS fallback
    try { console.warn('sha256 subtle failed:', e?.message || e); } catch {}
  }
  // Fallback: pure JS SHA-256 (works under file:// and older browsers)
  try { return sha256HexFallback(s); } catch (e2) { try { console.warn('sha256 fallback failed:', e2?.message || e2); } catch {}; return ''; }
}

// Minimal SHA-256 implementation (public domain style)
// Produces lowercase hex string for given UTF-8 input
function sha256HexFallback(ascii) {
  // Convert string to UTF-8 bytes
  function toBytes(str) {
    const out = [];
    for (let i = 0; i < str.length; i++) {
      let c = str.charCodeAt(i);
      if (c < 0x80) out.push(c);
      else if (c < 0x800) { out.push(0xC0 | (c >> 6), 0x80 | (c & 0x3F)); }
      else if (c >= 0xD800 && c <= 0xDBFF) { // surrogate pair
        i++; const c2 = str.charCodeAt(i);
        const code = 0x10000 + (((c & 0x3FF) << 10) | (c2 & 0x3FF));
        out.push(0xF0 | (code >> 18), 0x80 | ((code >> 12) & 0x3F), 0x80 | ((code >> 6) & 0x3F), 0x80 | (code & 0x3F));
      } else { out.push(0xE0 | (c >> 12), 0x80 | ((c >> 6) & 0x3F), 0x80 | (c & 0x3F)); }
    }
    return out;
  }
  const bytes = toBytes(ascii);
  const h = new Uint32Array(8);
  const k = new Uint32Array([
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
  ]);
  // Initial hash values
  h[0]=0x6a09e667; h[1]=0xbb67ae85; h[2]=0x3c6ef372; h[3]=0xa54ff53a; h[4]=0x510e527f; h[5]=0x9b05688c; h[6]=0x1f83d9ab; h[7]=0x5be0cd19;
  // Pad message
  const l = bytes.length * 8;
  const withOne = bytes.concat([0x80]);
  while (((withOne.length % 64) !== 56)) withOne.push(0);
  for (let i = 7; i >= 0; i--) withOne.push((l >>> (i*8)) & 0xFF);
  const w = new Uint32Array(64);
  for (let i = 0; i < withOne.length; i += 64) {
    // Prepare message schedule
    for (let t = 0; t < 16; t++) {
      const j = i + t*4;
      w[t] = (withOne[j]<<24) | (withOne[j+1]<<16) | (withOne[j+2]<<8) | (withOne[j+3]);
    }
    for (let t = 16; t < 64; t++) {
      const s0 = rrot(w[t-15],7) ^ rrot(w[t-15],18) ^ (w[t-15]>>>3);
      const s1 = rrot(w[t-2],17) ^ rrot(w[t-2],19) ^ (w[t-2]>>>10);
      w[t] = (w[t-16] + s0 + w[t-7] + s1) >>> 0;
    }
    // Initialize working vars
    let a=h[0],b=h[1],c=h[2],d=h[3],e=h[4],f=h[5],g=h[6],hh=h[7];
    for (let t=0; t<64; t++) {
      const S1 = rrot(e,6) ^ rrot(e,11) ^ rrot(e,25);
      const ch = (e & f) ^ (~e & g);
      const temp1 = (hh + S1 + ch + k[t] + w[t]) >>> 0;
      const S0 = rrot(a,2) ^ rrot(a,13) ^ rrot(a,22);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (S0 + maj) >>> 0;
      hh = g; g = f; f = e; e = (d + temp1) >>> 0; d = c; c = b; b = a; a = (temp1 + temp2) >>> 0;
    }
    h[0]=(h[0]+a)>>>0; h[1]=(h[1]+b)>>>0; h[2]=(h[2]+c)>>>0; h[3]=(h[3]+d)>>>0; h[4]=(h[4]+e)>>>0; h[5]=(h[5]+f)>>>0; h[6]=(h[6]+g)>>>0; h[7]=(h[7]+hh)>>>0;
  }
  function rrot(x,n){ return (x>>>n) | (x<<(32-n)); }
  const hex = n => n.toString(16).padStart(8,'0');
  return hex(h[0])+hex(h[1])+hex(h[2])+hex(h[3])+hex(h[4])+hex(h[5])+hex(h[6])+hex(h[7]);
}

// (HF token + direct dataset access removed; backend is the source of truth.)

// ---- Folder/File listing functions ----
/**
 * List folders in the audio dataset
 * @returns {Promise<Array<{name: string, type: 'directory'}>>}
 */
export async function listFolders() {
  const base = getBackendBase();
  const url = base ? `${base}/folders` : '/folders';
  let r;
  try {
    r = await fetchBackend(url);
  } catch (e) {
    throw new Error(`שגיאה בטעינת רשימת תיקיות: כשל רשת אל ${url}: ${e?.message || e}`);
  }
  if (!r.ok) {
    let body = '';
    try { body = await r.text(); } catch {}
    throw new Error(`שגיאה בטעינת רשימת תיקיות: ${r.status} ${r.statusText} מ-${url}${body ? ` — ${body.slice(0, 200)}` : ''}`);
  }
  let arr = [];
  try { arr = await r.json(); } catch (e) { throw new Error(`שגיאה בפענוח JSON מרשימת תיקיות: ${e?.message || e}`); }
  return Array.isArray(arr) ? arr.map(x => ({ name: x.name, type: 'directory' })) : [];
}

/**
 * List audio files in a specific folder
 * @param {string} folder - folder name
 * @returns {Promise<Array<{name: string, type: 'file', size: number}>>}
 */
export async function listFiles(folder) {
  if (!folder) return [];
  const base = getBackendBase();
  const url = base ? `${base}/files?folder=${encodeURIComponent(folder)}` : `/files?folder=${encodeURIComponent(folder)}`;
  let r;
  try {
    r = await fetchBackend(url);
  } catch (e) {
    throw new Error(`שגיאה בטעינת רשימת קבצים: כשל רשת אל ${url}: ${e?.message || e}`);
  }
  if (!r.ok) {
    let body = '';
    try { body = await r.text(); } catch {}
    throw new Error(`שגיאה בטעינת רשימת קבצים: ${r.status} ${r.statusText} מ-${url}${body ? ` — ${body.slice(0, 200)}` : ''}`);
  }
  let arr = [];
  try { arr = await r.json(); } catch (e) { throw new Error(`שגיאה בפענוח JSON מרשימת קבצים: ${e?.message || e}`); }
  return Array.isArray(arr) ? arr.map(x => ({ name: x.name, type: 'file', size: +x.size || 0 })) : [];
}

// ---- Normalization: transcript JSON → flat tokens -----------------
/**
 * Ensures segments/words exist and numbers are finite.
 * Preserves `probability` if present on words.
 */
function normalizeTranscript(raw) {
  const d = JSON.parse(JSON.stringify(raw || {}));
  d.text = d.text || '';
  d.segments = Array.isArray(d.segments) ? d.segments : [];

  d.segments.forEach((s) => {
    if (!Array.isArray(s.words) || !s.words.length) {
      s.words = [{
        word: s.text || ' ',
        start: +s.start || 0,
        end: +s.end || (+s.start || 0) + 0.5,
        probability: Number.isFinite(+s.probability) ? +s.probability : undefined
      }];
    } else {
      s.words.forEach((w) => {
        w.start = +w.start || 0;
        w.end = +w.end || (w.start + 0.25);
        w.word = String(w.word || '');
        if (w.probability != null) w.probability = Number(w.probability);
      });
    }
  });
  return d;
}

/** Flatten segments -> tokens with '\n' separators (keep probability) */
function flattenToTokens(d) {
  const toks = [];
  let lastEnd = 0;

  (d.segments || []).forEach((s, si) => {
    (s.words || []).forEach((w) => {
      toks.push({
        word: String(w.word || ''),
        start: +w.start || 0,
        end: +w.end || ((+w.start || 0) + 0.25),
        probability: Number.isFinite(+w.probability) ? +w.probability : NaN
      });
      lastEnd = toks[toks.length - 1].end;
    });
    if (si < (d.segments.length - 1)) {
      toks.push({ word: '\n', start: lastEnd, end: lastEnd, probability: NaN });
    }
  });

  return toks;
}

function wordsToText(tokens) {
  let s = '';
  for (const t of (tokens || [])) s += t.word;
  return s;
}

// ---- Supabase: corrections (optional) -----------------------------
async function loadCorrectionFromDB(filePath) {
  // Prefer local cache first (fast UX + offline)
  const local = getLocalCorrection(filePath);
  if (local) return local;

  if (!supa) return null;
  const { data, error } = await supa
    .from('corrections')
    .select('json_data')
    .eq('file_path', filePath)
    .maybeSingle();

  if (error) {
    // Non-fatal: just log and return null
    console.warn('Supabase corrections fetch failed:', error);
    return null;
  }
  const json = data?.json_data || null;
  if (json) setLocalCorrection(filePath, json);
  return json;
}

/** Upsert correction JSON */
export async function saveCorrectionToDB(filePath, jsonObj) {
  if (!supa) throw new Error('Supabase client not configured');
  const { data, error } = await supa
    .from('corrections')
    .upsert({ file_path: filePath, json_data: jsonObj }, { onConflict: 'file_path' })
    .select()
    .single();

  if (error) throw error;
  // Also persist locally for instant reloads/offline
  setLocalCorrection(filePath, jsonObj);
  return data;
}

// ---- Versioned transcripts (optional, if table exists) ------------
export async function getLatestTranscript(filePath) {
  const base = getBackendBase();
  if (!base) return null;
  const r = await fetchBackend(`${base}/transcripts/latest?doc=${encodeURIComponent(filePath)}`);
  if (!r.ok) return null;
  return await r.json();
}

/** Fetch a specific transcript version (text + words) */
export async function getTranscriptVersion(filePath, version) {
  const base = getBackendBase();
  if (!base || !filePath || !Number.isFinite(+version)) return null;
  const r = await fetchBackend(`${base}/transcripts/get?doc=${encodeURIComponent(filePath)}&version=${encodeURIComponent(version)}`);
  if (!r.ok) return null;
  return await r.json();
}

/** Prefer normalized words from backend table; fallback to JSON words in transcript */
export async function getTranscriptWords(filePath, version, opts = {}) {
  const base = getBackendBase();
  if (!base || !filePath || !Number.isFinite(+version)) return [];
  const qp = new URLSearchParams({ doc: filePath, version: String(version) });
  if (Number.isFinite(+opts.segment)) qp.set('segment', String(+opts.segment));
  if (Number.isFinite(+opts.count)) qp.set('count', String(+opts.count));
  const r = await fetchBackend(`${base}/transcripts/words?${qp.toString()}`);
  if (!r.ok) return [];
  return await r.json();
}

export async function saveTranscriptVersion(filePath, { parentVersion = null, text, words, expectedBaseSha256 = '', segment = null, neighbors = null }) {
  const base = getBackendBase();
  if (!base) throw new Error('Backend base not configured');
  const payload = {
    doc: filePath,
    parentVersion: parentVersion,
    expected_base_sha256: expectedBaseSha256 || '',
    text: String(text || ''),
    words: Array.isArray(words) ? words : []
  };
  try {
    if (Number.isFinite(+segment)) payload.segment = +segment;
  } catch {}
  try {
    if (Number.isFinite(+neighbors)) {
      const n = Math.max(0, Math.min(3, +neighbors));
      payload.neighbors = n;
    }
  } catch {}
  // Retry wrapper to mitigate transient SQLite "database is locked" errors under concurrent access
  const maxAttempts = 6; // ~0.8–1.2s total with backoff
  let attempt = 0;
  let lastErrText = '';
  while (attempt < maxAttempts) {
    attempt++;
    const r = await fetchBackend(`${base}/transcripts/save`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    if (r.ok) {
      return await r.json();
    }
    if (r.status === 409) {
      let payload = null; try { payload = await r.json(); } catch {}
      const err = new Error('Conflict'); err.code = 409; err.payload = payload; throw err;
    }
    // Capture body for diagnostics and retry on lock
    try { lastErrText = await r.text(); } catch { lastErrText = 'save failed'; }
    const retriable = r.status >= 500 || /database is locked/i.test(lastErrText || '');
    if (!retriable || attempt >= maxAttempts) {
      throw new Error(lastErrText || 'save failed');
    }
    // Simple backoff: 50ms, 100ms, 150ms, ...
    await new Promise(res => setTimeout(res, 50 * attempt));
  }
  throw new Error(lastErrText || 'save failed');
}

/** Request alignment for a segment neighborhood of a given version */
export async function alignSegment(filePath, { version, segment, neighbors = 1 } = {}) {
  const base = getBackendBase();
  if (!base) throw new Error('Backend base not configured');
  const body = {
    doc: filePath,
    version: Number.isFinite(+version) ? +version : undefined,
    segment: Number.isFinite(+segment) ? +segment : undefined,
    neighbors: Math.max(0, Math.min(3, Number.isFinite(+neighbors) ? +neighbors : 1)),
  };
  const r = await fetchBackend(`${base}/transcripts/align_segment`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(await r.text().catch(()=> 'align failed'));
  try { return await r.json(); } catch { return { ok: false }; }
}

/** Fetch transcript edits (diff layers) for a file */
// (removed stub) getTranscriptEdits implemented below

/**
 * Fetch transcript edit records (including optional timing changes) for a file
 * Row shape: { parent_version, child_version, dmp_patch, token_ops }
 */
export async function getTranscriptEdits(filePath) {
  const base = getBackendBase();
  if (!base || !filePath) return [];
  const url = `${base}/transcripts/edits?doc=${encodeURIComponent(filePath)}`;
  try {
    const r = await fetchBackend(url);
    if (!r.ok) return [];
    return await r.json();
  } catch {
    return [];
  }
}

/**
 * Request alignment for a segment neighborhood (n-1..n+1) of the latest version
 * Body: { doc, version?, segment, neighbors? }
 * Returns summary of timing adjustments or error
 */

/** Fetch all transcript versions with text (ASC) for diff layers */
export async function getAllTranscripts(filePath) {
  const out = [];
  try {
    const base = getBackendBase();
    if (!base || !filePath) return out;
    // 0) Try to include baseline as version 0 so layers show v0->v1 too
    try {
      const idx = String(filePath).lastIndexOf('/');
      if (idx > 0) {
        const folder = String(filePath).slice(0, idx);
        const file = String(filePath).slice(idx + 1);
        const url = `${base}/episode?folder=${encodeURIComponent(folder)}&file=${encodeURIComponent(file)}`;
        const r0 = await fetchBackend(url);
        if (r0.ok) {
          const j = await r0.json();
          const trRaw = j && (j.transcript || j.baseline || null);
          if (trRaw) {
            try {
              const norm = normalizeTranscript(trRaw);
              const tokens0 = flattenToTokens(norm);
              const text0 = wordsToText(tokens0);
              if (text0) out.push({ version: 0, text: text0, user: 'baseline' });
            } catch {}
          }
        }
      }
    } catch {}
    const h = await fetchBackend(`${base}/transcripts/history?doc=${encodeURIComponent(filePath)}`);
    if (!h.ok) return out;
    const hist = await h.json();
    const versions = Array.isArray(hist) ? hist.map(r => +r.version).filter(v => Number.isFinite(v) && v > 0) : [];
    versions.sort((a,b) => a - b);
    if (!versions.length) return out;
    for (const v of versions) {
      try {
        const row = await getTranscriptVersion(filePath, v);
        if (row && typeof row.text === 'string') {
          const user = row.created_by || row.author || row.user || '';
          out.push({ version: v, text: row.text, user });
        }
      } catch {}
    }
    // Fallback if texts failed: try at least v1 and latest
    if (!out.length && versions.length) {
      try {
        const v1 = await getTranscriptVersion(filePath, 1);
        const latestV = versions[versions.length - 1];
        const vl = await getTranscriptVersion(filePath, latestV);
        if (v1 && typeof v1.text === 'string') out.push({ version: 1, text: v1.text, user: v1.created_by || v1.author || v1.user || '' });
        if (vl && typeof vl.text === 'string') out.push({ version: latestV, text: vl.text, user: vl.created_by || vl.author || vl.user || '' });
        out.sort((a,b) => a.version - b.version);
      } catch {}
    }
  } catch {}
  // Ensure ascending by version and unique versions
  try {
    const seen = new Set();
    const uniq = [];
    for (const v of out.sort((a,b)=>a.version-b.version)) {
      const key = String(v.version);
      if (!seen.has(key)) { seen.add(key); uniq.push(v); }
    }
    return uniq;
  } catch { return out; }
}
// ---- Edits history (optional) -------------------------------------
export async function saveTranscriptEdit() { return null; }

// ---- Confirmations (anchored to version + hash) --------------------
export async function getConfirmations(filePath, version) {
  const base = getBackendBase();
  if (!base || !filePath || !Number.isFinite(+version)) return [];
  const r = await fetchBackend(`${base}/transcripts/confirmations?doc=${encodeURIComponent(filePath)}&version=${encodeURIComponent(version)}`);
  if (!r.ok) return [];
  const arr = await r.json();
  return (arr || []).map(r => ({ id: r.id, range: [r.start_offset, r.end_offset], prefix: r.prefix, exact: r.exact, suffix: r.suffix }));
}

export async function saveConfirmations(filePath, version, base_sha256, ranges, fullText) {
  const base = getBackendBase();
  if (!base) throw new Error('Backend base not configured');
  if (!filePath || !Number.isFinite(+version)) throw new Error('invalid version');
  const text = String(fullText || '');
  const mkCtx = (s, e) => {
    const preStart = Math.max(0, s - 16);
    const sufEnd = Math.max(e, Math.min(text.length, e + 16));
    return { start_offset: s, end_offset: e, prefix: text.slice(preStart, s), exact: text.slice(s, e), suffix: text.slice(e, sufEnd) };
  };
  const items = (ranges || []).map(r => mkCtx(r[0], r[1]));
  const r = await fetchBackend(`${base}/transcripts/confirmations/save`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ doc: filePath, version: +version, base_sha256: base_sha256 || '', items })
  });
  if (!r.ok) throw new Error(await r.text().catch(()=> 'save failed'));
  return await r.json();
}

// ---- Public: load one episode (baseline + initial tokens + audio) ----
/**
 * Load an episode:
 *  - HF baseline transcript (for diff/align)
 *  - Optional DB correction (as current view)
 *  - Audio URL (token-aware)
 *
 * @param {{ folder:string, file:string }} param0
 * @returns {Promise<{
 *   audioUrl: string,
 *   baselineTokens: Array<{word:string,start:number,end:number,probability:number}>,
 *   baselineText: string,
 *   initialTokens: Array<{word:string,start:number,end:number,probability:number}>,
 *   usedCorrection: boolean
 * }>}
 */
export async function loadEpisode({ folder, file }) {
  if (!folder || !file) throw new Error('loadEpisode: folder and file are required');
  const { audioPath, trPath } = normPaths(folder, file);

  // 1) Try latest versioned transcript first (if available)
  let latestVersion = null;
  try {
    latestVersion = await getLatestTranscript(audioPath);
  } catch (e) {
    console.warn('Latest transcript lookup error:', e);
  }

  // No legacy corrections fallback; backend is the source of truth.

  // 2) Prefer backend baseline transcript if available
  let baselineTokens = [];
  let baselineText = '';
  let audioUrl = '';
  {
    const base = getBackendBase();
    const primaryUrl = base
      ? `${base}/episode?folder=${encodeURIComponent(folder)}&file=${encodeURIComponent(file)}`
      : `/episode?folder=${encodeURIComponent(folder)}&file=${encodeURIComponent(file)}`;
    const fallbacks = [primaryUrl];
    if (!base || !/^https?:\/\//i.test(base)) fallbacks.push(`http://localhost:5000/episode?folder=${encodeURIComponent(folder)}&file=${encodeURIComponent(file)}`);
    let r;
    for (const url of fallbacks) {
      try {
        r = await fetchBackend(url);
        if (r.ok) {
          if (url.startsWith('http')) setRuntimeBase(new URL(url).origin);
          else if (typeof location !== 'undefined' && /^https?:$/.test(location.protocol)) setRuntimeBase(location.origin);
          const j = await r.json();
          const trRaw = j && (j.transcript || j.baseline || null);
          if (trRaw) {
            const norm = normalizeTranscript(trRaw);
            baselineTokens = flattenToTokens(norm);
            baselineText = wordsToText(baselineTokens);
            // Force playback through backend /audio to ensure CORS/Range/auth handled
            const enc = (s) => String(s || '').split('/').map(encodeURIComponent).join('/');
            const origin = getBackendBase();
            audioUrl = (origin ? `${origin}` : '') + `/audio/${enc(folder)}/${enc(file)}`;
          }
          break;
        }
      } catch {}
    }
  }

  // No HF fallback; baseline must come from backend.

  // 3) Choose initial tokens (latest transcript > correction > baseline)
  let initialTokens, usedCorrection = false, version = null, base_sha256 = '';
  if (latestVersion) {
    version = latestVersion.version;
    base_sha256 = latestVersion.base_sha256 || '';
    // Prefer normalized words when backend is available
    try {
      const words = await getTranscriptWords(audioPath, version);
      if (Array.isArray(words) && words.length) {
        initialTokens = words;
        usedCorrection = true;
      }
    } catch {}
    if (!initialTokens && Array.isArray(latestVersion.words)) {
      initialTokens = latestVersion.words; usedCorrection = true;
    }
  } else {
    initialTokens = baselineTokens;
    usedCorrection = false;
  }

  // 4) Audio URL
  // Audio URL is provided by backend (local or remote URL); no HF handling here.

  return {
    audioUrl,
    baselineTokens,
    baselineText,
    initialTokens,
    usedCorrection,
    version,
    base_sha256
  };
}

// Named export bundle (matches earlier import style: `import { api } from ...`)
export const api = {
  loadEpisode,
  getLatestTranscript,
  getTranscriptVersion,
  getTranscriptWords,
  // getTranscriptHistory: optional helper for history panel
  async getTranscriptHistory(filePath) {
    const base = getBackendBase();
    if (base) {
      try {
        const r = await fetchBackend(`${base}/transcripts/history?doc=${encodeURIComponent(filePath)}`);
        if (r.ok) return await r.json();
      } catch {}
    }
    return [];
  },
  saveTranscriptVersion,
  getTranscriptEdits,
  getAllTranscripts,
  saveTranscriptEdit,
  getConfirmations,
  saveConfirmations,
  listFolders,
  listFiles,
  hasCorrection,
  sha256Hex
};
export default api;
