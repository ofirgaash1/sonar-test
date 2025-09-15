// v2/workers/diff-worker.js
// Module worker that computes diffs off the main thread using Myers O(ND).
// Protocol:
//   { id?, type: 'init', baselineText }
//   { id?, type: 'setBaseline', baselineText }
//   { id?, type: 'diff', text, options? }
// Responses:
//   { id?, type: 'diff:ready' }
//   { id?, type: 'diff:baseline-set' }
//   { id?, type: 'diff:result', patchText, diffs, stats }

// Defaults (kept for API, not used deeply here)
const DEFAULT_TIMEOUT = 1.0;
const DEFAULT_EDIT_COST = 7;

let baselineText = '';

function safeString(x) { return typeof x === 'string' ? x : String(x ?? ''); }
function toChars(s) { return Array.from(s || ''); }
// Split into: whitespace runs | word runs (letters/digits/marks) | single punctuation/other
function toTokens(s) {
  try {
    const re = /\s+|[\p{L}\p{Nd}\p{M}]+|[^\s\p{L}\p{Nd}\p{M}]/gu;
    return String(s || '').match(re) || [];
  } catch {
    // Fallback when Unicode properties unsupported
    return String(s || '').match(/\s+|\w+|\W/gu) || [];
  }
}

function splitLinesKeepNL(s) {
  const parts = String(s || '').split('\n');
  const out = [];
  for (let i = 0; i < parts.length; i++) {
    const isLast = (i === parts.length - 1);
    const seg = isLast ? parts[i] : (parts[i] + '\n');
    out.push(seg);
  }
  return out;
}

// Token-level diff for a single line-sized string
function normalizeDiffs(diffs) {
  const out = [];
  for (const d of (diffs || [])) {
    const op = Array.isArray(d) ? d[0] : 0; const s = Array.isArray(d) ? String(d[1] || '') : '';
    if (!s) continue;
    if (out.length && out[out.length-1][0] === op) { out[out.length-1][1] += s; }
    else out.push([op, s]);
  }
  return out;
}

function charDiffStrings(aStr, bStr, dbgTag) {
  const aChars = toChars(aStr);
  const bChars = toChars(bStr);
  const pre = commonPrefixLen(aChars, bChars);
  const aMid = aChars.slice(pre);
  const bMid = bChars.slice(pre);
  const post = commonSuffixLen(aMid, bMid);
  const aC = aChars.slice(pre, aChars.length - post);
  const bC = bChars.slice(pre, bChars.length - post);
  let diffs = myersDiffSeq(aC, bC, (arr) => arr.join(''));
  if (pre) diffs.unshift([0, aChars.slice(0, pre).join('')]);
  if (post) diffs.push([0, aChars.slice(aChars.length - post).join('')]);
  return normalizeDiffs(diffs);
}

async function dmpDiffStrings(aStr, bStr, dbgTag) {
  try {
    const { diff_match_patch, DIFF_DELETE, DIFF_INSERT, DIFF_EQUAL } = await import('https://esm.sh/diff-match-patch@1.0.5');
    const dmp = new diff_match_patch();
    // Tune a little for responsiveness
    dmp.Diff_Timeout = 1.0;
    dmp.Diff_EditCost = 4;
    let diffs = dmp.diff_main(String(aStr||''), String(bStr||''));
    try { dmp.diff_cleanupSemantic(diffs); } catch {}
    const mapped = [];
    for (const [op, s] of diffs) {
      if (!s) continue;
      const code = (op === DIFF_INSERT) ? 1 : (op === DIFF_DELETE ? -1 : 0);
      if (mapped.length && mapped[mapped.length-1][0] === code) mapped[mapped.length-1][1] += s; else mapped.push([code, s]);
    }
    if (dbgTag) {
      try {
        const vis = (t) => String(t).replace(/\n/g,'⏎').replace(/ /g,'␠');
        const sample = mapped.slice(0,8).map(([op,s]) => [op, vis(String(s).slice(0,80))]);
        console.log(`[diff:${dbgTag}] dmp.sample`, JSON.stringify(sample));
      } catch {}
    }
    return mapped;
  } catch (e) {
    if (dbgTag) { try { console.warn(`[diff:${dbgTag}] dmp import failed`, e); } catch {} }
    return null;
  }
}

function tokenDiffStrings(aStr, bStr, dbgTag) {
  const aTokAll = toTokens(aStr);
  const bTokAll = toTokens(bStr);
  const preTok = commonPrefixLen(aTokAll, bTokAll);
  const aTokTail = aTokAll.slice(preTok);
  const bTokTail = bTokAll.slice(preTok);
  const postTok = commonSuffixLen(aTokTail, bTokTail);
  const aTokMid = aTokAll.slice(preTok, aTokAll.length - postTok);
  const bTokMid = bTokAll.slice(preTok, bTokAll.length - postTok);
  if (dbgTag) {
    try {
      const sample = (arr) => arr.slice(0, 12).map(t => JSON.stringify(t)).join(' | ');
      console.log(`[diff:${dbgTag}] tokens.pre/post`, preTok, postTok, 'aTokAll', aTokAll.length, 'bTokAll', bTokAll.length);
      console.log(`[diff:${dbgTag}] aTokMid.len`, aTokMid.length, 'bTokMid.len', bTokMid.length);
      console.log(`[diff:${dbgTag}] aTokAll.sample`, sample(aTokAll));
      console.log(`[diff:${dbgTag}] bTokAll.sample`, sample(bTokAll));
    } catch {}
  }
  let diffs = myersDiffSeq(aTokMid, bTokMid, (arr) => arr.join(''));
  if (preTok) diffs.unshift([0, aTokAll.slice(0, preTok).join('')]);
  if (postTok) diffs.push([0, aTokAll.slice(aTokAll.length - postTok).join('')]);
  diffs = normalizeDiffs(diffs);
  // Validate per-line; if broken (e.g., duplicated tokens), fall back to char diff for this line
  const reconNew = (ops) => ops.map(([op, s]) => op === -1 ? '' : s).join('');
  const reconOld = (ops) => ops.map(([op, s]) => op === 1 ? '' : s).join('');
  const okNew = reconNew(diffs) === String(bStr || '');
  const okOld = reconOld(diffs) === String(aStr || '');
  if (!okNew || !okOld) {
    if (dbgTag) { try { console.warn(`[diff:${dbgTag}] token-diff failed validate (per-line), falling back to char-diff`); } catch {} }
    diffs = charDiffStrings(aStr, bStr, dbgTag);
  }
  return diffs;
}

// Granular diff: line-level pairing with token-level refinement inside changed lines
function granularDiff(baseText, nextText, dbgTag) {
  const aLines = splitLinesKeepNL(baseText);
  const bLines = splitLinesKeepNL(nextText);
  const pre = commonPrefixLen(aLines, bLines);
  const aTail = aLines.slice(pre);
  const bTail = bLines.slice(pre);
  const post = commonSuffixLen(aTail, bTail);
  const out = [];
  if (dbgTag) {
    try { console.log(`[diff:${dbgTag}] line-anchors pre`, pre, 'post', post, 'aLines', aLines.length, 'bLines', bLines.length); } catch {}
  }
  if (pre) out.push([0, aLines.slice(0, pre).join('')]);

  const aMid = aLines.slice(pre, aLines.length - post);
  const bMid = bLines.slice(pre, bLines.length - post);
  if (aMid.length || bMid.length) {
    // Fast path: single-line replacement → go straight to token refinement
    if (aMid.length === 1 && bMid.length === 1) {
      if (dbgTag) {
        try {
          const vis = (s) => String(s).replace(/\n/g, '⏎').replace(/ /g, '␠');
          console.log(`[diff:${dbgTag}] single-line refine aMid.len 1 bMid.len 1`);
          console.log(`[diff:${dbgTag}] aMid[0]`, vis((aMid[0]||'').slice(0, 160)));
          console.log(`[diff:${dbgTag}] bMid[0]`, vis((bMid[0]||'').slice(0, 160)));
        } catch {}
      }
      const refined = tokenDiffStrings(aMid[0] || '', bMid[0] || '', dbgTag);
      for (const d of refined) {
        const L = out.length; if (L && out[L-1][0] === d[0]) out[L-1][1] += d[1]; else out.push([d[0], d[1]]);
      }
    }
    else {
      const lineDiffs = myersDiffSeq(aMid, bMid, (arr) => arr.join(''));
      const delBuf = [];
      const insBuf = [];
      if (dbgTag) {
        try {
          const vis = (s) => String(s).replace(/\n/g, '⏎').replace(/ /g, '␠');
          const sampleOps = lineDiffs.slice(0, 10).map(([op, s]) => [op, vis(String(s).slice(0, 80))]);
          const cDel = lineDiffs.filter(x=>x[0]===-1).length;
          const cIns = lineDiffs.filter(x=>x[0]===1).length;
          const cEq  = lineDiffs.filter(x=>x[0]===0).length;
          console.log(`[diff:${dbgTag}] lineDiffs.counts del`, cDel, 'ins', cIns, 'eq', cEq);
          console.log(`[diff:${dbgTag}] lineDiffs.sample`, JSON.stringify(sampleOps));
        } catch {}
      }
      for (const [op, chunk] of lineDiffs) {
        if (dbgTag) {
          try { console.log(`[diff:${dbgTag}] lineOp`, op, 'len', String(chunk||'').length); } catch {}
        }
        if (op === 0) {
          while (delBuf.length) { out.push([-1, delBuf.shift()]); }
          while (insBuf.length) { out.push([1, insBuf.shift()]); }
          out.push([0, chunk]);
          continue;
        }
        if (op === -1) {
          // If there is an unmatched insertion waiting, refine pair (ins first)
          if (insBuf.length) {
            if (dbgTag) { try { console.log(`[diff:${dbgTag}] refining (ins-first)`); } catch {} }
            const newChunk = insBuf.shift();
            if (dbgTag) {
              try {
                const vis = (s) => String(s).replace(/\n/g, '⏎').replace(/ /g, '␠');
                console.log(`[diff:${dbgTag}] refine-lines (ins-first) old.len`, chunk.length, 'new.len', newChunk.length);
                console.log(`[diff:${dbgTag}] old.preview`, vis(chunk.slice(0, 120)));
                console.log(`[diff:${dbgTag}] new.preview`, vis(newChunk.slice(0, 120)));
              } catch {}
            }
            const refined = tokenDiffStrings(chunk, newChunk, dbgTag);
            for (const d of refined) {
              const L = out.length; if (L && out[L-1][0] === d[0]) out[L-1][1] += d[1]; else out.push([d[0], d[1]]);
            }
          } else {
            delBuf.push(chunk);
          }
          continue;
        }
        // op === 1
        if (delBuf.length) {
          if (dbgTag) { try { console.log(`[diff:${dbgTag}] refining (del-first)`); } catch {} }
          const oldChunk = delBuf.shift();
          if (dbgTag) {
            try {
              const vis = (s) => String(s).replace(/\n/g, '⏎').replace(/ /g, '␠');
              console.log(`[diff:${dbgTag}] refine-lines old.len`, oldChunk.length, 'new.len', chunk.length);
              console.log(`[diff:${dbgTag}] old.preview`, vis(oldChunk.slice(0, 120)));
              console.log(`[diff:${dbgTag}] new.preview`, vis(chunk.slice(0, 120)));
            } catch {}
          }
          const refined = tokenDiffStrings(oldChunk, chunk, dbgTag);
          for (const d of refined) {
            const L = out.length; if (L && out[L-1][0] === d[0]) out[L-1][1] += d[1]; else out.push([d[0], d[1]]);
          }
        } else {
          insBuf.push(chunk);
        }
      }
      // Flush any remaining unmatched runs
      while (delBuf.length) { out.push([-1, delBuf.shift()]); }
      while (insBuf.length) { out.push([1, insBuf.shift()]); }
    }
  }

  if (post) out.push([0, aLines.slice(aLines.length - post).join('')]);
  if (dbgTag) {
    try {
      const vis = (s) => String(s).replace(/\n/g, '⏎').replace(/ /g, '␠');
      const sample = out.slice(0, 12).map(([op,s]) => [op, vis(String(s).slice(0, 80))]);
      const stats = computeStats(out);
      console.log(`[diff:${dbgTag}] granular.out sample`, JSON.stringify(sample), 'stats', stats);
    } catch {}
  }
  return out;
}

function commonPrefixLen(a, b) {
  const L = Math.min(a.length, b.length);
  let i = 0;
  while (i < L && a[i] === b[i]) i++;
  return i;
}
function commonSuffixLen(a, b) {
  const L = Math.min(a.length, b.length);
  let i = 0;
  while (i < L && a[a.length - 1 - i] === b[b.length - 1 - i]) i++;
  return i;
}

function computeStats(diffs) {
  let inserted = 0, deleted = 0, equal = 0, distance = 0;
  for (const [op, data] of diffs) {
    const len = data.length;
    if (op === 1) { inserted += len; distance += len; }
    else if (op === -1) { deleted += len; distance += len; }
    else { equal += len; }
  }
  return { inserted, deleted, equal, distance };
}

// Myers O(ND) diff on arrays, returns list of [op, runStr] with ops -1/0/1
function myersDiffSeq(a, b, joiner) {
  const N = a.length, M = b.length;
  const max = N + M;
  const v = new Int32Array(2 * max + 1);
  const trace = [];
  const offset = max;

  for (let d = 0; d <= max; d++) {
    trace.push(v.slice());
    for (let k = -d; k <= d; k += 2) {
      const idx = k + offset;
      let x;
      if (k === -d || (k !== d && v[idx - 1] < v[idx + 1])) {
        x = v[idx + 1]; // down (insertion in a → move in b)
      } else {
        x = v[idx - 1] + 1; // right (deletion from a)
      }
      let y = x - k;
      while (x < N && y < M && a[x] === b[y]) { x++; y++; }
      v[idx] = x;
      if (x >= N && y >= M) {
        return backtrackSeq(a, b, trace, k, x, y, d, offset, joiner);
      }
    }
  }
  return [[0, joiner(a)]]; // should not reach
}

function backtrackSeq(a, b, trace, k, x, y, d, offset, joiner) {
  const diffs = [];
  for (let D = d; D > 0; D--) {
    const v = trace[D];
    const kIdx = k + offset;
    let prevK;
    if (k === -D || (k !== D && v[kIdx - 1] < v[kIdx + 1])) {
      prevK = k + 1;
    } else {
      prevK = k - 1;
    }
    const prevX = trace[D - 1][prevK + offset];
    const prevY = prevX - prevK;
    while (x > prevX && y > prevY) {
      diffs.push([0, a[x - 1]]);
      x--; y--;
    }
    if (x === prevX) {
      // down → insertion in B
      diffs.push([1, b[prevY]]);
    } else {
      // right → deletion from A
      diffs.push([-1, a[prevX]]);
    }
    x = prevX; y = prevY; k = prevK;
  }
  while (x > 0 && y > 0) {
    diffs.push([0, a[x - 1]]);
    x--; y--;
  }
  while (x > 0) { diffs.push([-1, a[--x]]); }
  while (y > 0) { diffs.push([1, b[--y]]); }
  diffs.reverse();
  return coalesceSeq(diffs, joiner);
}

function coalesceSeq(diffs, joiner) {
  if (!diffs.length) return diffs;
  const out = [];
  let lastOp = diffs[0][0];
  let buffer = [diffs[0][1]];
  for (let i = 1; i < diffs.length; i++) {
    const [op, ch] = diffs[i];
    if (op === lastOp) buffer.push(ch);
    else { out.push([lastOp, joiner(buffer)]); lastOp = op; buffer = [ch]; }
  }
  out.push([lastOp, joiner(buffer)]);
  return out;
}

function patch_toText_fromDiffs(diffs) {
  // Minimal JSON encoding as a placeholder for storage/interop
  try { return JSON.stringify(diffs); } catch { return ''; }
}

self.onmessage = async (ev) => {
  const msg = ev?.data || {};
  const t = msg.type;
  const id = msg.id;

  try {
    if (t === 'init') {
      baselineText = safeString(msg.baselineText || '');
      // keep API knobs for future tuning
      self.postMessage({ id, type: 'diff:ready' });
      return;
    }

    if (t === 'setBaseline') {
      baselineText = safeString(msg.baselineText || '');
      self.postMessage({ id, type: 'diff:baseline-set' });
      return;
    }

    if (t === 'diff') {
      // Prefer an explicit baseline provided on the message to avoid races
      // with concurrent baseline changes. Fall back to the last set baseline.
      const baseText = (msg && Object.prototype.hasOwnProperty.call(msg, 'baselineText'))
        ? safeString(msg.baselineText)
        : safeString(baselineText);
      const nextText = safeString(msg.text || '');

      // Options kept for future use
      const timeout = Number(msg?.options?.timeoutSec);
      const editCost = Number(msg?.options?.editCost);
      const debugTag = msg?.options?.debugTag ? String(msg.options.debugTag) : '';
      void timeout; void editCost;

      // Prefer granular (line+token) diffs for readability
      if (debugTag) console.log(`[diff:${debugTag}] start`);
      if (debugTag) {
        const vis = (s) => String(s).replace(/\n/g, '⏎').replace(/ /g, '␠');
        console.log(`[diff:${debugTag}] baseline.len`, baseText.length, 'next.len', nextText.length);
        console.log(`[diff:${debugTag}] baseline.preview`, vis(baseText.slice(0,120)));
        console.log(`[diff:${debugTag}] next.preview`, vis(nextText.slice(0,120)));
        const aLines0 = splitLinesKeepNL(baseText);
        const bLines0 = splitLinesKeepNL(nextText);
        const preL = commonPrefixLen(aLines0, bLines0);
        const aTail0 = aLines0.slice(preL);
        const bTail0 = bLines0.slice(preL);
        const postL = commonSuffixLen(aTail0, bTail0);
        console.log(`[diff:${debugTag}] lines.pre`, preL, 'lines.post', postL, 'a.mid.lines', aLines0.length - preL - postL, 'b.mid.lines', bLines0.length - preL - postL);
      }

      let diffs = granularDiff(baseText, nextText, debugTag);
      let strategy = 'granular';
      // If something goes wrong (unlikely), fall back to previous strategies
      if (!Array.isArray(diffs) || diffs.length === 0) {
        strategy = 'fallback:token/char';
        const aTokAll = toTokens(baseText);
        const bTokAll = toTokens(nextText);
        const preTok = commonPrefixLen(aTokAll, bTokAll);
        const aTokTail = aTokAll.slice(preTok);
        const bTokTail = bTokAll.slice(preTok);
        const postTok = commonSuffixLen(aTokTail, bTokTail);
        const aTokMid = aTokAll.slice(preTok, aTokAll.length - postTok);
        const bTokMid = bTokAll.slice(preTok, bTokAll.length - postTok);
        const sumTok = aTokMid.length + bTokMid.length;
        if (sumTok <= 20000) {
          diffs = myersDiffSeq(aTokMid, bTokMid, (arr) => arr.join(''));
          if (preTok) diffs.unshift([0, aTokAll.slice(0, preTok).join('')]);
          if (postTok) diffs.push([0, aTokAll.slice(aTokAll.length - postTok).join('')]);
        } else {
          const aChars = toChars(baseText);
          const bChars = toChars(nextText);
          const pre = commonPrefixLen(aChars, bChars);
          const aMid = aChars.slice(pre);
          const bMid = bChars.slice(pre);
          const post = commonSuffixLen(aMid, bMid);
          const aC = aChars.slice(pre, aChars.length - post);
          const bC = bChars.slice(pre, bChars.length - post);
          const sumChars = aC.length + bC.length;
          diffs = (sumChars <= 16000) ? myersDiffSeq(aC, bC, (arr) => arr.join('')) : simpleGreedyDiff(aC.join(''), bC.join(''));
          if (pre) diffs.unshift([0, aChars.slice(0, pre).join('')]);
          if (post) diffs.push([0, aChars.slice(aChars.length - post).join('')]);
        }
      }
      // Validate reconstruction (with canonicalization to avoid false negatives)
      const canon = (s) => {
        try {
          let t = String(s || '');
          t = t.replace(/\r/g, '');
          t = t.replace(/\u00A0/g, ' ');
          t = t.replace(/[\u200E\u200F\u202A-\u202E\u2066-\u2069]/g, '');
          if (t.normalize) t = t.normalize('NFC');
          return t;
        } catch { return String(s || ''); }
      };
      const reconstructNew = (ops) => { try { return (ops || []).map(([op, s]) => (op === -1 ? '' : (s || ''))).join(''); } catch { return ''; } };
      const reconstructOld = (ops) => { try { return (ops || []).map(([op, s]) => (op === 1 ? '' : (s || ''))).join(''); } catch { return ''; } };
      let okNew = canon(reconstructNew(diffs)) === canon(nextText);
      let okOld = canon(reconstructOld(diffs)) === canon(baseText);
      if (debugTag) {
        try {
          const rn = reconstructNew(diffs); const ro = reconstructOld(diffs);
          console.log(`[diff:${debugTag}] recon.len new`, rn.length, 'old', ro.length);
          console.log(`[diff:${debugTag}] canon.ok new`, okNew, 'old', okOld);
        } catch {}
      }
      if (!(okNew && okOld)) {
        // Fallback 1a: jsdiff word-wise across entire text
        try {
          const jsd = await import('https://esm.sh/diff@5');
          const parts = jsd?.diffWordsWithSpace ? jsd.diffWordsWithSpace(baseText, nextText) : null;
          if (Array.isArray(parts) && parts.length) {
            const mapped = normalizeDiffs(parts.map(p => [p.added ? 1 : (p.removed ? -1 : 0), String(p.value || '')]));
            const rn = (ops) => ops.map(([op,s]) => (op === -1 ? '' : s)).join('');
            const ro = (ops) => ops.map(([op,s]) => (op === 1 ? '' : s)).join('');
            if (rn(mapped) === nextText && ro(mapped) === baseText) {
              diffs = mapped; okNew = okOld = true; strategy = 'fallback:jsdiff-words';
            }
          }
        } catch (e) { if (debugTag) try { console.warn(`[diff:${debugTag}] jsdiff import failed`, e); } catch {} }
      }
      if (!(okNew && okOld)) {
        // Fallback 1: pure char-level trimmed diff
        try {
          const aChars = toChars(baseText);
          const bChars = toChars(nextText);
          const pre = commonPrefixLen(aChars, bChars);
          const aMid = aChars.slice(pre);
          const bMid = bChars.slice(pre);
          const post = commonSuffixLen(aMid, bMid);
          const aC = aChars.slice(pre, aChars.length - post);
          const bC = bChars.slice(pre, bChars.length - post);
          let diffs2 = myersDiffSeq(aC, bC, (arr) => arr.join(''));
          if (pre) diffs2.unshift([0, aChars.slice(0, pre).join('')]);
          if (post) diffs2.push([0, aChars.slice(aChars.length - post).join('')]);
          if (canon(reconstructNew(diffs2)) === canon(nextText) && canon(reconstructOld(diffs2)) === canon(baseText)) {
            diffs = diffs2;
            okNew = okOld = true;
            strategy = 'fallback:char-trim';
          }
        } catch {}
      }
      if (!(okNew && okOld)) {
        // Fallback 1b: try diff-match-patch (semantic cleanup)
        try {
          const dmpDiffs = await dmpDiffStrings(baseText, nextText, debugTag);
          if (Array.isArray(dmpDiffs) && dmpDiffs.length) {
            const rn = (ops) => ops.map(([op,s]) => (op === -1 ? '' : s)).join('');
            const ro = (ops) => ops.map(([op,s]) => (op === 1 ? '' : s)).join('');
            if (rn(dmpDiffs) === nextText && ro(dmpDiffs) === baseText) {
              diffs = dmpDiffs; okNew = okOld = true; strategy = 'fallback:dmp';
            }
          }
        } catch {}
      }
      if (!(okNew && okOld)) {
        // Fallback 2: minimal but always-correct patch: delete all A, insert all B
        strategy = 'fallback:delete-insert-all';
        diffs = [];
        if (baseText) diffs.push([-1, baseText]);
        if (nextText) diffs.push([1, nextText]);
      }
      if (debugTag) {
        const statsDbg = computeStats(diffs);
        const vis = (s) => String(s).replace(/\n/g, '⏎').replace(/ /g, '␠');
        const sample = diffs.slice(0, 8).map(([op,s]) => [op, vis(String(s).slice(0, 48))]);
        console.log(`[diff:${debugTag}] strategy`, strategy, 'stats', statsDbg, 'ops', diffs.length);
        console.log(`[diff:${debugTag}] ops.sample`, sample);
      }
      const patchText = patch_toText_fromDiffs(diffs);

      const stats = {
        ...computeStats(diffs),
        charsBase: baseText.length,
        charsNew: nextText.length
      };

      // Return raw diffs (array of [op, text])
      self.postMessage({
        id,
        type: 'diff:result',
        patchText,
        diffs,
        stats
      });
      return;
    }

    // Unknown message → ignore politely
  } catch (err) {
    // Always surface errors in a structured way
    self.postMessage({
      id,
      type: 'diff:error',
      message: err?.message || String(err)
    });
  }
};

// Very low-memory greedy diff as last resort
function simpleGreedyDiff(a, b) {
  const diffs = [];
  let i = 0, j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      diffs.push([0, a[i]]);
      i++; j++;
    } else {
      diffs.push([-1, a[i]]);
      diffs.push([1, b[j]]);
      i++; j++;
    }
  }
  while (i < a.length) diffs.push([-1, a[i++]]);
  while (j < b.length) diffs.push([1, b[j++]]);
  return coalesceSeq(diffs, (arr) => arr.join(''));
}
