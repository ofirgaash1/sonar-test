// v2/workers/diff-core.js
// Pure JS diff helpers shared by tests (Node) and browser. No web worker APIs here.

function safeString(x) { return typeof x === 'string' ? x : String(x ?? ''); }
function toChars(s) { return Array.from(s || ''); }
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

function commonPrefixLen(a, b) { const L = Math.min(a.length, b.length); let i = 0; while (i < L && a[i] === b[i]) i++; return i; }
function commonSuffixLen(a, b) { const L = Math.min(a.length, b.length); let i = 0; while (i < L && a[a.length-1-i] === b[b.length-1-i]) i++; return i; }

// Myers O(ND) diff on arrays
function myersDiffSeq(a, b, joiner) {
  const N = a.length, M = b.length; const max = N + M; const v = new Int32Array(2 * max + 1); const trace = []; const offset = max;
  for (let d = 0; d <= max; d++) {
    trace.push(v.slice());
    for (let k = -d; k <= d; k += 2) {
      const idx = k + offset;
      let x;
      if (k === -d || (k !== d && v[idx - 1] < v[idx + 1])) x = v[idx + 1];
      else x = v[idx - 1] + 1;
      let y = x - k;
      while (x < N && y < M && a[x] === b[y]) { x++; y++; }
      v[idx] = x;
      if (x >= N && y >= M) return backtrackSeq(a, b, trace, k, x, y, d, offset, joiner);
    }
  }
  return [[0, joiner(a)]];
}
function backtrackSeq(a, b, trace, k, x, y, d, offset, joiner) {
  const diffs = [];
  for (let D = d; D > 0; D--) {
    const v = trace[D]; const kIdx = k + offset; let prevK;
    if (k === -D || (k !== D && v[kIdx - 1] < v[kIdx + 1])) prevK = k + 1; else prevK = k - 1;
    const prevX = trace[D - 1][prevK + offset]; const prevY = prevX - prevK;
    while (x > prevX && y > prevY) { diffs.push([0, a[x - 1]]); x--; y--; }
    if (x === prevX) diffs.push([1, b[prevY]]); else diffs.push([-1, a[prevX]]);
    x = prevX; y = prevY; k = prevK;
  }
  while (x > 0 && y > 0) { diffs.push([0, a[x - 1]]); x--; y--; }
  while (x > 0) { diffs.push([-1, a[--x]]); }
  while (y > 0) { diffs.push([1, b[--y]]); }
  diffs.reverse();
  return coalesceSeq(diffs, joiner);
}
function coalesceSeq(diffs, joiner) {
  if (!diffs.length) return diffs; const out = []; let lastOp = diffs[0][0]; let buffer = [diffs[0][1]];
  for (let i = 1; i < diffs.length; i++) { const [op, ch] = diffs[i]; if (op === lastOp) buffer.push(ch); else { out.push([lastOp, joiner(buffer)]); lastOp = op; buffer = [ch]; } }
  out.push([lastOp, joiner(buffer)]); return out;
}

function charDiffStrings(aStr, bStr) {
  const aChars = toChars(aStr); const bChars = toChars(bStr);
  const pre = commonPrefixLen(aChars, bChars);
  const aMid = aChars.slice(pre); const bMid = bChars.slice(pre);
  const post = commonSuffixLen(aMid, bMid);
  const aC = aChars.slice(pre, aChars.length - post); const bC = bChars.slice(pre, bChars.length - post);
  let diffs = myersDiffSeq(aC, bC, (arr) => arr.join(''));
  if (pre) diffs.unshift([0, aChars.slice(0, pre).join('')]);
  if (post) diffs.push([0, aChars.slice(aChars.length - post).join('')]);
  return normalizeDiffs(diffs);
}

function granularDiff(baseText, nextText) {
  const aLines = splitLinesKeepNL(baseText); const bLines = splitLinesKeepNL(nextText);
  const pre = commonPrefixLen(aLines, bLines); const aTail = aLines.slice(pre); const bTail = bLines.slice(pre);
  const post = commonSuffixLen(aTail, bTail); const out = [];
  if (pre) out.push([0, aLines.slice(0, pre).join('')]);
  const aMid = aLines.slice(pre, aLines.length - post); const bMid = bLines.slice(pre, bLines.length - post);
  if (aMid.length || bMid.length) {
    if (aMid.length === 1 && bMid.length === 1) {
      // refine single line with char diff
      const refined = charDiffStrings(aMid[0] || '', bMid[0] || '');
      for (const d of refined) { const L = out.length; if (L && out[L-1][0] === d[0]) out[L-1][1] += d[1]; else out.push([d[0], d[1]]); }
    } else {
      const lineDiffs = myersDiffSeq(aMid, bMid, (arr) => arr.join(''));
      const delBuf = []; const insBuf = [];
      for (const [op, chunk] of lineDiffs) {
        if (op === 0) { while (delBuf.length) { out.push([-1, delBuf.shift()]); } while (insBuf.length) { out.push([1, insBuf.shift()]); } out.push([0, chunk]); continue; }
        if (op === -1) { if (insBuf.length) { const newChunk = insBuf.shift(); const refined = charDiffStrings(chunk, newChunk); for (const d of refined) { const L = out.length; if (L && out[L-1][0] === d[0]) out[L-1][1] += d[1]; else out.push([d[0], d[1]]); } } else { delBuf.push(chunk); } continue; }
        // op === 1
        if (delBuf.length) { const oldChunk = delBuf.shift(); const refined = charDiffStrings(oldChunk, chunk); for (const d of refined) { const L = out.length; if (L && out[L-1][0] === d[0]) out[L-1][1] += d[1]; else out.push([d[0], d[1]]); } } else { insBuf.push(chunk); }
      }
      while (delBuf.length) { out.push([-1, delBuf.shift()]); }
      while (insBuf.length) { out.push([1, insBuf.shift()]); }
    }
  }
  if (post) out.push([0, aLines.slice(aLines.length - post).join('')]);
  return out;
}

export function reconstructNew(ops) { try { return (ops || []).map(([op, s]) => (op === -1 ? '' : (s || ''))).join(''); } catch { return ''; } }
export function reconstructOld(ops) { try { return (ops || []).map(([op, s]) => (op === 1 ? '' : (s || ''))).join(''); } catch { return ''; } }

function canon(s){ try{ let t = String(s||''); t = t.replace(/\r\n/g, '\n').replace(/\r/g, '\n'); t = t.replace(/\u00A0/g, ' '); t = t.replace(/[\u200E\u200F\u202A-\u202E\u2066-\u2069]/g, ''); if(t.normalize) t = t.normalize('NFC'); return t; } catch { return String(s||''); } }

export function diffStrings(a, b) {
  const A = safeString(a), B = safeString(b);
  // line+char granular
  let diffs = normalizeDiffs(granularDiff(A, B));
  const ok = (canon(reconstructNew(diffs)) === canon(B)) && (canon(reconstructOld(diffs)) === canon(A));
  if (ok) return diffs;
  // fallback to pure char diff over whole text
  diffs = normalizeDiffs(charDiffStrings(A, B));
  const ok2 = (canon(reconstructNew(diffs)) === canon(B)) && (canon(reconstructOld(diffs)) === canon(A));
  if (ok2) return diffs;
  // last resort: delete-insert
  const out = []; if (A) out.push([-1, A]); if (B) out.push([1, B]); return out;
}

export default { diffStrings, reconstructNew, reconstructOld };

