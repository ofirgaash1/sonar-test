// v2/ui/controls.js
import { store, getState } from '../core/state.js';
import { showToast } from './toast.js';
import { canonicalizeText } from '../shared/canonical.js';
import { verifyChainHash } from '../history/verify-chain.js';
import api from '../data/api.js';
import { computeAbsIndexMap } from '../render/overlay.js';

function validateTimingData(words) {
  /**
   * Validate timing data for monotonicity and reject fake timing patterns.
   * Throws Error if timing data is invalid.
   */
  if (!Array.isArray(words) || !words.length) {
    return;
  }
  
  const fakeTimingPattern = /^999999999\d/;
  
  for (let i = 0; i < words.length; i++) {
    const word = words[i];
    const start = word.start;
    const end = word.end;
    
    // Check for fake timing patterns
    if (start != null && fakeTimingPattern.test(String(start))) {
      throw new Error(`Fake timing data detected: word "${word.word || ''}" has fake start time ${start}`);
    }
    
    if (end != null && fakeTimingPattern.test(String(end))) {
      throw new Error(`Fake timing data detected: word "${word.word || ''}" has fake end time ${end}`);
    }
    
    // Check for valid timing data
    if (start != null && end != null) {
      if (end < start) {
        throw new Error(`Invalid timing data: word "${word.word || ''}" end (${end}) < start (${start})`);
      }
      
      // Check monotonicity with previous word
      if (i > 0) {
        const prevWord = words[i-1];
        const prevEnd = prevWord.end;
        if (prevEnd != null && start < prevEnd) {
          throw new Error(`Non-monotonic timing: word "${word.word || ''}" starts at ${start} but previous word ends at ${prevEnd}`);
        }
      }
    }
  }
}

export function setupUIControls(els, { workers, mergeModal }, virtualizer, playerCtrl, isIdle) {
  const dbg = (...args) => { try { if ((localStorage.getItem('v2:debug') || '').toLowerCase() === 'on') console.log(...args); } catch {} };
  // Ensure unreliable button is not hidden by default
  try { if (els.markUnreliable) els.markUnreliable.hidden = false; } catch {}
  const openAlignToast = (words = null, seconds = null) => {
    try {
      const w = Number.isFinite(+words) ? +words : null;
      const s = Number.isFinite(+seconds) ? +seconds : null;
      const msg = (w != null && s != null)
        ? `מיישר תזמונים: ${w} מילים, ${s.toFixed(1)} שניות — נשלח…`
        : 'מיישר תזמונים…';
      showToast(msg, 'info', 60000);
    } catch {}
  };
  const closeAlignToasts = () => {
    try {
      const cont = document.getElementById('toastContainer'); if (!cont) return;
      const list = Array.from(cont.querySelectorAll('.toast'));
      for (const n of list) {
        if ((n.textContent || '').trim().startsWith('מיישר תזמונים')) { try { n.remove(); } catch {} }
      }
    } catch {}
  };
  // Probability highlight toggle
  if (els.probToggle) {
    const LS_KEY = 'probHL';
    const readPref = () => {
      try { return (localStorage.getItem(LS_KEY) ?? 'on') !== 'off'; } catch { return true; }
    };
    const writePref = (on) => { try { localStorage.setItem(LS_KEY, on ? 'on' : 'off'); } catch {} };
    const updateBtn = (on) => {
      els.probToggle.setAttribute('aria-pressed', on ? 'true' : 'false');
      // Hebrew labels: on=true => show "cancel highlights"; off => "highlight low confidence"
      els.probToggle.textContent = on ? 'בטל הדגשות' : 'הדגש ודאות נמוכה';
    };

    // Initialize from store or fallback to localStorage
    try {
      const st = getState();
      const initOn = (st?.settings && typeof st.settings.probEnabled === 'boolean') ? !!st.settings.probEnabled : readPref();
      updateBtn(initOn);
      // Ensure store reflects persisted pref on first load
      if (initOn !== !!st?.settings?.probEnabled) {
        store.setProbEnabled(initOn);
      }
    } catch { /* noop */ }

    els.probToggle.addEventListener('click', () => {
      const cur = (els.probToggle.getAttribute('aria-pressed') === 'true');
      const next = !cur;
      updateBtn(next);
      writePref(next);
      try { store.setProbEnabled(next); } catch {}
    });

    // Keep button in sync if state changes elsewhere
    try {
      store.subscribe((st, tag) => {
        if (tag === 'settings:probEnabled') updateBtn(!!st.settings?.probEnabled);
      });
    } catch { /* noop */ }
  }
  // Download VTT
  const formatTimeVTT = (t) => {
    const ms = Math.max(0, Math.floor((+t || 0) * 1000));
    const h = Math.floor(ms / 3600000);
    const m = Math.floor((ms % 3600000) / 60000);
    const s = Math.floor((ms % 60000) / 1000);
    const ms3 = ms % 1000;
    const pad = (n, w) => String(n).padStart(w, '0');
    return `${pad(h,2)}:${pad(m,2)}:${pad(s,2)}.${pad(ms3,3)}`;
  };
  const buildSegmentsFromTokens = (tokens) => {
    const segs = []; let cur = null;
    for (const t of (tokens || [])) {
      if (!t || t.state === 'del') continue;
      if (t.word === '\n') { if (cur) { segs.push(cur); cur = null; } continue; }
      // CRITICAL: Do NOT generate artificial timing data by defaulting to 0
      // If timing data is missing, leave it as null to expose the bug
      if (!cur) cur = { words: [], start: Number.isFinite(t.start) ? +t.start : null, end: Number.isFinite(t.end) ? +t.end : null };
      const startVal = Number.isFinite(+t.start) ? +t.start : null;
      const endVal = Number.isFinite(+t.end) ? +t.end : null;
      cur.words.push({ word: String(t.word || ''), start: startVal, end: endVal, probability: Number.isFinite(t.probability) ? +t.probability : undefined });
      cur.end = Number.isFinite(t.end) ? +t.end : cur.end;
    }
    if (cur) segs.push(cur);
    segs.forEach(s => { s.text = (s.words || []).map(w => w.word).join(''); });
    return segs;
  };
  const generateVTT = (tokens) => {
    const segs = buildSegmentsFromTokens(tokens);
    const lines = ['WEBVTT',''];
    segs.forEach((s, i) => {
      const t1 = formatTimeVTT(s.start); const t2 = formatTimeVTT(s.end);
      lines.push(String(i+1)); lines.push(`${t1} --> ${t2}`); lines.push(s.text); lines.push('');
    });
    return lines.join('\n');
  };
  const downloadText = (filename, text, type = 'text/plain') => {
    try { const blob = new Blob([text], { type }); const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = filename || 'download.txt';
      document.body.appendChild(a); a.click(); setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 0);
    } catch (e) { console.error('download failed:', e); }
  };

  // Rate slider
  if (els.rate && els.rateVal) {
    const applyRate = (r) => { els.rateVal.textContent = `×${(+r||1).toFixed(2)}`; };
    const initRate = (() => { try { return els.player?.playbackRate || 1; } catch { return 1; }})();
    els.rate.value = String(initRate); applyRate(initRate);
    els.rate.addEventListener('input', () => {
      const r = +els.rate.value || 1;
      try { playerCtrl?.setRate(r); } catch {}
      try { if (!playerCtrl && els.player) els.player.playbackRate = r; } catch {}
      applyRate(r);
    });
  }

  // VTT export
  if (els.dlVtt) {
    els.dlVtt.addEventListener('click', () => {
      const st = getState();
      const tokens = st.tokens && st.tokens.length ? st.tokens : (st.baselineTokens || []);
      if (!tokens || !tokens.length) { showToast('אין נתונים לייצוא', 'error'); return; }
      const vtt = generateVTT(tokens);
      const folder = els.transcript?.dataset.folder || 'episode';
      const file = (els.transcript?.dataset.file || 'audio.opus').replace(/\.opus$/i, '');
      const name = `${folder}__${file}.vtt`;
      downloadText(name, vtt, 'text/vtt');
      showToast('VTT נוצר והורד', 'success');
    });
  }

  // Font size controls
  const getTextSize = () => {
    const s = getComputedStyle(document.documentElement).getPropertyValue('--text-size').trim() || '1.10rem';
    const m = /([0-9]*\.?[0-9]+)/.exec(s); return m ? parseFloat(m[1]) : 1.10;
  };
  const setTextSize = (em) => { const v = Math.max(0.8, Math.min(2.0, em)); document.documentElement.style.setProperty('--text-size', `${v}rem`); };
  if (els.fontMinus) els.fontMinus.addEventListener('click', () => setTextSize(getTextSize() - 0.05));
  if (els.fontPlus)  els.fontPlus.addEventListener('click', () => setTextSize(getTextSize() + 0.05));

  // Confirmations
  const overlaps = (a, b) => a[0] < b[1] && b[0] < a[1];
  const mergeRanges = (ranges) => { const arr = (ranges || []).slice().sort((x,y)=>x[0]-y[0]||x[1]-y[1]); const out=[]; for (const r of arr){ if(!out.length||out[out.length-1][1]<r[0]) out.push(r.slice()); else out[out.length-1][1]=Math.max(out[out.length-1][1], r[1]); } return out; };
  const selectionRange = () => {
    const sel = window.getSelection(); if (!sel || sel.rangeCount === 0) return null;
    const r = sel.getRangeAt(0); const container = els.transcript; if (!container) return null;
    const inC = n => n && (n === container || container.contains(n)); if (!(inC(r.startContainer) && inC(r.endContainer))) return null;
    const measure = (node, off) => { const rng = document.createRange(); rng.selectNodeContents(container); try { rng.setEnd(node, off); } catch { return 0; } return rng.toString().length; };
    const s = measure(r.startContainer, r.startOffset); const e = measure(r.endContainer, r.endOffset); return [Math.min(s,e), Math.max(s,e)];
  };
  const mayConfirmNow = async () => { const st = getState(); if (!st || !(st.version > 0) || !st.base_sha256) return false; try { const txt = canonicalizeText(st.liveText||''); const h = await api.api.sha256Hex(txt); return !!h && h === st.base_sha256; } catch { return false; } };
  const waitForConfirmable = async (timeoutMs = 2500) => {
    const t0 = Date.now();
    while (Date.now() - t0 < timeoutMs) {
      if (!saving) {
        const ok = await mayConfirmNow();
        if (ok) return true;
      }
      await new Promise(r => setTimeout(r, 100));
    }
    return false;
  };
  const persistConfirmations = async () => {
    const st = getState();
    if (!(st?.version > 0)) return;
    try {
      const txt = canonicalizeText(st.liveText||'');
      const ranges = (st.confirmedRanges || []).map(x => x.range);
      const filePath = `${els.transcript?.dataset.folder}/${els.transcript?.dataset.file}`;
      await api.saveConfirmations(filePath, st.version, st.base_sha256 || '', ranges, txt);
      // Round-trip reload to ensure persistence and normalize server state
      try {
        const confs = await api.getConfirmations(filePath, st.version);
        const norm = (confs || []).map(c => ({ id: c.id, range: c.range }));
        store.setConfirmedRanges(norm);
      } catch { /* ignore reload error */ }
      showToast('אישורים נשמרו', 'success');
    } catch (e) {
      console.warn('Persist confirmations failed:', e);
      showToast('שמירת אישורים נכשלה', 'error');
    }
  };
  const refreshConfirmButtons = () => {
    if (!els.markReliable || !els.markUnreliable) return;
    const conf = (getState().confirmedRanges || []).map(x=>x.range);
    const anyConfirmed = conf.length > 0;
    els.markReliable.style.display = anyConfirmed ? 'none' : '';
    // Force visible via inline-block to avoid any inherited/inline hidden styles
    if (anyConfirmed) {
      try { els.markUnreliable.hidden = false; } catch {}
      try { els.markUnreliable.style.setProperty('display', 'inline-block', 'important'); } catch {}
    } else {
      els.markUnreliable.style.display = 'none';
    }
  };
  if (els.markReliable) els.markReliable.addEventListener('click', async () => {
    let ok = await mayConfirmNow();
    if (!ok) { try { await performSave(); } catch {} }
    let sel = selectionRange();
    if (!sel) {
      try { const len = (getState().liveText || '').length; sel = [0, Math.max(0, len)]; } catch { sel = null; }
    }
    if (!sel || sel[0] === sel[1]) return;
    const conf = (getState().confirmedRanges || []).map(x=>x.range);
    const merged = mergeRanges(conf.concat([sel]));
    store.setConfirmedRanges(merged.map(r => ({ range: r })));
    // Toggle buttons immediately: selection may be cleared after clicking
    try {
      if (els.markReliable) els.markReliable.style.display = 'none';
      if (els.markUnreliable) { els.markUnreliable.hidden = false; els.markUnreliable.style.setProperty('display','inline-block','important'); }
    } catch {}
    // Persist only when confirmable; otherwise, try shortly after save completes
    if (ok || await waitForConfirmable(2500)) { try { await persistConfirmations(); } catch {} }
  });
  if (els.markUnreliable) els.markUnreliable.addEventListener('click', async () => {
    let ok = await mayConfirmNow();
    if (!ok) { try { await performSave(); } catch {} }
    const sel = selectionRange(); if (!sel) return;
    const keep = (getState().confirmedRanges || []).map(x=>x.range).filter(r => !overlaps(r, sel));
    store.setConfirmedRanges(keep.map(r => ({ range: r })));
    // Toggle buttons immediately: selection may be cleared after clicking
    try {
      if (els.markReliable) els.markReliable.style.display = '';
      if (els.markUnreliable) els.markUnreliable.style.display = 'none';
    } catch {}
    if (ok || await waitForConfirmable(2500)) { try { await persistConfirmations(); } catch {} }
  });
  document.addEventListener('selectionchange', () => { const sel = window.getSelection(); if (!sel || sel.rangeCount === 0) return; const n = sel.getRangeAt(0).commonAncestorContainer; if (els.transcript === n || (n && els.transcript.contains(n))) refreshConfirmButtons(); });
  store.subscribe((_, tag) => { if (tag === 'confirmedRanges') refreshConfirmButtons(); });

  // Back to top
  if (els.scrollTopBtn) {
    const onScroll = () => { const y = window.scrollY || document.documentElement.scrollTop || 0; els.scrollTopBtn.style.display = y > 200 ? 'block' : 'none'; };
    window.addEventListener('scroll', onScroll, { passive: true });
    els.scrollTopBtn.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));
    onScroll();
  }

  // Save (queued)
  let saveQueued = false; let saving = false;
  // Only disable the save button during an active save. While "waiting" for
  // the typing quiet window, keep it enabled so tests (and users) can click or
  // re-click without being locked out. The queued save will trigger when idle.
  const setSaveButton = (state) => {
    if (!els.submitBtn) return;
    // Keep button clickable in all states; internal flags gate actual saves
    if (state === 'saving') {
      els.submitBtn.disabled = false;
      els.submitBtn.textContent = 'שומר…';
    } else {
      // Treat 'waiting' the same as 'idle' for interactivity
      els.submitBtn.disabled = false;
      els.submitBtn.textContent = '⬆️ שמור תיקון';
    }
  };
  // Build words array directly from current tokens, preserving timings/probabilities/newlines.
  function buildWordsForSaveFromTokens(tokens) {
    const out = [];
    const src = Array.isArray(tokens) ? tokens : [];
    for (const t of src) {
      if (!t || t.state === 'del') continue;
      const w = String(t.word || '');
      const obj = { word: w };
      const s = +t.start; if (Number.isFinite(s)) obj.start = s;
      const e = +t.end;   if (Number.isFinite(e)) obj.end = e;
      const p = +t.probability; if (Number.isFinite(p)) obj.probability = p;
      out.push(obj);
    }
    return out;
  }

  // Legacy helper: build words from plain text while attempting to carry timings from source tokens when possible.
  function buildWordsForSaveFromText(text) {
    const s = String(text || '');
    const out = [];
    const st = getState();
    const source = (st && Array.isArray(st.tokens) && st.tokens.length)
      ? st.tokens
      : (Array.isArray(st?.baselineTokens) ? st.baselineTokens : []);
    let ti = 0; // scan index into source tokens

    const tryCopyFromSource = (piece) => {
      if (piece === '\n') { out.push({ word: '\n' }); return; }
      // scan forward in source to find next token with exact text match
      for (let j = ti; j < source.length; j++) {
        const t = source[j];
        if (!t || t.state === 'del') continue;
        const w = String(t.word || '');
        if (w === '\n') continue;
        if (w === piece) {
          ti = j + 1; // advance after match
          const obj = { word: piece };
          const sOK = Number.isFinite(+t.start);
          const eOK = Number.isFinite(+t.end);
          if (sOK) obj.start = +t.start;
          if (eOK) obj.end = +t.end;
          const p = +t.probability;
          if (Number.isFinite(p)) obj.probability = p;
          out.push(obj);
          return;
        }
      }
      // no match: push without timings/probability
      out.push({ word: piece });
    };

    const lines = s.split('\n');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.length) {
        const parts = line.split(/(\s+)/g);
        for (const p of parts) { if (p) tryCopyFromSource(p); }
      }
      if (i < lines.length - 1) out.push({ word: '\n' });
    }
    return out;
  }
  function getSelectionOffsets(container) {
    try {
      const sel = window.getSelection(); if (!sel || sel.rangeCount === 0) return null; const r = sel.getRangeAt(0);
      const inC = n => n && (n === container || container.contains(n)); if (!(inC(r.startContainer) && inC(r.endContainer))) return null;
      const measure = (node, off) => { const rng = document.createRange(); rng.selectNodeContents(container); try { rng.setEnd(node, off); } catch { return 0; } return rng.toString().length; };
      const s = measure(r.startContainer, r.startOffset); const e = measure(r.endContainer, r.endOffset); return [Math.min(s, e), Math.max(s, e)];
    } catch { return null; }
  }
  function estimateSegmentIndex(tokens, caretOffset) {
    if (!Array.isArray(tokens) || !tokens.length) return 0;
    const abs = computeAbsIndexMap(tokens);
    let seg = 0;
    for (let i = 0; i < tokens.length; i++) {
      const t = tokens[i];
      if (!t || t.state === 'del') continue;
      if (t.word === '\n') { if ((abs[i] || 0) <= (caretOffset || 0)) seg++; continue; }
      const startChar = abs[i] || 0;
      const endChar = startChar + (t.word ? t.word.length : 0);
      if ((caretOffset || 0) < endChar) break;
    }
    return Math.max(0, seg);
  }
  function computeWindowStats(tokens, segCenter, neighbors = 1) {
    const startSeg = Math.max(0, (segCenter|0) - Math.max(0, neighbors|0));
    const endSeg = (segCenter|0) + Math.max(0, neighbors|0);
    let seg = 0;
    let words = 0;
    let minS = Infinity, maxE = -Infinity;
    for (const t of (tokens || [])) {
      if (!t || t.state === 'del') continue;
      if (t.word === '\n') { seg++; continue; }
      if (seg >= startSeg && seg <= endSeg) {
        const w = String(t.word || '');
        if (!/^\s+$/.test(w)) words++;
        const s = Number.isFinite(+t.start) ? +t.start : NaN;
        const e = Number.isFinite(+t.end) ? +t.end : NaN;
        if (Number.isFinite(s)) minS = Math.min(minS, s);
        if (Number.isFinite(e)) maxE = Math.max(maxE, e);
      }
    }
    const seconds = (Number.isFinite(minS) && Number.isFinite(maxE) && maxE >= minS) ? (maxE - minS) : 0;
    return { words, seconds };
  }
  const countWithTimings = (arr = []) => {
    try { return (arr||[]).reduce((n,t)=> n + (((Number.isFinite(+t?.start) && +t.start>0) || (Number.isFinite(+t?.end) && +t.end>0)) ? 1 : 0), 0); } catch { return 0; }
  };
  const mergeProbabilities = (next = [], prev = []) => {
    try {
      const out = new Array(next.length);
      const nextAbs = computeAbsIndexMap(next);
      const prevAbs = computeAbsIndexMap(prev);
      // Build prev ranges with probs and timings
      const prevRanges = [];
      const prevTiming = [];
      for (let i = 0; i < prev.length; i++) {
        const p = prev[i] || {};
        const absStart = prevAbs[i] || 0;
        const absEnd = absStart + (p.word ? String(p.word).length : 0);
        if (Number.isFinite(p.probability)) {
          prevRanges.push({ s: absStart, e: absEnd, prob: +p.probability });
        }
        if (Number.isFinite(p.start) || Number.isFinite(p.end)) {
          prevTiming.push({ s: absStart, e: absEnd, start: Number.isFinite(+p.start) ? +p.start : NaN, end: Number.isFinite(+p.end) ? +p.end : NaN });
        }
      }
      const overlap = (aS, aE, bS, bE) => (aS < bE && bS < aE);
      for (let i = 0; i < next.length; i++) {
        const t = next[i] || {};
        const sAbs = nextAbs[i] || 0;
        const eAbs = sAbs + (t.word ? String(t.word).length : 0);
        let prob = t.probability;
        if (!Number.isFinite(prob)) {
          let best = NaN;
          for (const r of prevRanges) {
            if (overlap(sAbs, eAbs, r.s, r.e)) {
              if (!Number.isFinite(best) || r.prob > best) best = r.prob;
            }
          }
          if (Number.isFinite(best)) prob = best;
        }
        let start = Number.isFinite(+t.start) ? +t.start : NaN;
        let end = Number.isFinite(+t.end) ? +t.end : NaN;
        if ((!Number.isFinite(start) || start <= 0) || (!Number.isFinite(end) || end <= 0)) {
          for (const r of prevTiming) {
            if (!Number.isFinite(r.start) || !Number.isFinite(r.end)) continue;
            if (overlap(sAbs, eAbs, r.s, r.e)) {
              if (!Number.isFinite(start) || start <= 0) start = r.start;
              if (!Number.isFinite(end) || end <= 0) end = r.end;
              break;
            }
          }
        }
        const copy = { ...t };
        if (Number.isFinite(prob)) copy.probability = prob;
        if (Number.isFinite(start) && Number.isFinite(end) && end >= start) {
          copy.start = start;
          copy.end = end;
        }
        out[i] = copy;
      }
      return out;
    } catch { return next; }
  };
  async function performSave() {
    if (saving) return; const st = getState(); const tokens = st.tokens && st.tokens.length ? st.tokens : (st.baselineTokens || []);
    if (!tokens.length) { showToast('אין מה לשמור', 'error'); setSaveButton('idle'); saveQueued = false; return; }
    let text = canonicalizeText(st.liveText || ''); if (!text) text = canonicalizeText(tokens.map(t => t.word || '').join(''));
    const folder = els.transcript?.dataset.folder; const file = els.transcript?.dataset.file; if (!folder || !file) { showToast('לא נבחר קובץ', 'error'); setSaveButton('idle'); saveQueued = false; return; }
    const filePath = `${folder}/${file}`;
    // Capture caret segment index before saving
    let segIdxGuess = 0;
    try {
      const sel = getSelectionOffsets(els.transcript);
      const caret = sel ? sel[0] : 0;
      segIdxGuess = estimateSegmentIndex(tokens, caret);
    } catch {}
    try {
      // Early client-side conflict check: if server has a newer version than our base, show merge modal
      try {
        const latestProbe = await api.getLatestTranscript(filePath);
        if (latestProbe && Number.isFinite(+st.version) && (+st.version > 0) && +latestProbe.version !== +st.version) {
          const parentVer = +st.version || 0;
          const parent = await api.getTranscriptVersion(filePath, parentVer);
          const parentText = canonicalizeText(parent?.text || st.baselineText || '');
          const latestText = canonicalizeText(latestProbe?.text || '');
          const baseHash = await api.sha256Hex(parentText);
          const { diffs: d1 } = await workers.diff.send(parentText, latestText, { editCost: 8, timeoutSec: 0.8 });
          const { diffs: d2 } = await workers.diff.send(parentText, text, { editCost: 8, timeoutSec: 0.8 });
          const payload = {
            reason: 'version_conflict',
            latest: latestProbe,
            parent: { version: parentVer, base_sha256: baseHash, text: parent?.text || '' },
            diff_parent_to_latest: (Array.isArray(d1) ? d1 : []).map(x => x.join('\t')).join('\n'),
            diff_parent_to_client: (Array.isArray(d2) ? d2 : []).map(x => x.join('\t')).join('\n')
          };
          const { renderConflict } = await import('./merge-modal.js');
          renderConflict(els, payload);
          mergeModal?.open();
          setSaveButton('idle');
          return;
        }
      } catch {}

      saving = true; setSaveButton('saving');
      // Use client-known base to preserve conflict detection semantics
      const parentVersionGuess = (typeof st.version === 'number' && st.version > 0) ? st.version : null;
      const parentTextSnapshot = canonicalizeText(st.baselineText || '');
      // Skip creating a new version if nothing changed compared to the baseline snapshot
      if (parentVersionGuess != null && parentTextSnapshot === text) {
        showToast('אין שינוי לשמירה', 'info');
        return;
      }
      // Provide expectedBaseSha256 from client-known base
      const expectedBaseSha256 = st.base_sha256 || '';
      // Build words from the edited text, attempting to carry timings
      // from the current tokens where pieces match exactly.
      // This ensures the saved words reflect user text changes (e.g., מפתחים→מבטחים)
      // while preserving timings for unchanged tokens.
      let wordsForSave = buildWordsForSaveFromText(text);
      const stats = computeWindowStats(tokens, segIdxGuess, 1);
      dbg(`[dbg] save:start tokens=${tokens.length} with_timing=${countWithTimings(tokens)} seg=${segIdxGuess} window_words=${stats.words} window_sec=${stats.seconds.toFixed(3)}`);
      // Don't show alignment toast until we have actual alignment data
      const res = await api.saveTranscriptVersion(filePath, { parentVersion: parentVersionGuess, text, words: wordsForSave, expectedBaseSha256, segment: Math.max(0, segIdxGuess), neighbors: 1 });
      const childV = res?.version; const parentV = (typeof childV === 'number' && childV > 1) ? (childV - 1) : null;
      dbg(`[dbg] save:done version=${childV} base_sha256=${res?.base_sha256 ? String(res.base_sha256).slice(0,8) : ''}`);
      store.setState({ version: childV || 0, base_sha256: res?.base_sha256 || st.base_sha256 || '' }, 'version:saved');
      // Immediately refresh words from the saved version to reflect server-side normalization
      try {
        const wordsNow = await api.getTranscriptWords(filePath, childV);
        if (Array.isArray(wordsNow) && wordsNow.length) {
          const prevToks = (getState().tokens || []).slice();
          const enrichedNow = mergeProbabilities(wordsNow, prevToks);
          dbg(`[dbg] words:after-save count=${wordsNow.length} with_timing=${countWithTimings(wordsNow)}`);
          store.setTokens(enrichedNow);
          store.setLiveText(enrichedNow.map(t => t.word || '').join(''));
        }
      } catch {}
      // Trigger alignment in background to keep UI responsive and allow queued saves
      let alignmentTriggered = false;
      try {
        if (typeof childV === 'number') {
          const segHint = Math.max(0, segIdxGuess);
          const statsCopy = { words: stats.words, seconds: stats.seconds };
          if ((statsCopy.words > 0) && (statsCopy.seconds > 0)) {
            openAlignToast(statsCopy.words, statsCopy.seconds);
            alignmentTriggered = true;
          }
          if (alignmentTriggered) {
            Promise.resolve().then(async () => {
            let alignMsg = null, alignType = 'info';
            let finished = false;
            // Safety net: if align does not finish within ~12s, emit an error toast
            const fallbackTimer = setTimeout(() => {
              if (finished) return;
              try { closeAlignToasts(); } catch {}
              const w = statsCopy.words || 0; const sec = statsCopy.seconds || 0;
              try { showToast(`מיישר תזמונים: ${w} מילים, ${sec.toFixed(1)} שניות — שגיאה`, 'error'); } catch {}
            }, 12000);
            try {
              const t0 = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
              const ar = await api.alignSegment(filePath, { version: childV, segment: segHint, neighbors: 1 });
              const t1 = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
              const w = statsCopy.words || 0; const sec = statsCopy.seconds || 0;
              dbg(`[dbg] align:resp ok=${!!(ar&&ar.ok)} changed=${+ar?.changed_count||0} total=${+ar?.total_compared||0}`);
              if (ar && ar.ok) {
                const ch = Number.isFinite(+ar.changed_count) ? +ar.changed_count : 0;
                const dt = Math.max(0, (t1 - t0));
                const took = dt >= 1000 ? `${(dt/1000).toFixed(1)}s` : `${Math.round(dt)}ms`;
                alignMsg = `מיישר תזמונים: ${w} מילים, ${sec.toFixed(1)} שניות — עודכנו ${ch} — ${took}`;
                alignType = ch > 0 ? 'success' : 'info';
              } else {
                const dt = Math.max(0, (t1 - t0));
                const took = dt >= 1000 ? `${(dt/1000).toFixed(1)}s` : `${Math.round(dt)}ms`;
                alignMsg = `מיישר תזמונים: ${w} מילים, ${sec.toFixed(1)} שניות — ללא שינוי — ${took}`;
                alignType = 'info';
              }
            } catch (eAlign) {
              const w = statsCopy.words || 0; const sec = statsCopy.seconds || 0;
              dbg('[dbg] align failed:', eAlign?.message || eAlign);
              alignMsg = `מיישר תזמונים: ${w} מילים, ${sec.toFixed(1)} שניות — שגיאה`;
              alignType = 'error';
            }
            try {
              const aligned = await api.getTranscriptWords(filePath, childV);
              if (Array.isArray(aligned) && aligned.length) {
                // Validate timing data before accepting it
                validateTimingData(aligned);
                
                const prevToks = (getState().tokens || []).slice();
                const enriched = mergeProbabilities(aligned, prevToks);
                dbg(`[dbg] words:received count=${aligned.length} with_timing=${countWithTimings(aligned)} prev_with_timing=${countWithTimings(prevToks)} prob_before=${aligned.filter(x=>Number.isFinite(x.probability)).length} prob_after=${enriched.filter(x=>Number.isFinite(x.probability)).length}`);
                store.setTokens(enriched);
                store.setLiveText(enriched.map(t => t.word || '').join(''));
                
                // Only show success if timing data is valid
                finished = true; try { clearTimeout(fallbackTimer); } catch {}
                closeAlignToasts();
                if (alignMsg) { try { showToast(alignMsg, alignType); } catch {} }
                showToast('השינויים נשמרו בהצלחה', 'success');
              } else {
                throw new Error('No aligned data received');
              }
            } catch (validationError) {
              finished = true; try { clearTimeout(fallbackTimer); } catch {}
              closeAlignToasts();
              dbg('[dbg] timing validation failed:', validationError?.message || validationError);
              showToast(`שגיאה בנתוני התזמון: ${validationError?.message || 'נתונים לא תקינים'}`, 'error');
              throw validationError; // Re-throw to prevent success toast
            } finally {
              if (!finished) {
                finished = true; try { clearTimeout(fallbackTimer); } catch {}
                closeAlignToasts();
                if (alignMsg) { try { showToast(alignMsg, alignType); } catch {} }
              }
            }
          });
          }
        }
      } catch {}
      
      // Show success message immediately if no alignment was triggered
      if (!alignmentTriggered) {
        showToast('השינויים נשמרו בהצלחה', 'success');
      }
      try {
        if (typeof childV === 'number' && childV > 1) {
          // Re-fetch the actual parent by version (strongly consistent baseline)
          let parent = null;
          try { parent = await api.getTranscriptVersion(filePath, parentV); } catch {}
          const parentText = canonicalizeText(parent?.text || parentTextSnapshot || '');
          if (parentText) {
          // Optional: save edit ops history via backend if available (handled server-side on save)
          const { diffs } = await workers.diff.send(parentText, text, { timeoutSec: 0.8, editCost: 8 });
          // Server already records deltas; client-side persistence optional and omitted.
          }
        }
      } catch (eHist) {
        console.debug('Edit history save skipped:', eHist?.message || eHist);
      }
      setSaveButton('idle');
      // Verify version chain integrity (v1 + all ops → latest hash)
      try {
        const vRes = await verifyChainHash(filePath);
        if (vRes && vRes.ok) {
          const short = (vRes.expected || '').slice(0, 8);
          showToast(`אימות גרסה הצליח (hash ${short})`, 'success');
        } else if (vRes) {
          if (vRes.reason === 'no-version') {
            // nothing to verify (shouldn't happen right after save)
          } else if (vRes.reason === 'missing-v1') {
            showToast('אימות גרסה נכשל: v1 חסרה', 'error');
          } else if (vRes.reason === 'bad-ops') {
            showToast(`אימות גרסה נכשל: עדכון פגום ב-v${vRes.at}`, 'error');
          } else if (vRes.reason === 'ops-dont-match-parent') {
            showToast(`אימות גרסה נכשל: רצף עריכות לא עקבי ב-v${vRes.at}`, 'error');
          } else if (vRes.reason === 'exception') {
            showToast(`אימות גרסה נכשל: ${vRes.message || 'שגיאה'}`, 'error');
          } else {
            const got = (vRes.got || '').slice(0, 8);
            const exp = (vRes.expected || '').slice(0, 8);
            showToast(`אי-תאמה בגיבוב: ${got} ≠ ${exp}`, 'error');
            try {
              const { renderHashMismatch } = await import("./merge-modal.js");
              renderHashMismatch(els, { expected: vRes.expected, got: vRes.got, reason: vRes.reason, at: vRes.at, tip: "בדוק אם נשמרה גרסה חדשה במקביל (מזג/טען מחדש)." });
              mergeModal?.open?.();
            } catch {}
          }
        }
      } catch (e) {
        console.warn('verifyChainHash failed:', e);
      }
    } catch (e1) {
      // Conflict-aware handling: if backend responded with 409, open merge dialog
      if (e1 && e1.code === 409 && e1.payload) {
        try {
          const payload = e1.payload;
          // Render dialog
          const { renderConflict } = await import('./merge-modal.js');
          renderConflict(els, payload);
          mergeModal?.open();
          // Wire actions
          const reload = async () => {
            try {
              const latest = await api.getLatestTranscript(filePath);
              if (!latest) return;
              const words = await api.getTranscriptWords(filePath, latest.version);
              const toks = Array.isArray(words) && words.length ? words : tokens;
              store.setTokens(toks);
              store.setLiveText((toks || []).map(t => t.word || '').join(''));
              store.setState({ version: latest.version || 0, base_sha256: latest.base_sha256 || '' }, 'version:saved');
              showToast('נטענה הגרסה העדכנית', 'info');
            } catch (e) { console.warn('reload latest failed:', e); }
            mergeModal?.close();
          };
          const tryMerge = async () => {
            try {
              const baseText = canonicalizeText(payload?.parent?.text || '');
              const latestText = canonicalizeText(payload?.latest?.text || '');
              const clientText = text; // already canonicalized above

              // compute diffs base->latest and base->client using worker
              const [d1, d2] = await Promise.all([
                workers.diff.send(baseText, latestText, { editCost: 8, timeoutSec: 0.8 }),
                workers.diff.send(baseText, clientText, { editCost: 8, timeoutSec: 0.8 })
              ]);
              const diffsA = Array.isArray(d1?.diffs) ? d1.diffs : [];
              const diffsB = Array.isArray(d2?.diffs) ? d2.diffs : [];

              function toEdits(base, diffs) {
                const edits = [];
                let pos = 0;
                let pendingDelStart = null; let pendingDelLen = 0;
                for (const [op, str] of diffs) {
                  const s = String(str||'');
                  if (op === 0) { // equal
                    if (pendingDelStart != null) {
                      // deletion with no insertion becomes replacement with empty
                      edits.push({ start: pendingDelStart, end: pendingDelStart + pendingDelLen, ins: '' });
                      pendingDelStart = null; pendingDelLen = 0;
                    }
                    pos += s.length;
                  } else if (op === -1) { // delete
                    if (pendingDelStart == null) { pendingDelStart = pos; pendingDelLen = 0; }
                    pendingDelLen += s.length; pos += s.length;
                  } else if (op === 1) { // insert
                    if (pendingDelStart != null) {
                      edits.push({ start: pendingDelStart, end: pendingDelStart + pendingDelLen, ins: s });
                      pendingDelStart = null; pendingDelLen = 0;
                    } else {
                      edits.push({ start: pos, end: pos, ins: s });
                    }
                  }
                }
                if (pendingDelStart != null) {
                  edits.push({ start: pendingDelStart, end: pendingDelStart + pendingDelLen, ins: '' });
                }
                return edits;
              }

              function overlaps(a, b) {
                // insertion (start==end) conflicts if inside other's replacement range
                const aIns = (a.start === a.end); const bIns = (b.start === b.end);
                if (aIns && bIns) return a.start === b.start; // both insert at same point => conflict
                if (aIns) return (a.start >= b.start && a.start < b.end);
                if (bIns) return (b.start >= a.start && b.start < a.end);
                return a.start < b.end && b.start < a.end;
              }

              const editsLatest = toEdits(baseText, diffsA);
              const editsMine   = toEdits(baseText, diffsB);

              // detect overlap
              for (const e1 of editsLatest) {
                for (const e2 of editsMine) {
                  if (overlaps(e1, e2)) {
                    showToast('יש התנגשויות חופפות – מיזוג אוטומטי נכשל', 'error');
                    return; // leave modal open
                  }
                }
              }

              // combine and apply to base from right to left
              const combined = editsLatest.concat(editsMine).sort((a,b) => b.start - a.start || b.end - a.end);
              let merged = baseText;
              for (const e of combined) {
                merged = merged.slice(0, e.start) + e.ins + merged.slice(e.end);
              }

              // Build minimal tokens from merged text (server will adjust timings later)
              // CRITICAL: Do NOT generate artificial timing data with start: 0, end: 0
              // If timing data is missing, leave it as null to expose the bug
              const tokensMerged = Array.from(merged).map(ch => ({ word: ch === '\n' ? '\n' : ch, start: null, end: null }));

              // Try saving merged result against latest
              const latest = payload.latest;
              const expected = await api.sha256Hex(canonicalizeText(latest?.text || ''));
              const saveRes = await api.saveTranscriptVersion(filePath, { parentVersion: latest?.version ?? null, text: merged, words: tokensMerged, expectedBaseSha256: expected });

              // Update UI with merged saved
              store.setTokens(tokensMerged);
              store.setLiveText(merged);
              store.setState({ version: saveRes?.version || 0, base_sha256: saveRes?.base_sha256 || '' }, 'version:saved');
              showToast('מיזוג אוטומטי הצליח ונשמר', 'success');
              mergeModal?.close();
            } catch (err) {
              console.warn('Auto-merge failed:', err);
              showToast('מיזוג אוטומטי נכשל', 'error');
            }
          };
          if (els.mergeReload) {
            els.mergeReload.onclick = reload;
          }
          if (els.mergeTry) {
            els.mergeTry.onclick = tryMerge;
          }
          return; // don't fall back to legacy in conflict case
        } catch (e) {
          console.warn('Conflict dialog failed:', e);
        }
      }
      // Probe for conflict even if error wasn’t explicitly 409 (e.g., transient DB lock)
      try {
        const latestProbe = await api.getLatestTranscript(filePath);
        if (latestProbe && Number.isFinite(+st.version) && +latestProbe.version !== +st.version) {
          const parentVer = +st.version || 0;
          const parent = await api.getTranscriptVersion(filePath, parentVer);
          const parentText = canonicalizeText(parent?.text || st.baselineText || '');
          const latestText = canonicalizeText(latestProbe?.text || '');
          const baseHash = await api.sha256Hex(parentText);
          const { diffs: d1 } = await workers.diff.send(parentText, latestText, { editCost: 8, timeoutSec: 0.8 });
          const { diffs: d2 } = await workers.diff.send(parentText, text, { editCost: 8, timeoutSec: 0.8 });
          const payload = {
            reason: 'version_conflict',
            latest: latestProbe,
            parent: { version: parentVer, base_sha256: baseHash, text: parent?.text || '' },
            diff_parent_to_latest: (Array.isArray(d1) ? d1 : []).map(x => x.join('\t')).join('\n'),
            diff_parent_to_client: (Array.isArray(d2) ? d2 : []).map(x => x.join('\t')).join('\n')
          };
          const { renderConflict } = await import('./merge-modal.js');
          renderConflict(els, payload);
          mergeModal?.open();
          return;
        }
      } catch {}
      console.warn('Versioned save failed:', e1);
      showToast('שמירה נכשלה', 'error');
    } finally { saving = false; saveQueued = false; setSaveButton('idle'); try { api.markCorrection(filePath); } catch {}; try { const fileItem = els.files?.querySelector(`[data-file="${file}"]`); if (fileItem) { fileItem.classList.add('has-correction'); fileItem.classList.remove('no-correction'); } } catch {} }
  }
  function checkQueuedSave() { if (saveQueued && isIdle() && !saving) performSave(); }
  setInterval(checkQueuedSave, 200);
  if (els.submitBtn) els.submitBtn.addEventListener('click', async () => { if (!isIdle()) { saveQueued = true; setSaveButton('waiting'); showToast('השינויים טרם נשמרו - אנא המתן לסיום עיבוד', 'info'); return; } await performSave(); });
}



