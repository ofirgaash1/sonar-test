// v2/ui/controls.js
import { store, getState } from '../core/state.js';
import { showToast } from '../v2/ui/toast.js';
import { canonicalizeText } from '../shared/canonical.js';
import { verifyChainHash } from '../history/verify-chain.js';
import { saveTranscriptVersion, saveTranscriptEdit, saveCorrectionToDB, markCorrection, getLatestTranscript, getTranscriptVersion, saveConfirmations, sha256Hex } from '../data/api.js';

export function setupUIControls(els, { workers }, virtualizer, playerCtrl, isIdle) {
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
      if (!cur) cur = { words: [], start: Number.isFinite(t.start) ? +t.start : 0, end: Number.isFinite(t.end) ? +t.end : 0 };
      cur.words.push({ word: String(t.word || ''), start: +t.start || 0, end: +t.end || ((+t.start || 0) + 0.25), probability: Number.isFinite(t.probability) ? +t.probability : undefined });
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
  const mayConfirmNow = async () => { const st = getState(); if (!st || !(st.version > 0) || !st.base_sha256) return false; try { const txt = (st.liveText||''); const h = await sha256Hex(txt); return !!h && h === st.base_sha256; } catch { return false; } };
  const persistConfirmations = async () => {
    try { const st = getState(); const txt = canonicalizeText(st.liveText||''); const ranges = (st.confirmedRanges || []).map(x => x.range); if (!(st.version > 0)) return; await saveConfirmations(`${els.transcript?.dataset.folder}/${els.transcript?.dataset.file}`, st.version, st.base_sha256 || '', ranges, txt); showToast('אישורים נשמרו', 'success'); }
    catch (e) { console.warn('Persist confirmations failed:', e); showToast('שמירת אישורים נכשלה', 'error'); }
  };
  const refreshConfirmButtons = () => {
    if (!els.markReliable || !els.markUnreliable) return;
    const sel = selectionRange(); const conf = (getState().confirmedRanges || []).map(x=>x.range);
    const inConfirmed = sel && conf.some(r => overlaps(r, sel));
    els.markReliable.style.display = inConfirmed ? 'none' : '';
    els.markUnreliable.style.display = inConfirmed ? '' : 'none';
  };
  if (els.markReliable) els.markReliable.addEventListener('click', () => { mayConfirmNow().then(ok => { if (!ok) { showToast('שמור ואז אשר (hash mismatch)', 'error'); return; } const sel = selectionRange(); if (!sel || sel[0] === sel[1]) return; const conf = (getState().confirmedRanges || []).map(x=>x.range); const merged = mergeRanges(conf.concat([sel])); store.setConfirmedRanges(merged.map(r => ({ range: r }))); refreshConfirmButtons(); persistConfirmations(); }); });
  if (els.markUnreliable) els.markUnreliable.addEventListener('click', () => { mayConfirmNow().then(ok => { if (!ok) { showToast('שמור ואז אשר (hash mismatch)', 'error'); return; } const sel = selectionRange(); if (!sel) return; const keep = (getState().confirmedRanges || []).map(x=>x.range).filter(r => !overlaps(r, sel)); store.setConfirmedRanges(keep.map(r => ({ range: r }))); refreshConfirmButtons(); persistConfirmations(); }); });
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
  const setSaveButton = (state) => { if (!els.submitBtn) return; if (state === 'waiting') { els.submitBtn.disabled = true; els.submitBtn.textContent = 'ממתין לעיבוד…'; } else if (state === 'saving') { els.submitBtn.disabled = true; els.submitBtn.textContent = 'שומר…'; } else { els.submitBtn.disabled = false; els.submitBtn.textContent = '⬆️ שמור תיקון'; } };
<<<<<<< Updated upstream:ui/controls.js
=======
  // Build a compact words array from plain text, preserving newline boundaries and lightweight whitespace tokens
  function buildWordsForSaveFromText(text) {
    const out = [];
    const s = String(text || '');
    const lines = s.split('\n');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.length) {
        // Split to preserve spaces as separate tokens for faithful reconstruction
        const parts = line.split(/(\s+)/g);
        for (const p of parts) {
          if (!p) continue;
          out.push({ word: p });
        }
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
>>>>>>> Stashed changes:v2/ui/controls.js
  async function performSave() {
    if (saving) return; const st = getState(); const tokens = st.tokens && st.tokens.length ? st.tokens : (st.baselineTokens || []);
    if (!tokens.length) { showToast('אין מה לשמור', 'error'); setSaveButton('idle'); saveQueued = false; return; }
    let text = canonicalizeText(st.liveText || ''); if (!text) text = canonicalizeText(tokens.map(t => t.word || '').join(''));
    const folder = els.transcript?.dataset.folder; const file = els.transcript?.dataset.file; if (!folder || !file) { showToast('לא נבחר קובץ', 'error'); setSaveButton('idle'); saveQueued = false; return; }
    const filePath = `${folder}/${file}`;
    try {
      saving = true; setSaveButton('saving');
      // Pre-fetch latest for no-op check only
      let latest = null; try { latest = await getLatestTranscript(filePath); } catch {}
      const parentVersionGuess = latest?.version ?? null; const parentTextSnapshot = canonicalizeText(latest?.text || '');
      // Skip creating a new version if nothing changed compared to the latest snapshot
      if (parentVersionGuess != null && parentTextSnapshot === text) {
        showToast('אין שינוי לשמירה', 'info');
        return;
      }
<<<<<<< Updated upstream:ui/controls.js
      const res = await saveTranscriptVersion(filePath, { parentVersion: parentVersionGuess, text, words: tokens });
=======
      // Provide expectedBaseSha256 for authoritative hash-gate on backend: hash of parent text
      const expectedBaseSha256 = (latest?.text != null) ? await sha256Hex(String(latest.text)) : '';
      // Build compact words array from text for persistence (avoid per-character tokens)
      let wordsForSave = buildWordsForSaveFromText(text);
      const res = await saveTranscriptVersion(filePath, { parentVersion: parentVersionGuess, text, words: wordsForSave, expectedBaseSha256 });
>>>>>>> Stashed changes:v2/ui/controls.js
      const childV = res?.version; const parentV = (typeof childV === 'number' && childV > 1) ? (childV - 1) : null;
      store.setState({ version: childV || 0, base_sha256: res?.base_sha256 || st.base_sha256 || '' }, 'version:saved');
      try {
        if (typeof childV === 'number' && childV > 1) {
          // Re-fetch the actual parent by version (strongly consistent baseline)
          let parent = null;
          try { parent = await getTranscriptVersion(filePath, parentV); } catch {}
          const parentText = canonicalizeText(parent?.text || parentTextSnapshot || '');
          if (parentText) {
            const { diffs } = await workers.diff.send(parentText, text, { timeoutSec: 0.8, editCost: 8 });
            const patchJson = JSON.stringify(diffs || []);
            await saveTranscriptEdit(filePath, parentV, childV, patchJson, patchJson);
          }
        }
      } catch (eHist) {
        console.debug('Edit history save skipped:', eHist?.message || eHist);
      }
      showToast('השינויים נשמרו בהצלחה', 'success');
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
          }
        }
      } catch (e) {
        console.warn('verifyChainHash failed:', e);
      }
    } catch (e1) {
<<<<<<< Updated upstream:ui/controls.js
      console.warn('Versioned save failed, falling back to correction JSON:', e1);
      const segs = buildSegmentsFromTokens(tokens).map(s => ({ start: s.start, end: s.end, text: s.text, words: s.words })); const json = { text: segs.map(s=>s.text).join('\n'), segments: segs };
      const res2 = await saveCorrectionToDB(filePath, json); console.log('Correction saved (legacy):', res2);
      showToast('התיקון נשמר בהצלחה (legacy)', 'success');
=======
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
              const latest = await getLatestTranscript(filePath);
              if (!latest) return;
              const words = await getTranscriptWords(filePath, latest.version);
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

              // Build compact words from merged text (server will adjust timings later)
              const tokensMerged = buildWordsForSaveFromText(merged);

              // Try saving merged result against latest
              const latest = payload.latest;
              const expected = await sha256Hex(canonicalizeText(latest?.text || ''));
              const saveRes = await saveTranscriptVersion(filePath, { parentVersion: latest?.version ?? null, text: merged, words: tokensMerged, expectedBaseSha256: expected });

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
      console.warn('Versioned save failed:', e1);
      showToast('שמירה נכשלה', 'error');
>>>>>>> Stashed changes:v2/ui/controls.js
    } finally { saving = false; saveQueued = false; setSaveButton('idle'); try { markCorrection(filePath); } catch {}; try { const fileItem = els.files?.querySelector(`[data-file="${file}"]`); if (fileItem) { fileItem.classList.add('has-correction'); fileItem.classList.remove('no-correction'); } } catch {} }
  }
  function checkQueuedSave() { if (saveQueued && isIdle() && !saving) performSave(); }
  setInterval(checkQueuedSave, 200);
  if (els.submitBtn) els.submitBtn.addEventListener('click', async () => { if (!isIdle()) { saveQueued = true; setSaveButton('waiting'); showToast('השינויים טרם נשמרו - אנא המתן לסיום עיבוד', 'info'); return; } await performSave(); });
}
