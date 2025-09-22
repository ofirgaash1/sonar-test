"""
Private helper functions for transcript and export routes.
"""
from __future__ import annotations

import hashlib
import difflib
import os
from pathlib import Path
import logging
import time
import uuid
import re as _re
from typing import Optional

import orjson
from flask import current_app, abort
import subprocess
import requests

from ..services.db import DatabaseService

logger = logging.getLogger(__name__)


# --- Lightweight, idempotent schema migrations ---
_TARGET_SCHEMA_VERSION = 3

def _get_user_version(db: DatabaseService) -> int:
    try:
        cur = db.execute("PRAGMA user_version")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def _set_user_version(db: DatabaseService, v: int) -> None:
    db.execute(f"PRAGMA user_version = {int(v)}")


def _table_exists(db: DatabaseService, name: str) -> bool:
    cur = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [name])
    return cur.fetchone() is not None


def _column_exists(db: DatabaseService, table: str, column: str) -> bool:
    try:
        cur = db.execute(f"PRAGMA table_info({table})")
        for r in cur.fetchall() or []:
            if len(r) >= 2 and str(r[1]).lower() == column.lower():
                return True
    except Exception:
        pass
    return False


def ensure_schema(db: DatabaseService):
    """Create/upgrade schema in an idempotent, versioned manner.

    Uses SQLite PRAGMA user_version to track migrations.
    """
    current = _get_user_version(db)

    # v1: Base tables
    if current < 1:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcripts (
                file_path   TEXT NOT NULL,
                version     INTEGER NOT NULL,
                base_sha256 TEXT NOT NULL,
                text        TEXT NOT NULL,
                words       TEXT NOT NULL,
                created_by  TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (file_path, version)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_edits (
                file_path      TEXT NOT NULL,
                parent_version INTEGER NOT NULL,
                child_version  INTEGER NOT NULL,
                dmp_patch      TEXT,
                token_ops      TEXT,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (file_path, parent_version, child_version)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_confirmations (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path    TEXT NOT NULL,
                version      INTEGER NOT NULL,
                base_sha256  TEXT NOT NULL,
                start_offset INTEGER NOT NULL,
                end_offset   INTEGER NOT NULL,
                prefix       TEXT,
                exact        TEXT,
                suffix       TEXT,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_words (
                file_path     TEXT NOT NULL,
                version       INTEGER NOT NULL,
                segment_index INTEGER NOT NULL,
                word_index    INTEGER NOT NULL,
                word          TEXT NOT NULL,
                start_time    DOUBLE,
                end_time      DOUBLE,
                probability   DOUBLE,
                PRIMARY KEY (file_path, version, word_index)
            )
            """
        )
        # Helpful indexes
        db.execute("CREATE INDEX IF NOT EXISTS idx_transcripts_doc_ver ON transcripts(file_path, version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_edits_doc_child ON transcript_edits(file_path, child_version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_conf_doc_ver ON transcript_confirmations(file_path, version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_tw_doc_ver_seg ON transcript_words(file_path, version, segment_index)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_tw_doc_ver ON transcript_words(file_path, version)")
        _set_user_version(db, 1)

    # v2: Backfill created_by column on transcripts if missing; add secondary indexes
    if current < 2:
        if not _column_exists(db, 'transcripts', 'created_by'):
            db.execute("ALTER TABLE transcripts ADD COLUMN created_by TEXT")
        # Ensure helpful indexes exist (idempotent)
        db.execute("CREATE INDEX IF NOT EXISTS idx_transcripts_doc_ver ON transcripts(file_path, version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_edits_doc_child ON transcript_edits(file_path, child_version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_conf_doc_ver ON transcript_confirmations(file_path, version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_tw_doc_ver_seg ON transcript_words(file_path, version, segment_index)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_tw_doc_ver ON transcript_words(file_path, version)")
        _set_user_version(db, 2)

    # v3: Defensive create-if-missing for all tables
    if current < 3:
        # Tables (no-op if already exist)
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcripts (
                file_path   TEXT NOT NULL,
                version     INTEGER NOT NULL,
                base_sha256 TEXT NOT NULL,
                text        TEXT NOT NULL,
                words       TEXT NOT NULL,
                created_by  TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (file_path, version)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_edits (
                file_path      TEXT NOT NULL,
                parent_version INTEGER NOT NULL,
                child_version  INTEGER NOT NULL,
                dmp_patch      TEXT,
                token_ops      TEXT,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (file_path, parent_version, child_version)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_confirmations (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path    TEXT NOT NULL,
                version      INTEGER NOT NULL,
                base_sha256  TEXT NOT NULL,
                start_offset INTEGER NOT NULL,
                end_offset   INTEGER NOT NULL,
                prefix       TEXT,
                exact        TEXT,
                suffix       TEXT,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS transcript_words (
                file_path     TEXT NOT NULL,
                version       INTEGER NOT NULL,
                segment_index INTEGER NOT NULL,
                word_index    INTEGER NOT NULL,
                word          TEXT NOT NULL,
                start_time    DOUBLE,
                end_time      DOUBLE,
                probability   DOUBLE,
                PRIMARY KEY (file_path, version, word_index)
            )
            """
        )
        # Indexes
        db.execute("CREATE INDEX IF NOT EXISTS idx_transcripts_doc_ver ON transcripts(file_path, version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_edits_doc_child ON transcript_edits(file_path, child_version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_conf_doc_ver ON transcript_confirmations(file_path, version)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_tw_doc_ver_seg ON transcript_words(file_path, version, segment_index)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_tw_doc_ver ON transcript_words(file_path, version)")

        _set_user_version(db, 3)

    # Post-check: ensure required columns exist even if user_version is incorrect
    try:
        # transcripts
        if _table_exists(db, 'transcripts'):
            if not _column_exists(db, 'transcripts', 'words'):
                db.execute("ALTER TABLE transcripts ADD COLUMN words TEXT")
            if not _column_exists(db, 'transcripts', 'created_by'):
                db.execute("ALTER TABLE transcripts ADD COLUMN created_by TEXT")
            if not _column_exists(db, 'transcripts', 'created_at'):
                db.execute("ALTER TABLE transcripts ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            if not _column_exists(db, 'transcripts', 'base_sha256'):
                db.execute("ALTER TABLE transcripts ADD COLUMN base_sha256 TEXT")
            if not _column_exists(db, 'transcripts', 'text'):
                db.execute("ALTER TABLE transcripts ADD COLUMN text TEXT")
        # transcript_edits
        if _table_exists(db, 'transcript_edits'):
            if not _column_exists(db, 'transcript_edits', 'dmp_patch'):
                db.execute("ALTER TABLE transcript_edits ADD COLUMN dmp_patch TEXT")
            if not _column_exists(db, 'transcript_edits', 'token_ops'):
                db.execute("ALTER TABLE transcript_edits ADD COLUMN token_ops TEXT")
            if not _column_exists(db, 'transcript_edits', 'created_at'):
                db.execute("ALTER TABLE transcript_edits ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        # transcript_confirmations
        if _table_exists(db, 'transcript_confirmations'):
            if not _column_exists(db, 'transcript_confirmations', 'base_sha256'):
                db.execute("ALTER TABLE transcript_confirmations ADD COLUMN base_sha256 TEXT")
            if not _column_exists(db, 'transcript_confirmations', 'prefix'):
                db.execute("ALTER TABLE transcript_confirmations ADD COLUMN prefix TEXT")
            if not _column_exists(db, 'transcript_confirmations', 'exact'):
                db.execute("ALTER TABLE transcript_confirmations ADD COLUMN exact TEXT")
            if not _column_exists(db, 'transcript_confirmations', 'suffix'):
                db.execute("ALTER TABLE transcript_confirmations ADD COLUMN suffix TEXT")
            if not _column_exists(db, 'transcript_confirmations', 'created_at'):
                db.execute("ALTER TABLE transcript_confirmations ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        # transcript_words
        if _table_exists(db, 'transcript_words'):
            if not _column_exists(db, 'transcript_words', 'segment_index'):
                db.execute("ALTER TABLE transcript_words ADD COLUMN segment_index INTEGER")
            if not _column_exists(db, 'transcript_words', 'start_time'):
                db.execute("ALTER TABLE transcript_words ADD COLUMN start_time DOUBLE")
            if not _column_exists(db, 'transcript_words', 'end_time'):
                db.execute("ALTER TABLE transcript_words ADD COLUMN end_time DOUBLE")
            if not _column_exists(db, 'transcript_words', 'probability'):
                db.execute("ALTER TABLE transcript_words ADD COLUMN probability DOUBLE")
    finally:
        db.commit()

# Default number of segments to return when segment is provided without count
_DEFAULT_SEGMENT_CHUNK = 50


def _db() -> DatabaseService:
    path = current_app.config.get('SQLITE_PATH') or 'explore.sqlite'
    return DatabaseService(path=str(path))


def _sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode('utf-8')).hexdigest()


def _clamp_neighbors(n: int) -> int:
    try:
        n = int(n)
    except Exception:
        n = 1
    if n < 0:
        return 0
    if n > 3:
        return 3
    return n


def _log_info(msg: str, data: Optional[dict] = None) -> None:
    try:
        if data:
            # A bit verbose, but makes logs much more useful
            log_data = {}
            for k, v in data.items():
                if isinstance(v, (list, dict)) and len(str(v)) > 500:
                    log_data[k] = f"{str(v)[:250]}... (truncated) ...{str(v)[-250:]}"

                elif isinstance(v, str) and len(v) > 500:
                    log_data[k] = f"{v[:250]}... (truncated) ...{v[-250:]}"

                else:
                    log_data[k] = v

            logger.info(msg, extra={'data': log_data})
        else:
            logger.info(msg)
    except Exception:
        pass


def _ensure_safe_doc(doc: str) -> None:
    """Abort 400 if `doc` is unsafe (path traversal or absolute paths).

    Allows forward slashes for logical grouping but disallows absolute paths,
    Windows drive prefixes, backslashes, `..` components, nulls, and odd chars.
    """
    d = (doc or "").strip()
    if not d:
        abort(400, 'invalid doc')
    if '\x00' in d:
        abort(400, 'invalid doc')
    # Absolute path patterns
    if d.startswith('/') or d.startswith('\\') or _re.match(r'^[A-Za-z]:[\\/]', d):
        abort(400, 'invalid doc')
    # Parent directory traversal
    parts_slash = [p for p in d.split('/') if p]
    parts_backslash = [p for p in d.split('\\') if p]
    if any(p == '..' for p in parts_slash) or any(p == '..' for p in parts_backslash):
        abort(400, 'invalid doc')


def _maybe_deref_audio_pointer(audio_path: str) -> str:
    """If `audio_path` is a tiny pointer file, try to dereference to blobs/<sha>.
    Requires a strict marker like 'sha:<hex>'. Returns original path if no safe match.
    """
    try:
        if os.path.isfile(audio_path) and os.path.getsize(audio_path) <= 512:
            with open(audio_path, 'rb') as _pf:
                data = _pf.read(512)
            text = ''
            try:
                text = data.decode('utf-8', 'ignore')
            except Exception:
                text = ''
            import re as _re
            m = _re.search(r'\bsha:([a-fA-F0-9]{40,64})\b', text)
            if not m:
                return audio_path
            sha = m.group(1)
            audio_dir = current_app.config.get('AUDIO_DIR')
            if not audio_dir:
                return audio_path
            cand = os.path.join(audio_dir, 'blobs', sha)
            # Only dereference to an existing regular file
            if os.path.isfile(cand):
                return cand
    except Exception:
        pass
    return audio_path


def _ffmpeg_extract_wav_clip(audio_path: str, clip_start: float, clip_end: float, pad: float = 0.10) -> tuple[bytes, float, float]:
    """Extract a mono 16k wav clip using ffmpeg; returns (bytes, ss, to)."""
    ss = max(0.0, float(clip_start) - pad)
    to = float(clip_end) + pad
    cmd = [
        'ffmpeg', '-hide_banner', '-loglevel', 'error',
        '-ss', f'{ss:.3f}', '-to', f'{to:.3f}', '-i', audio_path,
        '-ac', '1', '-ar', '16000', '-f', 'wav', 'pipe:1'
    ]
    _log_info(f"[ALIGN] ffmpeg cmd: {' '.join(cmd)}")
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return p.stdout, ss, to


def _align_call(wav_bytes: bytes, transcript: str) -> dict:
    files = { 'audio': ('clip.wav', wav_bytes, 'audio/wav') }
    data = { 'transcript': transcript }
    # Always call the remote silence-remover endpoint (no local align mode)
    r = requests.post('http://silence-remover.com:8000/align', files=files, data=data, timeout=60)
    if not r.ok:
        raise RuntimeError(f'align-endpoint {r.status_code}: {r.text[:200]}')
    res = r.json() or {}
    try:
        rw = (res or {}).get('words') or []
        smpl = [(str((w or {}).get('word') or ''), (w or {}).get('start'), (w or {}).get('end')) for w in (rw[:10] or [])]
        _log_info(f"[ALIGN] response: words={len(rw)} sample={smpl}")
    except Exception:
        pass
    return res


def _alignment_log_dir() -> str:
    d = current_app.config.get('AUDIO_LOG_DIR') or os.path.join(os.getcwd(), 'audio-log')
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return d


def _safe_name(s: str) -> str:
    try:
        s = str(s or '')
        # Replace path separators and collapse spaces
        s = s.replace(os.sep, '__').replace('/', '__')
        s = ' '.join(s.split())
        # Keep a safe subset; replace others with '_'
        return ''.join(ch if ch.isalnum() or ch in ('_', '-', '.', '#') else '_' for ch in s)
    except Exception:
        return 'unknown'


def _save_alignment_artifacts(kind: str, doc: str, seg: Optional[int], ss: float, to: float, wav_bytes: bytes, response_json: dict, src_audio_path: Optional[str] = None) -> None:
    """Persist the cut audio and raw aligner response for debugging/investigation.

    Files saved under AUDIO_LOG_DIR (or ./audio-log) with a descriptive basename.
    """
    try:
        base_dir = _alignment_log_dir()
        ts = time.strftime('%Y%m%d-%H%M%S')
        uid = str(uuid.uuid4())[:8]
        seg_part = f"seg{int(seg)}" if seg is not None else 'segNA'
        base = f"{kind}_{_safe_name(doc)}_{seg_part}_{ts}_{uid}_{ss:.3f}-{to:.3f}"
        wav_path = os.path.join(base_dir, base + '.wav')
        json_path = os.path.join(base_dir, base + '.response.json')
        # Write WAV
        try:
            with open(wav_path, 'wb') as fh:
                fh.write(wav_bytes or b'')
        except Exception:
            pass
        # Write JSON
        try:
            with open(json_path, 'wb') as fh:
                fh.write(orjson.dumps(response_json))
        except Exception:
            pass
        # Optionally also save a high-quality/native-rate clip (no downmix/resample)
        try:
            if src_audio_path and current_app.config.get('AUDIO_LOG_NATIVE', True):
                native_path = os.path.join(base_dir, base + '.native.wav')
                cmd = [
                    'ffmpeg', '-hide_banner', '-loglevel', 'error',
                    '-ss', f'{ss:.3f}', '-to', f'{to:.3f}', '-i', src_audio_path,
                    '-f', 'wav', '-c:a', 'pcm_s16le', native_path
                ]
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except Exception:
            pass
        _log_info(f"[ALIGN-LOG] saved clip+resp: {wav_path} , {json_path}")
    except Exception:
        pass


def _build_new_window(words: list, start_seg: int, end_seg: int) -> tuple[list[tuple[int, str, int]], str]:
    seg_idx = 0
    new_window = []
    for wi, w in enumerate(words or []):
        try:
            t = str(w.get('word') or '')
        except Exception:
            t = ''
        if t == '\n':
            seg_idx += 1
            continue
        if seg_idx >= start_seg and seg_idx <= end_seg:
            new_window.append((wi, t, seg_idx))
    new_transcript = ' '.join(t for _, t, _ in new_window if t and (not str(t).isspace()))
    return new_window, new_transcript


def _map_aligned_to_updates(new_window: list[tuple[int,str,int]], resp_words: list, offset: float, min_dur: float = 0.20) -> tuple[list[tuple[float,float,int]], int]:
    def _norm(s):
        try:
            return str(s or '').strip()
        except Exception:
            return ''
    # Build comparable sequences (indices + lowercased tokens, excluding empties)
    new_seq = [(i, _norm(t)) for (i, t, _seg) in new_window if _norm(t) != '']
    resp_seq = [((w or {}), _norm((w or {}).get('word'))) for w in (resp_words or []) if _norm((w or {}).get('word')) != '']
    updates: list[tuple[float, float, int]] = []
    matched = 0

    # Fallback: aligner returned a single concatenated token; distribute equally/weighted
    if len(resp_seq) == 1 and len(new_seq) > 1:
        rw = resp_seq[0][0]
        try:
            rs = float(rw.get('start') or 0.0) + offset
        except Exception:
            rs = offset
        try:
            re = float(rw.get('end') or 0.0) + offset
        except Exception:
            re = rs
        if re <= rs:
            re = rs + 0.01
        span = re - rs
        total_chars = sum(max(1, len(t)) for (_wi, t) in new_seq) or len(new_seq)
        cur = rs
        for idx, (wi, t) in enumerate(new_seq):
            if idx == len(new_seq) - 1:
                ns = cur
                ne = re if re > ns else (ns + 0.01)
            else:
                frac = (max(1, len(t)) / total_chars)
                dur = max(0.01, span * frac)
                ns = cur
                ne = ns + dur
                if ne > re:
                    ne = re
            updates.append((float(ns), float(ne), int(wi)))
            matched += 1
            cur = ne
        return updates, matched

    # General case: align using difflib to handle insertions/deletions
    new_tokens = [t for (_wi, t) in new_seq]
    resp_tokens = [t for (_rw, t) in resp_seq]
    sm = difflib.SequenceMatcher(a=new_tokens, b=resp_tokens)
    opcodes = sm.get_opcodes()

    def _resp_time(idx: int) -> tuple[float, float]:
        rw = resp_seq[idx][0]
        try:
            rs = float(rw.get('start') or 0.0) + offset
        except Exception:
            rs = offset
        try:
            re = float(rw.get('end') or 0.0) + offset
        except Exception:
            re = rs
        if not (re > rs):
            # try next resp start as a bound
            next_rs = None
            if (idx + 1) < len(resp_seq):
                try:
                    rn = resp_seq[idx+1][0]
                    next_rs = float(rn.get('start') or 0.0) + offset
                except Exception:
                    next_rs = None
            re = next_rs if (next_rs is not None and next_rs > rs) else (rs + float(min_dur))
        return float(rs), float(re)

    for tag, i1, i2, j1, j2 in opcodes:
        if tag == 'equal':
            # one-to-one mapping
            for k in range(i2 - i1):
                wi = new_seq[i1 + k][0]
                rs, re = _resp_time(j1 + k)
                updates.append((rs, re, int(wi)))
                matched += 1
        elif tag in ('replace', 'delete', 'insert'):
            # best-effort: pair in order up to min length
            cnt = min(i2 - i1, j2 - j1)
            for k in range(cnt):
                wi = new_seq[i1 + k][0]
                rs, re = _resp_time(j1 + k)
                updates.append((rs, re, int(wi)))
                matched += 1
            # any extra new tokens in this block are left unmatched

    return updates, matched


def _compute_clip_from_prev_rows(prev_rows) -> tuple[Optional[float], Optional[float]]:
    clip_start = None; clip_end = None
    for _seg_i, _wi, _w, st, en, _pr in (prev_rows or []):
        if st is not None:
            clip_start = st if clip_start is None else min(clip_start, float(st))
        if en is not None:
            clip_end = en if clip_end is None else max(clip_end, float(en))
    if clip_start is None or clip_end is None or clip_end <= clip_start:
        return None, None
    return float(clip_start), float(clip_end)


def _compose_text_from_words(words: list) -> str:
    out = []
    for w in (words or []):
        try:
            t = str((w or {}).get('word') or '')
        except Exception:
            t = ''
        if t == '\n':
            continue
        out.append(t)
    return ''.join(out)


def _compose_full_text_from_words(words: list) -> str:
    """Compose full text from words including explicit '\n' tokens.

    This preserves segment boundaries in the resulting text. Each token's
    'word' is concatenated verbatim.
    """
    out = []
    for w in (words or []):
        try:
            t = str((w or {}).get('word') or '')
        except Exception:
            t = ''
        out.append(t)
    return ''.join(out)


def _canonicalize_text(s: str) -> str:
    """Canonicalize text similarly to the client canonicalizeText().

    - Normalize CRLF -> LF
    - Replace NBSP with regular space
    - Strip bidi/invisible formatting chars
    - Trim trailing spaces on each line
    """
    try:
        t = str(s or '')
        t = t.replace('\r', '')
        t = t.replace('\u00A0', ' ')
        t = _re.sub(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]", "", t)
        # Trim trailing spaces/tabs per line
        t = _re.sub(r"[ \t]+$", "", t, flags=_re.MULTILINE)
        return t
    except Exception:
        return str(s or '')


def _tokenize_text_to_words(text: str) -> list:
    """Tokenize text into words while preserving whitespace runs and newlines.

    - Emits non-whitespace runs verbatim as tokens
    - Emits whitespace runs (spaces/tabs etc.) as separate tokens to preserve spacing
    - Emits a {'word': '\n'} token between lines to preserve segment boundaries
    - No timings/probabilities are set here
    """
    _log_info(f"Tokenizing text", {"text": text})
    words = []
    # Ensure we handle None or empty string gracefully
    lines = (text or '').splitlines()
    if not lines:
        _log_info("Tokenize: input text is empty or None.")
    
    for i, line in enumerate(lines):
        buf = ''
        is_space = None
        for ch in line:
            ch_is_space = ch.isspace() and ch != '\n'
            if is_space is None:
                buf = ch
                is_space = ch_is_space
            elif ch_is_space == is_space:
                buf += ch
            else:
                if buf:
                    words.append({'word': buf})
                buf = ch
                is_space = ch_is_space
        if buf:
            words.append({'word': buf})
        
        # Add segment separator, but not for the very last line if text ends without newline
        if i < len(lines) - 1:
            words.append({'word': '\n'})

    # If the original text ended with a newline, the last word will be '\n'.
    # splitlines() might not capture a final trailing newline, so let's check.
    if text and text.endswith('\n') and (not words or words[-1].get('word') != '\n'):
         words.append({'word': '\n'})

    _log_info(f"Tokenized into {len(words)} words.", {"word_count": len(words), "words": words})
    return words


def _carry_over_timings(old_words: list, new_words: list) -> list:
    """
    Carries over timings and probabilities from an old list of word tokens to a new one.

    This function is useful when text has been edited and re-tokenized, and we want to
    preserve the metadata (start/end times, probabilities) of words that haven't changed.

    It uses `difflib.SequenceMatcher` to find matching sequences of word tokens between the
    old and new lists. For matching tokens, it copies the 'start', 'end', and 'probability'
    attributes from the old token to the new one.

    Args:
        old_words: A list of word dictionaries, which may contain timing and probability info.
        new_words: A list of word dictionaries, typically without timing info.

    Returns:
        A new list of word dictionaries, where timings and probabilities from `old_words`
        have been copied to matching words in `new_words`.
    """
    if not old_words or not new_words:
        _log_info("Carry over timings: one or both lists are empty. Returning new_words as is.", {
            "old_words_count": len(old_words) if old_words else 0,
            "new_words_count": len(new_words) if new_words else 0
        })
        return new_words

    _log_info(f"Carry over timings: old_words={len(old_words)}, new_words={len(new_words)}", {
        "old_words_count": len(old_words),
        "new_words_count": len(new_words),
        "old_words_sample": old_words[:20],
        "new_words_sample": new_words[:20]
    })

    old_tokens = [w.get('word', '') for w in old_words]
    new_tokens = [w.get('word', '') for w in new_words]

    matcher = difflib.SequenceMatcher(a=old_tokens, b=new_tokens, autojunk=False)

    enriched_words = [dict(w) for w in new_words]  # Make a mutable copy

    matches = 0
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            for i in range(i2 - i1):
                old_idx = i1 + i
                new_idx = j1 + i
                
                old_word_data = old_words[old_idx]
                
                # Carry over start, end, and probability if they exist in the old word
                if 'start' in old_word_data and old_word_data['start'] is not None:
                    enriched_words[new_idx]['start'] = old_word_data['start']
                if 'end' in old_word_data and old_word_data['end'] is not None:
                    enriched_words[new_idx]['end'] = old_word_data['end']
                if 'probability' in old_word_data and old_word_data['probability'] is not None:
                    enriched_words[new_idx]['probability'] = old_word_data['probability']
                matches += 1
    
    _log_info(f"Carry over timings: found {matches} matches.", {
        "matches": matches,
        "enriched_words_sample": enriched_words[:20]
    })
    return enriched_words

# --- End of primary helpers, start of duplicated function removal ---
