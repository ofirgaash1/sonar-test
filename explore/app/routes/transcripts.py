from __future__ import annotations

import hashlib
import difflib
import os
import logging
import time
import uuid
import re as _re
from typing import Optional

import orjson
from flask import Blueprint, current_app, jsonify, request, abort, session
import subprocess
import requests

from ..services.db import DatabaseService

# --- Lightweight, idempotent schema migrations ---
_TARGET_SCHEMA_VERSION = 3
# Default number of segments to return when segment is provided without count
_DEFAULT_SEGMENT_CHUNK = 50


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

bp = Blueprint("transcripts", __name__, url_prefix="/transcripts")
logger = logging.getLogger(__name__)


def _db() -> DatabaseService:
    path = current_app.config.get('SQLITE_PATH') or 'explore.sqlite'
    return DatabaseService(path=str(path))


def _ensure_schema(db: DatabaseService):
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


def _sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode('utf-8')).hexdigest()


# -----------------------------
# Small utility/helper routines
# -----------------------------

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


def _log_info(msg: str) -> None:
    try:
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
    r = requests.post(current_app.config.get('ALIGN_ENDPOINT', 'http://silence-remover.com:8000/align'), files=files, data=data, timeout=60)
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
    words = []
    for line in (text or '').splitlines():
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
        # segment separator
        words.append({'word': '\n'})
    return words


def _explode_resp_words_if_needed(resp_words: list) -> list:
    """If the aligner returns a single concatenated token, split into word tokens.

    Distributes the time range linearly by character length across non-space pieces.
    """
    try:
        if not resp_words:
            return resp_words
        # If already multiple tokens with reasonable lengths, keep as-is
        if len(resp_words) > 1:
            return resp_words
        rw = resp_words[0] or {}
        t = str(rw.get('word') or '')
        s = rw.get('start', None); e = rw.get('end', None)
        if not t or s is None or e is None:
            return resp_words
        # If contains internal whitespace, split
        pieces = [p for p in _re.split(r"\s+", t) if p]
        if len(pieces) <= 1:
            return resp_words
        total_chars = sum(len(p) for p in pieces) or 1
        out = []
        cur = float(s)
        span = max(0.0, float(e) - float(s))
        for i, p in enumerate(pieces):
            frac = (len(p) / total_chars)
            dur = span * frac
            ns = cur
            ne = cur + dur
            if ne <= ns:
                ne = ns + 0.01
            out.append({'word': p, 'start': ns, 'end': ne})
            cur = ne
        return out
    except Exception:
        return resp_words


def _ensure_words_match_text(text: str, words: list) -> list:
    """Ensure the words tokens reflect the current text content without discarding timings.

    Behavior changes:
    - If any incoming token carries timing/probability metadata, trust the provided words as-is
      (skip retokenization) to avoid losing alignment on first save.
    - Otherwise, compare text vs. composed words using a whitespace-tolerant canonical form
      (collapsing runs, trimming edges, removing bidi/NBSP/newlines). Only retokenize when the
      canonical contents differ.
    """
    def _canon_relaxed(s: str) -> str:
        try:
            t = str(s or '')
            # Normalize CRLF -> LF
            t = t.replace('\r', '')
            # Replace NBSP with regular space
            t = t.replace('\u00A0', ' ')
            # Strip bidi/invisible formatting chars (LRM/RLM/embeddings/isolates)
            t = _re.sub(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]", "", t)
            # Remove explicit newlines for comparison
            t = t.replace('\n', ' ')
            # Collapse all whitespace runs to a single space
            t = _re.sub(r"\s+", " ", t)
            # Trim leading/trailing spaces
            t = t.strip()
            # Unicode normalization to NFC to avoid canonically equivalent mismatches
            try:
                t = t.normalize('NFC') if hasattr(t, 'normalize') else t
            except Exception:
                pass
            return t
        except Exception:
            return str(s or '')

    # 1) If words already include any timing/probability, keep them intact to preserve metadata
    try:
        for w in (words or []):
            if not isinstance(w, dict):
                continue
            if (w.get('start') is not None) or (w.get('end') is not None) or (w.get('probability') is not None):
                return words
    except Exception:
        # If inspection fails, fall back to relaxed comparison path below
        pass

    # 2) Whitespace-tolerant comparison when no timings exist
    try:
        ws = _compose_text_from_words(words or [])
        if _canon_relaxed(ws) == _canon_relaxed(text):
            return words
    except Exception:
        pass
    # 3) Retokenize only when content differs materially
    return _tokenize_text_to_words(text)


def _validate_and_sanitize_words(words: list) -> list:
    """Validate that words is a list of dict items with expected fields.

    Ensures each item is a dict, has a string 'word', and optional numeric
    'start', 'end', 'probability' (or None). Returns a sanitized shallow copy.
    Aborts with 400 if structure is invalid.
    """
    if not isinstance(words, list):
        abort(400, 'words must be an array')
    out = []
    for i, w in enumerate(words):
        if not isinstance(w, dict):
            abort(400, f'words[{i}] must be an object')
        word_val = w.get('word')
        try:
            word_str = str(word_val or '')
        except Exception:
            abort(400, f'words[{i}].word must be string')
        # Allow newline tokens
        s = w.get('start', None)
        e = w.get('end', None)
        p = w.get('probability', None)
        # Coerce numeric-like values; allow None
        def _to_float_or_none(v):
            if v is None or v == '':
                return None
            try:
                val = float(v)
                # Treat NaN/inf as None
                if val != val or val == float('inf') or val == float('-inf'):
                    return None
                # Disallow negatives; clamp to 0.0
                if val < 0:
                    return 0.0
                return val
            except Exception:
                abort(400, f'words[{i}] timing/probability must be number or null')
        s_f = _to_float_or_none(s)
        e_f = _to_float_or_none(e)
        p_f = _to_float_or_none(p)
        # If both present and end < start, drop end to let normalizer fix
        if s_f is not None and e_f is not None and e_f < s_f:
            e_f = None
        out.append({'word': word_str, 'start': s_f, 'end': e_f, 'probability': p_f})
    return out

def _segment_window(seg_hint, neighbors: int) -> tuple[int, int]:
    seg = int(seg_hint)
    n = _clamp_neighbors(neighbors)
    start_seg = max(0, seg - n)
    end_seg = seg + n
    return start_seg, end_seg


def _fetch_prev_rows_for_window(db: DatabaseService, doc: str, version: int, start_seg: int, end_seg: int):
    cur = db.execute(
        """
        SELECT segment_index, word_index, word, start_time, end_time, probability
        FROM transcript_words
        WHERE file_path=? AND version=? AND segment_index >= ? AND segment_index <= ?
        ORDER BY word_index ASC
        """,
        [doc, int(version), start_seg, end_seg]
    )
    return cur.fetchall() or []


def _check_save_conflict(db: DatabaseService, doc: str, latest: Optional[dict], parent_version, expected_base_sha256: str, text: str):
    """Return a conflict payload (dict) if the save should be rejected, else None.

    This centralizes the branching logic used to gate concurrent saves.
    """
    # Canonicalize client text once for any diffs we might produce
    client_text_canon = _canonicalize_text(text or '')

    if not latest:
        # First version can proceed if parent is None/0.
        if parent_version not in (None, 0, '0'):
            return {"reason": "invalid_parent_for_first"}
        return None

    if parent_version is None:
        return {"reason": "missing_parent", "latest": latest}

    # When the parent is specified, the client should send expected_base_sha256
    if not expected_base_sha256:
        base = _row_for_version(db, doc, int(parent_version)) or {"text": ""}
        base_text = _canonicalize_text(base.get('text') or '')
        latest_text = _canonicalize_text((latest or {}).get('text') or '')
        return {
            "reason": "hash_missing",
            "latest": latest,
            "parent": {
                "version": int(parent_version),
                "base_sha256": _sha256_hex(base_text),
                "text": base.get('text') or ''
            },
            "diff_parent_to_latest": _diff(base_text, latest_text),
            "diff_parent_to_client": _diff(base_text, client_text_canon)
        }

    if int(parent_version) != int(latest['version']):
        base = _row_for_version(db, doc, int(parent_version)) or {"text": ""}
        base_text = _canonicalize_text(base.get('text') or '')
        latest_text = _canonicalize_text((latest or {}).get('text') or '')
        return {
            "reason": "version_conflict",
            "latest": latest,
            "parent": {
                "version": int(parent_version),
                "base_sha256": _sha256_hex(base_text),
                "text": base.get('text') or ''
            },
            "diff_parent_to_latest": _diff(base_text, latest_text),
            "diff_parent_to_client": _diff(base_text, client_text_canon)
        }

    if expected_base_sha256 and expected_base_sha256 != str(latest['base_sha256']):
        base = _row_for_version(db, doc, int(parent_version)) or {"text": ""}
        base_text = _canonicalize_text(base.get('text') or '')
        latest_text = _canonicalize_text((latest or {}).get('text') or '')
        return {
            "reason": "hash_conflict",
            "latest": latest,
            "parent": {
                "version": int(parent_version),
                "base_sha256": _sha256_hex(base_text),
                "text": base.get('text') or ''
            },
            "diff_parent_to_latest": _diff(base_text, latest_text),
            "diff_parent_to_client": _diff(base_text, client_text_canon)
        }
    return None


def _prealign_updates(db: DatabaseService, doc: str, latest: Optional[dict], words: list, seg_hint, neighbors: int):
    """Compute alignment updates for a window around seg_hint using the external aligner.

    Returns a tuple: (updates, token_ops_block). On any failure, returns ([], None).
    """
    updates = []
    token_ops_block = None
    if not (latest and seg_hint is not None):
        return updates, token_ops_block

    try:
        start_seg, end_seg = _segment_window(seg_hint, neighbors)
        # Gather previous timings to determine clip
        prev_rows = _fetch_prev_rows_for_window(db, doc, int(latest['version']), start_seg, end_seg)
        clip_start, clip_end = _compute_clip_from_prev_rows(prev_rows)
        if clip_start is None or clip_end is None:
            raise RuntimeError('prealign-skip:no-timings')

        # Build new window transcript and mapping of word indices
        new_window, new_transcript = _build_new_window(words, start_seg, end_seg)
        if not new_transcript:
            raise RuntimeError('prealign-skip:empty-window')

        # Resolve audio (with pointer deref safety)
        from ..utils import resolve_audio_path
        audio_path = resolve_audio_path(doc)
        if not audio_path:
            raise RuntimeError('prealign-skip:audio-not-found')
        audio_path = _maybe_deref_audio_pointer(audio_path)

        # Extract WAV clip via ffmpeg and call endpoint
        wav_bytes, ss, to = _ffmpeg_extract_wav_clip(audio_path, clip_start, clip_end, pad=0.10)
        align_res = _align_call(wav_bytes, new_transcript)
        resp_words = (align_res or {}).get('words') or []
        resp_words = _explode_resp_words_if_needed(resp_words)
        try:
            _save_alignment_artifacts('prealign', doc, int(seg_hint), ss, to, wav_bytes, align_res, src_audio_path=audio_path)
        except Exception:
            pass

        # Map response words to updates; ensure non-zero durations
        updates, matched = _map_aligned_to_updates(new_window, resp_words, ss, min_dur=0.20)
        _log_info(f"[ALIGN] prealign mapping: new_seq={len(new_window)} resp_seq={len(resp_words)} matched={matched} updates={len(updates)}")
        if matched == 0:
            raise RuntimeError('prealign-skip:no-match')

        token_ops_block = {
            'type': 'timing_adjust',
            'segment_start': start_seg,
            'segment_end': end_seg,
            'clip_start': ss,
            'clip_end': to,
            'items': [ { 'word_index': wi, 'new_start': s, 'new_end': e } for (s,e,wi) in updates ],
            'service': 'silence-remover',
        }
    except Exception as e:
        _log_info(f"[ALIGN] prealign skipped: {str(e)}")
        updates = []
        token_ops_block = None
    return updates, token_ops_block


def _carry_over_timings(db: DatabaseService, doc: str, latest: Optional[dict], words: list) -> tuple[list, str]:
    """Carry over timings/probabilities from previous version for unchanged tokens.

    Returns (enriched_words, words_json). If enrichment fails, returns the
    original words and its JSON serialization.
    """
    try:
        if latest and isinstance(words, list) and words:
            cur = db.execute(
                """
                SELECT word_index, word, start_time, end_time, probability
                FROM transcript_words
                WHERE file_path=? AND version=?
                ORDER BY word_index ASC
                """,
                [doc, int(latest['version'])]
            )
            prev_rows = cur.fetchall() or []
            prev_seq = []
            for wi, w, st, en, pr in prev_rows:
                try:
                    prev_seq.append({ 'word': str(w or ''), 'start': st, 'end': en, 'prob': pr })
                except Exception:
                    prev_seq.append({ 'word': str(w or ''), 'start': None, 'end': None, 'prob': None })

            def _enrich_with_prev_seq(prev_seq_local, words_local):
                LOOKAHEAD = 64
                pi = 0
                enriched_local = []
                for w in (words_local or []):
                    try:
                        t = str(w.get('word') or '')
                    except Exception:
                        t = ''
                    if t == '\n':
                        enriched_local.append({ 'word': '\n' })
                        continue
                    s_raw = w.get('start', None)
                    e_raw = w.get('end', None)
                    p_raw = w.get('probability', None)

                    def _num(x):
                        try:
                            return float(x)
                        except Exception:
                            return None

                    s_num = _num(s_raw)
                    e_num = _num(e_raw)
                    timings_present = ((e_num is not None and e_num > 0) or (s_num is not None and s_num > 0))
                    try:
                        p_num = float(p_raw)
                        prob_present = (p_raw is not None) and (p_num == p_num)  # not NaN
                    except Exception:
                        p_num = None
                        prob_present = False

                    s_val = s_raw
                    e_val = e_raw
                    p_val = p_raw
                    if not (timings_present and prob_present):
                        match_j = -1
                        limit = min(len(prev_seq_local), pi + LOOKAHEAD)
                        for j in range(pi, limit):
                            if str(prev_seq_local[j]['word'] or '') == t:
                                match_j = j
                                break
                        if match_j >= 0:
                            pi = match_j + 1
                            if not timings_present:
                                s_val = prev_seq_local[match_j]['start']
                                e_val = prev_seq_local[match_j]['end']
                            if not prob_present:
                                p_val = prev_seq_local[match_j]['prob']
                    enriched_local.append({ 'word': t, **({ 'start': s_val } if s_val is not None else {}), **({ 'end': e_val } if e_val is not None else {}), **({ 'probability': p_val } if p_val is not None else {}) })
                return enriched_local

            words = _enrich_with_prev_seq(prev_seq, words)
    except Exception:
        # On failure, fall through with the original words
        pass

    try:
        words_json = orjson.dumps(words).decode('utf-8')
    except Exception:
        # Best-effort serialization
        words_json = orjson.dumps(words or []).decode('utf-8')
    return words, words_json


def _build_segment_filter(seg_q: str, count_q: str):
    """Return (seg_filter_sql, params_extra, window) for segment filtering.

    window is either None or a tuple (seg, end_seg) for slicing fallback JSON.
    """
    seg_filter_sql = ''
    params_extra = []
    window = None
    if seg_q.isdigit():
        seg = int(seg_q)
        if count_q.isdigit():
            end_seg = seg + max(0, int(count_q)) - 1
        else:
            end_seg = seg + _DEFAULT_SEGMENT_CHUNK - 1
        seg_filter_sql = ' AND segment_index >= ? AND segment_index <= ?'
        params_extra = [seg, end_seg]
        window = (seg, end_seg)
    return seg_filter_sql, params_extra, window


def _normalize_db_words_rows(rows):
    """Normalize rows from transcript_words into the words JSON shape.

    Ensures per-segment non-zero durations and inserts newline tokens between segments.
    Returns (out, with_timing_count).
    """
    MIN_DUR = 0.20
    out = []
    with_timing = 0
    cur_seg = None
    buf = []  # collect tokens for current segment

    def flush_segment(segment_tokens):
        nonlocal out, with_timing
        n = len(segment_tokens)
        # First pass: ensure numeric values
        for t in segment_tokens:
            try:
                if t.get('start') is None:
                    t['start'] = 0.0
                else:
                    t['start'] = float(t.get('start') or 0.0)
            except Exception:
                t['start'] = 0.0
            try:
                if t.get('end') is None:
                    t['end'] = float(t.get('start') or 0.0)
                else:
                    t['end'] = float(t.get('end') or 0.0)
            except Exception:
                t['end'] = float(t.get('start') or 0.0)
        # Second pass: lookahead normalization
        for i in range(n):
            s = float(segment_tokens[i].get('start') or 0.0)
            e = float(segment_tokens[i].get('end') or 0.0)
            if not (e > s):
                # Try next start within segment
                next_s = None
                for j in range(i+1, n):
                    ns = float(segment_tokens[j].get('start') or 0.0)
                    if ns > s:
                        next_s = ns
                        break
                if next_s is not None:
                    e = next_s
                else:
                    e = s + MIN_DUR
                segment_tokens[i]['end'] = e
            # accumulate timing count
            if (s > 0) or (e > 0):
                with_timing += 1
        out.extend(segment_tokens)

    for seg, wi, word, st, en, pr in rows:
        if (cur_seg is not None) and (seg != cur_seg):
            # newline separator between segments
            if buf:
                flush_segment(buf)
                buf = []
            # Use the last known time from previous segment for the newline token
            try:
                prev_end = out[-1]['end'] if out else 0.0
            except Exception:
                prev_end = 0.0
            out.append({"word": "\n", "start": prev_end, "end": prev_end, "probability": None})
        # push current token into segment buffer
        buf.append({
            "word": word,
            "start": st if st is not None else 0.0,
            "end": en if en is not None else None,
            "probability": float(pr) if pr is not None else None,
        })
        cur_seg = seg
    # flush last segment
    if buf:
        flush_segment(buf)

    return out, with_timing


def _slice_words_json(words: list, seg: int, end_seg: int) -> list:
    """Slice stored JSON words by segment boundaries keeping newline tokens.

    Mirrors the previous inline logic for segment slicing.
    """
    out = []
    cur_seg = 0
    started = False
    for w in words:
        if not w:
            continue
        word_val = w.get('word') if isinstance(w, dict) else None
        if word_val == '\n':
            # If we've started collecting and reached the end segment, stop
            if started and cur_seg >= end_seg:
                break
            # Advance segment counter, and if still within the requested window, include a newline token
            cur_seg += 1
            if started and cur_seg <= end_seg:
                out.append({"word": "\n", "start": w.get('start') or 0.0, "end": w.get('start') or 0.0, "probability": None})
            continue
        # Skip words before the first requested segment
        if cur_seg < seg:
            continue
        started = True
        # Normalize fields and defaults
        out.append({
            "word": str(w.get('word') or ''),
            "start": float(w.get('start') or 0.0),
            "end": float(w.get('end') if w.get('end') is not None else (w.get('start') or 0.0)),
            "probability": (float(w.get('probability')) if (w.get('probability') is not None and w.get('probability') != '') else None),
        })
    return out


def _normalize_words_json_all(words: list) -> list:
    """Normalize full JSON `words` list to the same shape as DB path.

    - Preserves pure whitespace tokens and '\n' separators
    - Ensures numeric start/end defaults
    - Inserts newline tokens with zero-duration using previous end time when present
    """
    out = []
    prev_end = 0.0
    for w in (words or []):
        if not w:
            continue
        try:
            word_val = str((w or {}).get('word') or '')
        except Exception:
            word_val = ''
        if word_val == '\n':
            out.append({"word": "\n", "start": prev_end, "end": prev_end, "probability": None})
            continue
        # Preserve whitespace runs as tokens
        try:
            s = float(w.get('start') or 0.0)
        except Exception:
            s = 0.0
        try:
            e = float(w.get('end') if w.get('end') is not None else s)
        except Exception:
            e = s
        try:
            p = (float(w.get('probability')) if (w.get('probability') is not None and w.get('probability') != '') else None)
        except Exception:
            p = None
        out.append({"word": word_val, "start": s, "end": e, "probability": p})
        prev_end = e
    return out

def _latest_row(db: DatabaseService, file_path: str) -> Optional[dict]:
    cur = db.execute(
        "SELECT version, base_sha256, text, words, COALESCE(created_by,'') FROM transcripts WHERE file_path=? ORDER BY version DESC LIMIT 1",
        [file_path],
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "version": row[0],
        "base_sha256": row[1],
        "text": row[2],
        "words": orjson.loads(row[3]) if row[3] else [],
        "created_by": row[4] or "",
    }


def _row_for_version(db: DatabaseService, file_path: str, version: int) -> Optional[dict]:
    cur = db.execute(
        "SELECT version, base_sha256, text, words, COALESCE(created_by,'') FROM transcripts WHERE file_path=? AND version=?",
        [file_path, int(version)],
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "version": row[0],
        "base_sha256": row[1],
        "text": row[2],
        "words": orjson.loads(row[3]) if row[3] else [],
        "created_by": row[4] or "",
    }


def _diff(a: str, b: str) -> str:
    # Char-based unified diff for storage; compact but deterministic
    diff = difflib.unified_diff(a.splitlines(keepends=True), b.splitlines(keepends=True), n=0)
    return ''.join(diff)


def _populate_transcript_words(db: DatabaseService, doc: str, version: int, words: list):
    # Remove any existing rows for this version (shouldn't exist on new insert, but safe)
    db.execute("DELETE FROM transcript_words WHERE file_path=? AND version=?", [doc, int(version)])
    seg_idx = 0
    wrows = []
    for wi, w in enumerate(words or []):
        try:
            word = str(w.get('word', ''))
        except AttributeError:
            word = ''
        if word == '\n':
            seg_idx += 1
            continue
        # Preserve pure whitespace tokens as first-class rows to avoid collapsing spacing on reload
        start = w.get('start', None)
        end = w.get('end', None)
        prob = w.get('probability', None)
        wrows.append((doc, int(version), seg_idx, wi, word, start, end, prob))
    if wrows:
        db.batch_execute(
            """
            INSERT INTO transcript_words
              (file_path, version, segment_index, word_index, word, start_time, end_time, probability)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            wrows,
        )


def _normalize_end_times(db: DatabaseService, doc: str, version: int, min_dur: float = 0.20) -> int:
    """Ensure each token in transcript_words for (doc,version) has end_time > start_time.
    Uses next token's start within each segment when available; otherwise start + min_dur.
    Returns number of rows updated.
    """
    cur = db.execute(
        """
        SELECT segment_index, word_index, start_time, end_time
        FROM transcript_words
        WHERE file_path=? AND version=?
        ORDER BY segment_index ASC, word_index ASC
        """,
        [doc, int(version)]
    )
    rows = cur.fetchall() or []
    updated = []  # (start_time, end_time, doc, version, word_index)
    # Group by segment
    seg_map: dict[int, list[tuple[int, Optional[float], Optional[float]]]] = {}
    for seg, wi, st, en in rows:
        seg_map.setdefault(int(seg), []).append((int(wi), (float(st) if st is not None else None), (float(en) if en is not None else None)))
    for seg, items in seg_map.items():
        n = len(items)
        prev_end: Optional[float] = None
        # Build lookahead starts for efficiency
        starts = [items[i][1] for i in range(n)]
        for i in range(n):
            wi, s_raw, e_raw = items[i]
            # Preserve tokens with no timing info at all
            if s_raw is None and e_raw is None:
                continue
            # derive start: prefer given; else previous end; else 0.0
            s = s_raw if s_raw is not None else (prev_end if prev_end is not None else 0.0)
            # find next known start strictly greater than s for lookahead
            next_start = None
            for j in range(i+1, n):
                ns = starts[j]
                if ns is not None and ns > s:
                    next_start = ns
                    break
            # derive end: use given if valid; else next_start; else s + min_dur
            if e_raw is not None and e_raw > s:
                e = e_raw
            else:
                e = next_start if (next_start is not None) else (s + float(min_dur))
            # track prev_end for subsequent missing-start tokens
            prev_end = e
            # decide if update needed
            need_update = (s_raw is None) or (e_raw is None) or (e_raw <= (s_raw if s_raw is not None else s)) or (e != e_raw) or (s_raw is None)
            if need_update:
                updated.append((float(s), float(e), doc, int(version), int(wi)))
    if updated:
        db.batch_execute(
            "UPDATE transcript_words SET start_time=?, end_time=? WHERE file_path=? AND version=? AND word_index=?",
            updated
        )
    return len(updated)

@bp.route('/latest', methods=['GET'])
def get_latest():
    doc = request.args.get('doc', '').strip()
    if not doc:
        abort(400, 'missing ?doc=')
    _ensure_safe_doc(doc)
    db = _db(); _ensure_schema(db)
    row = _latest_row(db, doc)
    return jsonify(row or {})


@bp.route('/get', methods=['GET'])
def get_version():
    doc = request.args.get('doc', '').strip()
    version = request.args.get('version', '').strip()
    if not doc or not version.isdigit():
        abort(400, 'missing ?doc= and/or ?version=')
    _ensure_safe_doc(doc)
    db = _db(); _ensure_schema(db)
    row = _row_for_version(db, doc, int(version))
    if not row:
        abort(404, 'version not found')
    return jsonify(row)


@bp.route('/save', methods=['POST'])
def save_version():
    body = request.get_json(force=True, silent=False) or {}
    doc = (body.get('doc') or '').strip()
    parent_version = body.get('parentVersion', None)
    expected_base_sha256 = (body.get('expected_base_sha256') or '').strip()
    text = str(body.get('text') or '')
    words = body.get('words', [])
    # Track whether client explicitly provided an empty words array.
    # Tests rely on align_segment fallback when there are no transcript_words rows for a version.
    client_words_were_empty = isinstance(words, list) and len(words) == 0
    seg_hint = body.get('segment', None)
    neighbors = _clamp_neighbors(body.get('neighbors', 1) or 1)
    if not doc:
        abort(400, 'missing doc')
    _ensure_safe_doc(doc)
    if not isinstance(words, list):
        abort(400, 'words must be an array')
    _ensure_safe_doc(doc)
    # Validate input words structure early
    words = _validate_and_sanitize_words(words)

    db = _db(); _ensure_schema(db)
    latest = _latest_row(db, doc)

    # Concurrency/consistency gate
    conflict_payload = _check_save_conflict(db, doc, latest, parent_version, expected_base_sha256, text)
    if conflict_payload is not None:
        if conflict_payload.get('reason') == 'invalid_parent_for_first':
            return ("invalid parentVersion for first save", 400)
        return (jsonify(conflict_payload), 409)

    # Compute new version (hash computed later from recomposed text)
    new_version = (latest['version'] + 1) if latest else 1

    # Ensure client words reflect the edited text; if not, retokenize from text
    words = _ensure_words_match_text(text, words)
    # Re-sanitize after potential retokenization
    words = _validate_and_sanitize_words(words)
    # Serialize words for storage (will update after carry-over enrichment)
    try:
        words_json = orjson.dumps(words).decode('utf-8')
    except Exception:
        words_json = orjson.dumps(words or []).decode('utf-8')
    try:
        _wt = 0
        for _w in (words or []):
            try:
                s = float(_w.get('start')) if _w.get('start') is not None else 0.0
                e = float(_w.get('end')) if _w.get('end') is not None else 0.0
                if s > 0 or e > 0: _wt += 1
            except Exception:
                pass
        logger.info(f"[SAVE] incoming words: count={len(words or [])} with_timing={_wt} latest_ver={(latest or {}).get('version', 0)} seg_hint={seg_hint}")
    except Exception:
        pass

    if current_app.config.get('ALIGN_PREALIGN_ON_SAVE', True):
        updates, token_ops_block = _prealign_updates(db, doc, latest, words, seg_hint, neighbors)
    else:
        updates, token_ops_block = [], None
    words, words_json = _carry_over_timings(db, doc, latest, words)

    # Recompose authoritative text from words and canonicalize for storage + hashing
    try:
        recomposed_text = _compose_full_text_from_words(words)
        store_text = _canonicalize_text(recomposed_text)
    except Exception:
        # Fallback to client-provided text (canonicalized)
        store_text = _canonicalize_text(text)
    # Compute child hash from canonicalized text
    new_hash = _sha256_hex(store_text)

    # Begin transaction
    db.execute("BEGIN TRANSACTION")
    try:
        user_email = session.get('user_email', '')
        db.execute(
            "INSERT INTO transcripts (file_path, version, base_sha256, text, words, created_by) VALUES (?, ?, ?, ?, ?, ?)",
            [doc, new_version, new_hash, store_text, words_json, user_email]
        )
        # Populate normalized words rows for this version unless client explicitly sent an empty list.
        # This enables align_segment to return ok:false (no-words) for versions without normalized rows.
        if not client_words_were_empty:
            _populate_transcript_words(db, doc, new_version, words)
        # Apply alignment updates only when we have normalized rows
        if updates and not client_words_were_empty:
            db.batch_execute(
                "UPDATE transcript_words SET start_time=?, end_time=? WHERE file_path=? AND version=? AND word_index=?",
                [ (s, e, doc, int(new_version), wi) for (s,e,wi) in updates ]
            )
        # Normalize end_time to ensure non-zero durations per token
        try:
            norm_count = 0 if client_words_were_empty else _normalize_end_times(db, doc, int(new_version), min_dur=0.20)
        except Exception:
            norm_count = 0

        # Store edit deltas relative to parent (if exists)
        if latest:
            d_parent = _diff(latest['text'] or '', store_text)
            db.execute(
                "INSERT OR REPLACE INTO transcript_edits (file_path, parent_version, child_version, dmp_patch, token_ops) VALUES (?, ?, ?, ?, ?)",
                [doc, int(latest['version']), new_version, d_parent, orjson.dumps(token_ops_block).decode('utf-8') if token_ops_block else None]
            )

        # Also store delta relative to origin (v1) for fast replay
        if latest and latest['version'] >= 1:
            v1 = _row_for_version(db, doc, 1)
            if v1:
                d_origin = _diff(v1['text'] or '', store_text)
                db.execute(
                    "INSERT OR REPLACE INTO transcript_edits (file_path, parent_version, child_version, dmp_patch, token_ops) VALUES (?, ?, ?, ?, ?)",
                    [doc, 1, new_version, d_origin, None]
                )

        db.commit()
        try:
            # Debug: how many tokens have timings after save
            cur = db.execute(
                "SELECT COUNT(*), SUM(CASE WHEN start_time IS NOT NULL OR end_time IS NOT NULL THEN 1 ELSE 0 END) FROM transcript_words WHERE file_path=? AND version=?",
                [doc, int(new_version)]
            )
            row = cur.fetchone() or [0, 0]
            logger.info(f"[SAVE] persisted tokens: total={int(row[0] or 0)} with_timings={int(row[1] or 0)} normalized={int(norm_count or 0)}")
        except Exception:
            pass
    except Exception:
        db.execute("ROLLBACK")
        raise

    return jsonify({ "version": new_version, "base_sha256": new_hash })


@bp.route('/edits', methods=['GET'])
def list_edits():
    doc = request.args.get('doc', '').strip()
    if not doc:
        abort(400, 'missing ?doc=')
    db = _db(); _ensure_schema(db)
    cur = db.execute(
        """
        SELECT parent_version, child_version, dmp_patch, token_ops
        FROM transcript_edits
        WHERE file_path=?
        ORDER BY child_version ASC
        """,
        [doc]
    )
    rows = cur.fetchall() or []
    out = [
        {"parent_version": r[0], "child_version": r[1], "dmp_patch": r[2], "token_ops": r[3]} for r in rows
    ]
    return jsonify(out)


@bp.route('/align_segment', methods=['POST'])
def align_segment():
    _t0 = time.time()
    body = request.get_json(force=True, silent=False) or {}
    doc = (body.get('doc') or '').strip()
    version = body.get('version', None)
    seg = body.get('segment', None)
    # Neighbor window policy: clamp to [0, 3]
    try:
        neighbors = int(body.get('neighbors', 0) or 0)
    except Exception:
        neighbors = 0
    if neighbors < 0: neighbors = 0
    if neighbors > 3: neighbors = 3
    if not doc or seg is None:
        abort(400, 'missing doc/segment')
    _ensure_safe_doc(doc)

    db = _db(); _ensure_schema(db)
    # Resolve version (latest if not provided)
    if version is None:
        latest = _latest_row(db, doc)
        if not latest:
            abort(404, 'no transcript available')
        version = int(latest['version'])
    else:
        version = int(version)

    # Gather words for segments [seg-neighbors .. seg+neighbors]
    start_seg = max(0, int(seg) - max(0, neighbors))
    end_seg = int(seg) + max(0, neighbors)
    cur = db.execute(
        """
        SELECT segment_index, word_index, word, start_time, end_time, probability
        FROM transcript_words
        WHERE file_path=? AND version=? AND segment_index >= ? AND segment_index <= ?
        ORDER BY word_index ASC
        """,
        [doc, version, start_seg, end_seg]
    )
    rows = cur.fetchall() or []
    if not rows:
        return jsonify({ "ok": False, "reason": "no-words" }), 200

    # Build transcript text and time window
    words = []
    clip_start = None
    clip_end = None
    for seg_idx, wi, word, st, en, pr in rows:
        try:
            w = str(word or '')
        except Exception:
            w = ''
        words.append({ 'seg': seg_idx, 'wi': wi, 'word': w, 'start': st, 'end': en })
        if st is not None:
            clip_start = st if clip_start is None else min(clip_start, float(st))
        if en is not None:
            clip_end = en if clip_end is None else max(clip_end, float(en))

    transcript = ' '.join(str(w.get('word') or '') for w in words if str(w.get('word') or '') != '\n')
    # Debug: window stats
    try:
        nonspace = [w for w in words if not (w.get('word') or '').isspace()]
        none_timings = sum(1 for w in words if (w.get('start') is None or w.get('end') is None))
        logger.info(f"[ALIGN] window stats: tokens={len(words)} nonspace={len(nonspace)} none_timings={none_timings} clip={[clip_start, clip_end]} transcript_len={len(transcript)}")
        if words:
            logger.info(f"[ALIGN] window sample: {[ (w.get('seg'), w.get('word')) for w in words[:12] ]}")
    except Exception:
        pass

    if clip_start is None or clip_end is None or clip_end <= clip_start:
        # No timings to slice; nothing to do
        return jsonify({ "ok": False, "reason": "no-timings" }), 200

    # Resolve audio path
    try:
        from ..utils import resolve_audio_path
        audio_path = resolve_audio_path(doc)
    except Exception:
        audio_path = None
    if not audio_path:
        return ("audio not found", 404)

    # If the resolved path is a tiny pointer file, attempt to dereference to blobs/<sha>
    audio_path = _maybe_deref_audio_pointer(audio_path)

    # Debug: log resolved audio path and size (helps diagnose pointer stubs vs. real blobs)
    try:
        sz = os.path.getsize(audio_path) if os.path.isfile(audio_path) else -1
    except Exception:
        sz = -1
    try:
        logger.info(f"[ALIGN] doc={doc!r} ver={version} seg={seg} neighbors={neighbors} audio_path={audio_path!r} size={sz}")
    except Exception:
        pass

    # Extract WAV clip via ffmpeg and call align endpoint
    try:
        wav_bytes, ss, to = _ffmpeg_extract_wav_clip(audio_path, clip_start, clip_end, pad=0.10)
    except Exception as e:
        try:
            err = getattr(e, 'stderr', b'')
            logger.error(f"[ALIGN] ffmpeg failed for {audio_path!r}: {err.decode('utf-8','ignore')}")
        except Exception:
            pass
        return (f'ffmpeg failed: {getattr(e, "stderr", b"\"").decode("utf-8", "ignore")}', 500)

    try:
        align_res = _align_call(wav_bytes, transcript)
        resp_words = (align_res or {}).get('words') or []
        resp_words = _explode_resp_words_if_needed(resp_words)
        try:
            _save_alignment_artifacts('align', doc, int(seg), ss, to, wav_bytes, align_res, src_audio_path=audio_path)
        except Exception:
            pass
    except Exception as e:
        return (f'align request failed: {e}', 502)

    # Map response words to global times using helper and generate diffs
    resp_words = resp_words or []
    offset = ss  # our clip starts at ss; align times relative to this
    # new_window mirrors (word_index, text, seg) from the window
    def _norm(s):
        try:
            return str(s or '').strip()
        except Exception:
            return ''

    new_window = [(int(w.get('wi')), str(w.get('word') or ''), int(w.get('seg'))) for w in words]
    updates_compact, matched = _map_aligned_to_updates(new_window, resp_words, offset, min_dur=0.20)

    # For diffs, look up original token timings by word_index
    by_wi = { int(w.get('wi')): w for w in words if w and (w.get('wi') is not None) }
    diffs = []
    updates = []  # rows for DB update
    for new_s, new_e, wi in updates_compact:
        ow = by_wi.get(int(wi)) or {}
        try:
            old_s = float(ow.get('start') or ow.get('end') or 0.0)
        except Exception:
            old_s = 0.0
        try:
            old_e = float(ow.get('end') or ow.get('start') or 0.0)
        except Exception:
            old_e = old_s
        word_text = _norm(ow.get('word'))
        seg_idx = int(ow.get('seg') or seg)
        diffs.append({
            'word': word_text,
            'old_start': old_s,
            'old_end': old_e,
            'new_start': float(new_s),
            'new_end': float(new_e),
            'delta_start': float(new_s) - old_s,
            'delta_end': float(new_e) - old_e,
            'segment_index': seg_idx,
        })
        updates.append((float(new_s), float(new_e), doc, int(version), int(wi)))

    # Logging
    try:
        nonspace_old = sum(1 for _wi, t, _sg in new_window if _norm(t) != '')
        nonspace_resp = sum(1 for rw in resp_words if _norm((rw or {}).get('word')) != '')
        logger.info(f"[ALIGN] mapping: old_seq={nonspace_old} resp={nonspace_resp} matched={matched} skipped_text_mismatch=0 diffs={len(diffs)}")
    except Exception:
        pass

    # Persist timing updates for matched tokens (so inserts around the window still benefit)
    if updates:
        try:
            db.execute("BEGIN IMMEDIATE TRANSACTION")
            db.batch_execute(
                "UPDATE transcript_words SET start_time=?, end_time=? WHERE file_path=? AND version=? AND word_index=?",
                updates
            )
            db.commit()
            try:
                logger.info(f"[ALIGN] timings updated: {len(updates)} tokens")
            except Exception:
                pass
        except Exception:
            db.execute("ROLLBACK")

    # Update transcript_edits.token_ops for parent->child
    parent_version = max(0, version - 1)
    try:
        # Serialize read+write of transcript_edits to avoid lost updates
        db.execute("BEGIN IMMEDIATE TRANSACTION")
        cur = db.execute(
            "SELECT dmp_patch, token_ops FROM transcript_edits WHERE file_path=? AND parent_version=? AND child_version=?",
            [doc, parent_version, version]
        )
        ex = cur.fetchone()
        dmp = ex[0] if ex else None
        prev_ops_raw = ex[1] if ex else None
        block = {
            'type': 'timing_adjust',
            'segment_start': start_seg,
            'segment_end': end_seg,
            'clip_start': ss,
            'clip_end': to,
            'items': diffs,
            'service': 'silence-remover',
        }
        try:
            ops = []
            if prev_ops_raw:
                parsed = orjson.loads(prev_ops_raw)
                if isinstance(parsed, list):
                    ops = parsed
                elif isinstance(parsed, dict):
                    ops = [parsed]
            ops.append(block)
            ops_json = orjson.dumps(ops).decode('utf-8')
        except Exception:
            ops_json = orjson.dumps([block]).decode('utf-8')

        db.execute(
            "INSERT OR REPLACE INTO transcript_edits (file_path, parent_version, child_version, dmp_patch, token_ops) VALUES (?, ?, ?, ?, ?)",
            [doc, parent_version, version, dmp, ops_json]
        )
        db.commit()
    except Exception:
        db.execute("ROLLBACK")
        raise

    try:
        _log_info(f"[ALIGN] align_segment elapsed_ms={(time.time()-_t0)*1000:.1f} matched={matched} diffs={len(diffs)}")
    except Exception:
        pass
    return jsonify({
        'ok': True,
        'changed_count': len([d for d in diffs if abs(d.get('delta_start', 0)) > 1e-3 or abs(d.get('delta_end', 0)) > 1e-3]),
        'total_compared': len(diffs)
    })


@bp.route('/migrate_words', methods=['POST'])
def migrate_words():
    body = request.get_json(force=True, silent=False) or {}
    doc = (body.get('doc') or '').strip()
    version = body.get('version', None)
    if not doc:
        abort(400, 'missing doc')
    _ensure_safe_doc(doc)
    db = _db(); _ensure_schema(db)

    def _synth_words(text: str) -> list:
        # naive synthesis: split by whitespace; no timings/probabilities
        words = []
        seg_idx = 0
        idx = 0
        for line in (text or '').splitlines():
            parts = [p for p in line.split() if p]
            for p in parts:
                words.append({ 'word': p, 'start': None, 'end': None, 'probability': None })
                idx += 1
            seg_idx += 1
            words.append({ 'word': '\n' })
        return words

    rows = []
    if version is None:
        cur = db.execute("SELECT version, text, words FROM transcripts WHERE file_path=? ORDER BY version ASC", [doc])
        rows = cur.fetchall() or []
    else:
        cur = db.execute("SELECT version, text, words FROM transcripts WHERE file_path=? AND version=?", [doc, int(version)])
        r = cur.fetchone()
        if r:
            rows = [r]

    migrated = 0
    db.execute("BEGIN TRANSACTION")
    try:
        for (ver, text, words_json) in rows:
            try:
                words = orjson.loads(words_json) if words_json else None
            except Exception:
                words = None
            if not isinstance(words, list) or not words:
                words = _synth_words(text or '')
            _populate_transcript_words(db, doc, int(ver), words)
            migrated += 1
        db.commit()
    except Exception:
        db.execute("ROLLBACK")
        raise
    return jsonify({ 'migrated_versions': migrated })


@bp.route('/confirmations', methods=['GET'])
def get_confirmations():
    doc = request.args.get('doc', '').strip()
    version = request.args.get('version', '').strip()
    if not doc or not version.isdigit():
        abort(400, 'missing ?doc= and/or ?version=')
    _ensure_safe_doc(doc)
    db = _db(); _ensure_schema(db)
    cur = db.execute(
        "SELECT id, start_offset, end_offset, prefix, exact, suffix FROM transcript_confirmations WHERE file_path=? AND version=? ORDER BY start_offset ASC",
        [doc, int(version)]
    )
    rows = cur.fetchall() or []
    out = [
        {"id": r[0], "start_offset": r[1], "end_offset": r[2], "prefix": r[3], "exact": r[4], "suffix": r[5]}
        for r in rows
    ]
    return jsonify(out)


@bp.route('/history', methods=['GET'])
def history():
    """Return version lineage for a document.

    Response: [ { version, parent_version, hash, created_at, created_by }, ... ]
    """
    doc = request.args.get('doc', '').strip()
    if not doc:
        abort(400, 'missing ?doc=')
    _ensure_safe_doc(doc)
    db = _db(); _ensure_schema(db)

    # Load all transcript versions
    cur = db.execute(
        """
        SELECT version, base_sha256, created_at, COALESCE(created_by,'')
        FROM transcripts WHERE file_path=? ORDER BY version ASC
        """,
        [doc]
    )
    rows = cur.fetchall() or []
    if not rows:
        return jsonify([])

    # Load explicit immediate parent->child edges (exclude origin/summary links)
    cur2 = db.execute(
        """
        SELECT parent_version, child_version
        FROM transcript_edits
        WHERE file_path=? AND parent_version = child_version - 1
        """,
        [doc]
    )
    edges = cur2.fetchall() or []
    parent_of = { int(cv): int(pv) for (pv, cv) in edges if pv is not None and cv is not None }

    out = []
    for ver, h, created_at, created_by in rows:
        v = int(ver)
        # Prefer explicit parent from edits; otherwise fall back to v-1 (or 0 for first)
        pv = parent_of.get(v)
        if pv is None:
            pv = 0 if v <= 1 else (v - 1)
        out.append({
            'version': v,
            'parent_version': int(pv),
            'hash': str(h or ''),
            'created_at': created_at,
            'created_by': created_by or ''
        })

    return jsonify(out)


@bp.route('/words', methods=['GET'])
def get_words():
    doc = request.args.get('doc', '').strip()
    version = request.args.get('version', '').strip()
    seg_q = request.args.get('segment', '').strip()
    count_q = request.args.get('count', '').strip()
    if not doc or not version.isdigit():
        abort(400, 'missing ?doc= and/or ?version=')
    _ensure_safe_doc(doc)
    db = _db(); _ensure_schema(db)
    # Build segment filter
    seg_filter_sql, extra_params, window = _build_segment_filter(seg_q, count_q)
    params = [doc, int(version), *extra_params]

    cur = db.execute(
        f"""
        SELECT segment_index, word_index, word, start_time, end_time, probability
        FROM transcript_words
        WHERE file_path=? AND version=?{seg_filter_sql}
        ORDER BY word_index ASC
        """,
        params
    )
    rows = cur.fetchall() or []
    if rows:
        out, with_timing = _normalize_db_words_rows(rows)
        try:
            logger.info(f"[WORDS] doc={doc!r} ver={version} seg_q={seg_q!r} count_q={count_q!r} returned={len(out)} with_timing={with_timing}")
        except Exception:
            pass
        return jsonify(out)

    # Fallback: use stored JSON words (optionally segment-sliced)
    row = _row_for_version(db, doc, int(version))
    if not row:
        abort(404, 'version not found')
    words = row.get('words') or []
    if seg_filter_sql:
        # Slice by counting newlines as segment boundaries and preserve newline tokens
        seg, end_seg = window  # type: ignore
        out = _slice_words_json(words, int(seg), int(end_seg))
        return jsonify(out)
    # Normalize entire JSON words list to match DB path shape
    out = _normalize_words_json_all(words)
    return jsonify(out)


@bp.route('/confirmations/save', methods=['POST'])
def save_confirmations():
    body = request.get_json(force=True, silent=False) or {}
    doc = (body.get('doc') or '').strip()
    version = body.get('version', None)
    base_sha256 = (body.get('base_sha256') or '').strip()
    items = body.get('items', [])
    if not doc or not version:
        abort(400, 'missing doc/version')
    _ensure_safe_doc(doc)
    if not base_sha256:
        abort(400, 'missing base_sha256')
    if not isinstance(items, list):
        abort(400, 'items must be an array')

    db = _db(); _ensure_schema(db)
    # Validate against stored version hash
    row = _row_for_version(db, doc, int(version))
    if not row:
        abort(404, 'version not found')
    if str(row['base_sha256']) != base_sha256:
        return ("hash conflict: confirmations base_sha256 mismatch", 409)

    # Replace confirmations transactionally; IMMEDIATE prevents concurrent writers
    db.execute("BEGIN IMMEDIATE TRANSACTION")
    try:
        # Guard delete by base_sha256 to avoid racing with an unexpected hash change
        db.execute(
            "DELETE FROM transcript_confirmations WHERE file_path=? AND version=? AND base_sha256=?",
            [doc, int(version), row['base_sha256']]
        )
        for it in items:
            s = int(it.get('start_offset') or 0)
            e = int(it.get('end_offset') or s)
            pre = str(it.get('prefix') or '')
            ex = str(it.get('exact') or '')
            suf = str(it.get('suffix') or '')
            db.execute(
                "INSERT INTO transcript_confirmations (file_path, version, base_sha256, start_offset, end_offset, prefix, exact, suffix) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [doc, int(version), row['base_sha256'], s, e, pre, ex, suf]
            )
        db.commit()
    except Exception:
        db.execute("ROLLBACK")
        raise

    return jsonify({"count": len(items)})


@bp.after_request
def add_cors(resp):
    # CORS is applied centrally in app.after_request
    return resp
