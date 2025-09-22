from __future__ import annotations

import logging
import os
from pathlib import Path
import re as _re
import time
from typing import Optional

import orjson
import json
from flask import Blueprint, current_app, jsonify, request, abort, session

from ..services.db import DatabaseService
from ..transcripts import alignment as alignment_utils
from ..transcripts import db_ops
from ..transcripts import schema as schema_utils
from ..transcripts import text_ops
from ..transcripts import timing as timing_utils
from ..transcripts import utils as t_utils
from .browser import _read_transcript_json  # reuse transcript JSON loader

# Default number of segments to return when segment is provided without count
_DEFAULT_SEGMENT_CHUNK = 50

bp = Blueprint("transcripts", __name__, url_prefix="/transcripts")
logger = logging.getLogger(__name__)


def _db() -> DatabaseService:
    path = current_app.config.get('SQLITE_PATH') or 'explore.sqlite'
    return DatabaseService(path=str(path))


def _check_save_conflict(db: DatabaseService, doc: str, latest: Optional[dict], parent_version, expected_base_sha256: str, text: str):
    """Return a conflict payload (dict) if the save should be rejected, else None.

    This centralizes the branching logic used to gate concurrent saves.
    """
    # Canonicalize client text once for any diffs we might produce
    client_text_canon = text_ops.canonicalize_text(text or '')

    if not latest:
        # First version can proceed if parent is None/0.
        if parent_version not in (None, 0, '0'):
            return {"reason": "invalid_parent_for_first"}
        return None

    if parent_version is None:
        return {"reason": "missing_parent", "latest": latest}

    # When the parent is specified, the client should send expected_base_sha256
    if not expected_base_sha256:
        base = db_ops.row_for_version(db, doc, int(parent_version)) or {"text": ""}
        base_text = text_ops.canonicalize_text(base.get('text') or '')
        latest_text = text_ops.canonicalize_text((latest or {}).get('text') or '')
        return {
            "reason": "hash_missing",
            "latest": latest,
            "parent": {
                "version": int(parent_version),
                "base_sha256": t_utils.sha256_hex(base_text),
                "text": base.get('text') or ''
            },
            "diff_parent_to_latest": text_ops.diff_text(base_text, latest_text),
            "diff_parent_to_client": text_ops.diff_text(base_text, client_text_canon)
        }

    if int(parent_version) != int(latest['version']):
        base = db_ops.row_for_version(db, doc, int(parent_version)) or {"text": ""}
        base_text = text_ops.canonicalize_text(base.get('text') or '')
        latest_text = text_ops.canonicalize_text((latest or {}).get('text') or '')
        return {
            "reason": "version_conflict",
            "latest": latest,
            "parent": {
                "version": int(parent_version),
                "base_sha256": t_utils.sha256_hex(base_text),
                "text": base.get('text') or ''
            },
            "diff_parent_to_latest": text_ops.diff_text(base_text, latest_text),
            "diff_parent_to_client": text_ops.diff_text(base_text, client_text_canon)
        }

    if expected_base_sha256 and expected_base_sha256 != str(latest['base_sha256']):
        base = db_ops.row_for_version(db, doc, int(parent_version)) or {"text": ""}
        base_text = text_ops.canonicalize_text(base.get('text') or '')
        latest_text = text_ops.canonicalize_text((latest or {}).get('text') or '')
        return {
            "reason": "hash_conflict",
            "latest": latest,
            "parent": {
                "version": int(parent_version),
                "base_sha256": t_utils.sha256_hex(base_text),
                "text": base.get('text') or ''
            },
            "diff_parent_to_latest": text_ops.diff_text(base_text, latest_text),
            "diff_parent_to_client": text_ops.diff_text(base_text, client_text_canon)
        }
    return None


@bp.route('/latest', methods=['GET'])
def get_latest():
    doc = request.args.get('doc', '').strip()
    if not doc:
        abort(400, 'missing ?doc=')
    t_utils.ensure_safe_doc(doc)
    db = _db(); schema_utils.ensure_schema(db)
    row = db_ops.latest_row(db, doc)
    return jsonify(row or {})


@bp.route('/get', methods=['GET'])
def get_version():
    doc = request.args.get('doc', '').strip()
    version = request.args.get('version', '').strip()
    if not doc or not version.isdigit():
        abort(400, 'missing ?doc= and/or ?version=')
    t_utils.ensure_safe_doc(doc)
    db = _db(); schema_utils.ensure_schema(db)
    row = db_ops.row_for_version(db, doc, int(version))
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
    neighbors = t_utils.clamp_neighbors(body.get('neighbors', 1) or 1)
    if not doc:
        abort(400, 'missing doc')
    t_utils.ensure_safe_doc(doc)
    if not isinstance(words, list):
        abort(400, 'words must be an array')
    t_utils.ensure_safe_doc(doc)
    # Validate input words structure early
    words = text_ops.validate_and_sanitize_words(words)

    db = _db(); schema_utils.ensure_schema(db)
    latest = db_ops.latest_row(db, doc)

    # Concurrency/consistency gate
    conflict_payload = _check_save_conflict(db, doc, latest, parent_version, expected_base_sha256, text)
    if conflict_payload is not None:
        if conflict_payload.get('reason') == 'invalid_parent_for_first':
            return ("invalid parentVersion for first save", 400)
        return (jsonify(conflict_payload), 409)

    # Compute new version (hash computed later from recomposed text)
    new_version = (latest['version'] + 1) if latest else 1

    # Ensure client words reflect the edited text; if not, retokenize from text
    words = text_ops.ensure_words_match_text(text, words)
    # Re-sanitize after potential retokenization
    words = text_ops.validate_and_sanitize_words(words)
    # Serialize words for storage (will update after carry-over enrichment)
    try:
        sample = []
        for item in out[:10]:
            if isinstance(item, dict):
                sample.append((item.get('word'), item.get('start'), item.get('end')))
        logger.info(f"[WORDS_DEBUG] doc={doc!r} version={version} sample={sample}")
    except Exception:
        pass
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
        # Determine changed segments vs. latest version (fallback to hint if uncertain)
        changed_segments: set[int] = set()
        try:
            if latest and isinstance(latest.get('words'), list):
                prev_words_norm = db_ops.normalize_words_json_all(latest['words'])
                changed_segments = text_ops.detect_changed_segments(prev_words_norm, words)
        except Exception:
            changed_segments = set()

        # If none detected, fallback to provided hint (single window)
        if not changed_segments and (seg_hint is not None):
            try:
                changed_segments = { int(seg_hint) }
            except Exception:
                changed_segments = set()

        # Call aligner per changed segment window and merge updates
        all_updates: list[tuple[float,float,int]] = []
        token_ops_block = None
        if changed_segments:
            try:
                logger.info(f"[ALIGN] changed_segments={sorted(changed_segments)} neighbors={neighbors}")
            except Exception:
                pass
            seen_wi = set()
            for si in sorted(changed_segments):
                u_i, tok_i = alignment_utils.prealign_updates(db, doc, latest, words, si, neighbors)
                # Merge updates (last write wins per word_index)
                for (s,e,wi) in (u_i or []):
                    if wi in seen_wi:
                        # replace existing for same wi: remove previous then add
                        try:
                            for k in range(len(all_updates)-1, -1, -1):
                                if all_updates[k][2] == wi:
                                    all_updates.pop(k)
                                    break
                        except Exception:
                            pass
                    all_updates.append((s,e,wi))
                    seen_wi.add(wi)
                # Prefer the first non-null token_ops block
                if token_ops_block is None and tok_i is not None:
                    token_ops_block = tok_i
        else:
            # Nothing to align
            all_updates = []
        updates = all_updates
    else:
        updates, token_ops_block = [], None
    words, words_json = timing_utils.carry_over_timings_from_db(db, doc, latest, words, _read_transcript_json)

    # Recompose authoritative text from words and canonicalize for storage + hashing
    try:
        recomposed_text = text_ops.compose_full_text_from_words(words)
        store_text = text_ops.canonicalize_text(recomposed_text)
    except Exception:
        # Fallback to client-provided text (canonicalized)
        store_text = text_ops.canonicalize_text(text)
    # Compute child hash from canonicalized text
    new_hash = t_utils.sha256_hex(store_text)

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
            db_ops.populate_transcript_words(db, doc, new_version, words)
        # Apply alignment updates only when we have normalized rows
        if updates and not client_words_were_empty:
            db.batch_execute(
                "UPDATE transcript_words SET start_time=?, end_time=? WHERE file_path=? AND version=? AND word_index=?",
                [ (s, e, doc, int(new_version), wi) for (s,e,wi) in updates ]
            )
        # Backfill missing probabilities from previous version when word_index aligns
        try:
            if latest:
                db.execute(
                    """
                    UPDATE transcript_words AS t
                    SET probability = (
                        SELECT p.probability FROM transcript_words AS p
                        WHERE p.file_path = ? AND p.version = ? AND p.word_index = t.word_index
                    )
                    WHERE t.file_path = ? AND t.version = ? AND t.probability IS NULL
                    """,
                    [doc, int(latest['version']), doc, int(new_version)]
                )
        except Exception:
            pass
        # Normalize end_time to ensure non-zero durations per token
        try:
            norm_count = 0 if client_words_were_empty else db_ops.normalize_end_times(db, doc, int(new_version), min_dur=0.20)
        except Exception:
            norm_count = 0

        # Store edit deltas relative to parent (if exists)
        if latest:
            d_parent = text_ops.diff_text(latest['text'] or '', store_text)
            db.execute(
                "INSERT OR REPLACE INTO transcript_edits (file_path, parent_version, child_version, dmp_patch, token_ops) VALUES (?, ?, ?, ?, ?)",
                [doc, int(latest['version']), new_version, d_parent, orjson.dumps(token_ops_block).decode('utf-8') if token_ops_block else None]
            )

        # Also store delta relative to origin (v1) for fast replay
        if latest and latest['version'] >= 1:
            v1 = db_ops.row_for_version(db, doc, 1)
            if v1:
                d_origin = text_ops.diff_text(v1['text'] or '', store_text)
                db.execute(
                    "INSERT OR REPLACE INTO transcript_edits (file_path, parent_version, child_version, dmp_patch, token_ops) VALUES (?, ?, ?, ?, ?)",
                    [doc, 1, new_version, d_origin, None]
                )

        db.commit()
        try:
            # Debug: how many tokens have timings after save
            cur = db.execute(
                "SELECT COUNT(*), SUM(CASE WHEN start_time IS NOT NULL AND end_time IS NOT NULL AND end_time > start_time THEN 1 ELSE 0 END) FROM transcript_words WHERE file_path=? AND version=?",
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
    db = _db(); schema_utils.ensure_schema(db)
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
    try:
        sample = json.dumps(out[:10], ensure_ascii=False)
        logger.info(f"[WORDS_DEBUG] doc={doc!r} version={version} sample={sample}")
    except Exception:
        pass
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
    t_utils.ensure_safe_doc(doc)

    db = _db(); schema_utils.ensure_schema(db)
    # Resolve version (latest if not provided)
    if version is None:
        latest = db_ops.latest_row(db, doc)
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
    audio_path = alignment_utils.maybe_deref_audio_pointer(audio_path)

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
        wav_bytes, ss, to = alignment_utils.ffmpeg_extract_wav_clip(audio_path, clip_start, clip_end, pad=0.10)
    except Exception as e:
        try:
            err = getattr(e, 'stderr', b'')
            logger.error(f"[ALIGN] ffmpeg failed for {audio_path!r}: {err.decode('utf-8','ignore')}")
        except Exception:
            pass
        return (f'ffmpeg failed: {getattr(e, "stderr", b"\"").decode("utf-8", "ignore")}', 500)

    try:
        align_res = alignment_utils.align_call(wav_bytes, transcript)
        resp_words = (align_res or {}).get('words') or []
        resp_words = alignment_utils.explode_resp_words_if_needed(resp_words)
        try:
            alignment_utils.save_alignment_artifacts('align', doc, int(seg), ss, to, wav_bytes, align_res, src_audio_path=audio_path)
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
    updates_compact, matched = alignment_utils.map_aligned_to_updates(new_window, resp_words, offset, min_dur=0.20)

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
    updated_count = len(updates)
    if updates:
        try:
            db.execute("BEGIN IMMEDIATE TRANSACTION")
            db.batch_execute(
                "UPDATE transcript_words SET start_time=?, end_time=? WHERE file_path=? AND version=? AND word_index=?",
                updates
            )
            db.commit()
            try:
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
        t_utils.log_info(f"[ALIGN] align_segment elapsed_ms={(time.time()-_t0)*1000:.1f} matched={matched} diffs={len(diffs)}")
    except Exception:
        pass
    t_utils.log_info(f"[ALIGN] timings updated: {updated_count} tokens")
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
    t_utils.ensure_safe_doc(doc)
    db = _db(); schema_utils.ensure_schema(db)

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
            db_ops.populate_transcript_words(db, doc, int(ver), words)
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
    t_utils.ensure_safe_doc(doc)
    db = _db(); schema_utils.ensure_schema(db)
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
    t_utils.ensure_safe_doc(doc)
    db = _db(); schema_utils.ensure_schema(db)

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
    t_utils.ensure_safe_doc(doc)
    db = _db(); schema_utils.ensure_schema(db)
    # Build segment filter
    seg_filter_sql, extra_params, window = db_ops.build_segment_filter(seg_q, count_q, _DEFAULT_SEGMENT_CHUNK)
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
        out, with_timing = db_ops.normalize_db_words_rows(rows)
        try:
            logger.info(f"[WORDS] doc={doc!r} ver={version} seg_q={seg_q!r} count_q={count_q!r} returned={len(out)} with_timing={with_timing}")
        except Exception:
            pass
        return jsonify(out)

    # Fallback: use stored JSON words (optionally segment-sliced)
    row = db_ops.row_for_version(db, doc, int(version))
    if not row:
        abort(404, 'version not found')
    words = row.get('words') or []
    if seg_filter_sql:
        # Slice by counting newlines as segment boundaries and preserve newline tokens
        seg, end_seg = window  # type: ignore
        out = db_ops.slice_words_json(words, int(seg), int(end_seg))
        return jsonify(out)
    # Normalize entire JSON words list to match DB path shape
    out = db_ops.normalize_words_json_all(words)
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
    t_utils.ensure_safe_doc(doc)
    if not base_sha256:
        abort(400, 'missing base_sha256')
    if not isinstance(items, list):
        abort(400, 'items must be an array')

    db = _db(); schema_utils.ensure_schema(db)
    # Validate against stored version hash
    row = db_ops.row_for_version(db, doc, int(version))
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


