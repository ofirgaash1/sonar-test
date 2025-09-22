"""Database access helpers for transcript storage and retrieval."""
from __future__ import annotations

from typing import Optional

import orjson

from ..services.db import DatabaseService


def latest_row(db: DatabaseService, file_path: str) -> Optional[dict]:
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


def row_for_version(db: DatabaseService, file_path: str, version: int) -> Optional[dict]:
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


def populate_transcript_words(db: DatabaseService, doc: str, version: int, words: list) -> None:
    db.execute("DELETE FROM transcript_words WHERE file_path=? AND version=?", [doc, int(version)])
    seg_idx = 0
    rows = []
    for wi, token in enumerate(words or []):
        try:
            word = str(token.get("word", ""))
        except AttributeError:
            word = ""
        if word == "\n":
            seg_idx += 1
            continue
        start = token.get("start")
        end = token.get("end")
        prob = token.get("probability")
        rows.append((doc, int(version), seg_idx, wi, word, start, end, prob))
    if rows:
        db.batch_execute(
            """
            INSERT INTO transcript_words (file_path, version, segment_index, word_index, word, start_time, end_time, probability)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            rows,
        )


def fetch_words_rows(db: DatabaseService, doc: str, version: int, start_seg: int, end_seg: int):
    cur = db.execute(
        """
        SELECT segment_index, word_index, word, start_time, end_time, probability
        FROM transcript_words
        WHERE file_path=? AND version=? AND segment_index >= ? AND segment_index <= ?
        ORDER BY word_index ASC
        """,
        [doc, int(version), start_seg, end_seg],
    )
    return cur.fetchall() or []


def normalize_end_times(db: DatabaseService, doc: str, version: int, min_dur: float = 0.20) -> int:
    cur = db.execute(
        """
        SELECT segment_index, word_index, start_time, end_time
        FROM transcript_words
        WHERE file_path=? AND version=?
        ORDER BY segment_index ASC, word_index ASC
        """,
        [doc, int(version)],
    )
    rows = cur.fetchall() or []
    updated = []
    seg_map: dict[int, list[tuple[int, Optional[float], Optional[float]]]] = {}
    for seg, wi, st, en in rows:
        seg_map.setdefault(int(seg), []).append((int(wi), (float(st) if st is not None else None), (float(en) if en is not None else None)))
    for items in seg_map.values():
        prev_end: Optional[float] = None
        starts = [itm[1] for itm in items]
        for idx, (wi, s_raw, e_raw) in enumerate(items):
            if s_raw is None and e_raw is None:
                continue
            start = s_raw if s_raw is not None else (prev_end if prev_end is not None else 0.0)
            if prev_end is not None and start is not None and start < prev_end:
                start = prev_end
            next_start = None
            for future in starts[idx + 1:]:
                if future is not None and (start is None or future > start):
                    next_start = future
                    break
            if e_raw is not None and start is not None and e_raw > start:
                end = e_raw
            else:
                end = next_start if next_start is not None else ((start or 0.0) + float(min_dur))
            prev_end = end
            need_update = (
                s_raw is None or e_raw is None or end != e_raw or (start is not None and prev_end is not None and start < prev_end)
            )
            if need_update:
                updated.append((float(start or 0.0), float(end), doc, int(version), int(wi)))
    if updated:
        db.batch_execute(
            "UPDATE transcript_words SET start_time=?, end_time=? WHERE file_path=? AND version=? AND word_index=?",
            updated,
        )
    return len(updated)


def normalize_db_words_rows(rows):
    MIN_DUR = 0.20
    out = []
    with_timing = 0
    current_segment = None
    buffer = []

    def flush(segment_tokens):
        nonlocal out, with_timing
        n = len(segment_tokens)
        for token in segment_tokens:
            try:
                token['start'] = float(token.get('start') or 0.0)
            except Exception:
                token['start'] = 0.0
            try:
                token['end'] = float(token.get('end') if token.get('end') is not None else token['start'])
            except Exception:
                token['end'] = float(token['start'])
        for i in range(n):
            s_val = float(segment_tokens[i].get('start') or 0.0)
            e_val = float(segment_tokens[i].get('end') or 0.0)
            if not (e_val > s_val):
                next_s = None
                for future in range(i + 1, n):
                    ns = float(segment_tokens[future].get('start') or 0.0)
                    if ns > s_val:
                        next_s = ns
                        break
                if next_s is not None:
                    e_val = next_s
                else:
                    e_val = s_val + MIN_DUR
                segment_tokens[i]['end'] = e_val
            if (s_val > 0) or (e_val > 0):
                with_timing += 1
        out.extend(segment_tokens)

    for seg, wi, word, st, en, pr in rows:
        if (current_segment is not None) and (seg != current_segment):
            if buffer:
                flush(buffer)
                buffer = []
            try:
                prev_end = out[-1]['end'] if out else 0.0
            except Exception:
                prev_end = 0.0
            out.append({"word": "\n", "start": prev_end, "end": prev_end, "probability": None})
        buffer.append({
            "word": word,
            "start": st if st is not None else 0.0,
            "end": en if en is not None else None,
            "probability": float(pr) if pr is not None else None,
        })
        current_segment = seg
    if buffer:
        flush(buffer)
    return out, with_timing


def slice_words_json(words: list, seg: int, end_seg: int) -> list:
    out = []
    cur_seg = 0
    started = False
    for token in words:
        if not token:
            continue
        word_val = token.get('word') if isinstance(token, dict) else None
        if word_val == '\n':
            if started and cur_seg >= end_seg:
                break
            cur_seg += 1
            if started and cur_seg <= end_seg:
                out.append({"word": "\n", "start": token.get('start') or 0.0, "end": token.get('start') or 0.0, "probability": None})
            continue
        if cur_seg < seg:
            continue
        started = True
        out.append({
            "word": str(token.get('word') or ''),
            "start": float(token.get('start') or 0.0),
            "end": float(token.get('end') if token.get('end') is not None else (token.get('start') or 0.0)),
            "probability": (float(token.get('probability')) if (token.get('probability') not in (None, '')) else None),
        })
    return out


def normalize_words_json_all(words: list) -> list:
    out = []
    prev_end = 0.0
    for token in words or []:
        if not token:
            continue
        try:
            word_val = str(token.get('word') or '')
        except Exception:
            word_val = ''
        if word_val == '\n':
            out.append({"word": "\n", "start": prev_end, "end": prev_end, "probability": None})
            continue
        try:
            start = float(token.get('start') or 0.0)
        except Exception:
            start = 0.0
        try:
            end = float(token.get('end') if token.get('end') is not None else start)
        except Exception:
            end = start
        try:
            prob = (float(token.get('probability')) if (token.get('probability') not in (None, '')) else None)
        except Exception:
            prob = None
        out.append({"word": word_val, "start": start, "end": end, "probability": prob})
        prev_end = end
    return out


def build_segment_filter(seg_q: str, count_q: str, default_chunk: int = 50):
    """Return (SQL filter, params, window tuple) for segment slicing."""
    seg_filter_sql = ''
    params_extra: list[int] = []
    window = None
    if seg_q.isdigit():
        seg = int(seg_q)
        if count_q.isdigit():
            end_seg = seg + max(0, int(count_q)) - 1
        else:
            end_seg = seg + max(0, default_chunk) - 1
        seg_filter_sql = ' AND segment_index >= ? AND segment_index <= ?'
        params_extra = [seg, end_seg]
        window = (seg, end_seg)
    return seg_filter_sql, params_extra, window
