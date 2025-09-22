"""Timing enrichment helpers for transcripts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional

import orjson
from flask import current_app

from ..services.db import DatabaseService
from . import db_ops

BaselineLoader = Callable[[Path, str, str], Optional[dict]]


@dataclass
class _PrevToken:
    word: str
    start: Optional[float]
    end: Optional[float]
    prob: Optional[float]
    kind: str  # 'word', 'space', or 'newline'
    key: Optional[str]
    used: bool = False


def _build_prev_sequence_from_db(db: DatabaseService, doc: str, latest: Optional[dict]) -> List[_PrevToken]:
    tokens: List[_PrevToken] = []
    if latest and isinstance(latest.get('words'), list):
        for item in latest['words'] or []:
            try:
                word = str((item or {}).get('word') or '')
            except Exception:
                word = ''
            try:
                start = float((item or {}).get('start')) if (item or {}).get('start') is not None else None
            except Exception:
                start = None
            try:
                end_val = float((item or {}).get('end')) if (item or {}).get('end') is not None else None
            except Exception:
                end_val = None
            prob_val = (item or {}).get('probability')
            if prob_val in (None, ''):
                prob_val = (item or {}).get('prob')
            try:
                prob = float(prob_val) if prob_val is not None else None
            except Exception:
                prob = None
            tokens.append(_as_prev_token(word, start, end_val, prob))
    if tokens:
        return tokens
    if not latest:
        return []
    cur = db.execute(
        """
        SELECT word, start_time, end_time, probability
        FROM transcript_words
        WHERE file_path=? AND version=?
        ORDER BY word_index ASC
        """
        [doc, int(latest['version'])]
    )
    prev_rows = cur.fetchall() or []
    return [_as_prev_token(row[0], row[1], row[2], row[3]) for row in prev_rows]
def _build_prev_sequence_from_baseline(doc: str, loader: BaselineLoader) -> List[_PrevToken]:
    transcripts_dir = current_app.config.get('TRANSCRIPTS_DIR')
    if not transcripts_dir or not loader:
        return []
    try:
        folder, file_name = doc.split('/', 1)
    except ValueError:
        folder, file_name = '', doc
    try:
        data = loader(Path(transcripts_dir), folder, file_name)
    except Exception:
        data = None
    if not isinstance(data, dict):
        return []
    prev_seq: List[_PrevToken] = []
    segments = data.get('segments') if isinstance(data.get('segments'), list) else None
    if segments:
        last_end = 0.0
        for segment in segments:
            words_list = (segment or {}).get('words')
            if not isinstance(words_list, list):
                continue
            for token in words_list:
                word = str((token or {}).get('word') or '')
                try:
                    start = float((token or {}).get('start')) if (token or {}).get('start') is not None else None
                except Exception:
                    start = None
                try:
                    end = float((token or {}).get('end')) if (token or {}).get('end') is not None else None
                except Exception:
                    end = None
                try:
                    prob_val = (float((token or {}).get('probability')) if (token or {}).get('probability') not in (None, '') else None)
                except Exception:
                    prob_val = None
                prev_seq.append(_as_prev_token(word, start, end, prob_val))
                if end is not None:
                    last_end = float(end)
            prev_seq.append(_as_prev_token('\n', last_end, last_end, None))
        if prev_seq and prev_seq[-1].word == '\n':
            prev_seq.pop()
    elif isinstance(data.get('words'), list):
        for token in data.get('words'):
            word = str((token or {}).get('word') or '')
            try:
                start = float((token or {}).get('start')) if (token or {}).get('start') is not None else None
            except Exception:
                start = None
            try:
                end = float((token or {}).get('end')) if (token or {}).get('end') is not None else None
            except Exception:
                end = None
            try:
                prob_val = (float((token or {}).get('probability')) if (token or {}).get('probability') not in (None, '') else None)
            except Exception:
                prob_val = None
            prev_seq.append(_as_prev_token(word, start, end, prob_val))
    return prev_seq


def _as_prev_token(word: str, start: Optional[float], end: Optional[float], prob: Optional[float]) -> _PrevToken:
    if word == '\n':
        return _PrevToken(word='\n', start=start, end=end, prob=prob, kind='newline', key=None)
    stripped = word.strip()
    if not stripped:
        return _PrevToken(word=word, start=start, end=end, prob=prob, kind='space', key=None)
    return _PrevToken(word=word, start=start, end=end, prob=prob, kind='word', key=stripped)


def _assign_from_prev(prev_tokens: List[_PrevToken], words: list) -> list:
    results = []
    cursor = 0
    total = len(prev_tokens)

    def _match(key: str) -> Optional[_PrevToken]:
        nonlocal cursor
        LOOKAHEAD = 128
        for idx in range(cursor, min(total, cursor + LOOKAHEAD)):
            candidate = prev_tokens[idx]
            if candidate.kind == 'word' and not candidate.used and candidate.key == key:
                candidate.used = True
                cursor = idx + 1
                return candidate
        for idx in range(total):
            candidate = prev_tokens[idx]
            if candidate.kind == 'word' and not candidate.used and candidate.key == key:
                candidate.used = True
                cursor = idx + 1
                return candidate
        return None

    for token in words or []:
        word_text = str(token.get('word') or '')
        stripped = word_text.strip()
        enriched = dict(token)
        if word_text == '\n':
            results.append(enriched)
            continue
        if not stripped:
            results.append(enriched)
            continue
        match = _match(stripped)
        if match:
            if enriched.get('start') in (None, '') or float(enriched.get('start') or 0.0) == 0.0:
                if match.start is not None:
                    enriched['start'] = match.start
            if enriched.get('end') in (None, '') or float(enriched.get('end') or 0.0) == 0.0:
                if match.end is not None:
                    enriched['end'] = match.end
            if enriched.get('probability') in (None, '') and match.prob is not None:
                enriched['probability'] = match.prob
        results.append(enriched)
    return results


def carry_over_timings_from_db(
    db: DatabaseService,
    doc: str,
    latest: Optional[dict],
    words: list,
    baseline_loader: BaselineLoader
) -> tuple[list, str]:
    try:
        if not isinstance(words, list) or not words:
            words_json = orjson.dumps(words or []).decode('utf-8')
            return words, words_json
        prev_tokens = _build_prev_sequence_from_db(db, doc, latest)
        if not prev_tokens:
            prev_tokens = _build_prev_sequence_from_baseline(doc, baseline_loader)
        if prev_tokens:
            enriched = _assign_from_prev(prev_tokens, words)
        else:
            enriched = words
    except Exception:
        enriched = words
    try:
        words_json = orjson.dumps(enriched).decode('utf-8')
    except Exception:
        words_json = orjson.dumps(words or []).decode('utf-8')
    return enriched, words_json
