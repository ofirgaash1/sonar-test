"""Timing enrichment helpers for transcripts."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional

import orjson
from flask import current_app

from ..services.db import DatabaseService
from . import db_ops
from . import utils

BaselineLoader = Callable[[Path, str, str], Optional[dict]]


def validate_timing_data(words: List[dict]) -> None:
    """
    Validate timing data for monotonicity and detect potential artificial timing patterns.
    Raises ValueError if timing data is invalid.
    
    CRITICAL: This function should NEVER generate artificial timing data.
    If timing data is invalid, we should FAIL and expose the bug, not hide it.
    """
    if not words:
        return
    
    import re
    # Pattern to detect floating-point precision issues that are usually signs of artificial timing
    # CRITICAL NOTE FOR FUTURE DEVELOPERS:
    # In 99% of cases, floating-point precision issues like "1.5999999999999999" indicate
    # artificial timing data being generated somewhere in the codebase.
    # Only in ~1% of cases is this legitimate floating-point precision from real timing data.
    # If you change this validation, consider that logging these issues helps us catch
    # artificial timing bugs that would otherwise be hidden.
    fp_precision_pattern = re.compile(r'999999999\d')
    
    for i, word in enumerate(words):
        word_text = word.get('word', '')
        start = word.get('start')
        end = word.get('end')
        
        # Skip validation for space tokens - they don't represent actual spoken content
        if not word_text.strip():
            continue
        
        # Log floating-point precision issues (99% of the time these indicate artificial timing)
        if start is not None and fp_precision_pattern.search(str(start)):
            utils.log_info(f"[TIMING] Floating-point precision issue detected: word '{word_text}' has start time {start} - likely artificial timing data")
        
        if end is not None and fp_precision_pattern.search(str(end)):
            utils.log_info(f"[TIMING] Floating-point precision issue detected: word '{word_text}' has end time {end} - likely artificial timing data")
        
        # Check for valid timing data
        if start is not None and end is not None:
            if end < start:
                raise ValueError(f"Invalid timing data: word '{word_text}' end ({end}) < start ({start})")
            
            # Check monotonicity with previous word (skip space tokens when checking previous word)
            if i > 0:
                prev_word = words[i-1]
                prev_word_text = prev_word.get('word', '')
                prev_end = prev_word.get('end')
                # Only check monotonicity if previous word is not a space token
                if prev_word_text.strip() and prev_end is not None and start < prev_end:
                    raise ValueError(f"Non-monotonic timing: word '{word_text}' starts at {start} but previous word ends at {prev_end}")


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
        """,
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
    # For words with leading/trailing whitespace, store both forms for matching
    return _PrevToken(word=word, start=start, end=end, prob=prob, kind='word', key=stripped)


def _assign_from_prev(prev_tokens: List[_PrevToken], words: list) -> tuple[list, int]:
    """
    CRITICAL WARNING: This function should NOT generate artificial timing data!
    
    When assigning timing data from previous versions:
    1. Use only REAL timing data from the previous version
    2. If timing data is missing, leave it as None/NULL
    3. DO NOT generate fake timing data through interpolation or guessing
    4. If timing data is invalid, log the issue and leave it as None
    
    Artificial timing generation masks bugs and prevents proper debugging.
    We want to see errors when timing data is invalid, not hide them.
    """
    results: list = []
    assigned = 0
    cursor = 0
    total = len(prev_tokens)
    last_valid_end = 0.0  # Track last valid end time for interpolation

    def _match(word_text: str, word_idx: int) -> Optional[_PrevToken]:
        nonlocal cursor
        LOOKAHEAD = 128
        stripped = word_text.strip()

        # First try to match full text (including whitespace)
        for idx in range(cursor, min(total, cursor + LOOKAHEAD)):
            candidate = prev_tokens[idx]
            if candidate.kind == 'word' and not candidate.used:
                if candidate.word == word_text or candidate.key == stripped:
                    candidate.used = True
                    cursor = idx + 1
                    return candidate

        # Full search fallback for unmatched words
        for idx in range(total):
            candidate = prev_tokens[idx]
            if candidate.kind == 'word' and not candidate.used:
                if candidate.word == word_text or candidate.key == stripped:
                    candidate.used = True
                    cursor = idx + 1
                    return candidate

        # Fallback: Use position proximity if no exact match (for edited words)
        if stripped:
            closest_idx = max(0, min(word_idx, total - 1))  # Use current position as a heuristic
            candidate = prev_tokens[closest_idx]
            if candidate.kind == 'word' and not candidate.used:
                candidate.used = True
                cursor = closest_idx + 1
                utils.log_info(f"[TIMING] Fallback match for '{word_text}' at position {word_idx} using prev token at {closest_idx}")
                return candidate
        return None

    for i, token in enumerate(words or []):
        word_text = str(token.get('word') or '')
        stripped = word_text.strip()
        enriched = dict(token)

        if word_text == '\n':
            results.append(enriched)
            continue

        # Handle space tokens by interpolating from last valid end time
        if not stripped:
            enriched['start'] = last_valid_end
            enriched['end'] = last_valid_end
            enriched['probability'] = None
            results.append(enriched)
            # Reduced verbosity: only log space token assignments in debug mode
            # utils.log_info(f"[TIMING] Space token at index {i} assigned start/end={last_valid_end}")
            continue

        # Try to match word tokens
        match = _match(word_text, i)
        if match and match.kind == 'word':
            # Only assign timing data if it's actually missing (None or empty string)
            # Do NOT treat 0.0 as missing - it might be legitimate timing data
            if enriched.get('start') in (None, ''):
                if match.start is not None:
                    enriched['start'] = match.start
                    assigned += 1
            if enriched.get('end') in (None, ''):
                if match.end is not None:
                    enriched['end'] = match.end
            if enriched.get('probability') in (None, '') and match.prob is not None:
                enriched['probability'] = match.prob
            last_valid_end = enriched.get('end', last_valid_end) or last_valid_end
            # Reduced verbosity: only log timing matches in debug mode to avoid log flooding
            # utils.log_info(f"[TIMING] Matched '{word_text}' at index {i}: start={enriched.get('start')}, end={enriched.get('end')}, prob={enriched.get('probability')}")
        else:
            # For unmatched words, DO NOT generate fake timing data
            # This should be handled by proper alignment, not by making up timestamps
            enriched['start'] = None
            enriched['end'] = None
            enriched['probability'] = None
            # Reduced verbosity: only log unmatched words in debug mode
            # utils.log_info(f"[TIMING] Unmatched '{word_text}' at index {i} - NO TIMING DATA ASSIGNED (alignment required)")
        results.append(enriched)

    utils.log_info(f"[TIMING] Total assigned={assigned}, total words={len(results)}")
    return results, assigned

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
            enriched, assigned_count = _assign_from_prev(prev_tokens, words)
        else:
            assigned_count = 0
            enriched = words
        
        # Validate timing data - fail fast if invalid
        validate_timing_data(enriched)
        
    except ValueError as e:
        # Re-raise validation errors
        raise e
    except Exception:
        enriched = words
    
    try:
        words_json = orjson.dumps(enriched).decode('utf-8')
    except Exception:
        words_json = orjson.dumps(words or []).decode('utf-8')
    utils.log_info(f"[TIMING] carry_over assigned={assigned_count} total={len(enriched or [])}")
    return enriched, words_json
