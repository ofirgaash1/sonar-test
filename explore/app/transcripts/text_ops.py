"""Text composition, tokenization, and diff helpers for transcripts."""
from __future__ import annotations

import difflib
import re
from typing import List, Optional

from flask import abort


def compose_text_from_words(words: list) -> str:
    parts: List[str] = []
    for token in words or []:
        if not token:
            continue
        try:
            text = str((token or {}).get('word') or '')
        except Exception:
            text = ''
        parts.append('\n' if text == '\n' else text)
    return ''.join(parts)


def compose_full_text_from_words(words: list) -> str:
    return compose_text_from_words(words)


def canonicalize_text(value: str) -> str:
    return (value or '').replace('\r\n', '\n').replace('\r', '\n')


def diff_text(a: str, b: str) -> str:
    diff = difflib.unified_diff(a.splitlines(keepends=True), b.splitlines(keepends=True), n=0)
    return ''.join(diff)


def _canon_relaxed(text: str) -> str:
    try:
        out = str(text or '')
        out = out.replace('\r', '')
        out = out.replace('\u00A0', ' ')
        out = re.sub(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]", "", out)
        out = out.replace('\n', ' ')
        out = re.sub(r"\s+", " ", out)
        out = out.strip()
        return out
    except Exception:
        return str(text or '')


def validate_and_sanitize_words(words: list) -> list:
    if not isinstance(words, list):
        abort(400, 'words must be an array')
    sanitized = []
    for idx, token in enumerate(words):
        if not isinstance(token, dict):
            abort(400, f'words[{idx}] must be an object')
        word_val = token.get('word')
        try:
            word = str(word_val or '')
        except Exception:
            abort(400, f'words[{idx}].word must be string')
        def _to_float_or_none(value):
            if value in (None, ''):
                return None
            try:
                number = float(value)
                if number != number or number in (float('inf'), float('-inf')):
                    return None
                if number < 0:
                    return 0.0
                return number
            except Exception:
                abort(400, f'words[{idx}] timing/probability must be number or null')
        start = _to_float_or_none(token.get('start'))
        end = _to_float_or_none(token.get('end'))
        prob = _to_float_or_none(token.get('probability'))
        if start is not None and end is not None and end < start:
            end = None
        sanitized.append({'word': word, 'start': start, 'end': end, 'probability': prob})
    return sanitized


def tokenize_text_to_words(text: str) -> list:
    words = []
    lines = (text or '').splitlines()
    for idx, line in enumerate(lines):
        buffer = ''
        is_space: Optional[bool] = None
        for char in line:
            char_is_space = char.isspace() and char != '\n'
            if is_space is None:
                buffer = char
                is_space = char_is_space
            elif char_is_space == is_space:
                buffer += char
            else:
                if buffer:
                    words.append({'word': buffer})
                buffer = char
                is_space = char_is_space
        if buffer:
            words.append({'word': buffer})
        if idx < len(lines) - 1:
            words.append({'word': '\n'})
    if text and text.endswith('\n') and (not words or words[-1].get('word') != '\n'):
        words.append({'word': '\n'})
    return words


def carry_over_token_timings(old_words: list, new_words: list) -> list:
    if not old_words or not new_words:
        return new_words
    old_tokens = [w.get('word', '') for w in old_words]
    new_tokens = [w.get('word', '') for w in new_words]
    matcher = difflib.SequenceMatcher(a=old_tokens, b=new_tokens, autojunk=False)
    enriched = [dict(w) for w in new_words]
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag != 'equal':
            continue
        for offset in range(i2 - i1):
            old_idx = i1 + offset
            new_idx = j1 + offset
            old_word = old_words[old_idx]
            if old_word.get('start') is not None:
                enriched[new_idx]['start'] = old_word['start']
            if old_word.get('end') is not None:
                enriched[new_idx]['end'] = old_word['end']
            if old_word.get('probability') is not None:
                enriched[new_idx]['probability'] = old_word['probability']
    return enriched


def ensure_words_match_text(text: str, words: list) -> list:
    try:
        for token in words or []:
            if not isinstance(token, dict):
                continue
            if (token.get('start') is not None) or (token.get('end') is not None) or (token.get('probability') is not None):
                return words
    except Exception:
        pass
    try:
        if _canon_relaxed(compose_text_from_words(words or [])) == _canon_relaxed(text):
            return words
    except Exception:
        pass
    new_tokens = tokenize_text_to_words(text)
    return carry_over_token_timings(words, new_tokens)


def _segment_texts_from_words(words: list) -> list[str]:
    segments: list[str] = []
    buffer: list[str] = []
    for token in words or []:
        try:
            value = str((token or {}).get('word') or '')
        except Exception:
            value = ''
        if value == '\n':
            segment = ' '.join(' '.join(buffer).replace('\r', '').replace('\u00A0', ' ').split())
            segments.append(segment)
            buffer = []
            continue
        buffer.append(value)
    if buffer:
        segment = ' '.join(' '.join(buffer).replace('\r', '').replace('\u00A0', ' ').split())
        segments.append(segment)
    return segments


def detect_changed_segments(prev_words: list, new_words: list) -> set[int]:
    changed: set[int] = set()
    try:
        prev_segments = _segment_texts_from_words(prev_words or [])
        new_segments = _segment_texts_from_words(new_words or [])
        common = min(len(prev_segments), len(new_segments))
        for idx in range(common):
            if prev_segments[idx] != new_segments[idx]:
                changed.add(idx)
        for idx in range(common, len(new_segments)):
            changed.add(idx)
    except Exception:
        pass
    return changed
