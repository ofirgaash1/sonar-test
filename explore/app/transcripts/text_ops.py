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
        start_pos = 0
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
                    # Maintain word positions for better timing tracking
                    words.append({
                        'word': buffer,
                        '_position': start_pos
                    })
                buffer = char
                is_space = char_is_space
                start_pos += len(buffer)
        if buffer:
            words.append({
                'word': buffer,
                '_position': start_pos
            })
        if idx < len(lines) - 1:
            words.append({'word': '\n', '_position': len(line)})
    if text and text.endswith('\n') and (not words or words[-1].get('word') != '\n'):
        words.append({'word': '\n'})
    return words


def carry_over_token_timings(old_words: list, new_words: list) -> list:
    import logging
    logger = logging.getLogger(__name__)
    
    if not old_words or not new_words:
        logger.info("[TIMING] No words to process")
        return new_words

    # Create a mapping of word text to timing data from old words
    # Maps both stripped and unstripped forms for flexibility
    timing_map = {}
    logger.info(f"[TIMING] Processing {len(old_words)} old words")
    for w in old_words:
        word = str(w.get('word', ''))
        if not word:
            continue
        timing_data = {}
        if w.get('start') is not None:
            timing_data['start'] = w['start']
        if w.get('end') is not None:
            timing_data['end'] = w['end']
        if w.get('probability') is not None:
            timing_data['probability'] = w['probability']
        if timing_data:
            logger.info(f"[TIMING] Found timing data for '{word}': {timing_data}")
            timing_map[word] = timing_data  # Preserve original form with whitespace
            stripped = word.strip()
            if stripped and stripped != word:
                timing_map[stripped] = timing_data  # Also map stripped version

    # Apply timing data to new words when text matches
    enriched = []
    logger.info(f"[TIMING] Processing {len(new_words)} new words")
    
    # First pass: collect position information
    total_length = sum(len(str(w.get('word', ''))) for w in new_words)
    position_map = {}
    curr_pos = 0
    for w in new_words:
        word = str(w.get('word', ''))
        position_map[curr_pos] = word
        curr_pos += len(word)
    
    # Second pass: match words with timing data
    for w in new_words:
        new_token = dict(w)
        word = str(w.get('word', ''))
        pos = w.get('_position', 0)
        
        # Try matching based on position first
        matched = False
        if word in timing_map:
            # Try exact match first (preserves whitespace)
            timing_data = timing_map[word]
            logger.info(f"[TIMING] Found exact match for '{word}'")
            matched = True
        elif word.strip() in timing_map:
            # Fallback to stripped version
            timing_data = timing_map[word.strip()]
            logger.info(f"[TIMING] Found stripped match for '{word}'")
            matched = True
            
        if not matched:
            # No match found - try to find closest word by position
            logger.info(f"[TIMING] No direct match for '{word}', checking position {pos}/{total_length}")
            closest_pos = min(position_map.keys(), key=lambda x: abs(x - pos))
            closest_word = position_map[closest_pos]
            if closest_word in timing_map:
                timing_data = timing_map[closest_word]
                logger.info(f"[TIMING] Found position-based match: '{word}' ≈ '{closest_word}'")
                matched = True
            else:
                logger.info(f"[TIMING] No timing data found for '{word}'")
                enriched.append(new_token)
                continue
            
        # Carry over timing data
        for key, value in timing_data.items():
            new_token[key] = value
        logger.info(f"[TIMING] Applied timing data to '{word}': {new_token}")
        enriched.append(new_token)

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
