"""Alignment helpers for transcript timing adjustments."""
from __future__ import annotations

import os
import subprocess
import time
import uuid
import difflib
from typing import Optional

import orjson
import requests
from flask import current_app

from ..services.db import DatabaseService
from . import db_ops, utils


def maybe_deref_audio_pointer(audio_path: str) -> str:
    try:
        if os.path.isfile(audio_path) and os.path.getsize(audio_path) <= 512:
            with open(audio_path, 'rb') as pointer_file:
                data = pointer_file.read(512)
            try:
                text = data.decode('utf-8', 'ignore')
            except Exception:
                text = ''
            import re as _re
            match = _re.search(r'\bsha:([a-fA-F0-9]{40,64})\b', text)
            if not match:
                return audio_path
            sha = match.group(1)
            audio_dir = current_app.config.get('AUDIO_DIR')
            if not audio_dir:
                return audio_path
            candidate = os.path.join(audio_dir, 'blobs', sha)
            if os.path.isfile(candidate):
                return candidate
    except Exception:
        pass
    return audio_path


def ffmpeg_extract_wav_clip(audio_path: str, clip_start: float, clip_end: float, pad: float = 0.10) -> tuple[bytes, float, float]:
    ss = max(0.0, float(clip_start) - pad)
    to = float(clip_end) + pad
    cmd = [
        'ffmpeg', '-hide_banner', '-loglevel', 'error',
        '-ss', f'{ss:.3f}', '-to', f'{to:.3f}', '-i', audio_path,
        '-ac', '1', '-ar', '16000', '-f', 'wav', 'pipe:1'
    ]
    utils.log_info(f"[ALIGN] ffmpeg cmd: {' '.join(cmd)}")
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return proc.stdout, ss, to


def align_call(wav_bytes: bytes, transcript: str) -> dict:
    files = {'audio': ('clip.wav', wav_bytes, 'audio/wav')}
    data = {'transcript': transcript}
    response = requests.post('http://silence-remover.com:8000/align', files=files, data=data, timeout=60)
    if not response.ok:
        raise RuntimeError(f'align-endpoint {response.status_code}: {response.text[:200]}')
    payload = response.json() or {}
    try:
        words = (payload or {}).get('words') or []
        sample = [(str((w or {}).get('word') or ''), (w or {}).get('start'), (w or {}).get('end')) for w in (words[:10] or [])]
        utils.log_info(f"[ALIGN] response: words={len(words)} sample={sample}")
    except Exception:
        pass
    return payload


def _alignment_log_dir() -> str:
    base_dir = current_app.config.get('AUDIO_LOG_DIR') or os.path.join(os.getcwd(), 'audio-log')
    try:
        os.makedirs(base_dir, exist_ok=True)
    except Exception:
        pass
    return base_dir


def save_alignment_artifacts(
    kind: str,
    doc: str,
    seg: Optional[int],
    clip_start: float,
    clip_end: float,
    wav_bytes: bytes,
    response_json: dict,
    src_audio_path: Optional[str] = None,
) -> None:
    try:
        base_dir = _alignment_log_dir()
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        uid = str(uuid.uuid4())[:8]
        seg_part = f"seg{int(seg)}" if seg is not None else 'segNA'
        base_name = f"{kind}_{utils.safe_name(doc)}_{seg_part}_{timestamp}_{uid}_{clip_start:.3f}-{clip_end:.3f}"
        wav_path = os.path.join(base_dir, base_name + '.wav')
        json_path = os.path.join(base_dir, base_name + '.response.json')
        try:
            with open(wav_path, 'wb') as fh:
                fh.write(wav_bytes or b'')
        except Exception:
            pass
        try:
            with open(json_path, 'wb') as fh:
                fh.write(orjson.dumps(response_json))
        except Exception:
            pass
        try:
            if src_audio_path and current_app.config.get('AUDIO_LOG_NATIVE', True):
                native_path = os.path.join(base_dir, base_name + '.native.wav')
                cmd = [
                    'ffmpeg', '-hide_banner', '-loglevel', 'error',
                    '-ss', f'{clip_start:.3f}', '-to', f'{clip_end:.3f}', '-i', src_audio_path,
                    '-f', 'wav', '-c:a', 'pcm_s16le', native_path
                ]
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except Exception:
            pass
    except Exception:
        pass


def explode_resp_words_if_needed(words: list) -> list:
    if not isinstance(words, list):
        return []
    out = []
    for w in words:
        try:
            text = str((w or {}).get('word') or '')
        except Exception:
            text = ''
        if not text:
            out.append(w)
            continue
        if ' ' in text.strip():
            parts = text.split()
            start = float((w or {}).get('start') or 0.0)
            end = float((w or {}).get('end') or 0.0)
            if not parts:
                out.append(w)
                continue
            span = max(end - start, 0.0)
            step = span / max(len(parts), 1) if span > 0 else 0.0
            for idx, part in enumerate(parts):
                new_start = start + idx * step
                new_end = new_start + step if step else new_start
                out.append({'word': part, 'start': new_start, 'end': new_end, 'probability': (w or {}).get('probability')})
        else:
            out.append(w)
    return out


def build_new_window(words: list, start_seg: int, end_seg: int):
    window = []
    transcript_parts = []
    seg_idx = 0
    for idx, token in enumerate(words or []):
        try:
            word = str(token.get('word') or '')
        except Exception:
            word = ''
        if word == '\n':
            seg_idx += 1
            continue
        if seg_idx < start_seg or seg_idx > end_seg:
            continue
        window.append((idx, word, seg_idx))
        transcript_parts.append(word)
    transcript = ' '.join(transcript_parts).strip()
    return window, transcript


def map_aligned_to_updates(new_window: list[tuple[int, str, int]], resp_words: list, offset: float, min_dur: float = 0.20) -> tuple[list[tuple[float, float, int]], int]:
    def _norm(val):
        try:
            return str(val or '').strip()
        except Exception:
            return ''

    new_seq = [(idx, _norm(word)) for (idx, word, _seg) in new_window if _norm(word) != '']
    resp_seq = [((w or {}), _norm((w or {}).get('word'))) for w in (resp_words or []) if _norm((w or {}).get('word')) != '']

    updates: list[tuple[float, float, int]] = []
    matched = 0

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
        total_chars = sum(max(1, len(token_text)) for (_wi, token_text) in new_seq) or len(new_seq)
        cur = rs
        for idx, (wi, token_text) in enumerate(new_seq):
            if idx == len(new_seq) - 1:
                ns = cur
                ne = re if re > ns else (ns + 0.01)
            else:
                frac = max(1, len(token_text)) / total_chars
                dur = max(0.01, span * frac)
                ns = cur
                ne = min(re, ns + dur)
            updates.append((float(ns), float(ne), int(wi)))
            matched += 1
            cur = ne
        return updates, matched

    new_tokens = [token for (_wi, token) in new_seq]
    resp_tokens = [token for (_rw, token) in resp_seq]
    matcher = difflib.SequenceMatcher(a=new_tokens, b=resp_tokens)

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
            next_rs = None
            if (idx + 1) < len(resp_seq):
                try:
                    rn = resp_seq[idx + 1][0]
                    next_rs = float(rn.get('start') or 0.0) + offset
                except Exception:
                    next_rs = None
            re = next_rs if (next_rs is not None and next_rs > rs) else (rs + float(min_dur))
        return float(rs), float(re)

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            for rel in range(i2 - i1):
                wi = new_seq[i1 + rel][0]
                rs, re = _resp_time(j1 + rel)
                updates.append((rs, re, int(wi)))
                matched += 1

    return updates, matched
def compute_clip_from_prev_rows(prev_rows) -> tuple[Optional[float], Optional[float]]:
    starts = [float(row[2]) for row in prev_rows if row[2] is not None]
    ends = [float(row[3]) for row in prev_rows if row[3] is not None]
    if not starts or not ends:
        return None, None
    return min(starts), max(ends)


def segment_window(seg_hint, neighbors: int) -> tuple[int, int]:
    seg = int(seg_hint)
    n = utils.clamp_neighbors(neighbors)
    start_seg = max(0, seg - n)
    end_seg = seg + n
    return start_seg, end_seg


def prealign_updates(
    db: DatabaseService,
    doc: str,
    latest: Optional[dict],
    words: list,
    seg_hint,
    neighbors: int,
) -> tuple[list[tuple[float, float, int]], Optional[dict]]:
    updates = []
    token_ops_block = None
    if not (latest and seg_hint is not None):
        return updates, token_ops_block

    try:
        start_seg, end_seg = segment_window(seg_hint, neighbors)
        prev_rows = db_ops.fetch_words_rows(db, doc, int(latest['version']), start_seg, end_seg)
        clip_start, clip_end = compute_clip_from_prev_rows(prev_rows)
        if clip_start is None or clip_end is None:
            raise RuntimeError('prealign-skip:no-timings')
        new_window, transcript = build_new_window(words, start_seg, end_seg)
        if not transcript:
            raise RuntimeError('prealign-skip:empty-window')
        from ..utils import resolve_audio_path
        audio_path = resolve_audio_path(doc)
        if not audio_path:
            raise RuntimeError('prealign-skip:audio-not-found')
        audio_path = maybe_deref_audio_pointer(audio_path)
        wav_bytes, ss, to = ffmpeg_extract_wav_clip(audio_path, clip_start, clip_end, pad=0.10)
        align_res = align_call(wav_bytes, transcript)
        resp_words = (align_res or {}).get('words') or []
        resp_words = explode_resp_words_if_needed(resp_words)
        try:
            save_alignment_artifacts('prealign', doc, int(seg_hint), ss, to, wav_bytes, align_res, src_audio_path=audio_path)
        except Exception:
            pass
        updates, matched = map_aligned_to_updates(new_window, resp_words, ss, min_dur=0.20)
        utils.log_info(f"[ALIGN] prealign mapping: new_seq={len(new_window)} resp_seq={len(resp_words)} matched={matched} updates={len(updates)}")
        if matched == 0:
            raise RuntimeError('prealign-skip:no-match')
        token_ops_block = {
            'type': 'timing_adjust',
            'segment_start': start_seg,
            'segment_end': end_seg,
            'clip_start': ss,
            'clip_end': to,
            'items': [{'word_index': wi, 'new_start': start, 'new_end': end} for (start, end, wi) in updates],
            'service': 'silence-remover',
        }
    except Exception as exc:
        utils.log_info(f"[ALIGN] prealign skipped: {str(exc)}")
        updates = []
        token_ops_block = None
    return updates, token_ops_block
