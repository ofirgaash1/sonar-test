from flask import Blueprint, send_file, current_app, request, Response
from ..routes.auth import login_required
from ..utils import resolve_audio_path
import os
import mimetypes
import re
import time
import logging
import uuid
import glob

bp = Blueprint('audio', __name__)
logger = logging.getLogger(__name__)

def _win_long_path(p: str) -> str:
    try:
        if os.name == 'nt':
            p = os.path.abspath(p)
            if not p.startswith('\\\\?\\'):
                return '\\\\?\\' + p
    except Exception:
        pass
    return p


def _first_openable_path(candidates: list[str]) -> Optional[str]:
    for c in candidates:
        try:
            with open(c, 'rb'):
                return c
        except Exception:
            continue
    return None


def _file_size(p: str) -> Optional[int]:
    try:
        with open(p, 'rb') as fh:
            fh.seek(0, os.SEEK_END)
            return fh.tell()
    except Exception:
        try:
            return os.path.getsize(p)
        except Exception:
            return None


def _maybe_follow_pointer(fs_path: str) -> tuple[str, Optional[int]]:
    """If `fs_path` looks like a tiny pointer file, try to follow to blob target.

    Returns (resolved_path, size) where size is the updated file size if found.
    """
    size = _file_size(fs_path)
    try:
        if size is not None and size <= 512:
            with open(fs_path, 'rb') as _pf:
                data = _pf.read(512)
            # best-effort decodes
            text = ''
            for enc in ('utf-8','utf-16','utf-16-le','utf-16-be','latin-1'):
                try:
                    text = data.decode(enc, 'ignore').strip()
                except Exception:
                    text = ''
                if text:
                    break
            if text:
                import re as _re
                m = _re.search(r'([A-Fa-f0-9]{40,64})', text)
                if m:
                    sha = m.group(1)
                    audio_dir = current_app.config.get('AUDIO_DIR')
                    if audio_dir:
                        cand = os.path.join(audio_dir, 'blobs', sha)
                        if os.path.exists(cand):
                            fs_path = cand
                            size = _file_size(fs_path)
    except Exception:
        pass
    return fs_path, size


def _content_type_for(name_for_type: str) -> str:
    ct = mimetypes.guess_type(name_for_type)[0] or 'application/octet-stream'
    try:
        if (name_for_type or '').lower().endswith('.opus'):
            # Use a broadly compatible type for Opus-in-Ogg
            ct = 'audio/ogg; codecs=opus'
    except Exception:
        pass
    return ct


def _parse_range_header(range_header: str, size: int) -> Optional[tuple[int, int]]:
    """Parse a bytes Range header and clamp to file size.

    Returns (start, end) if valid, otherwise None for unsatisfiable.
    """
    m = re.search(r'bytes=(\d+)-(\d*)', range_header or '')
    if not m:
        return None
    byte1 = int(m.group(1))
    byte2 = int(m.group(2)) if m.group(2) else (size - 1)
    if byte1 >= size:
        return None
    if byte2 >= size:
        byte2 = size - 1
    if byte2 < byte1:
        byte2 = byte1
    return byte1, byte2


def _chunk_gen(fs_path: str, start_end: Optional[tuple[int,int]], request_id: Optional[str], size: int):
    chunk_size = 8192
    with open(fs_path, 'rb') as f:
        if start_end is not None:
            byte1, byte2 = start_end
            if request_id:
                logger.info(f"[TIMING] [REQ:{request_id}] Serving range request: bytes {byte1}-{byte2}/{size}")
            f.seek(byte1)
            remaining = (byte2 - byte1 + 1)
            while remaining > 0:
                chunk = f.read(min(chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk
            return
        # Full file
        if request_id:
            logger.info(f"[TIMING] [REQ:{request_id}] Serving full file: {size} bytes")
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk
def send_range_file(path, request_id=None, requested_name=None):
    start_time = time.time()
    if request_id:
        logger.info(f"[TIMING] [REQ:{request_id}] Starting to send file: {path}")

    range_header = request.headers.get('Range', None)
    # Resolve a usable filesystem path by trying long-path and plain variants.
    long_variant = _win_long_path(path)
    plain_variant = os.path.abspath(path)
    fs_path = _first_openable_path([long_variant, plain_variant])
    if not fs_path:
        if request_id:
            logger.error(
                f"[TIMING] [REQ:{request_id}] File not found. long='{long_variant}' plain='{plain_variant}'"
            )
        return "File not found", 404

    # Determine file size using a robust method
    size = _file_size(fs_path)
    if size is None:
        if request_id:
            logger.error(f"[TIMING] [REQ:{request_id}] Unable to determine file size for: {fs_path}")
        return "File not found", 404

    # If this is a tiny text pointer, follow to blob target (safety net even if resolver missed it)
    fs_path, size = _maybe_follow_pointer(fs_path)

    # Determine content type using the requested name if available (handles blob targets)
    name_for_type = requested_name or path
    content_type = _content_type_for(name_for_type)

    # Prepare range if requested
    start_end = _parse_range_header(range_header or '', int(size)) if size is not None else None
    if range_header and start_end is None:
        # 416 Range Not Satisfiable
        resp = Response(status=416)
        resp.headers.add('Content-Range', f'bytes */{size}')
        return resp
    if start_end is not None:
        byte1, byte2 = start_end
        length = byte2 - byte1 + 1
        resp = Response(_chunk_gen(fs_path, start_end, request_id, int(size)), 206, mimetype=content_type)
        resp.headers.add('Content-Range', f'bytes {byte1}-{byte2}/{size}')
        resp.headers.add('Accept-Ranges', 'bytes')
        resp.headers.add('Content-Length', str(length))
        if request_id:
            duration_ms = (time.time() - start_time) * 1000
            logger.info(f"[TIMING] [REQ:{request_id}] Range file served in {duration_ms:.2f}ms")
        return resp

    # No Range: return full file
    resp = Response(_chunk_gen(fs_path, None, request_id, int(size)), 200, mimetype=content_type)
    resp.headers.add('Accept-Ranges', 'bytes')
    resp.headers.add('Content-Length', str(size))

    if request_id:
        duration_ms = (time.time() - start_time) * 1000
        logger.info(f"[TIMING] [REQ:{request_id}] Full file served in {duration_ms:.2f}ms")

    return resp

@bp.route('/audio/<path:filename>')
@login_required
def serve_audio(filename):
    request_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    
    try:
        # Log the raw and repr forms to diagnose encoding issues
        logger.info(f"[TIMING] [REQ:{request_id}] Audio request received for: {filename}")
        logger.info(f"[TIMING] [REQ:{request_id}] filename.repr={repr(filename)}")
    except Exception:
        pass

    # Try to recover proper UTF-8 path from raw request URI (handles mojibake and double-encoding)
    src_candidates = []
    try:
        raw_uri = (request.environ.get('RAW_URI')
                   or request.environ.get('REQUEST_URI')
                   or (request.full_path or ''))
        if raw_uri:
            try:
                base = raw_uri.split('?', 1)[0]
                if '/audio/' in base:
                    raw_seg = base.split('/audio/', 1)[1]
                    # Generate multiple decoding variants
                    from urllib.parse import unquote, unquote_to_bytes
                    cand_texts = []
                    try:
                        cand_texts.append(unquote(raw_seg))
                        cand_texts.append(unquote(unquote(raw_seg)))
                    except Exception:
                        pass
                    try:
                        b = unquote_to_bytes(raw_seg)
                        cand_texts.append(b.decode('utf-8', 'ignore'))
                        # Sometimes browsers double-encode; try a second pass
                        cand_texts.append(unquote(b.decode('latin-1', 'ignore')))
                    except Exception:
                        pass
                    for fixed in cand_texts:
                        if fixed and fixed not in src_candidates:
                            src_candidates.append(fixed)
                    if cand_texts:
                        logger.info(f"[TIMING] [REQ:{request_id}] recovered_from_uri_variants={list(map(repr, cand_texts))}")
            except Exception:
                pass
    except Exception:
        pass

    # Add best-effort re-decode of the provided param (route variable)
    try:
        from urllib.parse import unquote, unquote_to_bytes
        # 1) Direct param
        if filename not in src_candidates:
            src_candidates.append(filename)
        # 2) Percent-decoded once/twice
        try:
            dec1 = unquote(filename)
            if dec1 and dec1 not in src_candidates:
                src_candidates.append(dec1)
            dec2 = unquote(dec1)
            if dec2 and dec2 not in src_candidates:
                src_candidates.append(dec2)
        except Exception:
            pass
        # 3) Bytes route (handles malformed % sequences)
        try:
            b = unquote_to_bytes(filename)
            u8 = b.decode('utf-8', 'ignore')
            if u8 and u8 not in src_candidates:
                src_candidates.append(u8)
        except Exception:
            pass
        # 4) latin1->utf8 fallback
        alt = filename.encode('latin-1', 'ignore').decode('utf-8', 'ignore')
        if alt and alt not in src_candidates:
            src_candidates.append(alt)
            logger.info(f"[TIMING] [REQ:{request_id}] latin1->utf8 candidate={repr(alt)}")
    except Exception:
        pass

    # Deduplicate while preserving order
    _seen = set()
    src_candidates = [x for x in src_candidates if not (x in _seen or _seen.add(x))]

    try:
        audio_path = None
        for src in src_candidates:
            audio_path = resolve_audio_path(src)
            if audio_path:
                break
        if audio_path:
            try:
                logger.info(f"[TIMING] [REQ:{request_id}] Streaming local audio: {audio_path}")
                logger.info(f"[TIMING] [REQ:{request_id}] audio_path.repr={repr(audio_path)}")
            except Exception:
                pass
            return send_range_file(audio_path, request_id, requested_name=filename)
        logger.error(f"[TIMING] [REQ:{request_id}] Local audio not found. tried={list(map(repr, src_candidates))}")
        return (f"Audio not found: {filename}", 404)
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(f"[TIMING] [REQ:{request_id}] Error serving audio file {filename}: {str(e)} after {duration_ms:.2f}ms")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}", 404


@bp.route('/debug/audio/resolve')
def debug_audio_resolve():
    # Dev-only helper to inspect resolved path
    if os.environ.get('FLASK_ENV') != 'development':
        return ("Not available", 404)
    folder = request.args.get('folder', '').strip()
    file = request.args.get('file', '').strip()
    if not folder or not file:
        return ("missing folder/file", 400)
    src = f"{folder}/{file}"
    # Provide additional debugging info
    try:
        from ..utils import _norm_text
        decoded = _norm_text(src)
        parts = decoded.split('/') if decoded else []
        key = '/'.join([_norm_text(parts[0]), _norm_text(parts[-1])]) if len(parts) >= 2 else decoded
        idx = (current_app.config or {}).get('AUDIO_INDEX') or {}
        endswith_matches = [k for k in idx.keys() if k.endswith('/' + (parts[-1] if parts else ''))]
        direct = idx.get(key)
        raw_direct = idx.get(src)
    except Exception:
        key = src
        endswith_matches = []
        idx = {}
        direct = None
        raw_direct = None
    path = resolve_audio_path(src)
    meta = {
        "requested": src,
        "computed_key": key,
        "index_size": len(idx),
        "endswith_candidates": endswith_matches[:5],
        "resolved": path or None,
        "direct": direct or None,
        "raw_direct": raw_direct or None
    }
    try:
        probe = path or direct or raw_direct
        if probe and os.path.isfile(probe):
            sz = os.path.getsize(probe)
            meta["resolved_size"] = sz
            if sz <= 512:
                with open(probe, 'rb') as fh:
                    data = fh.read(512)
                meta["resolved_preview_hex"] = data[:64].hex()
                previews = {}
                for enc in ('utf-8','utf-16','utf-16-le','utf-16-be','latin-1'):
                    try:
                        previews[enc] = data.decode(enc, 'ignore')[:120]
                    except Exception:
                        pass
                meta["resolved_preview_text"] = previews
    except Exception:
        pass
    return (meta, 200)


@bp.route('/debug/audio/reindex', methods=['GET','POST'])
def debug_audio_reindex():
    # Dev-only reindexer
    if os.environ.get('FLASK_ENV') != 'development':
        return ("Not available", 404)
    try:
        from ..utils import build_audio_index
        audio_dir = current_app.config.get('AUDIO_DIR')
        if not audio_dir:
            return ("AUDIO_DIR not set", 500)
        idx = build_audio_index(str(audio_dir))
        current_app.config['AUDIO_INDEX'] = idx
        # Return a small sample of keys for verification
        sample = []
        try:
            for i, k in enumerate(sorted(idx.keys())):
                sample.append(k)
                if i >= 4:
                    break
        except Exception:
            pass
        return ({"count": len(idx), "sample": sample}, 200)
    except Exception as e:
        return (str(e), 500)


@bp.route('/debug/audio/scan')
def debug_audio_scan():
    # Dev-only: scan a folder's files and report resolution + sizes
    if os.environ.get('FLASK_ENV') != 'development':
        return ("Not available", 404)
    folder = request.args.get('folder', '').strip()
    if not folder:
        return ("missing folder", 400)
    try:
        from ..utils import _norm_text
        idx = (current_app.config or {}).get('AUDIO_INDEX') or {}
        # Collect files by suffix in index keys that end with this folder
        files = []
        for k in sorted(idx.keys()):
            try:
                if k.startswith(_norm_text(folder) + '/'):
                    files.append(k.split('/', 1)[1])
            except Exception:
                continue
        out = []
        for f in files:
            src = f"{folder}/{f}"
            path = resolve_audio_path(src)
            rec = { 'file': f, 'resolved': path }
            try:
                if path and os.path.isfile(path):
                    sz = os.path.getsize(path)
                    rec['size'] = sz
                    if sz <= 512:
                        with open(path, 'rb') as fh:
                            data = fh.read(256)
                        rec['preview_hex'] = data[:64].hex()
                        try:
                            text = data.decode('utf-8', 'ignore')
                        except Exception:
                            text = ''
                        rec['preview_text'] = text[:120]
            except Exception:
                pass
            out.append(rec)
        return ({ 'folder': folder, 'count': len(out), 'items': out[:200] }, 200)
    except Exception as e:
        return (str(e), 500)
