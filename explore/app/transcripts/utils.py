import hashlib
import logging
import os
import re
from typing import Optional

from flask import abort

logger = logging.getLogger("app.routes.transcripts")


def sha256_hex(value: str) -> str:
    """Return SHA-256 hex digest for the provided value (empty-safe)."""
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def clamp_neighbors(value: int) -> int:
    """Clamp neighbor window size to the supported [0, 3] range."""
    try:
        n_val = int(value)
    except Exception:
        n_val = 1
    if n_val < 0:
        return 0
    if n_val > 3:
        return 3
    return n_val


def log_info(message: str, data: Optional[dict] = None) -> None:
    """Thin wrapper around logger.info that never raises."""
    try:
        if data:
            logger.info(message, extra={"data": data})
        else:
            logger.info(message)
    except Exception:
        pass


def ensure_safe_doc(doc: str) -> None:
    """Abort with 400 if the provided document identifier is unsafe."""
    cleaned = (doc or "").strip()
    if not cleaned:
        abort(400, "invalid doc")
    if "\x00" in cleaned:
        abort(400, "invalid doc")
    if cleaned.startswith("/") or cleaned.startswith("\\") or re.match(r"^[A-Za-z]:[\\/]", cleaned):
        abort(400, "invalid doc")
    parts_slash = [p for p in cleaned.split("/") if p]
    parts_backslash = [p for p in cleaned.split("\\") if p]
    if any(p == ".." for p in parts_slash) or any(p == ".." for p in parts_backslash):
        abort(400, "invalid doc")


def safe_name(value: str) -> str:
    """Produce a filesystem-safe token for logging artifacts."""
    try:
        text = str(value or "")
        text = text.replace(os.sep, "__").replace("/", "__")
        text = " ".join(text.split())
        return "".join(ch if ch.isalnum() or ch in ("_", "-", ".", "#") else "_" for ch in text)
    except Exception:
        return "unknown"
