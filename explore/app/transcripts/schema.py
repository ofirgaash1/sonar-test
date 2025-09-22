"""Database schema management for transcript storage."""
from __future__ import annotations

from ..services.db import DatabaseService

_TARGET_SCHEMA_VERSION = 3


def _get_user_version(db: DatabaseService) -> int:
    try:
        cur = db.execute("PRAGMA user_version")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def _set_user_version(db: DatabaseService, version: int) -> None:
    db.execute(f"PRAGMA user_version = {int(version)}")


def _table_exists(db: DatabaseService, name: str) -> bool:
    cur = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [name])
    return cur.fetchone() is not None


def _column_exists(db: DatabaseService, table: str, column: str) -> bool:
    try:
        cur = db.execute(f"PRAGMA table_info({table})")
        for row in cur.fetchall() or []:
            if len(row) >= 2 and str(row[1]).lower() == column.lower():
                return True
    except Exception:
        pass
    return False


def ensure_schema(db: DatabaseService) -> None:
    """Create or upgrade the SQLite schema as needed (idempotent)."""
    current = _get_user_version(db)

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
        _set_user_version(db, 1)
        current = 1

    if current < 2:
        if not _column_exists(db, "transcripts", "created_by"):
            db.execute("ALTER TABLE transcripts ADD COLUMN created_by TEXT")
        if not _column_exists(db, "transcripts", "created_at"):
            db.execute("ALTER TABLE transcripts ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if not _column_exists(db, "transcript_edits", "created_at"):
            db.execute("ALTER TABLE transcript_edits ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if not _column_exists(db, "transcript_words", "probability"):
            db.execute("ALTER TABLE transcript_words ADD COLUMN probability DOUBLE")
        _set_user_version(db, 2)
        current = 2

    if current < 3:
        if not _table_exists(db, "transcript_confirmations"):
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
        _set_user_version(db, 3)
