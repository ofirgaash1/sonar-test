from flask import Flask
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import orjson
from .services.analytics_service import AnalyticsService
import os
import threading
import atexit
from flask import request
from dotenv import load_dotenv, dotenv_values 
from flask_oauthlib.client import OAuth
from .services.index import IndexManager
from .services.search import SearchService

load_dotenv() 

class DuplicateLogFilter(logging.Filter):
    """Filter that suppresses consecutive duplicate log records and reports counts."""

    def __init__(self, notice_threshold: int = 100):
        super().__init__()
        self._notice_threshold = max(1, notice_threshold)
        self._lock = threading.RLock()
        self._last_key = None
        self._last_record = None
        self._duplicate_count = 0

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "_duplicate_notice", False):
            return True

        notice_payload = None
        allow_record = True
        message_key = (record.name, record.levelno, record.getMessage())

        with self._lock:
            if self._last_key is None:
                self._last_key = message_key
                self._last_record = record
                self._duplicate_count = 0
            elif message_key == self._last_key:
                self._duplicate_count += 1
                allow_record = False
                if self._duplicate_count >= self._notice_threshold:
                    notice_payload = (self._last_record, self._notice_threshold)
                    self._duplicate_count = 0
            else:
                if self._duplicate_count:
                    notice_payload = (self._last_record, self._duplicate_count)
                self._last_key = message_key
                self._last_record = record
                self._duplicate_count = 0

        if notice_payload:
            self._emit_notice(*notice_payload)

        return allow_record

    def configure(self, notice_threshold: int) -> None:
        if notice_threshold < 1:
            return
        with self._lock:
            self._notice_threshold = notice_threshold

    def flush_pending(self) -> None:
        notice_payload = None
        with self._lock:
            if self._duplicate_count and self._last_record:
                notice_payload = (self._last_record, self._duplicate_count)
                self._duplicate_count = 0
                self._last_key = None
                self._last_record = None

        if notice_payload:
            self._emit_notice(*notice_payload)

    def _emit_notice(self, record: logging.LogRecord, count: int) -> None:
        logging.getLogger(record.name).log(
            record.levelno,
            "[DUPLICATE] Suppressed %d duplicates of: %s",
            (count, record.getMessage()),
            extra={"_duplicate_notice": True},
        )


_duplicate_log_filter = DuplicateLogFilter()
atexit.register(_duplicate_log_filter.flush_pending)

def create_app(data_dir: str, index_file: str = None):
    app = Flask(__name__)

    # --- Logging Setup ---
    # Use a custom formatter to include extra data if we ever need structured (JSON) logs


    # Allow log level override via env (default INFO)
    _lv_name = os.environ.get('LOG_LEVEL') or os.environ.get('EXPLORE_LOG_LEVEL') or 'INFO'
    _LEVEL = getattr(logging, str(_lv_name).upper(), logging.INFO)

    # Clear any existing handlers on the root logger
    if app.logger.hasHandlers():
        app.logger.handlers.clear()
    
    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler("app.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    
    # Create formatters and add it to handlers
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(console_formatter) # Simple format for now

    raw_duplicate_notice = os.environ.get('LOG_DUPLICATE_NOTICE_EVERY')
    duplicate_notice_every = None
    invalid_duplicate_notice = False
    if raw_duplicate_notice:
        try:
            duplicate_notice_every = int(raw_duplicate_notice)
            if duplicate_notice_every < 1:
                raise ValueError
        except ValueError:
            invalid_duplicate_notice = True
            duplicate_notice_every = None

    if duplicate_notice_every:
        _duplicate_log_filter.configure(duplicate_notice_every)

    # Add handlers to the logger
    app.logger.addHandler(console_handler)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(_LEVEL)

    # Also configure the root logger to capture dependencies' logs
    root_logger = logging.getLogger()
    root_logger.setLevel(_LEVEL)
    if not root_logger.hasHandlers():
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

    # Temporarily disable duplicate filter to see actual errors
    # for target_logger in (app.logger, root_logger):
    #     if _duplicate_log_filter not in target_logger.filters:
    #         target_logger.addFilter(_duplicate_log_filter)

    if invalid_duplicate_notice:
        app.logger.warning(
            "LOG_DUPLICATE_NOTICE_EVERY must be a positive integer; ignoring %r",
            raw_duplicate_notice,
        )

    app.logger.info(f"Flask logger configured with level {_lv_name}")
    
    # --- End Logging Setup ---
    
    # Configure paths
    app.config['DATA_DIR'] = data_dir
    app.config['AUDIO_DIR'] = Path(data_dir) / "audio"
    # Transcripts (JSON/GZ) live under data/json
    app.config['TRANSCRIPTS_DIR'] = Path(data_dir) / "json"
    app.config['INDEX_FILE'] = index_file
    # Unified SQLite path under data dir (used by transcripts/confirmations)
    app.config['SQLITE_PATH'] = str(Path(data_dir) / 'explore.sqlite')
        
    # Configure PostHog
    app.config['POSTHOG_API_KEY'] = os.environ.get('POSTHOG_API_KEY', '')
    app.config['POSTHOG_HOST'] = os.environ.get('POSTHOG_HOST', 'https://app.posthog.com')
    app.config['DISABLE_ANALYTICS'] = os.environ.get('DISABLE_ANALYTICS', '').lower() in ('true', '1', 'yes')
    
    # Initialize analytics service
    analytics_service = AnalyticsService(
        api_key=app.config['POSTHOG_API_KEY'],
        host=app.config['POSTHOG_HOST'],
        disabled=app.config['DISABLE_ANALYTICS']
    )
    app.config['ANALYTICS_SERVICE'] = analytics_service
    
    app.config['MIME_TYPES'] = {'opus': 'audio/opus'}

    # Alignment and logging configuration (env overridable)
    def _env_bool(name: str, default: bool) -> bool:
        v = os.environ.get(name)
        if v is None:
            return default
        return str(v).strip().lower() not in ('false', '0', 'no', 'off')

    app.config['ALIGN_PREALIGN_ON_SAVE'] = _env_bool('ALIGN_PREALIGN_ON_SAVE', True)
    app.config['AUDIO_LOG_DIR'] = os.environ.get('AUDIO_LOG_DIR')
    app.config['AUDIO_LOG_NATIVE'] = _env_bool('AUDIO_LOG_NATIVE', True)
    app.config['ALIGN_ENDPOINT'] = os.environ.get('ALIGN_ENDPOINT', 'http://silence-remover.com:8000/align')
    
    # Set secret key for session
    app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key')
   
    # Initialize Google OAuth (Authlib preferred). Enable if credentials are set, or in non-dev envs.
    from .routes.auth import init_oauth, bp as auth_bp
    google = None
    try:
        if (os.environ.get('GOOGLE_CLIENT_ID') and os.environ.get('GOOGLE_CLIENT_SECRET')) or os.environ.get('FLASK_ENV') != 'development':
            google = init_oauth(app)

    except Exception:
        google = None
    if google:
        app.extensions['google_oauth'] = google
 
    # Build audio index (local-only) for fast, robust resolution
    try:
        from .utils import build_audio_index
        audio_idx = build_audio_index(str(app.config['AUDIO_DIR']))
        app.config['AUDIO_INDEX'] = audio_idx
    except Exception:
        # Non-fatal: resolver will fall back to globbing
        app.config['AUDIO_INDEX'] = {}

    # Register blueprints
    from .routes import main, search, auth, export, audio
    from .routes import transcripts
    from .routes import browser
    from .routes import frontend as frontend_static
    app.register_blueprint(main.bp)
    app.register_blueprint(search.bp)
    app.register_blueprint(auth.bp)
    app.register_blueprint(export.bp)
    app.register_blueprint(audio.bp)
    app.register_blueprint(transcripts.bp)
    app.register_blueprint(browser.bp)
    app.register_blueprint(frontend_static.bp)
    
    # Centralized CORS for API responses when needed
    allowed = os.environ.get('ALLOWED_ORIGINS', 'http://localhost:5000,null')
    try:
        allowed_set = set([o.strip() for o in allowed.split(',') if o.strip()])
    except Exception:
        allowed_set = set()
    app.config['ALLOWED_ORIGINS'] = allowed_set

    @app.after_request
    def _apply_cors(resp):
        try:
            origin = request.headers.get('Origin')
            if origin and origin in app.config.get('ALLOWED_ORIGINS', set()):
                resp.headers['Access-Control-Allow-Origin'] = origin
                resp.headers['Vary'] = (resp.headers.get('Vary', '') + ', Origin').strip(', ')
                resp.headers['Access-Control-Allow-Credentials'] = 'true'
                resp.headers.setdefault('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
                resp.headers.setdefault('Access-Control-Allow-Headers', 'Content-Type')
        except Exception:
            pass
        return resp

    @app.before_request
    def _handle_preflight():
        # Respond to CORS preflight early if origin is allowed
        if request.method == 'OPTIONS':
            origin = request.headers.get('Origin')
            if origin and origin in app.config.get('ALLOWED_ORIGINS', set()):
                from flask import make_response
                resp = make_response('', 204)
                resp.headers['Access-Control-Allow-Origin'] = origin
                resp.headers['Vary'] = (resp.headers.get('Vary', '') + ', Origin').strip(', ')
                resp.headers['Access-Control-Allow-Credentials'] = 'true'
                resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
                resp.headers['Access-Control-Allow-Headers'] = request.headers.get('Access-Control-Request-Headers', 'Content-Type')
                return resp
        return None

    # Ensure SQLite file handles are released between tests/requests (Windows tempdir cleanup)
    from .services.db import DatabaseService

    # Avoid closing all DB connections on each request in threaded dev server,
    # as it may race and close connections used by concurrent handlers.
    # Cleanup is handled on process exit or by tests explicitly when needed.

    return app

def init_index_manager(app, file_records=None, index_file=None, **db_kwargs):
    """Initialize the index manager with the given parameters.
    
    Args:
        app: Flask application instance
        file_records: Optional list of FileRecord objects
        index_file: Optional path to index file
        force_reindex: Whether to force rebuilding the index
        **db_kwargs: Database-specific connection parameters
    """
    # Set default database parameters if not provided
    if not db_kwargs:
        db_kwargs = {
            "path": app.config.get('SQLITE_PATH', os.environ.get('SQLITE_PATH', 'explore.sqlite'))
        }
    
    if index_file:
        # Load from flat index file
        index_mgr = IndexManager(index_path=index_file, **db_kwargs)
    elif file_records:
        # Build index from files
        index_mgr = IndexManager(file_records=file_records, **db_kwargs)
    else:
        raise ValueError("Either file_records or index_file must be provided")
    
    app.config['SEARCH_SERVICE'] = SearchService(index_mgr)
    return index_mgr

def register_error_handlers(app):
    @app.errorhandler(404)
    def handle_not_found(e):
        analytics = app.config.get('ANALYTICS_SERVICE')
        if analytics:
            analytics.capture_error('not_found', str(e))
        return 'Page not found', 404
        
    @app.errorhandler(500)
    def handle_server_error(e):
        analytics = app.config.get('ANALYTICS_SERVICE')
        if analytics:
            analytics.capture_error('server_error', str(e))
        return 'Internal server error', 500
