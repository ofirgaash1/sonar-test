from flask import current_app
from . import bp
import os
from pathlib import Path

@bp.route('/testing/reset-db', methods=['POST'])
def reset_db():
    """
    Endpoint to reset the database for testing purposes.
    Deletes and recreates the database file.
    """
    db_path = Path(current_app.config['SQLITE_PATH'])
    
    # Close the existing database connection if it exists
    if 'db_conn' in current_app.extensions:
        current_app.extensions['db_conn'].close()
        del current_app.extensions['db_conn']

    if db_path.exists():
        db_path.unlink()
    
    db_shm_path = db_path.with_suffix('.sqlite-shm')
    if db_shm_path.exists():
        db_shm_path.unlink()
        
    db_wal_path = db_path.with_suffix('.sqlite-wal')
    if db_wal_path.exists():
        db_wal_path.unlink()

    # Re-initialize the database
    from ..services.db import init_db
    with current_app.app_context():
        init_db()

    return 'OK', 200
