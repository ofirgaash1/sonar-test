import os
from pathlib import Path
from app import create_app

# Determine data directory from ENV or fall back to ../data
data_dir = os.environ.get('DATA_DIR')
if not data_dir:
    data_dir = str((Path(__file__).resolve().parent.parent / 'data'))

app = create_app(data_dir=os.path.abspath(data_dir))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
