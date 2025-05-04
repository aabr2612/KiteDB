import os

# Base paths
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DB_ROOT = os.path.join(ROOT_DIR, '..', 'db')
LOG_ROOT = os.path.join(DB_ROOT, 'logs')
DATA_ROOT = os.path.join(DB_ROOT, 'data')

# Storage settings
CHUNK_SIZE = 1000