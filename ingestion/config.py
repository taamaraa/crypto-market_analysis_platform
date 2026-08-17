import yaml
import os
import socket

config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# Автоматски детектира дали е во Docker или Windows
def is_docker():
    try:
        socket.gethostbyname('analytics_db')
        return True
    except:
        return False

DB_CONN = {
    'host': 'analytics_db' if is_docker() else 'localhost',
    'port': 5432 if is_docker() else 5433,
    'dbname': config['database']['dbname'],
    'user': config['database']['user'],
    'password': config['database']['password'],
}

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')