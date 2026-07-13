import os
import socket


def is_docker():
    """
    Auto-detects whether we're running inside the Docker network (where the
    'raw-db' service name resolves) or locally on the host machine (where it
    doesn't). Avoids having to manually flip DB_HOST/DB_PORT back and forth
    when testing ingest.py locally vs. running it through Airflow.
    """
    try:
        socket.gethostbyname('raw-db')
        return True
    except socket.error:
        return False


DB_CONN = {
    "host": "raw-db" if is_docker() else os.getenv("DB_HOST", "localhost"),
    "port": 5432 if is_docker() else int(os.getenv("DB_PORT", "5434")),
    "dbname": os.getenv("DB_NAME", "raw_data"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}

ALPHA_API_KEY = os.getenv("ALPHA_API_KEY")

COINS = ['bitcoin', 'ethereum', 'solana', 'binancecoin', 'cardano']
BINANCE_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'ADAUSDT']
ALPHA_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "TSLA",
    "AMZN"
]