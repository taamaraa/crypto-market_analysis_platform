import os

from dotenv import load_dotenv

load_dotenv()


DB_CONN = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "raw_data"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
    "sslmode": os.getenv("DB_SSLMODE", "prefer"),
}

ALPHA_API_KEY = os.getenv("ALPHA_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")