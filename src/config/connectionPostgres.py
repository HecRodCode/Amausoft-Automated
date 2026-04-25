import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "amausoft"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "admin"),
        port=os.getenv("DB_PORT", "5432")
    )