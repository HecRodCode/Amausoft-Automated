import pandas as pd
from psycopg2.extras import execute_values
import os

class DataLoader:
  def __init__(self):
    pass

  def bulk_insert(self, conn, file_path, table_name):
    """Insert data securely and in bulk from a CSV file"""
    if not os.path.exists(file_path):
      print(f"[DB] Error: File {file_path} not found.", flush=True)
      return False

    try:
      df = pd.read_csv(file_path, encoding="utf-8")
      df.columns = [c.strip().lower() for c in df.columns]

      columns = ", ".join(df.columns)
      query = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT DO NOTHING"

      with conn.cursor() as cur:
        values = df.where(pd.notnull(df), None).values.tolist()

        execute_values(cur, query, values)
        conn.commit()

      print(f"[DB] Success: {len(df)} rows loaded into '{table_name}'", flush=True)
      return True

    except Exception as e:
      if conn:
        conn.rollback()
      print(f"[DB] Error loading {table_name} from {file_path}: {e}", flush=True)
      return False


data_loader = DataLoader()