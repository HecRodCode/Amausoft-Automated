import pandas as pd
from psycopg2.extras import execute_values
import logging


class DataLoader:
  def __init__(self):
    pass

  def bulk_insert(self, conn, file_path, table_name):
    """Insert data securely and in bulk"""
    try:
      df = pd.read_csv(file_path, encoding="ISO-8859-1")

      # Clean names of the columns
      df.columns = [c.strip().lower() for c in df.columns]

      columns = ", ".join(df.columns)
      query = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT DO NOTHING"

      with self.conn.cursor() as cur:
        execute_values(cur, query, df.values.tolist())
        conn.commit()

      print(f"[DB] {len(df)} rows loaded into '{table_name}'", flush=True)
      return True

    except Exception as e:
      if conn:
        conn.rollback()
      print(f"[DB] Error loading {file_path}: {e}", flush=True)
      return False

data_loader = DataLoader()