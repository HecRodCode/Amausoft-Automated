from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# -- Imports de tus módulos --
from src.scripts.eda_sales import SalesExplorer
from src.scripts.transformation import sales_etl
from src.database.models import modeler
from src.database.loader import data_loader
from src.config.connectionPostgres import get_connection


def run_full_pipeline():
  # Extracción y Limpieza (ETL)
  print("[ETL] Extracting data from Kaggle...", flush=True)
  df_raw = sales_etl.extract()

  # Análisis Exploratorio (EDA)
  print("[EDA] Analyzing data quality...", flush=True)
  explorer = SalesExplorer(df_raw)  # Pasamos el DF al constructor
  explorer.run_professional_eda()

  # ETL process
  print("[ETL] Cleaning and transforming data...", flush=True)
  df_sales_clean = sales_etl.transform(df_raw)
  sales_etl.load(df_sales_clean)

  # Star Modeling and Integration
  print("[Modeling] Building a Star Model...", flush=True)
  success_modeling = modeler.process_star_schema()

  if not success_modeling:
    raise Exception("Data modeling failure. Pipeline aborted.")

  # Creating Tables and Loading into Database
  print("[DB] Verifying schema and loading into PostgreSQL...", flush=True)

  # We ensure that the tables exist before loading
  modeler.create_tables()

  conn = get_connection()
  try:
    tables = ["dim_regions", "dim_products", "dim_clients", "fact_sales"]

    for table in tables:
      csv_path = f"data/processed/{table}.csv"
      if os.path.exists(csv_path):
        data_loader.bulk_insert(conn, csv_path, table)
      else:
        print(f"[DB] Warning: Not found {csv_path}")

    print("[DAG] Success: Integration completed and persisted in Postgres", flush=True)
  except Exception as e:
    print(f"[DAG] Error during DB upload: {e}", flush=True)
    raise e
  finally:
    conn.close()


# Definition of DAG
with DAG(
    dag_id="amausoft_automated_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['data_analytics', 'emausoft_project']
) as dag:
  task_full_process = PythonOperator(
    task_id="full_data_orchestration",
    python_callable=run_full_pipeline
  )