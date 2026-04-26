import pandas as pd
import numpy as np
import os
from src.config.connectionPostgres import get_connection

class DataModeler:
  def __init__(self):
    self.raw_data_dir = "data/"
    self.output_dir = "data/processed/"
    os.makedirs(self.output_dir, exist_ok=True)

  def create_tables(self):
    """Create the physical structure in PostgreSQL"""
    commands = [
      """
      CREATE TABLE IF NOT EXISTS dim_regions (
          region_id SERIAL PRIMARY KEY,
          country_name VARCHAR(100) UNIQUE,
          continent VARCHAR(100),
          sub_region VARCHAR(100),
          latitude FLOAT,
          longitude FLOAT
      );
      """,
      """
      CREATE TABLE IF NOT EXISTS dim_products (
          product_id SERIAL PRIMARY KEY,
          product_code VARCHAR(50) UNIQUE,
          category VARCHAR(100)
      );
      """,
      """
      CREATE TABLE IF NOT EXISTS dim_clients (
          client_id SERIAL PRIMARY KEY,
          name VARCHAR(150),
          city VARCHAR(100),
          country_name VARCHAR(100) REFERENCES dim_regions(country_name),
          email VARCHAR(150),
          fetch_timestamp TIMESTAMP
      );
      """,
      """
      CREATE TABLE IF NOT EXISTS fact_sales (
          order_id SERIAL PRIMARY KEY,
          order_number INT,
          client_id INT REFERENCES dim_clients(client_id),
          product_id INT REFERENCES dim_products(product_id),
          quantity INT,
          price DECIMAL(10,2),
          total_sale DECIMAL(10,2),
          date DATE
      );
      """
    ]
    conn = get_connection()
    try:
      with conn.cursor() as cur:
        for command in commands:
          cur.execute(command)
      conn.commit()
      print("[DB] Schema created/verified successfully.")
    except Exception as e:
      print(f"[DB] Error creating schema: {e}")
    finally:
      conn.close()

  def process_star_schema(self):
    """Reads files from data/, applies logic and saves to data/processed/"""
    print("[Model] Starting Star Schema Transformation...")

    # Loading raw data
    df_sales = pd.read_csv(f"{self.raw_data_dir}sales_data_sample.csv", encoding="ISO-8859-1")
    df_clients = pd.read_csv(f"{self.raw_data_dir}clients_dataset.csv")
    df_regions = pd.read_csv(f"{self.raw_data_dir}regions_master.csv")

    # Process Products
    dim_products = df_sales[['PRODUCTCODE', 'PRODUCTLINE']].drop_duplicates().reset_index(drop=True)
    dim_products.columns = ['product_code', 'category']
    dim_products.insert(0, 'product_id', range(1, len(dim_products) + 1))

    # Process Regions (Geographic Enrichment)
    dim_regions = df_regions.copy()
    dim_regions.insert(0, 'region_id', range(1, len(dim_regions) + 1))

    # Process Customers
    dim_clients = df_clients.copy()
    dim_clients = dim_clients.rename(columns={'client_id': 'id_original'})

    # 4. FACT_SALES
    fact_sales = pd.merge(df_sales, dim_products, left_on='PRODUCTCODE', right_on='product_code')

    print("[Model] Linking customers to sales by country consistency...")

    def assign_client_id(row):
      pais_venta = row['COUNTRY'].upper()
      clientes_del_pais = dim_clients[dim_clients['country'].str.upper() == pais_venta]['id_original']
      if not clientes_del_pais.empty:
        return np.random.choice(clientes_del_pais.values)
      return np.random.choice(dim_clients['id_original'].values)

    fact_sales['client_id'] = fact_sales.apply(assign_client_id, axis=1)

    # Final cleaning of the Fact Table
    fact_sales_final = fact_sales[[
      'ORDERNUMBER', 'client_id', 'product_id',
      'QUANTITYORDERED', 'PRICEEACH', 'SALES', 'ORDERDATE'
    ]].rename(columns={
      'ORDERNUMBER': 'order_number',
      'QUANTITYORDERED': 'quantity',
      'PRICEEACH': 'price',
      'SALES': 'total_sale',
      'ORDERDATE': 'date'
    })

    # Saved in processed directory
    dim_products.to_csv(f"{self.output_dir}dim_products.csv", index=False)
    dim_regions.to_csv(f"{self.output_dir}dim_regions.csv", index=False)
    dim_clients.to_csv(f"{self.output_dir}dim_clients.csv", index=False)
    fact_sales_final.to_csv(f"{self.output_dir}fact_sales.csv", index=False)

    print(f"[Model] Star model saved in {self.output_dir}")
    return True

modeler = DataModeler()