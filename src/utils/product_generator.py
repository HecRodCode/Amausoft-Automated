import pandas as pd
import os

def generate_products_dataset(sales_path="/opt/airflow/data/sales_data_sample.csv",
                              output_path="/opt/airflow/data/products_dataset.csv"):
  print("[Server] Generating Products Dataset from Sales...", flush=True)

  if not os.path.exists(sales_path):
    print(f"[Error] Sales file not found at {sales_path}")
    return

  #  Read Sales CSV file
  df_sales = pd.read_csv(sales_path, encoding='latin1')

  # Extract unique values
  df_products = df_sales[['PRODUCTCODE', 'PRODUCTLINE']].drop_duplicates()

  # Rename columns
  df_products = df_products.rename(columns={
    'PRODUCTCODE': 'product_id',
    'PRODUCTLINE': 'category'
  })

  # Create product_name column (using the ID as the base name)
  df_products['product_nane'] = df_products['product_id']

  # Reorder columns
  df_products = df_products[['product_id', 'product_nane', 'category']]

  # Saved new Dataset
  df_products.to_csv(output_path, index=False)
  print(f"[Server] Products dataset created: {len(df_products)} unique products found.", flush=True)