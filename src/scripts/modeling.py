import pandas as pd
import os
import numpy as np

class DataModeler:
  def __init__(self):
    self.output_dir = "data/processed/"
    os.makedirs(self.output_dir, exist_ok=True)

  def build_star_schema(self, df_sales, df_clients, df_regions):
    print("[Model] Starting Star Schema Modeling...")

    # 1. DIM_PRODUCTS
    dim_products = df_sales[['PRODUCTCODE', 'PRODUCTLINE']].drop_duplicates().reset_index(drop=True)
    dim_products.insert(0, 'id_product', range(1, len(dim_products) + 1))
    dim_products = dim_products.rename(columns={'PRODUCTCODE': 'product_name', 'PRODUCTLINE': 'category'})

    # 2. DIM_REGIONS
    dim_regions = df_regions.copy()
    if 'id_region' not in dim_regions.columns:
      dim_regions.insert(0, 'id_region', range(1, len(dim_regions) + 1))

    # 3. DIM_CLIENTS
    dim_clients = pd.merge(
      df_clients,
      dim_regions[['id_region', 'country_name']],
      left_on='country', right_on='country_name', how='left'
    )
    dim_clients = dim_clients[['client_id', 'id_region', 'name', 'email', 'city', 'country']]
    dim_clients = dim_clients.rename(columns={'client_id': 'id_client'})

    # 4. FACT_SALES
    fact_sales = pd.merge(df_sales, dim_products, left_on='PRODUCTCODE', right_on='product_name')

    print("[Model] Linking customers by country consistency...")

    def assign_client_id(row):
      pais_venta = row['COUNTRY'].upper()
      clientes_del_pais = dim_clients[dim_clients['country'].str.upper() == pais_venta]['id_client']
      if not clientes_del_pais.empty:
        return np.random.choice(clientes_del_pais.values)
      return np.nan

    fact_sales['id_client'] = fact_sales.apply(assign_client_id, axis=1)

    # Final cleaning of the Fact Table
    fact_sales_final = fact_sales[[
      'ORDERNUMBER', 'id_client', 'id_product',
      'QUANTITYORDERED', 'PRICEEACH', 'SALES', 'ORDERDATE'
    ]].dropna(subset=['id_client']).rename(columns={
      'ORDERNUMBER': 'order_number',
      'QUANTITYORDERED': 'quantity',
      'PRICEEACH': 'price',
      'SALES': 'total_sale',
      'ORDERDATE': 'date'
    })

    # Saving processed files
    dim_products.to_csv(f"{self.output_dir}dim_products.csv", index=False)
    dim_regions.to_csv(f"{self.output_dir}dim_regions.csv", index=False)
    dim_clients.to_csv(f"{self.output_dir}dim_clients.csv", index=False)
    fact_sales_final.to_csv(f"{self.output_dir}fact_sales.csv", index=False)

    print(f"[Model] Star model saved in {self.output_dir}")

modeler = DataModeler()