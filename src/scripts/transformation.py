import pandas as pd
import os
from src.utils.downloadKaggle import download_sales_data

class SalesETL:
    def __init__(self, raw_path="data/sales_data_sample.csv", clean_path="data/sales_clean.csv"):
        self.raw_path = raw_path
        self.clean_path = clean_path
        self.columns = ["ORDERNUMBER", "ORDERDATE", "PRODUCTCODE", "QUANTITYORDERED", "PRICEEACH", "SALES", "COUNTRY"]

    def extract(self):
        """Downloading and uploading data"""
        download_sales_data()
        if not os.path.exists(self.raw_path):
            raise FileNotFoundError(f"No se encontró el archivo en {self.raw_path}")
        return pd.read_csv(self.raw_path, encoding="ISO-8859-1")

    def transform(self, df):
        """Cleaning and standardization"""
        df_final = df[self.columns].copy()
        df_final["ORDERDATE"] = pd.to_datetime(df_final["ORDERDATE"], errors="coerce").dt.normalize()
        return df_final

    def load(self, df):
        """Data persistence"""
        os.makedirs(os.path.dirname(self.clean_path), exist_ok=True)
        df.to_csv(self.clean_path, index=False, encoding='utf-8')
        return self.clean_path

    def run_pipeline(self):
        """Execute the entire flow in a controlled manner"""
        print(f"[ETL] Starting Pipeline...", flush=True)
        raw_df = self.extract()
        clean_df = self.transform(raw_df)
        path = self.load(clean_df)
        print(f"[ETL] Pipeline finished. File saved in: {path}", flush=True)
        return clean_df

# Single instance to use in the main
sales_etl = SalesETL()
