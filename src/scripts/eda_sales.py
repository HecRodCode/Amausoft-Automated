import pandas as pd

class SalesExplorer:
  def __init__(self, df):
    self.df = df

  def run_professional_eda(self):

    print("\n 1. ORDER AND PRODUCT INTEGRITY")
    print(f"--- Total records: {len(self.df)}")
    print(f"--- Unique orders (ORDERNUMBER): {self.df['ORDERNUMBER'].nunique()}")
    print(f"--- Unique Products (PRODUCTCODE): {self.df['PRODUCTCODE'].nunique()}")
    print(f"--- Product lines: {self.df['PRODUCTLINE'].unique()}")

    # Análisis de Calidad
    print("\n 2. DATA QUALITY")
    nulls = self.df.isnull().sum()
    nulls = nulls[nulls > 0]
    if not nulls.empty:
      print(f"--- Null values detected:\n{nulls}")
    else:
      print("--- No null values were detected.")

    dups = self.df.duplicated().sum()
    print(f"--- Completely duplicate records: {dups}")

    # Análisis Geográfico
    print("\n 3. GEOGRAPHIC COVERAGE")
    country_counts = self.df['COUNTRY'].value_counts().head(5)
    print("--- Top 5 Countries with the most sales:")
    print(country_counts)

    # Estadísticas de Negocio
    print("\n 4. BUSINESS METRICS (SALES)")
    stats = self.df[['QUANTITYORDERED', 'SALES', 'PRICEEACH']].describe().loc[['min', 'mean', 'max']]
    print(stats)

    # Rango Temporal
    if 'ORDERDATE' in self.df.columns:
      # Convertimos temporalmente para ver el rango
      temp_dates = pd.to_datetime(self.df['ORDERDATE'])
      print(f"\n 5. TEMPORARY WINDOW: {temp_dates.min()} until {temp_dates.max()}")


sales_explorer = SalesExplorer()