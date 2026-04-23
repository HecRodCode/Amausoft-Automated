import pandas as pd


def transform_data():
    df = pd.read_csv("data/sales_data_sample.csv", encoding="latin1")

    columnas = ["ORDERNUMBER", "ORDERDATE", "PRODUCTCODE", "QUANTITYORDERED", "PRICEEACH", "SALES","COUNTRY"]
    df_filtrado = df[columnas]

    df_filtrado.to_csv("data/sales_clean.csv", index=False)
    return df_filtrado