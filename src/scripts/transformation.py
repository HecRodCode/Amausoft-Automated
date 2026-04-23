import pandas as pd


def transform_data(df):

    columnas = ["ORDERNUMBER", "ORDERDATE", "PRODUCTCODE", "QUANTITYORDERED", "PRICEEACH", "SALES","COUNTRY"]
    df_filtrado = df[columnas]

    df_filtrado.to_csv("data/sales_clean.csv", index=False)
    return df_filtrado



def transform_data_date(df_filtrado):
    df_filtrado["ORDERDATE"] = pd.to_datetime(df_filtrado["ORDERDATE"], errors="coerce")
    df_filtrado["ORDERDATE"] = df_filtrado["ORDERDATE"].dt.normalize()
    df_filtrado.to_csv("data/sales_clean_date.csv", index=False)
    print("Transformación de fecha realizada y guardada en 'data/sales_clean_date.csv'")
    return df_filtrado

def transform_data_eliminated_duplicate(df_filtrado):
    df_filtrado = df_filtrado.drop_duplicates(subset=["ORDERNUMBER"])
    df_filtrado.to_csv("data/sales_clean_no_duplicates.csv", index=False)
    print("Transformación de eliminación de duplicados realizada y guardada en 'data/sales_clean_no_duplicates.csv'")
    return df_filtrado

def transform_data_products(df):
    df_nuevo = (
        df[["PRODUCTCODE"]]
        .dropna(subset=["PRODUCTCODE"])
        .drop_duplicates(subset=["PRODUCTCODE"])
        .reset_index(drop=True)
        .rename(columns={"PRODUCTCODE": "product_name"})
    )
    df_nuevo.insert(0, "id_product", df_nuevo.index + 1)
    df_nuevo["category"] = "UNKNOWN"

    df_nuevo.to_csv("data/products.csv", index=False)
    print("Transformación de productos realizada y guardada en 'data/products.csv'")
    return df_nuevo





