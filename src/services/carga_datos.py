import os
import pandas as pd
from psycopg2.extras import execute_values

def cargar_datos(file_path,tabla,conn):
    cur = conn.cursor()
    carga = pd.read_csv(file_path, encoding="ISO-8859-1")

    columns = ", ".join(carga.columns)
    placeholders = ", ".join(["%s"] * len(carga.columns))

    query = f"""
    INSERT INTO {tabla} ({columns})
    VALUES %s
    ON CONFLICT DO NOTHING
    """

    execute_values(cur, query, carga.values.tolist())
    conn.commit()
    cur.close()
    print(f"Carga de datos desde '{file_path}' a la tabla '{tabla}' realizada con éxito.")
