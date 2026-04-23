from src.config.connectionPostgres import get_connection

conn = get_connection()
print("conexión establecida:", conn)


from src.utils.downloadKaggle import download_sales_data

download_sales_data()


from src.scripts.eda import load_data

file_path = "data/sales_data.csv"
df = load_data(file_path)
print("Datos cargados exitosamente:")   
print(df.head())