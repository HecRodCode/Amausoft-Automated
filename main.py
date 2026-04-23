from src.config import get_connection

conn = get_connection()
print("conexión establecida:", conn)


from src.utils import download_sales_data

download_sales_data()