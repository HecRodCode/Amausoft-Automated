from src.services.clients_service import clients_service
from src.config.connectionPostgres import get_connection
from src.scripts.eda import load_data

from src.scripts.transformation import transform_data, transform_data_date,transform_data_eliminated_duplicate

from src.scripts.eda import basic_eda


from src.utils.downloadKaggle import download_sales_data
from fastapi import FastAPI

conn = get_connection()
print("conexión establecida:", conn)
download_sales_data()

file_path ="data/sales_data_sample.csv"
df = load_data(file_path)

print("Datos cargados exitosamente:")
df = transform_data(df)

basic_eda(df)

df = transform_data_date(df)

basic_eda(df)


df = transform_data_eliminated_duplicate(df)

basic_eda(df)




# Init FastAPI
app = FastAPI(
    title="Amausoft Automated API",
    description="ETL and Automation project by Camilo & Hector",
    version="1.0.0"
)

# Health Check
@app.get("/")
def read_root() -> dict:
    return {"status": "Project is running", "team": ["Camilo Guengue", "Hector Rios"]}

app.include_router(clients_router, prefix="/clients", tags=["Clients"])
