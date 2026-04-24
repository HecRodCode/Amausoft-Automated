import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

# -- Routes --
from src.routes.clients_route import router as clients_router
from src.routes.regions_route import router as regions_router

# -- Services --
from src.services.clients_service import clients_service
from src.services.regions_service import regions_service
from src.services.carga_datos import cargar_datos

# -- ETL & Utils --
from src.config.connectionPostgres import get_connection
from src.scripts.eda import load_data
from src.scripts.eda import load_data, basic_eda
from src.scripts.transformation import transform_data, transform_data_date,transform_data_products,transform_data_eliminated_duplicate
from src.utils.downloadKaggle import download_sales_data


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Init Amausoft Automated API")

    # Conexión a DB
    conn = get_connection()
    print("Established connection:", conn)

    download_sales_data()
    file_path = "data/sales_data_sample.csv"
    df = load_data(file_path)

    if df is not None:
        df2 = transform_data_products(df)
        cargar_datos("data/products.csv", "products", conn)
        df = transform_data(df)
        df= transform_data_eliminated_duplicate(df)
        df = transform_data_date(df)
        basic_eda(df)
        
    # Background Tasks
    asyncio.create_task(clients_service.fetch_clients_periodically())
    asyncio.create_task(regions_service.update_regions_dataset())

    yield

    if conn:
        conn.close()
    print('Turning off services')


# Init FastAPI
app = FastAPI(
    title="Amausoft Automated API",
    description="ETL and Automation project by Camilo & Hector",
    version="1.0.0",
    lifespan=lifespan
)

# Health Check
@app.get("/")
def read_root() -> dict:
    return {
        "status": "Project is running",
        "team": ["Camilo Guengue", "Hector Rios"],
        "environment": "Ubuntu 24.04"
    }

# Incluide Routes
app.include_router(clients_router, prefix="/clients", tags=["Clients"])
app.include_router(regions_router, prefix="/regions", tags=["Regions"])
