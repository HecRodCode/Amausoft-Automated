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
from src.scripts.transformation import sales_etl
from src.config.connectionPostgres import get_connection
from src.utils.downloadKaggle import download_sales_data


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- Amausoft Automated API: System Boot ---", flush=True)

    # Connection DB
    conn = None
    try:
        conn = get_connection()
        print(f"DB Connected: {conn}", flush=True)
    except Exception as e:
        print(f"DB Connection Failed: {e}", flush=True)

    async def master_orchestrator():
        await asyncio.sleep(1)

        try:
            await asyncio.to_thread(sales_etl.run_pipeline)


            print("ETL Ready. Activating Microservices...", flush=True)
            asyncio.create_task(clients_service.fetch_clients_periodically())
            asyncio.create_task(regions_service.update_regions_dataset())

        except Exception as e:
            print(f"Orchestrator Error: {e}", flush=True)

    asyncio.create_task(master_orchestrator())

    yield

    if conn: conn.close()
    print('System Shutdown Complete')


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
