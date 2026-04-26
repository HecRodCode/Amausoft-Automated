import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

# -- Connection Database --
from src.config.connectionPostgres import get_connection
from src.database.loader import data_loader

# -- Routes --
from src.routes.clients_route import router as clients_router
from src.routes.regions_route import router as regions_router

# -- Services --
from src.services.clients_service import clients_service
from src.services.regions_service import regions_service

from src.utils.downloadKaggle import downloadKaggle

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- Amausoft Automated API: System Boot ---", flush=True)

    # Connection DB
    conn = None
    try:
        conn = get_connection()
        app.state.db_conn = conn
        print(f"[DB] Connected: {conn}", flush=True)
    except Exception as e:
        print(f"[DB] Connection Failed: {e}", flush=True)

    print("======================================================")
    print("[API] Starting Kaggle Sync...", flush=True)
    try:
        await asyncio.to_thread(downloadKaggle)
        print("[API] Kaggle Sync Completed Successfully.", flush=True)
    except Exception as e:
        print(f"[API] ERROR en Kaggle Sync: {e}", flush=True)

    print("[API] Creating product dataset based in sales_sample", flush=True)
    try:
        from src.utils.product_generator import generate_products_dataset
        await asyncio.to_thread(generate_products_dataset)
    except Exception as e:
        print(f"[API] Error generating products: {e}", flush=True)

    print("======================================================")
    print("[API] Done. Activating Microservices...", flush=True)

    print("[API] Running initial regions sync...", flush=True)
    await regions_service.update_regions_dataset()

    print("[API] Activating Periodic Client Ingestion...", flush=True)
    asyncio.create_task(clients_service.fetch_clients_periodically())

    yield

    if conn: conn.close()
    print('[Server] System Shutdown Complete')

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