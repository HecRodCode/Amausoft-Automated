from fastapi import APIRouter
from fastapi.responses import FileResponse
from src.services.clients_service import clients_service
import os

router = APIRouter()

@router.get("/data")
async def get_clients_data():
    return clients_service.external_data

@router.get("/download-csv")
async def download_csv():
    """Download the accumulated dataset"""
    if os.path.exists(clients_service.csv_path):
        return FileResponse(path=clients_service.csv_path, filename="clients_dataset.csv")
    return {"error": "File not found"}