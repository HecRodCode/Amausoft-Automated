from fastapi import APIRouter
from src.services.regions_service import regions_service

router = APIRouter()

@router.post("/update")
async def update_regions():
    success = await regions_service.update_regions_dataset()
    if success:
        return {"[Server] message": "regions Dataset updated successfully"}
    return {"[Server] error": "The regions dataset could not be updated"}