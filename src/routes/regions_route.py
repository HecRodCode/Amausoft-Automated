from fastapi import APIRouter
from src.services.regions_service import regions_service

router = APIRouter()

@router.post("/update")
async def update_regions():
    success = await regions_service.update_regions_dataset()
    if success:
        return {"message": "regions Dataset updated successfully"}
    return {"error": "The regions dataset could not be updated"}