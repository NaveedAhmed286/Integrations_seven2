from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
async def health_check():
    return {"status": "healthy"}

@router.get("/ready")
async def readiness_check():
    # Add your service checks here
    return {
        "ready": True,
        "services": {
            "config": True,
            "memory": True,
            "apify": True,
            "google_sheets": True,
            "ai": True,
            "queues": True
        }
    }
