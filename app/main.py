import os
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from app.google_service import GoogleService
from app.memory_manager import MemoryManager
from app.apify_client import ApifyClient

# -----------------------------
# Configuration
# -----------------------------
WEBHOOK_SECRET = "a9K3pLq5Vn8R7sT2Xw6bY4dF1mC0zHjR"  # <- your Apify webhook secret
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
# -----------------------------
# Initialize app
# -----------------------------
app = FastAPI(title="Integrations Seven 2")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Initialize services
# -----------------------------
memory_manager = MemoryManager()
google_service = GoogleService(sheet_id=SHEET_ID)
apify_client = ApifyClient(token=APIFY_TOKEN)

# -----------------------------
# Lifecycle events
# -----------------------------
@app.on_event("startup")
async def startup_event():
    memory_manager.initialize_short_term()
    memory_manager.initialize_long_term()
    google_service.initialize()
    print("🚀 Application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    print("🛑 Application shutdown complete.")

# -----------------------------
# Health check
# -----------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}

# -----------------------------
# Webhook endpoint
# -----------------------------
@app.post("/api/v1/actor-webhook")
async def apify_webhook(request: Request):
    # -------------------------
    # Header verification
    # -------------------------
    secret = request.headers.get("X-Apify-Webhook-Secret")
    if not secret or secret != WEBHOOK_SECRET:
        print("❌ Unauthorized webhook attempt. Header:", secret)
        raise HTTPException(status_code=401, detail="Unauthorized webhook")

    # -------------------------
    # Parse payload
    # -------------------------
    payload = await request.json()
    actor_run_id = payload.get("runId")
    actor_id = payload.get("actorId")
    event_type = payload.get("eventType")
    custom_keyword = payload.get("customData", {}).get("keyword", "")

    print(f"✅ Webhook authorized. ActorRunId: {actor_run_id}, ActorId: {actor_id}, Keyword: {custom_keyword}")

    # -------------------------
    # Fetch dataset items from Apify
    # -------------------------
    dataset_id = payload.get("datasetId")
    if not dataset_id:
        raise HTTPException(status_code=400, detail="DatasetId not provided")

    items = await apify_client.get_dataset_items(dataset_id)

    if not items:
        print("⚠ No dataset items found.")
        return {"status": "no_data"}

    # -------------------------
    # Process items using real Amazon product page function
    # -------------------------
    rows_to_append = []
    for item in items:
        # Example structure returned by your real page function
        # Adjust these keys according to your real page function output
        row = [
            item.get("title", ""),
            item.get("price", ""),
            item.get("rating", ""),
            item.get("reviews", ""),
            item.get("asin", ""),
            item.get("url", ""),
        ]
        rows_to_append.append(row)

    # -------------------------
    # Write to Google Sheet
    # -------------------------
    if rows_to_append:
        google_service.append_rows(rows_to_append)
        print(f"📤 {len(rows_to_append)} rows appended to Google Sheet")

    return {"status": "success", "rows_added": len(rows_to_append)}


# -----------------------------
# Run Uvicorn
# -----------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
