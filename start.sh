#!/bin/bash
# Start the FastAPI app for Railway

echo "========================================="
echo "Starting Amazon Scraper System"
echo "Time: $(date)"
echo "========================================="

# Wait longer for Railway health check
echo "Waiting 15 seconds for system to stabilize..."
sleep 15

# Start the application
echo "Starting Uvicorn server on port 8080..."
exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --log-level info
