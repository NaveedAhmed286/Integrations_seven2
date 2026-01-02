#!/bin/bash
# Start the FastAPI app

echo "========================================="
echo "Starting Amazon Scraper System"
echo "Time: $(date)"
echo "========================================="

# Wait for system to stabilize
echo "Waiting 10 seconds..."
sleep 10

# Start the application
echo "Starting Uvicorn server..."
exec python -m uvicorn main:app --host 0.0.0.0 --port 8080 --log-level info
