#!/bin/bash
# Start the FastAPI app for Railway
# Railway uses port 8080 by default

# Always use port 8080 for Railway compatibility
PORT=8080
echo "========================================="
echo "Starting Amazon Scraper System"
echo "Port: $PORT | Time: $(date)"
echo "========================================="

# Add a delay to allow services to initialize
echo "Waiting 3 seconds before starting..."
sleep 3

# Start the application
echo "Starting Uvicorn server..."
exec python -m uvicorn app.main:app --host 0.0.0.0 --port $PORT --log-level info
