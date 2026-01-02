#!/bin/bash
# Start the FastAPI app

# FIX: Always use port 8080 for Railway (not 8000)
PORT=8080
echo "Starting Amazon Scraper System on port $PORT"

# Small delay to avoid immediate health check failure
sleep 2

exec uvicorn app.main:app --host 0.0.0.0 --port $PORT
