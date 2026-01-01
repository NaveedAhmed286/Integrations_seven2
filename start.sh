#!/bin/bash
# Start the FastAPI app
PORT=${PORT:-8000}
echo "Starting Amazon Scraper System on port $PORT"

# Small delay to avoid immediate health check failure
sleep 3

exec uvicorn app.main:app --host 0.0.0.0 --port $PORT
