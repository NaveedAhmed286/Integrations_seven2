#!/bin/bash
# Start the FastAPI app
PORT=${PORT:-8000}
echo "Starting Amazon Scraper System on port $PORT"
exec uvicorn app.main:app --host 0.0.0.0 --port $PORT
