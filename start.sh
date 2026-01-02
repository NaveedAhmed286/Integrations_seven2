#!/bin/bash

export PYTHONPATH="/app:$PYTHONPATH"

sleep 5

exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --log-level info
