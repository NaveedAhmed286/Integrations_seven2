# Integrations_seven2# Amazon Scraper System

A production-grade Amazon scraper system with memory, retry, and queue systems.

## Architecture

Built following strict engineering rules:
- **Data Contracts First**: Internal model defines system reality
- **Explicit Normalization**: All external data sanitized before entry
- **Wrapper Pattern**: All external services wrapped with retry/timeout
- **Three-Tier Memory**: Redis (short-term), PostgreSQL (long-term), Episodic (history)
- **Queue Separation**: Workflow (order) vs Retry (reliability)
- **Startup Safety**: Application starts without internet, recovers after
- **Observability**: Sentry integration for production insights

## Features

- ✅ Handles your specific Amazon scraper JSON structure
- ✅ Normalizes raw data into safe internal model
- ✅ Memory system for context and insights
- ✅ Retry queue for network failure recovery
- ✅ Google Sheets integration (optional)
- ✅ AI service integration (optional)
- ✅ Railway-ready containerization
- ✅ Startup without internet dependency

## Quick Start

### 1. Clone and Setup
```bash
git clone <your-repo>
cd amazon-scraper-system

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt