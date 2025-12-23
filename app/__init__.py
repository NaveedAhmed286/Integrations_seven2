"""
Amazon Scraper System - Production-grade scraper with memory and retry systems.
"""

__version__ = "1.0.0"
__author__ = "Engineering Team"

# Export main components for easy import
from app.config import config
from app.logger import logger
from app.errors import (
    ConfigError,
    NetworkError,
    ExternalServiceError,
    DataContractError,
    NormalizationError,
    MemoryError,
    QueueError,
    RetryExhaustedError
)

__all__ = [
    'config',
    'logger',
    'ConfigError',
    'NetworkError',
    'ExternalServiceError',
    'DataContractError',
    'NormalizationError',
    'MemoryError',
    'QueueError',
    'RetryExhaustedError'
]