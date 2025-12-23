"""
Custom domain exceptions for the entire system.
Every error has a name, not chaos.
"""


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""
    pass


class NetworkError(Exception):
    """Base class for network-related failures."""
    pass


class ExternalServiceError(Exception):
    """Raised when an external service (Apify, Google, AI) fails."""
    pass


class DataContractError(Exception):
    """Raised when data doesn't conform to internal model."""
    pass


class NormalizationError(Exception):
    """Raised when raw external data cannot be normalized."""
    pass


class MemoryError(Exception):
    """Raised when memory operations fail."""
    pass


class QueueError(Exception):
    """Raised when queue operations fail."""
    pass


class RetryExhaustedError(ExternalServiceError):
    """Raised when all retry attempts for an external service are exhausted."""
    pass