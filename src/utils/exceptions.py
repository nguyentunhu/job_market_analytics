"""
Custom exception classes for job market analytics scrapers.

Provides a hierarchy of exceptions for different failure modes,
enabling precise error handling and logging in scrapers.
"""


class ScraperException(Exception):
    """Base exception for all scraper-related errors."""
    pass


class FetchException(ScraperException):
    """Raised when fetching a page fails (network, timeout, HTTP error)."""
    pass


class ParseException(ScraperException):
    """Raised when parsing HTML content fails."""
    pass


class ValidationException(ScraperException):
    """Raised when scraped job data fails validation."""
    pass


class DuplicateException(ScraperException):
    """Raised when a duplicate URL is encountered (informational, not an error)."""
    pass


class RateLimitException(ScraperException):
    """Raised when rate limit is hit (HTTP 429 or similar)."""
    pass


class ConfigException(ScraperException):
    """Raised when configuration is invalid."""
    pass
