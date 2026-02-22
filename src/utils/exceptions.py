"""
custom exception classes for the job market analytics project.
"""

class ScraperException(Exception):
    """base class for all scraper-related exceptions."""
    pass

class FetchException(ScraperException):
    """raised when an http request fails after all retries."""
    pass

class ParseException(ScraperException):
    """raised when parsing of html or json content fails."""
    pass

class ValidationException(ScraperException):
    """raised when scraped data fails validation checks."""
    pass

class RateLimitException(ScraperException):
    """raised when a rate limit is exceeded."""
    pass

class ConfigException(Exception):
    """raised for errors in configuration files."""
    pass
