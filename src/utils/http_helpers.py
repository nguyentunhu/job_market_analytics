"""
HTTP session and request helpers for job market analytics scrapers.

Provides utility functions for managing HTTP sessions, headers, and requests
with built-in error handling and retry logic.
"""

import logging
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as UrllibRetry

from .exceptions import FetchException, RateLimitException


def create_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    timeout: int = 10,
    user_agent: Optional[str] = None,
) -> requests.Session:
    """
    Create a requests Session with retry strategy and common headers.
    
    Args:
        retries: Number of retries for failed requests (default: 3)
        backoff_factor: Backoff factor for retries (default: 0.5)
        timeout: Default request timeout in seconds (default: 10)
        user_agent: Custom User-Agent header (default: Chrome-like agent)
        
    Returns:
        Configured requests.Session instance
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = UrllibRetry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    
    # Mount adapters for HTTP and HTTPS
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set default headers
    if user_agent is None:
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36 "
            "Edg/144.0.0.0"
        )
    
    session.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    })
    
    # Store timeout as session attribute for convenience
    session.timeout = timeout  # type: ignore
    
    return session


def fetch_page(
    session: requests.Session,
    url: str,
    timeout: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    """
    Fetch a page using a session, with error handling.
    
    Args:
        session: requests.Session instance
        url: URL to fetch
        timeout: Request timeout in seconds (default: session.timeout)
        logger: Logger instance for logging failures (optional)
        
    Returns:
        Page content as string, or None if fetch failed
        
    Raises:
        FetchException: If fetch fails after retries
        RateLimitException: If rate limited (HTTP 429)
    """
    if timeout is None:
        timeout = getattr(session, "timeout", 10)
    
    try:
        response = session.get(url, timeout=timeout)
        
        # Check for rate limiting
        if response.status_code == 429:
            msg = f"Rate limited (HTTP 429): {url}"
            if logger:
                logger.warning(msg)
            raise RateLimitException(msg)
        
        # Check for other HTTP errors
        if response.status_code != 200:
            msg = f"HTTP {response.status_code}: {url}"
            if logger:
                logger.warning(msg)
            raise FetchException(msg)
        
        return response.text
    
    except requests.exceptions.Timeout as e:
        msg = f"Timeout fetching {url}: {str(e)}"
        if logger:
            logger.error(msg)
        raise FetchException(msg) from e
    
    except requests.exceptions.RequestException as e:
        msg = f"Request failed for {url}: {str(e)}"
        if logger:
            logger.error(msg)
        raise FetchException(msg) from e


def normalize_url(url: str, base_domain: str) -> str:
    """
    Normalize a URL, converting relative URLs to absolute.
    
    Args:
        url: The URL to normalize (may be relative)
        base_domain: Base domain to prepend (e.g., "https://example.com")
        
    Returns:
        Absolute URL
        
    Example:
        >>> normalize_url("/job/123", "https://example.com")
        'https://example.com/job/123'
    """
    if url.startswith("http://") or url.startswith("https://"):
        return url
    
    if base_domain.endswith("/"):
        base_domain = base_domain.rstrip("/")
    
    if not url.startswith("/"):
        url = "/" + url
    
    return base_domain + url
