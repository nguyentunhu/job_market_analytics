"""
http helper functions for scrapers.
"""

import logging
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

from .exceptions import FetchException

# default user agent to mimic a browser
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/96.0.4664.110 Safari/537.36"
)

def create_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    timeout: int = 10,
    user_agent: str = DEFAULT_USER_AGENT,
) -> requests.Session:
    """
    create a requests session with retry logic and standard headers.
    
    args:
        retries: number of retry attempts.
        backoff_factor: backoff factor for retries.
        timeout: request timeout in seconds.
        user_agent: user-agent string for headers.
        
    returns:
        configured requests session.
    """
    session = requests.Session()
    
    # define retry strategy
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["head", "get", "options"]
    )
    
    # mount http and https adapters
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # set default headers
    session.headers.update({"User-Agent": user_agent})
    
    # set default timeout
    session.timeout = timeout
    
    return session

def fetch_page(
    session: requests.Session,
    url: str,
    timeout: int,
    logger: logging.Logger,
) -> Optional[str]:
    """
    fetch a single page using a requests session.
    
    args:
        session: requests session to use.
        url: url to fetch.
        timeout: request timeout.
        logger: logger for logging messages.
        
    returns:
        page content as text, or none on failure.
        
    raises:
        fetchexception: if the request fails after all retries.
    """
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()  # raise httperror for bad responses (4xx or 5xx)
        return response.text
    
    except requests.exceptions.RequestException as e:
        logger.error(f"failed to fetch page {url}: {e}")
        raise FetchException(f"request failed for url: {url}") from e

def normalize_url(url: str, base_domain: str) -> str:
    """
    normalize a url by joining it with a base domain if it's relative.
    
    args:
        url: url to normalize.
        base_domain: base domain to use.
        
    returns:
        absolute url.
    """
    if not url:
        return ""
    return urljoin(base_domain, url)
