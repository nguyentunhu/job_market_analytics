"""
Abstract base class for job market analytics scrapers.

Defines the interface and shared functionality that all platform-specific
scrapers must implement. Centralizes logging, error handling, stats tracking,
and other common operations.
"""

import abc
import time
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from src.utils.logging_config import LoggerFactory
from src.utils.exceptions import FetchException
from src.utils.http_helpers import create_session, fetch_page, normalize_url
from src.utils.text_utils import normalize_text, extract_text_by_selector
from src.utils.decorators import rate_limit


class BaseScraper(abc.ABC):
    """
    Abstract base class for job scrapers across different platforms.
    
    All platform-specific scrapers must inherit from this class and implement the abstract methods.
    
    Inherited functionality:
    - Logger initialization with platform-specific prefixes
    - HTTP session management with retry strategy
    - Stats tracking (pages visited, jobs scraped, duplicates, errors)
    - Duplicate URL detection
    - Rate limiting between requests
    - Centralized error handling
    
    Platform-specific implementations:
    - scrape(): Main scraping orchestration
    - _scrape_page(): Extract job URLs from listing page
    - _scrape_job_detail(): Extract job data from detail page
    """
    
    # Platform name 
    PLATFORM: str = None  
    
    # Platform base URL 
    BASE_LIST_URL: str = None 
    
    def __init__(
        self,
        timeout: int = 10,
        max_results: int = 10000,
        request_delay: float = 2.0,
    ):
        """
        Initialize the scraper with common configuration.
        
        Args:
            timeout: HTTP request timeout in seconds (default: 10)
            max_results: Maximum results to collect before stopping (default: 10000)
            request_delay: Delay between requests in seconds (default: 2.0)
            
        Raises:
            ValueError: If PLATFORM is not set or is empty
        """
        # Resolve platform name
        self.platform = self.PLATFORM
        if not self.platform:
            raise ValueError(
                f"{self.__class__.__name__} must set PLATFORM class variable"
            )
        
        # Request configuration
        self.timeout = timeout
        self.max_results = max_results
        self.request_delay = request_delay
        
        # HTTP session with retry strategy
        self.session = create_session(timeout=timeout)
        
        # Logger with platform-specific prefix
        self.logger = logging.getLogger(self.platform)
        
        # Statistics tracking
        self.stats = {
            "pages_visited": 0,
            "jobs_scraped": 0,
            "duplicates_skipped": 0,
            "errors": 0,
            "run_duration_seconds": 0.0,
        }
        
        # Duplicate tracking: in-memory set of visited URLs
        self.visited_urls: Set[str] = set()
        
        self.logger.debug(
            f"Initialized {self.__class__.__name__} | "
            f"timeout={timeout}s, max_results={max_results}, request_delay={request_delay}s"
        )
    
    # ------------------------------------------------------------------
    # Public API 
    # ------------------------------------------------------------------
    @abc.abstractmethod
    def scrape(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        Scrape job listings from the platform.
        
        Returns:
            List of job dictionaries with standardized schema
        """
        pass
    
    # ------------------------------------------------------------------
    # Abstract methods (must be implemented by subclasses)
    # ------------------------------------------------------------------
    @abc.abstractmethod
    def _scrape_page(self, page_content: Any) -> List[str]:
        """
        Extract job URLs from a listing page.
        
        Args:
            page_content: Platform-specific page content 
            
        Returns:
            List of job detail URLs found on the page
        """
        pass
    
    @abc.abstractmethod
    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single job detail page.
        
        Should return a dictionary with the standardized job schema,
        or None if scraping fails.
        
        Standard schema:
        {
            "job_title": str,
            "job_description": str,
            "job_url": str,
            "posting_date": str (raw text, no parsing),
            "platform": self.platform,
            "scraped_at": ISO timestamp
        }
        
        Args:
            job_url: URL of the job detail page
            
        Returns:
            Job dictionary or None if scraping failed
        """
        pass
    
    # ------------------------------------------------------------------
    # Shared functionality 
    # ------------------------------------------------------------------
    @rate_limit(calls=1, period=2.0)
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a page using the shared session.
        
        Includes rate limiting, retry logic, and error handling.
        On failure, logs warning and returns None (caller handles gracefully).
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup parsed HTML or None if failed
        """
        try:
            content = fetch_page(
                self.session,
                url,
                timeout=self.timeout,
                logger=self.logger,
            )
            if content:
                return BeautifulSoup(content, "html.parser")
        except FetchException as e:
            self.logger.warning(f"Fetch failed: {str(e)}")
            self.stats["errors"] += 1
        
        return None
    
    def _is_duplicate(self, url: str) -> bool:
        """Check if URL has been visited before."""
        return url in self.visited_urls
    
    def _mark_visited(self, url: str) -> None:
        """Mark URL as visited."""
        self.visited_urls.add(url)
    
    def _normalize_url(self, url: str, base_domain: str) -> str:
        """Normalize a URL (convert relative to absolute)."""
        return normalize_url(url, base_domain)
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text (whitespace, zero-width chars)."""
        return normalize_text(text)
    
    def _extract_text(
        self,
        soup: BeautifulSoup,
        tag: str,
        class_name: str,
        separator: str = " ",
    ) -> str:
        """Extract and normalize text by tag and class."""
        return extract_text_by_selector(soup, tag, class_name, separator)
    
    # ------------------------------------------------------------------
    # Statistics and logging
    # ------------------------------------------------------------------
    def _log_run_summary(self) -> None:
        """Log comprehensive summary of the scraping run."""
        summary = (
            f"Scraping completed | "
            f"Pages visited: {self.stats['pages_visited']} | "
            f"Jobs scraped: {self.stats['jobs_scraped']} | "
            f"Duplicates skipped: {self.stats['duplicates_skipped']} | "
            f"Errors: {self.stats['errors']} | "
            f"Run duration: {self.stats['run_duration_seconds']:.2f}s"
        )
        self.logger.info(summary)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current statistics dictionary."""
        return self.stats.copy()
    
    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close HTTP session and flush loggers."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __del__(self):
        """Cleanup on garbage collection."""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors during cleanup
