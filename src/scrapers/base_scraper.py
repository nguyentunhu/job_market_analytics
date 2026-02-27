"""
abstract base class for job market analytics scrapers.

defines the interface and shared functionality that all platform-specific
scrapers must implement. centralizes logging, error handling, stats tracking,
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
    abstract base class for job scrapers across different platforms.
    
    all platform-specific scrapers must inherit from this class and implement the abstract methods.
    
    inherited functionality:
    - logger initialization with platform-specific prefixes
    - http session management with retry strategy
    - stats tracking (pages visited, jobs scraped, duplicates, errors)
    - duplicate url detection
    - rate limiting between requests
    - centralized error handling
    
    platform-specific implementations:
    - scrape(): main scraping orchestration
    - _scrape_page(): extract job urls from listing page
    - _scrape_job_detail(): extract job data from detail page
    """
    
    # platform name 
    PLATFORM: str = None  
    
    # platform base url 
    BASE_LIST_URL: str = None 
    
    def __init__(
        self,
        timeout: int = 10,
        max_results: int = 500,
        request_delay: float = 2.0,
    ):
        """
        initialize the scraper with common configuration.
        
        args:
            timeout: http request timeout in seconds (default: 10)
            max_results: maximum results to collect before stopping (default: 10000)
            request_delay: delay between requests in seconds (default: 2.0)
            
        raises:
            valueerror: if platform is not set or is empty
        """
        # resolve platform name
        self.platform = self.PLATFORM
        if not self.platform:
            raise ValueError(
                f"{self.__class__.__name__} must set platform class variable"
            )
        
        # request configuration
        self.timeout = timeout
        self.max_results = max_results
        self.request_delay = request_delay
        
        # http session with retry strategy
        self.session = create_session(timeout=timeout)
        
        # logger for this scraper, named after the platform
        # now, dynamically name the logger to be a child of 'scrapers'
        self.logger = LoggerFactory.get_logger(f'scrapers.{self.platform}') 
        
        # statistics tracking
        self.stats = {
            "pages_visited": 0,
            "jobs_scraped": 0,
            "duplicates_skipped": 0,
            "errors": 0,
            "run_duration_seconds": 0.0,
        }
        
        # duplicate tracking: in-memory set of visited urls
        self.visited_urls: Set[str] = set()
        
        self.logger.debug(
            f"initialized {self.__class__.__name__} | "
            f"timeout={timeout}s, max_results={max_results}, request_delay={request_delay}s"
        )
    
    # public api 
    @abc.abstractmethod
    def scrape(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        scrape job listings from the platform.
        
        returns:
            list of job dictionaries with standardized schema
        """
        pass
    
    # abstract methods (must be implemented by subclasses)
    @abc.abstractmethod
    def _scrape_page(self, page_content: Any) -> List[str]:
        """
        extract job urls from a listing page.
        
        args:
            page_content: platform-specific page content 
            
        returns:
            list of job detail urls found on the page
        """
        pass
    
    @abc.abstractmethod
    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        """
        scrape a single job detail page.
        
        should return a dictionary with the standardized job schema,
        or none if scraping fails.
        
        standard schema:
        {
            "job_title": str,
            "job_description": str,
            "job_url": str,
            "posting_date": str (raw text, no parsing),
            "platform": self.platform,
            "scraped_at": iso timestamp
        }
        
        args:
            job_url: url of the job detail page
            
        returns:
            job dictionary or none if scraping failed
        """
        pass
    
    # shared functionality 
    @rate_limit(calls=1, period=2.0)
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        fetch and parse a page using the shared session.
        
        includes rate limiting, retry logic, and error handling.
        on failure, logs warning and returns none (caller handles gracefully).
        
        args:
            url: url to fetch
            
        returns:
            beautifulsoup parsed html or none if failed
        """
        try:
            content = fetch_page(
                self.session,
                url,
                timeout=self.timeout,
                # here, we pass self.logger, which is the platform-named logger
                logger=self.logger, 
            )
            if content:
                return BeautifulSoup(content, "html.parser")
        except FetchException as e:
            self.logger.warning(f"[{self.platform}] fetch failed: {str(e)}") # add platform prefix
            self.stats["errors"] += 1
        
        return None
    
    def _is_duplicate(self, url: str) -> bool:
        """check if url has been visited before."""
        return url in self.visited_urls
    
    def _mark_visited(self, url: str) -> None:
        """mark url as visited."""
        self.visited_urls.add(url)
    
    def _normalize_url(self, url: str, base_domain: str) -> str:
        """normalize a url (convert relative to absolute)."""
        return normalize_url(url, base_domain)
    
    def _extract_text(
        self,
        soup: BeautifulSoup,
        tag: str,
        class_name: str,
        separator: str = " ",
    ) -> str:
        """extract and normalize text by tag and class."""
        return extract_text_by_selector(soup, tag, class_name, separator)
    
    # statistics and logging
    def _log_run_summary(self) -> None:
        """log comprehensive summary of the scraping run."""
        summary = (
            f"scraping completed | "
            f"pages visited: {self.stats['pages_visited']} | "
            f"jobs scraped: {self.stats['jobs_scraped']} | "
            f"duplicates skipped: {self.stats['duplicates_skipped']} | "
            f"errors: {self.stats['errors']} | "
            f"run duration: {self.stats['run_duration_seconds']:.2f}s"
        )
        self.logger.info(f"[{self.platform}] {summary}") # add platform prefix
    
    def get_statistics(self) -> Dict[str, Any]:
        """get current statistics dictionary."""
        return self.stats.copy()
    
    # cleanup
    def close(self) -> None:
        """close http session and flush loggers."""
        self.session.close()
    
    def __enter__(self):
        """context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """context manager exit."""
        self.close()
    
    def __del__(self):
        """cleanup on garbage collection."""
        try:
            self.close()
        except Exception:
            pass  # ignore errors during cleanup
