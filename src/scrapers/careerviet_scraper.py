"""
CareerViet Jobs Scraper (based on query, detects duplicates, logs to file, saves JSON)
https://www.careerviet.vn/

Extracts:
- Job title
- Full job description
- Posting date
- Job URL
- Scraped timestamp
"""

import logging
import time
import re
import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup


# ------------------------------------------------------------------
# Logging Setup (Module-level, initialized once)
# ------------------------------------------------------------------
class _CareerVietLogFilter(logging.Filter):
    """
    [CareerViet] prefix to all log messages.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if not msg.startswith("[CareerViet]"):
            record.msg = f"[CareerViet] {record.msg}"
            # Reset args to avoid formatting issues
            record.args = ()
        return True


def _setup_logging() -> logging.Logger:
    """
    Initialize file-only logging with [CareerViet] prefix.
    
    Design decisions:
    - File-only output (no console) for production environments
    - git/job_market_analytics/logs/scrapers.log as single shared log file for all scrapers
    - Safe directory creation with exist_ok=True
    - Idempotent: safe to call multiple times
    - Filter enforces [CareerViet] prefix programmatically
    
    Returns:
        Configured logger instance
    """
    log_dir = "git/job_market_analytics/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, "scrapers.log")
    
    logger = logging.getLogger("CareerViet")
    
    # Clear existing handlers to allow reconfiguration
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Remove existing filters
    for filt in logger.filters[:]:
        logger.removeFilter(filt)
    
    logger.setLevel(logging.DEBUG)
    
    # File handler only (no console output for production)
    handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Attach filter to enforce [CareerViet] prefix on all messages
    logger.addFilter(_CareerVietLogFilter())
    
    return logger


# Initialize logger once at module import time
logger = _setup_logging()

# Debug: verify logger is configured correctly
if logger.handlers:
    handler = logger.handlers[0]
    if isinstance(handler, logging.FileHandler):
        # Force flush to ensure logs are written
        handler.flush()


class CareerVietScraper:
    """
    Production-grade scraper for CareerViet job listings.
    
    Part of multi-platform scraping framework. Designed for extensibility
    to other job sources (VietnamWorks, TopCV, etc).
    
    Duplicate handling:
    - In-memory set tracks visited URLs across a single scrape run
    - Duplicate URLs are skipped (not scraped again) but don't stop the run
    - Strategy: Simple, memory-efficient, sufficient for single-run contexts
    
    Error handling:
    - Failed job detail pages are logged and skipped
    - Scraping continues even if individual jobs fail
    - No retry logic: transient failures are acceptable in batch scraping
    
    Logging:
    - All messages tagged with [CareerViet] via programmatic filter
    - Errors include context (URL, page number, exception message)
    - Run summary includes success/failure metrics
    """
    
    BASE_LIST_URL = "https://www.careerviet.vn/viec-lam"

    def __init__(self, timeout: int = 10, max_results: int = 10000):
        self.timeout = timeout
        self.max_results = max_results
        self.platform = "careerviet"
        
        # Statistics tracking for run summary
        self.stats = {
            "pages_visited": 0,
            "jobs_scraped": 0,
            "duplicates_skipped": 0,
            "errors": 0,
            "run_duration_seconds": 0.0
        }
        
        # Duplicate tracking: in-memory set of visited URLs
        # Design: Simple, fast lookup O(1), sufficient for single-run context
        self.visited_urls: set = set()
        
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36 "
                "Edg/144.0.0.0"
            )
        }


    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self,
        query: str,
    ) -> List[Dict[str, Any]]:
        """
        Scrape CareerViet job listings using a search query.
        
        Behavior:
        - Tracks duplicate URLs and skips re-scraping
        - Collects statistics on pages, jobs, errors
        - Saves results to JSON with timestamp
        - Logs comprehensive run summary at end
        
        Args:
            query: Search keyword (e.g. "Data Analyst")
            
        Returns:
            List of job dictionaries with schema:
            {
                "job_title": str,
                "job_description": str,
                "posting_date": str (raw text),
                "job_url": str,
                "platform": "careerviet",
                "scraped_at": ISO timestamp
            }
        """
        start_time = time.time()
        jobs: List[Dict[str, Any]] = []
        encoded_query = quote_plus(query)

        logger.info(
            f"Start scraping | query='{query}' | max_results={self.max_results}"
        )

        page = 1
        while len(jobs) < self.max_results:
            try:
                logger.info(
                    f"Scraping page {page} | collected={len(jobs)}"
                )

                if page == 1:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-vi.html"
                else:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-trang-{page}-vi.html"

                # Track page visit
                self.stats["pages_visited"] += 1

                job_urls = self._scrape_page(url)

                if not job_urls:
                    logger.info(
                        "No jobs found on page, stopping"
                    )
                    break

                for job_url in job_urls:
                    if len(jobs) >= self.max_results:
                        break
                    
                    # Duplicate detection: skip if already visited
                    if job_url in self.visited_urls:
                        self.stats["duplicates_skipped"] += 1
                        logger.debug(f"Skipping duplicate URL: {job_url}")
                        continue
                    
                    # Mark as visited before scraping detail
                    self.visited_urls.add(job_url)
                    
                    jd = self._scrape_job_detail(job_url)
                    if jd and self._validate_job(jd):
                        jobs.append(jd)

                page += 1
                time.sleep(2)  # Fixed delay - no randomization for consistency

            except Exception as e:
                logger.error(f"Error scraping page {page}: {str(e)}")
                self.stats["errors"] += 1
                page += 1

        # Record run statistics
        self.stats["run_duration_seconds"] = time.time() - start_time
        self.stats["jobs_scraped"] = len(jobs)
        
        # Log run summary
        self._log_run_summary()
        
        # Save results to JSON
        self._save_results_to_json(jobs)
        
        # Flush all handlers to ensure logs are written to disk
        for handler in logger.handlers:
            handler.flush()

        return jobs

    # ------------------------------------------------------------------
    # Page Handling
    # ------------------------------------------------------------------
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a single page.
        On failure, logs warning and returns None (caller handles gracefully).
        """
        try:
            response = requests.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            )
            if response.status_code != 200:
                logger.warning(f"Fetch failed [{response.status_code}]: {url}")
                self.stats["errors"] += 1
                return None
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.error(f"Fetch error: {str(e)}")
            self.stats["errors"] += 1
            return None

    def _scrape_page(self, url: str) -> List[str]:
        """
        Extract job URLs from a listing page.
        
        Returns:
            List of absolute job detail URLs found on the page
        """
        job_urls: List[str] = []

        soup = self._fetch_page(url)
        if not soup:
            return job_urls

        jobs = soup.find_all("div", class_="job-item")
        if len(jobs) > self.max_results:
            jobs = jobs[:self.max_results]

        logger.debug(f"Found {len(jobs)} job links on page")

        for job in jobs:
            try:
                job_url = job.find_all("a")
                job_url = job_url[1]['href'] if job_url else ""
                
                if job_url and not job_url.startswith("http"):
                    job_url = "https://careerviet.vn/" + job_url

                if not job_url:
                    continue

                job_urls.append(job_url)

            except Exception as e:
                logger.warning(f"Error extracting job link: {str(e)}")
                self.stats["errors"] += 1

        return job_urls

    # ------------------------------------------------------------------
    # Job Detail
    # ------------------------------------------------------------------
    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single job detail page.
        
        On failure:
        - Logs the error with URL and exception message
        - Returns None
        - Does NOT retry (design: efficient for batch scraping)
        
        Preserves posting_date as raw text (no parsing).
        """
        try:
            response = requests.get(
                job_url,
                headers=self.headers,
                timeout=self.timeout
            )
            if response.status_code != 200:
                logger.warning(f"Failed to fetch job detail [{response.status_code}]: {job_url}")
                self.stats["errors"] += 1
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            job_title = self._get_text(soup, "h1", "title")
            posting_date = self._get_text(soup, "div", "detail-box has-background")

            description_block = soup.find("section", class_="job-detail-content")
            description_text = (
                description_block.get_text(separator=" ", strip=True)
                if description_block else ""
            )

            return {
                "job_title": self._normalize_text(job_title),
                "job_description": self._normalize_text(description_text),
                "posting_date": posting_date,  # Kept as raw text, no parsing
                "job_url": job_url,
                "platform": self.platform,
                "scraped_at": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error scraping job detail: {str(e)}")
            self.stats["errors"] += 1
            return None

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    @staticmethod
    def _get_text(
        soup: BeautifulSoup,
        tag: str,
        class_name: str
    ) -> str:
        """Extract and return text content from an HTML element."""
        elem = soup.find(tag, class_=class_name)
        return elem.get_text(strip=True) if elem else ""

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Normalize whitespace and remove zero-width characters."""
        text = text.replace("\u00a0", " ").replace("\u200b", "")
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _validate_job(job: Dict[str, Any]) -> bool:
        """Validate that job has required fields: title and description."""
        return bool(job.get("job_title") and job.get("job_description"))

    def _log_run_summary(self) -> None:
        """
        Log a comprehensive summary of the scraping run.
        
        Includes: pages visited, jobs scraped, duplicates skipped,
        errors encountered, and total duration.
        """
        summary = (
            f"Scraping completed | "
            f"Pages visited: {self.stats['pages_visited']} | "
            f"Jobs scraped: {self.stats['jobs_scraped']} | "
            f"Duplicates skipped: {self.stats['duplicates_skipped']} | "
            f"Errors: {self.stats['errors']} | "
            f"Run duration: {self.stats['run_duration_seconds']:.2f}s"
        )
        logger.info(summary)

    def _save_results_to_json(self, jobs: List[Dict[str, Any]]) -> None:
        """
        Save scraping results to timestamped JSON file.
        
        Location: data/careerviet_jobs_<timestamp>.json
        File creation is idempotent (creates directory if missing).
        Failures are logged but don't interrupt the scrape.
        """
        try:
            output_dir = "git/job_market_analytics/data"
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"careerviet_jobs_{timestamp}.json")
            
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(jobs, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Results saved to {output_file}")
        except Exception as e:
            logger.error(f"Failed to save results to JSON: {str(e)}")


# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------
if __name__ == "__main__":
    scraper = CareerVietScraper(max_results=5)
    results = scraper.scrape(query="Data Analyst")
    
    logger.info(f"Collected {len(results)} jobs")
    
    # Flush all handlers to ensure logs are written
    for handler in logger.handlers:
        handler.flush()
    
    if results:
        print("\nSample result:")
        print(json.dumps(results[0], ensure_ascii=False, indent=2))
