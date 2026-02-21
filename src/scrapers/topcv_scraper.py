"""
TopCV Jobs Scraper (based on query, log messages, saves JSON)
https://www.topcv.vn/

Extracts:
- Job title
- Full job description
- Posting date: None (no specified) 
- Job URL
- Scraped timestamp
"""

import time
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import sys 
from pathlib import Path
from urllib.parse import quote_plus

try:
    from .base_scraper import BaseScraper
    from ..utils.text_utils import extract_text_by_selector, extract_all_text_from_block
except ImportError:
    # When run directly as a script
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from base_scraper import BaseScraper
    from utils.text_utils import extract_text_by_selector, extract_all_text_from_block

class TopCVScraper(BaseScraper):
    """
    TopCV job scraper implementation.
    """

    PLATFORM = "topcv"
    BASE_LIST_URL = "https://www.topcv.vn/tim-viec-lam"
    JOB_URL_PREFIX = "https://www.topcv.vn/viec-lam/"

    def scrape(self, query: str) -> List[Dict[str, Any]]:
        start_time = time.time()
        jobs: List[Dict[str, Any]] = []

        self.logger.info(
            f"Start scraping | search_query='{query}' | max_results={self.max_results}"
        )

        page = 1
        while len(jobs) < self.max_results:
            try:
                self.logger.info(
                    f"Scraping page {page} | collected={len(jobs)}"
                )

                url = f"{self.BASE_LIST_URL}-{quote_plus(query)}?page={page}" if page>1 \
                    else f"{self.BASE_LIST_URL}-{quote_plus(query)}"
                self.stats["pages_visited"] += 1

                soup = self._fetch_page(url)
                if not soup:
                    self.logger.info("Failed to fetch page, stopping")
                    break

                job_urls = self._scrape_page(soup)

                if not job_urls:
                    self.logger.info("No jobs found on page, stopping")
                    break

                for job_url in job_urls:
                    if len(jobs) >= self.max_results:
                        break

                    if self._is_duplicate(job_url):
                        self.stats["duplicates_skipped"] += 1
                        self.logger.debug(f"Skipping duplicate URL: {job_url}")
                        continue

                    self._mark_visited(job_url)

                    job = self._scrape_job_detail(job_url)
                    if job:
                        jobs.append(job)

                time.sleep(self.request_delay)
                page += 1

            except Exception as e:
                self.logger.error(f"Error scraping page {page}: {str(e)}")
                self.stats["errors"] += 1
                page += 1

        self.stats["run_duration_seconds"] = time.time() - start_time
        self.stats["jobs_scraped"] = len(jobs)

        self._log_run_summary()
        self._save_results_to_json(jobs)
        self.close()

        return jobs

    def _scrape_page(self, soup: BeautifulSoup) -> List[str]:
        job_urls: List[str] = []

        try:
            job_items = soup.find_all("div", class_="job-item-search-result") 
            if not job_items:
                self.logger.debug("No job items found on page")
                return job_urls

            for job_item in job_items:
                try:
                    h3 = job_item.find("h3", class_="title")
                    if not h3:
                        continue

                    link = h3.find("a")
                    if not link:
                        continue

                    job_url = link.get("href", "")
                    if job_url.startswith(self.JOB_URL_PREFIX):
                        job_urls.append(job_url)
                except Exception as e:
                    self.logger.warning(f"Error extracting job link: {str(e)}")
                    self.stats["errors"] += 1

            self.logger.debug(f"Found {len(job_urls)} job links on page")
            return job_urls
        
        except Exception as e:
            self.logger.error(f"Error scraping page: {str(e)}")
            self.stats["errors"] += 1
            return job_urls

    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single TopCV job detail page.
        
        Returns job dictionary or None if scraping fails.
        """
        try:
            soup = self._fetch_page(job_url)
            if not soup:
                return None
            
            # Extract fields using platform-specific CSS classes
            job_title = extract_text_by_selector(soup, "h1", "job-detail__info--title")
            job_description = extract_all_text_from_block(
                soup, "div", "job-detail__information-detail"
            )
            
            return {
                "job_title": job_title,
                "job_description": job_description,
                "posting_date": None,
                "job_url": job_url,
                "platform": self.platform,
                "scraped_at": self._get_timestamp(),
            }
        
        except Exception as e:
            self.logger.error(f"Error scraping job detail: {str(e)}")
            self.stats["errors"] += 1
            return None
    
    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------
    @staticmethod
    def _get_timestamp() -> str:
        """Get current ISO timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()