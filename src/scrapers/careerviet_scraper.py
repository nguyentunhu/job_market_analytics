"""
CareerViet Jobs Scraper (based on query, log messages, saves JSON)
https://www.careerviet.vn/

Extracts:
- Job title
- Full job description
- Posting date
- Job URL
- Scraped timestamp
"""

import time
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
from ..utils.text_utils import extract_text_by_selector, extract_all_text_from_block


class CareerVietScraper(BaseScraper):
    """
    Production-grade scraper for CareerViet job listings.
    
    Implements platform-specific scraping logic while inheriting
    common functionality from BaseScraper:
    - Logging with [CareerViet] prefix
    - HTTP session management with retry strategy
    - Duplicate URL detection
    - Statistics tracking
    - Results saving to JSON with timestamps
    - Rate limiting between requests
    """
    
    PLATFORM = "careerviet"
    BASE_LIST_URL = "https://www.careerviet.vn/viec-lam"
    BASE_DOMAIN = "https://careerviet.vn"
    
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
            List of job dictionaries with standardized schema
        """
        start_time = time.time()
        jobs: List[Dict[str, Any]] = []
        encoded_query = quote_plus(query)
        
        self.logger.info(
            f"Start scraping | query='{query}' | max_results={self.max_results}"
        )
        
        page = 1
        while len(jobs) < self.max_results:
            try:
                self.logger.info(
                    f"Scraping page {page} | collected={len(jobs)}"
                )
                
                # Construct page URL
                if page == 1:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-vi.html"
                else:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-trang-{page}-vi.html"
                
                # Fetch page
                self.stats["pages_visited"] += 1
                soup = self._fetch_page(url)
                
                if not soup:
                    self.logger.info("Failed to fetch page, stopping")
                    break
                
                # Extract job URLs from page
                job_urls = self._scrape_page(soup)
                
                if not job_urls:
                    self.logger.info("No jobs found on page, stopping")
                    break
                
                # Scrape each job detail
                for job_url in job_urls:
                    if len(jobs) >= self.max_results:
                        break
                    
                    # Duplicate detection
                    if self._is_duplicate(job_url):
                        self.stats["duplicates_skipped"] += 1
                        self.logger.debug(f"Skipping duplicate URL: {job_url}")
                        continue
                    
                    # Mark as visited
                    self._mark_visited(job_url)
                    
                    # Scrape job detail
                    job_dict = self._scrape_job_detail(job_url)
                    if job_dict and self._validate_job(job_dict):
                        jobs.append(job_dict)
                
                # Rate limiting between pages
                time.sleep(self.request_delay)
                page += 1
            
            except Exception as e:
                self.logger.error(f"Error scraping page {page}: {str(e)}")
                self.stats["errors"] += 1
                page += 1
        
        # Record statistics
        self.stats["run_duration_seconds"] = time.time() - start_time
        self.stats["jobs_scraped"] = len(jobs)
        
        # Log summary and save results
        self._log_run_summary()
        self._save_results_to_json(jobs)
        self.close()
        
        return jobs
    
    # ------------------------------------------------------------------
    # Platform-specific implementation
    # ------------------------------------------------------------------
    def _scrape_page(self, soup: BeautifulSoup) -> List[str]:
        """
        Extract job URLs from a CareerViet listing page.
        
        Returns:
            List of absolute job detail URLs
        """
        job_urls: List[str] = []
        
        try:
            # Find all job items
            job_items = soup.find_all("div", class_="job-item")
            
            if len(job_items) > self.max_results:
                job_items = job_items[:self.max_results]
            
            self.logger.debug(f"Found {len(job_items)} job links on page")
            
            for job in job_items:
                try:
                    # Extract job URL from the job item
                    links = job.find_all("a")
                    if len(links) >= 2:
                        job_url = links[1].get("href", "")
                    else:
                        continue
                    
                    # Normalize URL
                    job_url = self._normalize_url(job_url, self.BASE_DOMAIN)
                    
                    if job_url:
                        job_urls.append(job_url)
                
                except Exception as e:
                    self.logger.warning(f"Error extracting job link: {str(e)}")
                    self.stats["errors"] += 1
            
            return job_urls
        
        except Exception as e:
            self.logger.error(f"Error scraping page: {str(e)}")
            self.stats["errors"] += 1
            return job_urls
    
    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single CareerViet job detail page.
        
        Returns job dictionary or None if scraping fails.
        """
        try:
            soup = self._fetch_page(job_url)
            if not soup:
                return None
            
            # Extract fields using platform-specific CSS classes
            job_title = extract_text_by_selector(soup, "h1", "title")
            posting_date = extract_text_by_selector(
                soup, "div", "detail-box has-background"
            )
            job_description = extract_all_text_from_block(
                soup, "section", "job-detail-content"
            )
            
            # Extract company and location if available
            company = extract_text_by_selector(soup, "h3", "company-name")
            location = extract_text_by_selector(soup, "span", "job-location")
            
            return {
                "job_title": job_title,
                "job_description": job_description,
                "posting_date": posting_date,
                "job_url": job_url,
                "company": company,
                "location": location,
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
    def _validate_job(job: Dict[str, Any]) -> bool:
        """Validate that job has required fields."""
        return bool(job.get("job_title") and job.get("job_description"))
    
    @staticmethod
    def _get_timestamp() -> str:
        """Get current ISO timestamp."""
        from datetime import datetime
        return datetime.now().isoformat()


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    scraper = CareerVietScraper(max_results=5)
    results = scraper.scrape(query="Data Analyst")
    
    print(f"\nCollected {len(results)} jobs")
    if results:
        print("\nSample result:")
        import json
        print(json.dumps(results[0], ensure_ascii=False, indent=2))
