import time
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus
from datetime import datetime

from bs4 import BeautifulSoup

from src.scrapers.base_scraper import BaseScraper
from src.utils.text_utils import extract_text_by_selector, extract_all_text_from_block


class Vieclam24hScraper(BaseScraper):
    """scraper for vieclam24h job listings."""
    
    PLATFORM = "vieclam24h"
    BASE_LIST_URL = "https://vieclam24h.vn/tim-kiem-viec-lam-nhanh?"
    
    def scrape(self, query: str = "Data Analyst", max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        scrape vieclam24h job listings using pagination.
        
        args:
            query: search keyword (e.g. "data analyst")
            max_pages: maximum number of pages to scrape.
            
        returns:
            list of job dictionaries with standardized schema
        """
        start_time = time.time()
        jobs: List[Dict[str, Any]] = []
        
        self.logger.info(
            f"start scraping | query='{query}' | max_pages={max_pages}"
        )
        
        page = 1
        while page <= max_pages and len(jobs) < self.max_results: # add max_results check
            try:
                self.logger.info(f"scraping page {page} | collected={len(jobs)}")
                
                # vieclam24h pagination url
                if page == 1:
                    url = f"{self.BASE_LIST_URL}q={quote_plus(query).replace('+', '%20')}"
                else:
                    url = f"{self.BASE_LIST_URL}page={page}&q={quote_plus(query).replace('+', '%20')}"
                
                # fetch page
                self.stats["pages_visited"] += 1
                soup = self._fetch_page(url)
                
                if not soup:
                    self.logger.info("failed to fetch page, stopping")
                    break
                
                # extract job urls from page
                page_jobs = self._scrape_page(soup)
                
                if not page_jobs:
                    self.logger.info("no jobs found on page, stopping")
                    break
                
                # scrape each job detail
                for job_url in page_jobs:
                    if len(jobs) >= self.max_results: # use max_results from basescraper
                        break
                    
                    # duplicate detection
                    if self._is_duplicate(job_url):
                        self.stats["duplicates_skipped"] += 1
                        self.logger.debug(f"skipping duplicate url: {job_url}")
                        continue
                    
                    # mark as visited
                    self._mark_visited(job_url)
                    
                    # scrape job detail
                    job_dict = self._scrape_job_detail(job_url)
                    if job_dict: # removed _validate_job as it's no longer present
                        jobs.append(job_dict)
                
                # rate limiting between pages
                time.sleep(self.request_delay)
                page += 1
            
            except Exception as e:
                self.logger.error(f"error scraping page {page}: {str(e)}")
                self.stats["errors"] += 1
                page += 1
        
        # record statistics
        self.stats["run_duration_seconds"] = time.time() - start_time
        self.stats["jobs_scraped"] = len(jobs)
        
        # log summary 
        self._log_run_summary()
        self.close()
        
        return jobs
    
    # platform-specific implementation
    def _scrape_page(self, soup: BeautifulSoup) -> List[str]:
        """
        extract job urls from a vieclam24h listing page.
        
        returns:
            list of absolute job detail urls
        """
        job_urls: List[str] = []
        
        try:
            # find all job links (a tags with target="_blank")
            job_elements = soup.find_all("a", target="_blank")
            
            if not job_elements:
                self.logger.debug("no job items found on page")
                return job_urls
            
            self.logger.debug(f"found {len(job_elements)} job links on page")
            
            for job in job_elements:
                try:
                    # extract job url
                    job_url = job.get("href", "")
                    
                    # normalize url
                    job_url = self._normalize_url(job_url, self.BASE_LIST_URL)
                    
                    if job_url:
                        job_urls.append(job_url)
                
                except Exception as e:
                    self.logger.warning(f"error extracting job link: {str(e)}")
                    self.stats["errors"] += 1
            
            return job_urls
        
        except Exception as e:
            self.logger.error(f"error scraping page: {str(e)}")
            self.stats["errors"] += 1
            return job_urls
    
    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        """
        scrape a single vieclam24h job detail page.
        
        returns job dictionary or none if scraping fails.
        """
        try:
            soup = self._fetch_page(job_url)
            if not soup:
                return None
            
            # extract fields using platform-specific css classes
            job_title = extract_text_by_selector(
                soup, "div", "text-24 font-bold leading-10 text-se-neutral-84 !font-medium"
            )
            posting_date = extract_text_by_selector(
                soup, "div", "text-14 font-normal leading-6 text-se-neutral-84 line-clamp-2 break-words"
            )
            job_description = extract_all_text_from_block(
                soup, "div", "flex flex-col gap-8 w-full sm_cv:gap-6"
            )
            
            # vieclam24h doesn't easily expose company and location on job detail pages in a consistent css class.
            # we'll leave these as empty strings for now or try best effort from listing page if possible.
            company = ""
            location = ""
            
            return {
                "job_title": job_title,
                "company": company,
                "location": location,
                "job_description": job_description,
                "job_url": job_url,
                "posting_date": posting_date, # keep as raw string
                "platform": self.PLATFORM,
                "scraped_at": datetime.now().isoformat(),
            }
        
        except Exception as e:
            self.logger.error(f"error scraping job detail {job_url}: {str(e)}")
            self.stats["errors"] += 1
            return None