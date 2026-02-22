import time
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus
from datetime import datetime

from bs4 import BeautifulSoup

from src.scrapers.base_scraper import BaseScraper
from src.utils.text_utils import extract_text_by_selector, extract_all_text_from_block


class CareerVietScraper(BaseScraper):
    """scraper for careerviet job listings."""
    
    PLATFORM = "careerviet"
    BASE_LIST_URL = "https://www.careerviet.vn/viec-lam"
    
    def scrape(
        self,
        query: str,
        max_pages: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        scrape careerviet job listings using a search query.
        
        args:
            query: search keyword (e.g. "data analyst")
            max_pages: maximum number of pages to scrape.
            
        returns:
            list of job dictionaries with standardized schema
        """
        start_time = time.time()
        jobs: List[Dict[str, Any]] = []
        encoded_query = quote_plus(query)
        
        self.logger.info(
            f"start scraping | query='{query}' | max_pages={max_pages}"
        )
        
        page = 1
        while page <= max_pages and len(jobs) < self.max_results: # add max_results check
            try:
                self.logger.info(
                    f"scraping page {page} | collected={len(jobs)}"
                )
                
                # construct page url
                if page == 1:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-vi.html"
                else:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-trang-{page}-vi.html"
                
                # fetch page
                self.stats["pages_visited"] += 1
                soup = self._fetch_page(url)
                
                if not soup:
                    self.logger.info("failed to fetch page, stopping")
                    break
                
                # extract job urls from page
                job_urls = self._scrape_page(soup)
                
                if not job_urls:
                    self.logger.info("no jobs found on page, stopping")
                    break
                
                # scrape each job detail
                for job_url in job_urls:
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
        extract job urls from a careerviet listing page.
        
        returns:
            list of absolute job detail urls
        """
        job_urls: List[str] = []
        
        try:
            # find all job items
            job_items = soup.find_all("div", class_="job-item")
            
            if not job_items:
                self.logger.debug("no job items found on page")
                return job_urls
            
            self.logger.debug(f"found {len(job_items)} job links on page")
            
            for job in job_items:
                try:
                    # extract job url from the job item
                    links = job.find_all("a")
                    if len(links) >= 2:
                        job_url = links[1].get("href", "")
                    else:
                        continue
                    
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
        scrape a single careerviet job detail page.
        
        returns job dictionary or none if scraping fails.
        """
        try:
            soup = self._fetch_page(job_url)
            if not soup:
                return None
            
            # extract fields using platform-specific css classes
            job_title = extract_text_by_selector(soup, "h1", "title")
            
            # use self._extract_text for general text extraction, which handles normalization
            # example for posting date - often in a detail-box with a specific structure
            # assuming the date is within a span with class 'date' inside a div with class 'detail-box has-background'
            posting_date_container = soup.find('div', class_='detail-box has-background')
            posting_date_raw = None
            if posting_date_container:
                posting_date_raw = extract_text_by_selector(posting_date_container, 'span', 'date') # assuming 'date' is the class
            
            job_description = extract_all_text_from_block(
                soup, "section", "job-detail-content"
            )
            
            # extract company and location if available
            company = extract_text_by_selector(soup, "h3", "company-name")
            location = extract_text_by_selector(soup, "span", "job-location")
            
            return {
                "job_title": job_title,
                "company": company,
                "location": location,
                "job_description": job_description,
                "job_url": job_url,
                "posting_date": posting_date_raw,  # keep as raw string, transformation handles parsing
                "platform": self.PLATFORM,
                "scraped_at": datetime.now().isoformat(),
            }
        
        except Exception as e:
            self.logger.error(f"error scraping job detail {job_url}: {str(e)}")
            self.stats["errors"] += 1
            return None