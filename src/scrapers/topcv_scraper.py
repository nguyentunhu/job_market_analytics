import time
from typing import List, Dict, Any, Optional
from urllib.parse import quote_plus
from datetime import datetime

from bs4 import BeautifulSoup

from src.scrapers.base_scraper import BaseScraper
from src.utils.text_utils import extract_text_by_selector, extract_all_text_from_block


class TopCVScraper(BaseScraper):
    """scraper for topcv job listings."""

    PLATFORM = "topcv"
    BASE_LIST_URL = "https://www.topcv.vn/tim-viec-lam"
    JOB_URL_PREFIX = "https://www.topcv.vn/viec-lam/" # this is needed for filtering valid job urls

    def scrape(self, query: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        start_time = time.time()
        jobs: List[Dict[str, Any]] = []

        self.logger.info(
            f"[{self.PLATFORM}] start scraping | search_query='{query}' | max_pages={max_pages}"
        )

        page = 1
        while page <= max_pages and len(jobs) < self.max_results:
            try:
                self.logger.info(
                    f"[{self.PLATFORM}] scraping page {page} | collected={len(jobs)}"
                )

                url = f"{self.BASE_LIST_URL}-{quote_plus(query)}?page={page}" if page>1 \
                    else f"{self.BASE_LIST_URL}-{quote_plus(query)}"
                self.stats["pages_visited"] += 1

                soup = self._fetch_page(url)
                if not soup:
                    self.logger.info(f"[{self.PLATFORM}] failed to fetch page, stopping")
                    break

                job_urls = self._scrape_page(soup)

                if not job_urls:
                    self.logger.info(f"[{self.PLATFORM}] no jobs found on page, stopping")
                    break

                for job_url in job_urls:
                    if len(jobs) >= self.max_results:
                        break

                    if self._is_duplicate(job_url):
                        self.stats["duplicates_skipped"] += 1
                        self.logger.debug(f"[{self.PLATFORM}] skipping duplicate url: {job_url}")
                        continue

                    self._mark_visited(job_url)

                    job = self._scrape_job_detail(job_url)
                    if job:
                        jobs.append(job)

                time.sleep(self.request_delay)
                page += 1

            except Exception as e:
                self.logger.error(f"[{self.PLATFORM}] error scraping page {page}: {str(e)}")
                self.stats["errors"] += 1
                page += 1

        self.stats["run_duration_seconds"] = time.time() - start_time
        self.stats["jobs_scraped"] = len(jobs)

        self._log_run_summary()
        self.close()

        return jobs

    def _scrape_page(self, soup: BeautifulSoup) -> List[str]:
        job_urls: List[str] = []

        try:
            job_items = soup.find_all("div", class_="job-item-search-result") 
            if not job_items:
                self.logger.debug(f"[{self.PLATFORM}] no job items found on page")
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
                    self.logger.warning(f"[{self.PLATFORM}] error extracting job link: {str(e)}")
                    self.stats["errors"] += 1

            self.logger.debug(f"[{self.PLATFORM}] found {len(job_urls)} job links on page")
            return job_urls
        
        except Exception as e:
            self.logger.error(f"[{self.PLATFORM}] error scraping page: {str(e)}")
            self.stats["errors"] += 1
            return job_urls

    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        """
        scrape a single topcv job detail page.
        
        returns job dictionary or none if scraping fails.
        """
        try:
            soup = self._fetch_page(job_url)
            if not soup:
                return None
            
            # extract fields using platform-specific css classes
            job_title = extract_text_by_selector(soup, "h1", "job-detail__info--title")
            
            # topcv often doesn't show posting date on detail page, or it's hard to parse consistently
            posting_date = None # set to none, transformation will handle it

            job_description = extract_all_text_from_block(
                soup, "div", "job-detail__information-detail"
            )
            
            # company and location are often on the listing page, or require more complex parsing from detail page
            # for simplicity, we'll extract directly if available or leave blank
            company = extract_text_by_selector(soup, "a", "company-name") # placeholder, might need refinement
            location = extract_text_by_selector(soup, "span", "address") # placeholder, might need refinement
            
            return {
                "job_title": job_title,
                "company": company,
                "location": location,
                "job_description": job_description,
                "job_url": job_url,
                "posting_date": posting_date,
                "platform": self.PLATFORM,
                "scraped_at": datetime.now().isoformat(),
            }
        
        except Exception as e:
            self.logger.error(f"[{self.PLATFORM}] error scraping job detail {job_url}: {str(e)}")
            self.stats["errors"] += 1
            return None