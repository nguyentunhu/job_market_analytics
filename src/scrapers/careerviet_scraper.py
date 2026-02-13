"""
CareerViet Jobs Scraper (Query-Based, Minimal Schema, Max Results)
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
from typing import List, Dict, Any, Optional
from datetime import datetime
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CareerVietScraper:
    BASE_LIST_URL = "https://www.careerviet.vn/viec-lam"

    def __init__(self, timeout: int = 10, max_results: int = 10000):
        self.timeout = timeout
        self.max_results = max_results
        self.platform = "careerviet"
        self.jobs_scraped = 0
        self.errors: List[str] = []

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

        Args:
            query: search keyword (e.g. "Data Analyst")
        """

        jobs: List[Dict[str, Any]] = []
        encoded_query = quote_plus(query)

        logger.info(
            f"[CareerViet] [scrape] Start scraping | query='{query}' | max_results={self.max_results}"
        )

        page = 1
        while len(jobs) < self.max_results:
            try:
                logger.info(
                    f"[CareerViet] [scrape] Page {page} | collected={len(jobs)}"
                )

                if page == 1:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-vi.html"
                else:
                    url = f"{self.BASE_LIST_URL}/{encoded_query}-k-trang-{page}-vi.html"

                job_urls = self._scrape_page(url)

                if not job_urls:
                    logger.info(
                        "[CareerViet] [scrape] No jobs found on page, stopping"
                    )
                    break

                for job_url in job_urls:
                    if len(jobs) >= self.max_results:
                        break
                    jd = self._scrape_job_detail(job_url)
                    # if jd and self._validate_job(jd):
                    jobs.append(jd)

                page += 1
                time.sleep(2)

            except Exception as e:
                logger.error(f"[CareerViet] [scrape] Page {page} error: {e}")
                self.errors.append(str(e))
                page += 1

        self.jobs_scraped = len(jobs)
        logger.info(f"[CareerViet] [scrape] Finished | total jobs={self.jobs_scraped}")
        return jobs

    # ------------------------------------------------------------------
    # Page Handling
    # ------------------------------------------------------------------
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = requests.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            )
            if response.status_code != 200:
                logger.warning(f"[_fetch_page] Fetch failed [{response.status_code}]: {url}")
                return None
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.error(f"[_fetch_page] Fetch error: {e}")
            return None

    def _scrape_page(self, url: str) -> List[Dict[str, Any]]:
        jds: List[Dict[str, Any]] = []

        soup = self._fetch_page(url)
        if not soup:
            return jds

        jobs = soup.find_all("div", class_="job-item")
        if len(jobs) > self.max_results:
            jobs = jobs[:self.max_results]

        logger.info(f"[CareerViet] [_scrape_page] Found {len(jobs)} jobs")

        for job in jobs:
            try:
                job_url = job.find_all("a")
                job_url = job_url[1]['href'] if job_url else ""
                
                if job_url and not job_url.startswith("http"):
                    job_url = "https://careerviet.vn/" + job_url

                if not job_url:
                    continue

                jd = self._scrape_job_detail(job_url)
                if jd and self._validate_job(jd):
                    jds.append(jd)

            except Exception as e:
                logger.warning(f"[_scrape_page] Job link error: {e}")

        return jds

    # ------------------------------------------------------------------
    # Job Detail
    # ------------------------------------------------------------------
    def _scrape_job_detail(self, job_url: str) -> Optional[Dict[str, Any]]:
        try:
            response = requests.get(
                job_url,
                headers=self.headers,
                timeout=self.timeout
            )
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, "html.parser")

            job_title = self._get_text(soup, "h1", "title")
            posting_date = self._get_text(soup, "div","detail-box has-background")

            description_block = soup.find("section", class_="job-detail-content")
            description_text = (
                description_block.get_text(separator=" ", strip=True)
                if description_block else ""
            )

            return {
                "job_title": self._normalize_text(job_title),
                "job_description": self._normalize_text(description_text),
                "posting_date": posting_date,
                "job_url": job_url,
                "platform": self.platform,
                "scraped_at": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"[_scrape_job_detail] Detail scrape error: {e}")
            self.errors.append(str(e))
            return None

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------
    @staticmethod
    def _get_text(
        soup: BeautifulSoup,
        tag: str,
        class_name: str
    ) -> str:
        elem = soup.find(tag, class_=class_name)
        return elem.get_text(strip=True) if elem else ""

    @staticmethod
    def _normalize_text(text: str) -> str:
        text = text.replace("\u00a0", " ").replace("\u200b", "")
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _validate_job(job: Dict[str, Any]) -> bool:
        return bool(job.get("job_title") and job.get("job_description"))


# ------------------------------------------------------------------
# Example run
# ------------------------------------------------------------------
if __name__ == "__main__":
    scraper = CareerVietScraper(max_results=5)
    results = scraper.scrape(query="Data Analyst")
    logger.info(f"Collected {len(results)} jobs\n\n")
    print("\n\n",results[0])  # Print first job as a sample
