import logging
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from src.scrapers.topcv_scraper import TopCVScraper
from src.scrapers.careerviet_scraper import CareerVietScraper
from src.scrapers.vieclam24h_scraper import Vieclam24hScraper

logger = logging.getLogger(__name__)


class ScraperOrchestrator:
    """
    Orchestrates multiple job scrapers.
    Scrapers are responsible for pagination & limits.
    """

    SCRAPERS = {
        "topcv": TopCVScraper,
        "careerviet": CareerVietScraper,
        "vieclam24h": Vieclam24hScraper,
    }

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers

    def scrape_all(
        self,
        query: str,
        enabled_platforms: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        start_time = datetime.now()
        jobs: List[Dict[str, Any]] = []
        platform_stats = {}

        platforms = enabled_platforms or list(self.SCRAPERS.keys())

        logger.info(
            f"Scraping platforms={platforms} | query='{query}' | workers={self.max_workers}"
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            for name in platforms:
                if name not in self.SCRAPERS:
                    logger.warning(f"Unknown platform: {name}")
                    continue

                scraper = self.SCRAPERS[name]()
                futures[executor.submit(self._run_scraper, name, scraper, query)] = name

            for future in as_completed(futures):
                platform = futures[future]
                try:
                    result = future.result()
                    jobs.extend(result["jobs"])
                    platform_stats[platform] = result["stats"]

                    logger.info(
                        f"[OK] {platform}: {len(result['jobs'])} jobs"
                    )

                except Exception as e:
                    logger.error(f"[FAIL] {platform}: {e}")
                    platform_stats[platform] = {
                        "platform": platform,
                        "status": "failed",
                        "error": str(e),
                    }

        return {
            "query": query,
            "total_jobs": len(jobs),
            "jobs": jobs,
            "platform_stats": platform_stats,
            "started_at": start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
        }

    @staticmethod
    def _run_scraper(platform: str, scraper, query: str) -> Dict[str, Any]:
        jobs = scraper.scrape(query=query)
        stats = scraper.get_statistics() if hasattr(scraper, "get_statistics") else {}
        scraper.close()

        return {
            "platform": platform,
            "jobs": jobs,
            "stats": stats,
        }