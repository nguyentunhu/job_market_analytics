"""
orchestrator for running all job platform scrapers.
manages concurrent scraping, error handling, and result aggregation.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# import specific scrapers
from src.scrapers.careerviet_scraper import CareerVietScraper
from src.scrapers.topcv_scraper import TopCVScraper
from src.scrapers.vieclam24h_scraper import Vieclam24hScraper

logger = logging.getLogger('scrapers')


class Orchestrator:
    """orchestrates scraping across all platforms."""
    
    SCRAPERS = {
        'careerviet': CareerVietScraper,
        'topcv': TopCVScraper,
        'vieclam24h': Vieclam24hScraper,
    }

    def __init__(self, max_workers: int = 3, max_results_per_platform: int = 200, request_delay: float = 2.0):
        """
        initialize scraper orchestrator.
        
        args:
            max_workers: maximum number of concurrent scraping tasks.
            max_results_per_platform: max jobs to scrape per platform.
            request_delay: delay between http requests for individual scrapers.
        """
        self.max_workers = max_workers
        self.max_results_per_platform = max_results_per_platform
        self.request_delay = request_delay
        self.all_jobs = []
        self.platform_stats = {}
        self.start_time = None
        self.end_time = None

    def scrape(self, query: str = "Data Analyst", max_pages: int = 5, enabled_platforms: Optional[List[str]] = None) -> List[Dict]:
        """
        scrape all platforms concurrently for a given query.
        
        args:
            query: job title to search for.
            max_pages: maximum number of pages to scrape per platform.
            enabled_platforms: specific platforms to scrape (none = all platforms).
            
        returns:
            list of raw job dictionaries from all platforms.
        """
        self.start_time = datetime.now()
        
        # use all available platforms if none specified
        platforms_to_scrape = enabled_platforms if enabled_platforms else list(self.SCRAPERS.keys())
        
        logger.info(f"scraping {len(platforms_to_scrape)} platforms: {', '.join(platforms_to_scrape)}")
        logger.info(f"search query: '{query}' | max results per platform: {self.max_results_per_platform} | max pages per platform: {max_pages}")
        
        all_raw_jobs = []
        scraper_tasks = {}
        
        # submit all scraping tasks to thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for platform_name in platforms_to_scrape:
                if platform_name not in self.SCRAPERS:
                    logger.warning(f"platform '{platform_name}' not found. available: {list(self.SCRAPERS.keys())}")
                    continue
                
                # instantiate the scraper class with common parameters
                scraper_class = self.SCRAPERS[platform_name]
                scraper = scraper_class(
                    max_results=self.max_results_per_platform,
                    request_delay=self.request_delay
                )
                
                scraper_tasks[platform_name] = executor.submit(
                    self._run_single_scraper,
                    scraper,
                    query,
                    max_pages,
                )
            
            # collect results as they complete
            for platform_name, future in scraper_tasks.items():
                try:
                    jobs_from_platform, stats_from_platform = future.result()
                    self.platform_stats[platform_name] = {
                        'jobs_scraped': len(jobs_from_platform),
                        'errors': stats_from_platform.get('errors', 0),
                        'duration': stats_from_platform.get('run_duration_seconds', 0),
                        'status': 'success'
                    }
                    all_raw_jobs.extend(jobs_from_platform)
                    logger.info(f"{platform_name}: {len(jobs_from_platform)} jobs scraped, {stats_from_platform.get('errors', 0)} errors.")
                
                except Exception as e:
                    self.platform_stats[platform_name] = {
                        'jobs_scraped': 0,
                        'errors': 1,
                        'duration': 0,
                        'status': 'failed',
                        'error_message': str(e)
                    }
                    logger.error(f"[error] {platform_name}: {str(e)}")
        
        self.end_time = datetime.now()
        self._log_summary()
        return all_raw_jobs

    def _run_single_scraper(self, scraper, query: str, max_pages: int) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """runs a single scraper and returns its results and statistics."""
        try:
            # each scraper's scrape method now takes query and max_pages directly
            jobs = scraper.scrape(query=query, max_pages=max_pages) 
            stats = scraper.get_statistics()
            scraper.close()
            return jobs, stats
        except Exception as e:
            logger.error(f"single scraper error for {scraper.platform}: {e}")
            scraper.close() # ensure session is closed even on error
            raise # re-raise to be caught by the orchestrator
    
    def _log_summary(self):
        """log scraping summary."""
        if not self.start_time or not self.end_time:
            logger.warning("scraping session not completed, cannot log summary.")
            return

        duration = (self.end_time - self.start_time).total_seconds()
        total_jobs = sum(s['jobs_scraped'] for s in self.platform_stats.values())
        
        logger.info("scraping summary")
        logger.info(f"total duration: {duration:.2f} seconds")
        logger.info(f"total jobs collected: {total_jobs}")
        
        for platform, stats in self.platform_stats.items():
            status_msg = f"  {platform}: {stats.get('jobs_scraped', 0)} jobs, {stats.get('errors', 0)} errors ({stats.get('status', 'unknown')})"
            if stats.get('error_message'):
                status_msg += f" - {stats['error_message']}"
            logger.info(status_msg)
        
    def get_summary(self) -> Dict[str, Any]:
        """return summary of scraping session."""
        if not self.start_time or not self.end_time:
            return {}
        
        return {
            'total_jobs_scraped': sum(s['jobs_scraped'] for s in self.platform_stats.values()),
            'total_duration_seconds': (self.end_time - self.start_time).total_seconds(),
            'platform_stats': self.platform_stats,
            'started_at': self.start_time.isoformat(),
            'finished_at': self.end_time.isoformat()
        }
