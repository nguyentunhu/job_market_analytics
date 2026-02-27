"""
Orchestrator for running all job platform scrapers.
Manages concurrent scraping, error handling, and result aggregation.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from src.scrapers.topcv_scraper import TopCVScraper
from src.scrapers.careerviet_scraper import CareerVietScraper
from src.scrapers.vieclam24h_scraper import Vieclam24hScraper

logger = logging.getLogger(__name__)


class ScraperOrchestrator:
    """Orchestrates scraping across all platforms."""
    

    SCRAPERS = {
        'topcv': TopCVScraper,
        'careerviet': CareerVietScraper,
        'vieclam24h': Vieclam24hScraper,
    }

    def __init__(self, max_workers: int = 3):
        """
        Initialize scraper orchestrator.
        
        Args:
            max_workers: Maximum number of concurrent scraping tasks
        """
        self.max_workers = max_workers
        self.all_jobs = []
        self.scraper_stats = {}
        self.start_time = None
        self.end_time = None

    def scrape_all_platforms(self, search_query: str = "Data Analyst", 
                            max_pages: int = 5,
                            enabled_platforms: Optional[List[str]] = None) -> List[Dict]:
        """
        Scrape all platforms concurrently.
        
        Args:
            search_query: Job title to search for
            max_pages: Maximum pages per platform
            enabled_platforms: Specific platforms to scrape (None = all platforms)
            
        Returns:
            List of job dictionaries from all platforms
        """
        # Use all available platforms if none specified
        platforms_to_scrape = enabled_platforms if enabled_platforms else list(self.SCRAPERS.keys())
        
        logger.info(f"Scraping {len(platforms_to_scrape)} platforms: {', '.join(platforms_to_scrape)}")
        logger.info(f"Search query: '{search_query}' | Max pages: {max_pages}")
        
        all_jobs = []
        scraper_tasks = {}
        
        # Submit all scraping tasks to thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for platform_name in platforms_to_scrape:
                if platform_name not in self.SCRAPERS:
                    logger.warning(f"Platform '{platform_name}' not found. Available: {list(self.SCRAPERS.keys())}")
                    continue
                
                # Instantiate the scraper class
                scraper_class = self.SCRAPERS[platform_name]
                scraper = scraper_class()
                
                scraper_tasks[platform_name] = executor.submit(
                    self._scrape_platform,
                    platform_name,
                    scraper,
                    search_query,
                    max_pages
                )
            
            # Collect results as they complete
            for platform_name, future in scraper_tasks.items():
                try:
                    jobs, stats = future.result(timeout=60)
                    self.scraper_stats[platform_name] = {
                        'jobs_scraped': len(jobs),
                        'platform': platform_name,
                        'status': 'success'
                    }
                    all_jobs.extend(jobs)
                    logger.info(f"[OK] {platform_name}: {len(jobs)} jobs")
                
                except Exception as e:
                    self.scraper_stats[platform_name] = {
                        'jobs_scraped': 0,
                        'platform': platform_name,
                        'status': 'failed',
                        'error': str(e)
                    }
                    logger.error(f"[ERROR] {platform_name}: {str(e)}")
        
        return all_jobs

    def _scrape_platform(self, platform_name: str, scraper, 
                        search_query: str, max_pages: int) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Scrape a single platform."""
        try:
            jobs = scraper.scrape(search_query=search_query, max_pages=max_pages)
            stats = scraper.get_statistics() if hasattr(scraper, 'get_statistics') else {}
            scraper.close()
            return jobs, stats
        except Exception as e:
            logger.error(f"Platform scraping error for {platform_name}: {e}")
            raise
    
    def _log_summary(self):
        """Log scraping summary."""
        duration = (self.end_time - self.start_time).total_seconds()
        total_jobs = len(self.all_jobs)
        
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total duration: {duration:.2f} seconds")
        logger.info(f"Total jobs collected: {total_jobs}")
        
        for platform, stats in self.scraper_stats.items():
            jobs = stats.get('jobs_scraped', 0)
            errors = stats.get('errors', 0)
            logger.info(f"  {platform}: {jobs} jobs, {errors} errors")
        
        logger.info("=" * 60)
    
    def get_summary(self) -> Dict[str, Any]:
        """Return summary of scraping session."""
        if not self.start_time or not self.end_time:
            return {}
        
        return {
            'total_jobs': len(self.all_jobs),
            'duration_seconds': (self.end_time - self.start_time).total_seconds(),
            'platform_stats': self.scraper_stats,
            'timestamp': datetime.now().isoformat()
        }


def run_scraping_task(search_query: str = 'Data Analyst', 
                     max_pages: int = 5,
                     enabled_platforms: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Main entry point for scraping all platforms.
    
    Args:
        search_query: Job title to search for
        max_pages: Maximum pages per platform
        enabled_platforms: List of platforms to include
        
    Returns:
        List of scraped jobs
    """
    orchestrator = ScraperOrchestrator()
    jobs = orchestrator.scrape_all_platforms(
        search_query=search_query,
        max_pages=max_pages,
        enabled_platforms=enabled_platforms
    )
    return jobs
