"""
Demo Mock Scraper - Returns sample data for testing the ETL pipeline.
Useful for portfolio demonstrations without relying on actual web scraping.
"""

import logging
from typing import List, Dict, Any
from src.scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class MockScraper(BaseScraper):
    """Demo scraper that returns sample job data for testing."""
    
    def __init__(self):
        super().__init__('mock')
        self.platform_name = 'mock'
    
    SAMPLE_JOBS = [
        {
            'id': '1',
            'platform': 'mock',
            'job_title': 'Senior Data Analyst',
            'company': 'Tech Corp Vietnam',
            'location': 'Ho Chi Minh City',
            'job_description': 'Looking for experienced Data Analyst with 5+ years in SQL, Python, Tableau. Must know distributed computing with Spark.',
            'job_url': 'https://example.com/job/1',
            'posting_date': '2026-02-08',
            'salary': '40,000,000 - 60,000,000 VND',
        },
        {
            'id': '2',
            'platform': 'mock',
            'job_title': 'Junior Data Engineer',
            'company': 'Data Solutions Inc',
            'location': 'Hanoi',
            'job_description': 'Entry-level Data Engineer needed. Experience with Python, PostgreSQL, and ETL pipelines required. Knowledge of AWS and Apache Airflow preferred.',
            'job_url': 'https://example.com/job/2',
            'posting_date': '2026-02-09',
            'salary': '25,000,000 - 35,000,000 VND',
        },
        {
            'id': '3',
            'platform': 'mock',
            'job_title': 'BI Developer',
            'company': 'Analytics Firm',
            'location': 'Da Nang',
            'job_description': 'Tableau and Power BI expert needed. Must have SQL and database design skills. 3+ years experience.',
            'job_url': 'https://example.com/job/3',
            'posting_date': '2026-02-07',
            'salary': '30,000,000 - 45,000,000 VND',
        },
        {
            'id': '4',
            'platform': 'mock',
            'job_title': 'Machine Learning Engineer',
            'company': 'AI Startup',
            'location': 'Ho Chi Minh City',
            'job_description': 'ML Engineer with Python, TensorFlow, and scikit-learn. Work with large datasets using Spark and Hadoop.',
            'job_url': 'https://example.com/job/4',
            'posting_date': '2026-02-09',
            'salary': '50,000,000 - 70,000,000 VND',
        },
        {
            'id': '5',
            'platform': 'mock',
            'job_title': 'Data Scientist',
            'company': 'E-Commerce Giant',
            'location': 'Ho Chi Minh City',
            'job_description': 'Senior Data Scientist needed. Expertise in Python, R, machine learning, and statistical analysis. Knowledge of AWS and Azure.',
            'job_url': 'https://example.com/job/5',
            'posting_date': '2026-02-06',
            'salary': '60,000,000 - 80,000,000 VND',
        },
        {
            'id': '6',
            'platform': 'mock',
            'job_title': 'Database Administrator',
            'company': 'Enterprise Solutions',
            'location': 'Hanoi',
            'job_description': 'DBA with experience in MySQL, PostgreSQL, and Oracle. Must know backup, recovery, and performance tuning.',
            'job_url': 'https://example.com/job/6',
            'posting_date': '2026-02-05',
            'salary': '35,000,000 - 50,000,000 VND',
        },
        {
            'id': '7',
            'platform': 'mock',
            'job_title': 'ETL Developer',
            'company': 'Data Integration Firm',
            'location': 'Ho Chi Minh City',
            'job_description': 'ETL specialist with Talend or Informatica. Skills in SQL, Python, and Apache Airflow. Working with large data pipelines.',
            'job_url': 'https://example.com/job/7',
            'posting_date': '2026-02-08',
            'salary': '45,000,000 - 65,000,000 VND',
        },
        {
            'id': '8',
            'platform': 'mock',
            'job_title': 'Data Analyst - Financial',
            'company': 'Bank Vietnam',
            'location': 'Ho Chi Minh City',
            'job_description': 'Financial Data Analyst required. SQL, Excel advanced skills, and knowledge of Power BI. Working with financial datasets.',
            'job_url': 'https://example.com/job/8',
            'posting_date': '2026-02-04',
            'salary': '35,000,000 - 55,000,000 VND',
        },
    ]
    
    def scrape(self, search_query: str = 'Data', max_pages: int = 1) -> List[Dict[str, Any]]:
        """
        Return mock job data for testing.
        
        Args:
            search_query: Ignored (for testing)
            max_pages: Ignored (all sample data returned)
            
        Returns:
            List of sample job records
        """
        logger.info(f"[Mock] Mock scraper started (demo mode, returns sample data)")
        logger.info(f"[Mock] Returning {len(self.SAMPLE_JOBS)} sample jobs for testing")
        
        self.jobs_scraped = len(self.SAMPLE_JOBS)
        return self.SAMPLE_JOBS
