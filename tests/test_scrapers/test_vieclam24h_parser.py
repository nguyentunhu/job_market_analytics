import pytest
from bs4 import BeautifulSoup
from unittest.mock import patch

from src.scrapers.vieclam24h_scraper import Vieclam24hScraper

# mock html content for a vieclam24h job detail page.
MOCK_VIECLAM24H_HTML = """
<html>
<body>
    <div class="text-24 font-bold leading-10 text-se-neutral-84 !font-medium">
        data analyst (sql)
    </div>
    <div class="text-14 font-normal leading-6 text-se-neutral-84 line-clamp-2 break-words">
        hạn nộp hồ sơ: 28/02/2026
    </div>
    <div class="flex flex-col gap-8 w-full sm_cv:gap-6">
        <p><strong>mô tả công việc:</strong></p>
        <p>phân tích dữ liệu bán hàng, làm báo cáo.</p>
        <p><strong>yêu cầu:</strong></p>
        <p>thành thạo sql, có kinh nghiệm với python.</p>
    </div>
</body>
</html>
"""

@pytest.fixture
def scraper():
    """provides a vieclam24hscraper instance for tests."""
    return Vieclam24hScraper()

@patch('src.scrapers.base_scraper.BaseScraper._fetch_page')
def test_vieclam24h_detail_parser(mock_fetch_page, scraper):
    """
    tests the `_scrape_job_detail` method of vieclam24hscraper to ensure it
    correctly parses job details from mock html content.
    """
    # arrange
    mock_soup = BeautifulSoup(MOCK_VIECLAM24H_HTML, "html.parser")
    mock_fetch_page.return_value = mock_soup
    test_url = "https://vieclam24h.vn/chi-tiet-cong-viec/123-data-analyst-sql"

    # act
    job_details = scraper._scrape_job_detail(test_url)

    # assert
    assert job_details is not None
    assert job_details['job_title'].lower() == "data analyst (sql)"
    assert "phân tích dữ liệu bán hàng" in job_details['job_description'].lower()
    assert "thành thạo sql" in job_details['job_description'].lower()
    assert "hạn nộp hồ sơ: 28/02/2026" in job_details['posting_date'].lower()
    assert job_details['job_url'] == test_url
    assert job_details['platform'] == "vieclam24h"
