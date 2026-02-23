import pytest
from bs4 import BeautifulSoup
from unittest.mock import patch

from src.scrapers.topcv_scraper import TopCVScraper

# mock html content for a topcv job detail page.
# this simulates the html structure the scraper expects to parse.
MOCK_TOPCV_HTML = """
<html>
<body>
    <div class="job-detail__info">
        <h1 class="job-detail__info--title">chuyên viên phân tích dữ liệu (data analyst)</h1>
    </div>
    <div class="job-detail__information-detail">
        <h2>chi tiết tin tuyển dụng</h2>
        <div class="content">
            <h3>mô tả công việc</h3>
            <p>- thu thập, tổng hợp, phân tích dữ liệu.</p>
            <p>- xây dựng các báo cáo, dashboard.</p>
            <h3>yêu cầu ứng viên</h3>
            <p>- có kinh nghiệm sử dụng sql, python.</p>
            <p>- biết power bi là một lợi thế.</p>
        </div>
    </div>
    <a class="company-name">awesome tech company</a>
    <div class="box-address">
        <span class="address">hà nội</span>
    </div>
</body>
</html>
"""

@pytest.fixture
def scraper():
    """provides a topcvscraper instance for tests."""
    return TopCVScraper()

@patch('src.scrapers.base_scraper.BaseScraper._fetch_page')
def test_topcv_detail_parser(mock_fetch_page, scraper):
    """
    tests the `_scrape_job_detail` method of topcvscraper to ensure it
    correctly parses job details from mock html content.
    """
    # arrange
    mock_soup = BeautifulSoup(MOCK_TOPCV_HTML, "html.parser")
    mock_fetch_page.return_value = mock_soup
    test_url = "https://www.topcv.vn/viec-lam/chuyen-vien-phan-tich-du-lieu-data-analyst/12345.html"

    # act
    job_details = scraper._scrape_job_detail(test_url)

    # assert
    assert job_details is not None
    assert job_details['job_title'].lower() == "chuyên viên phân tích dữ liệu (data analyst)"
    assert "sql, python" in job_details['job_description'].lower()
    assert "power bi" in job_details['job_description'].lower()
    assert job_details['company'].lower() == "awesome tech company"
    assert job_details['location'].lower() == "hà nội"
    assert job_details['job_url'] == test_url
    assert job_details['platform'] == "topcv"
