import pytest
from unittest.mock import patch, MagicMock

from src.scrapers.vietnamworks_scraper import VietnamWorksScraper

# vietnamworks sometimes uses a "flight" mechanism where job data is embedded
# as json in a script tag or fetched via a separate api call after page load.
# this test simulates parsing data from such a mechanism, assuming the scraper
# can extract this json data. for this example, we'll mock the json response
# directly, assuming a helper method `_fetch_flight_data` would exist.

MOCK_FLIGHT_JSON = {
    "jobTitle": "data visualization expert (tableau)",
    "companyName": "global analytics inc.",
    "locations": [{"name": "da nang"}],
    "jobDescription": "<p>we are seeking an expert in <b>tableau</b> to create stunning dashboards.</p>",
    "jobUrl": "https://www.vietnamworks.com/data-visualization-expert-tableau-12345.html",
    "postedDate": "2026-02-20t10:00:00.000z"
}

@pytest.fixture
def scraper():
    """provides a vietnamworksscraper instance for tests."""
    return VietnamWorksScraper()

@patch('src.scrapers.base_scraper.BaseScraper._fetch_page')
def test_vietnamworks_detail_parser(mock_fetch_page, scraper):
    """
    tests the `_scrape_job_detail` method of vietnamworkscraper to ensure it
    correctly parses job details. this version assumes standard html parsing.
    """
    # arrange
    # this html is simplified. a real page would be more complex.
    mock_html = f"""
    <html><body>
        <h1 class="job-detail__title">{MOCK_FLIGHT_JSON['jobTitle']}</h1>
        <a class="employer-name">{MOCK_FLIGHT_JSON['companyName']}</a>
        <p class="job-detail__location">{MOCK_FLIGHT_JSON['locations'][0]['name']}</p>
        <div class="job-detail__description">{MOCK_FLIGHT_JSON['jobDescription']}</div>
        <div class="job-detail__posted-date">{MOCK_FLIGHT_JSON['postedDate']}</div>
    </body></html>
    """
    mock_soup = BeautifulSoup(mock_html, "html.parser")
    mock_fetch_page.return_value = mock_soup
    
    # act
    job_details = scraper._scrape_job_detail(MOCK_FLIGHT_JSON['jobUrl'])

    # assert
    assert job_details is not None
    assert job_details['job_title'].lower() == MOCK_FLIGHT_JSON['jobTitle'].lower()
    assert job_details['company'].lower() == MOCK_FLIGHT_JSON['companyName'].lower()
    assert job_details['location'].lower() == MOCK_FLIGHT_JSON['locations'][0]['name'].lower()
    assert "tableau" in job_details['job_description'].lower()
    assert job_details['job_url'] == MOCK_FLIGHT_JSON['jobUrl']
    assert job_details['platform'] == "vietnamworks"
