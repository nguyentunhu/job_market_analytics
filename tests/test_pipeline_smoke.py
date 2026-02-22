import pytest
from unittest.mock import patch, MagicMock

from src.pipeline import run_pipeline

# a mock raw job record that scrapers would return
MOCK_RAW_JOB = {
    "job_title": "senior data analyst (sql, python)",
    "company": "tech corp",
    "location": "ho chi minh city",
    "job_description": "we need a senior data analyst with strong sql and python skills. experience with power bi is a plus. must have 5 years of experience.",
    "job_url": "https://example.com/job/123",
    "posting_date": "2026-02-22",
    "platform": "mock_platform",
    "scraped_at": "2026-02-22t12:00:00",
}

@patch('src.orchestrator.Orchestrator.scrape')
@patch('src.load.load_to_db.JobDataLoader.load_jobs')
@patch('src.utils.database.DatabaseManager.setup_database')
def test_pipeline_smoke_run(mock_setup_db, mock_load_jobs, mock_scrape):
    """
    a smoke test to ensure the main pipeline runs without crashing.
    it mocks the external dependencies (scraping, loading) to run quickly.
    """
    # arrange: configure the mocks
    mock_scrape.return_value = [MOCK_RAW_JOB] # simulate one scraped job
    mock_load_jobs.return_value = None # no return value needed
    mock_setup_db.return_value = None # no return value needed

    # act & assert: the pipeline should run to completion without raising an exception
    try:
        run_pipeline()
    except Exception as e:
        pytest.fail(f"pipeline smoke test failed with an exception: {e}")

    # assert: verify that the core components were called
    mock_setup_db.assert_called_once()
    mock_scrape.assert_called_once()
    mock_load_jobs.assert_called_once()
