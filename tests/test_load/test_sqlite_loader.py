import pytest
import sqlite3

from src.load.load_to_db import JobDataLoader
from src.utils.database import DatabaseManager

# a sample processed job record, as output by the transformer
MOCK_PROCESSED_JOB = {
    'platform_job_id': 'test_platform_123',
    'platform': 'test_platform',
    'job_url': 'https://example.com/job/123',
    'job_title': 'senior data analyst',
    'company_name': 'test company',
    'location': 'ho chi minh city',
    'posted_date': '2026-02-22',
    'scraped_at': '2026-02-22t12:00:00',
    'seniority_level': 'senior',
    'salary_min': 50000000,
    'salary_max': 70000000,
    'salary_currency': 'vnd',
    'raw_description': 'raw description with sql, python.',
    'clean_description': 'clean description with sql, python.',
    'extracted_skills': [
        {'skill_name': 'sql', 'skill_category': 'language'},
        {'skill_name': 'python', 'skill_category': 'language'}
    ]
}

@pytest.fixture
def in_memory_db_manager():
    """
    provides a databasemanager instance using an in-memory sqlite database.
    it sets up the schema for each test and tears it down afterwards.
    """
    manager = DatabaseManager(db_path=":memory:")
    manager.setup_database()
    yield manager
    manager.close_connection()

def test_sqlite_loader(in_memory_db_manager):
    """
    tests the jobdataloader to ensure it correctly inserts a processed job,
    its description, and its skills into the database.
    """
    # arrange
    loader = JobDataLoader()
    # override the default db manager with the in-memory one
    loader.db = in_memory_db_manager
    
    # act
    loader.load_jobs([MOCK_PROCESSED_JOB])

    # assert
    # 1. check if the job was inserted correctly
    job_query = "select * from jobs where job_url = ?"
    job_result = in_memory_db_manager.execute_query(job_query, (MOCK_PROCESSED_JOB['job_url'],), fetch_one=True)
    
    assert job_result is not None
    assert job_result['job_title'] == MOCK_PROCESSED_JOB['job_title']
    assert job_result['seniority_level'] == MOCK_PROCESSED_JOB['seniority_level']
    assert job_result['salary_min'] == MOCK_PROCESSED_JOB['salary_min']
    
    job_id = job_result['job_id']

    # 2. check if the description was inserted
    desc_query = "select * from job_descriptions where job_id = ?"
    desc_result = in_memory_db_manager.execute_query(desc_query, (job_id,), fetch_one=True)
    
    assert desc_result is not None
    assert desc_result['clean_description'] == MOCK_PROCESSED_JOB['clean_description']
    
    # 3. check if skills were inserted and linked
    skills_query = """
        select s.skill_name from skills s
        join job_skills js on s.skill_id = js.skill_id
        where js.job_id = ?
    """
    skills_result = in_memory_db_manager.execute_query(skills_query, (job_id,))
    
    assert skills_result is not None
    extracted_skill_names = {row['skill_name'] for row in skills_result}
    expected_skill_names = {skill['skill_name'] for skill in MOCK_PROCESSED_JOB['extracted_skills']}
    
    assert extracted_skill_names == expected_skill_names
