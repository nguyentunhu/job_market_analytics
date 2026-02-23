import pytest

from src.transform.job_transformer import JobDataTransformer

# test cases: (job_title, description, expected_seniority)
# expected_seniority should be in the standardized 'snake_case' format.
TEST_CASES = [
    ("senior data analyst", "looking for an experienced professional.", "senior"),
    ("data analyst", "this is an entry-level position.", "junior"),
    ("thực tập sinh data", "opportunity for interns.", "intern"),
    ("lead data engineer", "will manage a team of 3 engineers.", "manager_lead"),
    ("data analyst (fresher)", "open for fresh graduates.", "junior"),
    ("chuyên viên phân tích dữ liệu", "3-5 years of experience required.", "mid_level"),
    ("director of analytics", "oversee the entire data department.", "director_vp"),
    ("data analyst", "no seniority mentioned.", "not_specified"),
]

@pytest.fixture
def transformer():
    """provides a jobdatatransformer instance for tests."""
    return JobDataTransformer()

@pytest.mark.parametrize("job_title, description, expected_seniority", TEST_CASES)
def test_seniority_detection(transformer, job_title, description, expected_seniority):
    """
    tests that seniority level is correctly detected from job title and description.
    """
    # arrange
    raw_job = {
        "job_title": job_title,
        "job_description": description,
        "platform": "test",
        "job_url": "http://example.com/job/1"
    }

    # act
    processed_job = transformer.transform_job(raw_job)

    # assert
    assert processed_job is not None
    assert processed_job['seniority_level'] == expected_seniority
