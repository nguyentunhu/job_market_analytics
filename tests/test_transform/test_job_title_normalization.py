import pytest

from src.transform.job_transformer import JobDataTransformer

# test cases: (input_title, expected_normalized_title)
# the user's previous `job_transformer.py` had a `_normalize_job_title` method,
# which has been removed in the refactoring. the current transformer uses the raw title.
# this test will verify that the raw title is passed through correctly.
# if normalization logic were to be re-introduced, these tests would be the place for it.

TEST_CASES = [
    ("senior data analyst", "senior data analyst"),
    ("data analyst (sql/python)", "data analyst (sql/python)"),
    ("chuyên viên phân tích dữ liệu", "chuyên viên phân tích dữ liệu"),
    ("   junior data analyst   ", "   junior data analyst   "), # assuming no stripping is done
    ("data SCIENTIST", "data SCIENTIST"),
]

@pytest.fixture
def transformer():
    """provides a jobdatatransformer instance for tests."""
    return JobDataTransformer()

@pytest.mark.parametrize("input_title, expected_title", TEST_CASES)
def test_job_title_is_preserved(transformer, input_title, expected_title):
    """
    tests that the job title from the raw job is preserved in the transformed output.
    the current implementation does not normalize the title, so input should equal output.
    """
    # arrange
    raw_job = {
        "job_title": input_title,
        "job_description": "some description",
        "platform": "test",
        "job_url": "http://example.com/job/1"
    }

    # act
    processed_job = transformer.transform_job(raw_job)

    # assert
    assert processed_job is not None
    assert processed_job['job_title'] == expected_title
