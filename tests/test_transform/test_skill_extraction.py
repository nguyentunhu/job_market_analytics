import pytest

from src.transform.job_transformer import JobDataTransformer

# test cases: (description, expected_skills)
# each tuple contains a job description string and a list of expected skill names.
TEST_CASES = [
    (
        "we are looking for a data analyst with strong skills in sql, python, and tableau. knowledge of power bi is a plus.",
        ["sql", "python", "tableau", "power bi"]
    ),
    (
        "must be proficient in excel and have experience with r. aws or gcp knowledge is highly desirable.",
        ["excel", "r", "aws", "gcp"]
    ),
    (
        "the ideal candidate is a great communicator and problem-solver.",
        ["communication", "problem solving"]
    ),
    (
        "no relevant tech skills mentioned in this description.",
        []
    ),
    (
        "experience with spark, hadoop, and other big data technologies is required. familiar with git.",
        ["spark", "hadoop", "git"]
    ),
    (
        "requires python (pandas, numpy) and sql.",
        ["python", "pandas", "numpy", "sql"]
    ),
]

@pytest.fixture
def transformer():
    """provides a jobdatatransformer instance for tests."""
    return JobDataTransformer()

@pytest.mark.parametrize("description, expected_skills", TEST_CASES)
def test_skill_extraction(transformer, description, expected_skills):
    """
    tests that skills are correctly extracted from a job description.
    """
    # arrange
    raw_job = {
        "job_title": "data analyst",
        "job_description": description,
        "platform": "test",
        "job_url": "http://example.com/job/1"
    }

    # act
    processed_job = transformer.transform_job(raw_job)
    extracted_skill_names = [skill['skill_name'].lower() for skill in processed_job['extracted_skills']]

    # assert
    assert processed_job is not None
    # use set for order-agnostic comparison
    assert set(extracted_skill_names) == set(expected_skills)
