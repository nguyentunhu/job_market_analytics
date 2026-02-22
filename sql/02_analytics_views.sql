-- Analytics Views for Job Market Data
-- These views simplify common analytical queries for dashboarding and reporting.

-- View 1: job_details_view
-- Combines core job information with cleaned descriptions and seniority.
CREATE VIEW IF NOT EXISTS job_details_view AS
SELECT
    j.job_id,
    j.platform,
    j.job_url,
    j.job_title,
    j.company_name,
    j.location,
    j.posted_date,
    j.scraped_at,
    j.seniority_level,
    j.salary_min,
    j.salary_max,
    j.salary_currency,
    jd.clean_description,
    jd.raw_description
FROM
    jobs j
JOIN
    job_descriptions jd ON j.job_id = jd.job_id;

-- View 2: skill_demand_view
-- Aggregates the count of each skill across all jobs.
CREATE VIEW IF NOT EXISTS skill_demand_view AS
SELECT
    s.skill_name,
    s.skill_category,
    COUNT(js.job_id) AS job_count,
    CAST(COUNT(js.job_id) AS REAL) * 100 / (SELECT COUNT(DISTINCT job_id) FROM job_skills) AS percentage_of_jobs
FROM
    skills s
JOIN
    job_skills js ON s.skill_id = js.skill_id
GROUP BY
    s.skill_name, s.skill_category
ORDER BY
    job_count DESC;

-- View 3: salary_by_seniority_location_view
-- Calculates average salary ranges by seniority level and location.
CREATE VIEW IF NOT EXISTS salary_by_seniority_location_view AS
SELECT
    seniority_level,
    location,
    AVG(salary_min) AS avg_salary_min,
    AVG(salary_max) AS avg_salary_max,
    salary_currency,
    COUNT(job_id) AS job_count
FROM
    jobs
WHERE
    salary_min IS NOT NULL AND salary_max IS NOT NULL AND seniority_level IS NOT NULL AND location IS NOT NULL
GROUP BY
    seniority_level, location, salary_currency
ORDER BY
    seniority_level, location;

-- View 4: platform_job_count_view
-- Shows the total number of jobs found per platform.
CREATE VIEW IF NOT EXISTS platform_job_count_view AS
SELECT
    platform,
    COUNT(job_id) AS total_jobs
FROM
    jobs
GROUP BY
    platform
ORDER BY
    total_jobs DESC;

-- View 5: job_postings_over_time_view
-- Tracks the number of job postings over time.
CREATE VIEW IF NOT EXISTS job_postings_over_time_view AS
SELECT
    DATE(posted_date) AS date,
    COUNT(job_id) AS daily_job_count
FROM
    jobs
WHERE posted_date IS NOT NULL
GROUP BY
    DATE(posted_date)
ORDER BY
    date;
