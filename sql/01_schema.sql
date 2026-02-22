-- Main fact table for jobs
CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_job_id TEXT NOT NULL, -- The job's unique ID on the source platform
    platform TEXT NOT NULL,
    job_url TEXT UNIQUE NOT NULL,
    job_title TEXT NOT NULL,
    company_name TEXT,
    location TEXT,
    posted_date DATE,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Transformed fields
    seniority_level TEXT,
    salary_min INTEGER,
    salary_max INTEGER,
    salary_currency TEXT
);

-- Dimension table for skills
CREATE TABLE IF NOT EXISTS skills (
    skill_id INTEGER PRIMARY KEY AUTOINCREMENT,
    skill_name TEXT UNIQUE NOT NULL,
    skill_category TEXT -- e.g., 'Programming Language', 'BI Tool', 'Cloud'
);

-- Bridge table to link jobs and skills (many-to-many)
CREATE TABLE IF NOT EXISTS job_skills (
    job_id INTEGER NOT NULL,
    skill_id INTEGER NOT NULL,
    PRIMARY KEY (job_id, skill_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    FOREIGN KEY (skill_id) REFERENCES skills(skill_id)
);

-- Raw job descriptions for reference and re-processing
CREATE TABLE IF NOT EXISTS job_descriptions (
    job_id INTEGER PRIMARY KEY,
    raw_description TEXT,
    clean_description TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_jobs_platform ON jobs(platform);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority ON jobs(seniority_level);
CREATE INDEX IF NOT EXISTS idx_skills_name ON skills(skill_name);
CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs(job_url);
