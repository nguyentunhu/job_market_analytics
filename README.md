# End-to-End Job Market Analytics Pipeline for Vietnam's Data Sector

## Project Goal

This project builds an end-to-end job market analytics pipeline for the Vietnam job market. It's designed to be a portfolio piece for a junior Data Analyst / Analytics Engineer, demonstrating core skills in data extraction, transformation, loading, and analysis. The pipeline is robust, maintainable, and focuses on clarity over complexity, using only Python and SQLite.

The pipeline:
- **Scrapes** job data from multiple leading Vietnamese job platforms.
- **Normalizes** and **enriches** the raw data.
- **Extracts** skills and tools demand.
- **Stores** data in an analytics-ready SQLite database.
- **Enables** SQL analysis and dashboarding (e.g., with Power BI or Streamlit).

---

## 🚀 Setup and Run

### Prerequisites

*   Python 3.8+
*   `pip` (Python package installer)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/job_market_analytics.git
    cd job_market_analytics
    ```
2.  **Create a virtual environment** (recommended):
    ```bash
    python -m venv venv
    .\venv\Scripts\activate # On Windows
    # source venv/bin/activate # On macOS/Linux
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### Running the Pipeline

To run the entire ETL pipeline, execute the `pipeline.py` script:

```bash
python src/pipeline.py
```

This will:
1.  Initialize the SQLite database (`data/job_market_analytics.db`).
2.  Scrape job data from configured platforms.
3.  Process and clean the scraped data.
4.  Load the data into the database tables.

### Running Tests

To execute the test suite and ensure everything is working as expected:

```bash
pytest tests/
```

---

## 📈 End-to-End Data Flow

The pipeline follows a clear Extract-Transform-Load (ETL) pattern:

```mermaid
graph TD
    A[Start Pipeline (pipeline.py)] --> B(Scrapers: CareerViet, TopCV, Vieclam24h, VietnamWorks)
    B -- Raw Job Listings (List[Dict]) --> C[Orchestrator (orchestrator.py)]
    C -- Aggregated Raw Jobs --> D[JobDataTransformer (job_transformer.py)]
    D -- Processed Jobs (List[Dict]) --> E[JobDataLoader (load_to_db.py)]
    E -- Insert Data --> F(SQLite Database: job_market_analytics.db)
    F -- SQL Queries --> G[SQL Analysis (02_analytics_views.sql)]
    G -- Analyzed Data --> H[Dashboard (Power BI / Streamlit)]
```

1.  **Extract (Scrapers)**: Individual scraper modules (e.g., `careerviet_scraper.py`, `topcv_scraper.py`) extend a `BaseScraper` class, providing platform-specific logic for navigating job listings and extracting raw job details. They handle HTTP requests, retries, and rate-limiting.
2.  **Orchestrator (`orchestrator.py`)**: Manages the concurrent execution of multiple scrapers using a thread pool. It aggregates all raw job data collected from different platforms into a single, unified list of dictionaries.
3.  **Transform (`job_transformer.py`)**: Takes the raw job data, cleans it, normalizes fields (job titles, company names, locations), detects seniority levels, extracts salary ranges, and identifies key skills and tools mentioned in job descriptions. It outputs a list of structured, analytics-ready job records.
4.  **Load (`load_to_db.py`)**: Connects to an SQLite database (`data/job_market_analytics.db`). It inserts the transformed job data into the `jobs`, `job_descriptions`, `skills`, and `job_skills` tables, handling deduplication and ensuring data integrity.
5.  **SQL Analysis (`02_analytics_views.sql`)**: Defines SQL views that simplify common analytical queries. These views serve as the direct data source for any reporting or dashboarding tools.
6.  **Dashboard (Power BI / Streamlit)**: A visualization layer (not implemented in this repository but enabled by the structured database) for exploring insights.

---

## 🛠️ Optimizations and Code Standards

To enhance clarity, maintainability, and productivity, the project underwent several optimizations:

*   **Removed Unnecessary Files**: Eliminated boilerplate and unused configuration files (`docker-compose.yml`, `Dockerfile`, `Makefile`, `setup.py`, `test_vw_structure.py`, `.env.template`) and the `airflow/` directory, aligning with the constraint of a lean, Python-only, SQLite-based solution.
*   **Centralized and Granular Logging**: Implemented a `LoggerFactory` to provide distinct log files for different pipeline stages (`scrapers.log`, `transformer.log`, `load.log`, `pipeline.log`). This improves debugging and monitoring by segregating logs based on their origin.
*   **Standardized Code Style**:
    *   **Comments**: All comments and docstrings are now in lowercase (excluding proper nouns and specific technical terms) for consistency.
    *   **Headings**: Multi-line comment headings (e.g., `===`) have been replaced with single-line `#` comments.
    *   **Emojis**: All emojis have been removed to maintain a professional and clean code aesthetic.

---

## 🧱 Final Database Schema

The SQLite database (`data/job_market_analytics.db`) is structured with a star-like schema to facilitate analytical queries.

```sql
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

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_jobs_platform ON jobs(platform);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority ON jobs(seniority_level);
CREATE INDEX IF NOT EXISTS idx_skills_name ON skills(skill_name);
CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs(job_url);
```

---

## 🧪 Suggested Validation Checks

To ensure data quality throughout the pipeline:

*   **Scraper Output**: Implement checks that `job_url` and `job_title` are consistently present and non-empty for every raw job record.
*   **Transformation Logic**:
    *   Verify that `salary_min` is never greater than `salary_max`.
    *   Confirm that skill extraction always returns a list (even if empty).
    *   Log any jobs where `seniority_level` is detected as "Not Specified" for review.
*   **Database Integrity**: After loading, run SQL queries to compare record counts per `platform` in the `jobs` table against the scraper's reported statistics.
    ```sql
    SELECT platform, COUNT(*) FROM jobs GROUP BY platform;
    ```

---

## 📊 Suggested Analytical Questions

This project is well-equipped to answer crucial questions about the Vietnamese data job market:

1.  **Skill Demand**: What are the top 10 most in-demand technical and soft skills for data professionals?
2.  **Tool Popularity**: Which BI tools (e.g., Power BI, Tableau) or cloud platforms (AWS, GCP, Azure) are most frequently requested?
3.  **Salary Benchmarking**: What are the typical salary ranges for different seniority levels (e.g., Junior, Senior) across various locations?
4.  **Platform Effectiveness**: Which job platforms are most effective for finding data roles, indicated by the volume of relevant postings?
5.  **Market Trends**: How have job postings or skill requirements changed over time? (Requires multiple runs over time.)
6.  **Geographic Distribution**: Which cities in Vietnam are the primary hubs for data analytics jobs?

---

## 📈 Dashboard Metrics & Visuals

A dashboard (e.g., in Power BI, Tableau, or Streamlit) can bring these insights to life:

### Key Performance Indicators (KPIs)
*   **Total Active Jobs**: A single card showing the total number of unique jobs in the database.
*   **Avg. Salary (Overall & by Seniority)**: Cards displaying the average minimum and maximum salaries, with filters for seniority.
*   **Top 5 In-Demand Skills**: A list or small chart of the most frequently mentioned skills.

### Visualizations

1.  **Bar Chart: "Top 15 Most Demanded Skills"**:
    *   **X-axis**: Skill Name
    *   **Y-axis**: Number of Job Postings mentioning the skill
    *   **Segmentation**: Color by `skill_category` (Language, Tool, Platform, etc.)
2.  **Bar Chart: "Job Count by Platform"**:
    *   **X-axis**: Job Platform
    *   **Y-axis**: Total Jobs Scraped
    *   *Insight*: Identify which platforms are richest in data job postings.
3.  **Box Plot / Bar Chart: "Salary Distribution by Seniority Level"**:
    *   **X-axis**: Seniority Level (Intern, Junior, Mid-level, Senior, Manager/Lead)
    *   **Y-axis**: Salary Range (Min/Max, or Average)
    *   *Insight*: Understand compensation differences.
4.  **Map / Bar Chart: "Job Distribution by Location"**:
    *   **Visual**: A map of Vietnam with circles sized by job count, or a bar chart of top cities.
    *   **X-axis**: City
    *   **Y-axis**: Number of Jobs
    *   *Insight*: Pinpoint key employment centers.
5.  **Line Chart: "Job Postings Over Time"**:
    *   **X-axis**: `posted_date` (month/year)
    *   **Y-axis**: Count of new job postings
    *   *Insight*: Track market growth or seasonal trends.

### Filters / Slicers
*   **Job Title**: Filter by specific keywords in job titles.
*   **Location**: Filter by city/region.
*   **Seniority Level**: Isolate junior vs. senior roles.
*   **Platform**: View data from specific sources.

---

## 📚 Future Enhancements

*   **Advanced Salary Parsing**: Implement more sophisticated NLP models for better salary extraction, including handling various units (USD, monthly/annual) and implicit ranges.
*   **Dynamic Skill Extraction**: Explore machine learning models (e.g., spaCy) for more accurate and context-aware skill extraction beyond keyword matching.
*   **Company Data Enrichment**: Integrate with external APIs (e.g., LinkedIn, Crunchbase) to enrich company profiles.
*   **Scheduler Integration**: For continuous operation, integrate with a scheduler (e.g., systemd timers, Windows Task Scheduler) to run `pipeline.py` periodically.
*   **Web Interface**: Implement a simple web interface (e.g., with Streamlit or Flask) to display real-time insights from the database.

This project provides a solid foundation for understanding the data engineering lifecycle and generating valuable insights for career development in the data sector.
