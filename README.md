# End-to-End Job Market Analytics Pipeline for Vietnam's Data Sector

## Project Goal

This project builds an end-to-end job market analytics pipeline for the Vietnam job market.

The pipeline:
- **Scrapes** job data from multiple leading Vietnamese job platforms.
- **Normalizes** and **enriches** the raw data.
- **Extracts** skills and tools demand.
- **Stores** data in an analytics-ready SQLite database.
- **Enables** SQL analysis and dashboarding (e.g., with Power BI or Streamlit).

---

## Setup and Run

### Prerequisites

*   Python 3.8+
*   `pip` (Python package installer)

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/nguyentunhu/job_market_analytics.git
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
    A[Start Pipeline (pipeline.py)] --> B(Scrapers: CareerViet, TopCV, Vieclam24h)
    B -- Raw Job Listings (List[Dict]) --> C[Orchestrator (orchestrator.py)]
    C -- Aggregated Raw Jobs --> D[JobDataTransformer (job_transformer.py)]
    D -- Processed Jobs (List[Dict]) --> E[JobDataLoader (load_to_db.py)]
    E -- Insert Data --> F(SQLite Database: job_market_analytics.db)
    F -- SQL Queries --> G[SQL Analysis (02_analytics_views.sql)]
    G -- Analyzed Data --> H[Dashboard (not implemented yet)]
```

1.  **Extract (Scrapers)**: Individual scraper modules (`careerviet_scraper.py`, `topcv_scraper.py` and `vieclam24h_scraper.py`) extend a `BaseScraper` class, providing platform-specific logic for navigating job listings and extracting raw job details. They handle HTTP requests, retries, and rate-limiting.
2.  **Orchestrator (`orchestrator.py`)**: Manages the concurrent execution of multiple scrapers using a thread pool. It aggregates all raw job data collected from different platforms into a single, unified list of dictionaries.
3.  **Transform (`job_transformer.py`)**: Takes the raw job data, cleans it, normalizes fields (job titles, company names, locations), detects seniority levels, extracts salary ranges, and identifies key skills and tools mentioned in job descriptions. It outputs a list of structured, analytics-ready job records.
4.  **Load (`load_to_db.py`)**: Connects to an SQLite database (`data/job_market_analytics.db`). It inserts the transformed job data into the `jobs`, `job_descriptions`, `skills`, and `job_skills` tables, handling deduplication and ensuring data integrity.
5.  **SQL Analysis (`02_analytics_views.sql`)**: Defines SQL views that simplify common analytical queries. These views serve as the direct data source for any reporting or dashboarding tools.
6.  **Dashboard (not implemented yet)**: A visualization layer for exploring insights.


---

## Database Schema

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

