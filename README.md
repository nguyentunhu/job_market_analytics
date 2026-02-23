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
python -m src.pipeline
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

## End-to-End Data Flow

The pipeline follows a clear Extract-Transform-Load (ETL) pattern:

1.  **Extract (Scrapers)**: Individual scraper modules (e.g., `careerviet_scraper.py`, `topcv_scraper.py`) extend a `BaseScraper` class, providing platform-specific logic for navigating job listings and extracting raw job details. They handle HTTP requests, retries, and rate-limiting.
2.  **Orchestrator (`orchestrator.py`)**: Manages the concurrent execution of multiple scrapers using a thread pool. It aggregates all raw job data collected from different platforms into a single, unified list of dictionaries.
3.  **Transform (`job_transformer.py`)**: Takes the raw job data, cleans it, normalizes fields (job titles, company names, locations), detects seniority levels, extracts salary ranges, and identifies key skills and tools mentioned in job descriptions. It outputs a list of structured, analytics-ready job records.
4.  **Load (`load_to_db.py`)**: Connects to an SQLite database (`data/job_market_analytics.db`). It inserts the transformed job data into the `jobs`, `job_descriptions`, `skills`, and `job_skills` tables, handling deduplication and ensuring data integrity.
5.  **SQL Analysis (`02_analytics_views.sql`)**: Defines SQL views that simplify common analytical queries. These views serve as the direct data source for any reporting or dashboarding tools.
6.  **Dashboard (not implemented yet)**: A visualization layer (not implemented in this repository but enabled by the structured database) for exploring insights.
