# End-to-End Job Market Analytics Pipeline for Vietnam's Data Sector

A production-ready ETL pipeline for analyzing the Vietnamese job market, with a focus on data-related positions. The pipeline scrapes job listings, applies NLP-based relevance filtering, extracts structured insights, and stores analytics-ready data in SQLite.

## Features

- **Multi-Platform Web Scraping**: Concurrent scraping from CareerViet, TopCV, and ViecLam24h
- **NLP-Based Relevance Filtering**: Semantic similarity filtering using `all-MiniLM-L6-v2` transformer model
- **Data Extraction**: Automated extraction of job titles, skills, salary ranges, seniority levels, and locations
- **Data Quality**: NULL values for missing fields instead of empty strings
- **SQL Analytics**: Pre-built views for analysis and reporting
- **Test Coverage**: Comprehensive unit tests for scrapers, transformers, and loaders

## Architecture

```
job_market_analytics/
├── src/
│   ├── orchestrator.py       # Manages concurrent scraping across platforms
│   ├── pipeline.py           # Main ETL workflow orchestration
│   ├── config.py             # Skill/seniority/location configurations
│   ├── scrapers/             # Platform-specific scrapers (3 platforms)
│   ├── transform/            # Data transformation and NLP filtering
│   ├── load/                 # Database loading logic
│   └── utils/                # Shared utilities (NLP, database, logging)
├── sql/                      # Database schema and analytics views
├── tests/                    # Unit and integration tests
├── data/                     # SQLite database location
├── logs/                     # Runtime logs
└── requirements.txt          # Python dependencies (optimized)
```

## NLP Model Specification

### Relevance Filter
- **Model**: `all-MiniLM-L6-v2` (Sentence-Transformers)
- **Type**: Lightweight transformer (61 MB, ~22M parameters)
- **Mechanism**: Cosine similarity between job embeddings and query embedding
- **Threshold**: 0.3 (configurable)
- **Speed**: ~100ms per comparison on CPU
- **Fallback**: Keyword-based matching if model fails or is unavailable

### Why This Model?
- **Efficient**: Orders of magnitude faster than large models (BERT base ~110M params)
- **Accurate**: Trained on semantic similarity tasks, designed for short texts
- **Portable**: Works with CPU, no GPU required
- **Production-Ready**: Widely used for similarity tasks in production systems

### Usage
```python
from src.transform.job_transformer import JobDataTransformer

# Default: NLP filtering enabled with 0.3 threshold
transformer = JobDataTransformer(
    enable_nlp_filter=True,
    relevance_threshold=0.3
)

# Disable NLP filtering to use keyword-based fallback
transformer = JobDataTransformer(enable_nlp_filter=False)
```

---

## Setup and Installation

### Prerequisites
- Python 3.9+
- pip (Python package installer)
- Virtual environment (recommended)

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/nguyentunhu/job_market_analytics.git
   cd job_market_analytics
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   .\venv\Scripts\activate      # On Windows
   # source venv/bin/activate   # On macOS/Linux
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
   This installs:
   - `sentence-transformers` (NLP model)
   - `beautifulsoup4` + `requests` (web scraping)
   - `SQLAlchemy` (ORM)
   - `pandas` (data manipulation)
   - `pytest` (testing)

---

## Running the Pipeline

### Full ETL Pipeline
```bash
python src/pipeline.py
```

This performs:
1. Database schema initialization
2. Concurrent scraping from 3 platforms (10 pages, max 500 jobs each)
3. NLP-based relevance filtering
4. Skill extraction and seniority detection
5. Data loading into SQLite

**Expected Output:**
- `data/job_market_analytics.db` (SQLite database)
- `logs/pipeline.log` (execution log)
- Console output with statistics (jobs scraped, transformed, loaded)

### Scraping Configuration

Edit `src/orchestrator.py` to customize:
```python
orchestrator = Orchestrator(
    max_workers=3,                  # Concurrent threads
    max_results_per_platform=500,   # Max jobs per platform
    request_delay=2.0               # Delay between requests (seconds)
)

raw_jobs = orchestrator.scrape(
    query="Data Analyst",           # Search query
    max_pages=10,                   # Pages per platform
    enabled_platforms=['topcv', 'careerviet', 'vieclam24h']  # Which to scrape
)
```

### Running Tests
```bash
pytest tests/ -v             # Verbose output
pytest tests/ --cov=src      # With coverage report
pytest tests/test_transform/test_nlp_filtering.py  # Specific test
```

---

## Data Schema

### Extracted Fields (with NULL handling)
- **Core**: platform, job_url, job_title, platform_job_id
- **Company**: company_name (NULL if not found)
- **Location**: location (canonical location or NULL)
- **Posting**: posted_date (YYYY-MM-DD or NULL)
- **Compensation**: salary_min, salary_max, salary_currency (NULL if not specified)
- **Level**: seniority_level (or NULL if not detected)
- **Skills**: extracted_skills list with categories
- **Descriptions**: raw_description, clean_description

### Database Views
- `job_market_analytics_views.sql` includes pre-built SQL views for:
  - Skills demand analysis
  - Salary statistics by seniority
  - Location-based job distribution

---

## Key Design Decisions

1. **NULL over Empty Strings**: Missing data is stored as NULL for proper SQL analytics
2. **Concurrent Scraping**: ThreadPoolExecutor runs all platforms in parallel (faster collection)
3. **Semantic Filtering**: NLP filtering reduces noise and improves data quality
4. **Configurable Thresholds**: Seniority detection and relevance filtering are tunable
5. **SQLite for Portability**: No external database required; single .db file is portable
6. **Modular Structure**: Each component (scraper, transformer, loader) is independently testable

---

## Dependencies & Performance

### Core Dependencies
- **requests**: HTTP requests for web scraping
- **beautifulsoup4**: HTML parsing
- **SQLAlchemy**: Database ORM
- **sentence-transformers**: NLP semantic similarity
- **torch**: PyTorch (required by sentence-transformers)

### Performance Metrics
- Scraping: ~5-10 seconds per page per platform (with 2-second delays)
- Transformation (100 jobs): ~2-3 seconds with NLP filtering
- Full pipeline (1,500 jobs): ~3-5 minutes end-to-end

### Memory Usage
- Model loading: ~300 MB (one-time load)
- Per-job processing: ~1-2 MB
- Typical run: <500 MB peak memory

---

## Troubleshooting

### NLP Model Download Fails
```bash
# Manually download the model:
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
```

### Scraping Timeouts
Increase timeout in `orchestrator.py`:
```python
scraper_timeout_seconds = 600  # Change from 300 to 600
```

### Database Locked Error
Close any open connections and delete `.db` file to restart:
```bash
rm data/job_market_analytics.db
python src/pipeline.py
```

---

## Project Structure Rationale

**Included:**
- `src/` - Core logic (scrapers, transformers, loaders)
- `tests/` - Unit and integration tests for reliability
- `sql/` - Schema and analytics views
- `data/`, `logs/` - Runtime artifacts

**Excluded (removed for minimalism):**
- Demo scripts (edge cases covered in tests)
- PowerBI folder (use SQL views instead)
- Apache Airflow (orchestration not needed at this scale)
- Flask server (data loading is one-shot)



---

## End-to-End Data Flow

The pipeline follows a clear Extract-Transform-Load (ETL) pattern:

1.  **Extract (Scrapers)**: Individual scraper modules (e.g., `careerviet_scraper.py`, `topcv_scraper.py`) extend a `BaseScraper` class, providing platform-specific logic for navigating job listings and extracting raw job details. They handle HTTP requests, retries, and rate-limiting.
2.  **Orchestrator (`orchestrator.py`)**: Manages the concurrent execution of multiple scrapers using a thread pool. It aggregates all raw job data collected from different platforms into a single, unified list of dictionaries.
3.  **Transform (`job_transformer.py`)**: Takes the raw job data, cleans it, normalizes fields (job titles, company names, locations), detects seniority levels, extracts salary ranges, and identifies key skills and tools mentioned in job descriptions. It outputs a list of structured, analytics-ready job records.
4.  **Load (`load_to_db.py`)**: Connects to an SQLite database (`data/job_market_analytics.db`). It inserts the transformed job data into the `jobs`, `job_descriptions`, `skills`, and `job_skills` tables, handling deduplication and ensuring data integrity.
5.  **SQL Analysis (`02_analytics_views.sql`)**: Defines SQL views that simplify common analytical queries. These views serve as the direct data source for any reporting or dashboarding tools.
6.  **Dashboard (Power BI / Streamlit)**: A visualization layer (not implemented in this repository but enabled by the structured database) for exploring insights.
