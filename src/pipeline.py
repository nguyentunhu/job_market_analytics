import logging
from datetime import datetime

from src.orchestrator import Orchestrator
from src.transform.job_transformer import JobDataTransformer
from src.load.load_to_db import JobDataLoader
from src.utils.database import get_db_manager
from src.utils.logging_config import LoggerFactory

# setup centralized logging
LoggerFactory.setup_loggers()
logger = LoggerFactory.get_logger('pipeline')


def run_pipeline():
    """
    executes the full etl pipeline.
    """
    logger.info("starting job market analytics pipeline run")

    # setup
    db_manager = get_db_manager()
    db_manager.setup_database()
    logger.info("database setup is complete.")

    # extract
    logger.info("starting extract phase")
    orchestrator = Orchestrator()
    raw_jobs = orchestrator.scrape(query="Data Analyst")

    if not raw_jobs:
        logger.warning("no jobs were scraped. terminating pipeline run.")
        return

    logger.info(f"successfully scraped a total of {len(raw_jobs)} raw job listings.")
    logger.info("extract phase complete")

    # transform
    logger.info("starting transform phase")
    transformer = JobDataTransformer()
    processed_jobs = transformer.transform_batch(raw_jobs)
    
    if not processed_jobs:
        logger.warning("no jobs were successfully transformed. terminating pipeline run.")
        return

    logger.info(f"successfully transformed {len(processed_jobs)} jobs.")
    logger.info("transform phase complete")

    # load
    logger.info("starting load phase")
    loader = JobDataLoader()
    loader.load_jobs(processed_jobs)

    stats = loader.get_statistics()
    logger.info(f"jobs inserted: {stats.get('jobs_inserted', 0)}")
    logger.info(f"skills associated: {stats.get('skills_inserted', 0)}")
    logger.info("load phase complete")

    logger.info("pipeline run completed successfully")


if __name__ == "__main__":
    run_pipeline()
