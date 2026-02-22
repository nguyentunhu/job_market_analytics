"""
Centralized logging configuration for the job market analytics pipeline.
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler

LOGS_DIR = "logs"

# Ensure the logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)

class LoggerFactory:
    """
    Configures and provides loggers for different parts of the application.
    """
    
    LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
    
    @staticmethod
    def setup_loggers():
        """
        Sets up the root logger and specific handlers for different modules.
        This should be called once at the start of the application.
        """
        # Basic configuration for the root logger
        logging.basicConfig(
            level=logging.INFO,
            format=LoggerFactory.LOG_FORMAT,
            handlers=[logging.StreamHandler()] # Default to console output
        )
        
        # Create specific handlers for different logs
        scraper_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'scrapers.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        scraper_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))

        transformer_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'transformer.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        transformer_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        
        load_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'load.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        load_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))

        # Get and configure loggers
        logging.getLogger('scrapers').addHandler(scraper_handler)
        logging.getLogger('scrapers').propagate = False # Prevent double logging to console
        
        logging.getLogger('transformer').addHandler(transformer_handler)
        logging.getLogger('transformer').propagate = False
        
        logging.getLogger('load').addHandler(load_handler)
        logging.getLogger('load').propagate = False

        # General pipeline logger
        pipeline_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'pipeline.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        pipeline_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        logging.getLogger('pipeline').addHandler(pipeline_handler)
        logging.getLogger('pipeline').propagate = False

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Returns a logger with the specified name.
        """
        return logging.getLogger(name)

