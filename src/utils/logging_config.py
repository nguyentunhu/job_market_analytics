"""
centralized logging configuration for the job market analytics pipeline.
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler

LOGS_DIR = "logs"

# ensure the logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)

class LoggerFactory:
    """
    configures and provides loggers for different parts of the application.
    """
    
    LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
    
    @staticmethod
    def setup_loggers():
        """
        sets up the root logger and specific handlers for different modules.
        this should be called once at the start of the application.
        """
        # get the root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG) # capture all messages from root
        
        # clear any existing handlers to prevent duplicates on successive calls
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # 1. console handler for all messages (or general info)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        root_logger.addHandler(console_handler)

        # 2. main log file handler (captures everything from root)
        main_log_file_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'main.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        main_log_file_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        root_logger.addHandler(main_log_file_handler)
        
        # 3. specific loggers and their file handlers
        
        # 'pipeline' logger (for src/pipeline.py)
        pipeline_logger = logging.getLogger('pipeline')
        pipeline_logger.setLevel(logging.INFO)
        pipeline_logger.propagate = False # prevent messages from going to root logger
        pipeline_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'pipeline.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        pipeline_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        pipeline_logger.addHandler(pipeline_handler)
        pipeline_logger.addHandler(console_handler) # also show pipeline messages on console

        # 'scrapers' logger (this will be the parent for careerviet, topcv, vieclam24h)
        scrapers_logger_root = logging.getLogger('scrapers')
        scrapers_logger_root.setLevel(logging.INFO)
        scrapers_logger_root.propagate = False # prevent messages from going to root logger
        scrapers_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'scrapers.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        scrapers_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        scrapers_logger_root.addHandler(scrapers_handler)
        scrapers_logger_root.addHandler(console_handler)
        
        # individual platform loggers will inherit from 'scrapers' and thus use its handlers
        # no need to explicitly configure them here as long as they are children (e.g., 'scrapers.careerviet')
            
        # 'transformer' logger
        transformer_logger = logging.getLogger('transformer')
        transformer_logger.setLevel(logging.INFO)
        transformer_logger.propagate = False
        transformer_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'transformer.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        transformer_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        transformer_logger.addHandler(transformer_handler)
        transformer_logger.addHandler(console_handler)

        # 'load' logger
        load_logger = logging.getLogger('load')
        load_logger.setLevel(logging.INFO)
        load_logger.propagate = False
        load_handler = TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'load.log'), when='D', interval=1, backupCount=7, encoding='utf-8'
        )
        load_handler.setFormatter(logging.Formatter(LoggerFactory.LOG_FORMAT))
        load_logger.addHandler(load_handler)
        load_logger.addHandler(console_handler)



    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        returns a logger with the specified name.
        """
        return logging.getLogger(name)

