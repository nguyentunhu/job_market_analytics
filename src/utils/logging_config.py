"""
Centralized logging configuration for job market analytics scrapers.

Provides a LoggerFactory that creates loggers with consistent formatting,
prefix enforcement, and file-based output for production use.

Design:
- All scrapers log to the same shared file: git/job_market_analytics/logs/scrapers.log
- Each logger has a mandatory prefix (e.g., [CareerViet], [Vieclam24h])
- Enforced via programmatic filters to ensure consistency
- File-only output (no console) for production environments
- Idempotent: safe to call multiple times
"""

import logging
import os
from typing import Optional


class _PrefixLogFilter(logging.Filter):
    """
    Adds a prefix to all log messages if not already present.
    
    Ensures consistent logging across scrapers by enforcing
    a mandatory prefix (e.g., [CareerViet], [Vieclam24h]).
    """
    
    def __init__(self, prefix: str):
        """
        Args:
            prefix: The prefix to add (e.g., "CareerViet", "Vieclam24h")
        """
        super().__init__()
        self.prefix = f"[{prefix}]" if prefix and not prefix.startswith("[") else prefix
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add prefix to message if not already present."""
        msg = record.getMessage()
        if not msg.startswith(self.prefix):
            record.msg = f"{self.prefix} {record.msg}"
            record.args = ()
        return True


class LoggerFactory:
    """
    Factory for creating consistently configured loggers.
    
    All loggers share the same file output and are suffixed with
    platform-specific prefixes.
    
    Usage:
        logger = LoggerFactory.create("CareerViet")
        logger.info("Starting scrape")
    """
    
    _LOG_DIR = "git/job_market_analytics/logs"
    _LOG_FILE = "scrapers.log"
    _SHARED_LOGGERS = {}  # Cache to ensure idempotency
    
    @classmethod
    def create(cls, platform_name: str, log_level: int = logging.DEBUG) -> logging.Logger:
        """
        Create or retrieve a logger for a specific platform.
        
        Args:
            platform_name: Name of the platform (e.g., "CareerViet", "Vieclam24h")
            log_level: Logging level (default: DEBUG)
            
        Returns:
            Configured logger instance
            
        Raises:
            ValueError: If platform_name is empty or None
        """
        if not platform_name:
            raise ValueError("platform_name cannot be empty")
        
        # Check cache first (return existing logger)
        if platform_name in cls._SHARED_LOGGERS:
            return cls._SHARED_LOGGERS[platform_name]
        
        # Create directory if needed
        os.makedirs(cls._LOG_DIR, exist_ok=True)
        
        # Get or create logger
        logger = logging.getLogger(platform_name)
        
        # Clear existing handlers/filters to allow reconfiguration
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        for filt in logger.filters[:]:
            logger.removeFilter(filt)
        
        logger.setLevel(log_level)
        logger.propagate = False  # Prevent duplicate logging
        
        # File handler (appending mode for shared log file)
        log_path = os.path.join(cls._LOG_DIR, cls._LOG_FILE)
        handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        handler.setLevel(log_level)
        
        # Formatter with timestamp, level, message
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Attach prefix filter to enforce [PlatformName] on all messages
        logger.addFilter(_PrefixLogFilter(platform_name))
        
        # Flush handler after configuration
        handler.flush()
        
        # Cache and return
        cls._SHARED_LOGGERS[platform_name] = logger
        return logger
    
    @classmethod
    def get_log_file_path(cls) -> str:
        """Get the full path to the shared log file."""
        return os.path.join(cls._LOG_DIR, cls._LOG_FILE)
    
    @classmethod
    def flush_all(cls) -> None:
        """Flush all handlers for all loggers (useful before process exit)."""
        for logger in cls._SHARED_LOGGERS.values():
            for handler in logger.handlers:
                handler.flush()
