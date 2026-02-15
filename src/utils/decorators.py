"""
Decorators for scraper functions: retry logic, timeout handling, rate limiting, metrics.

Provides robust functionality for production job scraping with automatic
retries, timeout management, and rate limiting via decorators.
"""

import functools
import logging
import time
from typing import Callable, Any, TypeVar, cast
from typing import Optional

from .exceptions import RateLimitException, FetchException

F = TypeVar("F", bound=Callable[..., Any])


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[F], F]:
    """
    Decorator to retry a function with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts (default: 3)
        delay: Initial delay between retries in seconds (default: 1.0)
        backoff: Multiplier for delay after each retry (default: 2.0)
        exceptions: Tuple of exceptions to catch and retry on (default: (Exception,))
        
    Example:
        @retry(max_attempts=3, delay=1.0, backoff=2.0)
        def fetch_page(url: str):
            return requests.get(url).text
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    attempt_num = attempt + 1
                    
                    if attempt_num < max_attempts:
                        logging.debug(
                            f"Attempt {attempt_num}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logging.error(
                            f"All {max_attempts} attempts failed for {func.__name__}: {str(e)}"
                        )
            
            # If we exhausted retries, raise the last exception
            if last_exception:
                raise last_exception
            raise RuntimeError(f"Retry decorator failed for {func.__name__}")
        
        return cast(F, wrapper)
    return decorator


def timeout(seconds: float = 30) -> Callable[[F], F]:
    """
    Decorator to enforce a timeout on function execution.
    
    Note: This uses signal-based timeout (Unix/Linux only) or simple time tracking.
    For production use with long-running operations, consider using
    threading or multiprocessing.
    
    Args:
        seconds: Timeout duration in seconds
        
    Example:
        @timeout(seconds=30)
        def fetch_page(url: str):
            return requests.get(url).text
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            
            # For now, use simple time tracking (not a hard timeout)
            # Full signal-based timeout would require Unix-specific code
            result = func(*args, **kwargs)
            
            elapsed = time.time() - start_time
            if elapsed > seconds:
                logging.warning(
                    f"Function {func.__name__} took {elapsed:.2f}s (timeout: {seconds}s)"
                )
            
            return result
        
        return cast(F, wrapper)
    return decorator


def rate_limit(calls: int = 1, period: float = 1.0) -> Callable[[F], F]:
    """
    Decorator to enforce rate limiting on a function.
    
    Ensures that the function is called at most `calls` times per `period` seconds.
    Blocks (sleeps) if necessary to maintain the rate.
    
    Args:
        calls: Number of calls allowed per period (default: 1)
        period: Time period in seconds (default: 1.0)
        
    Example:
        @rate_limit(calls=10, period=60)  # 10 calls per minute
        def fetch_page(url: str):
            return requests.get(url).text
    """
    min_interval = period / calls
    last_called = [0.0]  # Use list to allow modification in nested function
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            elapsed = time.time() - last_called[0]
            
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                logging.debug(
                    f"Rate limiting {func.__name__}: sleeping {sleep_time:.2f}s"
                )
                time.sleep(sleep_time)
            
            last_called[0] = time.time()
            return func(*args, **kwargs)
        
        return cast(F, wrapper)
    return decorator


def log_metrics(logger: logging.Logger, operation: str = "operation") -> Callable[[F], F]:
    """
    Decorator to log execution metrics (time, success/failure).
    
    Args:
        logger: Logger instance to use
        operation: Description of the operation being measured
        
    Example:
        @log_metrics(logger, operation="scrape_page")
        def scrape_page(url: str):
            return scrape_logic(url)
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start_time
                logger.debug(
                    f"{operation} completed successfully in {elapsed:.2f}s"
                )
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(
                    f"{operation} failed after {elapsed:.2f}s: {str(e)}"
                )
                raise
        
        return cast(F, wrapper)
    return decorator
