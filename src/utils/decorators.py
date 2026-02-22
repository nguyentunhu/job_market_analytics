"""
custom decorators for the job market analytics project.
"""

import time
import functools
from typing import Callable, Any

def rate_limit(calls: int, period: float):
    """
    decorator to enforce rate limiting on a function.
    
    args:
        calls: maximum number of calls allowed within the period.
        period: time period in seconds.
    """
    def decorator(func: Callable) -> Callable:
        # attributes to store state on the decorated function
        if not hasattr(func, 'timestamps'):
            func.timestamps = []

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            now = time.time()
            
            # remove timestamps older than the specified period
            func.timestamps = [ts for ts in func.timestamps if now - ts < period]
            
            if len(func.timestamps) >= calls:
                # calculate wait time
                wait_time = (func.timestamps[0] + period) - now
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # call the original function
            result = func(*args, **kwargs)
            func.timestamps.append(time.time())
            return result
        return wrapper
    return decorator

def retry(attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    decorator to retry a function on failure with exponential backoff.
    
    args:
        attempts: number of retry attempts.
        delay: initial delay in seconds.
        backoff: multiplier for the delay on each subsequent retry.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            nonlocal attempts, delay
            while attempts > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # using logger from the function's module if available
                    logger = getattr(func, '__module__', None)
                    if logger:
                        logging.getLogger(logger).warning(
                            f"retrying {func.__name__} due to error: {e}. attempts left: {attempts - 1}"
                        )
                    time.sleep(delay)
                    attempts -= 1
                    delay *= backoff
            
            # final attempt
            return func(*args, **kwargs)
        return wrapper
    return decorator
