import logging
import time
import asyncio
from functools import wraps
from typing import Optional, Type, Union, Tuple, Callable

logger = logging.getLogger(__name__)

def retry_with_backoff(
    retries: int = 3,
    base_delay: float = 2,
    max_delay: float = 60,
    exponential_base: float = 2,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        retries (int): Maximum number of retries
        base_delay (float): Initial delay in seconds
        max_delay (float): Maximum delay between retries in seconds
        exponential_base (float): Base for exponential backoff
        exceptions (Exception or tuple): Exception(s) to catch and retry on
    
    Example:
        @retry_with_backoff(retries=3, exceptions=(RequestError, TimeoutError))
        def fetch_data():
            # do something that might fail
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            attempt = 0
            
            while True:
                try:
                    return await func(*args, **kwargs)
                    
                except exceptions as e:
                    attempt += 1
                    
                    # Log more detailed information about the exception
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    if attempt >= retries:
                        logger.error(
                            f"RETRY FAILED: {func.__name__} failed after {retries} attempts. "
                            f"Error type: {error_type}, Error: {error_msg}", 
                            exc_info=True
                        )
                        raise  # Re-raise the last exception if we're out of retries
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                    
                    logger.warning(
                        f"RETRY ATTEMPT: {func.__name__} - Attempt {attempt}/{retries} failed. "
                        f"Error type: {error_type}, Error: {error_msg}. "
                        f"Retrying in {delay:.2f} seconds...",
                        exc_info=True
                    )
                    
                    await asyncio.sleep(delay)
                    
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            attempt = 0
            
            while True:
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    attempt += 1
                    
                    # Log more detailed information about the exception
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    if attempt >= retries:
                        logger.error(
                            f"RETRY FAILED: {func.__name__} failed after {retries} attempts. "
                            f"Error type: {error_type}, Error: {error_msg}", 
                            exc_info=True
                        )
                        raise  # Re-raise the last exception if we're out of retries
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                    
                    logger.warning(
                        f"RETRY ATTEMPT: {func.__name__} - Attempt {attempt}/{retries} failed. "
                        f"Error type: {error_type}, Error: {error_msg}. "
                        f"Retrying in {delay:.2f} seconds...",
                        exc_info=True
                    )
                    
                    time.sleep(delay)
        
        # Determine if the function is async or not
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
                    
    return decorator 