import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retriable_exceptions: tuple[type[BaseException], ...] | None = None,
) -> Callable:
    """Decorator adding retry logic with exponential backoff and jitter.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Multiplier for exponential backoff
        jitter: Whether to add random jitter to delays
        retriable_exceptions: Tuple of exception types that should
            trigger retries
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Define default exceptions if none provided
            exceptions_to_retry = retriable_exceptions or (Exception,)

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions_to_retry as e:
                    if attempt == max_retries:
                        logger.error(
                            f"All {max_retries + 1} attempts failed. Last error: {e}"
                        )
                        raise e

                    delay = min(base_delay * (backoff_factor**attempt), max_delay)
                    if jitter:
                        # Add up to 10% jitter
                        delay += random.uniform(0, delay * 0.1)

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    time.sleep(delay)

                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    raise e

        return wrapper

    return decorator
