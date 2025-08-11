from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from lubrikit.utils.retry import RetryConfig, retry_with_backoff


class BaseConnector(ABC):
    def __init__(
        self, headers_cache: dict[str, str], retry_config: RetryConfig | None = None
    ) -> None:
        self.headers_cache = headers_cache
        self.retry_config = retry_config or RetryConfig()

        if TYPE_CHECKING:
            self.retriable_exceptions: tuple[type[Exception], ...] = ()

    def check(self) -> Any:
        """Check the HTTP resource without downloading it.

        Returns:
            Any: A dictionary containing the updated headers cache
                if the check is successful, otherwise None.
        """
        # Create a retry decorator with instance configuration
        retry_decorator = retry_with_backoff(
            max_retries=self.retry_config.max_retries,
            base_delay=self.retry_config.base_delay,
            max_delay=self.retry_config.max_delay,
            backoff_factor=self.retry_config.backoff_factor,
            retriable_exceptions=self.retriable_exceptions,
        )

        return retry_decorator(self._check)()

    def download(self) -> Any:
        """Perform an HTTP request with caching and retries.

        Returns:
            Any: The response data if the request is successful,
                otherwise None.
        """
        # Create a retry decorator with instance configuration
        retry_decorator = retry_with_backoff(
            max_retries=self.retry_config.max_retries,
            base_delay=self.retry_config.base_delay,
            max_delay=self.retry_config.max_delay,
            backoff_factor=self.retry_config.backoff_factor,
            retriable_exceptions=self.retriable_exceptions,
        )

        return retry_decorator(self._download)()

    @abstractmethod
    def _check(self) -> dict[str, Any] | None:
        """Check the resource without downloading it.

        Returns:
            dict[str, Any]: A dictionary containing the updated headers cache.
        """
        ...

    @abstractmethod
    def _download(self) -> tuple[dict[str, Any] | None, Any]:
        """Internal method that performs the actual resource download.

        Returns:
            tuple[dict[str, Any], Any]: A tuple containing the updated
                headers cache and the response object if the request was
                successful, otherwise None.
        """
        ...

    @abstractmethod
    def _prepare_cache(self, response: Any) -> dict[str, str]:
        """Prepare cache metadata from the response.

        Args:
            response (Any): The response object.

        Returns:
            dict[str, str]: A dictionary containing cache metadata.
        """
        ...
