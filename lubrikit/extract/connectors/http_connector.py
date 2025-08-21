import logging
from typing import Any

import requests
from requests import Response

from lubrikit.extract.connectors.base import BaseConnector
from lubrikit.extract.connectors.configs import HTTPConfig

logger = logging.getLogger(__name__)


class HTTPConnector(BaseConnector):
    """Connector for making HTTP requests with retry logic and caching.

    This connector provides a robust interface for making HTTP requests
    (GET/POST) with built-in retry mechanisms, response caching, and
    error handling. It supports various data formats including form
    data, JSON, and query parameters.

    Attributes:
        config (dict[str, Any]): Configuration object containing HTTP
            request parameters including method, URL, headers, and data.
        headers_cache (dict[str, str]): Cache for HTTP headers to be
            included in requests. This can include authentication tokens,
            content types, and other custom headers.
        retriable_exceptions (tuple[type[Exception], ...]): Tuple of
            exception types that should trigger retry logic. Includes
            connection errors, timeouts, HTTP errors, and general
            request exceptions.
        retry_config (dict[str, Any] | None): Configuration for retry
            behavior when encountering retriable exceptions. If None,
            default retry behavior is used.
    """

    def __init__(
        self,
        headers_cache: dict[str, str],
        retry_config: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        if not config:
            raise ValueError("HTTPConnector requires a configuration.")

        super().__init__(headers_cache, retry_config)

        # Configuration object containing HTTP request parameters
        self.config = HTTPConfig(**config)

        # Tuple of exception types that should trigger retry logic
        self.retriable_exceptions = (
            requests.exceptions.ConnectionError,  # Network connection failures
            requests.exceptions.Timeout,  # Request timeout errors
            requests.exceptions.HTTPError,  # HTTP 4xx/5xx status code errors
            requests.exceptions.RequestException,  # Base requests library exceptions
        )

    def _prepare_cache(self, response: Response) -> dict[str, str]:
        """Prepare cache metadata from the response.

        Args:
            response (Response): The HTTP response object.

        Returns:
            dict[str, str]: A dictionary containing cache metadata.
        """
        cache: dict[str, str] = {}

        if "ETag" in response.headers:
            cache["etag"] = response.headers["ETag"]
        if "Last-Modified" in response.headers:
            cache["last_modified"] = response.headers["Last-Modified"]
        if "Content-Length" in response.headers:
            cache["content_length"] = response.headers["Content-Length"]

        return cache

    def _check(self) -> dict[str, Any] | None:
        """Check the HTTP resource without downloading it.

        Returns:
            dict[str, Any]: A dictionary containing the updated headers cache.
        """
        logger.info(f"Checking {self.config.method} {self.config.url}...")

        # Send request
        r = requests.request(
            method=self.config.method,
            url=self.config.url,
            params=self.config.params,
            data=self.config.data,
            json=self.config.json_data,
            headers=self.headers_cache.update(self.config.extra_headers or {}),
            timeout=self.retry_config.timeout,
        )

        if r.ok:
            return self._prepare_cache(r)
        else:
            logger.error(f"Check failed: {r.status_code} {r.reason}")
            return None

    def _download(self) -> tuple[dict[str, Any] | None, requests.Response | None]:
        """Internal method that performs the actual HTTP request.

        Returns:
            tuple[dict[str, Any], requests.Response | None]: A tuple
                containing the updated headers cache and the response
                object if the request was successful, otherwise None.
        """
        logger.info(f"Downloading {self.config.method} {self.config.url}...")

        # Send request
        r = requests.request(
            method=self.config.method,
            url=self.config.url,
            params=self.config.params,
            data=self.config.data,
            json=self.config.json_data,
            headers=self.headers_cache.update(self.config.extra_headers or {}),
            timeout=self.retry_config.timeout,
        )
        new_headers = self._prepare_cache(r)

        # Check for failure first
        if not r.ok:
            logger.error(f"Request failed: {r.status_code} {r.reason}")
            return None, None

        # If resource unchanged (only for successful responses)
        if r.status_code == 304 or all(
            self.headers_cache.get(k) == new_headers.get(k) for k in new_headers.keys()
        ):
            logger.info(
                f"Response: {r.status_code} {r.reason}. "
                "No new version. Skipping download."
            )
            return new_headers, None

        # Successful response with new content
        logger.info(f"Response: {r.status_code} {r.reason}")
        return new_headers, r
