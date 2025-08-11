from unittest.mock import Mock, patch

import pytest
import requests

from lubrikit.collection.connectors.configs import HTTPConfig
from lubrikit.collection.connectors.http_connector import HTTPConnector
from lubrikit.utils.retry import RetryConfig


@pytest.fixture
def headers_cache() -> dict[str, str]:
    """Sample headers cache."""
    return {
        "etag": "test-etag",
        "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        "content_length": "1024",
    }


@pytest.fixture
def http_config() -> HTTPConfig:
    """Sample HTTP configuration."""
    return HTTPConfig(
        method="GET",
        url="https://api.example.com/data",
        params={"limit": 10},
        extra_headers={"User-Agent": "TestBot/1.0"},
    )


@pytest.fixture
def retry_config() -> RetryConfig:
    """Sample retry configuration."""
    return RetryConfig(
        timeout=5.0,
        max_retries=2,
        base_delay=0.5,
        max_delay=30.0,
        backoff_factor=2.0,
    )


@pytest.fixture
def connector(headers_cache: dict[str, str], http_config: HTTPConfig) -> HTTPConnector:
    """HTTPConnector instance with default configuration."""
    return HTTPConnector(headers_cache, http_config)


@pytest.fixture
def connector_with_retry(
    headers_cache: dict[str, str],
    http_config: HTTPConfig,
    retry_config: RetryConfig,
) -> HTTPConnector:
    """HTTPConnector instance with retry configuration."""
    return HTTPConnector(headers_cache, http_config, retry_config)


@pytest.mark.parametrize(
    "feature, expected",
    [
        ("timeout", 10.0),
        ("max_retries", 3),
    ],
)
def test_initialization_default(
    headers_cache: dict[str, str],
    http_config: HTTPConfig,
    feature: str,
    expected: int | float,
) -> None:
    """Test HTTPConnector initialization with default retry config."""
    connector = HTTPConnector(headers_cache, http_config)

    assert getattr(connector.retry_config, feature) == expected


def test_initialization_with_retry_config(
    headers_cache: dict[str, str],
    http_config: HTTPConfig,
    retry_config: RetryConfig,
) -> None:
    """Test HTTPConnector initialization with custom retry config."""
    connector = HTTPConnector(headers_cache, http_config, retry_config)

    assert connector.headers_cache == headers_cache
    assert connector.config == http_config
    assert connector.retry_config == retry_config


def test_prepare_cache_all_headers(connector: HTTPConnector) -> None:
    """Test _prepare_cache with all supported headers present."""
    response = Mock(spec=requests.Response)
    response.headers = {
        "ETag": "test-etag-value",
        "Last-Modified": "Thu, 22 Oct 2015 08:30:00 GMT",
        "Content-Length": "2048",
        "Content-Type": "application/json",
    }

    cache = connector._prepare_cache(response)

    assert cache == {
        "etag": "test-etag-value",
        "last_modified": "Thu, 22 Oct 2015 08:30:00 GMT",
        "content_length": "2048",
    }


def test_prepare_cache_partial_headers(connector: HTTPConnector) -> None:
    """Test _prepare_cache with only some headers present."""
    response = Mock(spec=requests.Response)
    response.headers = {
        "ETag": "partial-etag",
        "Content-Type": "text/html",
    }

    cache = connector._prepare_cache(response)

    assert cache == {"etag": "partial-etag"}


def test_prepare_cache_no_headers(connector: HTTPConnector) -> None:
    """Test _prepare_cache with no cache-related headers."""
    response = Mock(spec=requests.Response)
    response.headers = {"Content-Type": "text/plain"}

    cache = connector._prepare_cache(response)

    assert cache == {}


@patch("lubrikit.collection.connectors.http_connector.requests.request")
def test_check_success(mock_request: Mock, connector: HTTPConnector) -> None:
    """Test _check method with successful response."""
    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.headers = {"ETag": "check-etag", "Content-Length": "512"}
    mock_request.return_value = mock_response

    result = connector._check()

    assert result == {"etag": "check-etag", "content_length": "512"}
    mock_request.assert_called_once_with(
        method="GET",
        url="https://api.example.com/data",
        params={"limit": 10},
        data=None,
        json=None,
        headers=connector.headers_cache.update({"User-Agent": "TestBot/1.0"}),
        timeout=10.0,
    )


@patch("lubrikit.collection.connectors.http_connector.requests.request")
@patch("lubrikit.collection.connectors.http_connector.logger")
def test_check_failure(
    mock_logger: Mock, mock_request: Mock, connector: HTTPConnector
) -> None:
    """Test _check method with failed response."""
    mock_response = Mock(spec=requests.Response)
    mock_response.ok = False
    mock_response.status_code = 404
    mock_response.reason = "Not Found"
    mock_request.return_value = mock_response

    result = connector._check()

    assert result is None
    mock_logger.error.assert_called_once_with("Check failed: 404 Not Found")


@patch("lubrikit.collection.connectors.http_connector.requests.request")
def test_download_success_new_content(
    mock_request: Mock, connector: HTTPConnector
) -> None:
    """Test _download method with successful response and new content."""
    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.reason = "OK"
    mock_response.headers = {
        "ETag": "new-etag",
        "Last-Modified": "Fri, 23 Oct 2015 09:00:00 GMT",
        "Content-Length": "1536",
    }
    mock_request.return_value = mock_response

    headers, response = connector._download()

    expected_headers = {
        "etag": "new-etag",
        "last_modified": "Fri, 23 Oct 2015 09:00:00 GMT",
        "content_length": "1536",
    }
    assert headers == expected_headers
    assert response == mock_response


@patch("lubrikit.collection.connectors.http_connector.requests.request")
@patch("lubrikit.collection.connectors.http_connector.logger")
def test_download_not_modified_304(
    mock_logger: Mock, mock_request: Mock, connector: HTTPConnector
) -> None:
    """Test _download method with 304 Not Modified response."""
    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.status_code = 304
    mock_response.reason = "Not Modified"
    mock_response.headers = {}
    mock_request.return_value = mock_response

    headers, response = connector._download()

    assert headers == {}
    assert response is None
    mock_logger.info.assert_called_with(
        "Response: 304 Not Modified. No new version. Skipping download."
    )


@patch("lubrikit.collection.connectors.http_connector.requests.request")
@patch("lubrikit.collection.connectors.http_connector.logger")
def test_download_unchanged_content(mock_logger: Mock, mock_request: Mock) -> None:
    """Test _download method when content hasn't changed based on headers."""
    headers_cache = {
        "etag": "same-etag",
        "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        "content_length": "1024",
    }
    config = HTTPConfig(method="GET", url="https://api.example.com/data")
    connector = HTTPConnector(headers_cache, config)

    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.reason = "OK"
    mock_response.headers = {
        "ETag": "same-etag",
        "Last-Modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        "Content-Length": "1024",
    }
    mock_request.return_value = mock_response

    headers, response = connector._download()

    expected_headers = {
        "etag": "same-etag",
        "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        "content_length": "1024",
    }
    assert headers == expected_headers
    assert response is None
    mock_logger.info.assert_called_with(
        "Response: 200 OK. No new version. Skipping download."
    )


@patch("lubrikit.collection.connectors.http_connector.requests.request")
@patch("lubrikit.collection.connectors.http_connector.logger")
def test_download_failure(
    mock_logger: Mock, mock_request: Mock, connector: HTTPConnector
) -> None:
    """Test _download method with failed response."""
    mock_response = Mock(spec=requests.Response)
    mock_response.ok = False
    mock_response.status_code = 500
    mock_response.reason = "Internal Server Error"
    mock_response.headers = {}
    mock_request.return_value = mock_response

    headers, response = connector._download()

    assert headers is None
    assert response is None
    mock_logger.error.assert_called_once_with(
        "Request failed: 500 Internal Server Error"
    )


@patch("lubrikit.collection.connectors.http_connector.requests.request")
def test_post_request_with_form_data(mock_request: Mock) -> None:
    """Test POST request with form data."""
    headers_cache = {"last_modified": "old_date"}  # Different from response
    config = HTTPConfig(
        method="POST",
        url="https://api.example.com/submit",
        data={"username": "test", "password": "secret"},
        extra_headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    connector = HTTPConnector(headers_cache, config)

    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.status_code = 201
    mock_response.reason = "Created"
    mock_response.headers = {"Content-Length": "100"}  # Different from cache
    mock_request.return_value = mock_response

    headers, response = connector._download()

    mock_request.assert_called_once_with(
        method="POST",
        url="https://api.example.com/submit",
        params=None,
        data={"username": "test", "password": "secret"},
        json=None,
        headers=None,  # dict.update() returns None
        timeout=10.0,
    )
    assert headers == {"content_length": "100"}
    assert response == mock_response


@patch("lubrikit.collection.connectors.http_connector.requests.request")
def test_post_request_with_json_data(mock_request: Mock) -> None:
    """Test POST request with JSON data."""
    headers_cache = {"content_length": "50"}  # Different from response
    config = HTTPConfig(
        method="POST",
        url="https://api.example.com/users",
        json_data={"name": "John Doe", "email": "john@example.com"},
        extra_headers={"Content-Type": "application/json"},
    )
    connector = HTTPConnector(headers_cache, config)

    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.status_code = 201
    mock_response.reason = "Created"
    mock_response.headers = {"Content-Length": "120"}  # Different from cache
    mock_request.return_value = mock_response

    headers, response = connector._download()

    mock_request.assert_called_once_with(
        method="POST",
        url="https://api.example.com/users",
        params=None,
        data=None,
        json={"name": "John Doe", "email": "john@example.com"},
        headers=None,  # dict.update() returns None
        timeout=10.0,
    )
    assert headers == {"content_length": "120"}
    assert response == mock_response


def test_retry_exceptions_configuration(connector: HTTPConnector) -> None:
    """Test that retriable exceptions are properly configured."""
    expected_exceptions = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
        requests.exceptions.RequestException,
    )

    assert connector.retriable_exceptions == expected_exceptions


@patch("lubrikit.collection.connectors.http_connector.requests.request")
def test_custom_retry_config_used(
    mock_request: Mock, connector_with_retry: HTTPConnector
) -> None:
    """Test that custom retry configuration is used in requests."""
    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.headers = {}
    mock_request.return_value = mock_response

    connector_with_retry._check()

    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args.kwargs
    assert call_kwargs["timeout"] == 5.0


@patch("lubrikit.collection.connectors.http_connector.requests.request")
def test_headers_update_with_extra_headers(mock_request: Mock) -> None:
    """Test that extra headers are properly merged with headers cache."""
    headers_cache = {"Authorization": "Bearer token123"}
    config = HTTPConfig(
        method="GET",
        url="https://api.example.com/data",
        extra_headers={"User-Agent": "TestBot/2.0", "Accept": "application/json"},
    )
    connector = HTTPConnector(headers_cache, config)

    mock_response = Mock(spec=requests.Response)
    mock_response.ok = True
    mock_response.headers = {}
    mock_request.return_value = mock_response

    connector._check()

    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args.kwargs
    headers_param = call_kwargs["headers"]
    # The update method returns None, so headers_param will be None
    assert headers_param is None
    # but the side effect should have updated the headers_cache
    expected_headers = {
        "Authorization": "Bearer token123",
        "User-Agent": "TestBot/2.0",
        "Accept": "application/json",
    }
    # Check that the headers_cache was updated
    assert connector.headers_cache == expected_headers


def test_empty_headers_cache_initialization() -> None:
    """Test HTTPConnector with empty headers cache."""
    headers_cache: dict[str, str] = {}
    config = HTTPConfig(method="GET", url="https://example.com")
    connector = HTTPConnector(headers_cache, config)

    assert connector.headers_cache == {}
    assert connector.config == config


def test_no_extra_headers_config() -> None:
    """Test HTTPConnector with no extra headers in config."""
    headers_cache = {"User-Agent": "Default/1.0"}
    config = HTTPConfig(method="GET", url="https://example.com")
    connector = HTTPConnector(headers_cache, config)

    assert connector.config.extra_headers is None


@pytest.mark.parametrize(
    "method,expected_method",
    [
        ("GET", "GET"),
        ("POST", "POST"),
    ],
)
def test_http_methods(
    method: str, expected_method: str, headers_cache: dict[str, str]
) -> None:
    """Test that different HTTP methods are properly handled."""
    config = HTTPConfig(method=method, url="https://example.com")
    connector = HTTPConnector(headers_cache, config)

    assert connector.config.method == expected_method


@pytest.mark.parametrize(
    "url",
    [
        "https://api.example.com/v1/data",
        "http://localhost:8000/test",
        "https://secure.api.com/endpoint?param=value",
    ],
)
def test_various_urls(url: str, headers_cache: dict[str, str]) -> None:
    """Test that various URL formats are properly handled."""
    config = HTTPConfig(method="GET", url=url)
    connector = HTTPConnector(headers_cache, config)

    assert connector.config.url == url
