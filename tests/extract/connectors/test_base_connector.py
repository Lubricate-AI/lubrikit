from typing import Any
from unittest.mock import Mock, patch

import pytest

from lubrikit.extract.connectors.base import BaseConnector
from lubrikit.utils.retry import RetryConfig


class ConcreteConnector(BaseConnector):
    """Concrete implementation of BaseConnector for testing purposes."""

    def __init__(
        self,
        headers_cache: dict[str, str] | None = None,
        retry_config: dict[str, int | float] | None = None,
        retriable_exceptions: tuple[type[Exception], ...] = (),
    ) -> None:
        super().__init__(headers_cache, retry_config)
        self.retriable_exceptions = retriable_exceptions

    def _check(self) -> dict[str, Any] | None:
        """Mock implementation of _check."""
        return {"status": "checked", "timestamp": "2023-01-01T00:00:00Z"}

    def _download(self) -> tuple[dict[str, Any] | None, Any]:
        """Mock implementation of _download."""
        return {"status": "downloaded", "size": 1024}, "mock_response_data"

    def _prepare_cache(self, *args: Any, **kwargs: Any) -> dict[str, str]:
        """Mock implementation of _prepare_cache."""
        return {"cache_key": "cache_value"}


@pytest.fixture
def headers_cache() -> dict[str, str]:
    """Sample headers cache."""
    return {
        "etag": "test-etag",
        "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        "content_length": "1024",
    }


@pytest.fixture
def connector(headers_cache: dict[str, str]) -> ConcreteConnector:
    """BaseConnector concrete implementation for testing."""
    return ConcreteConnector(headers_cache)


@pytest.fixture
def connector_with_retry(
    headers_cache: dict[str, str], retry_config: dict[str, int | float]
) -> ConcreteConnector:
    """BaseConnector with custom retry configuration."""
    return ConcreteConnector(headers_cache, retry_config)


@pytest.mark.parametrize(
    "feature, expected",
    [
        ("timeout", 10.0),
        ("max_retries", 3),
        ("base_delay", 1.0),
        ("max_delay", 60.0),
        ("backoff_factor", 2.0),
    ],
)
def test_initialization_default(
    headers_cache: dict[str, str], feature: str, expected: int | float
) -> None:
    """Test BaseConnector initialization with default retry config."""
    connector = ConcreteConnector(headers_cache)

    assert connector.headers_cache == headers_cache
    assert getattr(connector.retry_config, feature) == expected


def test_initialization_with_retry_config(
    headers_cache: dict[str, str], retry_config: dict[str, int | float]
) -> None:
    """Test BaseConnector initialization with custom retry config."""
    connector = ConcreteConnector(headers_cache, retry_config)

    assert connector.headers_cache == headers_cache
    assert connector.retry_config == RetryConfig(**retry_config)


def test_initialization_empty_headers_cache() -> None:
    """Test BaseConnector initialization with empty headers cache."""
    connector = ConcreteConnector()

    assert connector.headers_cache == {}
    assert isinstance(connector.retry_config, RetryConfig)


def test_initialization_none_headers_cache() -> None:
    """Test BaseConnector initialization with None headers cache."""
    connector = ConcreteConnector(headers_cache=None)

    assert connector.headers_cache == {}


@patch("lubrikit.extract.connectors.base.retry_with_backoff")
def test_check_method_calls_retry_decorator(
    mock_retry_with_backoff: Mock, connector: ConcreteConnector
) -> None:
    """Test that check method creates and uses retry decorator correctly."""
    mock_decorator = Mock()
    mock_retry_with_backoff.return_value = mock_decorator
    mock_decorated_function = Mock()
    mock_decorator.return_value = mock_decorated_function
    mock_decorated_function.return_value = {"test": "result"}

    result = connector.check()

    # Verify retry decorator was created with correct parameters
    mock_retry_with_backoff.assert_called_once_with(
        max_retries=connector.retry_config.max_retries,
        base_delay=connector.retry_config.base_delay,
        max_delay=connector.retry_config.max_delay,
        backoff_factor=connector.retry_config.backoff_factor,
        retriable_exceptions=connector.retriable_exceptions,
    )

    # Verify decorator was applied to _check method
    mock_decorator.assert_called_once_with(connector._check)

    # Verify decorated function was called
    mock_decorated_function.assert_called_once_with()

    assert result == {"test": "result"}


@patch("lubrikit.extract.connectors.base.retry_with_backoff")
def test_download_method_calls_retry_decorator(
    mock_retry_with_backoff: Mock, connector: ConcreteConnector
) -> None:
    """Test that download method creates and uses retry decorator correctly."""
    mock_decorator = Mock()
    mock_retry_with_backoff.return_value = mock_decorator
    mock_decorated_function = Mock()
    mock_decorator.return_value = mock_decorated_function
    mock_decorated_function.return_value = ({"test": "headers"}, "response_data")

    result = connector.download()

    # Verify retry decorator was created with correct parameters
    mock_retry_with_backoff.assert_called_once_with(
        max_retries=connector.retry_config.max_retries,
        base_delay=connector.retry_config.base_delay,
        max_delay=connector.retry_config.max_delay,
        backoff_factor=connector.retry_config.backoff_factor,
        retriable_exceptions=connector.retriable_exceptions,
    )

    # Verify decorator was applied to _download method
    mock_decorator.assert_called_once_with(connector._download)

    # Verify decorated function was called
    mock_decorated_function.assert_called_once_with()

    assert result == ({"test": "headers"}, "response_data")


def test_check_method_direct_call(connector: ConcreteConnector) -> None:
    """Test check method without mocking retry decorator."""
    result = connector.check()

    # Should return the result from _check method
    assert result == {"status": "checked", "timestamp": "2023-01-01T00:00:00Z"}


def test_download_method_direct_call(connector: ConcreteConnector) -> None:
    """Test download method without mocking retry decorator."""
    result = connector.download()

    # Should return the result from _download method
    assert result == ({"status": "downloaded", "size": 1024}, "mock_response_data")


@patch("lubrikit.extract.connectors.base.retry_with_backoff")
def test_check_uses_custom_retry_config(
    mock_retry_with_backoff: Mock, connector_with_retry: ConcreteConnector
) -> None:
    """Test that check method uses custom retry configuration."""
    mock_decorator = Mock()
    mock_retry_with_backoff.return_value = mock_decorator
    mock_decorated_function = Mock()
    mock_decorator.return_value = mock_decorated_function

    connector_with_retry.check()

    # Verify custom retry config values were used
    mock_retry_with_backoff.assert_called_once_with(
        max_retries=2,  # Custom value
        base_delay=0.5,  # Custom value
        max_delay=30.0,  # Custom value
        backoff_factor=2.0,  # Custom value
        retriable_exceptions=connector_with_retry.retriable_exceptions,
    )


@patch("lubrikit.extract.connectors.base.retry_with_backoff")
def test_download_uses_custom_retry_config(
    mock_retry_with_backoff: Mock, connector_with_retry: ConcreteConnector
) -> None:
    """Test that download method uses custom retry configuration."""
    mock_decorator = Mock()
    mock_retry_with_backoff.return_value = mock_decorator
    mock_decorated_function = Mock()
    mock_decorator.return_value = mock_decorated_function

    connector_with_retry.download()

    # Verify custom retry config values were used
    mock_retry_with_backoff.assert_called_once_with(
        max_retries=2,  # Custom value
        base_delay=0.5,  # Custom value
        max_delay=30.0,  # Custom value
        backoff_factor=2.0,  # Custom value
        retriable_exceptions=connector_with_retry.retriable_exceptions,
    )


def test_check_with_retriable_exceptions() -> None:
    """Test check method with custom retriable exceptions."""
    custom_exceptions = (ConnectionError, TimeoutError, ValueError)
    connector = ConcreteConnector(
        headers_cache={}, retriable_exceptions=custom_exceptions
    )

    # This should work without raising an exception
    result = connector.check()
    assert result == {"status": "checked", "timestamp": "2023-01-01T00:00:00Z"}


def test_download_with_retriable_exceptions() -> None:
    """Test download method with custom retriable exceptions."""
    custom_exceptions = (ConnectionError, TimeoutError, ValueError)
    connector = ConcreteConnector(
        headers_cache={}, retriable_exceptions=custom_exceptions
    )

    # This should work without raising an exception
    result = connector.download()
    assert result == ({"status": "downloaded", "size": 1024}, "mock_response_data")


@pytest.mark.parametrize(
    "method_name, feature",
    [
        ("check", "max_retries"),
        ("check", "base_delay"),
        ("check", "max_delay"),
        ("check", "backoff_factor"),
        ("download", "max_retries"),
        ("download", "base_delay"),
        ("download", "max_delay"),
        ("download", "backoff_factor"),
    ],
)
@patch("lubrikit.extract.connectors.base.retry_with_backoff")
def test_methods_use_same_retry_configuration(
    mock_retry_with_backoff: Mock,
    connector: ConcreteConnector,
    method_name: str,
    feature: str,
) -> None:
    """Test that both methods use the same retry configuration."""
    mock_decorator = Mock()
    mock_retry_with_backoff.return_value = mock_decorator
    mock_decorated_function = Mock()
    mock_decorator.return_value = mock_decorated_function

    method = getattr(connector, method_name)
    method()

    # Both methods should use the same retry configuration
    expected_call = mock_retry_with_backoff.call_args
    assert expected_call[1][feature] == getattr(connector.retry_config, feature)


def test_headers_cache_is_mutable(connector: ConcreteConnector) -> None:
    """Test that headers_cache can be modified after initialization."""
    original_cache = connector.headers_cache.copy()
    connector.headers_cache["new_key"] = "new_value"

    assert connector.headers_cache != original_cache
    assert connector.headers_cache["new_key"] == "new_value"


@pytest.mark.parametrize(
    "feature, expected",
    [
        ("timeout", 10.0),
        ("max_retries", 3),
        ("base_delay", 1.0),
        ("max_delay", 60.0),
        ("backoff_factor", 2.0),
    ],
)
def test_retry_config_defaults(feature: str, expected: int | float) -> None:
    """Test that default retry configuration is properly set."""
    connector = ConcreteConnector()

    assert getattr(connector.retry_config, feature) == expected
