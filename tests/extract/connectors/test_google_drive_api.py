from unittest.mock import Mock, patch

import pytest
from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import Error as GoogleAPIError
from googleapiclient.errors import HttpError

from lubrikit.extract import GoogleDriveAPIConnector
from lubrikit.extract.connectors.configs import (
    GoogleDriveAPIConfig,
    GoogleDriveAPIServiceAccountInfo,
)
from lubrikit.utils.retry import RetryConfig


@pytest.fixture
def headers_cache() -> dict[str, str]:
    """Sample headers cache."""
    return {
        "file_name": "test_file.csv",
        "last_modified": "2023-08-12T21:52:29.054Z",
        "content_length": "1605",
    }


@pytest.fixture
def google_drive_config() -> GoogleDriveAPIConfig:
    """Sample GoogleDrive configuration."""
    return GoogleDriveAPIConfig(file_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms")


@pytest.fixture
def service_account_info() -> GoogleDriveAPIServiceAccountInfo:
    """Sample service account info."""
    return GoogleDriveAPIServiceAccountInfo(
        type="service_account",
        project_id="test-project",
        private_key_id="test-key-id",
        private_key="-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        client_email="test@test-project.iam.gserviceaccount.com",
        client_id="123456789012345678901",
        auth_uri="https://accounts.google.com/o/oauth2/auth",
        token_uri="https://oauth2.googleapis.com/token",
        auth_provider_x509_cert_url="https://www.googleapis.com/oauth2/v1/certs",
        client_x509_cert_url="https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com",
        universe_domain="googleapis.com",
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
def connector(
    google_drive_config: GoogleDriveAPIConfig,
    headers_cache: dict[str, str],
    service_account_info: GoogleDriveAPIServiceAccountInfo,
) -> GoogleDriveAPIConnector:
    """GoogleDriveAPIConnector instance with default configuration."""
    return GoogleDriveAPIConnector(
        config=google_drive_config,
        headers_cache=headers_cache,
        service_account_info=service_account_info,
    )


@pytest.fixture
def connector_with_service_account(
    google_drive_config: GoogleDriveAPIConfig,
    service_account_info: GoogleDriveAPIServiceAccountInfo,
    headers_cache: dict[str, str],
) -> GoogleDriveAPIConnector:
    """GoogleDriveAPIConnector instance with explicit service account info."""
    return GoogleDriveAPIConnector(
        config=google_drive_config,
        service_account_info=service_account_info,
        headers_cache=headers_cache,
    )


@pytest.fixture
def connector_with_retry(
    google_drive_config: GoogleDriveAPIConfig,
    headers_cache: dict[str, str],
    retry_config: RetryConfig,
    service_account_info: GoogleDriveAPIServiceAccountInfo,
) -> GoogleDriveAPIConnector:
    """GoogleDriveAPIConnector instance with retry configuration."""
    return GoogleDriveAPIConnector(
        config=google_drive_config,
        headers_cache=headers_cache,
        retry_config=retry_config,
        service_account_info=service_account_info,
    )


@pytest.fixture
def mock_google_api_client() -> Mock:
    """Mock Google API client resource."""
    mock_client = Mock()
    mock_files = Mock()
    mock_client.files.return_value = mock_files

    # Mock get method for file metadata
    mock_get = Mock()
    mock_files.get.return_value = mock_get
    mock_get.execute.return_value = {
        "size": "1024",
        "name": "test_file.csv",
        "modifiedTime": "2023-08-12T21:52:29.054Z",
    }

    # Mock get_media method for file download
    mock_get_media = Mock()
    mock_files.get_media.return_value = mock_get_media

    return mock_client


@patch("lubrikit.extract.connectors.google_drive_api.GoogleDriveAPIServiceAccountInfo")
def test_initialization_default(
    mock_service_account_info: Mock,
    google_drive_config: GoogleDriveAPIConfig,
    headers_cache: dict[str, str],
) -> None:
    """Test GoogleDriveAPIConnector initialization with default settings."""
    mock_service_account_instance = Mock()
    mock_service_account_info.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(
        config=google_drive_config, headers_cache=headers_cache
    )

    assert connector.config == google_drive_config
    assert connector.headers_cache == headers_cache
    assert connector.service_account_info == mock_service_account_instance
    assert connector.retry_config.timeout == 10.0
    assert connector.retry_config.max_retries == 3
    assert connector.client is None


def test_initialization_with_service_account(
    google_drive_config: GoogleDriveAPIConfig,
    service_account_info: GoogleDriveAPIServiceAccountInfo,
    headers_cache: dict[str, str],
) -> None:
    """Test GoogleDriveAPIConnector initialization with explicit service account."""
    connector = GoogleDriveAPIConnector(
        config=google_drive_config,
        service_account_info=service_account_info,
        headers_cache=headers_cache,
    )

    assert connector.config == google_drive_config
    assert connector.service_account_info == service_account_info
    assert connector.headers_cache == headers_cache


@patch("lubrikit.extract.connectors.google_drive_api.GoogleDriveAPIServiceAccountInfo")
def test_initialization_with_retry_config(
    mock_service_account_info: Mock,
    google_drive_config: GoogleDriveAPIConfig,
    headers_cache: dict[str, str],
    retry_config: RetryConfig,
) -> None:
    """Test GoogleDriveAPIConnector initialization with custom retry config."""
    mock_service_account_instance = Mock()
    mock_service_account_info.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(
        config=google_drive_config,
        headers_cache=headers_cache,
        retry_config=retry_config,
    )

    assert connector.config == google_drive_config
    assert connector.retry_config == retry_config


@patch("lubrikit.extract.connectors.google_drive_api.GoogleDriveAPIServiceAccountInfo")
def test_initialization_empty_headers_cache(
    mock_service_account_info: Mock, google_drive_config: GoogleDriveAPIConfig
) -> None:
    """Test GoogleDriveAPIConnector initialization with empty headers cache."""
    mock_service_account_instance = Mock()
    mock_service_account_info.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(config=google_drive_config)

    assert connector.headers_cache == {}
    assert connector.config == google_drive_config


def test_retriable_exceptions_configuration(connector: GoogleDriveAPIConnector) -> None:
    """Test that retriable exceptions are properly configured."""
    expected_exceptions = (
        HttpError,
        RefreshError,
        TransportError,
        GoogleAPIError,
        ConnectionError,
        TimeoutError,
    )

    assert connector.retriable_exceptions == expected_exceptions


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_connect_method(
    mock_service_account: Mock,
    mock_build: Mock,
    connector: GoogleDriveAPIConnector,
    mock_google_api_client: Mock,
) -> None:
    """Test the connect method creates Google Drive client."""
    # Setup mocks
    mock_credentials = Mock()
    mock_scoped_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_scoped_credentials
    mock_build.return_value = mock_google_api_client

    # Call connect method
    result = connector.connect()

    # Verify calls
    mock_service_account.Credentials.from_service_account_info.assert_called_once_with(
        info=connector.service_account_info.model_dump()
    )
    mock_credentials.with_scopes.assert_called_once_with(GoogleDriveAPIConnector.scopes)
    mock_build.assert_called_once_with(
        GoogleDriveAPIConnector.api_name,
        GoogleDriveAPIConnector.api_version,
        credentials=mock_scoped_credentials,
    )

    assert result == mock_google_api_client


def test_content_length_property(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test content_length cached property."""
    connector.client = mock_google_api_client

    content_length = connector.content_length

    assert content_length == 1024
    mock_google_api_client.files().get.assert_called_once_with(
        fileId=connector.config.file_id, fields="size"
    )


def test_content_length_no_client(connector: GoogleDriveAPIConnector) -> None:
    """Test content_length property raises error when client not initialized."""
    with pytest.raises(ValueError, match="Google Drive client is not initialized"):
        _ = connector.content_length


def test_file_name_property(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test file_name cached property."""
    connector.client = mock_google_api_client

    file_name = connector.file_name

    assert file_name == "test_file.csv"
    mock_google_api_client.files().get.assert_called_once_with(
        fileId=connector.config.file_id, fields="name"
    )


def test_file_name_no_client(connector: GoogleDriveAPIConnector) -> None:
    """Test file_name property raises error when client not initialized."""
    with pytest.raises(ValueError, match="Google Drive client is not initialized"):
        _ = connector.file_name


def test_last_modified_at_property(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test last_modified_at cached property."""
    connector.client = mock_google_api_client

    last_modified = connector.last_modified_at

    assert last_modified == "2023-08-12T21:52:29.054Z"
    mock_google_api_client.files().get.assert_called_once_with(
        fileId=connector.config.file_id, fields="modifiedTime"
    )


def test_last_modified_at_no_client(connector: GoogleDriveAPIConnector) -> None:
    """Test last_modified_at property raises error when client not initialized."""
    with pytest.raises(ValueError, match="Google Drive client is not initialized"):
        _ = connector.last_modified_at


@patch("lubrikit.extract.connectors.google_drive_api.logger")
def test_check_success(
    mock_logger: Mock,
    connector: GoogleDriveAPIConnector,
    mock_google_api_client: Mock,
) -> None:
    """Test _check method with successful connection."""
    connector.client = None  # Start with no client

    with patch.object(
        connector, "connect", return_value=mock_google_api_client
    ) as mock_connect:
        with patch.object(
            connector, "_prepare_cache", return_value={"test": "cache"}
        ) as mock_prepare_cache:
            result = connector._check()

    assert result == {"test": "cache"}
    mock_connect.assert_called_once()
    mock_prepare_cache.assert_called_once()


@patch("lubrikit.extract.connectors.google_drive_api.logger")
def test_check_failure(
    mock_logger: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test _check method with connection failure."""
    with patch.object(connector, "connect", side_effect=Exception("Connection failed")):
        result = connector._check()

    assert result is None
    mock_logger.error.assert_called_once_with(
        "Failed to connect to Google Drive API: Connection failed"
    )


def test_prepare_cache(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test _prepare_cache method."""
    connector.client = mock_google_api_client

    # Mock the cached properties
    with patch.object(connector, "file_name", "test_file.csv"):
        with patch.object(connector, "last_modified_at", "2023-08-12T21:52:29.054Z"):
            with patch.object(connector, "content_length", 1024):
                cache = connector._prepare_cache()

    expected_cache = {
        "file_name": "test_file.csv",
        "last_modified": "2023-08-12T21:52:29.054Z",
        "content_length": "1024",
    }
    assert cache == expected_cache


def test_prepare_cache_with_none_values(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test _prepare_cache method with None values."""
    connector.client = mock_google_api_client

    # Mock the cached properties with None values
    with patch.object(connector, "file_name", "test.csv"):
        with patch.object(connector, "last_modified_at", None):
            with patch.object(connector, "content_length", None):
                cache = connector._prepare_cache()

    expected_cache = {
        "file_name": "test.csv",
        "last_modified": "",
        "content_length": "",
    }
    assert cache == expected_cache


@patch("lubrikit.extract.connectors.google_drive_api.MediaIoBaseDownload")
@patch("lubrikit.extract.connectors.google_drive_api.io.BytesIO")
def test_download_success_new_content(
    mock_bytes_io: Mock,
    mock_media_download: Mock,
    connector: GoogleDriveAPIConnector,
    mock_google_api_client: Mock,
) -> None:
    """Test _download method with successful download of new content."""
    # Setup
    mock_stream = Mock()
    mock_bytes_io.return_value = mock_stream
    mock_downloader = Mock()
    mock_media_download.return_value = mock_downloader

    # Mock different cache values to trigger download
    connector.headers_cache = {"last_modified": "old-date", "content_length": "500"}

    with patch.object(connector, "connect", return_value=mock_google_api_client):
        with patch.object(
            connector,
            "_prepare_cache",
            return_value={"last_modified": "new-date", "content_length": "1024"},
        ):
            headers, (stream, downloader) = connector._download()

    # Verify results
    assert headers == {"last_modified": "new-date", "content_length": "1024"}
    assert stream == mock_stream
    assert downloader == mock_downloader

    # Verify API calls
    mock_google_api_client.files().get_media.assert_called_once_with(
        fileId=connector.config.file_id
    )
    mock_media_download.assert_called_once_with(
        mock_stream, mock_google_api_client.files().get_media.return_value
    )


def test_download_no_new_version(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test _download method when no new version is available."""
    # Setup cache with same values as will be returned
    connector.headers_cache = {
        "last_modified": "same-date",
        "content_length": "1024",
    }

    with patch.object(connector, "connect", return_value=mock_google_api_client):
        with patch.object(
            connector,
            "_prepare_cache",
            return_value={"last_modified": "same-date", "content_length": "1024"},
        ):
            with patch(
                "lubrikit.extract.connectors.google_drive_api.logger"
            ) as mock_logger:
                headers, (stream, downloader) = connector._download()

    # Verify results
    assert headers == {"last_modified": "same-date", "content_length": "1024"}
    assert stream is None
    assert downloader is None

    # Verify no download was attempted
    mock_google_api_client.files().get_media.assert_not_called()
    mock_logger.info.assert_called_once_with("No new version. Skipping download.")


@pytest.mark.parametrize(
    "exception_type",
    [
        HttpError,
        RefreshError,
        TransportError,
        GoogleAPIError,
        ConnectionError,
        TimeoutError,
    ],
)
def test_retriable_exceptions_are_configured(
    connector: GoogleDriveAPIConnector, exception_type: type
) -> None:
    """Test that all expected retriable exceptions are configured."""
    assert exception_type in connector.retriable_exceptions


def test_class_attributes() -> None:
    """Test GoogleDriveAPIConnector class attributes."""
    assert GoogleDriveAPIConnector.api_name == "drive"
    assert GoogleDriveAPIConnector.api_version == "v3"
    assert GoogleDriveAPIConnector.scopes == ["https://www.googleapis.com/auth/drive"]


@patch("lubrikit.extract.connectors.google_drive_api.logger")
@patch("lubrikit.extract.connectors.google_drive_api.GoogleDriveAPIServiceAccountInfo")
def test_service_account_info_loading_from_env(
    mock_service_account_class: Mock,
    mock_logger: Mock,
    google_drive_config: GoogleDriveAPIConfig,
) -> None:
    """Test that service account info is loaded from environment when not provided."""
    mock_service_account_instance = Mock()
    mock_service_account_class.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(config=google_drive_config)

    assert connector.service_account_info == mock_service_account_instance
    mock_logger.info.assert_called_once_with(
        "Loading Google Drive service account info from environment variables"
    )


def test_cached_properties_are_cached(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test that cached properties are actually cached."""
    connector.client = mock_google_api_client

    # Call content_length multiple times
    _ = connector.content_length
    _ = connector.content_length
    _ = connector.content_length

    # Should only be called once due to caching
    assert mock_google_api_client.files().get.call_count == 1


def test_file_id_from_config(connector: GoogleDriveAPIConnector) -> None:
    """Test that file_id is correctly taken from config."""
    assert connector.config.file_id == "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"


@pytest.mark.parametrize(
    "headers_cache_input,expected_cache",
    [
        (None, {}),
        ({}, {}),
        ({"key": "value"}, {"key": "value"}),
    ],
)
@patch("lubrikit.extract.connectors.google_drive_api.GoogleDriveAPIServiceAccountInfo")
def test_headers_cache_initialization(
    mock_service_account_info: Mock,
    google_drive_config: GoogleDriveAPIConfig,
    headers_cache_input: dict[str, str] | None,
    expected_cache: dict[str, str],
) -> None:
    """Test headers_cache initialization with different inputs."""
    mock_service_account_instance = Mock()
    mock_service_account_info.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(
        config=google_drive_config, headers_cache=headers_cache_input
    )
    assert connector.headers_cache == expected_cache


def test_download_cache_comparison_logic(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test the specific cache comparison logic in _download method."""
    # Test case 1: Different last_modified should trigger download
    connector.headers_cache = {
        "last_modified": "old-date",
        "content_length": "1024",
    }

    with patch.object(connector, "connect", return_value=mock_google_api_client):
        with patch.object(
            connector,
            "_prepare_cache",
            return_value={"last_modified": "new-date", "content_length": "1024"},
        ):
            with patch(
                "lubrikit.extract.connectors.google_drive_api.MediaIoBaseDownload"
            ):
                with patch("lubrikit.extract.connectors.google_drive_api.io.BytesIO"):
                    _, (stream, downloader) = connector._download()

    # Should trigger download
    assert stream is not None
    assert downloader is not None

    # Test case 2: Different content_length should trigger download
    connector.headers_cache = {
        "last_modified": "same-date",
        "content_length": "500",
    }

    with patch.object(connector, "connect", return_value=mock_google_api_client):
        with patch.object(
            connector,
            "_prepare_cache",
            return_value={"last_modified": "same-date", "content_length": "1024"},
        ):
            with patch(
                "lubrikit.extract.connectors.google_drive_api.MediaIoBaseDownload"
            ):
                with patch("lubrikit.extract.connectors.google_drive_api.io.BytesIO"):
                    _, (stream, downloader) = connector._download()

    # Should trigger download
    assert stream is not None
    assert downloader is not None
