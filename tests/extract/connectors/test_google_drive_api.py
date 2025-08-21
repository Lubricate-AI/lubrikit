from typing import Any
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
def google_drive_config() -> dict[str, Any]:
    """Sample GoogleDrive configuration."""
    return {"fileId": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"}


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
def connector(
    google_drive_config: dict[str, Any],
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
    google_drive_config: dict[str, Any],
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
    google_drive_config: dict[str, Any],
    headers_cache: dict[str, str],
    retry_config: dict[str, int | float],
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
    google_drive_config: dict[str, Any],
    headers_cache: dict[str, str],
) -> None:
    """Test GoogleDriveAPIConnector initialization with default settings."""
    mock_service_account_instance = Mock()
    # Mock the model_dump() method that the client property will call
    mock_service_account_instance.model_dump.return_value = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "test-key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----\n",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "client_id": "123456789012345678901",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
    }
    mock_service_account_info.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(
        config=google_drive_config, headers_cache=headers_cache
    )

    assert connector.config == GoogleDriveAPIConfig(**google_drive_config)
    assert connector.headers_cache == headers_cache
    assert connector.service_account_info == mock_service_account_instance
    assert connector.retry_config.timeout == 10.0
    assert connector.retry_config.max_retries == 3
    # Note: client is now a cached_property that creates a real client when accessed
    # We don't test it directly here to avoid authentication issues


def test_initialization_with_service_account(
    google_drive_config: dict[str, Any],
    service_account_info: GoogleDriveAPIServiceAccountInfo,
    headers_cache: dict[str, str],
) -> None:
    """Test GoogleDriveAPIConnector initialization with explicit service account."""
    connector = GoogleDriveAPIConnector(
        config=google_drive_config,
        service_account_info=service_account_info,
        headers_cache=headers_cache,
    )

    assert connector.config == GoogleDriveAPIConfig(**google_drive_config)
    assert connector.service_account_info == service_account_info
    assert connector.headers_cache == headers_cache


@patch("lubrikit.extract.connectors.google_drive_api.GoogleDriveAPIServiceAccountInfo")
def test_initialization_with_retry_config(
    mock_service_account_info: Mock,
    google_drive_config: dict[str, Any],
    headers_cache: dict[str, str],
    retry_config: dict[str, int | float],
) -> None:
    """Test GoogleDriveAPIConnector initialization with custom retry config."""
    mock_service_account_instance = Mock()
    mock_service_account_info.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(
        config=google_drive_config,
        headers_cache=headers_cache,
        retry_config=retry_config,
    )

    assert connector.config == GoogleDriveAPIConfig(**google_drive_config)
    assert connector.retry_config == RetryConfig(**retry_config)


@patch("lubrikit.extract.connectors.google_drive_api.GoogleDriveAPIServiceAccountInfo")
def test_initialization_empty_headers_cache(
    mock_service_account_info: Mock, google_drive_config: dict[str, Any]
) -> None:
    """Test GoogleDriveAPIConnector initialization with empty headers cache."""
    mock_service_account_instance = Mock()
    mock_service_account_info.return_value = mock_service_account_instance

    connector = GoogleDriveAPIConnector(config=google_drive_config)

    assert connector.headers_cache == {}
    assert connector.config == GoogleDriveAPIConfig(**google_drive_config)


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


def test_content_length_property(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test content_length cached property."""
    connector.client = mock_google_api_client

    content_length = connector.content_length

    assert content_length == 1024
    mock_google_api_client.files().get.assert_called_once_with(
        fileId=connector.config.fileId, fields="size"
    )


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_content_length_with_mocked_client(
    mock_service_account: Mock, mock_build: Mock, connector: GoogleDriveAPIConnector
) -> None:
    """Test content_length property with mocked Google API client."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client
    mock_client.files().get().execute.return_value = {"size": "2048"}

    content_length = connector.content_length

    assert content_length == 2048


def test_file_name_property(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test file_name cached property."""
    connector.client = mock_google_api_client

    file_name = connector.file_name

    assert file_name == "test_file.csv"
    mock_google_api_client.files().get.assert_called_once_with(
        fileId=connector.config.fileId, fields="name"
    )


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_file_name_with_mocked_client(
    mock_service_account: Mock, mock_build: Mock, connector: GoogleDriveAPIConnector
) -> None:
    """Test file_name property with mocked Google API client."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client
    mock_client.files().get().execute.return_value = {"name": "mocked_file.csv"}

    file_name = connector.file_name

    assert file_name == "mocked_file.csv"


def test_last_modified_at_property(
    connector: GoogleDriveAPIConnector, mock_google_api_client: Mock
) -> None:
    """Test last_modified_at cached property."""
    connector.client = mock_google_api_client

    last_modified = connector.last_modified_at

    assert last_modified == "2023-08-12T21:52:29.054Z"
    mock_google_api_client.files().get.assert_called_once_with(
        fileId=connector.config.fileId, fields="modifiedTime"
    )


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_last_modified_at_with_mocked_client(
    mock_service_account: Mock, mock_build: Mock, connector: GoogleDriveAPIConnector
) -> None:
    """Test last_modified_at property with mocked Google API client."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client
    mock_client.files().get().execute.return_value = {
        "modifiedTime": "2023-12-01T10:00:00.000Z"
    }

    last_modified = connector.last_modified_at

    assert last_modified == "2023-12-01T10:00:00.000Z"


@patch("lubrikit.extract.connectors.google_drive_api.logger")
@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_check_success(
    mock_service_account: Mock,
    mock_build: Mock,
    mock_logger: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test _check method with successful connection."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Set up separate mock objects for different API calls
    mock_files = Mock()
    mock_client.files.return_value = mock_files

    # Mock file_name property call
    mock_name_get = Mock()
    mock_name_get.execute.return_value = {"name": "test_file.csv"}

    # Mock last_modified_at property call
    mock_modified_get = Mock()
    mock_modified_get.execute.return_value = {
        "modifiedTime": "2023-08-12T21:52:29.054Z"
    }

    # Mock content_length property call
    mock_size_get = Mock()
    mock_size_get.execute.return_value = {"size": "1605"}

    # Mock supported_mime_types property call (for _validate_mime_type)
    mock_export_get = Mock()
    mock_export_get.execute.return_value = {"exportLinks": {}}

    # Configure the mock to return different responses based on the fields parameter
    def mock_get(**kwargs: Any) -> Any:
        fields = kwargs.get("fields", "")
        if fields == "name":
            return mock_name_get
        elif fields == "modifiedTime":
            return mock_modified_get
        elif fields == "size":
            return mock_size_get
        elif fields == "exportLinks":
            return mock_export_get
        else:
            return Mock()

    mock_files.get.side_effect = mock_get

    result = connector._check()

    assert result is not None
    assert result["file_name"] == "test_file.csv"
    assert result["last_modified"] == "2023-08-12T21:52:29.054Z"
    assert result["content_length"] == "1605"


@patch("lubrikit.extract.connectors.google_drive_api.logger")
@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_check_failure(
    mock_service_account: Mock,
    mock_build: Mock,
    mock_logger: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test _check method with connection failure."""
    # Mock service account to raise an exception
    mock_service_account.Credentials.from_service_account_info.side_effect = Exception(
        "Connection failed"
    )

    # This should raise an exception since the client property will fail
    with pytest.raises(Exception, match="Connection failed"):
        _ = connector._check()


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
@patch("lubrikit.extract.connectors.google_drive_api.io.FileIO")
@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_download_success_new_content(
    mock_service_account: Mock,
    mock_build: Mock,
    mock_file_io: Mock,
    mock_media_download: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test _download method with successful download of new content."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Setup file and download mocks
    mock_file_handle = Mock()
    mock_file_io.return_value = mock_file_handle
    mock_downloader = Mock()
    mock_media_download.return_value = mock_downloader

    # Mock different cache values to trigger download
    connector.headers_cache = {"last_modified": "old-date", "content_length": "500"}

    # Mock the properties that _prepare_cache calls
    mock_client.files().get().execute.side_effect = [
        {"name": "test_file.csv"},
        {"modifiedTime": "new-date"},
        {"size": "1024"},
    ]

    # Mock validation to pass
    with patch.object(connector, "_validate_mime_type"):
        headers, downloader = connector._download()

    # Verify results
    assert (headers or {}).get("file_name") == "test_file.csv"
    assert (headers or {}).get("last_modified") == "new-date"
    assert (headers or {}).get("content_length") == "1024"
    assert downloader == mock_downloader


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_download_no_new_version(
    mock_service_account: Mock,
    mock_build: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test _download method when no new version is available."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Setup cache with same values as will be returned
    connector.headers_cache = {
        "file_name": "test_file.csv",
        "last_modified": "same-date",
        "content_length": "1024",
    }

    # Mock the properties that _prepare_cache calls to return same values
    mock_client.files().get().execute.side_effect = [
        {"name": "test_file.csv"},
        {"modifiedTime": "same-date"},
        {"size": "1024"},
    ]

    with patch("lubrikit.extract.connectors.google_drive_api.logger") as mock_logger:
        with patch.object(connector, "_validate_mime_type"):
            headers, downloader = connector._download()

    # Verify results
    assert (headers or {}).get("file_name") == "test_file.csv"
    assert (headers or {}).get("last_modified") == "same-date"
    assert (headers or {}).get("content_length") == "1024"
    assert downloader is None

    # Verify no download was attempted
    mock_client.files().get_media.assert_not_called()
    mock_client.files().export_media.assert_not_called()
    # Check that the specific message was logged (among other info messages)
    mock_logger.info.assert_any_call("No new version. Skipping download.")


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
    google_drive_config: dict[str, Any],
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


def test_fileId_from_config(connector: GoogleDriveAPIConnector) -> None:
    """Test that fileId is correctly taken from config."""
    assert connector.config.fileId == "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"


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
    google_drive_config: dict[str, Any],
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


@patch("lubrikit.extract.connectors.google_drive_api.MediaIoBaseDownload")
@patch("lubrikit.extract.connectors.google_drive_api.io.FileIO")
@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_download_cache_comparison_logic(
    mock_service_account: Mock,
    mock_build: Mock,
    mock_file_io: Mock,
    mock_media_download: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test the specific cache comparison logic in _download method."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client
    mock_downloader = Mock()
    mock_media_download.return_value = mock_downloader
    mock_file_handle = Mock()
    mock_file_io.return_value = mock_file_handle

    # Test case 1: Different last_modified should trigger download
    connector.headers_cache = {
        "file_name": "test.csv",
        "last_modified": "old-date",
        "content_length": "1024",
    }

    # Mock the properties that _prepare_cache calls - different last_modified
    mock_client.files().get().execute.side_effect = [
        {"name": "test.csv"},
        {"modifiedTime": "new-date"},
        {"size": "1024"},
    ]

    with patch.object(connector, "_validate_mime_type"):
        _, downloader = connector._download()

    # Should trigger download
    assert downloader is not None

    # Reset for test case 2
    mock_client.reset_mock()

    # Test case 2: Different content_length should trigger download
    connector.headers_cache = {
        "file_name": "test.csv",
        "last_modified": "same-date",
        "content_length": "500",
    }

    # Mock the properties that _prepare_cache calls - different content_length
    mock_client.files().get().execute.side_effect = [
        {"name": "test.csv"},
        {"modifiedTime": "same-date"},
        {"size": "1024"},
    ]

    with patch.object(connector, "_validate_mime_type"):
        _, downloader = connector._download()

    # Should trigger download
    assert downloader is not None


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_supported_mime_types_property(
    mock_service_account: Mock,
    mock_build: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test supported_mime_types property returns sorted list of MIME types."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Mock the API response with export links
    mock_client.files().get().execute.return_value = {
        "exportLinks": {
            "text/csv": "https://docs.google.com/spreadsheets/export?id=123&format=csv",
            "application/pdf": "https://docs.google.com/spreadsheets/export?id=123&format=pdf",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "https://docs.google.com/spreadsheets/export?id=123&format=xlsx",
        }
    }

    # Get supported MIME types
    supported_types = connector.supported_mime_types

    # Should return sorted list of MIME types
    expected_types = [
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "text/csv",
    ]
    assert supported_types == expected_types

    # Verify the API was called correctly with the expected parameters
    mock_client.files().get.assert_any_call(
        fileId=connector.config.fileId, fields="exportLinks"
    )


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_supported_mime_types_empty_export_links(
    mock_service_account: Mock,
    mock_build: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test supported_mime_types property with empty export links."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Mock the API response with no export links
    mock_client.files().get().execute.return_value = {"exportLinks": {}}

    # Get supported MIME types
    supported_types = connector.supported_mime_types

    # Should return empty list
    assert supported_types == []


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_supported_mime_types_missing_export_links(
    mock_service_account: Mock,
    mock_build: Mock,
    connector: GoogleDriveAPIConnector,
) -> None:
    """Test supported_mime_types property when exportLinks key is missing."""
    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Mock the API response without exportLinks key
    mock_client.files().get().execute.return_value = {}

    # Get supported MIME types
    supported_types = connector.supported_mime_types

    # Should return empty list when exportLinks key is missing
    assert supported_types == []


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_validate_mime_type_success(
    mock_service_account: Mock,
    mock_build: Mock,
    google_drive_config: dict[str, Any],
    service_account_info: GoogleDriveAPIServiceAccountInfo,
    headers_cache: dict[str, str],
) -> None:
    """Test _validate_mime_type method with valid MIME type."""
    # Create config with a MIME type
    config_with_mime = {"fileId": google_drive_config["fileId"], "mimeType": "text/csv"}

    connector = GoogleDriveAPIConnector(
        config=config_with_mime,
        service_account_info=service_account_info,
        headers_cache=headers_cache,
    )

    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Mock the API response to include the configured MIME type
    mock_client.files().get().execute.return_value = {
        "exportLinks": {
            "text/csv": "https://docs.google.com/spreadsheets/export?id=123&format=csv",
            "application/pdf": "https://docs.google.com/spreadsheets/export?id=123&format=pdf",
        }
    }

    # Should not raise any exception
    connector._validate_mime_type()


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_validate_mime_type_invalid(
    mock_service_account: Mock,
    mock_build: Mock,
    google_drive_config: dict[str, Any],
    service_account_info: GoogleDriveAPIServiceAccountInfo,
    headers_cache: dict[str, str],
) -> None:
    """Test _validate_mime_type method with invalid MIME type."""
    # Create config with an unsupported MIME type
    config_with_mime = {
        "fileId": google_drive_config["fileId"],
        "mimeType": "unsupported/type",
    }

    connector = GoogleDriveAPIConnector(
        config=config_with_mime,
        service_account_info=service_account_info,
        headers_cache=headers_cache,
    )

    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Mock the API response to NOT include the configured MIME type
    mock_client.files().get().execute.return_value = {
        "exportLinks": {
            "text/csv": "https://docs.google.com/spreadsheets/export?id=123&format=csv",
            "application/pdf": "https://docs.google.com/spreadsheets/export?id=123&format=pdf",
        }
    }

    with pytest.raises(ValueError, match="Unsupported MIME type 'unsupported/type'"):
        connector._validate_mime_type()


def test_validate_mime_type_none(
    google_drive_config: dict[str, Any],
    service_account_info: GoogleDriveAPIServiceAccountInfo,
    headers_cache: dict[str, str],
) -> None:
    """Test _validate_mime_type method with None MIME type."""
    # Use config without MIME type (None)
    connector = GoogleDriveAPIConnector(
        config=google_drive_config,  # This has mimeType=None
        service_account_info=service_account_info,
        headers_cache=headers_cache,
    )

    # Should not raise any exception and should not check supported types
    # This should work without any mocking since it returns early when mimeType is None
    connector._validate_mime_type()


@patch("lubrikit.extract.connectors.google_drive_api.build")
@patch("lubrikit.extract.connectors.google_drive_api.service_account")
def test_validate_mime_type_error_message(
    mock_service_account: Mock,
    mock_build: Mock,
    google_drive_config: dict[str, Any],
    service_account_info: GoogleDriveAPIServiceAccountInfo,
    headers_cache: dict[str, str],
) -> None:
    """Test _validate_mime_type method error message includes supported types."""
    # Create config with an unsupported MIME type
    config_with_mime = {
        "fileId": google_drive_config["fileId"],
        "mimeType": "invalid/format",
    }

    connector = GoogleDriveAPIConnector(
        config=config_with_mime,
        service_account_info=service_account_info,
        headers_cache=headers_cache,
    )

    # Mock the service account credentials
    mock_credentials = Mock()
    mock_service_account.Credentials.from_service_account_info.return_value = (
        mock_credentials
    )
    mock_credentials.with_scopes.return_value = mock_credentials

    # Mock the Google API client
    mock_client = Mock()
    mock_build.return_value = mock_client

    # Mock the API response with specific supported types
    mock_client.files().get().execute.return_value = {
        "exportLinks": {
            "text/csv": "https://docs.google.com/spreadsheets/export?id=123&format=csv",
            "application/pdf": "https://docs.google.com/spreadsheets/export?id=123&format=pdf",
            "text/html": "https://docs.google.com/spreadsheets/export?id=123&format=html",
        }
    }

    with pytest.raises(ValueError) as exc_info:
        connector._validate_mime_type()

    error_message = str(exc_info.value)
    assert "Unsupported MIME type 'invalid/format'" in error_message
    assert "Supported types: application/pdf, text/csv, text/html" in error_message
