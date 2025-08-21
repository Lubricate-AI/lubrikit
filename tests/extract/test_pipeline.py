from typing import Any
from unittest.mock import Mock, patch

import pytest
from googleapiclient.http import MediaIoBaseDownload
from requests import Response

from lubrikit.extract.connectors import GoogleDriveAPIConnector, HTTPConnector
from lubrikit.extract.pipeline import ExtractPipeline
from lubrikit.extract.storage import ExtractStorageClient


@pytest.fixture
def http_metadata() -> dict[str, Any]:
    """Sample metadata for HTTPConnector."""
    return {
        "connector": "HTTPConnector",
        "connector_config": {
            "url": "https://api.example.com/data.json",
            "method": "GET",
        },
        "headers_cache": {},
        "prefix": "api_data",
        "source_name": "test_api",
        "created_at": "2024-01-01T00:00:00",
        "modified_at": "2024-01-01T00:00:00",
    }


@pytest.fixture
def gdrive_metadata() -> dict[str, Any]:
    """Sample metadata for GoogleDriveAPIConnector."""
    return {
        "connector": "GoogleDriveAPIConnector",
        "connector_config": {"fileId": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"},
        "headers_cache": {},
        "prefix": "gdrive_files",
        "source_name": "google_drive",
        "created_at": "2024-01-01T00:00:00",
        "modified_at": "2024-01-01T00:00:00",
    }


def test_init_with_http_metadata(http_metadata: dict[str, Any]) -> None:
    """Test ExtractPipeline initialization with HTTP metadata."""
    pipeline = ExtractPipeline(http_metadata)

    assert isinstance(pipeline.metadata, dict)
    assert pipeline.metadata["connector"] == "HTTPConnector"
    assert pipeline.metadata["source_name"] == "test_api"
    assert pipeline.metadata["prefix"] == "api_data"


def test_init_with_gdrive_metadata(gdrive_metadata: dict[str, Any]) -> None:
    """Test ExtractPipeline initialization with Google Drive metadata."""
    pipeline = ExtractPipeline(gdrive_metadata)

    assert isinstance(pipeline.metadata, dict)
    assert pipeline.metadata["connector"] == "GoogleDriveAPIConnector"
    assert pipeline.metadata["source_name"] == "google_drive"
    assert pipeline.metadata["prefix"] == "gdrive_files"


def test_client_property(http_metadata: dict[str, Any]) -> None:
    """Test client property returns ExtractStorageClient."""
    pipeline = ExtractPipeline(http_metadata)

    client = pipeline.client
    assert isinstance(client, ExtractStorageClient)
    assert client.metadata == pipeline.metadata


def test_client_cached_property(http_metadata: dict[str, Any]) -> None:
    """Test client property is cached."""
    pipeline = ExtractPipeline(http_metadata)

    client1 = pipeline.client
    client2 = pipeline.client

    # Should be the same instance due to @cached_property
    assert client1 is client2


def test_connector_property_http(http_metadata: dict[str, Any]) -> None:
    """Test connector property returns HTTPConnector class."""
    pipeline = ExtractPipeline(http_metadata)

    connector_class = pipeline.connector
    assert connector_class is HTTPConnector
    assert issubclass(connector_class, HTTPConnector)


def test_connector_property_gdrive(gdrive_metadata: dict[str, Any]) -> None:
    """Test connector property returns GoogleDriveAPIConnector class."""
    pipeline = ExtractPipeline(gdrive_metadata)

    connector_class = pipeline.connector
    assert connector_class is GoogleDriveAPIConnector
    assert issubclass(connector_class, GoogleDriveAPIConnector)


def test_connector_property_invalid_connector() -> None:
    """Test connector property with invalid connector name."""
    invalid_metadata = {
        "connector": "InvalidConnector",
        "connector_config": "{}",
        "created_at": "2024-01-01T00:00:00",
        "modified_at": "2024-01-01T00:00:00",
    }

    pipeline = ExtractPipeline(invalid_metadata)

    with pytest.raises(ValueError, match="Connector InvalidConnector not found"):
        _ = pipeline.connector


@patch("lubrikit.extract.pipeline.connectors")
def test_connector_property_none_connector(
    mock_connectors: Mock, http_metadata: dict[str, Any]
) -> None:
    """Test connector property when getattr returns None."""
    mock_connectors.HTTPConnector = None

    pipeline = ExtractPipeline(http_metadata)

    with pytest.raises(ValueError, match="Connector HTTPConnector not found"):
        _ = pipeline.connector


@patch.object(ExtractStorageClient, "write")
@patch("lubrikit.extract.connectors.HTTPConnector")
def test_run_with_http_connector_success(
    mock_http_connector: Mock, mock_write: Mock, http_metadata: dict[str, Any]
) -> None:
    """Test run method with HTTPConnector returning data."""
    pipeline = ExtractPipeline(http_metadata)

    # Mock the connector and its download method
    mock_response = Mock(spec=Response)
    mock_headers = {"etag": "abc123", "content_length": "1024"}

    mock_connector_instance = Mock()
    mock_connector_instance.download.return_value = (mock_headers, mock_response)
    mock_http_connector.return_value = mock_connector_instance

    pipeline.run()

    # Verify connector was instantiated with correct parameters
    mock_http_connector.assert_called_once_with(
        config=pipeline.metadata.get("connector_config"),
        headers_cache=pipeline.metadata.get("headers_cache"),
        retry_config=pipeline.metadata.get("retry_config"),
    )

    # Verify download was called
    mock_connector_instance.download.assert_called_once()

    # Verify write was called with the response
    mock_write.assert_called_once_with(mock_response)


@patch.object(ExtractStorageClient, "write")
@patch("lubrikit.extract.connectors.GoogleDriveAPIConnector")
def test_run_with_gdrive_connector_success(
    mock_gdrive_connector: Mock, mock_write: Mock, gdrive_metadata: dict[str, Any]
) -> None:
    """Test run method with GoogleDriveAPIConnector returning data."""
    pipeline = ExtractPipeline(gdrive_metadata)

    # Mock the connector and its download method
    mock_downloader = Mock(spec=MediaIoBaseDownload)
    mock_headers = {"file_name": "test.xlsx", "last_modified": "2024-01-01T00:00:00Z"}

    mock_connector_instance = Mock()
    mock_connector_instance.download.return_value = (mock_headers, mock_downloader)
    mock_gdrive_connector.return_value = mock_connector_instance

    pipeline.run()

    # Verify connector was instantiated with correct parameters
    mock_gdrive_connector.assert_called_once_with(
        config=pipeline.metadata.get("connector_config"),
        headers_cache=pipeline.metadata.get("headers_cache"),
        retry_config=pipeline.metadata.get("retry_config"),
    )

    # Verify download was called
    mock_connector_instance.download.assert_called_once()

    # Verify write was called with the downloader
    mock_write.assert_called_once_with(mock_downloader)


@patch.object(ExtractStorageClient, "write")
@patch("lubrikit.extract.connectors.HTTPConnector")
def test_run_with_no_data_to_download(
    mock_http_connector: Mock, mock_write: Mock, http_metadata: dict[str, Any]
) -> None:
    """Test run method when connector returns None (no new data)."""
    pipeline = ExtractPipeline(http_metadata)

    mock_headers = {"etag": "abc123"}

    mock_connector_instance = Mock()
    # Connector returns None when no new data is available
    mock_connector_instance.download.return_value = (mock_headers, None)
    mock_http_connector.return_value = mock_connector_instance

    pipeline.run()

    # Verify connector was called
    mock_http_connector.assert_called_once()
    mock_connector_instance.download.assert_called_once()

    # Verify write was NOT called since downloader is None
    mock_write.assert_not_called()


@patch.object(ExtractStorageClient, "write")
@patch("lubrikit.extract.connectors.HTTPConnector")
def test_run_with_false_downloader(
    mock_http_connector: Mock, mock_write: Mock, http_metadata: dict[str, Any]
) -> None:
    """Test run method when connector returns falsy downloader."""
    pipeline = ExtractPipeline(http_metadata)

    mock_headers = {"etag": "abc123"}

    mock_connector_instance = Mock()
    # Connector returns empty string (falsy value)
    mock_connector_instance.download.return_value = (mock_headers, "")
    mock_http_connector.return_value = mock_connector_instance

    pipeline.run()

    # Verify write was NOT called since downloader is falsy
    mock_write.assert_not_called()


@patch("lubrikit.extract.connectors.HTTPConnector")
def test_run_connector_instantiation_parameters(
    mock_http_connector: Mock, http_metadata: dict[str, Any]
) -> None:
    """Test that connector is instantiated with correct parameters."""
    pipeline = ExtractPipeline(http_metadata)

    mock_connector_instance = Mock()
    mock_connector_instance.download.return_value = ({}, None)
    mock_http_connector.return_value = mock_connector_instance

    pipeline.run()

    # Verify the exact parameters passed to connector
    mock_http_connector.assert_called_once_with(
        config=pipeline.metadata.get("connector_config"),
        headers_cache=pipeline.metadata.get("headers_cache"),
        retry_config=pipeline.metadata.get("retry_config"),
    )


def test_inheritance_from_base_pipeline(http_metadata: dict[str, Any]) -> None:
    """Test that ExtractPipeline properly inherits from Pipeline."""
    from lubrikit.base import Pipeline

    pipeline = ExtractPipeline(http_metadata)
    assert isinstance(pipeline, Pipeline)


def test_run_method_exists(http_metadata: dict[str, Any]) -> None:
    """Test that run method is properly implemented."""
    pipeline = ExtractPipeline(http_metadata)
    assert hasattr(pipeline, "run")
    assert callable(pipeline.run)


@patch.object(ExtractStorageClient, "write")
@patch("lubrikit.extract.connectors.HTTPConnector")
def test_run_end_to_end_flow(
    mock_http_connector: Mock, mock_write: Mock, http_metadata: dict[str, Any]
) -> None:
    """Test complete run flow from start to finish."""
    pipeline = ExtractPipeline(http_metadata)

    # Setup mocks
    mock_response = Mock(spec=Response)
    mock_headers = {"etag": "def456", "content_length": "2048"}

    mock_connector_instance = Mock()
    mock_connector_instance.download.return_value = (mock_headers, mock_response)
    mock_http_connector.return_value = mock_connector_instance

    # Execute the pipeline
    pipeline.run()

    # Verify the complete flow
    assert mock_http_connector.called
    assert mock_connector_instance.download.called
    assert mock_write.called

    # Verify the data flow
    call_args = mock_write.call_args
    assert call_args[0][0] is mock_response  # First positional argument


def test_metadata_conversion_types() -> None:
    """Test that metadata is properly converted to appropriate types."""
    string_metadata = {
        "connector": "HTTPConnector",
        "connector_config": {},
        "headers_cache": {},
        "created_at": "2024-01-01T00:00:00",
        "modified_at": "2024-01-01T00:00:00",
    }

    pipeline = ExtractPipeline(string_metadata)

    # Should be able to access as FileMetadata
    assert pipeline.metadata["connector"] == "HTTPConnector"
    assert isinstance(pipeline.metadata["connector_config"], dict)
    assert isinstance(pipeline.metadata["headers_cache"], dict)
