import os
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from googleapiclient.http import MediaIoBaseDownload
from requests import Response

from lubrikit.base.storage import FileMode, Layer
from lubrikit.extract.storage.client import ExtractStorageClient
from lubrikit.extract.storage.file_metadata import FileMetadata


@pytest.fixture
def sample_metadata() -> FileMetadata:
    """Sample FileMetadata for testing."""
    return {
        "connector": "HTTPConnector",
        "connector_config": {"url": "https://example.com/data.json"},
        "headers_cache": None,
        "prefix": None,
        "retry_config": None,
        "source_name": None,
        "created_at": datetime.now(),
        "modified_at": datetime.now(),
        "deleted_at": None,
        "checked_at": None,
        "landed_at": None,
        "staged_at": None,
        "processed_at": None,
    }


@pytest.fixture
def metadata_with_source_and_prefix() -> FileMetadata:
    """FileMetadata with source_name and prefix for testing path construction."""
    return {
        "connector": "GoogleDriveAPIConnector",
        "connector_config": {"service_account_file": "/path/to/service.json"},
        "headers_cache": {"etag": "abc123"},
        "prefix": "data/files",
        "retry_config": None,
        "source_name": "test_source",
        "created_at": datetime.now(),
        "modified_at": datetime.now(),
        "deleted_at": None,
        "checked_at": None,
        "landed_at": None,
        "staged_at": None,
        "processed_at": None,
    }


@pytest.fixture
def mock_s3_filesystem():  # type: ignore[no-untyped-def]
    """Mock S3FileSystem for testing."""
    with patch("lubrikit.base.storage.client.s3fs.S3FileSystem") as mock_s3fs:
        mock_s3 = MagicMock()
        mock_s3.exists.return_value = False
        mock_s3fs.return_value = mock_s3
        yield mock_s3


def test_init(sample_metadata: FileMetadata) -> None:
    """Test ExtractStorageClient initialization."""
    client = ExtractStorageClient(sample_metadata)
    assert client.metadata == sample_metadata
    assert isinstance(client.metadata, dict)


def test_init_with_different_metadata(
    metadata_with_source_and_prefix: FileMetadata,
) -> None:
    """Test initialization with different metadata configurations."""
    client = ExtractStorageClient(metadata_with_source_and_prefix)
    assert client.metadata == metadata_with_source_and_prefix
    assert client.metadata["source_name"] == "test_source"
    assert client.metadata["prefix"] == "data/files"


@patch.dict(os.environ, {"AWS_LANDING_BUCKET": "test-landing-bucket"}, clear=False)
def test_get_folder_with_env_var(sample_metadata: FileMetadata) -> None:
    """Test get_folder when AWS_LANDING_BUCKET environment variable is set."""
    client = ExtractStorageClient(sample_metadata)
    expected_folder = os.path.join(client.base_path, "test-landing-bucket")
    assert client.get_folder() == expected_folder


def test_get_folder_default(sample_metadata: FileMetadata) -> None:
    """Test get_folder with default Layer.LANDING.bucket value."""
    with patch.dict(os.environ, {}, clear=True):
        client = ExtractStorageClient(sample_metadata)
        expected_folder = os.path.join(client.base_path, Layer.LANDING.value)
        assert client.get_folder() == expected_folder


def test_get_path_no_source_no_prefix(sample_metadata: FileMetadata) -> None:
    """Test get_path with metadata containing no source_name or prefix."""
    client = ExtractStorageClient(sample_metadata)

    with patch.object(client, "get_folder", return_value="s3://landing"):
        path = client.get_path(sample_metadata)
        assert path == "s3://landing"


def test_get_path_with_source_only(sample_metadata: FileMetadata) -> None:
    """Test get_path with metadata containing only source_name."""
    sample_metadata["source_name"] = "api_source"
    client = ExtractStorageClient(sample_metadata)

    with patch.object(client, "get_folder", return_value="s3://landing"):
        path = client.get_path(sample_metadata)
        assert path == "s3://landing/api_source"


def test_get_path_with_prefix_only(sample_metadata: FileMetadata) -> None:
    """Test get_path with metadata containing only prefix."""
    sample_metadata["prefix"] = "data/exports"
    client = ExtractStorageClient(sample_metadata)

    with patch.object(client, "get_folder", return_value="s3://landing"):
        path = client.get_path(sample_metadata)
        assert path == "s3://landing/data/exports"


def test_get_path_with_source_and_prefix(
    metadata_with_source_and_prefix: FileMetadata,
) -> None:
    """Test get_path with metadata containing both source_name and prefix."""
    client = ExtractStorageClient(metadata_with_source_and_prefix)

    with patch.object(client, "get_folder", return_value="s3://landing"):
        path = client.get_path(metadata_with_source_and_prefix)
        assert path == "s3://landing/test_source/data/files"


def test_get_path_with_different_metadata(sample_metadata: FileMetadata) -> None:
    """Test get_path can work with different metadata than the instance metadata."""
    client = ExtractStorageClient(sample_metadata)

    different_metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {},
        "source_name": "different_source",
        "prefix": "different/prefix",
        "created_at": datetime.now(),
        "modified_at": datetime.now(),
    }

    with patch.object(client, "get_folder", return_value="s3://landing"):
        path = client.get_path(different_metadata)
        assert path == "s3://landing/different_source/different/prefix"


def test_write_unsupported_type(sample_metadata: FileMetadata) -> None:
    """Test write method with unsupported data type."""
    client = ExtractStorageClient(sample_metadata)

    with pytest.raises(NotImplementedError, match="Write not implemented for type"):
        client.write("unsupported string data")


def test_write_unsupported_object(sample_metadata: FileMetadata) -> None:
    """Test write method with unsupported object type."""
    client = ExtractStorageClient(sample_metadata)

    with pytest.raises(NotImplementedError, match="Write not implemented for type"):
        client.write({"dict": "data"})


@patch("builtins.open")
@patch("lubrikit.extract.storage.client.logger")
def test_write_response_success(
    mock_logger: Mock,
    mock_open: Mock,
    sample_metadata: FileMetadata,
    mock_s3_filesystem: Mock,
) -> None:
    """Test write method with Response object - successful case."""
    # Create a mock Response object
    response = Mock(spec=Response)
    response.headers = {"Content-Length": "1024"}
    response.raise_for_status.return_value = None
    response.iter_content.return_value = [b"chunk1", b"chunk2", b"chunk3"]

    # Create client and mock its methods
    client = ExtractStorageClient(sample_metadata)

    with (
        patch.object(client, "get_path", return_value="s3://landing/test.json"),
        patch.object(client, "get_folder", return_value="s3://landing"),
        patch.object(client, "_make_dirs") as mock_make_dirs,
    ):
        # Mock file object
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Execute write
        client.write(response)

        # Verify Response methods were called
        response.raise_for_status.assert_called_once()
        response.iter_content.assert_called_once_with(
            chunk_size=ExtractStorageClient.chunk_size
        )

        # Verify file operations
        mock_make_dirs.assert_called_once_with(path="s3://landing")
        mock_open.assert_called_once_with(
            "s3://landing/test.json", FileMode.WRITING_BINARY
        )

        # Verify chunks were written
        assert mock_file.write.call_count == 3
        mock_file.write.assert_any_call(b"chunk1")
        mock_file.write.assert_any_call(b"chunk2")
        mock_file.write.assert_any_call(b"chunk3")

        # Verify logging
        mock_logger.info.assert_called_once_with(
            "Writing 1024 bytes to s3://landing/test.json"
        )


def test_write_response_http_error(sample_metadata: FileMetadata) -> None:
    """Test write method with Response object that has HTTP error."""
    # Create a mock Response object that raises an HTTP error
    response = Mock(spec=Response)
    response.raise_for_status.side_effect = Exception("HTTP 404 Not Found")

    client = ExtractStorageClient(sample_metadata)

    with pytest.raises(Exception, match="HTTP 404 Not Found"):
        client.write(response)

    # Verify raise_for_status was called
    response.raise_for_status.assert_called_once()


@patch("builtins.open")
@patch("lubrikit.extract.storage.client.logger")
def test_write_response_with_metadata_paths(
    mock_logger: Mock,
    mock_open: Mock,
    metadata_with_source_and_prefix: FileMetadata,
    mock_s3_filesystem: Mock,
) -> None:
    """Test write method uses correct paths from metadata."""
    response = Mock(spec=Response)
    response.headers = {"Content-Length": "2048"}
    response.raise_for_status.return_value = None
    response.iter_content.return_value = [b"data_chunk"]

    client = ExtractStorageClient(metadata_with_source_and_prefix)

    # Mock the actual path construction
    expected_folder = os.path.join(client.base_path, Layer.LANDING.bucket)
    expected_path = f"{expected_folder}/test_source/data/files"

    with (
        patch.object(client, "get_path", return_value=expected_path),
        patch.object(client, "get_folder", return_value=expected_folder),
        patch.object(client, "_make_dirs") as mock_make_dirs,
    ):
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        client.write(response)

        mock_make_dirs.assert_called_once_with(path=expected_folder)
        mock_open.assert_called_once_with(expected_path, FileMode.WRITING_BINARY)
        mock_logger.info.assert_called_once_with(
            f"Writing 2048 bytes to {expected_path}"
        )


def test_write_response_chunk_size(sample_metadata: FileMetadata) -> None:
    """Test write method uses correct chunk size."""
    response = Mock(spec=Response)
    response.headers = {"Content-Length": "1024"}
    response.raise_for_status.return_value = None
    response.iter_content.return_value = []

    client = ExtractStorageClient(sample_metadata)

    with (
        patch.object(client, "get_path", return_value="test_path"),
        patch.object(client, "get_folder", return_value="test_folder"),
        patch.object(client, "_make_dirs"),
        patch("builtins.open"),
    ):
        client.write(response)

        response.iter_content.assert_called_once_with(
            chunk_size=ExtractStorageClient.chunk_size
        )


@pytest.mark.parametrize(
    "attr_name", ["base_path", "s3", "_make_dirs", "chunk_size", "encoding"]
)
def test_inheritance_from_storage_client(
    sample_metadata: FileMetadata, attr_name: str
) -> None:
    """Test ExtractStorageClient properly inherits from StorageClient."""
    from lubrikit.base.storage import StorageClient

    client = ExtractStorageClient(sample_metadata)
    assert isinstance(client, StorageClient)
    assert hasattr(client, attr_name)


@pytest.mark.parametrize(
    "attr_name, expected",
    [
        ("chunk_size", 1024),
        ("encoding", "utf-8"),
        ("base_path", "s3://"),
    ],
)
def test_class_attributes(
    sample_metadata: FileMetadata, attr_name: str, expected: int | str
) -> None:
    """Test class-level attributes are inherited correctly."""
    client = ExtractStorageClient(sample_metadata)
    assert getattr(client, attr_name) == expected


@patch("builtins.open")
def test_write_response_empty_content(
    mock_open: Mock,
    sample_metadata: FileMetadata,
    mock_s3_filesystem: Mock,
) -> None:
    """Test write method with Response object containing no content."""
    response = Mock(spec=Response)
    response.headers = {"Content-Length": "0"}
    response.raise_for_status.return_value = None
    response.iter_content.return_value = []  # No chunks

    client = ExtractStorageClient(sample_metadata)

    with (
        patch.object(client, "get_path", return_value="test_path"),
        patch.object(client, "get_folder", return_value="test_folder"),
        patch.object(client, "_make_dirs"),
    ):
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        client.write(response)

        # Verify file was opened but no writes occurred
        mock_open.assert_called_once()
        mock_file.write.assert_not_called()


def test_singledispatchmethod_registration(sample_metadata: FileMetadata) -> None:
    """Test that the write method handles different types via singledispatchmethod."""
    client = ExtractStorageClient(sample_metadata)

    # Test that unsupported types raise NotImplementedError
    with pytest.raises(NotImplementedError):
        client.write("string data")

    with pytest.raises(NotImplementedError):
        client.write(123)

    # Test that Response type is supported (would work if properly mocked)
    from requests import Response

    response = Mock(spec=Response)
    response.headers = {"Content-Length": "0"}
    response.raise_for_status.return_value = None
    response.iter_content.return_value = []

    with (
        patch.object(client, "get_path", return_value="test_path"),
        patch.object(client, "get_folder", return_value="test_folder"),
        patch.object(client, "_make_dirs"),
        patch("builtins.open"),
    ):
        # This should not raise NotImplementedError
        client.write(response)


@patch("builtins.open")
def test_write_media_io_base_download_success(
    mock_open: Mock,
    sample_metadata: FileMetadata,
    mock_s3_filesystem: Mock,
) -> None:
    """Test write method with MediaIoBaseDownload object - successful case."""
    # Create mock file data
    file_content = b"Google Drive file content data"
    mock_file_handle = Mock()
    mock_file_handle.read.return_value = file_content
    mock_file_handle.seek.return_value = None

    # Create a mock MediaIoBaseDownload object
    downloader = Mock(spec=MediaIoBaseDownload)
    downloader._fd = mock_file_handle

    # Mock the download process
    downloader.next_chunk.side_effect = [
        (None, False),  # First chunk, not done
        (None, False),  # Second chunk, not done
        (None, True),  # Final chunk, done
    ]

    client = ExtractStorageClient(sample_metadata)

    with (
        patch.object(client, "get_path", return_value="s3://landing/gdrive_file.xlsx"),
        patch.object(client, "get_folder", return_value="s3://landing"),
        patch.object(client, "_make_dirs") as mock_make_dirs,
    ):
        # Mock file object
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Execute write
        client.write(downloader)

        # Verify download process
        assert downloader.next_chunk.call_count == 3

        # Verify file handle operations
        mock_file_handle.seek.assert_called_once_with(0)  # Reset to beginning
        mock_file_handle.read.assert_called_once()

        # Verify file operations
        mock_make_dirs.assert_called_once_with(path="s3://landing")
        mock_open.assert_called_once_with(
            "s3://landing/gdrive_file.xlsx", FileMode.WRITING_BINARY
        )

        # Verify file content was written
        mock_file.write.assert_called_once_with(file_content)


@patch("builtins.open")
def test_write_media_io_base_download_single_chunk(
    mock_open: Mock,
    sample_metadata: FileMetadata,
    mock_s3_filesystem: Mock,
) -> None:
    """Test write method with MediaIoBaseDownload that completes in one chunk."""
    file_content = b"Small file"
    mock_file_handle = Mock()
    mock_file_handle.read.return_value = file_content
    mock_file_handle.seek.return_value = None

    downloader = Mock(spec=MediaIoBaseDownload)
    downloader._fd = mock_file_handle
    downloader.next_chunk.return_value = (None, True)  # Done in one chunk

    client = ExtractStorageClient(sample_metadata)

    with (
        patch.object(client, "get_path", return_value="s3://landing/small_file.txt"),
        patch.object(client, "get_folder", return_value="s3://landing"),
        patch.object(client, "_make_dirs"),
    ):
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        client.write(downloader)

        # Should only call next_chunk once
        downloader.next_chunk.assert_called_once()

        # File should still be written
        mock_file.write.assert_called_once_with(file_content)


@patch("builtins.open")
def test_write_media_io_base_download_empty_file(
    mock_open: Mock,
    sample_metadata: FileMetadata,
    mock_s3_filesystem: Mock,
) -> None:
    """Test write method with MediaIoBaseDownload containing empty file."""
    mock_file_handle = Mock()
    mock_file_handle.read.return_value = b""  # Empty file
    mock_file_handle.seek.return_value = None

    downloader = Mock(spec=MediaIoBaseDownload)
    downloader._fd = mock_file_handle
    downloader.next_chunk.return_value = (None, True)

    client = ExtractStorageClient(sample_metadata)

    with (
        patch.object(client, "get_path", return_value="s3://landing/empty_file.txt"),
        patch.object(client, "get_folder", return_value="s3://landing"),
        patch.object(client, "_make_dirs"),
    ):
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        client.write(downloader)

        # Should still write (empty content)
        mock_file.write.assert_called_once_with(b"")


@patch("builtins.open")
def test_write_media_io_base_download_with_metadata_paths(
    mock_open: Mock,
    metadata_with_source_and_prefix: FileMetadata,
    mock_s3_filesystem: Mock,
) -> None:
    """Test write method uses correct paths from metadata with MediaIoBaseDownload."""
    file_content = b"Google Sheets data"
    mock_file_handle = Mock()
    mock_file_handle.read.return_value = file_content
    mock_file_handle.seek.return_value = None

    downloader = Mock(spec=MediaIoBaseDownload)
    downloader._fd = mock_file_handle
    downloader.next_chunk.return_value = (None, True)

    client = ExtractStorageClient(metadata_with_source_and_prefix)

    # Mock the actual path construction
    expected_folder = os.path.join(client.base_path, Layer.LANDING.bucket)
    expected_path = f"{expected_folder}/test_source/data/files"

    with (
        patch.object(client, "get_path", return_value=expected_path),
        patch.object(client, "get_folder", return_value=expected_folder),
        patch.object(client, "_make_dirs") as mock_make_dirs,
    ):
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        client.write(downloader)

        mock_make_dirs.assert_called_once_with(path=expected_folder)
        mock_open.assert_called_once_with(expected_path, FileMode.WRITING_BINARY)


def test_write_media_io_base_download_file_handle_operations(
    sample_metadata: FileMetadata,
) -> None:
    """Test that MediaIoBaseDownload file handle is properly manipulated."""
    file_content = b"Test file content for handle operations"
    mock_file_handle = Mock()
    mock_file_handle.read.return_value = file_content
    mock_file_handle.seek.return_value = None

    downloader = Mock(spec=MediaIoBaseDownload)
    downloader._fd = mock_file_handle
    downloader.next_chunk.return_value = (None, True)

    client = ExtractStorageClient(sample_metadata)

    with (
        patch.object(client, "get_path", return_value="test_path"),
        patch.object(client, "get_folder", return_value="test_folder"),
        patch.object(client, "_make_dirs"),
        patch("builtins.open") as mock_open,
    ):
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        client.write(downloader)

        # Verify file handle was reset to beginning
        mock_file_handle.seek.assert_called_once_with(0)

        # Verify full content was read and written
        mock_file_handle.read.assert_called_once()
        mock_file.write.assert_called_once_with(file_content)


def test_singledispatchmethod_supports_both_types(
    sample_metadata: FileMetadata,
) -> None:
    """Test that write method supports both Response and MediaIoBaseDownload."""
    client = ExtractStorageClient(sample_metadata)

    # Test with Response
    response = Mock(spec=Response)
    response.headers = {"Content-Length": "0"}
    response.raise_for_status.return_value = None
    response.iter_content.return_value = []

    with (
        patch.object(client, "get_path", return_value="test_path"),
        patch.object(client, "get_folder", return_value="test_folder"),
        patch.object(client, "_make_dirs"),
        patch("builtins.open"),
    ):
        # Should not raise NotImplementedError
        client.write(response)

    # Test with MediaIoBaseDownload
    mock_file_handle = Mock()
    mock_file_handle.read.return_value = b"test"
    mock_file_handle.seek.return_value = None
    downloader = Mock(spec=MediaIoBaseDownload)
    downloader._fd = mock_file_handle
    downloader.next_chunk.return_value = (None, True)

    with (
        patch.object(client, "get_path", return_value="test_path"),
        patch.object(client, "get_folder", return_value="test_folder"),
        patch.object(client, "_make_dirs"),
        patch("builtins.open"),
    ):
        # Should not raise NotImplementedError
        client.write(downloader)
