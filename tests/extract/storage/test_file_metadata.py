from datetime import datetime
from typing import Any

import pytest

from lubrikit.extract.storage.file_metadata import FileMetadata


def test_file_metadata_creation_with_required_fields() -> None:
    """Test creating FileMetadata with only required fields."""
    now = datetime.now()
    metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {"url": "https://example.com"},
        "headers_cache": None,
        "prefix": None,
        "retry_config": None,
        "source_name": None,
        "created_at": now,
        "modified_at": now,
        "deleted_at": None,
        "checked_at": None,
        "landed_at": None,
        "staged_at": None,
        "processed_at": None,
    }

    assert metadata["connector"] == "HTTPConnector"
    assert metadata["connector_config"] == {"url": "https://example.com"}
    assert metadata["created_at"] == now
    assert metadata["modified_at"] == now


def test_file_metadata_creation_with_all_fields() -> None:
    """Test creating FileMetadata with all fields populated."""
    now = datetime.now()
    metadata: FileMetadata = {
        "connector": "GoogleDriveAPIConnector",
        "connector_config": {"client_id": "test", "client_secret": "secret"},
        "headers_cache": {"etag": "abc123", "last-modified": "Mon, 01 Jan 2024"},
        "prefix": "data/files",
        "retry_config": {"max_retries": 3, "backoff_factor": 1.5},
        "source_name": "Test Source",
        "created_at": now,
        "modified_at": now,
        "deleted_at": now,
        "checked_at": now,
        "landed_at": now,
        "staged_at": now,
        "processed_at": now,
    }

    assert metadata["connector"] == "GoogleDriveAPIConnector"
    assert metadata["source_name"] == "Test Source"
    assert metadata["prefix"] == "data/files"
    assert metadata["headers_cache"] == {
        "etag": "abc123",
        "last-modified": "Mon, 01 Jan 2024",
    }
    assert metadata["retry_config"] == {"max_retries": 3, "backoff_factor": 1.5}


def test_file_metadata_partial_creation() -> None:
    """Test creating FileMetadata with partial field assignment."""
    now = datetime.now()
    metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {},
        "created_at": now,
        "modified_at": now,
    }

    # Should work even with missing optional fields due to total=False
    assert metadata["connector"] == "HTTPConnector"
    assert metadata["connector_config"] == {}

    # These fields should not be present since they weren't set
    assert "headers_cache" not in metadata
    assert "prefix" not in metadata
    assert "source_name" not in metadata


def test_file_metadata_connector_types() -> None:
    """Test various connector types."""
    now = datetime.now()

    http_metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {"url": "https://api.example.com/data"},
        "created_at": now,
        "modified_at": now,
    }

    gdrive_metadata: FileMetadata = {
        "connector": "GoogleDriveAPIConnector",
        "connector_config": {"service_account_file": "/path/to/service.json"},
        "created_at": now,
        "modified_at": now,
    }

    assert http_metadata["connector"] == "HTTPConnector"
    assert gdrive_metadata["connector"] == "GoogleDriveAPIConnector"


@pytest.mark.parametrize(
    "field_name, value",
    [
        ("created_at", datetime(2024, 1, 1, 12, 0, 0)),
        ("modified_at", datetime(2024, 1, 2, 12, 0, 0)),
        ("deleted_at", datetime(2024, 1, 3, 12, 0, 0)),
        ("checked_at", datetime(2024, 1, 4, 12, 0, 0)),
        ("landed_at", datetime(2024, 1, 5, 12, 0, 0)),
        ("staged_at", datetime(2024, 1, 6, 12, 0, 0)),
        ("processed_at", datetime(2024, 1, 7, 12, 0, 0)),
    ],
)
def test_file_metadata_datetime_fields(field_name: str, value: datetime) -> None:
    """Test all datetime fields are properly handled."""

    metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {},
        "created_at": value,
        "modified_at": value,
        "deleted_at": value,
        "checked_at": value,
        "landed_at": value,
        "staged_at": value,
        "processed_at": value,
    }

    assert metadata.get(field_name) == value


@pytest.mark.parametrize(
    "field_name",
    [
        "headers_cache",
        "prefix",
        "retry_config",
        "source_name",
        "deleted_at",
    ],
)
def test_file_metadata_none_values(field_name: str) -> None:
    """Test FileMetadata with None values for optional fields."""
    now = datetime.now()
    metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {},
        "headers_cache": None,
        "prefix": None,
        "retry_config": None,
        "source_name": None,
        "created_at": now,
        "modified_at": now,
        "deleted_at": None,
        "checked_at": None,
        "landed_at": None,
        "staged_at": None,
        "processed_at": None,
    }

    assert metadata.get(field_name) is None


def test_file_metadata_connector_config_variations() -> None:
    """Test various connector_config structures."""
    now = datetime.now()

    # HTTP connector config
    http_config = {
        "url": "https://api.example.com",
        "headers": {"Authorization": "Bearer token"},
        "timeout": 30,
    }

    # Google Drive API connector config
    gdrive_config = {
        "service_account_file": "/path/to/service.json",
        "folder_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
        "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
    }

    http_metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": http_config,
        "created_at": now,
        "modified_at": now,
    }

    gdrive_metadata: FileMetadata = {
        "connector": "GoogleDriveAPIConnector",
        "connector_config": gdrive_config,
        "created_at": now,
        "modified_at": now,
    }

    assert http_metadata["connector_config"] == http_config
    assert gdrive_metadata["connector_config"] == gdrive_config


@pytest.mark.parametrize(
    "field_name, value",
    [
        ("connector", "HTTPConnector"),
        ("source_name", "Test Source"),
        ("prefix", None),
        ("nonexistent_field", None),
    ],
)
def test_file_metadata_field_access(field_name: str, value: Any) -> None:
    """Test accessing fields from FileMetadata instance."""
    now = datetime.now()
    metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {"url": "https://example.com"},
        "source_name": "Test Source",
        "created_at": now,
        "modified_at": now,
    }

    # Test field access
    assert metadata.get(field_name) == value


@pytest.mark.parametrize(
    "field_name, value",
    [
        ("modified_at", datetime(2024, 1, 2, 12, 0, 0)),
        ("source_name", "Updated Source"),
        ("checked_at", datetime(2024, 1, 2, 12, 0, 0)),
    ],
)
def test_file_metadata_update(field_name: str, value: Any) -> None:
    """Test updating FileMetadata fields."""
    now = datetime.now()
    metadata: FileMetadata = {
        "connector": "HTTPConnector",
        "connector_config": {"url": "https://example.com"},
        "created_at": now,
        "modified_at": now,
    }

    # Update fields
    metadata[field_name] = value  # type: ignore[literal-required]

    assert metadata[field_name] == value  # type: ignore[literal-required]
