import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from lubrikit.base.storage import FileType
from lubrikit.extract.storage.client import ExtractStorageClient
from lubrikit.extract.storage.file_metadata import FileMetadata


@pytest.fixture
def sample_metadata() -> FileMetadata:
    return {
        "connector": "HTTPConnector",
        "connector_config": {"url": "https://example.com/data.csv"},
        "headers_cache": None,
        "prefix": None,
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
def client(sample_metadata: FileMetadata) -> ExtractStorageClient:
    return ExtractStorageClient(sample_metadata)


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def test_read_polars_csv(
    client: ExtractStorageClient, sample_df: pl.DataFrame
) -> None:
    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source"),
        patch("lubrikit.extract.storage.client.pl.read_csv", return_value=sample_df),
    ):
        result = client.read_polars("data.csv", FileType.CSV)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_parquet(
    client: ExtractStorageClient, sample_df: pl.DataFrame
) -> None:
    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source"),
        patch(
            "lubrikit.extract.storage.client.pl.read_parquet", return_value=sample_df
        ),
    ):
        result = client.read_polars("data.parquet", FileType.PARQUET)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_json(
    client: ExtractStorageClient, sample_df: pl.DataFrame
) -> None:
    mock_s3 = MagicMock()
    mock_file = MagicMock()
    mock_s3.open.return_value.__enter__ = MagicMock(return_value=mock_file)
    mock_s3.open.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source"),
        patch.object(client, "s3", mock_s3),
        patch(
            "lubrikit.extract.storage.client.pl.read_json", return_value=sample_df
        ),
    ):
        result = client.read_polars("data.json", FileType.JSON)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_excel(
    client: ExtractStorageClient, sample_df: pl.DataFrame
) -> None:
    mock_s3 = MagicMock()
    mock_file = MagicMock()
    mock_s3.open.return_value.__enter__ = MagicMock(return_value=mock_file)
    mock_s3.open.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source"),
        patch.object(client, "s3", mock_s3),
        patch(
            "lubrikit.extract.storage.client.pl.read_excel", return_value=sample_df
        ),
    ):
        result = client.read_polars("data.xls", FileType.EXCEL)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_access(
    client: ExtractStorageClient, sample_df: pl.DataFrame
) -> None:
    mock_s3 = MagicMock()
    mock_file = MagicMock()
    mock_s3.open.return_value.__enter__ = MagicMock(return_value=mock_file)
    mock_s3.open.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source"),
        patch.object(client, "s3", mock_s3),
        patch(
            "lubrikit.extract.storage.client.pl.read_excel", return_value=sample_df
        ),
    ):
        result = client.read_polars("data.mdb", FileType.ACCESS)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_unsupported_type(client: ExtractStorageClient) -> None:
    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source"),
        pytest.raises(NotImplementedError, match="read_polars not implemented"),
    ):
        client.read_polars("data.zip", FileType.ZIP)


def test_read_polars_builds_correct_path(
    client: ExtractStorageClient, sample_df: pl.DataFrame
) -> None:
    with (
        patch.object(
            client, "get_path", return_value="s3://landing/test_source"
        ) as mock_path,
        patch("lubrikit.extract.storage.client.pl.read_csv", return_value=sample_df),
    ):
        client.read_polars("report.csv", FileType.CSV)
        mock_path.assert_called_once_with(client.metadata)


@patch.dict(os.environ, {"AWS_LANDING_BUCKET": "my-landing"}, clear=False)
def test_read_polars_uses_env_bucket(
    client: ExtractStorageClient, sample_df: pl.DataFrame
) -> None:
    with patch(
        "lubrikit.extract.storage.client.pl.read_csv", return_value=sample_df
    ) as mock_read:
        with patch("lubrikit.base.storage.client.s3fs.S3FileSystem"):
            client.read_polars("data.csv", FileType.CSV)
            called_path = mock_read.call_args[0][0]
            assert "my-landing" in called_path
