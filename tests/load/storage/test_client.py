import os
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from lubrikit.base.storage import FileType, Layer
from lubrikit.load.storage.client import LoadStorageClient


@pytest.fixture
def client() -> LoadStorageClient:
    return LoadStorageClient(source_name="test_source")


@pytest.fixture
def client_no_source() -> LoadStorageClient:
    return LoadStorageClient()


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})


def test_init_with_source(client: LoadStorageClient) -> None:
    assert client.source_name == "test_source"


def test_init_without_source(client_no_source: LoadStorageClient) -> None:
    assert client_no_source.source_name is None


@patch.dict(os.environ, {"AWS_LANDING_BUCKET": "my-landing"}, clear=False)
def test_get_folder_env_var(client: LoadStorageClient) -> None:
    expected = os.path.join(client.base_path, "my-landing")
    assert client.get_folder() == expected


def test_get_folder_default(client: LoadStorageClient) -> None:
    with patch.dict(os.environ, {}, clear=True):
        expected = os.path.join(client.base_path, Layer.LANDING.value)
        assert client.get_folder() == expected


def test_get_path_with_source(client: LoadStorageClient) -> None:
    with patch.object(client, "get_folder", return_value="s3://landing"):
        assert client.get_path("data.csv") == "s3://landing/test_source/data.csv"


def test_get_path_without_source(client_no_source: LoadStorageClient) -> None:
    with patch.object(client_no_source, "get_folder", return_value="s3://landing"):
        assert client_no_source.get_path("data.csv") == "s3://landing/data.csv"


def test_read_polars_csv(client: LoadStorageClient, sample_df: pl.DataFrame) -> None:
    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source/f.csv"),
        patch("lubrikit.load.storage.client.pl.read_csv", return_value=sample_df),
    ):
        result = client.read_polars("f.csv", FileType.CSV)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_parquet(
    client: LoadStorageClient, sample_df: pl.DataFrame
) -> None:
    with (
        patch.object(
            client, "get_path", return_value="s3://landing/test_source/f.parquet"
        ),
        patch(
            "lubrikit.load.storage.client.pl.read_parquet", return_value=sample_df
        ),
    ):
        result = client.read_polars("f.parquet", FileType.PARQUET)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_json(
    client: LoadStorageClient, sample_df: pl.DataFrame
) -> None:
    mock_s3 = MagicMock()
    mock_file = MagicMock()
    mock_s3.open.return_value.__enter__ = MagicMock(return_value=mock_file)
    mock_s3.open.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(
            client, "get_path", return_value="s3://landing/test_source/f.json"
        ),
        patch.object(client, "s3", mock_s3),
        patch("lubrikit.load.storage.client.pl.read_json", return_value=sample_df),
    ):
        result = client.read_polars("f.json", FileType.JSON)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_excel(
    client: LoadStorageClient, sample_df: pl.DataFrame
) -> None:
    mock_s3 = MagicMock()
    mock_file = MagicMock()
    mock_s3.open.return_value.__enter__ = MagicMock(return_value=mock_file)
    mock_s3.open.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(
            client, "get_path", return_value="s3://landing/test_source/f.xls"
        ),
        patch.object(client, "s3", mock_s3),
        patch("lubrikit.load.storage.client.pl.read_excel", return_value=sample_df),
    ):
        result = client.read_polars("f.xls", FileType.EXCEL)
        assert isinstance(result, pl.DataFrame)
        assert result.equals(sample_df)


def test_read_polars_unsupported(client: LoadStorageClient) -> None:
    with (
        patch.object(client, "get_path", return_value="s3://landing/test_source/f.zip"),
        pytest.raises(NotImplementedError, match="read_polars not implemented"),
    ):
        client.read_polars("f.zip", FileType.ZIP)
