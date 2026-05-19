import os
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from lubrikit.base.storage import FileType, Layer
from lubrikit.load.storage.client import LoadStorageClient


@pytest.fixture
def client() -> LoadStorageClient:
    return LoadStorageClient(source_name="test_source")


@pytest.fixture
def client_no_source() -> LoadStorageClient:
    return LoadStorageClient()


def test_init(client: LoadStorageClient) -> None:
    assert client.source_name == "test_source"


def test_init_no_source(client_no_source: LoadStorageClient) -> None:
    assert client_no_source.source_name is None


@patch.dict(os.environ, {"AWS_LANDING_BUCKET": "test-landing"}, clear=False)
def test_get_folder_with_env_var(client: LoadStorageClient) -> None:
    expected = os.path.join(client.base_path, "test-landing")
    assert client.get_folder() == expected


def test_get_folder_default(client: LoadStorageClient) -> None:
    with patch.dict(os.environ, {}, clear=True):
        expected = os.path.join(client.base_path, Layer.LANDING.value)
        assert client.get_folder() == expected


def test_get_path_with_source(client: LoadStorageClient) -> None:
    with patch.object(client, "get_folder", return_value="s3://landing"):
        path = client.get_path("data.csv")
        assert path == "s3://landing/test_source/data.csv"


def test_get_path_no_source(client_no_source: LoadStorageClient) -> None:
    with patch.object(client_no_source, "get_folder", return_value="s3://landing"):
        path = client_no_source.get_path("data.csv")
        assert path == "s3://landing/data.csv"


@patch("lubrikit.load.storage.client.pd.read_csv")
def test_read_csv(mock_read_csv: Mock, client: LoadStorageClient) -> None:
    expected_df = pd.DataFrame({"a": [1, 2]})
    mock_read_csv.return_value = expected_df

    with patch.object(
        client, "get_path", return_value="s3://landing/test_source/data.csv"
    ):
        result = client.read("data.csv", FileType.CSV)

    mock_read_csv.assert_called_once_with(
        "s3://landing/test_source/data.csv", storage_options={"anon": False}
    )
    assert result is expected_df


@patch("lubrikit.load.storage.client.pd.read_parquet")
def test_read_parquet(mock_read_parquet: Mock, client: LoadStorageClient) -> None:
    expected_df = pd.DataFrame({"b": [3, 4]})
    mock_read_parquet.return_value = expected_df

    with patch.object(
        client, "get_path", return_value="s3://landing/test_source/data.parquet"
    ):
        result = client.read("data.parquet", FileType.PARQUET)

    mock_read_parquet.assert_called_once_with(
        "s3://landing/test_source/data.parquet", storage_options={"anon": False}
    )
    assert result is expected_df


@patch("lubrikit.load.storage.client.pd.read_json")
def test_read_json(mock_read_json: Mock, client: LoadStorageClient) -> None:
    expected_df = pd.DataFrame({"c": [5, 6]})
    mock_read_json.return_value = expected_df

    with patch.object(
        client, "get_path", return_value="s3://landing/test_source/data.json"
    ):
        result = client.read("data.json", FileType.JSON)

    mock_read_json.assert_called_once_with(
        "s3://landing/test_source/data.json", storage_options={"anon": False}
    )
    assert result is expected_df


@patch("lubrikit.load.storage.client.pd.read_excel")
def test_read_excel(mock_read_excel: Mock, client: LoadStorageClient) -> None:
    expected_df = pd.DataFrame({"d": [7, 8]})
    mock_read_excel.return_value = expected_df

    mock_s3 = MagicMock()
    mock_file = MagicMock()
    mock_s3.open.return_value.__enter__.return_value = mock_file

    with (
        patch.object(
            client, "get_path", return_value="s3://landing/test_source/data.xls"
        ),
        patch.object(client, "s3", mock_s3),
    ):
        result = client.read("data.xls", FileType.EXCEL)

    mock_read_excel.assert_called_once_with(mock_file)
    assert result is expected_df


@patch("lubrikit.load.storage.client.pd.read_excel")
def test_read_access(mock_read_excel: Mock, client: LoadStorageClient) -> None:
    expected_df = pd.DataFrame({"e": [9, 10]})
    mock_read_excel.return_value = expected_df

    mock_s3 = MagicMock()
    mock_file = MagicMock()
    mock_s3.open.return_value.__enter__.return_value = mock_file

    with (
        patch.object(
            client, "get_path", return_value="s3://landing/test_source/data.mdb"
        ),
        patch.object(client, "s3", mock_s3),
    ):
        result = client.read("data.mdb", FileType.ACCESS)

    mock_read_excel.assert_called_once_with(mock_file)
    assert result is expected_df


def test_read_unsupported_type(client: LoadStorageClient) -> None:
    with pytest.raises(NotImplementedError, match="Read not implemented for file type"):
        client.read("data.zip", FileType.ZIP)


def test_write_not_implemented(client: LoadStorageClient) -> None:
    with pytest.raises(NotImplementedError):
        client.write("unsupported")


def test_inheritance_from_storage_client(client: LoadStorageClient) -> None:
    from lubrikit.base.storage import StorageClient

    assert isinstance(client, StorageClient)
