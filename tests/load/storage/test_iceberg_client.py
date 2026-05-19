import os
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import polars as pl
import pytest

from lubrikit.base.storage import Layer, StorageClient
from lubrikit.load.storage.iceberg_client import IcebergClient


@pytest.fixture
def client() -> IcebergClient:
    return IcebergClient(namespace="test_ns", table_name="test_table")


def test_init(client: IcebergClient) -> None:
    assert client.namespace == "test_ns"
    assert client.table_name == "test_table"
    assert client.catalog_name == "default"


def test_init_custom_catalog() -> None:
    c = IcebergClient(namespace="ns", table_name="tbl", catalog_name="my_catalog")
    assert c.catalog_name == "my_catalog"


@patch.dict(os.environ, {"AWS_BRONZE_BUCKET": "test-bronze"}, clear=False)
def test_get_folder_with_env_var(client: IcebergClient) -> None:
    expected = os.path.join(client.base_path, "test-bronze")
    assert client.get_folder() == expected


def test_get_folder_default(client: IcebergClient) -> None:
    with patch.dict(os.environ, {}, clear=True):
        expected = os.path.join(client.base_path, Layer.BRONZE.value)
        assert client.get_folder() == expected


def test_get_path(client: IcebergClient) -> None:
    with patch.object(client, "get_folder", return_value="s3://bronze"):
        assert client.get_path() == "s3://bronze/test_ns/test_table"


def test_inheritance_from_storage_client(client: IcebergClient) -> None:
    assert isinstance(client, StorageClient)


def test_write_unsupported_type(client: IcebergClient) -> None:
    with pytest.raises(NotImplementedError, match="Write not implemented for type"):
        client.write("unsupported")


def test_catalog_uses_glue_type(client: IcebergClient) -> None:
    with patch("lubrikit.load.storage.iceberg_client.load_catalog") as mock_load:
        mock_load.return_value = MagicMock()
        _ = client.catalog
        _, kwargs = mock_load.call_args
        assert kwargs["type"] == "glue"


# --- pandas write ---


@patch("lubrikit.load.storage.iceberg_client.load_catalog")
def test_write_pandas_existing_table(
    mock_load_catalog: Mock, client: IcebergClient
) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    client.write(df)

    mock_catalog.load_table.assert_called_once_with("test_ns.test_table")
    mock_table.append.assert_called_once()
    mock_catalog.create_table.assert_not_called()


@patch("lubrikit.load.storage.iceberg_client.load_catalog")
def test_write_pandas_creates_table_when_missing(
    mock_load_catalog: Mock, client: IcebergClient
) -> None:
    from pyiceberg.exceptions import NoSuchTableError

    df = pd.DataFrame({"a": [1, 2]})
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.side_effect = [NoSuchTableError(), mock_table]
    mock_load_catalog.return_value = mock_catalog

    with patch.object(
        client, "get_path", return_value="s3://bronze/test_ns/test_table"
    ):
        client.write(df)

    mock_catalog.create_table.assert_called_once()
    create_kwargs = mock_catalog.create_table.call_args.kwargs
    assert create_kwargs["identifier"] == "test_ns.test_table"
    assert create_kwargs["location"] == "s3://bronze/test_ns/test_table"
    mock_table.append.assert_called_once()


@patch("lubrikit.load.storage.iceberg_client.load_catalog")
def test_write_pandas_logs(mock_load_catalog: Mock, client: IcebergClient) -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    mock_load_catalog.return_value = MagicMock()

    with patch("lubrikit.load.storage.iceberg_client.logger") as mock_logger:
        client.write(df)
        mock_logger.info.assert_called_once_with(
            "Writing 3 rows to Iceberg table test_ns.test_table"
        )


# --- polars write ---


@patch("lubrikit.load.storage.iceberg_client.load_catalog")
def test_write_polars_existing_table(
    mock_load_catalog: Mock, client: IcebergClient
) -> None:
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    client.write(df)

    mock_catalog.load_table.assert_called_once_with("test_ns.test_table")
    mock_table.append.assert_called_once()
    mock_catalog.create_table.assert_not_called()


@patch("lubrikit.load.storage.iceberg_client.load_catalog")
def test_write_polars_creates_table_when_missing(
    mock_load_catalog: Mock, client: IcebergClient
) -> None:
    from pyiceberg.exceptions import NoSuchTableError

    df = pl.DataFrame({"a": [1, 2]})
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.side_effect = [NoSuchTableError(), mock_table]
    mock_load_catalog.return_value = mock_catalog

    with patch.object(
        client, "get_path", return_value="s3://bronze/test_ns/test_table"
    ):
        client.write(df)

    mock_catalog.create_table.assert_called_once()
    create_kwargs = mock_catalog.create_table.call_args.kwargs
    assert create_kwargs["identifier"] == "test_ns.test_table"
    assert create_kwargs["location"] == "s3://bronze/test_ns/test_table"
    mock_table.append.assert_called_once()


@patch("lubrikit.load.storage.iceberg_client.load_catalog")
def test_write_polars_logs(mock_load_catalog: Mock, client: IcebergClient) -> None:
    df = pl.DataFrame({"a": [1, 2, 3]})
    mock_load_catalog.return_value = MagicMock()

    with patch("lubrikit.load.storage.iceberg_client.logger") as mock_logger:
        client.write(df)
        mock_logger.info.assert_called_once_with(
            "Writing 3 rows to Iceberg table test_ns.test_table"
        )


@patch("lubrikit.load.storage.iceberg_client.load_catalog")
def test_write_polars_arrow_row_count(
    mock_load_catalog: Mock, client: IcebergClient
) -> None:
    df = pl.DataFrame({"x": [10, 20, 30, 40]})
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    client.write(df)

    arrow_arg = mock_table.append.call_args[0][0]
    assert arrow_arg.num_rows == 4
