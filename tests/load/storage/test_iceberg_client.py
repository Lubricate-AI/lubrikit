import os
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from lubrikit.base.storage import Layer
from lubrikit.load.storage.iceberg_client import IcebergClient


@pytest.fixture
def client() -> IcebergClient:
    return IcebergClient(namespace="raw", table_name="events")


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def test_init(client: IcebergClient) -> None:
    assert client.namespace == "raw"
    assert client.table_name == "events"
    assert client.catalog_name == "default"


def test_init_custom_catalog() -> None:
    c = IcebergClient(namespace="ns", table_name="tbl", catalog_name="my_catalog")
    assert c.catalog_name == "my_catalog"


@patch.dict(os.environ, {"AWS_BRONZE_BUCKET": "my-bronze"}, clear=False)
def test_get_folder_env_var(client: IcebergClient) -> None:
    expected = os.path.join(client.base_path, "my-bronze")
    assert client.get_folder() == expected


def test_get_folder_default(client: IcebergClient) -> None:
    with patch.dict(os.environ, {}, clear=True):
        expected = os.path.join(client.base_path, Layer.BRONZE.value)
        assert client.get_folder() == expected


def test_get_path(client: IcebergClient) -> None:
    with patch.object(client, "get_folder", return_value="s3://bronze"):
        assert client.get_path() == "s3://bronze/raw/events"


def test_write_unsupported_type(client: IcebergClient) -> None:
    with pytest.raises(NotImplementedError, match="Write not implemented"):
        client.write("not a dataframe")


def test_write_polars_appends_existing_table(
    client: IcebergClient, sample_df: pl.DataFrame
) -> None:
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    with (
        patch.object(
            type(client),
            "catalog",
            new_callable=lambda: property(lambda self: mock_catalog),
        ),
        patch("lubrikit.load.storage.iceberg_client.logger"),
    ):
        client.write(sample_df)

    mock_catalog.load_table.assert_called_once_with("raw.events")
    mock_table.append.assert_called_once()
    arrow_arg = mock_table.append.call_args[0][0]
    assert arrow_arg.num_rows == 3


def test_write_polars_creates_table_when_missing(
    client: IcebergClient, sample_df: pl.DataFrame
) -> None:
    from pyiceberg.exceptions import NoSuchTableError

    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.side_effect = [NoSuchTableError("raw.events"), mock_table]

    with (
        patch.object(
            type(client),
            "catalog",
            new_callable=lambda: property(lambda self: mock_catalog),
        ),
        patch.object(client, "get_path", return_value="s3://bronze/raw/events"),
        patch("lubrikit.load.storage.iceberg_client.logger"),
    ):
        client.write(sample_df)

    mock_catalog.create_table.assert_called_once()
    create_kwargs = mock_catalog.create_table.call_args[1]
    assert create_kwargs["identifier"] == "raw.events"
    assert create_kwargs["location"] == "s3://bronze/raw/events"
    mock_table.append.assert_called_once()


def test_write_polars_logs_row_count(
    client: IcebergClient, sample_df: pl.DataFrame
) -> None:
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    with (
        patch.object(
            type(client),
            "catalog",
            new_callable=lambda: property(lambda self: mock_catalog),
        ),
        patch("lubrikit.load.storage.iceberg_client.logger") as mock_logger,
    ):
        client.write(sample_df)

    mock_logger.info.assert_called_once()
    log_msg = mock_logger.info.call_args[0][0]
    assert "3" in log_msg
    assert "raw.events" in log_msg


def test_catalog_uses_glue_type(client: IcebergClient) -> None:
    with patch(
        "lubrikit.load.storage.iceberg_client.load_catalog"
    ) as mock_load:
        mock_load.return_value = MagicMock()
        _ = client.catalog
        _, kwargs = mock_load.call_args
        assert kwargs["type"] == "glue"
