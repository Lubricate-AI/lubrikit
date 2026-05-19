import logging
import os
from functools import singledispatchmethod
from typing import Any

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from lubrikit.base.storage import Layer, StorageClient

logger = logging.getLogger(__name__)


class IcebergClient(StorageClient):
    def __init__(
        self,
        namespace: str,
        table_name: str,
        catalog_name: str = "default",
    ) -> None:
        self.namespace = namespace
        self.table_name = table_name
        self.catalog_name = catalog_name

    @property
    def catalog(self) -> Catalog:
        """Return the Iceberg catalog.

        Returns:
            Catalog: The Iceberg catalog.
        """
        warehouse = os.environ.get("AWS_BRONZE_BUCKET", Layer.BRONZE.bucket)
        return load_catalog(
            self.catalog_name,
            **{
                "type": "glue",
                "warehouse": f"{self.base_path}{warehouse}",
            },
        )

    def get_folder(self) -> str:
        """Get the folder path for the bronze layer.

        Returns:
            str: The folder path in storage.
        """
        folder = os.environ.get("AWS_BRONZE_BUCKET", Layer.BRONZE.bucket)
        return os.path.join(self.base_path, folder)

    def get_path(self, metadata: Any = None) -> str:
        """Get the full path for the Iceberg table.

        Args:
            metadata (Any): Unused; path is derived from instance attributes.

        Returns:
            str: The full path to the table in storage.
        """
        folder = self.get_folder()
        return "/".join([folder, self.namespace, self.table_name])

    @singledispatchmethod
    def write(self, data: Any) -> None:
        """Write data to an Iceberg table.

        Args:
            data (Any): The data to write.
        Raises:
            NotImplementedError: If the data type is not supported.
        """
        raise NotImplementedError(f"Write not implemented for type {type(data)}")

    @write.register
    def _(self, data: pd.DataFrame) -> None:
        """Write a pandas DataFrame to an Iceberg table.

        Creates the table if it does not already exist, then appends
        the DataFrame contents to the table.

        Args:
            data (pd.DataFrame): The DataFrame to write.
        """
        arrow_table = pa.Table.from_pandas(data)
        full_table_name = f"{self.namespace}.{self.table_name}"

        logger.info(f"Writing {len(data)} rows to Iceberg table {full_table_name}")

        try:
            table = self.catalog.load_table(full_table_name)
            table.append(arrow_table)
        except NoSuchTableError:
            self.catalog.create_table(
                identifier=full_table_name,
                schema=arrow_table.schema,
                location=self.get_path(),
            )
            table = self.catalog.load_table(full_table_name)
            table.append(arrow_table)
