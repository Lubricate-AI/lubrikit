import logging
import os
from functools import singledispatchmethod
from typing import Any

import polars as pl
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
        warehouse = os.environ.get("AWS_BRONZE_BUCKET", Layer.BRONZE.bucket)
        return load_catalog(
            self.catalog_name,
            **{
                "type": "glue",
                "warehouse": f"{self.base_path}{warehouse}",
            },
        )

    def get_folder(self) -> str:
        folder = os.environ.get("AWS_BRONZE_BUCKET", Layer.BRONZE.bucket)
        return os.path.join(self.base_path, folder)

    def get_path(self, metadata: Any = None) -> str:
        folder = self.get_folder()
        return "/".join([folder, self.namespace, self.table_name])

    @singledispatchmethod
    def write(self, data: Any) -> None:
        raise NotImplementedError(f"Write not implemented for type {type(data)}")

    @write.register
    def _(self, data: pl.DataFrame) -> None:
        arrow_table: pa.Table = data.to_arrow()  # type: ignore[no-any-unimported]
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
