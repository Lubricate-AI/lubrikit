import logging
import os
from typing import Any

import pandas as pd
import polars as pl

from lubrikit.base.storage import FileType, Layer, StorageClient

logger = logging.getLogger(__name__)


class LoadStorageClient(StorageClient):
    def __init__(self, source_name: str | None = None) -> None:
        self.source_name = source_name

    def get_folder(self) -> str:
        folder = os.environ.get("AWS_LANDING_BUCKET", Layer.LANDING.bucket)
        return os.path.join(self.base_path, folder)

    def get_path(self, file_name: Any) -> str:
        folder = self.get_folder()
        components = [folder]
        if self.source_name:
            components.append(self.source_name)
        components.append(str(file_name))
        return "/".join(components)

    def read(self, file_name: str, file_type: FileType) -> pd.DataFrame:
        path = self.get_path(file_name)

        if file_type == FileType.CSV:
            return pd.read_csv(path, storage_options={"anon": False})
        elif file_type == FileType.PARQUET:
            return pd.read_parquet(path, storage_options={"anon": False})
        elif file_type == FileType.JSON:
            return pd.read_json(path, storage_options={"anon": False})
        elif file_type in (FileType.EXCEL, FileType.ACCESS):
            with self.s3.open(path, "rb") as f:
                return pd.read_excel(f)
        else:
            raise NotImplementedError(f"Read not implemented for file type {file_type}")

    def read_polars(self, file_name: str, file_type: FileType) -> pl.DataFrame:
        path = self.get_path(file_name)

        if file_type == FileType.CSV:
            return pl.read_csv(path, storage_options={"anon": False})
        elif file_type == FileType.PARQUET:
            return pl.read_parquet(path, storage_options={"anon": False})
        elif file_type in (FileType.JSON, FileType.EXCEL, FileType.ACCESS):
            with self.s3.open(path, "rb") as f:
                if file_type == FileType.JSON:
                    return pl.read_json(f)
                return pl.read_excel(f)
        else:
            raise NotImplementedError(
                f"read_polars not implemented for file type {file_type}"
            )
