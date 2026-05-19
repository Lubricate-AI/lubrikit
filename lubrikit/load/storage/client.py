import logging
import os
from typing import Any

import pandas as pd

from lubrikit.base.storage import FileType, Layer, StorageClient

logger = logging.getLogger(__name__)


class LoadStorageClient(StorageClient):
    def __init__(self, source_name: str | None = None) -> None:
        self.source_name = source_name

    def get_folder(self) -> str:
        """Get the folder path for the landing layer.

        Returns:
            str: The folder path in storage.
        """
        folder = os.environ.get("AWS_LANDING_BUCKET", Layer.LANDING.bucket)
        return os.path.join(self.base_path, folder)

    def get_path(self, file_name: Any) -> str:
        """Get the full path for the given file name.

        Args:
            file_name (Any): The name of the file.

        Returns:
            str: The full path to the file in storage.
        """
        folder = self.get_folder()
        components = [folder]
        if self.source_name:
            components.append(self.source_name)
        components.append(str(file_name))
        return "/".join(components)

    def read(self, file_name: str, file_type: FileType) -> pd.DataFrame:
        """Read a file from storage into a pandas DataFrame.

        Args:
            file_name (str): The name of the file to read.
            file_type (FileType): The type of the file.

        Returns:
            pd.DataFrame: The file contents as a DataFrame.

        Raises:
            NotImplementedError: If the file type is not supported.
        """
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
