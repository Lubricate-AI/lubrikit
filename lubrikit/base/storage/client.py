import logging
from functools import cached_property, singledispatchmethod
from typing import Any

import s3fs  # type: ignore

logger = logging.getLogger(__name__)


class StorageClient:
    chunk_size: int = 1024
    encoding: str = "utf-8"

    @property
    def base_path(self) -> str:
        """Get the base path for the storage client.

        Returns:
            str: The base path for the storage client.
        """
        return "s3://"

    @cached_property
    def s3(self) -> s3fs.S3FileSystem:  # type: ignore
        """Create an S3 file system object.

        Returns:
            s3fs.S3FileSystem: The S3 file system object.
        """
        return s3fs.S3FileSystem()

    def _make_dirs(self, path: str) -> None:
        if not self.s3.exists(path):
            self.s3.mkdir(path)

    @singledispatchmethod
    def write(self, data: Any) -> None:
        """Write data to storage.

        Args:
            data (Any): The data to write.
        Raises:
            NotImplementedError: If the data type is not supported.
        """
        raise NotImplementedError(f"Write not implemented for type {type(data)}")
