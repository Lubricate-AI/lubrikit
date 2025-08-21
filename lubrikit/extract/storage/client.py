import logging
import os
from functools import singledispatchmethod
from typing import Any

from requests import Response

from lubrikit.base.storage import (
    FileMode,
    Layer,
    StorageClient,
)
from lubrikit.extract.storage.file_metadata import FileMetadata

logger = logging.getLogger(__name__)


class ExtractStorageClient(StorageClient):
    def __init__(self, file_metadata: FileMetadata) -> None:
        self.metadata = file_metadata

    def get_folder(self) -> str:
        """Get the folder path for the given metadata.

        Returns:
            str: The folder path in storage.
        """
        folder = os.environ.get("AWS_LANDING_BUCKET", Layer.LANDING.bucket)

        return os.path.join(self.base_path, folder)

    def get_path(self, metadata: FileMetadata) -> str:
        """Get the path to the table.

        Args:
            metadata (FileMetadata): The metadata of the file.
        Returns:
                str: The path to the table.
        """
        folder: str = self.get_folder()
        source_name: str | None = metadata.get("source_name")
        prefix: str | None = metadata.get("prefix")

        path_components: list[str] = [folder]
        if source_name:
            path_components.append(source_name)
        if prefix:
            path_components.append(prefix)

        return "/".join(path_components)

    @singledispatchmethod
    def write(self, data: Any) -> None:
        """Write data to storage.

        Args:
            data (Any): The data to write.
        Raises:
            NotImplementedError: If the data type is not supported.
        """
        raise NotImplementedError(f"Write not implemented for type {type(data)}")

    @write.register
    def _(self, data: Response) -> None:
        """Write a requests.Response object to storage.

        Args:
            data (Response): The Response object to write.
        """
        data.raise_for_status()

        output_path: str = self.get_path(self.metadata)
        self._make_dirs(path=self.get_folder())

        logger.info(f"Writing {data.headers['Content-Length']} bytes to {output_path}")
        with open(output_path, FileMode.WRITING_BINARY) as f:
            for chunk in data.iter_content(chunk_size=ExtractStorageClient.chunk_size):
                f.write(chunk)
