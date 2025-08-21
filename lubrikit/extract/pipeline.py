from functools import cached_property
from typing import Any, cast

from lubrikit.base import Pipeline
from lubrikit.extract import connectors
from lubrikit.extract.storage import ExtractStorageClient, FileMetadata


class ExtractPipeline(Pipeline):
    def __init__(self, metadata: dict[str, Any]) -> None:
        self.metadata: FileMetadata = cast(FileMetadata, metadata)

    @cached_property
    def client(self) -> ExtractStorageClient:
        """Return the ExtractStorageClient instance."""
        return ExtractStorageClient(self.metadata)

    @property
    def connector(self) -> type[connectors.BaseConnector]:
        """Return the connector class based on the metadata."""
        Connector: type[connectors.BaseConnector] | None = getattr(
            connectors, self.metadata["connector"], None
        )
        if not Connector:
            raise ValueError(f"Connector {self.metadata['connector']} not found.")

        return Connector

    def run(self) -> None:
        """Runs the extract pipeline.

        Executes the pipeline process by initializing the connector,
        downloading data, and writing the downloaded data using the
        storage client.
        """
        connector = self.connector(
            config=self.metadata.get("connector_config"),
            headers_cache=self.metadata.get("headers_cache"),
            retry_config=self.metadata.get("retry_config"),
        )

        _, downloader = connector.download()
        if downloader:
            self.client.write(downloader)
