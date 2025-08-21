from datetime import datetime
from typing import Any

from typing_extensions import TypedDict


class FileMetadata(TypedDict, total=False):
    """Metadata for a file in storage.

    Attributes:
        source_name (str | None): The human-readable name of the source
            system.
        prefix (str | None): An optional prefix for the file path.
        headers_cache (dict[str, str] | None): A cache of HTTP headers
            associated with the file. These are used to determine if the
            file has changed since the last retrieval.
        connector (Literal["HTTPConnector", "GoogleDriveAPIConnector"]):
            The type of connector used to retrieve the file.
        connector_config (dict[str, Any]): Configuration details for the
            connector.
        retry_config (dict[str, int | float] | None): Retry
            configuration for handling transient errors.
        created_at (datetime): Timestamp when the file metadata was
            created.
        modified_at (datetime): Timestamp when the file metadata was
            last modified.
        deleted_at (datetime | None): Timestamp when the file metadata
            was deleted.
        checked_at (datetime | None): Timestamp when the files' headers
            were last checked.
        landed_at (datetime | None): Timestamp when the file was last
            landed in storage.
        staged_at (datetime | None): Timestamp when the file was last
            staged for processing.
        processed_at (datetime | None): Timestamp when the file was last
            processed.
    """

    connector: str
    connector_config: dict[str, Any]
    headers_cache: dict[str, str] | None
    prefix: str | None
    retry_config: dict[str, int | float] | None
    source_name: str | None

    # file metadata lineage
    created_at: datetime
    modified_at: datetime
    deleted_at: datetime | None

    # file lineage
    checked_at: datetime | None
    landed_at: datetime | None
    staged_at: datetime | None
    processed_at: datetime | None
