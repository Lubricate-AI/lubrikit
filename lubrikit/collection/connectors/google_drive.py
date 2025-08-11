import io
import logging
from functools import cached_property
from typing import Any

from google.auth.exceptions import RefreshError, TransportError
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import Error as GoogleAPIError
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from lubrikit.collection.connectors.base import BaseConnector
from lubrikit.collection.connectors.configs import (
    GoogleDriveConfig,
    GoogleDriveServiceAccountInfo,
)
from lubrikit.utils.retry import RetryConfig

logger = logging.getLogger(__name__)


class GoogleDriveConnector(BaseConnector):
    """Connector for Google Drive API."""

    api_name: str = "drive"
    api_version: str = "v3"
    scopes = ["https://www.googleapis.com/auth/drive"]

    def __init__(
        self,
        headers_cache: dict[str, str],
        config: GoogleDriveConfig,
        service_account_info: GoogleDriveServiceAccountInfo | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(headers_cache, retry_config)

        self.config = config
        self.service_account_info = (
            service_account_info or GoogleDriveServiceAccountInfo()
        )
        self.retriable_exceptions = (
            HttpError,  # HTTP errors from Google API (rate limits, server errors)
            RefreshError,  # Authentication token refresh failures
            TransportError,  # Network/transport layer errors
            GoogleAPIError,  # Base Google API errors
            ConnectionError,  # Network connection issues
            TimeoutError,  # Request timeout errors
        )
        self.client: Resource | None = None

    @cached_property
    def content_length(self) -> int | None:
        """Get the content length of the file in Google Drive.

        Returns:
            int: Content length of the file.
        """
        if not self.client:
            raise ValueError("Google Drive client is not initialized.")

        return int(
            self.client.files()  # type: ignore[attr-defined]
            .get(fileId=self.config.file_id, fields="size")
            .execute()
            .get("size")
        )

    @cached_property
    def etag(self) -> str | None:
        """Get the ETag of the file in Google Drive.

        Returns:
            str: ETag of the file.
        """
        if not self.client:
            raise ValueError("Google Drive client is not initialized.")

        return str(
            self.client.files()  # type: ignore[attr-defined]
            .get(fileId=self.config.file_id, fields="etag")
            .execute()
            .get("etag")
        )

    @cached_property
    def last_modified_at(self) -> str | None:
        """When the file was updated in Google Drive.

        Returns:
            datetime: Datetime for when the data source file was updated.
        """
        if not self.client:
            raise ValueError("Google Drive client is not initialized.")

        return str(
            self.client.files()  # type: ignore[attr-defined]
            .get(fileId=self.config.file_id, fields="modifiedTime")
            .execute()
            .get("modifiedTime", "")
        )

    def connect(self) -> Resource:
        """Create a client that communicates to a Google API.

        Returns:
            Resource: A Google API client resource.
        """
        credentials = service_account.Credentials.from_service_account_info(
            info=self.service_account_info.model_dump()
        )
        scoped_credentials = credentials.with_scopes(GoogleDriveConnector.scopes)

        client = build(
            GoogleDriveConnector.api_name,
            GoogleDriveConnector.api_version,
            credentials=scoped_credentials,
        )
        logger.info("Google Drive API client connected")

        return client  # type: ignore[no-any-return]

    def _check(self) -> dict[str, Any] | None:
        """Check the Google Drive resource without downloading it.

        Returns:
            dict[str, Any]: A dictionary containing the updated headers cache.
        """
        try:
            self.client = self.connect()
            return self._prepare_cache()
        except Exception as e:
            logger.error(f"Failed to connect to Google Drive API: {e}")
            return None

    def _download(self) -> tuple[dict[str, Any] | None, MediaIoBaseDownload]:
        """Download the Google Drive file.

        Returns:
            tuple[dict[str, Any] | None, MediaIoBaseDownload]: A tuple
                containing the file metadata and the download stream.
        """
        self.client = self.connect()
        request = self.client.files().get_media(fileId=self.config.file_id)  # type: ignore[attr-defined]
        stream = io.BytesIO()
        downloader = MediaIoBaseDownload(stream, request)

        return self._prepare_cache(), downloader

    def _prepare_cache(self) -> dict[str, str]:
        """Prepare cache metadata from the Google Drive file.

        Returns:
            dict[str, str]: A dictionary containing cache metadata.
        """
        cache: dict[str, str] = {
            "etag": self.etag or "",
            "last_modified": self.last_modified_at or "",
            "content_length": str(self.content_length) if self.content_length else "",
        }

        return cache
