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

from lubrikit.extract.connectors.base import BaseConnector
from lubrikit.extract.connectors.configs import (
    GoogleDriveAPIConfig,
    GoogleDriveAPIServiceAccountInfo,
)
from lubrikit.utils.retry import RetryConfig

logger = logging.getLogger(__name__)


class GoogleDriveAPIConnector(BaseConnector):
    """Connector for Google Drive API.

    This module provides a connector for Google Drive API that allows data
    engineers to programmatically access and download files from Google
    Drive using service account authentication. It supports caching to
    avoid re-downloading unchanged files.

    Attributes:
        api_name (str): (class attribute) The name of the Google API
            service. Always "drive" for Google Drive API.
        api_version (str): (class attribute) The version of the Google
            Drive API to use. Currently "v3".
        client (Resource | None): Google API client resource for making
            Drive API calls. Initialized to None and set during
            connection establishment.
        config (GoogleDriveAPIConfig): Configuration object containing
            the Google Drive file ID and other connection parameters.
        retriable_exceptions (tuple[type[Exception], ...]): Tuple of
            exception types that should trigger retry logic. Includes
            HTTP errors, authentication failures, and network issues.
        scopes (list[str]): (class attribute) The OAuth2 scopes required
            for Google Drive access. Contains
            ["https://www.googleapis.com/auth/drive"] for full Drive
            access.
        service_account_info (GoogleDriveAPIServiceAccountInfo): Service
            account credentials for Google Cloud authentication. Loaded
            from environment variables if not provided.

    Setting Up Google Drive API Service Account
    ===========================================

    To use this connector, you need to create a Google Cloud service
    account with access to the Google Drive API. Follow these detailed
    steps:

    Step 1: Create a Google Cloud Project
    -------------------------------------
    1. Go to the Google Cloud Console: https://console.cloud.google.com/
    2. Click "Select a project" dropdown at the top
    3. Click "New Project"
    4. Enter a project name (e.g., "my-data-pipeline")
    5. Select your organization (if applicable)
    6. Click "Create"
    7. Wait for the project to be created and make sure it's selected

    Step 2: Enable the Google Drive API
    -----------------------------------
    1. In the Google Cloud Console, go to "APIs & Services" > "Library"
    2. Search for "Google Drive API"
    3. Click on "Google Drive API" from the results
    4. Click the "Enable" button
    5. Wait for the API to be enabled (this may take a few minutes)

    Step 3: Create a Service Account
    --------------------------------
    1. Go to "APIs & Services" > "Credentials"
    2. Click "Create Credentials" > "Service account"
    3. Fill in the service account details:
    - Service account name: e.g., "drive-data-connector"
    - Service account ID: will be auto-generated (e.g.,
            "drive-data-connector")
    - Description: e.g., "Service account for accessing Google Drive
            files"
    4. Click "Create and Continue"
    5. For "Grant this service account access to project":
    - You can skip this step for basic usage (click "Continue")
    6. For "Grant users access to this service account":
    - You can skip this step (click "Done")

    Step 4: Create and Download Service Account Key
    -----------------------------------------------
    1. In the "Credentials" page, find the newly created service account
    2. Click on the service account email to open its details
    3. Go to the "Keys" tab
    4. Click "Add Key" > "Create new key"
    5. Select "JSON" as the key type
    6. Click "Create"
    7. A JSON file will be downloaded to your computer
    8. **IMPORTANT**: Store this file securely and never commit it to
        version control

    Step 5: Share Google Drive Files with Service Account
    -----------------------------------------------------
    For the service account to access files in Google Drive, you need to
    share them:

    1. Open Google Drive in your browser
    2. Right-click on the file or folder you want to access
    3. Click "Share"
    4. In the share dialog, enter the service account email address
    (found in the JSON file as "client_email")
    5. Set appropriate permissions (typically "Viewer")
    6. Click "Send"

    Step 6: Configure Environment Variables
    --------------------------------------
    Extract the values from your downloaded JSON file and set these
    environment variables:

    .. code-block:: bash

        export GOOGLE_TYPE="service_account"
        export GOOGLE_PROJECT_ID="your-project-id"
        export GOOGLE_PRIVATE_KEY_ID="your-private-key-id"
        export GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\\nYour...Key\\n-----END PRIVATE KEY-----\\n"
        export GOOGLE_CLIENT_EMAIL="service-account@project.iam.gserviceaccount.com"
        export GOOGLE_CLIENT_ID="123456789012345678901"
        export GOOGLE_AUTH_URI="https://accounts.google.com/o/oauth2/auth"
        export GOOGLE_TOKEN_URI="https://oauth2.googleapis.com/token"
        export GOOGLE_AUTH_PROVIDER_X509_CERT_URL="https://www.googleapis.com/oauth2/v1/certs"
        export GOOGLE_CLIENT_X509_CERT_URL="https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
        export GOOGLE_UNIVERSE_DOMAIN="googleapis.com"

    Alternative: Using .env File
    ---------------------------
    Instead of setting environment variables manually, you can create a
    `.env` file:

    .. code-block:: bash

        # .env file
        GOOGLE_TYPE=service_account
        GOOGLE_PROJECT_ID=your-project-id
        GOOGLE_PRIVATE_KEY_ID=your-private-key-id
        GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\\nYour...Key\\n-----END PRIVATE KEY-----\\n"
        GOOGLE_CLIENT_EMAIL=service-account@project.iam.gserviceaccount.com
        GOOGLE_CLIENT_ID=123456789012345678901
        GOOGLE_AUTH_URI=https://accounts.google.com/o/oauth2/auth
        GOOGLE_TOKEN_URI=https://oauth2.googleapis.com/token
        GOOGLE_AUTH_PROVIDER_X509_CERT_URL=https://www.googleapis.com/oauth2/v1/certs
        GOOGLE_CLIENT_X509_CERT_URL=https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com
        GOOGLE_UNIVERSE_DOMAIN=googleapis.com

    Finding Google Drive File IDs
    =============================

    To use this connector, you need the Google Drive file ID:

    Method 1: From the Browser URL
    ------------------------------
    1. Open the file in Google Drive
    2. Look at the URL in your browser
    3. The file ID is the long string after "/d/" and before "/view" or "/edit"
    Example: https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view
    File ID: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms

    Method 2: From Share Link
    -------------------------
    1. Right-click the file in Google Drive
    2. Click "Get link"
    3. Copy the link - the file ID is in the same position as Method 1

    Security Best Practices
    =======================

    1. **Never commit service account keys to version control**
    2. **Use environment variables or secure secret management**
    3. **Grant minimal permissions** (only "Viewer" if you only need to
        read)
    4. **Regularly rotate service account keys**
    5. **Monitor service account usage** in Google Cloud Console
    6. **Use different service accounts for different environments**
        (dev, staging, prod)

    Troubleshooting Common Issues
    ============================

    "403 Forbidden" Error
    --------------------
    - Ensure the Google Drive API is enabled in your project
    - Verify the service account email has been granted access to the
        file
    - Check that the file ID is correct

    "Authentication Error"
    ---------------------
    - Verify all environment variables are set correctly
    - Check that the private key includes the full
        "-----BEGIN PRIVATE KEY-----" header
    - Ensure newlines in the private key are properly escaped as "\\n"

    "File Not Found" Error
    ----------------------
    - Double-check the file ID
    - Ensure the file hasn't been deleted or moved
    - Verify the service account has access to the file

    Example:
        Basic usage with file download:

        ```python
        from lubrikit.collection.connectors.google_drive import GoogleDriveAPIConnector
        from lubrikit.collection.connectors.configs import GoogleDriveAPIConfig

        # Optional: Use cached headers to avoid re-downloading unchanged files
        headers_cache = {
            "file_name": "[YOUR_FILE_NAME]",
            "last_modified": "[YOUR_LAST_MODIFIED]",
            "content_length": "[YOUR_CONTENT_LENGTH]",
        }

        # Configure the file to download
        config = GoogleDriveAPIConfig(file_id="[YOUR_FILE_ID]")

        # Create connector instance
        connector = GoogleDriveAPIConnector(config=config, headers_cache=headers_cache)

        # Check for updates and get download info
        headers, (stream, downloader) = connector.download()
        print(headers)

        # Download file if there are updates
        if downloader:
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Download {int(status.progress() * 100)}%.")

        # Save downloaded content to file
        if stream:
            with open(headers["file_name"], "wb") as f:
                stream.seek(0)
                f.write(stream.read())
        ```

    Note:
        Requires Google service account credentials to be configured via
        environment variables or passed as
        GoogleDriveAPIServiceAccountInfo.
    """

    api_name: str = "drive"  # Google API service name for Drive API
    api_version: str = "v3"  # Google Drive API version
    scopes = [
        "https://www.googleapis.com/auth/drive"
    ]  # OAuth2 scopes for full Drive access

    def __init__(
        self,
        config: GoogleDriveAPIConfig,
        service_account_info: GoogleDriveAPIServiceAccountInfo | None = None,
        headers_cache: dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(headers_cache, retry_config)

        # Configuration object containing file ID and connection parameters
        self.config = config

        # Service account credentials for Google Cloud authentication
        self.service_account_info: GoogleDriveAPIServiceAccountInfo
        if service_account_info:
            self.service_account_info = service_account_info
        else:
            logger.info(
                "Loading Google Drive service account info from environment variables"
            )
            self.service_account_info = GoogleDriveAPIServiceAccountInfo()

        # Tuple of exception types that should trigger retry logic
        self.retriable_exceptions = (
            HttpError,  # HTTP errors from Google API (rate limits, server errors)
            RefreshError,  # Authentication token refresh failures
            TransportError,  # Network/transport layer errors
            GoogleAPIError,  # Base Google API errors
            ConnectionError,  # Network connection issues
            TimeoutError,  # Request timeout errors
        )

        # Google API client resource for making Drive API calls
        self.client: Resource | None = None

    @cached_property
    def content_length(self) -> int:
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
    def file_name(self) -> str:
        """Get the name of the file in Google Drive.

        Returns:
            str: Name of the file.
        """
        if not self.client:
            raise ValueError("Google Drive client is not initialized.")

        return str(
            self.client.files()  # type: ignore[attr-defined]
            .get(fileId=self.config.file_id, fields="name")
            .execute()
            .get("name")
        )

    @cached_property
    def last_modified_at(self) -> str:
        """When the file was updated in Google Drive.

        Returns:
            str: Datetime string for when the data source file was updated.
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
        scoped_credentials = credentials.with_scopes(GoogleDriveAPIConnector.scopes)

        client = build(
            GoogleDriveAPIConnector.api_name,
            GoogleDriveAPIConnector.api_version,
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

    def _download(
        self,
    ) -> tuple[
        dict[str, Any] | None, tuple[io.BytesIO | None, MediaIoBaseDownload | None]
    ]:
        """Download the Google Drive file.

        Returns:
            tuple[dict[str, Any] | None, MediaIoBaseDownload]: A tuple
                containing the file metadata and the download stream.
        """
        self.client = self.connect()
        new_headers = self._prepare_cache()

        # If resource unchanged
        if all(
            self.headers_cache.get(k) == new_headers.get(k)
            for k in ["last_modified", "content_length"]
        ):
            logger.info("No new version. Skipping download.")
            return new_headers, (None, None)

        logger.info(f"Downloading file {self.config.file_id} from Google Drive...")
        request = self.client.files().get_media(fileId=self.config.file_id)  # type: ignore[attr-defined]
        stream = io.BytesIO()
        downloader = MediaIoBaseDownload(stream, request)

        return new_headers, (stream, downloader)

    def _prepare_cache(self) -> dict[str, str]:
        """Prepare cache metadata from the Google Drive file.

        Returns:
            dict[str, str]: A dictionary containing cache metadata.
        """
        cache: dict[str, str] = {
            "file_name": self.file_name,
            "last_modified": self.last_modified_at or "",
            "content_length": str(self.content_length) if self.content_length else "",
        }

        return cache
