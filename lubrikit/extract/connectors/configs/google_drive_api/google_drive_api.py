from pydantic import BaseModel, Field


class GoogleDriveAPIConfig(BaseModel):
    """Configuration for Google Drive API connector.

    This class defines the configuration parameters required to connect
    to and interact with the Google Drive API. It uses Pydantic for data
    validation and type checking.

    Attributes:
        fileId (str): The unique identifier of the Google Drive file to
            access. This is a required field with a minimum length of 1
            character. The file ID can be found in the Google Drive URL
            when viewing a file.
            Example: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        mimeType (str | None): The MIME type of the file. This is an
            optional field that can be used to specify the format of the
            file when downloading or exporting it. If not specified, the
            default MIME type will be used.
            Examples: "text/csv", "application/pdf"

    Example:
        >>> config = GoogleDriveAPIConfig(
        ...     fileId="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        ... )
        >>> print(config.fileId)
        1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms
    """

    fileId: str = Field(
        min_length=1,
        description="The unique identifier of the Google Drive file to access. "
        "Found in the Google Drive URL when viewing a file.",
    )
    mimeType: str | None = Field(default=None, description="The MIME type of the file.")
