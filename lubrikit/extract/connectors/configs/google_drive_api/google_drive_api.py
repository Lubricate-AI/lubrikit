from pydantic import BaseModel, Field


class GoogleDriveAPIConfig(BaseModel):
    """Configuration for Google Drive API connector.

    This class defines the configuration parameters required to connect to and
    interact with the Google Drive API. It uses Pydantic for data validation
    and type checking.

    Attributes:
        file_id (str): The unique identifier of the Google Drive file to access.
            This is a required field with a minimum length of 1 character.
            The file ID can be found in the Google Drive URL when viewing a file.
            Example: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"

    Example:
        >>> config = GoogleDriveAPIConfig(
        ...     file_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        ... )
        >>> print(config.file_id)
        1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms
    """

    file_id: str = Field(
        min_length=1,
        description="The unique identifier of the Google Drive file to access. "
        "Found in the Google Drive URL when viewing a file.",
    )
