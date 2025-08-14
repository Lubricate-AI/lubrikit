from pydantic import BaseModel, Field


class GoogleDriveAPIConfig(BaseModel):
    """Configuration for Google Drive API connector."""

    file_id: str = Field(
        min_length=1, description="The ID of the Google Drive file to access"
    )
