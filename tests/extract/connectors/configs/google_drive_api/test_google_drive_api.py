import pytest
from pydantic import ValidationError

from lubrikit.extract.connectors.configs.google_drive_api import GoogleDriveAPIConfig


def test_valid_fileId() -> None:
    """Test GoogleDriveAPIConfig with valid fileId."""
    fileId = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    config = GoogleDriveAPIConfig(fileId=fileId)

    assert config.fileId == fileId


@pytest.mark.parametrize(
    "fileId",
    [
        "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",  # Standard file ID
        "1n6RHOzzrpAXSRdVwpLNGIwVxcI4Pqzy_",  # Short file ID
        "1BDcdA4czft0wdvYeyvdgpaK-pSz4SmRG5WhHvwBRmyg",  # Another valid format
        "a" * 100,  # Very long file ID
        "123-abc_DEF",  # File ID with special characters
    ],
)
def test_valid_fileId_formats(fileId: str) -> None:
    """Test GoogleDriveAPIConfig accepts various valid file ID formats."""
    config = GoogleDriveAPIConfig(fileId=fileId)
    assert config.fileId == fileId


def test_fileId_required() -> None:
    """Test that fileId field is required."""
    with pytest.raises(ValidationError) as exc_info:
        GoogleDriveAPIConfig()  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("fileId",)
    assert "missing" in errors[0]["type"]


def test_invalid_fileId_empty_string() -> None:
    """Test that fileId field rejects empty string."""
    with pytest.raises(ValidationError) as exc_info:
        GoogleDriveAPIConfig(fileId="")

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("fileId",)
    assert (
        "string_too_short" in errors[0]["type"]
        or "at least 1 character" in errors[0]["msg"]
    )


@pytest.mark.parametrize(
    "whitespace_fileId",
    [
        "   ",  # Whitespace only
        "\t",  # Tab character
        "\n",  # Newline character
    ],
)
def test_whitespace_fileId_allowed(whitespace_fileId: str) -> None:
    """Test fileId field allows whitespace-only strings (Pydantic behavior)."""
    # Note: Pydantic's min_length counts whitespace characters, so these are valid
    config = GoogleDriveAPIConfig(fileId=whitespace_fileId)
    assert config.fileId == whitespace_fileId


def test_fileId_type_validation() -> None:
    """Test that fileId field rejects non-string types."""
    with pytest.raises(ValidationError) as exc_info:
        GoogleDriveAPIConfig(fileId=123)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("fileId",)
    assert (
        "string_type" in errors[0]["type"]
        or "Input should be a valid string" in errors[0]["msg"]
    )
