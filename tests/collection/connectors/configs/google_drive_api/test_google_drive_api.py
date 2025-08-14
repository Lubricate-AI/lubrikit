import pytest
from pydantic import ValidationError

from lubrikit.collection.connectors.configs.google_drive_api import GoogleDriveAPIConfig


def test_valid_file_id() -> None:
    """Test GoogleDriveAPIConfig with valid file_id."""
    file_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    config = GoogleDriveAPIConfig(file_id=file_id)

    assert config.file_id == file_id


@pytest.mark.parametrize(
    "file_id",
    [
        "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",  # Standard file ID
        "1n6RHOzzrpAXSRdVwpLNGIwVxcI4Pqzy_",  # Short file ID
        "1BDcdA4czft0wdvYeyvdgpaK-pSz4SmRG5WhHvwBRmyg",  # Another valid format
        "a" * 100,  # Very long file ID
        "123-abc_DEF",  # File ID with special characters
    ],
)
def test_valid_file_id_formats(file_id: str) -> None:
    """Test GoogleDriveAPIConfig accepts various valid file ID formats."""
    config = GoogleDriveAPIConfig(file_id=file_id)
    assert config.file_id == file_id


def test_file_id_required() -> None:
    """Test that file_id field is required."""
    with pytest.raises(ValidationError) as exc_info:
        GoogleDriveAPIConfig()  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("file_id",)
    assert "missing" in errors[0]["type"]


def test_invalid_file_id_empty_string() -> None:
    """Test that file_id field rejects empty string."""
    with pytest.raises(ValidationError) as exc_info:
        GoogleDriveAPIConfig(file_id="")

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("file_id",)
    assert (
        "string_too_short" in errors[0]["type"]
        or "at least 1 character" in errors[0]["msg"]
    )


@pytest.mark.parametrize(
    "whitespace_file_id",
    [
        "   ",  # Whitespace only
        "\t",  # Tab character
        "\n",  # Newline character
    ],
)
def test_whitespace_file_id_allowed(whitespace_file_id: str) -> None:
    """Test file_id field allows whitespace-only strings (Pydantic behavior)."""
    # Note: Pydantic's min_length counts whitespace characters, so these are valid
    config = GoogleDriveAPIConfig(file_id=whitespace_file_id)
    assert config.file_id == whitespace_file_id


def test_file_id_type_validation() -> None:
    """Test that file_id field rejects non-string types."""
    with pytest.raises(ValidationError) as exc_info:
        GoogleDriveAPIConfig(file_id=123)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("file_id",)
    assert (
        "string_type" in errors[0]["type"]
        or "Input should be a valid string" in errors[0]["msg"]
    )
