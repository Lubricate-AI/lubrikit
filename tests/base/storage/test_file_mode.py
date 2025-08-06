import pytest

from lubrikit.base.storage.file_mode import FileMode


@pytest.mark.parametrize(
    "member, value",
    [
        (FileMode.READING, "r"),
        (FileMode.READING_BINARY, "rb"),
        (FileMode.READING_AND_WRITING, "r+"),
        (FileMode.READING_AND_WRITING_BINARY, "rb+"),
        (FileMode.WRITING, "w"),
        (FileMode.WRITING_BINARY, "wb"),
        (FileMode.WRITING_AND_READING, "w+"),
        (FileMode.WRITING_AND_READING_BINARY, "wb+"),
        (FileMode.APPENDING, "a"),
        (FileMode.APPENDING_BINARY, "ab"),
        (FileMode.APPENDING_AND_READING, "a+"),
        (FileMode.APPENDING_AND_READING_BINARY, "ab+"),
    ],
)
def test_file_mode_members(member: FileMode, value: str) -> None:
    """Test that each FileMode member has the correct value."""
    assert member.value == value, f"Expected {value}, got {member.value}"
    assert member == value


@pytest.mark.parametrize(
    "member",
    [
        "READING",
        "READING_BINARY",
        "READING_AND_WRITING",
        "READING_AND_WRITING_BINARY",
        "WRITING",
        "WRITING_BINARY",
        "WRITING_AND_READING",
        "WRITING_AND_READING_BINARY",
        "APPENDING",
        "APPENDING_BINARY",
        "APPENDING_AND_READING",
        "APPENDING_AND_READING_BINARY",
    ],
)
def test_file_mode_member_names(member: str) -> None:
    assert hasattr(FileMode, member), f"FileMode does not have member {member}"


def test_file_mode_unique_values() -> None:
    values = [member.value for member in FileMode]
    assert len(values) == len(set(values)), "FileMode values are not unique"
