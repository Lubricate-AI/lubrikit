import pytest

from lubrikit.base.storage.file_type import FileType


@pytest.mark.parametrize(
    "member, value",
    [
        (FileType.ACCESS, "mdb"),
        (FileType.CSV, "csv"),
        (FileType.DELTA, "delta"),
        (FileType.EXCEL, "xls"),
        (FileType.JSON, "json"),
        (FileType.PARQUET, "parquet"),
        (FileType.XML, "xml"),
        (FileType.ZIP, "zip"),
    ],
)
def test_file_type_members(member: FileType, value: str) -> None:
    """Test that each FileType member has the correct value."""
    assert member.value == value, f"Expected {value}, got {member.value}"
    assert member == value


@pytest.mark.parametrize(
    "member",
    [
        "ACCESS",
        "CSV",
        "DELTA",
        "EXCEL",
        "JSON",
        "PARQUET",
        "XML",
        "ZIP",
    ],
)
def test_file_type_member_names(member: str) -> None:
    assert hasattr(FileType, member), f"FileType does not have member {member}"


def test_file_type_unique_values() -> None:
    values = [member.value for member in FileType]
    assert len(values) == len(set(values)), "FileType values are not unique"
