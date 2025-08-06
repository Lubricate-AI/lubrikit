from enum import Enum


class FileType(str, Enum):
    """File types supported by the project for reading and writing."""

    ACCESS = "mdb"
    CSV = "csv"
    DELTA = "delta"
    EXCEL = "xls"
    JSON = "json"
    HTML = "html"
    PARQUET = "parquet"
    XML = "xml"
    ZIP = "zip"
