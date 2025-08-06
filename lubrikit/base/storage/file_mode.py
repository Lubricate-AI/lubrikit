from enum import Enum


class FileMode(str, Enum):
    """Modes for how files are opened, read, or written."""

    READING = "r"
    READING_BINARY = "rb"
    READING_AND_WRITING = "r+"
    READING_AND_WRITING_BINARY = "rb+"
    WRITING = "w"
    WRITING_BINARY = "wb"
    WRITING_AND_READING = "w+"
    WRITING_AND_READING_BINARY = "wb+"
    APPENDING = "a"
    APPENDING_BINARY = "ab"
    APPENDING_AND_READING = "a+"
    APPENDING_AND_READING_BINARY = "ab+"
