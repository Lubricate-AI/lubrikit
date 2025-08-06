from functools import cached_property
from typing import Any
from unittest.mock import MagicMock

import pytest

from lubrikit.base.storage import S3Client


def test_base_path() -> None:
    client = S3Client()
    assert client.base_path == "s3://"


@pytest.fixture
def MockS3Client() -> type:
    class MockS3Client(S3Client):
        @cached_property
        def s3(self) -> Any:
            s3 = MagicMock()
            s3.exists.return_value = False
            return s3

    return MockS3Client


def test_make_dirs(MockS3Client: type) -> None:
    client = MockS3Client()
    client._make_dirs("test_folder/test_path")

    client.s3.mkdir.assert_called_once_with("test_folder/test_path")


def test_write_not_implemented(MockS3Client: type) -> None:
    client = MockS3Client()
    with pytest.raises(NotImplementedError):
        client.write("test data")
