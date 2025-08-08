from functools import cached_property
from typing import Any
from unittest.mock import MagicMock

import pytest

from lubrikit.base.storage import StorageClient


def test_base_path() -> None:
    client = StorageClient()
    assert client.base_path == "s3://"


@pytest.fixture
def MockStorageClient() -> type:
    class MockStorageClient(StorageClient):
        @cached_property
        def s3(self) -> Any:
            s3 = MagicMock()
            s3.exists.return_value = False
            return s3

    return MockStorageClient


def test_make_dirs(MockStorageClient: type) -> None:
    client = MockStorageClient()
    client._make_dirs("test_folder/test_path")

    client.s3.mkdir.assert_called_once_with("test_folder/test_path")


def test_write_not_implemented(MockStorageClient: type) -> None:
    client = MockStorageClient()
    with pytest.raises(NotImplementedError):
        client.write("test data")
