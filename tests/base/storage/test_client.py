from functools import cached_property
from typing import Any
from unittest.mock import MagicMock

import pytest

from lubrikit.base.storage import StorageClient


@pytest.fixture
def MockStorageClient() -> type:
    class MockStorageClient(StorageClient):
        @cached_property
        def s3(self) -> Any:
            s3 = MagicMock()
            s3.exists.return_value = False
            return s3

        def get_folder(self) -> str:
            return "test_folder"

        def get_path(self, *args: Any, **kwargs: Any) -> str:
            return "test_folder/test_path"

    return MockStorageClient


def test_base_path(MockStorageClient: type) -> None:
    client = MockStorageClient()
    assert client.base_path == "s3://"


def test_make_dirs(MockStorageClient: type) -> None:
    client = MockStorageClient()
    client._make_dirs("test_folder/test_path")

    client.s3.mkdir.assert_called_once_with("test_folder/test_path")


def test_write_not_implemented(MockStorageClient: type) -> None:
    client = MockStorageClient()
    with pytest.raises(NotImplementedError):
        client.write("test data")
