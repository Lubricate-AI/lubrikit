import pytest
from pydantic import ValidationError

from lubrikit.collection.connectors.configs.http_config import HTTPConfig


@pytest.mark.parametrize(
    "field_name, expected_value",
    [
        ("method", "GET"),
        ("url", "https://api.example.com/data"),
        ("params", None),
        ("data", None),
        ("json_data", None),
        ("extra_headers", None),
    ],
)
def test_required_fields_only(field_name: str, expected_value: str | None) -> None:
    """Test that HTTPConfig can be created with only required fields."""
    config = HTTPConfig(method="GET", url="https://api.example.com/data")

    assert getattr(config, field_name) == expected_value


@pytest.mark.parametrize(
    "field_name, expected_value",
    [
        ("method", "POST"),
        ("url", "https://api.example.com/submit"),
        ("params", {"key": "value", "limit": 10}),
        ("data", {"form_field": "form_value"}),
        ("json_data", {"json_field": "json_value"}),
        (
            "extra_headers",
            {
                "Authorization": "Bearer token",
                "Content-Type": "application/json",
            },
        ),
    ],
)
def test_all_fields_populated(
    field_name: str, expected_value: str | dict | None
) -> None:
    """Test that HTTPConfig accepts all fields with values."""
    config = HTTPConfig(
        method="POST",
        url="https://api.example.com/submit",
        params={"key": "value", "limit": 10},
        data={"form_field": "form_value"},
        json_data={"json_field": "json_value"},
        extra_headers={
            "Authorization": "Bearer token",
            "Content-Type": "application/json",
        },
    )

    assert getattr(config, field_name) == expected_value


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_method_validation(method: str) -> None:
    """Test that GET method is valid."""
    config = HTTPConfig(method=method, url="https://example.com")
    assert config.method == method


@pytest.mark.parametrize(
    "method",
    ["PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"],
)
def test_method_validation_invalid(method: str) -> None:
    """Test that invalid HTTP methods are rejected."""
    with pytest.raises(ValidationError) as exc_info:
        HTTPConfig(method=method, url="https://example.com")

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("method",)
    assert "literal_error" in errors[0]["type"] or "Input should be" in errors[0]["msg"]


@pytest.mark.parametrize("method", ["get", "post"])
def test_method_validation_case_sensitive(method: str) -> None:
    """Test that method validation is case-sensitive."""
    with pytest.raises(ValidationError):
        HTTPConfig(method=method, url="https://example.com")


def test_url_required() -> None:
    """Test that url field is required."""
    with pytest.raises(ValidationError) as exc_info:
        HTTPConfig(method="GET")  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("url",)
    assert "missing" in errors[0]["type"]


def test_url_string_validation() -> None:
    """Test that url field accepts string values."""
    # Valid URLs
    valid_urls = [
        "https://example.com",
        "http://localhost:8000/api",
        "https://api.github.com/users/octocat",
        "ftp://files.example.com/data.csv",
        "relative/path",
        "",  # Empty string should be allowed by pydantic
    ]

    for url in valid_urls:
        config = HTTPConfig(method="GET", url=url)
        assert config.url == url


def test_url_type_validation() -> None:
    """Test that url field rejects non-string types."""
    with pytest.raises(ValidationError) as exc_info:
        HTTPConfig(method="GET", url=123)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("url",)


@pytest.mark.parametrize(
    "params",
    [
        None,
        {},
        {
            "string_param": "value",
            "int_param": 42,
            "bool_param": True,
            "list_param": [1, 2, 3],
            "nested_dict": {"nested": "value"},
        },
    ],
)
def test_params_validation(params: dict | None) -> None:
    """Test that params field accepts dict or None."""
    config = HTTPConfig(method="GET", url="https://example.com", params=params)

    assert config.params == params


def test_params_type_validation() -> None:
    """Test that params field rejects non-dict types."""
    with pytest.raises(ValidationError) as exc_info:
        HTTPConfig(method="GET", url="https://example.com", params="invalid")

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("params",)


@pytest.mark.parametrize(
    "data",
    [
        None,
        {},
        {
            "username": "testuser",
            "password": "secret123",
            "remember": True,
        },
    ],
)
def test_data_optional_dict(data: dict | None) -> None:
    """Test that data field accepts dict or None."""
    config = HTTPConfig(method="POST", url="https://example.com", data=data)
    assert config.data == data


def test_data_type_validation() -> None:
    """Test that data field rejects non-dict types."""
    with pytest.raises(ValidationError):
        HTTPConfig(method="POST", url="https://example.com", data="form-data")


@pytest.mark.parametrize(
    "json_payload",
    [
        None,
        {},
        {
            "user": {
                "name": "John Doe",
                "email": "john@example.com",
                "preferences": {"theme": "dark", "notifications": True},
            },
            "metadata": ["tag1", "tag2", "tag3"],
        },
    ],
)
def test_json_data_optional_dict(json_payload: dict | None) -> None:
    """Test that json_data field accepts dict or None."""
    config = HTTPConfig(
        method="POST", url="https://example.com", json_data=json_payload
    )
    assert config.json_data == json_payload


def test_json_data_type_validation() -> None:
    """Test that json_data field rejects non-dict types."""
    with pytest.raises(ValidationError):
        HTTPConfig(method="POST", url="https://example.com", json_data="json string")


@pytest.mark.parametrize(
    "extra_headers",
    [
        None,
        {},
        {
            "Authorization": "Bearer abc123",
            "User-Agent": "MyApp/1.0",
            "Accept": "application/json",
            "X-Custom-Header": "custom-value",
        },
    ],
)
def test_extra_headers_optional_dict(extra_headers: dict | None) -> None:
    """Test that extra_headers field accepts dict or None."""
    config = HTTPConfig(
        method="GET", url="https://example.com", extra_headers=extra_headers
    )
    assert config.extra_headers == extra_headers


def test_extra_headers_type_validation() -> None:
    """Test that extra_headers field rejects non-dict types."""
    with pytest.raises(ValidationError):
        HTTPConfig(method="GET", url="https://example.com", extra_headers="headers")


def test_get_request_typical_usage() -> None:
    """Test typical GET request configuration."""
    config = HTTPConfig(
        method="GET",
        url="https://jsonplaceholder.typicode.com/posts",
        params={"userId": 1, "_limit": 10},
        extra_headers={"Accept": "application/json"},
    )

    assert config.method == "GET"
    assert config.url == "https://jsonplaceholder.typicode.com/posts"
    assert config.params == {"userId": 1, "_limit": 10}
    assert config.data is None
    assert config.json_data is None
    assert config.extra_headers == {"Accept": "application/json"}


def test_post_form_data_usage() -> None:
    """Test typical POST request with form data."""
    config = HTTPConfig(
        method="POST",
        url="https://httpbin.org/post",
        data={"key": "value", "file": "content"},
        extra_headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    assert config.method == "POST"
    assert config.url == "https://httpbin.org/post"
    assert config.params is None
    assert config.data == {"key": "value", "file": "content"}
    assert config.json_data is None
    assert config.extra_headers == {"Content-Type": "application/x-www-form-urlencoded"}


def test_post_json_data_usage() -> None:
    """Test typical POST request with JSON data."""
    config = HTTPConfig(
        method="POST",
        url="https://api.example.com/users",
        json_data={"name": "Alice", "email": "alice@example.com"},
        extra_headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer token",
        },
    )

    assert config.method == "POST"
    assert config.url == "https://api.example.com/users"
    assert config.params is None
    assert config.data is None
    assert config.json_data == {"name": "Alice", "email": "alice@example.com"}
    assert config.extra_headers == {
        "Content-Type": "application/json",
        "Authorization": "Bearer token",
    }


def test_multiple_validation_errors() -> None:
    """Test that multiple validation errors are caught."""
    with pytest.raises(ValidationError) as exc_info:
        HTTPConfig(
            method="PUT",  # Invalid method
            # Missing required url field
            params="invalid",  # Invalid params type
            data=123,  # Invalid data type
        )  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    # Should have multiple validation errors
    assert len(errors) >= 3

    error_fields = {error["loc"][0] for error in errors}
    assert "method" in error_fields
    assert "url" in error_fields
    assert "params" in error_fields or "data" in error_fields
