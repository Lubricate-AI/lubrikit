from typing import Any, Literal

from pydantic import BaseModel, Field


class HTTPConfig(BaseModel):
    """Configuration for HTTP connector requests.

    This class defines the configuration parameters required to make
    HTTP requests through the HTTP connector. It supports both GET and
    POST methods with various data formats and headers. Uses Pydantic
    for data validation and type checking.

    Attributes:
        method (Literal["GET", "POST"]): The HTTP method to use for the
            request. Must be either "GET" or "POST". This is a required
            field.
        url (str): The target URL to send the HTTP request to. This is a
            required field and must be a valid URL string.
            Example: "https://api.example.com/data"
        params (dict[str, Any] | None): Optional query parameters to
            append to the URL. These will be URL-encoded and appended as
            a query string.
            Example: {"key": "value", "limit": 10}
            Default: None
        data (dict[str, Any] | None): Optional form data to send with
            the request. This is typically used for POST requests with
            form-encoded data. Cannot be used together with json_data.
            Example: {"username": "user", "password": "pass"}
            Default: None
        json_data (dict[str, Any] | None): Optional JSON data to send
            with the request. This is typically used for POST requests
            with JSON payloads. Cannot be used together with data.
            Example: {"name": "John", "age": 30}
            Default: None
        extra_headers (dict[str, Any] | None): Optional additional
            headers to include in the HTTP request. These will be merged
            with default headers.
            Example: {"Authorization": "Bearer token", "User-Agent": "MyApp/1.0"}
            Default: None

    Example:
        GET request with query parameters:
        >>> config = HTTPConfig(
        ...     method="GET",
        ...     url="https://api.example.com/users",
        ...     params={"page": 1, "limit": 50}
        ... )

        POST request with JSON data:
        >>> config = HTTPConfig(
        ...     method="POST",
        ...     url="https://api.example.com/users",
        ...     json_data={"name": "John", "email": "john@example.com"},
        ...     extra_headers={"Authorization": "Bearer token"}
        ... )
    """

    method: Literal["GET", "POST"] = Field(
        description="HTTP method to use for the request. Must be 'GET' or 'POST'."
    )

    url: str = Field(
        description="The target URL to send the HTTP request to. Must be a valid URL."
    )

    params: dict[str, Any] | None = Field(
        default=None,
        description="Optional query parameters to append to the URL as a query string. "
        "Example: {'key': 'value', 'limit': 10}",
    )

    data: dict[str, Any] | None = Field(
        default=None,
        description="Optional form data to send with the request (form-encoded). "
        "Typically used for POST requests. Cannot be used with json_data.",
    )

    json_data: dict[str, Any] | None = Field(
        default=None,
        description="Optional JSON data to send with the request body. "
        "Typically used for POST requests. Cannot be used with data.",
    )

    extra_headers: dict[str, Any] | None = Field(
        default=None,
        description="Optional additional headers to include in the HTTP request. "
        "These are merged with default headers.",
    )
