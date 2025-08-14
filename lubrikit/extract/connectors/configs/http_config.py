from typing import Any, Literal

from pydantic import BaseModel, Field


class HTTPConfig(BaseModel):
    method: Literal["GET", "POST"] = Field(
        description="HTTP method to use (GET or POST)"
    )
    url: str = Field(description="The URL to send the request to")
    params: dict[str, Any] | None = Field(
        default=None, description="Query parameters for the request"
    )
    data: dict[str, Any] | None = Field(
        default=None, description="Form data to send with the request"
    )
    json_data: dict[str, Any] | None = Field(
        default=None, description="JSON data to send with the request"
    )
    extra_headers: dict[str, Any] | None = Field(
        default=None, description="Additional headers to include in the request"
    )
