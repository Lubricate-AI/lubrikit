from pydantic import BaseModel, ConfigDict, Field


class RetryConfig(BaseModel):
    """Configuration for retry behavior in HTTP requests."""

    model_config = ConfigDict(extra="forbid")

    timeout: float = Field(
        default=10.0, gt=0.0, description="Request timeout in seconds"
    )
    max_retries: int = Field(
        default=3, gt=0, description="Maximum number of retry attempts"
    )
    base_delay: float = Field(
        default=1.0, ge=0.0, description="Initial delay between retries in seconds"
    )
    max_delay: float = Field(
        default=60.0, ge=0.0, description="Maximum delay between retries in seconds"
    )
    backoff_factor: float = Field(
        default=2.0, ge=0.0, description="Multiplier for exponential backoff"
    )
