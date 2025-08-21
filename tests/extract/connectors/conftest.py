import pytest


@pytest.fixture
def retry_config() -> dict[str, int | float]:
    """Sample retry configuration."""
    return {
        "timeout": 5.0,
        "max_retries": 2,
        "base_delay": 0.5,
        "max_delay": 30.0,
        "backoff_factor": 2.0,
    }
