import pytest
from pydantic import ValidationError

from lubrikit.utils.retry import RetryConfig


@pytest.mark.parametrize(
    "field_name, value",
    [
        ("timeout", 10.0),
        ("max_retries", 3),
        ("base_delay", 1.0),
        ("max_delay", 60.0),
        ("backoff_factor", 2.0),
    ],
)
def test_default_values(field_name: str, value: int | float) -> None:
    """Test that RetryConfig creates with expected default values."""
    config = RetryConfig()

    assert getattr(config, field_name) == value


@pytest.mark.parametrize(
    "field_name, value",
    [
        ("timeout", 30.0),
        ("max_retries", 5),
        ("base_delay", 2.0),
        ("max_delay", 120.0),
        ("backoff_factor", 3.0),
    ],
)
def test_custom_values(field_name: str, value: int | float) -> None:
    """Test that RetryConfig accepts custom values."""
    config = RetryConfig(**{field_name: value})

    assert getattr(config, field_name) == value


@pytest.mark.parametrize("field_name", ["base_delay", "max_delay", "backoff_factor"])
def test_zero_values(field_name: str) -> None:
    """Test edge case with zero values - only allowed for ge fields."""
    # These fields allow zero (ge=0.0)
    config = RetryConfig(**{field_name: 0.0})

    assert getattr(config, field_name) == 0.0


@pytest.mark.parametrize("field_name", ["timeout", "max_retries"])
def test_zero_constraints(field_name: str) -> None:
    # timeout and max_retries should fail with zero (gt constraint)
    with pytest.raises(ValidationError) as exc_info:
        RetryConfig(**{field_name: 0.0})

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == (field_name,)
    assert "greater_than" in errors[0]["type"]


@pytest.mark.parametrize(
    "field_name, value, constraint_type",
    [
        ("timeout", -1.0, "greater_than"),
        ("max_retries", -1, "greater_than"),
        ("base_delay", -1.0, "greater_than_equal"),
        ("max_delay", -1.0, "greater_than_equal"),
        ("backoff_factor", -1.0, "greater_than_equal"),
    ],
)
def test_negative_values(
    field_name: str, value: int | float, constraint_type: str
) -> None:
    """Test that negative values are rejected due to validation constraints."""
    # All fields should reject negative values
    with pytest.raises(ValidationError) as exc_info:
        RetryConfig(**{field_name: value})

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == (field_name,)
    assert constraint_type in errors[0]["type"]


@pytest.mark.parametrize(
    "field_name",
    ["timeout", "max_retries", "base_delay", "max_delay", "backoff_factor"],
)
def test_large_values(field_name: str) -> None:
    """Test with very large values."""
    config = RetryConfig(**{field_name: 999999})

    assert getattr(config, field_name) == 999999


@pytest.mark.parametrize(
    "field_name, value",
    [
        ("timeout", 10),
        ("timeout", 10.0),
    ],
)
def test_type_validation_timeout(field_name: str, value: int | float) -> None:
    """Test that timeout field validates type correctly."""
    # Valid types
    config = RetryConfig(**{field_name: value})

    assert getattr(config, field_name) == float(value)


def test_type_validation_timeout_raises() -> None:
    # Invalid types should raise ValidationError
    with pytest.raises(ValidationError) as exc_info:
        RetryConfig(timeout="invalid")

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("timeout",)
    assert "float_parsing" in errors[0]["type"] or "input_should_be" in errors[0]["msg"]


def test_type_validation_max_retries() -> None:
    """Test that max_retries field validates type correctly."""
    # Valid types
    config = RetryConfig(max_retries=5)  # int should work
    assert config.max_retries == 5

    # Invalid types should raise ValidationError
    with pytest.raises(ValidationError):
        RetryConfig(max_retries=5.5)  # float should not work for int field

    with pytest.raises(ValidationError):
        RetryConfig(max_retries="invalid")


def test_type_validation_delays() -> None:
    """Test that delay fields validate type correctly."""
    # Valid types
    config = RetryConfig(base_delay=1, max_delay=60)  # int should work
    assert config.base_delay == 1.0
    assert config.max_delay == 60.0

    # Invalid types
    with pytest.raises(ValidationError):
        RetryConfig(base_delay="invalid")

    with pytest.raises(ValidationError):
        RetryConfig(max_delay="invalid")


def test_type_validation_backoff_factor() -> None:
    """Test that backoff_factor field validates type correctly."""
    # Valid types
    config = RetryConfig(backoff_factor=2)  # int should work
    assert config.backoff_factor == 2.0

    config = RetryConfig(backoff_factor=2.5)  # float should work
    assert config.backoff_factor == 2.5

    # Invalid types
    with pytest.raises(ValidationError):
        RetryConfig(backoff_factor="invalid")


def test_unknown_field_error() -> None:
    """Test that unknown fields raise ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        RetryConfig(unknown_field="value")  # type: ignore[call-arg]

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert "unknown_field" in str(errors[0])
