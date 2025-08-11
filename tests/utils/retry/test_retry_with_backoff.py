import logging
import time
from unittest.mock import MagicMock, Mock, patch

import pytest

from lubrikit.utils.retry.retry_with_backoff import retry_with_backoff


def test_successful_execution_no_retry() -> None:
    """Test that successful function executes without retries."""
    mock_func = Mock(return_value="success")
    decorated_func = retry_with_backoff()(mock_func)

    result = decorated_func("arg1", kwarg1="kwarg1")

    assert result == "success"
    mock_func.assert_called_once_with("arg1", kwarg1="kwarg1")


def test_successful_execution_after_retries() -> None:
    """Test that function succeeds after some retries."""
    mock_func = Mock(side_effect=[ValueError("fail"), ValueError("fail"), "success"])

    with patch("time.sleep"):
        decorated_func = retry_with_backoff(max_retries=3)(mock_func)
        result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 3


def test_max_retries_exceeded() -> None:
    """Test that function raises exception after max retries exceeded."""
    mock_func = Mock(side_effect=ValueError("persistent error"))

    with patch("time.sleep"):
        decorated_func = retry_with_backoff(max_retries=2)(mock_func)

        with pytest.raises(ValueError, match="persistent error"):
            decorated_func()

    # Should be called max_retries + 1 times (initial + retries)
    assert mock_func.call_count == 3


def test_exponential_backoff_timing() -> None:
    """Test that exponential backoff calculates delays correctly."""
    mock_func = Mock(
        side_effect=[ValueError("fail"), ValueError("fail"), ValueError("fail")]
    )

    with patch("time.sleep") as mock_sleep:
        decorated_func = retry_with_backoff(
            max_retries=2,
            base_delay=1.0,
            backoff_factor=2.0,
            jitter=False,  # Disable jitter for predictable timing
        )(mock_func)

        with pytest.raises(ValueError):
            decorated_func()

    # Check sleep was called with correct delays
    expected_delays = [
        1.0,  # base_delay * (backoff_factor ** 0) = 1.0 * 1 = 1.0
        2.0,  # base_delay * (backoff_factor ** 1) = 1.0 * 2 = 2.0
    ]

    sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
    assert sleep_calls == expected_delays


def test_max_delay_cap() -> None:
    """Test that delays are capped at max_delay."""
    mock_func = Mock(side_effect=[ValueError("fail"), ValueError("fail")])

    with patch("time.sleep") as mock_sleep:
        decorated_func = retry_with_backoff(
            max_retries=1,
            base_delay=10.0,
            backoff_factor=10.0,
            max_delay=5.0,  # Cap lower than calculated delay
            jitter=False,
        )(mock_func)

        with pytest.raises(ValueError):
            decorated_func()

    # Calculated delay would be 10.0 * 10.0 = 100.0, but should be capped at 5.0
    mock_sleep.assert_called_once_with(5.0)


def test_jitter_adds_randomness() -> None:
    """Test that jitter adds random variation to delays."""
    mock_func = Mock(side_effect=[ValueError("fail"), ValueError("fail")])

    with patch("time.sleep") as mock_sleep, patch("random.uniform") as mock_uniform:
        mock_uniform.return_value = 0.05  # 5% jitter

        decorated_func = retry_with_backoff(
            max_retries=1, base_delay=1.0, backoff_factor=1.0, jitter=True
        )(mock_func)

        with pytest.raises(ValueError):
            decorated_func()

    # Should call random.uniform with (0, delay * 0.1)
    mock_uniform.assert_called_once_with(0, 0.1)  # 1.0 * 0.1 = 0.1
    # Sleep should be called with base_delay + jitter = 1.0 + 0.05 = 1.05
    mock_sleep.assert_called_once_with(1.05)


def test_no_jitter() -> None:
    """Test that jitter can be disabled."""
    mock_func = Mock(side_effect=[ValueError("fail"), ValueError("fail")])

    with patch("time.sleep") as mock_sleep, patch("random.uniform") as mock_uniform:
        decorated_func = retry_with_backoff(
            max_retries=1, base_delay=2.0, jitter=False
        )(mock_func)

        with pytest.raises(ValueError):
            decorated_func()

    # random.uniform should not be called when jitter is False
    mock_uniform.assert_not_called()
    # Sleep should be called with exact base_delay
    mock_sleep.assert_called_once_with(2.0)


def test_custom_retriable_exceptions() -> None:
    """Test that only specified exceptions trigger retries."""
    mock_func = Mock(side_effect=[ValueError("retriable"), "success"])

    with patch("time.sleep"):
        decorated_func = retry_with_backoff(
            max_retries=2, retriable_exceptions=(ValueError,)
        )(mock_func)

        result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 2


def test_non_retriable_exception_not_retried() -> None:
    """Test that non-retriable exceptions are not retried."""
    mock_func = Mock(side_effect=TypeError("not retriable"))

    decorated_func = retry_with_backoff(
        max_retries=3,
        retriable_exceptions=(ValueError,),  # TypeError not in list
    )(mock_func)

    with pytest.raises(TypeError, match="not retriable"):
        decorated_func()

    # Should only be called once (no retries)
    mock_func.assert_called_once()


def test_default_retriable_exceptions() -> None:
    """Test that default retriable_exceptions catches all Exceptions."""
    mock_func = Mock(side_effect=[RuntimeError("fail"), "success"])

    with patch("time.sleep"):
        decorated_func = retry_with_backoff(max_retries=1)(mock_func)
        result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 2


def test_zero_max_retries() -> None:
    """Test behavior with zero max_retries (no retries)."""
    mock_func = Mock(side_effect=ValueError("immediate fail"))

    decorated_func = retry_with_backoff(max_retries=0)(mock_func)

    with pytest.raises(ValueError, match="immediate fail"):
        decorated_func()

    # Should only be called once (no retries allowed)
    mock_func.assert_called_once()


def test_zero_base_delay() -> None:
    """Test behavior with zero base_delay."""
    mock_func = Mock(side_effect=[ValueError("fail"), "success"])

    with patch("time.sleep") as mock_sleep:
        decorated_func = retry_with_backoff(
            max_retries=1, base_delay=0.0, jitter=False
        )(mock_func)

        result = decorated_func()

    assert result == "success"
    mock_sleep.assert_called_once_with(0.0)


def test_preserves_function_metadata() -> None:
    """Test that decorator preserves original function metadata."""

    def original_func(x: int) -> str:
        """Original function docstring."""
        return str(x)

    decorated_func = retry_with_backoff()(original_func)

    assert decorated_func.__name__ == "original_func"
    assert decorated_func.__doc__ == "Original function docstring."


def test_logging_on_retry(caplog: pytest.LogCaptureFixture) -> None:
    """Test that retry attempts are logged correctly."""
    mock_func = Mock(side_effect=[ValueError("fail"), "success"])

    with patch("time.sleep"), caplog.at_level(logging.WARNING):
        decorated_func = retry_with_backoff(max_retries=1)(mock_func)
        decorated_func()

    # Should log the retry attempt
    assert len(caplog.records) == 1
    assert "Attempt 1/2 failed" in caplog.records[0].message
    assert "fail" in caplog.records[0].message
    assert "Retrying in" in caplog.records[0].message


def test_logging_on_final_failure(caplog: pytest.LogCaptureFixture) -> None:
    """Test that final failure is logged correctly."""
    mock_func = Mock(side_effect=ValueError("persistent fail"))

    with patch("time.sleep"), caplog.at_level(logging.WARNING):
        decorated_func = retry_with_backoff(max_retries=1)(mock_func)

        with pytest.raises(ValueError):
            decorated_func()

    # Should log the retry attempt (WARNING) and final failure (ERROR)
    assert len(caplog.records) == 2  # 1 warning + 1 error

    # First record should be the retry warning
    warning_record = caplog.records[0]
    assert warning_record.levelname == "WARNING"
    assert "Attempt 1/2 failed" in warning_record.message

    # Second record should be the final error
    error_record = caplog.records[1]
    assert error_record.levelname == "ERROR"
    assert "All 2 attempts failed" in error_record.message
    assert "persistent fail" in error_record.message


def test_unexpected_exception_handling(caplog: pytest.LogCaptureFixture) -> None:
    """Test handling of unexpected exceptions (non-retriable)."""
    mock_func = Mock(side_effect=TypeError("unexpected"))

    with caplog.at_level(logging.ERROR):
        decorated_func = retry_with_backoff(
            max_retries=2,
            retriable_exceptions=(ValueError,),  # TypeError not retriable
        )(mock_func)

        with pytest.raises(TypeError, match="unexpected"):
            decorated_func()

    # Should log the unexpected error
    assert len(caplog.records) == 1
    assert "Unexpected error: unexpected" in caplog.records[0].message


def test_complex_exponential_backoff_sequence() -> None:
    """Test complex exponential backoff sequence."""
    mock_func = Mock(
        side_effect=[
            ValueError("fail1"),
            ValueError("fail2"),
            ValueError("fail3"),
            ValueError("fail4"),
        ]
    )

    with patch("time.sleep") as mock_sleep:
        decorated_func = retry_with_backoff(
            max_retries=3,
            base_delay=0.5,
            backoff_factor=3.0,
            max_delay=10.0,
            jitter=False,
        )(mock_func)

        with pytest.raises(ValueError):
            decorated_func()

    # Calculate expected delays: base_delay * (backoff_factor ** attempt)
    expected_delays = [
        0.5,  # 0.5 * (3.0 ** 0) = 0.5
        1.5,  # 0.5 * (3.0 ** 1) = 1.5
        4.5,  # 0.5 * (3.0 ** 2) = 4.5
    ]

    actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
    assert actual_delays == expected_delays


def test_max_delay_with_high_backoff() -> None:
    """Test that max_delay properly caps very high exponential values."""
    mock_func = Mock(side_effect=[ValueError("fail"), ValueError("fail")])

    with patch("time.sleep") as mock_sleep:
        decorated_func = retry_with_backoff(
            max_retries=1,
            base_delay=100.0,
            backoff_factor=100.0,  # Would calculate to 10000.0
            max_delay=2.0,  # Much lower cap
            jitter=False,
        )(mock_func)

        with pytest.raises(ValueError):
            decorated_func()

    # Should be capped at max_delay
    mock_sleep.assert_called_once_with(2.0)


@patch("time.sleep")
def test_actual_timing_behavior(mock_sleep: MagicMock) -> None:
    """Test that the decorator actually calls time.sleep with correct timing."""
    call_times = []

    def timing_func() -> str:
        call_times.append(time.time())
        if len(call_times) <= 2:
            raise ValueError("fail")
        return "success"

    decorated_func = retry_with_backoff(max_retries=2, base_delay=0.1, jitter=False)(
        timing_func
    )

    result = decorated_func()

    assert result == "success"
    assert mock_sleep.call_count == 2
    # Verify sleep was called with increasing delays
    assert mock_sleep.call_args_list[0][0][0] == 0.1
    assert mock_sleep.call_args_list[1][0][0] == 0.2  # 0.1 * 2.0


def test_function_with_return_annotations() -> None:
    """Test decorator works with functions that have type annotations."""

    @retry_with_backoff(max_retries=1)
    def typed_function(x: int, y: str = "default") -> str:
        if x < 0:
            raise ValueError("negative value")
        return f"{x}-{y}"

    # Test successful execution
    result = typed_function(5)
    assert result == "5-default"

    result = typed_function(3, "test")
    assert result == "3-test"

    # Test retry behavior
    with patch("time.sleep"):
        with pytest.raises(ValueError):
            typed_function(-1)


def test_multiple_exception_types() -> None:
    """Test that multiple exception types are retriable."""
    mock_func = Mock(
        side_effect=[ValueError("fail1"), RuntimeError("fail2"), "success"]
    )

    with patch("time.sleep"):
        decorated_func = retry_with_backoff(
            max_retries=3, retriable_exceptions=(ValueError, RuntimeError)
        )(mock_func)

        result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 3


def test_baseexception_not_caught_by_default() -> None:
    """Test that BaseException subclasses are not retried."""
    mock_func = Mock(side_effect=KeyboardInterrupt("user interrupt"))

    decorated_func = retry_with_backoff(max_retries=2)(mock_func)

    # KeyboardInterrupt should not be caught by default Exception handling
    with pytest.raises(KeyboardInterrupt):
        decorated_func()

    # Should only be called once (KeyboardInterrupt caught by second except block)
    mock_func.assert_called_once()


def test_custom_baseexception_retriable() -> None:
    """Test that BaseException can be made retriable if explicitly specified."""
    mock_func = Mock(side_effect=[KeyboardInterrupt("interrupt1"), "success"])

    with patch("time.sleep"):
        decorated_func = retry_with_backoff(
            max_retries=1, retriable_exceptions=(KeyboardInterrupt,)
        )(mock_func)

        result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 2


def test_parameter_defaults() -> None:
    """Test that decorator works with default parameters."""
    mock_func = Mock(side_effect=[ValueError("fail"), "success"])

    with patch("time.sleep") as mock_sleep:
        # Use decorator with no parameters (all defaults)
        decorated_func = retry_with_backoff()(mock_func)
        result = decorated_func()

    assert result == "success"
    assert mock_func.call_count == 2
    # Should have called sleep once with default timing
    assert mock_sleep.call_count == 1


def test_edge_case_negative_delays() -> None:
    """Test behavior with negative delays (should not cause issues)."""
    mock_func = Mock(side_effect=[ValueError("fail"), "success"])

    with patch("time.sleep") as mock_sleep:
        decorated_func = retry_with_backoff(
            max_retries=1,
            base_delay=-1.0,  # Negative delay
            jitter=False,
        )(mock_func)

        result = decorated_func()

    assert result == "success"
    # time.sleep should still be called with the negative value
    mock_sleep.assert_called_once_with(-1.0)


def test_very_small_delays() -> None:
    """Test behavior with very small delays."""
    mock_func = Mock(side_effect=[ValueError("fail"), "success"])

    with patch("time.sleep") as mock_sleep:
        decorated_func = retry_with_backoff(
            max_retries=1, base_delay=0.001, jitter=False
        )(mock_func)

        result = decorated_func()

    assert result == "success"
    mock_sleep.assert_called_once_with(0.001)
