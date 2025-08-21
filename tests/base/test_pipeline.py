import pytest

from lubrikit.base import Pipeline


def test_pipeline_is_abstract_base_class() -> None:
    """Test that Pipeline is an abstract base class."""
    # Pipeline cannot be instantiated directly due to abstract run method
    with pytest.raises(TypeError, match="Can't instantiate abstract class Pipeline"):
        Pipeline()  # type: ignore[abstract]


def test_pipeline_inheritance() -> None:
    """Test that Pipeline can be properly inherited."""

    class ConcretePipeline(Pipeline):
        def run(self) -> None:
            pass

    # Should be able to instantiate a concrete implementation
    pipeline = ConcretePipeline()
    assert isinstance(pipeline, Pipeline)


def test_pipeline_has_abc_base() -> None:
    """Test that Pipeline inherits from ABC."""
    from abc import ABC

    assert issubclass(Pipeline, ABC)


def test_pipeline_concrete_implementation() -> None:
    """Test a concrete Pipeline implementation."""

    class TestPipeline(Pipeline):
        def __init__(self) -> None:
            self.executed = False

        def run(self) -> None:
            self.executed = True

    pipeline = TestPipeline()
    assert not pipeline.executed

    pipeline.run()
    assert pipeline.executed
