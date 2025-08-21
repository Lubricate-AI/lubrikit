from abc import ABC, abstractmethod


class Pipeline(ABC):
    @abstractmethod
    def run(self) -> None:
        """Runs the pipeline process."""
        ...
