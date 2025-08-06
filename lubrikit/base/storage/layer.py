from __future__ import annotations

import os
from enum import Enum


class Layer(str, Enum):
    """Data Lake layers enumerated.

    Attributes:
        LANDING: Landing zone. All ingested data is stored here
            unaltered from the source.
        STAGING: Staging area. File-level processing steps are applied
            to data from the LANDING layer. Examples include:
                - Archive extraction (i.e. unzipping)
                - OCR (object character recognition)
        BRONZE: Tabular representation of the raw data.
        SILVER: Cleaned, filtered, and modeled data.
        GOLD: Analytics-ready data.
    """

    LANDING = "landing"
    STAGING = "staging"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PRESENTATION = "presentation"

    @property
    def bucket(self) -> str:
        """Returns the bucket name for the layer."""
        env_var = f"AWS_{self.name.upper()}_BUCKET"
        return os.getenv(env_var, self.value)

    @property
    def next(self) -> Layer:
        """Returns the next layer in the data pipeline."""
        if self == Layer.LANDING:
            return Layer.STAGING
        elif self == Layer.STAGING:
            return Layer.BRONZE
        elif self == Layer.BRONZE:
            return Layer.SILVER
        elif self == Layer.SILVER:
            return Layer.GOLD
        elif self == Layer.GOLD:
            return Layer.PRESENTATION
        else:
            raise ValueError(f"Invalid layer: {self}")

    @property
    def previous(self) -> Layer:
        """Returns the previous layer in the data pipeline."""
        if self == Layer.STAGING:
            return Layer.LANDING
        elif self == Layer.BRONZE:
            return Layer.STAGING
        elif self == Layer.SILVER:
            return Layer.BRONZE
        elif self == Layer.GOLD:
            return Layer.SILVER
        elif self == Layer.PRESENTATION:
            return Layer.GOLD
        else:
            raise ValueError(f"Invalid layer: {self}")
