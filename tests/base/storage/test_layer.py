import pytest

from lubrikit.base.storage.layer import Layer


@pytest.mark.parametrize(
    "member, value",
    [
        (Layer.LANDING, "landing"),
        (Layer.STAGING, "staging"),
        (Layer.BRONZE, "bronze"),
        (Layer.SILVER, "silver"),
        (Layer.GOLD, "gold"),
        (Layer.PRESENTATION, "presentation"),
    ],
)
def test_enum_members(member: Layer, value: str) -> None:
    assert member.value == value
    assert member == value


@pytest.mark.parametrize(
    "name",
    [
        "LANDING",
        "STAGING",
        "BRONZE",
        "SILVER",
        "GOLD",
        "PRESENTATION",
    ],
)
def test_enum_names(name: str) -> None:
    assert name in Layer.__members__, f"Layer does not have member {name}"


def test_enum_unique_values() -> None:
    values = [member.value for member in Layer]
    assert len(values) == len(set(values)), "Layer values are not unique"


@pytest.fixture
def bucket_mapping() -> dict[Layer, str]:
    return {
        Layer.LANDING: "AWS_LANDING_BUCKET",
        Layer.STAGING: "AWS_STAGING_BUCKET",
        Layer.BRONZE: "AWS_BRONZE_BUCKET",
        Layer.SILVER: "AWS_SILVER_BUCKET",
        Layer.GOLD: "AWS_GOLD_BUCKET",
        Layer.PRESENTATION: "AWS_PRESENTATION_BUCKET",
    }


@pytest.mark.parametrize(
    "layer, bucket, use_env_var",
    [
        (Layer.LANDING, "landing", False),
        (Layer.LANDING, "landing-bucket", True),
        (Layer.STAGING, "staging", False),
        (Layer.STAGING, "staging-bucket", True),
        (Layer.BRONZE, "bronze", False),
        (Layer.BRONZE, "bronze-bucket", True),
        (Layer.SILVER, "silver", False),
        (Layer.SILVER, "silver-bucket", True),
        (Layer.GOLD, "gold", False),
        (Layer.GOLD, "gold-bucket", True),
        (Layer.PRESENTATION, "presentation", False),
        (Layer.PRESENTATION, "presentation-bucket", True),
    ],
)
def test_bucket(
    monkeypatch: pytest.MonkeyPatch,
    layer: Layer,
    bucket: str,
    use_env_var: bool,
    bucket_mapping: dict[Layer, str],
) -> None:
    if use_env_var:
        monkeypatch.setenv(bucket_mapping[layer], bucket)

    assert layer.bucket == bucket


@pytest.mark.parametrize(
    "layer, next_layer",
    [
        (Layer.LANDING, Layer.STAGING),
        (Layer.STAGING, Layer.BRONZE),
        (Layer.BRONZE, Layer.SILVER),
        (Layer.SILVER, Layer.GOLD),
        (Layer.GOLD, Layer.PRESENTATION),
    ],
)
def test_next_layer(layer: Layer, next_layer: Layer) -> None:
    assert layer.next == next_layer


def test_next_layer_raises() -> None:
    with pytest.raises(ValueError):
        _ = Layer.PRESENTATION.next


@pytest.mark.parametrize(
    "layer, previous_layer",
    [
        (Layer.STAGING, Layer.LANDING),
        (Layer.BRONZE, Layer.STAGING),
        (Layer.SILVER, Layer.BRONZE),
        (Layer.GOLD, Layer.SILVER),
        (Layer.PRESENTATION, Layer.GOLD),
    ],
)
def test_previous_layer(layer: Layer, previous_layer: Layer) -> None:
    assert layer.previous == previous_layer


def test_previous_layer_raises() -> None:
    with pytest.raises(ValueError):
        _ = Layer.LANDING.previous
