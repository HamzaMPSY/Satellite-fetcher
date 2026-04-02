from __future__ import annotations

from typing import Any

from nimbuschain_mask_service.inference import CloudMaskResult, run_heuristic_cloud_inference
from nimbuschain_mask_service.sensor_mapping import SensorMaskSpec

NAME = "heuristic"
KIND = "cloud"


def available() -> bool:
    return True


def required_bands(sensor: SensorMaskSpec) -> tuple[str, ...]:
    return sensor.cloud_required_bands


def normalize_inputs(sensor: SensorMaskSpec) -> bool:
    return True


def run(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, Any],
    threshold: float,
    inference_device: str | None = None,
    include_shadows: bool = True,
) -> CloudMaskResult:
    del inference_device
    return run_heuristic_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=threshold,
        include_shadows=include_shadows,
    )
