from __future__ import annotations

import importlib.util
import sys
from typing import Any

from nimbuschain_mask_service.inference import CloudMaskResult, run_omnicloudmask_cloud_inference
from nimbuschain_mask_service.sensor_mapping import SensorMaskSpec

NAME = "omnicloudmask"
KIND = "cloud"


def available() -> bool:
    return "omnicloudmask" in sys.modules or importlib.util.find_spec("omnicloudmask") is not None


def required_bands(sensor: SensorMaskSpec) -> tuple[str, ...]:
    return sensor.cloud_rgbnir_bands


def normalize_inputs(sensor: SensorMaskSpec) -> bool:
    return sensor.sensor_key.startswith("landsat-")


def run(
    *,
    sensor: SensorMaskSpec,
    channels: dict[str, Any],
    threshold: float,
    inference_device: str | None = None,
    include_shadows: bool = True,
) -> CloudMaskResult:
    return run_omnicloudmask_cloud_inference(
        sensor=sensor,
        channels=channels,
        threshold=threshold,
        inference_device=inference_device,
        include_shadows=include_shadows,
    )
