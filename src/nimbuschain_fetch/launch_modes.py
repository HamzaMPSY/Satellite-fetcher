from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class PipelineLaunchMode(str, Enum):
    mps = "mps"
    oci = "oci"


DEFAULT_HOST_MPS_MASK_PORT = "18021"
DEFAULT_HOST_MPS_SEN2LIKE_PORT = "18031"


@dataclass(frozen=True, slots=True)
class PipelineServiceDefaults:
    launch_mode: PipelineLaunchMode
    zarr_service_url: str
    mask_service_url: str
    sen2like_service_url: str
    stage_dir: str
    zarr_dir: str


def normalize_pipeline_launch_mode(value: str | None) -> PipelineLaunchMode:
    normalized = str(value or os.getenv("NIMBUS_PIPELINE_LAUNCH_MODE") or "mps").strip().lower()
    if normalized in {"mps", "local-mps", "local_mps", "ui"}:
        return PipelineLaunchMode.mps
    if normalized in {"oci", "cloud", "vm"}:
        return PipelineLaunchMode.oci
    raise ValueError("launch mode must be one of: mps, oci.")


def default_host_mps_mask_url(*, container: bool = False) -> str:
    host = "host.containers.internal" if container else "127.0.0.1"
    port = str(os.getenv("NIMBUS_HOST_MPS_MASK_PORT") or DEFAULT_HOST_MPS_MASK_PORT).strip()
    return f"http://{host}:{port}"


def default_host_mps_sen2like_url(*, container: bool = False) -> str:
    host = "host.containers.internal" if container else "127.0.0.1"
    port = str(os.getenv("NIMBUS_HOST_MPS_SEN2LIKE_PORT") or DEFAULT_HOST_MPS_SEN2LIKE_PORT).strip()
    return f"http://{host}:{port}"


def service_defaults(
    launch_mode: str | PipelineLaunchMode | None,
    *,
    container: bool = False,
) -> PipelineServiceDefaults:
    mode = (
        launch_mode
        if isinstance(launch_mode, PipelineLaunchMode)
        else normalize_pipeline_launch_mode(launch_mode)
    )
    if mode is PipelineLaunchMode.mps:
        return PipelineServiceDefaults(
            launch_mode=mode,
            zarr_service_url=_env(
                "NIMBUS_ZARR_SERVICE_URL",
                _default_service_url("nimbus-zarr", "8010", container=container),
            ),
            mask_service_url=_env(
                "NIMBUS_HOST_MPS_MASK_URL",
                default_host_mps_mask_url(container=container),
            ),
            sen2like_service_url=_env(
                "NIMBUS_HOST_MPS_SEN2LIKE_URL",
                default_host_mps_sen2like_url(container=container),
            ),
            stage_dir=_env("NIMBUS_PIPELINE_STAGE_DIR", "./data/downloads/staged"),
            zarr_dir=_env("NIMBUS_PIPELINE_ZARR_DIR", "./data/downloads/zarr"),
        )

    return PipelineServiceDefaults(
        launch_mode=mode,
        zarr_service_url=_env(
            "NIMBUS_ZARR_SERVICE_URL",
            _default_service_url("nimbus-zarr", "8010", container=container),
        ),
        mask_service_url=_env(
            "NIMBUS_MASK_SERVICE_URL",
            _default_service_url("nimbus-mask", "8020", container=container),
        ),
        sen2like_service_url=_env(
            "NIMBUS_SEN2LIKE_SERVICE_URL",
            _default_service_url("nimbus-sen2like", "8030", container=container),
        ),
        stage_dir=_env("NIMBUS_PIPELINE_STAGE_DIR", "/data/downloads/staged"),
        zarr_dir=_env("NIMBUS_PIPELINE_ZARR_DIR", "/data/downloads/zarr"),
    )


def _default_service_url(service_name: str, port: str, *, container: bool = False) -> str:
    host = service_name if container else "127.0.0.1"
    return f"http://{host}:{port}"


def _env(name: str, fallback: str) -> str:
    value = str(os.getenv(name) or "").strip()
    return value or fallback


__all__ = [
    "DEFAULT_HOST_MPS_MASK_PORT",
    "DEFAULT_HOST_MPS_SEN2LIKE_PORT",
    "PipelineLaunchMode",
    "PipelineServiceDefaults",
    "default_host_mps_mask_url",
    "default_host_mps_sen2like_url",
    "normalize_pipeline_launch_mode",
    "service_defaults",
]
