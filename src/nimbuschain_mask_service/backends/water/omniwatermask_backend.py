from __future__ import annotations

from typing import Any

from nimbuschain_mask_service.omniwater import apply_omniwatermask_to_zarr, omniwater_support_status

NAME = "omniwatermask"
KIND = "water"


def available() -> bool:
    return bool(omniwater_support_status().get("available"))


def run(**kwargs: Any) -> dict[str, Any]:
    return apply_omniwatermask_to_zarr(runtime_preference="model", **kwargs)
