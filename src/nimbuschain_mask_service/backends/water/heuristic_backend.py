from __future__ import annotations

from typing import Any

from nimbuschain_mask_service.omniwater import apply_omniwatermask_to_zarr

NAME = "heuristic"
KIND = "water"


def available() -> bool:
    return True


def run(**kwargs: Any) -> dict[str, Any]:
    return apply_omniwatermask_to_zarr(runtime_preference="fallback", **kwargs)
