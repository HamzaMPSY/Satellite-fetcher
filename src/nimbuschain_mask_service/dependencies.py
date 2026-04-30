from __future__ import annotations

from fastapi import HTTPException, Request, status

from nimbuschain_mask_service.service import MaskService


def get_mask_service(request: Request) -> MaskService:
    service = getattr(request.app.state, "mask_service", None)
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Mask service is not ready.",
        )
    return service


__all__ = ["get_mask_service"]
