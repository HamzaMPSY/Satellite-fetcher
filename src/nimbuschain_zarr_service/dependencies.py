from __future__ import annotations

from nimbuschain_zarr_service.service import ZarrConversionService


_CONVERSION_SERVICE = ZarrConversionService()


def get_conversion_service() -> ZarrConversionService:
    return _CONVERSION_SERVICE
