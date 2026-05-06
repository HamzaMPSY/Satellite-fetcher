from __future__ import annotations

__all__ = [
    "MaskServiceClient",
    "Sen2LikeServiceClient",
    "ZarrServiceClient",
]


def __getattr__(name: str):
    if name == "MaskServiceClient":
        from nimbuschain_shared.clients.mask import MaskServiceClient

        return MaskServiceClient
    if name == "Sen2LikeServiceClient":
        from nimbuschain_shared.clients.sen2like import Sen2LikeServiceClient

        return Sen2LikeServiceClient
    if name == "ZarrServiceClient":
        from nimbuschain_shared.clients.zarr import ZarrServiceClient

        return ZarrServiceClient
    raise AttributeError(name)
