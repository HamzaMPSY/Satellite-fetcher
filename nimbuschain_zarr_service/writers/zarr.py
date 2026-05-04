from __future__ import annotations

from nimbuschain_zarr_service.core import write_dataset_to_zarr


class ZarrWriter:
    def write(self, dataset, output_uri: str) -> str:
        return write_dataset_to_zarr(dataset, output_uri)
