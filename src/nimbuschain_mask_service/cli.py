from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from nimbuschain_shared.clients.mask import MaskServiceClient
from nimbuschain_mask_service.zarr_context import open_zarr_group, read_context
from nimbuschain_shared.zarr import ConversionError


def _json_default(value: Any) -> Any:
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _load_zarr_metadata(zarr_uri: str) -> dict[str, Any]:
    root = open_zarr_group(zarr_uri, mode="r")
    context = read_context(root, zarr_uri=zarr_uri)
    attrs = dict(root.attrs)
    return {
        "provider": context.provider,
        "collection": context.collection,
        "product_type": context.product_type,
        "scene_id": context.scene_id,
        "dataset_summary": {
            "band_names": list(context.band_names),
            "shape": list(context.imagery_shape),
            "dimensions": list(attrs.get("dimensions") or ["time", "band", "y", "x"]),
            "crs": attrs.get("crs"),
            "transform": attrs.get("transform"),
            "data_family": attrs.get("data_family"),
            "zarr_uri": zarr_uri,
        },
        "acquisition_datetime": attrs.get("acquisition_datetime"),
    }


def _mask_types(raw_value: str) -> list[str]:
    values = [item.strip().lower() for item in str(raw_value or "").split(",") if item.strip()]
    if not values:
        raise ValueError("At least one mask type is required.")
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply cloud and/or water masks to an existing local scene Zarr store."
    )
    parser.add_argument("source_zarr_uri", help="Local path to the source Zarr store.")
    parser.add_argument("--output-zarr-uri", default=None)
    parser.add_argument("--mask-types", default="water", help="Comma-separated: water,cloud")
    parser.add_argument("--provider", default=None)
    parser.add_argument("--collection", default=None)
    parser.add_argument("--product-type", default=None)
    parser.add_argument("--scene-id", default=None)
    parser.add_argument("--service-url", required=True, help="Remote mask service base URL.")
    parser.add_argument("--cloud-backend", default="auto", choices=["auto", "omnicloudmask"])
    parser.add_argument("--cloud-threshold", type=float, default=0.45)
    parser.add_argument("--cloud-overwrite", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--cloud-device", default=None)
    parser.add_argument("--include-shadows", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--water-backend", default="auto", choices=["auto", "heuristic", "omniwatermask", "fallback", "ndwi"])
    parser.add_argument("--water-overwrite", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--water-device", default=None)
    parser.add_argument("--fail-on-error", action="store_true")
    return parser


def run(args: argparse.Namespace) -> int:
    metadata = _load_zarr_metadata(str(args.source_zarr_uri).strip())
    provider = str(args.provider or metadata.get("provider") or "").strip()
    collection = str(args.collection or metadata.get("collection") or "").strip()
    scene_id = str(args.scene_id or metadata.get("scene_id") or "").strip()
    if not provider or not collection or not scene_id:
        raise ConversionError(
            "Mask CLI requires provider, collection, and scene_id. "
            "Either pass them explicitly or ensure the source Zarr attrs include them."
        )

    client = MaskServiceClient(service_url=str(args.service_url or "").strip())
    try:
        result = client.apply_masks_to_zarr(
            zarr_uri=str(args.source_zarr_uri).strip(),
            output_zarr_uri=str(args.output_zarr_uri).strip() or None if args.output_zarr_uri else None,
            provider=provider,
            collection=collection,
            product_type=str(args.product_type or metadata.get("product_type") or "").strip() or None,
            scene_id=scene_id,
            acquisition_datetime=metadata.get("acquisition_datetime"),
            dataset_summary=dict(metadata.get("dataset_summary") or {}),
            mask_types=_mask_types(args.mask_types),
            fail_on_error=bool(args.fail_on_error),
            backend=str(args.cloud_backend).strip().lower(),
            threshold=float(args.cloud_threshold),
            overwrite=bool(args.cloud_overwrite),
            inference_device=str(args.cloud_device).strip() or None if args.cloud_device else None,
            include_shadows=bool(args.include_shadows),
            water_backend=str(args.water_backend).strip().lower(),
            water_overwrite=bool(args.water_overwrite),
            water_inference_device=str(args.water_device).strip() or None if args.water_device else None,
        )
    finally:
        client.close()

    print(json.dumps(result, default=_json_default))
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        code = run(args)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        sys.exit(1)
    sys.exit(code)


if __name__ == "__main__":
    main()
