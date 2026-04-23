from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from nimbuschain_mask_service.client import MaskServiceClient
from nimbuschain_zarr_service.cube import build_grouped_time_cubes, build_time_cube
from nimbuschain_zarr_service.oci_storage import OCIStorageError, OCIStore, is_oci_uri, parse_oci_uri
from nimbuschain_zarr_service.service import ZarrConversionService


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _scene_id_from_uri(raw_uri: str) -> str:
    name = Path(str(raw_uri)).name
    for suffix in (".SAFE.zip", ".SAFE", ".tar.gz", ".tgz", ".tar", ".zip", ".nc", ".tif", ".tiff"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem or "scene"


def _resolve_output_root(raw_value: str | None, fallback: str) -> Path:
    value = str(raw_value or "").strip()
    return Path(value or fallback).expanduser().resolve()


def _materialize_raw_source(raw_uri: str, stage_dir: Path) -> str:
    if not is_oci_uri(raw_uri):
        return raw_uri

    parsed = parse_oci_uri(raw_uri)
    store = OCIStore(bucket=parsed.bucket, namespace=parsed.namespace)
    stage_dir.mkdir(parents=True, exist_ok=True)
    target_name = Path(parsed.path.rstrip("/")).name or "input"
    if store.is_dir(parsed.path):
        target_path = stage_dir / target_name
        written = store.download_tree(parsed.path, target_path)
    elif store.is_file(parsed.path):
        target_path = stage_dir / target_name
        written = store.download_file(parsed.path, target_path)
    else:
        raise OCIStorageError(f"OCI source not found: {raw_uri}")
    return str(written)


def _mask_types(raw_value: str | None) -> list[str]:
    return [
        item.strip().lower()
        for item in str(raw_value or "").split(",")
        if item.strip().lower() in {"water", "cloud"}
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "VM-friendly pipeline runner: stage raw inputs from OCI when needed, "
            "convert them to Zarr, optionally apply masks, and optionally build cubes."
        )
    )
    parser.add_argument("sources", nargs="+", help="Raw input paths or oci:// URIs.")
    parser.add_argument("--provider", required=True, choices=["copernicus", "usgs"])
    parser.add_argument("--collection", required=True)
    parser.add_argument("--product-type", default=None)
    parser.add_argument("--stage-dir", default="./data/downloads/staged")
    parser.add_argument("--zarr-dir", default="./data/downloads/zarr")
    parser.add_argument("--mask-types", default=None, help="Comma-separated: water,cloud")
    parser.add_argument("--mask-service-url", default=None)
    parser.add_argument("--cube-mode", choices=["none", "single", "grouped"], default="none")
    parser.add_argument("--cube-output-uri", default=None)
    parser.add_argument("--cube-output-dir", default="./data/downloads/zarr/cubes/manual")
    parser.add_argument("--group-by-tile", action="store_true")
    parser.add_argument("--include-masks-in-cube", action="store_true")
    parser.add_argument("--skip-ancillary", action="store_true")
    return parser


def run(args: argparse.Namespace) -> int:
    stage_dir = _resolve_output_root(args.stage_dir, "./data/downloads/staged")
    zarr_dir = _resolve_output_root(args.zarr_dir, "./data/downloads/zarr")
    zarr_dir.mkdir(parents=True, exist_ok=True)

    converter = ZarrConversionService()
    mask_client: MaskServiceClient | None = None
    requested_mask_types = _mask_types(args.mask_types)
    if requested_mask_types:
        mask_client = MaskServiceClient(service_url=str(args.mask_service_url or "").strip() or None)

    raw_items: list[dict[str, Any]] = []
    converted_items: list[dict[str, Any]] = []
    final_scene_uris: list[str] = []

    try:
        for source in args.sources:
            local_raw_uri = _materialize_raw_source(str(source).strip(), stage_dir)
            scene_id = _scene_id_from_uri(local_raw_uri)
            output_uri = str((zarr_dir / f"{scene_id}.zarr").resolve())
            written_uri, data_family, summary, dataset_summary = converter.convert(
                provider=str(args.provider).strip().lower(),
                collection=str(args.collection).strip(),
                product_type=str(args.product_type).strip() or None if args.product_type is not None else None,
                scene_id=scene_id,
                raw_uri=local_raw_uri,
                output_uri=output_uri,
            )
            raw_items.append(
                {
                    "source_uri": source,
                    "local_raw_uri": local_raw_uri,
                    "scene_id": scene_id,
                }
            )
            item_result: dict[str, Any] = {
                "scene_id": scene_id,
                "source_uri": source,
                "local_raw_uri": local_raw_uri,
                "zarr_uri": written_uri,
                "data_family": data_family,
                "summary": summary,
                "dataset_summary": dataset_summary,
            }
            final_scene_uri = written_uri
            if requested_mask_types and mask_client is not None:
                mask_result = mask_client.apply_masks_to_zarr(
                    zarr_uri=written_uri,
                    provider=str(args.provider).strip().lower(),
                    collection=str(args.collection).strip(),
                    product_type=str(args.product_type).strip() or None if args.product_type is not None else None,
                    scene_id=scene_id,
                    acquisition_datetime=dataset_summary.get("acquisition_datetime"),
                    dataset_summary=dataset_summary,
                    mask_types=requested_mask_types,
                )
                item_result["mask_result"] = mask_result
                final_scene_uri = str(mask_result.get("masked_zarr_uri") or written_uri)
            item_result["final_scene_uri"] = final_scene_uri
            converted_items.append(item_result)
            final_scene_uris.append(final_scene_uri)

        cube_result: dict[str, Any] | None = None
        if args.cube_mode == "single":
            if not args.cube_output_uri:
                raise ValueError("--cube-output-uri is required when --cube-mode=single.")
            cube_result = build_time_cube(
                final_scene_uris,
                str(args.cube_output_uri).strip(),
                include_ancillary=not bool(args.skip_ancillary),
                include_masks=bool(args.include_masks_in_cube),
            )
        elif args.cube_mode == "grouped" or args.group_by_tile:
            cube_result = build_grouped_time_cubes(
                final_scene_uris,
                str(args.cube_output_dir).strip(),
                include_ancillary=not bool(args.skip_ancillary),
                include_masks=bool(args.include_masks_in_cube),
            )

        print(
            json.dumps(
                {
                    "status": "completed",
                    "provider": args.provider,
                    "collection": args.collection,
                    "product_type": args.product_type,
                    "raw_items": raw_items,
                    "converted_items": converted_items,
                    "final_scene_uris": final_scene_uris,
                    "cube_result": cube_result,
                },
                default=_json_default,
            )
        )
        return 0
    finally:
        if mask_client is not None:
            mask_client.close()


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
