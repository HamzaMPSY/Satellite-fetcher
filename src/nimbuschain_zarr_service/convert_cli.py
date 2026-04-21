from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from nimbuschain_zarr_service.core import ConversionDependencyError, ConversionError
from nimbuschain_zarr_service.service import ZarrConversionService


def _scene_id_from_uri(raw_uri: str) -> str:
    name = Path(str(raw_uri)).name
    for suffix in (".SAFE.zip", ".SAFE", ".tar.gz", ".tgz", ".tar", ".zip", ".nc", ".tif", ".tiff"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem or "scene"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert one raw scene bundle into a normalized scene-level Zarr store. "
            "The source may be a local path or an oci:// URI."
        )
    )
    parser.add_argument("raw_uri", help="Local path or oci:// URI to the raw input bundle.")
    parser.add_argument("--provider", required=True, choices=["copernicus", "usgs"])
    parser.add_argument("--collection", required=True)
    parser.add_argument("--product-type", default=None)
    parser.add_argument("--scene-id", default=None)
    parser.add_argument("--output-uri", required=True, help="Destination local path or oci:// URI for the Zarr store.")
    return parser


def run(args: argparse.Namespace) -> int:
    scene_id = str(args.scene_id or _scene_id_from_uri(args.raw_uri)).strip()
    service = ZarrConversionService()
    written_uri, data_family, summary, dataset_summary = service.convert(
        provider=str(args.provider).strip().lower(),
        collection=str(args.collection).strip(),
        scene_id=scene_id,
        raw_uri=str(args.raw_uri).strip(),
        output_uri=str(args.output_uri).strip(),
        product_type=str(args.product_type).strip() or None if args.product_type is not None else None,
    )
    print(
        json.dumps(
            {
                "status": "written",
                "scene_id": scene_id,
                "zarr_uri": written_uri,
                "data_family": data_family,
                "summary": summary,
                "dataset_summary": dataset_summary,
            },
            default=_json_default,
        )
    )
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        code = run(args)
    except (ConversionError, ConversionDependencyError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        sys.exit(1)
    sys.exit(code)


if __name__ == "__main__":
    main()
