from __future__ import annotations

import argparse
import json
import sys

from nimbuschain_zarr_service.core import ConversionDependencyError, ConversionError
from nimbuschain_zarr_service.cube import build_grouped_time_cubes, build_time_cube


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a time-series Zarr cube by stacking compatible scene-level Zarr "
            "stores along their real acquisition timestamps."
        )
    )
    parser.add_argument(
        "sources",
        nargs="+",
        help="One or more source scene Zarr URIs or local paths.",
    )
    parser.add_argument(
        "--output-uri",
        default=None,
        help="Destination Zarr URI or local path for the stacked cube.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Destination directory for grouped cube outputs.",
    )
    parser.add_argument(
        "--skip-ancillary",
        action="store_true",
        help="Write only imagery even if compatible ancillary layers exist.",
    )
    parser.add_argument(
        "--group-by-tile",
        action="store_true",
        help="Group source scene Zarrs by tile/path-row and build one cube per group.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive start date filter for grouped builds (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive end date filter for grouped builds (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--stage-label",
        default=None,
        help="Optional suffix added to grouped cube names, for example before_mask or after_mask.",
    )
    return parser


def run(args: argparse.Namespace) -> int:
    if args.group_by_tile:
        if not args.output_dir:
            raise ConversionError("--output-dir is required when --group-by-tile is used.")
        summary = build_grouped_time_cubes(
            args.sources,
            args.output_dir,
            include_ancillary=not args.skip_ancillary,
            start_date=args.start_date,
            end_date=args.end_date,
            stage_label=args.stage_label,
        )
    else:
        if not args.output_uri:
            raise ConversionError("--output-uri is required for a single explicit cube build.")
        summary = build_time_cube(
            args.sources,
            args.output_uri,
            include_ancillary=not args.skip_ancillary,
        )
    print(json.dumps(summary))
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
