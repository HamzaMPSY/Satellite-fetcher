from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from nimbuschain_fetch.client import NimbusFetcherClient


FINAL_STATES = {"succeeded", "failed", "cancelled"}


def _load_aoi_payload(aoi_file: str) -> dict[str, Any]:
    path = Path(aoi_file)
    if not path.exists():
        raise FileNotFoundError(f"AOI file not found: {aoi_file}")

    raw = path.read_text(encoding="utf-8").strip()
    if path.suffix.lower() in {".geojson", ".json"}:
        return {"geojson": json.loads(raw)}
    return {"wkt": raw}


def _build_request(args: argparse.Namespace) -> dict[str, Any]:
    if args.product_ids:
        product_ids = [item.strip() for item in args.product_ids.split(",") if item.strip()]
        return {
            "job_type": "download_products",
            "provider": args.provider,
            "collection": args.collection,
            "product_ids": product_ids,
            "output_dir": args.output_dir,
        }

    if not args.start_date or not args.end_date:
        raise ValueError("start-date and end-date are required for search_download mode.")
    if not args.product_type:
        raise ValueError("product-type is required for search_download mode.")
    if not args.aoi_file:
        raise ValueError("aoi_file is required for search_download mode.")

    request = {
        "job_type": "search_download",
        "provider": args.provider,
        "collection": args.collection,
        "product_type": args.product_type,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "aoi": _load_aoi_payload(args.aoi_file),
        "tile_id": args.tile_id,
        "output_dir": args.output_dir,
    }
    return request


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NimbusChain Fetch CLI")

    parser.add_argument("--mode", choices=["direct", "service"], default="direct")
    parser.add_argument("--service-url", default="http://127.0.0.1:8000")
    parser.add_argument("--api-key", default=None)

    parser.add_argument("--provider", required=True, choices=["copernicus", "usgs"])
    parser.add_argument("--collection", required=True)
    parser.add_argument("--product-type", default=None)
    parser.add_argument("--tile-id", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--aoi_file", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--product-ids", default=None)

    # Legacy CLI compatibility flags (accepted but currently handled by engine defaults).
    parser.add_argument("--config", default=None)
    parser.add_argument("--log-type", default="all")
    parser.add_argument("--destination", default="local")
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--max-concurrent", type=int, default=None)
    parser.add_argument("--parallel-days", type=int, default=None)
    parser.add_argument("--concurrent-per-day", type=int, default=None)
    parser.add_argument("--crop-aoi", action="store_true")

    parser.add_argument("--no-wait", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=1.5)
    return parser


def run(args: argparse.Namespace) -> int:
    request = _build_request(args)

    with NimbusFetcherClient(
        mode=args.mode,
        service_url=args.service_url,
        api_key=args.api_key,
    ) as client:
        job_id = client.submit_job(request)
        print(json.dumps({"job_id": job_id}))

        if args.no_wait:
            return 0

        while True:
            status = client.get_job(job_id)
            print(
                json.dumps(
                    {
                        "job_id": status.job_id,
                        "state": status.state.value,
                        "progress": status.progress,
                        "bytes_downloaded": status.bytes_downloaded,
                        "bytes_total": status.bytes_total,
                    }
                )
            )
            if status.state.value in FINAL_STATES:
                break
            time.sleep(max(0.2, args.poll_interval))

        if status.state.value != "succeeded":
            return 1

        result = client.get_result(job_id)
        print(result.model_dump_json())
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
