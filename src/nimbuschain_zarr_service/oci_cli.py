from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from nimbuschain_zarr_service.oci_storage import OCIStorageError, OCIStore, parse_oci_uri


def _normalize_remote_name(remote_path: str) -> str:
    value = str(remote_path or "").rstrip("/")
    return Path(value).name or "object"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _resolve_copy_target(destination: Path, remote_name: str, *, treat_destination_as_dir: bool) -> Path:
    if treat_destination_as_dir:
        if destination.exists() and destination.is_dir():
            return destination / remote_name
        return destination
    if destination.exists() and destination.is_dir():
        return destination / remote_name
    return destination


def _build_store(uri: str) -> tuple[OCIStore, str]:
    parsed = parse_oci_uri(uri)
    store = OCIStore(bucket=parsed.bucket, namespace=parsed.namespace)
    return store, parsed.path


def _command_ls(args: argparse.Namespace) -> int:
    store, remote_path = _build_store(args.uri)
    if args.recursive:
        items = store.find(remote_path)
        if args.detail:
            payload = [
                {
                    "path": item,
                    "info": dict(store.fs.info(item)),
                }
                for item in items
            ]
        else:
            payload = [{"path": item} for item in items]
    else:
        payload = store.listdir(remote_path, detail=bool(args.detail))
    print(json.dumps(payload, default=_json_default))
    return 0


def _command_stat(args: argparse.Namespace) -> int:
    store, remote_path = _build_store(args.uri)
    info = store.info(remote_path)
    if info is None:
        raise OCIStorageError(f"OCI object not found: {args.uri}")
    payload = {
        "uri": args.uri,
        "bucket": store.bucket,
        "namespace": store.namespace,
        "path": remote_path,
        "info": info,
        "is_dir": store.is_dir(remote_path),
        "is_file": store.is_file(remote_path),
    }
    print(json.dumps(payload, default=_json_default))
    return 0


def _command_cp(args: argparse.Namespace) -> int:
    store, remote_path = _build_store(args.uri)
    destination = Path(args.destination).expanduser()
    remote_name = _normalize_remote_name(remote_path)

    if store.is_dir(remote_path):
        if not args.recursive:
            raise OCIStorageError("The source is a directory. Re-run with --recursive.")
        target_path = _resolve_copy_target(destination, remote_name, treat_destination_as_dir=True)
        written_path = store.download_tree(remote_path, target_path)
        payload = {
            "status": "downloaded",
            "source_uri": args.uri,
            "source_kind": "directory",
            "local_path": written_path,
        }
    elif store.is_file(remote_path):
        target_path = _resolve_copy_target(destination, remote_name, treat_destination_as_dir=False)
        written_path = store.download_file(remote_path, target_path)
        payload = {
            "status": "downloaded",
            "source_uri": args.uri,
            "source_kind": "file",
            "local_path": written_path,
        }
    else:
        raise OCIStorageError(f"OCI object not found: {args.uri}")

    print(json.dumps(payload, default=_json_default))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect and download OCI Object Storage inputs for VM-based pipeline runs."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ls_parser = subparsers.add_parser("ls", help="List OCI objects under a prefix.")
    ls_parser.add_argument("uri", help="An oci://bucket@namespace/path URI.")
    ls_parser.add_argument("--recursive", action="store_true")
    ls_parser.add_argument("--detail", action="store_true")
    ls_parser.set_defaults(handler=_command_ls)

    stat_parser = subparsers.add_parser("stat", help="Show metadata for one OCI object or prefix.")
    stat_parser.add_argument("uri", help="An oci://bucket@namespace/path URI.")
    stat_parser.set_defaults(handler=_command_stat)

    cp_parser = subparsers.add_parser("cp", help="Download one OCI object or prefix to local disk.")
    cp_parser.add_argument("uri", help="An oci://bucket@namespace/path URI.")
    cp_parser.add_argument("destination", help="Local destination path or directory.")
    cp_parser.add_argument("--recursive", action="store_true", help="Required when the source URI is a directory/prefix.")
    cp_parser.set_defaults(handler=_command_cp)
    return parser


def run(args: argparse.Namespace) -> int:
    return int(args.handler(args))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        code = run(args)
    except OCIStorageError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        sys.exit(1)
    sys.exit(code)


if __name__ == "__main__":
    main()
