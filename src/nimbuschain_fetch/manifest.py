from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def checksums_for_paths(paths: list[str]) -> dict[str, str]:
    output: dict[str, str] = {}
    for file_path in paths:
        p = Path(file_path)
        if p.exists() and p.is_file():
            output[file_path] = sha256_file(p)
    return output


def build_manifest_entry(
    job_id: str,
    provider: str,
    collection: str,
    metadata: dict[str, Any],
    paths: list[str],
    checksums: dict[str, str],
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "provider": provider,
        "collection": collection,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "paths": paths,
        "checksums": checksums,
        "metadata": metadata,
    }


def write_manifest(output_dir: Path, manifest_entry: dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_entry, indent=2), encoding="utf-8")
    return manifest_path
