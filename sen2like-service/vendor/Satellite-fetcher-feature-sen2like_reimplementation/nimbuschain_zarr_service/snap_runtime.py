from __future__ import annotations

from pathlib import Path
from shutil import which
from typing import Any
import os
import subprocess


def find_snap_gpt() -> Path | None:
    explicit = os.getenv("NIMBUS_SNAP_GPT_PATH") or os.getenv("SNAP_GPT_PATH")
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    path_hit = which("gpt")
    if path_hit:
        candidates.append(Path(path_hit))
    candidates.extend(
        [
            Path("/opt/snap/bin/gpt"),
            Path("/usr/local/snap/bin/gpt"),
            Path("/usr/local/lib/snap/bin/gpt"),
            Path("/snap/bin/gpt"),
        ]
    )

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser()
        if resolved in seen:
            continue
        seen.add(resolved)
        if not resolved.exists() or not resolved.is_file():
            continue
        if str(resolved) == "/usr/sbin/gpt":
            continue
        if _looks_like_snap_gpt(resolved):
            return resolved
    return None


def snap_support_status() -> dict[str, Any]:
    gpt_path = find_snap_gpt()
    return {
        "available": gpt_path is not None,
        "gpt_path": str(gpt_path) if gpt_path is not None else None,
        "java_home": os.getenv("JAVA_HOME"),
    }


def _looks_like_snap_gpt(candidate: Path) -> bool:
    try:
        result = subprocess.run(
            [str(candidate), "--help"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception:
        return False
    output = f"{result.stdout}\n{result.stderr}"
    upper = output.upper()
    return "GRAPH PROCESSING TOOL" in upper or "SNAP" in upper
