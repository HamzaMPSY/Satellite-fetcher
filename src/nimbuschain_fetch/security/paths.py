from __future__ import annotations

from pathlib import Path, PurePosixPath


class UnsafePathError(ValueError):
    """Raised when a user path is unsafe."""


def _validate_relative_path(user_path: str) -> PurePosixPath:
    path = PurePosixPath(user_path)
    if path.is_absolute():
        raise UnsafePathError("Absolute paths are not allowed for output_dir.")

    for part in path.parts:
        if part in {"", ".", ".."}:
            if part in {".", ".."}:
                raise UnsafePathError("Path traversal segments are not allowed.")
            continue
        if "\x00" in part:
            raise UnsafePathError("NUL byte detected in output_dir.")

    return path


def sanitize_output_dir(base_dir: Path, requested: str | None, fallback_name: str) -> Path:
    """Return a path guaranteed to remain inside base_dir."""

    target_rel = _validate_relative_path(requested) if requested else PurePosixPath(fallback_name)
    base_resolved = base_dir.resolve()
    final_path = (base_resolved / Path(target_rel.as_posix())).resolve()

    if base_resolved not in final_path.parents and final_path != base_resolved:
        raise UnsafePathError("output_dir resolves outside NIMBUS_DATA_DIR.")

    final_path.mkdir(parents=True, exist_ok=True)
    return final_path
