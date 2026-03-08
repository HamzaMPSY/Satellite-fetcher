from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator
import shutil
import tempfile

import fsspec


class RemoteFS:
    """Thin wrapper over fsspec for listing and optional localizing remote files."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.fs, self.base = fsspec.core.url_to_fs(self.url)
        self._tmpdir: str | None = None
        self._downloaded: list[str] = []

    def list_files(self, extensions: set[str] | None = None) -> list[str]:
        files: list[str] = []
        for _, _, filenames in self.fs.walk(self.base):
            for name in filenames:
                if extensions and Path(name).suffix.lower() not in extensions:
                    continue
                files.append(f"{self.base}/{name}" if not name.startswith(self.base) else name)
        return files

    def open(self, path: str):
        return self.fs.open(path, "rb")

    def download(self, remote_path: str) -> Path:
        if self._tmpdir is None:
            self._tmpdir = tempfile.mkdtemp(prefix="nimbus_remote_")
        local_path = Path(self._tmpdir) / Path(remote_path).name
        with self.fs.open(remote_path, "rb") as src, open(local_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        self._downloaded.append(str(local_path))
        return local_path

    def download_batch(self, paths: Iterable[str]) -> list[Path]:
        return [self.download(p) for p in paths]

    def cleanup(self) -> None:
        if self._tmpdir and Path(self._tmpdir).exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        self._tmpdir = None
        self._downloaded.clear()


def is_remote_path(uri: str) -> bool:
    return "://" in uri and not uri.startswith("file://")


def iter_matching_files(root: Path, extensions: set[str] | None = None) -> Iterator[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if extensions and path.suffix.lower() not in extensions:
            continue
        yield path
