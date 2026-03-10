import re
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

import rioxarray
import xarray as xr
from loguru import logger

try:
    from converter.utilities import OCIStore

    OCI_AVAILABLE = True
except ImportError:
    OCI_AVAILABLE = False


class RemoteFS:
    """Lightweight wrapper around OCIStore for SAFE directory browsing.

    Provides file discovery (walk/glob) and a way to open remote files
    for rasterio reading via temporary local copies.
    """

    def __init__(self, oci_store: "OCIStore"):
        self.store = oci_store
        self.fs = oci_store.fs
        self._temp_files: dict[str, str] = {}  # Map remote_path -> local_path

    def _full_path(self, path: str) -> str:
        """Build the full OCI path: bucket@namespace/path."""
        return self.store._build_path(path)

    def listdir(self, path: str) -> List[str]:
        """List immediate children of a remote directory."""
        full = self._full_path(path)
        logger.debug(f"Listing OCI directory: {full}")
        try:
            items = self.fs.ls(full, detail=False)
        except FileNotFoundError:
            logger.warning(f"OCI directory not found: {full}")
            return []
        prefix = self._full_path("").rstrip("/") + "/"
        return [i[len(prefix):] if i.startswith(prefix) else i for i in items]

    def glob(self, path: str, pattern: str) -> List[str]:
        """Glob for files under a remote path.

        Returns relative paths (stripped of bucket@namespace prefix).
        """
        full = self._full_path(path.rstrip("/"))
        glob_pattern = f"{full}/**/{pattern}" if "**" not in pattern else f"{full}/{pattern}"
        logger.debug(f"Globbing OCI: {glob_pattern}")
        try:
            matches = self.fs.glob(glob_pattern)
        except FileNotFoundError:
            logger.warning(f"No matches for glob: {glob_pattern}")
            return []
        prefix = self._full_path("").rstrip("/") + "/"
        return [m[len(prefix):] if m.startswith(prefix) else m for m in matches]

    def find(self, path: str) -> List[str]:
        """Recursively find all files under a remote path.

        Returns relative paths (stripped of bucket@namespace prefix).
        """
        full = self._full_path(path)
        logger.debug(f"Finding all files in OCI: {full}")
        try:
            items = self.fs.find(full)
            logger.debug(f"Found {len(items)} files in {path}")
        except FileNotFoundError:
            logger.warning(f"OCI path not found for find: {full}")
            return []
        prefix = self._full_path("").rstrip("/") + "/"
        return [i[len(prefix):] if i.startswith(prefix) else i for i in items]

    def download_to_temp(self, remote_path: str, suffix: str = "") -> str:
        """Download a remote file to a local temp file for rasterio access.

        Uses cached file if already downloaded.
        """
        if remote_path in self._temp_files:
            return self._temp_files[remote_path]

        full = self._full_path(remote_path)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.close()
        logger.info(f"Downloading {remote_path} -> {tmp.name}")
        self.fs.get(full, tmp.name)
        self._temp_files[remote_path] = tmp.name
        return tmp.name

    def download_batch(self, remote_paths: List[str], max_workers: int = 1) -> None:
        """Download multiple files (serially by default to avoid OCI auth issues)."""
        from concurrent.futures import ThreadPoolExecutor

        to_download = [p for p in remote_paths if p not in self._temp_files]
        if not to_download:
            return

        logger.info(f"Downloading {len(to_download)} files in parallel (workers={max_workers})...")

        def _download(path):
            try:
                # We reuse existing logic but capture the result
                suffix = Path(path).suffix
                self.download_to_temp(path, suffix=suffix)
            except Exception as e:
                logger.error(f"Failed to download {path}: {e}")
                raise

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(_download, to_download))

    def cleanup(self):
        """Remove all temporary files."""
        import os

        for f in self._temp_files.values():
            try:
                os.unlink(f)
            except OSError:
                pass
        self._temp_files.clear()

    def __del__(self):
        self.cleanup()


class BaseReader(ABC):
    @abstractmethod
    def read(self, file_path: str) -> xr.Dataset:
        pass

    def _find_bands(
        self,
        bands_config: dict,
        img_dir: Path,
        extension: str,
        subdir_filter: Optional[str] = None,
    ) -> List[Path]:
        """Find band files in a local directory."""
        files = []
        for subdir_name, band_list in bands_config.items():
            search_dir = img_dir / subdir_name if subdir_name and subdir_name != "empty" else img_dir
            if not search_dir.exists():
                continue

            regexes = [re.compile(p) for p in band_list]

            for p in search_dir.glob(f"*.{extension}"):
                if not p.is_file():
                    continue
                if any(r.search(p.name) for r in regexes):
                    files.append(p)
        return files

    def _find_bands_remote(
        self,
        bands_config: dict,
        base_path: str,
        extension: str,
        remote_fs: RemoteFS,
    ) -> List[str]:
        """Find band files in a remote OCI directory.

        Returns a list of remote relative paths (not full OCI URIs).
        """
        logger.info(f"Finding remote bands in: {base_path} (ext={extension})")
        all_files = remote_fs.find(base_path)
        logger.debug(f"Total files found in {base_path}: {len(all_files)}")
        if not all_files:
            logger.warning(f"RemoteFS.find returned NO files for path: {base_path}")

        ext_lower = extension.lower()
        matching = [f for f in all_files if f.lower().endswith(f".{ext_lower}")]

        all_bands = [b for sublist in bands_config.values() for b in sublist]
        regexes = [re.compile(p) for p in all_bands]

        files = []
        for f in matching:
            fname = f.split("/")[-1]
            if any(r.search(fname) for r in regexes):
                files.append(f)
        return files

    def _open_rasterio(
        self,
        path,
        chunks: Optional[dict] = None,
        remote_fs: Optional[RemoteFS] = None,
    ) -> xr.DataArray:
        """Open a raster file with rioxarray, from local path or OCI.

        Args:
            path: Local Path object or remote path string.
            chunks: Dask chunk spec.
            remote_fs: If provided, path is treated as a remote OCI path
                       and downloaded to a temp file before reading.
        """
        if remote_fs is not None and isinstance(path, str):
            suffix = Path(path).suffix
            local_tmp = remote_fs.download_to_temp(path, suffix=suffix)
            return rioxarray.open_rasterio(local_tmp, chunks=chunks)
        else:
            return rioxarray.open_rasterio(path, chunks=chunks)
