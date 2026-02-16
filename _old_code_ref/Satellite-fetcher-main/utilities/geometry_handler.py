import os
import shutil
from loguru import logger
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Polygon, mapping
import tarfile
import rasterio
from rasterio.mask import mask
from shapely.ops import transform
import pyproj
import zipfile
# cspell:words arcname jp2
class GeometryHandler:
    """
    Handles geometry-related operations for satellite data.

    Able to load various supported geometry file formats (WKT, GeoJSON) and makes the geometry
    available as a Shapely Polygon for downstream processing.

    Attributes:
        file_path (str): Path to the geometry file (.geojson or .wkt).
        geometry (Polygon): The loaded Shapely geometry object.
    """
    def __init__(self, file_path: str):
        """
        Initialize the GeometryHandler and automatically load the geometry.

        Args:
            file_path (str): Path to the geometry file (.geojson or .wkt).

        Raises:
            ValueError: If the file type is unsupported.
        """
        self.file_path = file_path
        self.geometries = []
        self._load_geometry()

    def _load_geometry(self) -> Polygon:
        """
        Load geometry data from the specified file, supporting GeoJSON (.geojson)
        and Well-Known Text (.wkt) formats.

        Returns:
            Polygon: A Shapely geometry object loaded from the file.

        Raises:
            ValueError: If the file format is not supported.
        """
        # Check for supported file extensions and load using the right library
        if self.file_path.endswith('.geojson'):
            logger.info(f"Loading geometry from GeoJSON file: {self.file_path}")
            return gpd.read_file(self.file_path).geometry.values[0]
        elif self.file_path.endswith('.wkt'):
            logger.info(f"Loading geometry from WKT file: {self.file_path}")
            with open(self.file_path, 'r') as file:
                for line in file.readlines():
                    # Assuming the WKT file contains a single geometry
                    if line.strip():
                        self.geometries.append(wkt.loads(line.strip()))
        else:
            logger.error(f"Unsupported geometry file format: {self.file_path}. Must be '.geojson' or '.wkt'.")
            raise ValueError("Unsupported geometry file format. Use .geojson or .wkt")

    def crop_aoi(self, folder_path: str, provider: str, aoi: Polygon) -> None:
        """
        Crop the downloaded products to the specified area of interest (AOI).

        Args:
            folder_path (str): The folder path containing the downloaded products.
            provider (str): The name of the data provider.
            aoi (Polygon): The area of interest to crop to.

        Returns:
            Polygon: The cropped geometry.
        """
        
        logger.info(f"Cropping products in {folder_path} to AOI using provider {provider}")
        if provider.lower() == "usgs":
            # Handle USGS archives: supports .tar, .tar.gz, .tgz
            for entry in os.listdir(folder_path):
                if not entry.lower().endswith((".tar", ".tar.gz", ".tgz")):
                    continue

                archive_path = os.path.join(folder_path, entry)
                read_mode, write_mode = self._tar_modes_for(archive_path)

                # Prepare a clean extraction directory next to the archive
                base_name = entry
                if base_name.lower().endswith(".tar.gz"):
                    base_name = base_name[: -len(".tar.gz")]
                elif base_name.lower().endswith(".tgz"):
                    base_name = base_name[: -len(".tgz")]
                elif base_name.lower().endswith(".tar"):
                    base_name = base_name[: -len(".tar")]

                extracted_dir = os.path.join(folder_path, base_name)
                logger.debug(f"Extracting archive {archive_path} to temporary directory {extracted_dir}")
                if os.path.exists(extracted_dir):
                    shutil.rmtree(extracted_dir, ignore_errors=True)
                os.makedirs(extracted_dir, exist_ok=True)

                # Secure extract
                with tarfile.open(archive_path, read_mode) as tar:
                    self._safe_extract(tar, extracted_dir)

                # Crop all tif/tiff files in place
                for root, _, files in os.walk(extracted_dir):
                    for name in files:
                        if name.lower().endswith((".tif", ".tiff")):
                            self._crop_tiff(os.path.join(root, name), aoi)

                # Repack into a temporary archive preserving compression
                tmp_archive = archive_path + ".tmp"
                if os.path.exists(tmp_archive):
                    os.remove(tmp_archive)

                # Preserve original archive structure (names and directories)
                with tarfile.open(archive_path, read_mode) as src_tar:
                    original_members = src_tar.getmembers()

                with tarfile.open(tmp_archive, write_mode) as tar:
                    for member in original_members:
                        member_path = os.path.join(extracted_dir, member.name)
                        if os.path.exists(member_path):
                            # Add using the original member name to keep structure identical
                            if os.path.isdir(member_path):
                                # Ensure directories exist in archive (including empty)
                                tar.add(member_path, arcname=member.name, recursive=False)
                            else:
                                tar.add(member_path, arcname=member.name, recursive=False)
                        else:
                            logger.warning(f"Skipping missing path from original archive: {member.name}")

                # Atomically replace original archive and cleanup
                os.replace(tmp_archive, archive_path)
                shutil.rmtree(extracted_dir, ignore_errors=True)
                logger.info(f"Cropped and recompressed {entry} to AOI")
        elif provider.lower() == "copernicus":
            # Handle Copernicus archives: .zip containing .jp2
            for entry in os.listdir(folder_path):
                if not entry.lower().endswith(".zip"):
                    continue

                archive_path = os.path.join(folder_path, entry)

                # Prepare a clean extraction directory next to the archive
                base_name = entry
                if base_name.lower().endswith(".zip"):
                    base_name = base_name[: -len(".zip")]

                extracted_dir = os.path.join(folder_path, base_name)
                if os.path.exists(extracted_dir):
                    shutil.rmtree(extracted_dir, ignore_errors=True)
                os.makedirs(extracted_dir, exist_ok=True)

                # Secure extract and capture original member list
                with zipfile.ZipFile(archive_path, "r") as zf:
                    self._safe_extract_zip(zf, extracted_dir)
                    original_members = zf.infolist()

                # Crop all JP2 files in place
                for root, _, files in os.walk(extracted_dir):
                    for name in files:
                        if name.lower().endswith(".jp2"):
                            self._crop_tiff(os.path.join(root, name), aoi)

                # Repack into a temporary archive, preserving member names and compression
                tmp_archive = archive_path + ".tmp"
                if os.path.exists(tmp_archive):
                    os.remove(tmp_archive)

                with zipfile.ZipFile(tmp_archive, mode="w") as dst:
                    for member in original_members:
                        member_path = os.path.join(extracted_dir, member.filename)
                        if getattr(member, "is_dir", None) and member.is_dir() or member.filename.endswith("/"):
                            # Recreate directory entry (including empty directories)
                            zi = zipfile.ZipInfo(member.filename, date_time=member.date_time)
                            zi.external_attr = member.external_attr
                            zi.compress_type = member.compress_type
                            dst.writestr(zi, b"")
                        elif os.path.exists(member_path):
                            # Preserve per-file compression, attributes and timestamps using streaming copy
                            zi = zipfile.ZipInfo(member.filename, date_time=member.date_time)
                            zi.external_attr = member.external_attr
                            zi.compress_type = member.compress_type
                            with open(member_path, "rb") as src, dst.open(zi, "w") as zf_out:
                                # Stream file content to avoid loading large files in memory
                                import shutil as _shutil
                                _shutil.copyfileobj(src, zf_out, length=1024 * 1024)
                        else:
                            logger.warning(f"Skipping missing path from original archive: {member.filename}")

                # Atomically replace original archive and cleanup
                os.replace(tmp_archive, archive_path)
                shutil.rmtree(extracted_dir, ignore_errors=True)
                logger.info(f"Cropped and recompressed {entry} to AOI")
        elif provider.lower() == "opentopography":
            # here there would be just 1 tif file that should be croped
            for entry in os.listdir(folder_path):
                if entry.lower().endswith(".tif"):
                    tiff_path = os.path.join(folder_path, entry)
                    self._crop_tiff(tiff_path, aoi)
        elif provider.lower() == "cds":
            # Implement CDS-specific cropping logic here
            pass
    
    def _tar_modes_for(self, tar_path: str) -> tuple[str, str]:
        """
        Determine tar read/write modes based on file extension to preserve compression.
        """
        lower = tar_path.lower()
        if lower.endswith(".tar.gz") or lower.endswith(".tgz"):
            return "r:gz", "w:gz"
        if lower.endswith(".tar"):
            return "r:", "w"
        # Fallbacks
        return "r:*", "w"

    def _safe_extract(self, tar: tarfile.TarFile, path: str) -> None:
        """
        Safely extract a tar archive to 'path' preventing path traversal.
        """
        base_path = os.path.abspath(path)
        for member in tar.getmembers():
            member_path = os.path.abspath(os.path.join(path, member.name))
            if os.path.commonpath([base_path, member_path]) != base_path:
                raise Exception("Attempted Path Traversal in Tar File")
        tar.extractall(path)

    def _safe_extract_zip(self, zf: zipfile.ZipFile, path: str) -> None:
        """
        Safely extract a zip archive to 'path' preventing path traversal (Zip Slip).
        """
        base_path = os.path.abspath(path)
        for member in zf.infolist():
            member_path = os.path.abspath(os.path.join(path, member.filename))
            if os.path.commonpath([base_path, member_path]) != base_path:
                raise Exception("Attempted Path Traversal in Zip File")
        zf.extractall(path)

    def _crop_tiff(self, tiff_path: str, aoi: Polygon) -> None:
        """
        Crop a TIFF file to the specified AOI (reprojecting if needed).
        The result overwrites the original TIFF file.
        
        Args:
            tiff_path (str): Path to the TIFF file.
            aoi (Polygon): AOI polygon (any CRS, assumed to be in EPSG:4326 if not specified).
        """
        logger.info(f"Cropping TIFF file {tiff_path} to AOI")

        with rasterio.open(tiff_path) as src:
            # Ensure AOI is in the same CRS as the TIFF
            if src.crs is not None:
                try:
                    # Assume AOI is in EPSG:4326 (WGS84), reproject if needed
                    aoi_crs = "EPSG:4326"
                    if src.crs.to_string() != aoi_crs:
                        project = pyproj.Transformer.from_crs(aoi_crs, src.crs, always_xy=True).transform
                        aoi = transform(project, aoi)
                except Exception as e:
                    logger.warning(f"Could not reproject AOI, using as-is. Error: {e}")

            # Crop
            out_image, out_transform = mask(src, [mapping(aoi)], crop=True)
            out_meta = src.meta.copy()
            out_meta.update({
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform
            })

        # Overwrite original file safely
        tmp_path = tiff_path + ".tmp"
        with rasterio.open(tmp_path, "w", **out_meta) as dest:
            dest.write(out_image)

        os.replace(tmp_path, tiff_path)  # atomically replace original
