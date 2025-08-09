import aiohttp
import asyncio
from pathlib import Path
from loguru import logger
import zipfile
import shutil
import geopandas as gpd
from rasterio.mask import mask
import rasterio
from typing import List, Union


class DownloadManager:
    """
    Async Download Manager for Copernicus products using aiohttp and asyncio concurrency.
    """

    def __init__(self, base_url: str, download_url: str, access_token: str, max_concurrent: int = 5):
        self.base_url = base_url
        self.download_url = download_url
        self.access_token = access_token
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def download_product(self, product_id: str, output_dir: str = "downloads") -> str:
        headers = {'Authorization': f'Bearer {self.access_token}'}
        product_url = f"{self.base_url}/odata/v1/Products({product_id})"

        async with aiohttp.ClientSession() as session:
            async with session.get(product_url, headers=headers) as resp:
                resp.raise_for_status()
                product_info = await resp.json()
                product_name = product_info['Name']

            download_url = f"{self.download_url}/odata/v1/Products({product_id})/$value"
            filepath = Path(output_dir) / f"{product_name}.zip"
            filepath.parent.mkdir(parents=True, exist_ok=True)
            chunk_size = 1024 * 1024  # 1MB

            async with session.get(download_url, headers=headers) as download_resp:
                download_resp.raise_for_status()
                with open(filepath, 'wb') as f:
                    async for chunk in download_resp.content.iter_chunked(chunk_size):
                        f.write(chunk)

            logger.info(f"Downloaded product {product_name} to {filepath}")
            return str(filepath)

    async def download_product_with_semaphore(self, product_id: str, output_dir: str):
        async with self.semaphore:
            return await self.download_product(product_id, output_dir)

    def find_actual_safe_dir(self, extracted_path: Path) -> Path:
        if (extracted_path / "GRANULE").exists():
            return extracted_path
        safe_candidates = list(extracted_path.glob("*.SAFE"))
        if safe_candidates:
            candidate = safe_candidates[0]
            if (candidate / "GRANULE").exists():
                return candidate
        for subdir in extracted_path.iterdir():
            if subdir.is_dir() and (subdir / "GRANULE").exists():
                return subdir
        return extracted_path

    def crop_sentinel2_product(
        self,
        safe_dir: Path,
        aoi_geometry,
        output_dir: str = "cropped_output",
        target_resolutions: Union[List[str], None] = None
    ) -> List[str]:
        if target_resolutions is None:
            target_resolutions = ["R10m", "R20m", "R60m"]

        logger.info(f"Cropping Sentinel-2 product {safe_dir} for resolutions: {target_resolutions}")
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        actual_safe_dir = self.find_actual_safe_dir(safe_dir)
        logger.info(f"Using SAFE directory: {actual_safe_dir}")
        gdf = gpd.GeoDataFrame(geometry=[aoi_geometry], crs="EPSG:4326")
        cropped_files = []
        try:
            granule_base = actual_safe_dir / "GRANULE"
            if not granule_base.exists():
                logger.warning(f"GRANULE directory not found in {actual_safe_dir}. Skipping.")
                return []
            granule_subdirs = list(granule_base.glob("*"))
            if not granule_subdirs:
                logger.warning(f"No granule subdirectories found in {granule_base}. Skipping.")
                return []
            granule_dir = granule_subdirs[0]
            logger.info(f"Found granule directory: {granule_dir}")
            img_data_dir = granule_dir / "IMG_DATA"
            if not img_data_dir.exists():
                logger.warning(f"IMG_DATA directory not found in {granule_dir}. Skipping.")
                return []
            for res in target_resolutions:
                res_dir = img_data_dir / res
                if not res_dir.exists():
                    logger.warning(f"Resolution {res} not found. Skipping.")
                    continue
                band_files = sorted(res_dir.glob("*.jp2"))
                if not band_files:
                    logger.warning(f"No band files in {res_dir}. Skipping.")
                    continue
                logger.info(f"Processing {len(band_files)} bands at {res} resolution")
                for i, band_file in enumerate(band_files, start=1):
                    try:
                        logger.info(f"[{res}] Band {i}/{len(band_files)}: {band_file.name}")
                        with rasterio.open(band_file) as src:
                            gdf_reprojected = gdf.to_crs(src.crs)
                            out_image, out_transform = mask(src, gdf_reprojected.geometry, crop=True, nodata=0)
                            out_meta = src.meta.copy()
                            out_meta.update({
                                "driver": "GTiff",
                                "height": out_image.shape[1],
                                "width": out_image.shape[2],
                                "transform": out_transform
                            })
                            output_filename = f"{band_file.stem}_cropped.tif"
                            output_filepath = Path(output_dir) / output_filename
                            with rasterio.open(output_filepath, "w", **out_meta) as dest:
                                dest.write(out_image)
                            cropped_files.append(str(output_filepath))
                    except Exception as e:
                        logger.error(f"Failed to process band {band_file}: {e}")
                        continue
            logger.info(f"Successfully cropped {len(cropped_files)} bands.")
            return cropped_files
        except Exception as e:
            logger.error(f"Failed to process {actual_safe_dir}: {e}")
            return []

    async def download_products_concurrent(
        self,
        product_ids: List[str],
        aoi_geometry=None,
        output_dir: str = "downloads",
        crop: bool = False,
        target_resolutions: Union[List[str], None] = None
    ) -> List[str]:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        results = []
        tasks = [asyncio.create_task(self.download_product_with_semaphore(pid, output_dir)) for pid in product_ids]

        downloaded_files = await asyncio.gather(*tasks)

        for downloaded_path in downloaded_files:
            results.append(downloaded_path)
            product_name = Path(downloaded_path).stem
            safe_dir = Path(downloaded_path).with_suffix("")
            extracted = False
            if not safe_dir.exists():
                with zipfile.ZipFile(downloaded_path, 'r') as zip_ref:
                    zip_ref.extractall(safe_dir)
                logger.info(f"Extracted {downloaded_path} to {safe_dir}")
                extracted = True

            if crop and aoi_geometry:
                cropped_output_dir = Path(output_dir) / f"{product_name}_cropped"
                cropped_output_dir.mkdir(parents=True, exist_ok=True)
                cropped_files = self.crop_sentinel2_product(
                    safe_dir,
                    aoi_geometry,
                    str(cropped_output_dir),
                    target_resolutions=target_resolutions
                )
                results.extend(cropped_files)

            # Cleanup
            try:
                if Path(downloaded_path).exists():
                    Path(downloaded_path).unlink()
                    logger.info(f"Deleted archive: {downloaded_path}")
                if safe_dir.exists() and safe_dir.is_dir():
                    shutil.rmtree(safe_dir)
                    logger.info(f"Deleted SAFE directory: {safe_dir}")
            except Exception as cleanup_e:
                logger.warning(f"Cleanup failed for {product_name}: {cleanup_e}")

        return results
