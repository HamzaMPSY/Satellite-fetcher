from __future__ import annotations
import json
import logging
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np
import rasterio
import rioxarray
import xarray as xr
from affine import Affine
from dask.diagnostics import ProgressBar
from pyproj import CRS
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject as rio_reproject, transform_bounds
from scipy.ndimage import map_coordinates
from skimage.registration import phase_cross_correlation

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------------------------------------------------------
# Landsat QA_PIXEL bit layout (Collection 2)
# ---------------------------------------------------------------------------
_QA_FILL_BIT          = 0
_QA_DILATED_CLOUD_BIT = 1
_QA_CIRRUS_BIT        = 2
_QA_CLOUD_BIT         = 3
_QA_CLOUD_SHADOW_BIT  = 4


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class GRIConfig:
    gri_path: Path
    gri_band: int            = 1
    target_resolution: float = 10.0
    target_shape: Optional[Tuple[int, int]] = None
    resampling_method: Resampling = Resampling.bilinear
    max_shift_pixels: float  = 50.0
    upsample_factor: int     = 8
    n_crops: int             = 16          # grille 4x4 pour meilleure couverture
    min_valid_frac: float    = 0.30
    output_dir: Path         = Path("./output")
    chunk_size: int          = 1024
    do_orthorectify: bool    = False
    dem_path: Optional[Path] = None

    # Seuils de robustesse co-registration
    min_crops_for_consensus: int = 3
    max_shift_disagreement_px: float = 2.0

    sentinel2_bands: List[str] = field(
        default_factory=lambda: ["B02", "B03", "B04", "B08"]
    )
    landsat_optical_bands: List[str] = field(
        default_factory=lambda: ["B2", "B3", "B4", "B5", "B6", "B7"]
    )

    s2_red_band_idx: int = 2
    ls_red_band_idx: int = 2

    io_workers: int  = 6
    cpu_workers: int = 4


# ---------------------------------------------------------------------------
# Scène dataclasses
# ---------------------------------------------------------------------------

@dataclass
class LandsatScene:
    scene_dir: Path
    band_files: Dict[str, Path]
    mtl: dict
    native_crs: CRS


@dataclass
class Sentinel2Scene:
    safe_dir: Path
    band_files: Dict[str, Path]
    native_crs: CRS


# ---------------------------------------------------------------------------
# GRI — chargement et construction
# ---------------------------------------------------------------------------

def load_gri(config: GRIConfig) -> xr.DataArray:
    if not config.gri_path.exists():
        raise FileNotFoundError(
            f"GRI introuvable : {config.gri_path}\n"
            f"→ Génère-le avec --build-gri depuis une scène S2 de référence."
        )
    gri = rioxarray.open_rasterio(
        config.gri_path,
        chunks={"band": 1, "x": config.chunk_size, "y": config.chunk_size},
        masked=True,
    )
    logger.info(
        "GRI chargé : %s | CRS: %s | shape: %s | résolution: %.1f m",
        config.gri_path.name, gri.rio.crs, gri.shape,
        abs(float(gri.rio.resolution()[0])),
    )
    return gri


def _parse_target_shape(raw: object | None) -> Optional[Tuple[int, int]]:
    text = str(raw or "").strip().lower()
    if text in {"", "native", "none", "off", "false", "0"}:
        return None
    normalized = text.replace(" ", "").replace(",", "x").replace(":", "x")
    parts = [part for part in normalized.split("x") if part]
    try:
        if len(parts) == 1:
            side = max(1, int(parts[0]))
            return side, side
        return max(1, int(parts[0])), max(1, int(parts[1]))
    except (IndexError, ValueError):
        logger.warning("[geo] Invalid target shape %r — falling back to 512x512", raw)
        return 512, 512


def _resolve_target_shape(config: GRIConfig) -> Optional[Tuple[int, int]]:
    if config.target_shape is not None:
        return max(1, int(config.target_shape[0])), max(1, int(config.target_shape[1]))
    return _parse_target_shape(
        os.getenv("NIMBUS_SEN2LIKE_PREPROCESS_TARGET_SHAPE")
        or os.getenv("NIMBUS_ZARR_TARGET_SHAPE")
        or "512x512"
    )


def _resize_gri_to_target_shape(
    gri_clipped: xr.DataArray,
    *,
    target_shape: Tuple[int, int],
) -> xr.DataArray:
    height, width = int(target_shape[0]), int(target_shape[1])
    if gri_clipped.rio.height == height and gri_clipped.rio.width == width:
        return gri_clipped

    from rasterio.transform import from_bounds

    bounds = gri_clipped.rio.bounds()
    transform = from_bounds(
        bounds[0],
        bounds[1],
        bounds[2],
        bounds[3],
        width,
        height,
    )
    resized = gri_clipped.rio.reproject(
        gri_clipped.rio.crs,
        transform=transform,
        shape=(height, width),
        resampling=Resampling.bilinear,
        nodata=0,
    )
    logger.info(
        "GRI target grid forced to %dx%d | resolution: %.2f x %.2f m",
        height, width,
        abs(float(resized.rio.resolution()[0])),
        abs(float(resized.rio.resolution()[1])),
    )
    return resized


def build_gri(
    sentinel2_safe: str | Path,
    gri_out: str | Path,
    resolution: float = 10.0,
    band: str = "B04",
    chunk_size: int = 1024,
) -> Path:
    safe_dir = Path(sentinel2_safe)
    gri_out  = Path(gri_out)
    gri_out.parent.mkdir(parents=True, exist_ok=True)

    granule   = next((safe_dir / "GRANULE").iterdir())
    jp2_files = list((granule / "IMG_DATA").rglob("*.jp2"))

    band_path = None
    for jp2 in jp2_files:
        m = re.search(r"_(B04)(?:_\d+m)?\.jp2$", jp2.name, re.I)
        if m:
            rm  = re.search(r"_(\d+)m\.jp2$", jp2.name)
            res = int(rm.group(1)) if rm else 9999
            if band_path is None or res < band_path[1]:
                band_path = (jp2, res)

    if band_path is None:
        raise FileNotFoundError(f"Bande B04 introuvable dans {safe_dir}")

    logger.info("Construction du GRI depuis %s", band_path[0].name)
    arr = rioxarray.open_rasterio(
        band_path[0],
        chunks={"band": 1, "x": chunk_size, "y": chunk_size},
        masked=True,
    )

    if abs(float(arr.rio.resolution()[0])) != resolution:
        arr = arr.rio.reproject(
            arr.rio.crs,
            resolution=resolution,
            resampling=Resampling.bilinear,
            nodata=0,
        )

    logger.info("Écriture du GRI → %s", gri_out)
    with ProgressBar():
        arr.rio.to_raster(
            gri_out,
            driver="GTiff",
            dtype="uint16",
            nodata=0,
            compress="deflate",
            tiled=True,
            blockxsize=512,
            blockysize=512,
        )

    logger.info("✓ GRI créé : %s | shape: %s", gri_out, arr.shape)
    return gri_out


# ---------------------------------------------------------------------------
# Lecteurs de scènes
# ---------------------------------------------------------------------------

def open_sentinel2(safe_dir: str | Path, config: GRIConfig) -> Sentinel2Scene:
    safe_dir = Path(safe_dir)
    granule  = next((safe_dir / "GRANULE").iterdir(), None)
    if not granule:
        raise FileNotFoundError(f"Aucun granule dans {safe_dir}/GRANULE")

    jp2_files  = list((granule / "IMG_DATA").rglob("*.jp2"))
    band_files: Dict[str, Path] = {}

    def _res(p: Path) -> int:
        rm = re.search(r"_(\d+)m\.jp2$", p.name)
        return int(rm.group(1)) if rm else 9999

    for jp2 in jp2_files:
        m = re.search(r"_(B\d+[A-Z]?)(?:_\d+m)?\.jp2$", jp2.name, re.I)
        if m:
            b = m.group(1).upper()
            if b in config.sentinel2_bands:
                if b not in band_files or _res(jp2) < _res(band_files[b]):
                    band_files[b] = jp2

    if not band_files:
        raise FileNotFoundError(f"Aucune bande trouvée dans {safe_dir}")

    with rasterio.open(next(iter(band_files.values()))) as ds:
        native_crs = CRS.from_user_input(ds.crs)

    logger.info("S2  bands: %s | CRS: %s", sorted(band_files), native_crs)
    return Sentinel2Scene(safe_dir=safe_dir, band_files=band_files, native_crs=native_crs)


def _parse_mtl_txt(path: Path) -> dict:
    data: dict = {}
    stack = [data]
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line == "END":
                continue
            if line.startswith("GROUP ="):
                g: dict = {}
                stack[-1][line.split("=", 1)[1].strip()] = g
                stack.append(g)
            elif line.startswith("END_GROUP"):
                stack.pop()
            elif "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"')
                try:
                    v = int(v)
                except ValueError:
                    try:
                        v = float(v)
                    except ValueError:
                        pass
                stack[-1][k] = v
    return data


def _load_mtl(scene_dir: Path) -> dict:
    for pattern, loader in [
        ("*_MTL.json", lambda p: json.load(open(p))),
        ("*_MTL.txt",  lambda p: _parse_mtl_txt(p)),
    ]:
        files = list(scene_dir.glob(pattern))
        if files:
            logger.info("MTL lu depuis : %s", files[0].name)
            return loader(files[0])

    xml = list(scene_dir.glob("*_MTL.xml"))
    if xml:
        import xml.etree.ElementTree as ET
        root = ET.parse(xml[0]).getroot()

        def _x2d(el):
            d = {}
            for c in el:
                if len(c):
                    d[c.tag] = _x2d(c)
                else:
                    v = c.text.strip() if c.text else ""
                    try:
                        v = int(v)
                    except ValueError:
                        try:
                            v = float(v)
                        except ValueError:
                            pass
                    d[c.tag] = v
            return d

        return _x2d(root)

    raise FileNotFoundError(f"Aucun fichier MTL dans {scene_dir}")


def open_landsat(scene_dir: str | Path, config: GRIConfig) -> LandsatScene:
    scene_dir = Path(scene_dir)
    mtl  = _load_mtl(scene_dir)
    proj = mtl["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]
    zone = int(proj["UTM_ZONE"])
    row  = int(mtl["LANDSAT_METADATA_FILE"]["IMAGE_ATTRIBUTES"]["WRS_ROW"])
    epsg = 32600 + zone if row < 60 else 32700 + zone

    band_files = {
        b: p
        for b in config.landsat_optical_bands
        if (p := next(scene_dir.glob(f"*_{b}.TIF"), None))
    }
    missing = [b for b in config.landsat_optical_bands if b not in band_files]
    if missing:
        logger.warning("Bandes Landsat manquantes (ignorées) : %s", missing)

    logger.info("LS  bands: %s | CRS: EPSG:%d", sorted(band_files), epsg)
    return LandsatScene(
        scene_dir=scene_dir,
        band_files=band_files,
        mtl=mtl,
        native_crs=CRS.from_epsg(epsg),
    )


# ---------------------------------------------------------------------------
# Empilement bandes → lazy DataArray
# ---------------------------------------------------------------------------

def scene_to_xarray(scene, config: GRIConfig) -> xr.DataArray:
    sorted_bands = sorted(scene.band_files.items())

    def _open_band(args: tuple) -> tuple[xr.DataArray, str]:
        band, path = args
        arr = rioxarray.open_rasterio(
            path,
            chunks={"band": 1, "x": config.chunk_size, "y": config.chunk_size},
            masked=True,
        )
        return arr, band

    n_workers = min(len(sorted_bands), config.io_workers)
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        results = list(pool.map(_open_band, sorted_bands))

    arrays = [arr for arr, _ in results]
    names  = [name for _, name in results]

    reference = arrays[0]
    ref_shape  = reference.shape

    aligned = [reference]
    for arr in arrays[1:]:
        if arr.shape != ref_shape:
            arr = arr.rio.reproject_match(reference, resampling=Resampling.bilinear)
        aligned.append(arr)

    stacked        = xr.concat(aligned, dim="band")
    stacked["band"] = names
    return stacked


# ---------------------------------------------------------------------------
# Masque de validité depuis QA_PIXEL (pour filtrer les crops nuageux)
# ---------------------------------------------------------------------------

def _build_landsat_validity_mask(
    scene_path: Path,
    target_shape: Tuple[int, int],
    target_array: xr.DataArray,
) -> Optional[Tuple[np.ndarray, np.ndarray]]:
    """
    Lit QA_PIXEL, décode les bits nuage/ombre/fill et eau, reprojette sur la
    grille cible, renvoie (clear_mask, land_mask) — tous deux booléens.
      - clear_mask: True = pixel utilisable (ni nuage, ni ombre, ni fill)
      - land_mask:  True = terre (non-eau)
    Renvoie None si QA_PIXEL absent — fallback sur le masque nodata seul.
    """
    qa_files = list(scene_path.glob("*_QA_PIXEL.TIF"))
    if not qa_files:
        logger.info(
            "[coreg] QA_PIXEL absent dans %s — filtrage chip par nodata uniquement.",
            scene_path,
        )
        return None

    try:
        with rasterio.open(qa_files[0]) as qa_src:
            qa_data = qa_src.read(1)
            qa_crs = qa_src.crs
            qa_trans = qa_src.transform

        bad = (
            ((qa_data >> _QA_FILL_BIT)          & 1).astype(bool)
            | ((qa_data >> _QA_DILATED_CLOUD_BIT) & 1).astype(bool)
            | ((qa_data >> _QA_CIRRUS_BIT)        & 1).astype(bool)
            | ((qa_data >> _QA_CLOUD_BIT)         & 1).astype(bool)
            | ((qa_data >> _QA_CLOUD_SHADOW_BIT)  & 1).astype(bool)
        )
        clear_native = (~bad).astype(np.uint8)

        # QA_PIXEL bit 7 = water (Collection-2)
        water_native = ((qa_data >> 7) & 1).astype(np.uint8)
        land_native = (1 - water_native).astype(np.uint8)

        def _reproj_to_target(src_arr: np.ndarray) -> np.ndarray:
            dst = np.zeros(target_shape, dtype=np.uint8)
            rio_reproject(
                source        = src_arr,
                destination   = dst,
                src_transform = qa_trans,
                src_crs       = qa_crs,
                dst_transform = target_array.rio.transform(),
                dst_crs       = target_array.rio.crs,
                resampling    = Resampling.nearest,
            )
            return dst.astype(bool)

        clear_mask = _reproj_to_target(clear_native)
        land_mask  = _reproj_to_target(land_native)

        logger.info(
            "[coreg] QA-derived masks: %.1f%% clear, %.1f%% land",
            100.0 * clear_mask.sum() / max(clear_mask.size, 1),
            100.0 * land_mask.sum()  / max(land_mask.size,  1),
        )
        return clear_mask, land_mask

    except Exception as exc:
        logger.warning(
            "[coreg] QA_PIXEL lecture/reproj échouée (%s) — fallback nodata-only.",
            exc,
        )
        return None


# ---------------------------------------------------------------------------
# Étapes géométriques — single-pass reproject + align
# ---------------------------------------------------------------------------

def reproject_and_align(
    array: xr.DataArray,
    gri: xr.DataArray,
    config: GRIConfig,
) -> Tuple[xr.DataArray, xr.DataArray]:
    """
    Single-pass: compute overlap bounds in GRI CRS, clip GRI to overlap,
    then reproject source array directly onto the clipped GRI grid.
    Replaces the old reproject_to_gri + align_to_gri pair which reprojected
    the full cube twice (first to GRI CRS/res, then again clipped to overlap).
    """
    gri_crs = gri.rio.crs
    gri_res = abs(float(gri.rio.resolution()[0]))
    logger.info(
        "Reprojection → %s @ %.1f m (imposé par le GRI, single-pass)",
        gri_crs, gri_res,
    )

    # Transform the source scene's bounds into the GRI's CRS so we can
    # intersect them without materializing a full-resolution reprojection.
    src_bounds_in_gri = transform_bounds(
        array.rio.crs, gri_crs, *array.rio.bounds(), densify_pts=21,
    )
    gri_b = gri.rio.bounds()
    overlap = (
        max(src_bounds_in_gri[0], gri_b[0]),
        max(src_bounds_in_gri[1], gri_b[1]),
        min(src_bounds_in_gri[2], gri_b[2]),
        min(src_bounds_in_gri[3], gri_b[3]),
    )
    if overlap[0] >= overlap[2] or overlap[1] >= overlap[3]:
        raise ValueError(
            f"Aucun chevauchement entre la scène et le GRI.\n"
            f"  Scène (en CRS GRI) : {src_bounds_in_gri}\n"
            f"  GRI                 : {gri_b}\n"
            f"→ Vérifie que ton GRI couvre la zone de la scène."
        )

    logger.info("Zone commune : %.1f %.1f %.1f %.1f", *overlap)
    gri_clipped = gri.rio.clip_box(*overlap)
    target_shape = _resolve_target_shape(config)
    if target_shape is not None:
        gri_clipped = _resize_gri_to_target_shape(
            gri_clipped,
            target_shape=target_shape,
        )

    # Single reproject directly onto the clipped GRI grid. This replaces
    # the former two-step process (reproject to full GRI, then reproject
    # again to the clipped grid).
    aligned = array.rio.reproject_match(
        gri_clipped,
        resampling=config.resampling_method,
        nodata=0,
    )
    logger.info("  shape aligné sur GRI : %s", aligned.shape)
    return aligned, gri_clipped


# ---------------------------------------------------------------------------
# Co-registration  [ROBUSTE: rejet, quorum, médiane + MAD]
# ---------------------------------------------------------------------------

def estimate_shift_vs_gri(
    array: xr.DataArray,
    gri_clipped: xr.DataArray,
    config: GRIConfig,
    red_band_idx: int = 2,
    scene_validity_mask: Optional[np.ndarray] = None,
    scene_land_mask: Optional[np.ndarray] = None,
) -> Tuple[float, float]:
    """
    Estime un décalage (row, col) sub-pixel entre `array` et le GRI via
    phase correlation sur plusieurs chips, réduit par médiane + MAD.

    Retourne (0.0, 0.0) — shift no-op — quand la corrélation n'est pas fiable:
      * Moins de `min_crops_for_consensus` chips valides.
      * Shifts en désaccord (MAD > `max_shift_disagreement_px`).
      * Chip individuel avec shift > `max_shift_pixels` → REJETÉ (pas clampé).
      * Chip majoritairement sur l'eau → rejeté (signal non fiable).
      * Chip à faible variance → rejeté (corrélation peu informative).

    Dans ces cas, la géométrie d'entrée est préservée (L1TP est déjà précis
    au sous-pixel contre des GCPs).
    """

    scene_red = array.isel(band=red_band_idx).compute().values.astype(np.float32)
    gri_arr   = gri_clipped.isel(band=0).compute().values.astype(np.float32)

    # ------------------------------------------------------------------
    # Align QA clear-mask to the correlation grid
    # ------------------------------------------------------------------
    qa_mask: Optional[np.ndarray] = None
    if scene_validity_mask is not None:
        if scene_validity_mask.shape != scene_red.shape:
            try:
                from skimage.transform import resize
                qa_mask = resize(
                    scene_validity_mask.astype(np.uint8),
                    scene_red.shape,
                    order=0,
                    preserve_range=True,
                    anti_aliasing=False,
                ).astype(bool)
            except Exception as exc:
                logger.warning(
                    "[coreg] Resize QA mask échoué (%s) — ignoré.", exc,
                )
                qa_mask = None
        else:
            qa_mask = scene_validity_mask.astype(bool)

    # ------------------------------------------------------------------
    # Align land mask to the correlation grid
    # ------------------------------------------------------------------
    land_mask: Optional[np.ndarray] = None
    if scene_land_mask is not None:
        if scene_land_mask.shape != scene_red.shape:
            try:
                from skimage.transform import resize
                land_mask = resize(
                    scene_land_mask.astype(np.uint8),
                    scene_red.shape,
                    order=0,
                    preserve_range=True,
                    anti_aliasing=False,
                ).astype(bool)
            except Exception as exc:
                logger.warning(
                    "[coreg] Resize land mask échoué (%s) — ignoré.", exc,
                )
                land_mask = None
        else:
            land_mask = scene_land_mask.astype(bool)

    # ------------------------------------------------------------------
    # Build crop grid
    # ------------------------------------------------------------------
    nr, nc  = scene_red.shape
    crop_h  = nr // 4
    crop_w  = nc // 4

    crop_args = []
    grid_side = int(round(config.n_crops ** 0.5))
    if grid_side * grid_side == config.n_crops and grid_side >= 2:
        step_y = nr // (grid_side + 1)
        step_x = nc // (grid_side + 1)
        idx = 0
        for gy in range(1, grid_side + 1):
            for gx in range(1, grid_side + 1):
                y0 = max(0, gy * step_y - crop_h // 2)
                x0 = max(0, gx * step_x - crop_w // 2)
                crop_args.append((
                    idx,
                    slice(y0, min(nr, y0 + crop_h)),
                    slice(x0, min(nc, x0 + crop_w)),
                ))
                idx += 1
    else:
        for i in range(config.n_crops):
            y0 = max(0, nr // (config.n_crops + 1) * (i + 1) - crop_h // 2)
            x0 = max(0, nc // (config.n_crops + 1) * (i + 1) - crop_w // 2)
            crop_args.append((
                i,
                slice(y0, min(nr, y0 + crop_h)),
                slice(x0, min(nc, x0 + crop_w)),
            ))

    min_frac = config.min_valid_frac
    if qa_mask is not None:
        min_frac = max(min_frac, 0.30)

    # Variance threshold: Landsat DN range is ~7000-30000, so std<50 means
    # a very uniform surface (calm water, thick cloud, bare soil).
    # Lower it to ~0.01 if inputs are already in reflectance.
    MIN_VARIANCE_DN = 50.0
    # Minimum land fraction to accept a crop (phase correlation over water
    # is unreliable due to lack of texture).
    MIN_LAND_FRAC = 0.50

    def _process_crop(args) -> Tuple[int, Optional[np.ndarray], float]:
        i, sy, sx = args
        scene_crop = scene_red[sy, sx]
        gri_crop   = gri_arr[sy, sx]

        scene_mask = ~np.isnan(scene_crop) & (scene_crop != 0)
        gri_mask   = ~np.isnan(gri_crop)   & (gri_crop   != 0)

        if qa_mask is not None:
            scene_mask &= qa_mask[sy, sx]

        # Reject crops dominated by water — phase correlation lacks
        # exploitable texture over water and produces spurious large shifts.
        if land_mask is not None:
            land_frac = float(land_mask[sy, sx].mean())
            if land_frac < MIN_LAND_FRAC:
                logger.debug(
                    "Crop %d rejected: only %.0f%% land (need ≥%.0f%%)",
                    i, land_frac * 100, MIN_LAND_FRAC * 100,
                )
                return i, None, 0.0

        # Coverage gate.
        scene_frac = float(scene_mask.mean())
        gri_frac   = float(gri_mask.mean())
        if scene_frac < min_frac or gri_frac < min_frac:
            logger.debug(
                "Crop %d ignoré: scene=%.1f%% gri=%.1f%% (min=%.0f%%)",
                i, scene_frac * 100, gri_frac * 100, min_frac * 100,
            )
            return i, None, 0.0

        # Variance gate — reject uniform surfaces that would give
        # ambiguous correlation peaks.
        sc_valid = scene_crop[scene_mask]
        if sc_valid.size == 0:
            return i, None, 0.0
        sc_std = float(np.std(sc_valid))
        if sc_std < MIN_VARIANCE_DN:
            logger.debug(
                "Crop %d rejected: std=%.1f too low (<%0.1f)",
                i, sc_std, MIN_VARIANCE_DN,
            )
            return i, None, 0.0

        # Prepare arrays for phase correlation. Masked pixels are filled
        # with the mean of the valid region to avoid hard discontinuities.
        sc = np.nan_to_num(scene_crop, nan=0.0).copy()
        gc = np.nan_to_num(gri_crop,   nan=0.0).copy()
        if scene_mask.any():
            sc[~scene_mask] = sc[scene_mask].mean()
        if gri_mask.any():
            gc[~gri_mask] = gc[gri_mask].mean()

        result = phase_cross_correlation(
            gc, sc,
            upsample_factor=config.upsample_factor,
        )
        shift = result[0]
        mag   = float(np.hypot(*shift))

        # Shift négligeable = pas de signal exploitable.
        if mag < 0.05:
            return i, None, 0.0

        # Shift énorme = corrélation foireuse — REJET (pas clamping).
        if mag > config.max_shift_pixels:
            logger.warning(
                "Crop %d : shift %.2f px exceeds max_shift=%.1f — REJECTED "
                "(correlation almost certainly spurious).",
                i, mag, config.max_shift_pixels,
            )
            return i, None, 0.0

        logger.debug(
            "Crop %d : row=%.3f col=%.3f mag=%.3f px",
            i, shift[0], shift[1], mag,
        )
        return i, shift, mag

    n_workers = min(len(crop_args), config.cpu_workers)
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        raw = list(pool.map(_process_crop, crop_args))

    shifts = [s for _, s, _ in raw if s is not None]
    accepted_ids = [i for i, s, _ in raw if s is not None]

    if len(shifts) < config.min_crops_for_consensus:
        logger.warning(
            "Only %d/%d crops produced valid shifts (need ≥%d for consensus) "
            "→ refusing to apply shift. Input L1/L1TP geometry is retained "
            "(already sub-pixel accurate against GCPs).",
            len(shifts), len(crop_args), config.min_crops_for_consensus,
        )
        return 0.0, 0.0

    shifts_arr = np.array(shifts)

    # Log each accepted crop so the final reduction is debuggable.
    for idx, shift in zip(accepted_ids, shifts_arr):
        logger.info(
            "[coreg] Accepted crop %d: row=%+.3f col=%+.3f mag=%.3f px",
            idx, shift[0], shift[1], float(np.hypot(*shift)),
        )

    med = np.median(shifts_arr, axis=0)
    mad = np.median(np.abs(shifts_arr - med), axis=0)
    mad_mag = float(np.hypot(*mad))

    if mad_mag > config.max_shift_disagreement_px:
        logger.warning(
            "Crop shifts disagree strongly (MAD=%.2f px > %.2f px threshold) "
            "→ refusing to apply shift.",
            mad_mag, config.max_shift_disagreement_px,
        )
        return 0.0, 0.0

    logger.info(
        "Co-registration vs GRI → row=%.3f px  col=%.3f px  (%.1f m)  "
        "[%d/%d crops, MAD=%.2f px]",
        med[0], med[1],
        float(np.hypot(*med)) * abs(float(gri_clipped.rio.resolution()[0])),
        len(shifts), len(crop_args), mad_mag,
    )
    return float(med[0]), float(med[1])


# ---------------------------------------------------------------------------
# Apply shift
# ---------------------------------------------------------------------------

def apply_shift(
    array: xr.DataArray,
    row_shift: float,
    col_shift: float,
) -> xr.DataArray:

    if abs(row_shift) < 1e-4 and abs(col_shift) < 1e-4:
        logger.debug("apply_shift: shift négligeable, pas de traitement")
        return array

    pixel_size = abs(float(array.rio.resolution()[0]))

    def _shift_all_bands(block: np.ndarray) -> np.ndarray:
        nb, ny, nx = block.shape
        r, c      = np.mgrid[0:ny, 0:nx]
        r_shifted = (r - row_shift).ravel()
        c_shifted = (c - col_shift).ravel()
        coords    = np.array([r_shifted, c_shifted])

        out = np.empty_like(block)
        for b in range(nb):
            interp = map_coordinates(
                block[b].astype(np.float32),
                coords,
                order=1,
                mode="nearest",
            )
            out[b] = interp.reshape(ny, nx)
        return out

    # Compute direct au lieu du pattern dask="parallelized" fragile
    shifted_np = _shift_all_bands(array.compute().values)
    shifted = xr.DataArray(
        shifted_np,
        dims=array.dims,
        coords=array.coords,
        attrs=array.attrs,
    )

    t     = array.rio.transform()
    new_t = Affine(
        t.a, t.b, t.c - col_shift * pixel_size,
        t.d, t.e, t.f + row_shift * pixel_size,
    )
    return shifted.rio.write_transform(new_t).rio.write_crs(array.rio.crs)


# ---------------------------------------------------------------------------
# Write bands  [parallélisé, cast uint16 propre, compression multi-thread]
# ---------------------------------------------------------------------------

def write_bands_and_copy_mtl(
    array: xr.DataArray,
    band_names: list,
    scene_path: Path,
    output_dir: Path,
    scene_id: str,
    config: GRIConfig,
    nodata: int = 0,
) -> Path:

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Writing %d geo-corrected bands → %s", len(band_names), output_dir)

    def _write_band(args: tuple) -> Path:
        i, band_name = args

        # Materialize the band once (was being computed twice before).
        band_data = array.isel(band=i).compute()
        arr_np = band_data.values

        # Only cast if we actually need to. Saves a ~480 MB copy per band
        # when the source is already uint16 (typical for raw Landsat).
        if not np.issubdtype(arr_np.dtype, np.integer):
            arr_np = np.clip(arr_np, 0, 65535).astype(np.uint16)
        elif arr_np.dtype != np.uint16:
            arr_np = arr_np.astype(np.uint16)

        out_path = output_dir / f"{scene_id}_{band_name}.TIF"

        profile = {
            "driver":      "GTiff",
            "dtype":       "uint16",
            "nodata":      nodata,
            "width":       band_data.rio.width,
            "height":      band_data.rio.height,
            "count":       1,
            "crs":         band_data.rio.crs,
            "transform":   band_data.rio.transform(),
            "compress":    "deflate",
            "tiled":       True,
            "blockxsize":  512,
            "blockysize":  512,
            "num_threads": "ALL_CPUS",   # parallelize deflate compression
        }

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(arr_np, 1)
            dst.update_tags(band_id=band_name)

        logger.info("  ✓ %s", out_path.name)
        return out_path

    n_workers = min(len(band_names), config.io_workers)
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(_write_band, (i, name)): name
                   for i, name in enumerate(band_names)}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error("Erreur écriture bande %s : %s", futures[future], exc)
                raise

    for mtl_file in scene_path.glob("*_MTL*"):
        dest = output_dir / mtl_file.name
        shutil.copy2(mtl_file, dest)
        logger.info("  ✓ MTL copié : %s", mtl_file.name)

    return output_dir


# ---------------------------------------------------------------------------
# Pipeline principal (une scène)
# ---------------------------------------------------------------------------

def process_scene(
    scene_path: str | Path,
    gri: xr.DataArray,
    config: GRIConfig,
) -> Path:

    scene_path = Path(scene_path)
    is_s2      = (scene_path / "GRANULE").exists()
    is_landsat = bool(list(scene_path.glob("*_MTL.*")))

    if is_s2:
        scene      = open_sentinel2(scene_path, config)
        red_idx    = config.s2_red_band_idx
        label      = "S2"
        band_names = list(scene.band_files.keys())
    elif is_landsat:
        scene      = open_landsat(scene_path, config)
        red_idx    = config.ls_red_band_idx
        label      = "LS"
        band_names = sorted(scene.band_files.keys())
    else:
        raise ValueError(
            f"Type de scène non reconnu : {scene_path}\n"
            f"Attendu : répertoire .SAFE (S2) ou dossier Landsat avec *_MTL.*"
        )

    logger.info("── Traitement %s : %s ──", label, scene_path.name)

    # 1. Chargement parallèle des bandes
    da = scene_to_xarray(scene, config)

    # 2. Single-pass reproject + align to GRI grid.
    #    Replaces the old two-step reproject_to_gri + align_to_gri which
    #    reprojected the full cube twice (~60 s wasted on a 23793×23433 cube).
    da_aligned, gri_clipped = reproject_and_align(da, gri, config)

    # 3. Masques QA pour filtrer les crops nuageux et aquatiques (LS only)
    scene_validity = None
    scene_land = None
    if is_landsat:
        aligned_shape = (da_aligned.rio.height, da_aligned.rio.width)
        qa_result = _build_landsat_validity_mask(
            scene_path, aligned_shape, da_aligned,
        )
        if qa_result is not None:
            scene_validity, scene_land = qa_result

    # 4. Estimation robuste du shift (médiane + MAD, refuse si peu fiable)
    row_shift, col_shift = estimate_shift_vs_gri(
        da_aligned, gri_clipped, config, red_idx,
        scene_validity_mask=scene_validity,
        scene_land_mask=scene_land,
    )

    # 5. Application du shift (no-op si row/col ~ 0)
    da_coreg = apply_shift(da_aligned, row_shift, col_shift)

    # 6. Écriture parallèle des bandes
    out_dir = write_bands_and_copy_mtl(
        da_coreg,
        band_names=band_names,
        scene_path=scene_path,
        output_dir=config.output_dir / scene_path.name,
        scene_id=scene_path.name,
        config=config,
    )

    # Sidecar JSON avec le shift appliqué (pour traçabilité)
    shift_mag_px = float(np.hypot(row_shift, col_shift))
    output_res = tuple(abs(float(value)) for value in da_coreg.rio.resolution())
    shift_m = shift_mag_px * output_res[0]
    sidecar = {
        "row_shift_px": float(row_shift),
        "col_shift_px": float(col_shift),
        "shift_mag_px": shift_mag_px,
        "shift_mag_m":  shift_m,
        "applied":      shift_mag_px > 1e-4,
        "output_shape": [
            int(da_coreg.rio.height),
            int(da_coreg.rio.width),
        ],
        "output_resolution_m": [output_res[0], output_res[1]],
    }
    (out_dir / "_geo_shift.json").write_text(json.dumps(sidecar, indent=2))

    logger.info(
        "✓ %s traité → shift=%.3f px (%.1f m) → %s",
        label, shift_mag_px, shift_m, out_dir,
    )
    return out_dir


# ---------------------------------------------------------------------------
# Point d'entrée : traiter N scènes
# ---------------------------------------------------------------------------

def run_pipeline(
    scene_paths: List[str | Path],
    config: GRIConfig,
) -> List[Path]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    gri = load_gri(config)

    outputs = []
    for i, scene_path in enumerate(scene_paths):
        logger.info("\n[%d/%d] %s", i + 1, len(scene_paths), Path(scene_path).name)
        out = process_scene(scene_path, gri, config)
        outputs.append(out)

    logger.info("\n✓ Pipeline terminé — %d scène(s) traitée(s)", len(outputs))
    for o in outputs:
        logger.info("  → %s", o)
    return outputs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Geometric processing GRI-based (inspiré de sen2like)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  # Étape 0 : construire le GRI depuis une scène S2 de référence
  python pipelineGRI.py --build-gri \\
      S2A_MSIL2A_20260101T105501_N0511_R051_T31TDN_20260101T145209.SAFE \\
      --gri-out gri/GRI_T31TDN.tif

  # Étape 1 : traiter des scènes avec le GRI
  python pipelineGRI.py \\
      --gri gri/GRI_T31TDN.tif \\
      LC08_L2SP_198027_20260105_20260114_02_T1 \\
      --outdir ./output_gri
        """,
    )
    p.add_argument("--build-gri",   action="store_true")
    p.add_argument("--gri-out",     default="gri/GRI.tif")
    p.add_argument("--gri",         default=None)
    p.add_argument("scenes",        nargs="*")
    p.add_argument("--outdir",      default="./output")
    p.add_argument("--resolution",  type=float, default=10.0)
    p.add_argument("--max-shift",   type=float, default=50.0,
                   help="Shift max par chip (px). Au-delà: REJETÉ. Défaut: 50")
    p.add_argument("--n-crops",     type=int, default=16,
                   help="Nombre de chips de corrélation (carré parfait = grille). Défaut: 16")
    p.add_argument("--min-crops",   type=int, default=3,
                   help="Nombre min de chips valides pour appliquer un shift. Défaut: 3")
    p.add_argument("--max-mad",     type=float, default=2.0,
                   help="MAD max (px) entre chips. Au-delà: refus. Défaut: 2.0")
    p.add_argument("--s2-bands",    default="B02,B03,B04,B08")
    p.add_argument("--ls-bands",    default="B2,B3,B4,B5,B6,B7")
    p.add_argument("--dem",         default=None)
    p.add_argument("--io-workers",  type=int, default=6,
                   help="Threads pour I/O bandes (défaut: 6)")
    p.add_argument("--cpu-workers", type=int, default=4,
                   help="Threads pour corrélation (défaut: 4)")
    args = p.parse_args()

    if args.build_gri:
        if not args.scenes:
            p.error("--build-gri nécessite une scène S2 en argument")
        build_gri(
            sentinel2_safe=args.scenes[0],
            gri_out=args.gri_out,
            resolution=args.resolution,
        )
        print(f"\n✓ GRI créé → {args.gri_out}")
        raise SystemExit(0)

    if not args.gri:
        p.error("--gri est obligatoire (ou utilise --build-gri pour en créer un)")
    if not args.scenes:
        p.error("Fournis au moins une scène à traiter")

    cfg = GRIConfig(
        gri_path=Path(args.gri),
        target_resolution=args.resolution,
        max_shift_pixels=args.max_shift,
        n_crops=args.n_crops,
        min_crops_for_consensus=args.min_crops,
        max_shift_disagreement_px=args.max_mad,
        output_dir=Path(args.outdir),
        sentinel2_bands=args.s2_bands.split(","),
        landsat_optical_bands=args.ls_bands.split(","),
        dem_path=Path(args.dem) if args.dem else None,
        do_orthorectify=bool(args.dem),
        io_workers=args.io_workers,
        cpu_workers=args.cpu_workers,
    )

    run_pipeline(args.scenes, cfg)
