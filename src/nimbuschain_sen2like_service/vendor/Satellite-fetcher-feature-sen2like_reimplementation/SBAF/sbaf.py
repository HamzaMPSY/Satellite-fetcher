from __future__ import annotations

import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Tuple

import dask
import dask.array as da
import numpy as np
import rasterio
from dask.diagnostics import ProgressBar
from rasterio.enums import Resampling

log = logging.getLogger("sbaf")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")


# ---------------------------------------------------------------------------
# Coefficient tables  (from HLS Guide v1.4 / sen2like)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SbafParams:
    slope: float
    offset: float

    def __repr__(self) -> str:
        return f"SbafParams(slope={self.slope:.6f}, offset={self.offset:.6f})"


# OLI-like coefficients: transform S2 → OLI-equivalent  (used to derive sen2like coefs)
_OLI_LIKE: Dict[str, Dict[str, Dict[str, SbafParams]]] = {
    "Sentinel-2A": {
        "B01": SbafParams(0.9959, -0.0002),
        "B02": SbafParams(0.9778, -0.0040),
        "B03": SbafParams(1.0053, -0.0009),
        "B04": SbafParams(0.9765,  0.0009),
        "B8A": SbafParams(0.9983, -0.0001),
        "B11": SbafParams(0.9987, -0.0011),
        "B12": SbafParams(1.0030, -0.0012),
    },
    "Sentinel-2B": {
        "B01": SbafParams(0.9959, -0.0002),
        "B02": SbafParams(0.9778, -0.0040),
        "B03": SbafParams(1.0075, -0.0008),
        "B04": SbafParams(0.9761,  0.0010),
        "B8A": SbafParams(0.9966,  0.0000),
        "B11": SbafParams(1.0000, -0.0003),
        "B12": SbafParams(0.9867,  0.0004),
    },
}

# Adaptive coefficients: factor = slope * NDVI + offset  (vegetation-sensitive)
_ADAPTIVE: Dict[str, SbafParams] = {
    "B01": SbafParams(-0.13363398,  0.92552824),
    "B02": SbafParams( 0.14222380,  1.05114394),
    "B03": SbafParams( 0.00898318,  0.97937428),
    "B04": SbafParams(-0.09417763,  1.02965730),
    "B8A": SbafParams(-0.00292645,  0.99517658),
    "B11": SbafParams( 0.01377031,  0.99593527),
    "B12": SbafParams( 0.01272434,  0.97395877),
}

# Landsat OLI band number → S2 band name
_LS_TO_S2: Dict[str, str] = {
    "B1": "B01",
    "B2": "B02",
    "B3": "B03",
    "B4": "B04",
    "B5": "B8A",
    "B6": "B11",
    "B7": "B12",
}

# Default adaptive band candidates (S2 naming)
DEFAULT_ADAPTIVE_BANDS = ("B02", "B03", "B04", "B8A", "B11", "B12")


# ---------------------------------------------------------------------------
# Static coefficient derivation
# ---------------------------------------------------------------------------

def get_static_coefs(mission: str, target: str = "Sentinel-2A") -> Dict[str, SbafParams]:
    """Derive static coefs that transform Landsat → target S2 mission."""
    if mission not in ("LANDSAT_8", "LANDSAT_9"):
        raise ValueError(f"Unsupported mission: {mission}")

    oli_like = _OLI_LIKE[target]
    coefs: Dict[str, SbafParams] = {}
    for s2_band, fwd in oli_like.items():
        slope  =  1.0 / fwd.slope
        offset = -fwd.offset / fwd.slope
        coefs[s2_band] = SbafParams(slope, offset)
    return coefs


# ---------------------------------------------------------------------------
# Dask-based processing helpers
# ---------------------------------------------------------------------------

def _apply_static_dask(arr: da.Array, params: SbafParams) -> da.Array:
    """out = arr * slope + offset  (element-wise, Dask)."""
    return arr * params.slope + params.offset


def _apply_adaptive_dask(
    arr: da.Array,
    ndvi: da.Array,
    static_params: SbafParams,
    adaptive_params: SbafParams,
    ndvi_threshold: float = 0.1,
) -> da.Array:
    factor = adaptive_params.slope * ndvi + adaptive_params.offset
    adaptive_out = arr * factor
    static_out   = _apply_static_dask(arr, static_params)

    use_static = ndvi <= ndvi_threshold
    out = da.where(use_static, static_out, adaptive_out)
    return out


def _restore_nodata(out: da.Array, original: da.Array, nodata_val: float = -9999.0) -> da.Array:
    return da.where(original == nodata_val, nodata_val, out)


# ---------------------------------------------------------------------------
# Raster I/O
# ---------------------------------------------------------------------------

def _read_band(path: str | Path, chunks: int = 1024) -> Tuple[da.Array, dict]:
    """Read a single-band GeoTIFF as a Dask array."""
    with rasterio.open(path) as src:
        meta = src.profile.copy()
        data = src.read(1).astype(np.float32)
    arr = da.from_array(data, chunks=chunks)
    return arr, meta


def _write_band(path: str | Path, arr: da.Array, meta: dict, nodata: float = -9999.0) -> None:
    meta = meta.copy()
    meta.update(dtype="float32", count=1, compress="lzw", nodata=nodata)
    result = arr.compute().astype(np.float32)
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(result, 1)
        dst.update_tags(data_type="boa_reflectance")


def _compute_ndvi(red: da.Array, nir: da.Array) -> da.Array:
    denom = nir + red
    safe_denom = da.where(denom == 0, 1.0, denom)
    ndvi = da.where(denom == 0, 0.0, (nir - red) / safe_denom)
    return ndvi.clip(-1.0, 1.0)


def _resample_to_shape(arr: np.ndarray, target_shape: Tuple[int, int]) -> np.ndarray:
    from rasterio.transform import from_bounds
    from rasterio import MemoryFile

    h, w = arr.shape
    th, tw = target_shape
    if (h, w) == (th, tw):
        return arr

    src_transform = from_bounds(0, 0, w, h, w, h)
    dst_transform = from_bounds(0, 0, w, h, tw, th)

    out = np.empty(target_shape, dtype=np.float32)
    with MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff", height=h, width=w, count=1,
            dtype="float32", crs="EPSG:4326", transform=src_transform,
        ) as dataset:
            dataset.write(arr.astype(np.float32), 1)
        with memfile.open() as dataset:
            rasterio.warp.reproject(
                source=rasterio.band(dataset, 1),
                destination=out,
                src_transform=src_transform,
                dst_transform=dst_transform,
                src_crs="EPSG:4326",
                dst_crs="EPSG:4326",
                resampling=Resampling.bilinear,
            )
    return out


# ---------------------------------------------------------------------------
# Band file discovery
# ---------------------------------------------------------------------------

def _find_band_files(scene_dir: str | Path) -> Dict[str, Path]:
    scene_dir = Path(scene_dir)
    bands: Dict[str, Path] = {}

    # Try SR bands first (atm correction output)
    for path in scene_dir.glob("*_SR_B*.TIF"):
        m = re.search(r"_SR_(B\d+)\.TIF$", path.name)
        if m:
            bands[m.group(1)] = path

    # Fallback: plain _B*.TIF (geo step output or raw scene)
    if not bands:
        for path in scene_dir.glob("*_B*.TIF"):
            m = re.search(r"_(B\d+)\.TIF$", path.name)
            if m and not any(x in path.name for x in ("QA", "SAA", "SZA", "VAA", "VZA")):
                bands[m.group(1)] = path

    return bands


# ---------------------------------------------------------------------------
# Main SBAF processor
# ---------------------------------------------------------------------------

@dataclass
class SbafProcessor:
    mission: str = "LANDSAT_8"
    s2_target: str = "Sentinel-2A"
    adaptive: bool = True
    adaptive_bands: Tuple[str, ...] = DEFAULT_ADAPTIVE_BANDS
    chunks: int = 1024
    output_scale: float = 1.0
    input_is_reflectance: bool = False
    nodata: float = -9999.0

    # internal state filled during processing
    _static_coefs: Dict[str, SbafParams] = field(default_factory=dict, init=False, repr=False)
    _used_params:  Dict[str, SbafParams] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        self._static_coefs = get_static_coefs(self.mission, self.s2_target)
        log.info("Static coefs derived for %s → %s", self.mission, self.s2_target)
        for s2b, p in self._static_coefs.items():
            log.debug("  %s  %s", s2b, p)

    # ------------------------------------------------------------------
    def process_scene(
        self,
        scene_dir: str | Path,
        output_dir: str | Path,
        bands: Optional[list[str]] = None,
        in_executor: bool = False,
    ) -> Dict[str, Path]:
        
        scene_dir  = Path(scene_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        band_files = _find_band_files(scene_dir)
        if not band_files:
            raise FileNotFoundError(f"No SR band files found in {scene_dir}")

        if bands:
            band_files = {k: v for k, v in band_files.items() if k in bands}

        ndvi_tmp_path: Optional[str] = None
        ndvi_shape: Optional[Tuple[int, int]] = None

        if self.adaptive and "B4" in band_files and "B5" in band_files:
            log.info("Computing NDVI from B4 (Red) and B5 (NIR)…")
            red_arr, red_meta = _read_band(band_files["B4"], self.chunks)
            nir_arr, _        = _read_band(band_files["B5"], self.chunks)
            red_rf   = self._to_reflectance(red_arr)
            nir_rf   = self._to_reflectance(nir_arr)
            ndvi_da  = _compute_ndvi(red_rf, nir_rf)
            ndvi_np  = ndvi_da.compute().astype(np.float32)
            ndvi_shape = ndvi_np.shape

            # Write to a temp file so executors can load it from disk.
            tmp_fd, ndvi_tmp_path = tempfile.mkstemp(suffix="_ndvi.tif")
            os.close(tmp_fd)
            ndvi_meta = red_meta.copy()
            ndvi_meta.update(dtype="float32", count=1, nodata=-9999.0)
            with rasterio.open(ndvi_tmp_path, "w", **ndvi_meta) as dst:
                dst.write(ndvi_np, 1)
            log.info("[sbaf] NDVI written to temp file: %s", ndvi_tmp_path)

        # Build job list
        jobs = []
        for ls_band, in_path in sorted(band_files.items()):
            s2_band = _LS_TO_S2.get(ls_band)
            if s2_band is None:
                log.debug("No S2 mapping for %s, skipping", ls_band)
                continue
            if s2_band not in self._static_coefs:
                log.warning("No static coef for %s (%s), skipping", s2_band, ls_band)
                continue
            jobs.append((ls_band, str(in_path), s2_band))

        if not jobs:
            if ndvi_tmp_path and os.path.exists(ndvi_tmp_path):
                os.remove(ndvi_tmp_path)
            return {}

        try:
            outputs = self._dispatch_jobs(
                jobs,
                output_dir=output_dir,
                ndvi_tmp_path=ndvi_tmp_path,
                ndvi_shape=ndvi_shape,
                in_executor=in_executor,
            )
        finally:
            # Always clean up the NDVI temp file, even on error.
            if ndvi_tmp_path and os.path.exists(ndvi_tmp_path):
                os.remove(ndvi_tmp_path)
                log.debug("[sbaf] NDVI temp file cleaned up")

        return outputs

    # ------------------------------------------------------------------
    def _dispatch_jobs(
        self,
        jobs: list,
        output_dir: Path,
        ndvi_tmp_path: Optional[str],
        ndvi_shape: Optional[Tuple[int, int]],
        in_executor: bool,
    ) -> Dict[str, Path]:

        force_sequential = os.environ.get("SBAF_FORCE_SEQUENTIAL", "").lower() in ("1", "true", "yes")
        if force_sequential:
            log.info("[sbaf] SBAF_FORCE_SEQUENTIAL active — running sequential path.")
            return self._process_jobs_sequential(jobs, output_dir, ndvi_tmp_path, ndvi_shape)

        if in_executor:
            return self._process_jobs_sequential(jobs, output_dir, ndvi_tmp_path, ndvi_shape)

        # For small band counts, JVM startup (~6s) dominates the work —
        # run sequentially instead. Matches the pipeline-wide threshold
        # used by the BRDF and fusion steps.
        SPARK_THRESHOLD = 16
        if len(jobs) < SPARK_THRESHOLD:
            log.info(
                "[sbaf] %d jobs below Spark threshold (%d) — running sequential.",
                len(jobs), SPARK_THRESHOLD,
            )
            return self._process_jobs_sequential(jobs, output_dir, ndvi_tmp_path, ndvi_shape)

        return self._process_jobs_spark(jobs, output_dir, ndvi_tmp_path, ndvi_shape)

    def _process_jobs_sequential(
        self,
        jobs: list,
        output_dir: Path,
        ndvi_tmp_path: Optional[str],
        ndvi_shape: Optional[Tuple[int, int]],
    ) -> Dict[str, Path]:
        log.info(
            "[sbaf] Processing %d SBAF bands with threads (%d workers)…",
            len(jobs), min(len(jobs), 6),
        )
        outputs: Dict[str, Path] = {}

        # Load NDVI once from the temp file if available — shared across threads.
        ndvi_np: Optional[np.ndarray] = None
        if ndvi_tmp_path and os.path.exists(ndvi_tmp_path):
            with rasterio.open(ndvi_tmp_path) as src:
                ndvi_np = src.read(1).astype(np.float32)

        # Bands are independent: read-transform-write pipelines that share
        # only the read-only NDVI array. Threading is safe and avoids JVM
        # startup overhead of Spark for small job counts.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _run_one(job):
            ls_band, in_path_str, s2_band = job
            out_path = self._process_single_band(
                ls_band, in_path_str, s2_band,
                output_dir, ndvi_np, ndvi_shape,
            )
            return ls_band, out_path

        n_workers = min(len(jobs), 6)
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_run_one, job): job for job in jobs}
            for future in as_completed(futures):
                ls_band, out_path = future.result()
                if out_path is not None:
                    outputs[ls_band] = out_path

        return outputs

    def _process_jobs_spark(
        self,
        jobs: list,
        output_dir: Path,
        ndvi_tmp_path: Optional[str],
        ndvi_shape: Optional[Tuple[int, int]],
    ) -> Dict[str, Path]:
        """Dispatch SBAF bands as Spark tasks (driver-only)."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        sc    = spark.sparkContext

        bc_static_coefs   = sc.broadcast(self._static_coefs)
        bc_adaptive_coefs = sc.broadcast(_ADAPTIVE)
        bc_adaptive_bands = sc.broadcast(self.adaptive_bands)
        bc_chunks         = sc.broadcast(self.chunks)
        bc_output_scale   = sc.broadcast(self.output_scale)
        bc_nodata         = sc.broadcast(self.nodata)
        bc_output_dir     = sc.broadcast(str(output_dir))
        bc_use_adaptive   = sc.broadcast(self.adaptive)
        bc_input_is_refl  = sc.broadcast(self.input_is_reflectance)
        bc_ndvi_path      = sc.broadcast(ndvi_tmp_path)   # path string, not array
        bc_ndvi_shape     = sc.broadcast(ndvi_shape)
        bc_sbaf_dir       = sc.broadcast(str(Path(__file__).resolve().parent))

        def _process_band(job):
            import sys as _sys
            _sbaf_dir = bc_sbaf_dir.value
            if _sbaf_dir not in _sys.path:
                _sys.path.insert(0, _sbaf_dir)

            ls_band, in_path_str, s2_band = job
            import numpy as _np
            import rasterio as _rio
            import dask.array as _da
            from pathlib import Path as _P
            from sbaf import (
                _read_band, _write_band, _apply_static_dask, _apply_adaptive_dask,
                _restore_nodata, _resample_to_shape,
            )

            chunks       = bc_chunks.value
            nodata       = bc_nodata.value
            output_scale = bc_output_scale.value
            out_dir      = _P(bc_output_dir.value)
            in_path      = _P(in_path_str)

            arr, meta = _read_band(in_path, chunks)

            # Convert to reflectance
            if bc_input_is_refl.value:
                arr_rf = _da.where(arr == _np.float32(nodata), 0.0, _da.clip(arr, 0.0, 1.0))
            else:
                arr_rf = _da.where(arr > 0, arr * _np.float32(0.0000275) - _np.float32(0.2), 0.0)
                arr_rf = arr_rf.clip(0.0, 1.0)

            static_params = bc_static_coefs.value[s2_band]

            ndvi_path = bc_ndvi_path.value
            use_adaptive = (
                bc_use_adaptive.value
                and s2_band in bc_adaptive_bands.value
                and ndvi_path is not None
            )

            if use_adaptive:
                import os as _os
                if not _os.path.exists(ndvi_path):
                    use_adaptive = False  # temp file missing — fall back gracefully
                else:
                    with _rio.open(ndvi_path) as _src:
                        ndvi_np = _src.read(1).astype(_np.float32)
                    ndvi_shape = bc_ndvi_shape.value
                    if ndvi_np.shape != arr_rf.shape:
                        ndvi_np = _resample_to_shape(ndvi_np, arr_rf.shape)
                    ndvi_ready   = _da.from_array(ndvi_np, chunks=chunks)
                    adaptive_params = bc_adaptive_coefs.value[s2_band]
                    adjusted = _apply_adaptive_dask(arr_rf, ndvi_ready, static_params, adaptive_params)
            if not use_adaptive:
                adjusted = _apply_static_dask(arr_rf, static_params)

            adjusted = _restore_nodata(adjusted, arr_rf, nodata_val=nodata)
            adjusted = adjusted * output_scale

            out_stem = in_path.stem.replace("_SR_", "_SBAF_")
            out_path = out_dir / f"{out_stem}.TIF"
            _write_band(out_path, adjusted, meta, nodata=nodata)

            return (ls_band, str(out_path))

        log.info("[sbaf] Processing %d SBAF bands via Spark…", len(jobs))
        spark_results = (
            sc.parallelize(jobs, numSlices=len(jobs))
            .map(_process_band)
            .collect()
        )

        for bc in [bc_static_coefs, bc_adaptive_coefs, bc_adaptive_bands,
                   bc_chunks, bc_output_scale, bc_nodata, bc_output_dir,
                   bc_use_adaptive, bc_input_is_refl, bc_ndvi_path,
                   bc_ndvi_shape, bc_sbaf_dir]:
            bc.unpersist()

        outputs: Dict[str, Path] = {}
        for ls_band, out_path_str in spark_results:
            outputs[ls_band] = Path(out_path_str)
            self._used_params[ls_band] = (
                _ADAPTIVE.get(_LS_TO_S2[ls_band])
                if self.adaptive and _LS_TO_S2.get(ls_band) in self.adaptive_bands
                and ndvi_tmp_path is not None
                else self._static_coefs.get(_LS_TO_S2.get(ls_band))
            )

        return outputs

    def _process_single_band(
        self,
        ls_band: str,
        in_path_str: str,
        s2_band: str,
        output_dir: Path,
        ndvi_np: Optional[np.ndarray],
        ndvi_shape: Optional[Tuple[int, int]],
    ) -> Optional[Path]:
        try:
            arr, meta = _read_band(in_path_str, self.chunks)
            arr_rf    = self._to_reflectance(arr)

            static_params = self._static_coefs[s2_band]
            use_adaptive  = (
                self.adaptive
                and s2_band in self.adaptive_bands
                and ndvi_np is not None
            )

            if use_adaptive:
                ndvi_arr = ndvi_np
                if ndvi_arr.shape != arr_rf.shape:
                    ndvi_arr = _resample_to_shape(ndvi_arr, arr_rf.shape)
                ndvi_ready      = da.from_array(ndvi_arr, chunks=self.chunks)
                adaptive_params = _ADAPTIVE[s2_band]
                adjusted = _apply_adaptive_dask(arr_rf, ndvi_ready, static_params, adaptive_params)
            else:
                adjusted = _apply_static_dask(arr_rf, static_params)

            adjusted = _restore_nodata(adjusted, arr_rf, nodata_val=self.nodata)
            adjusted = adjusted * self.output_scale

            in_path  = Path(in_path_str)
            out_stem = in_path.stem.replace("_SR_", "_SBAF_")
            out_path = output_dir / f"{out_stem}.TIF"
            _write_band(out_path, adjusted, meta, nodata=self.nodata)
            log.info("[sbaf] Written: %s", out_path.name)
            return out_path

        except Exception as exc:
            log.error("[sbaf] Band %s failed: %s", ls_band, exc)
            return None

    def _to_reflectance(self, arr: da.Array) -> da.Array:
        """Convert input array to [0, 1] reflectance.

        If input_is_reflectance is True (data from atmospheric correction step):
          - Mask the pipeline nodata sentinel (-9999) with 0.
          - Clip to [0, 1].
        If input_is_reflectance is False (raw Collection-2 DN):
          - Apply scale (0.0000275) and offset (-0.2).
          - Clip to [0, 1].
        """
        if self.input_is_reflectance:

            return da.where(
                arr == np.float32(self.nodata),
                0.0,
                da.clip(arr, 0.0, 1.0),
            )
        else:
            # Raw Collection-2 DN — apply scale + offset.
            rf = da.where(arr > 0, arr * np.float32(0.0000275) - np.float32(0.2), 0.0)
            return rf.clip(0.0, 1.0)

    def _align_ndvi(
        self,
        ndvi: da.Array,
        target: da.Array,
        ndvi_shape: Optional[Tuple[int, int]],
    ) -> da.Array:
        """Resample NDVI to match target array shape if needed."""
        if ndvi.shape == target.shape:
            return ndvi
        log.info("  Resampling NDVI %s → %s", ndvi.shape, target.shape)
        ndvi_np   = ndvi.compute()
        resampled = _resample_to_shape(ndvi_np, target.shape)
        return da.from_array(resampled, chunks=self.chunks)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_output(
    original_path: str | Path,
    adjusted_path: str | Path,
    s2_band: str,
    sample_frac: float = 0.01,
    n_hist_bins: int = 100,
) -> dict:
    with rasterio.open(original_path) as src:
        orig_dn = src.read(1).astype(np.float32)

    with rasterio.open(adjusted_path) as src:
        adj = src.read(1).astype(np.float32)

    valid = orig_dn > 0
    orig_rf = np.where(valid, orig_dn * 0.0000275 - 0.2, np.nan).clip(0, 1)

    flat_orig_full = orig_rf[valid].ravel()
    flat_adj_full  = adj[valid].ravel()

    bin_edges = np.linspace(0.0, 1.0, n_hist_bins + 1)
    hist_orig, _ = np.histogram(flat_orig_full, bins=bin_edges)
    hist_adj,  _ = np.histogram(flat_adj_full,  bins=bin_edges)
    bin_width      = bin_edges[1] - bin_edges[0]
    hist_orig_norm = hist_orig / (hist_orig.sum() * bin_width)
    hist_adj_norm  = hist_adj  / (hist_adj.sum()  * bin_width)
    hist_similarity = float(np.minimum(hist_orig_norm, hist_adj_norm).sum() * bin_width)

    flat_orig = flat_orig_full
    flat_adj  = flat_adj_full
    if sample_frac < 1.0:
        n = max(1000, int(len(flat_orig) * sample_frac))
        idx = np.random.choice(len(flat_orig), min(n, len(flat_orig)), replace=False)
        flat_orig = flat_orig[idx]
        flat_adj  = flat_adj[idx]

    diff      = flat_adj - flat_orig
    mae       = float(np.mean(np.abs(diff)))
    rmse      = float(np.sqrt(np.mean(diff ** 2)))
    safe_orig = np.where(flat_orig > 0.01, flat_orig, 1.0)
    ratio     = np.where(flat_orig > 0.01, flat_adj / safe_orig, np.nan)

    stats = {
        "band":              s2_band,
        "n_valid_pixels":    int(valid.sum()),
        "orig_mean":         float(np.nanmean(flat_orig)),
        "orig_std":          float(np.nanstd(flat_orig)),
        "adj_mean":          float(np.nanmean(flat_adj)),
        "adj_std":           float(np.nanstd(flat_adj)),
        "diff_mean":         float(np.nanmean(diff)),
        "diff_std":          float(np.nanstd(diff)),
        "diff_max_abs":      float(np.nanmax(np.abs(diff))),
        "mae":               mae,
        "rmse":              rmse,
        "ratio_mean":        float(np.nanmean(ratio)),
        "ratio_std":         float(np.nanstd(ratio)),
        "nodata_preserved":  bool(np.all(adj[~valid] == 0.0)),
        "hist_bin_centers":  ((bin_edges[:-1] + bin_edges[1:]) / 2).tolist(),
        "hist_orig":         hist_orig_norm.tolist(),
        "hist_adj":          hist_adj_norm.tolist(),
        "hist_similarity":   hist_similarity,
    }
    return stats


def print_validation_report(stats_list: list[dict]) -> None:
    header = (
        f"{'Band':<6} {'N valid':>10} {'Orig μ':>8} {'Adj μ':>8} "
        f"{'Δμ':>8} {'MAE':>8} {'RMSE':>8} {'Ratio μ':>8} {'Hist∩':>6} {'NoData':>7}"
    )
    print("\n" + "=" * len(header))
    print("SBAF Validation Report")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for s in stats_list:
        nd = "✓" if s["nodata_preserved"] else "✗"
        print(
            f"{s['band']:<6} {s['n_valid_pixels']:>10,} "
            f"{s['orig_mean']:>8.4f} {s['adj_mean']:>8.4f} "
            f"{s['diff_mean']:>+8.5f} {s['mae']:>8.5f} {s['rmse']:>8.5f} "
            f"{s['ratio_mean']:>8.5f} {s['hist_similarity']:>6.3f} {nd:>7}"
        )
    print("=" * len(header))

    for s in stats_list:
        if abs(s["diff_mean"]) > 0.05:
            log.warning("Band %s: large mean diff (%.4f) — check coefficients", s["band"], s["diff_mean"])
        if not s["nodata_preserved"]:
            log.error("Band %s: nodata NOT preserved in output!", s["band"])
        if not (0.9 <= s["ratio_mean"] <= 1.1):
            log.warning("Band %s: unusual mean ratio (%.4f)", s["band"], s["ratio_mean"])
        if s["hist_similarity"] < 0.90:
            log.warning("Band %s: low histogram similarity (%.3f) — large spectral shift", s["band"], s["hist_similarity"])


def plot_histograms(
    stats_list: list[dict],
    output_path: str | Path,
    scene_name: str = "",
) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        log.error("matplotlib is required for histogram plots: pip install matplotlib")
        return

    n_bands = len(stats_list)
    fig = plt.figure(figsize=(6 * n_bands, 5), facecolor="#0e1117")
    fig.patch.set_facecolor("#0e1117")

    BAND_COLOURS = {
        "B01": ("#7ec8e3", "#2a7fa5"),
        "B02": ("#4a90d9", "#1a5fa8"),
        "B03": ("#5cb85c", "#2d7a2d"),
        "B04": ("#d9534f", "#a02020"),
        "B8A": ("#8e6fba", "#5a3d8a"),
        "B11": ("#e8a020", "#b06010"),
        "B12": ("#c0392b", "#7b1818"),
    }

    title = f"SBAF Histogram Comparison — {scene_name}" if scene_name else "SBAF Histogram Comparison"
    fig.suptitle(title, color="white", fontsize=13, fontweight="bold", y=1.01)

    for i, s in enumerate(stats_list):
        band    = s["band"]
        centers = np.array(s["hist_bin_centers"])
        h_orig  = np.array(s["hist_orig"])
        h_adj   = np.array(s["hist_adj"])
        col_adj, col_orig = BAND_COLOURS.get(band, ("#aaaaaa", "#555555"))

        ax = fig.add_subplot(1, n_bands, i + 1, facecolor="#1a1e2a")
        ax.fill_between(centers, h_orig, alpha=0.45, color=col_orig, label="Original")
        ax.fill_between(centers, h_adj,  alpha=0.55, color=col_adj,  label="Adjusted")
        ax.plot(centers, h_orig, color=col_orig, linewidth=1.2, alpha=0.9)
        ax.plot(centers, h_adj,  color=col_adj,  linewidth=1.5)

        ax2 = ax.twinx()
        diff_hist = h_adj - h_orig
        ax2.bar(centers, diff_hist, width=(centers[1] - centers[0]) * 0.9,
                color=np.where(diff_hist >= 0, "#48bb78", "#fc8181"),
                alpha=0.35, label="Δ (adj−orig)")
        ax2.axhline(0, color="white", linewidth=0.5, alpha=0.4)
        ax2.tick_params(colors="#888888", labelsize=7)
        ax2.set_ylabel("Δ density", color="#888888", fontsize=7)

        ax.set_title(
            f"{band}\nMAE={s['mae']:.5f}  RMSE={s['rmse']:.5f}\nHist∩={s['hist_similarity']:.3f}",
            color="white", fontsize=8.5, pad=6
        )
        ax.set_xlabel("Reflectance", color="#aaaaaa", fontsize=8)
        ax.set_ylabel("Density", color="#aaaaaa", fontsize=8)
        ax.tick_params(colors="#888888", labelsize=7)
        ax.spines[:].set_color("#333344")

        if i == 0:
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(lines1 + lines2, labels1 + labels2,
                      fontsize=7, framealpha=0.3,
                      labelcolor="white", facecolor="#1a1e2a")

    plt.tight_layout()
    output_path = Path(output_path)
    plt.savefig(output_path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    log.info("Histogram plot saved → %s", output_path)


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="SBAF — Spectral Band Adjustment Factor  (Landsat → Sentinel-2)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("scene_dir",  help="Path to Landsat L2SP scene directory")
    parser.add_argument("output_dir", help="Directory for SBAF-adjusted output TIFs")
    parser.add_argument("--mission",  default="LANDSAT_8",
                        choices=["LANDSAT_8", "LANDSAT_9"])
    parser.add_argument("--target",   default="Sentinel-2A",
                        choices=["Sentinel-2A", "Sentinel-2B"])
    parser.add_argument("--no-adaptive", dest="adaptive", action="store_false")
    parser.add_argument("--adaptive-bands", nargs="+",
                        default=list(DEFAULT_ADAPTIVE_BANDS))
    parser.add_argument("--bands", nargs="+")
    parser.add_argument("--chunks", type=int, default=1024)
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--histogram", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger("sbaf").setLevel(logging.DEBUG)

    dask.config.set(scheduler="threads", num_workers=args.workers)
    log.info("Dask: threads, %d workers", args.workers)

    processor = SbafProcessor(
        mission        = args.mission,
        s2_target      = args.target,
        adaptive       = args.adaptive,
        adaptive_bands = tuple(args.adaptive_bands),
        chunks         = args.chunks,
    )

    t_start = time.perf_counter()
    outputs = processor.process_scene(
        args.scene_dir, args.output_dir, args.bands,
        in_executor=False,
    )
    log.info("All bands processed in %.1f s", time.perf_counter() - t_start)

    if args.validate:
        band_files = _find_band_files(args.scene_dir)
        stats_list = []
        for ls_band, out_path in outputs.items():
            s2_band = _LS_TO_S2[ls_band]
            s = validate_output(band_files[ls_band], out_path, s2_band)
            stats_list.append(s)
        print_validation_report(stats_list)

        if args.histogram:
            scene_name = Path(args.scene_dir).name
            hist_path  = Path(args.output_dir) / f"{scene_name}_sbaf_histograms.png"
            plot_histograms(stats_list, hist_path, scene_name=scene_name)


if __name__ == "__main__":
    main()
