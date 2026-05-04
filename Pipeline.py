from __future__ import annotations
from Monitoring.metrics import MetricsCollector
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import argparse
import hashlib
import importlib
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import numpy as np
import rasterio


# ---------------------------------------------------------------------------
# Pipeline-wide constants
# ---------------------------------------------------------------------------

PIPELINE_NODATA: float = -9999.0
PIPELINE_VERSION_FALLBACK: str = "unknown"

NBAR_BAND_MAP: dict[str, tuple[str, str]] = {
    # logical name : (SBAF glob pattern, NBAR filename)
    "Blue":  ("*_SBAF_B2.TIF", "NBAR_Blue.tif"),
    "Green": ("*_SBAF_B3.TIF", "NBAR_Green.tif"),
    "Red":   ("*_SBAF_B4.TIF", "NBAR_Red.tif"),
    "NIR":   ("*_SBAF_B5.TIF", "NBAR_NIR.tif"),
    "SWIR1": ("*_SBAF_B6.TIF", "NBAR_SWIR1.tif"),
    "SWIR2": ("*_SBAF_B7.TIF", "NBAR_SWIR2.tif"),
}

_BASE: Path
_atm_mod: object | None = None
_bootstrap_done: bool = False


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sen2like")


# ---------------------------------------------------------------------------
# Pipeline version — cached at module load
# ---------------------------------------------------------------------------

def _resolve_pipeline_version() -> str:
    """Return the short git SHA of this file's repo, or the fallback string."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=3,
            cwd=str(Path(__file__).resolve().parent),
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return PIPELINE_VERSION_FALLBACK


_PIPELINE_VERSION: str = _resolve_pipeline_version()


def _pipeline_version() -> str:
    """Public accessor — kept as a function for backward compatibility."""
    return _PIPELINE_VERSION


# ---------------------------------------------------------------------------
# Exception hook — installed early so failures during bootstrap are captured
# ---------------------------------------------------------------------------

def _install_excepthook() -> None:
    def _hook(exc_type, exc_value, exc_traceback):
        if exc_type in (SystemExit, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        log.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = _hook


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

_SUBDIRS = (
    "Geometric_Processing",
    "atmospheric_correction",
    "SBAF",
    "Valid_Pixel_Mask",
    "BRDF_Adjustment",
    "data_fusion",
    "Packaging",
)


def _bootstrap(base_dir: str | Path | None = None, *, force: bool = False) -> None:
    """Wire up sys.path and import the atmospheric correction module."""
    global _BASE, _atm_mod, _bootstrap_done

    resolved = Path(
        str(base_dir)
        if base_dir is not None
        else os.environ.get(
            "LANDSAT_UPSAMPLING_BASE",
            str(Path(__file__).resolve().parent),
        )
    ).resolve()

    if _bootstrap_done and not force:
        if resolved != _BASE:
            raise RuntimeError(
                f"_bootstrap() already called with base={_BASE}; "
                f"cannot reinitialise to {resolved}. Pass force=True if "
                "this is intentional (e.g. Spark worker reuse)."
            )
        return

    if _bootstrap_done and force:
        old_entries = {str(_BASE / subdir) for subdir in _SUBDIRS}
        sys.path[:] = [p for p in sys.path if p not in old_entries]
        sys.modules.pop("atmospheric_correction.atmospheric_correction_pipeline", None)

    _BASE = resolved

    for subdir in _SUBDIRS:
        path = str(_BASE / subdir)
        if path not in sys.path:
            sys.path.insert(0, path)

    module_name = "atmospheric_correction.atmospheric_correction_pipeline"
    if force and module_name in sys.modules:
        sys.modules.pop(module_name, None)
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ImportError(
            f"Cannot load atmospheric correction module '{module_name}'. "
            "Check that the file exists and is a valid Python source."
        ) from exc
    _atm_mod = module

    _bootstrap_done = True


# ---------------------------------------------------------------------------
# Spark / thread helpers
# ---------------------------------------------------------------------------

_IN_SPARK_EXECUTOR_ENV = "IN_SPARK_EXECUTOR"
_SPARK_THRESHOLD_JOBS = 16


def _running_inside_spark_executor() -> bool:
    return os.environ.get(_IN_SPARK_EXECUTOR_ENV, "") == "1"


def _use_spark(n_jobs: int, force: bool = False) -> bool:
    if _running_inside_spark_executor():
        return False
    if force:
        return True
    return n_jobs >= _SPARK_THRESHOLD_JOBS


def _get_spark(app_name: str = "sen2like", workers: int = 4):
    from pyspark.sql import SparkSession

    existing = SparkSession.getActiveSession()
    if existing is not None:
        requested = f"local[{workers}]"
        actual = existing.sparkContext.master
        if requested != actual and not os.environ.get("SPARK_MASTER"):
            log.debug(
                "[spark] Session already active (master=%s); ignoring workers=%d.",
                actual, workers,
            )
        return existing

    master = os.environ.get("SPARK_MASTER", f"local[{workers}]")
    spark = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _run_parallel_threads(
    fn: Callable[[Any], Any],
    items: list,
    workers: int,
) -> list:
    if not items:
        return []
    effective_workers = min(workers, len(items), 32)
    results = []
    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = {pool.submit(fn, item): item for item in items}
        for future in as_completed(futures):
            results.append(future.result())
    return results


# ---------------------------------------------------------------------------
# Pipeline scaffolding
# ---------------------------------------------------------------------------

@dataclass
class Context:
    product_id: str
    working_dir: Path
    config: dict
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class Result:
    step: str
    success: bool
    elapsed: float = 0.0
    error: str = ""
    outputs: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def _utcnow() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _dict_hash(d: dict) -> str:
    return hashlib.sha256(
        json.dumps(d, sort_keys=True, default=str).encode()
    ).hexdigest()


def _input_paths_from_config(config: dict) -> dict[str, str]:
    keys = ("landsat_path", "s2_path", "gri_path", "gri_cache_dir", "dem_path", "lut_path")
    return {k: str(config[k]) for k in keys if config.get(k)}


def _nodata_or_default(src_nodata: Any) -> float:
    """Return the rasterio nodata value as float, or PIPELINE_NODATA if None."""
    return float(src_nodata) if src_nodata is not None else PIPELINE_NODATA


_SUPPORTED_SPACECRAFT = ("LANDSAT_8", "LANDSAT_9")
_DEFAULT_SPACECRAFT = "LANDSAT_8"


def _detect_spacecraft(landsat_path: str | Path) -> str:
    """Return 'LANDSAT_8' or 'LANDSAT_9' from the scene's MTL JSON."""
    scene_dir = Path(str(landsat_path))
    mtl_files = list(scene_dir.glob("*_MTL.json"))
    if not mtl_files:
        log.debug("[spacecraft] No MTL.json in %s — defaulting to %s",
                  scene_dir, _DEFAULT_SPACECRAFT)
        return _DEFAULT_SPACECRAFT

    try:
        mtl = json.loads(mtl_files[0].read_text())
        spacecraft = (
            mtl.get("LANDSAT_METADATA_FILE", {})
               .get("IMAGE_ATTRIBUTES", {})
               .get("SPACECRAFT_ID", _DEFAULT_SPACECRAFT)
        )
        spacecraft = spacecraft.replace(" ", "_").upper()
    except Exception as exc:
        log.warning("[spacecraft] Could not parse %s (%s) — defaulting to %s",
                    mtl_files[0].name, exc, _DEFAULT_SPACECRAFT)
        return _DEFAULT_SPACECRAFT

    if spacecraft not in _SUPPORTED_SPACECRAFT:
        log.warning(
            "[spacecraft] Unsupported spacecraft '%s' in %s — defaulting to %s. "
            "Supported: %s",
            spacecraft, mtl_files[0].name, _DEFAULT_SPACECRAFT, _SUPPORTED_SPACECRAFT,
        )
        return _DEFAULT_SPACECRAFT

    log.info("[spacecraft] Detected %s from %s", spacecraft, mtl_files[0].name)
    return spacecraft


def _spacecraft_short_tag(spacecraft: str) -> str:
    """Short tag used for LUT filenames and the atm module: 'L8' or 'L9'."""
    return "L9" if spacecraft == "LANDSAT_9" else "L8"


# ---------------------------------------------------------------------------
# Adaptive tile routing
# ---------------------------------------------------------------------------

def _read_qa_cloud_mask(
    scene_dir: Path,
    out_h: int,
    out_w: int,
) -> np.ndarray | None:
    qa_files = list(scene_dir.glob("*_QA_PIXEL.TIF"))
    if not qa_files:
        log.debug("[router] QA_PIXEL not found in %s — cloud fraction will be estimated", scene_dir)
        return None

    try:
        with rasterio.open(qa_files[0]) as src:
            qa = src.read(
                1,
                out_shape=(out_h, out_w),
                resampling=rasterio.enums.Resampling.nearest,
            ).astype(np.uint16)

        FILL_BIT = 0
        DILATED_CLOUD_BIT = 1
        CIRRUS_BIT = 2
        CLOUD_BIT = 3
        CLOUD_SHADOW_BIT = 4

        fill_mask = (qa >> FILL_BIT) & 1
        cloud_mask = (
            ((qa >> DILATED_CLOUD_BIT) & 1)
            | ((qa >> CIRRUS_BIT) & 1)
            | ((qa >> CLOUD_BIT) & 1)
            | ((qa >> CLOUD_SHADOW_BIT) & 1)
        )
        result = np.where(fill_mask, 0, cloud_mask).astype(np.uint8)
        log.debug(
            "[router] QA_PIXEL decoded: cloud/shadow pixels = %.1f%%",
            100.0 * result.sum() / max(result.size, 1),
        )
        return result

    except Exception as exc:
        log.warning("[router] QA_PIXEL read failed (%s) — falling back to brightness estimate", exc)
        return None


def _load_tile_array_for_routing(landsat_path: str) -> tuple[np.ndarray, dict]:
    scene_dir = Path(landsat_path)

    BAND_PATTERNS = [
        ("blue",  ["*_SR_B2.TIF", "*_B2.TIF"]),
        ("green", ["*_SR_B3.TIF", "*_B3.TIF"]),
        ("red",   ["*_SR_B4.TIF", "*_B4.TIF"]),
        ("nir",   ["*_SR_B5.TIF", "*_B5.TIF"]),
    ]
    MAX_DIM = 512

    resolved: list[tuple[str, Path | None]] = []
    target_h: int | None = None
    target_w: int | None = None

    for logical, patterns in BAND_PATTERNS:
        fpath = next(
            (candidates[0] for pat in patterns
             if (candidates := list(scene_dir.glob(pat)))),
            None,
        )
        resolved.append((logical, fpath))

        if target_h is None and fpath is not None:
            try:
                with rasterio.open(fpath) as src:
                    scale = max(1, src.width // MAX_DIM, src.height // MAX_DIM)
                    target_h = src.height // scale
                    target_w = src.width // scale
            except Exception as exc:
                log.debug("[router] Could not probe %s for shape (%s)", fpath, exc)

    if target_h is None or target_w is None:
        log.debug("[router] No readable band — using %d×%d fallback shape", MAX_DIM, MAX_DIM)
        target_h = target_w = MAX_DIM

    # Pass 2: read or zero-fill each band at the target shape.
    arrays: list[np.ndarray] = []
    band_names: list[str] = []

    for logical, fpath in resolved:
        if fpath is None:
            log.debug("[router] Band '%s' not found in %s — filling with zeros", logical, scene_dir)
            arrays.append(np.zeros((target_h, target_w), dtype=np.float32))
            band_names.append(logical.upper())
            continue

        with rasterio.open(fpath) as src:
            raw = src.read(
                1,
                out_shape=(target_h, target_w),
                resampling=rasterio.enums.Resampling.average,
            ).astype(np.float32)
            nd_val = _nodata_or_default(src.nodata)

        valid = raw != nd_val
        if valid.any():
            vmax = float(raw[valid].max())
            if vmax > 10000:
                raw = np.where(valid, raw * 0.0000275 - 0.2, nd_val)
            elif vmax > 2.0:
                vmin = float(raw[valid].min())
                rng = vmax - vmin
                if rng > 0:
                    raw = np.where(valid, (raw - vmin) / rng, nd_val)

        arrays.append(raw)
        band_names.append(logical.upper())

    tile = np.stack(arrays, axis=0)
    _, tile_h, tile_w = tile.shape

    sensor = "LANDSAT_8"
    mtl_json = list(scene_dir.glob("*_MTL.json"))
    if mtl_json:
        try:
            mtl = json.loads(mtl_json[0].read_text())
            spacecraft = (
                mtl.get("LANDSAT_METADATA_FILE", {})
                   .get("IMAGE_ATTRIBUTES", {})
                   .get("SPACECRAFT_ID", "LANDSAT_8")
            )
            sensor = spacecraft.replace(" ", "_").upper()
        except Exception:
            pass

    cloud_mask_arr = _read_qa_cloud_mask(scene_dir, tile_h, tile_w)

    meta: dict[str, Any] = {
        "tile_id":    scene_dir.name,
        "sensor":     sensor,
        "band_names": band_names,
        "nodata":     PIPELINE_NODATA,
    }
    if cloud_mask_arr is not None:
        meta["cloud_mask"] = cloud_mask_arr

    return tile, meta


def _route_landsat_scene(
    landsat_path: str,
    user_only: list[str] | None,
    *,
    fallback_on_error: bool = False,
) -> tuple[str, list[str] | None]:
    """Classify a Landsat scene and decide which pipeline steps to run."""
    from Routing.tile_router import profile_tile, classify_tile, route_tile, TileClass

    try:
        tile, meta = _load_tile_array_for_routing(landsat_path)
        profile = profile_tile(tile, meta)
        tile_class = classify_tile(profile)
        routed = route_tile(tile_class)

        cloud_source = "QA" if meta.get("cloud_mask") is not None else "est"
        log.info(
            "[router] %s → class=%-18s cloud=%.0f%% (%s)  NDVI=%+.3f  NDWI=%+.3f",
            Path(landsat_path).name,
            tile_class.value,
            profile.cloud_fraction * 100,
            cloud_source,
            profile.ndvi,
            profile.ndwi,
        )

        if tile_class == TileClass.SKIP:
            log.warning(
                "[router] SKIP %s (cloud=%.0f%%, nodata=%.0f%%) — tile will not be processed.",
                Path(landsat_path).name,
                profile.cloud_fraction * 100,
                profile.nodata_fraction * 100,
            )
            return tile_class.value, None

        if user_only:
            routed = [s for s in routed if s in user_only or s == _ALWAYS_RUN_STEP]
            log.info("[router] After --steps intersection: %s", routed)

        return tile_class.value, routed

    except Exception as exc:
        if not fallback_on_error:
            log.error(
                "[router] Routing FAILED for %s (%s). "
                "Re-run with --router-fallback-ok to process anyway with the full pipeline.",
                Path(landsat_path).name, exc,
            )
            raise

        log.error(
            "[router] Routing failed for %s (%s) — fallback enabled, "
            "running full pipeline. THIS INDICATES A ROUTER BUG; please investigate.",
            Path(landsat_path).name, exc,
        )
        # Preserve user's --steps filter if given, otherwise fall back to
        # the full pipeline. Never return None here: None is the SKIP sentinel.
        fallback_steps = list(user_only) if user_only else list(_ROUTABLE_CORE_STEPS)
        return "MIXED", fallback_steps


# ---------------------------------------------------------------------------
# Step that bypasses the `--steps` filter (runs even when not selected)
# ---------------------------------------------------------------------------

_ALWAYS_RUN_STEP = "packaging"

_ROUTABLE_CORE_STEPS = [
    "geometric_processing",
    "atmospheric_correction",
    "sbaf",
    "valid_pixel_mask",
    "brdf_adjustment",
    "data_fusion",
    "packaging",
]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _outputs_from_ctx_data(step_class: type, ctx_data: dict) -> list[str]:
    keys: Iterable[str] = getattr(step_class, "output_keys", ())
    paths: list[str] = []
    for key in keys:
        val = ctx_data.get(key)
        if val is None:
            continue
        p = Path(str(val))
        if p.exists():
            paths.append(str(p))
    return paths


class Pipeline:

    def __init__(self, config: dict, working_dir: str | Path):
        self.config = config
        self.working_dir = Path(working_dir)
        self.steps: list[type] = []
        self._version = _pipeline_version()  
        self._metrics: MetricsCollector | None = None

    def register(self, *step_classes: type) -> "Pipeline":
        self.steps.extend(step_classes)
        return self

    def _step_order(self) -> list[str]:
        return [cls.name for cls in self.steps]

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    def _manifest_path(self, product_id: str) -> Path:
        return self.working_dir / Path(product_id).name / "manifest.json"

    def _checkpoint_path(self, product_id: str) -> Path:
        return self.working_dir / Path(product_id).name / "checkpoint.json"

    # ------------------------------------------------------------------
    # Manifest I/O
    # ------------------------------------------------------------------

    def _load_manifest(self, product_id: str) -> dict:
        mp = self._manifest_path(product_id)

        if not mp.exists():
            cp = self._checkpoint_path(product_id)
            if cp.exists():
                try:
                    old = json.loads(cp.read_text())
                    completed = old.get("completed", [])
                    if completed:
                        log.info(
                            "[manifest] Migrating old checkpoint (%d steps) → manifest",
                            len(completed),
                        )
                    return self._seed_manifest_from_completed(product_id, completed)
                except (json.JSONDecodeError, OSError):
                    pass
            return self._empty_manifest(product_id)

        try:
            data = json.loads(mp.read_text())
            completed = [
                name for name, s in data.get("steps", {}).items()
                if s.get("status") == "success"
            ]
            if completed:
                log.info("[manifest] Resuming — already completed: %s", sorted(completed))
            return data
        except (json.JSONDecodeError, OSError):
            corrupt = mp.with_suffix(".json.corrupt")
            log.warning(
                "[manifest] Corrupt manifest at %s — renaming to %s and starting fresh.",
                mp, corrupt.name,
            )
            try:
                mp.rename(corrupt)
            except OSError:
                pass
            return self._empty_manifest(product_id)

    def _empty_manifest(self, product_id: str) -> dict:
        return {
            "product_id":       product_id,
            "pipeline_version": self._version,
            "config_hash":      _dict_hash(self.config),
            "started_at":       _utcnow(),
            "finished_at":      None,
            "input_paths":      _input_paths_from_config(self.config),
            "tile_class":       None,
            "steps_routed":     None,
            "steps":            {},
            "completed":        [],
        }

    def _seed_manifest_from_completed(self, product_id: str, completed: list[str]) -> dict:
        manifest = self._empty_manifest(product_id)
        for name in completed:
            manifest["steps"][name] = {
                "status":                   "success",
                "started_at":               None,
                "finished_at":              None,
                "elapsed":                  None,
                "outputs":                  [],
                "input_paths":              {},
                "config_hash":              None,
                "config_snapshot":          {},
                "error":                    None,
                "migrated_from_checkpoint": True,
            }
        manifest["completed"] = sorted(completed)
        return manifest

    def _save_manifest(self, product_id: str, manifest: dict) -> None:
        mp = self._manifest_path(product_id)
        mp.parent.mkdir(parents=True, exist_ok=True)

        manifest["completed"] = sorted(
            name for name, s in manifest.get("steps", {}).items()
            if s.get("status") == "success"
        )

        tmp = mp.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(manifest, indent=2, default=str))
        tmp.replace(mp)

        cp = self._checkpoint_path(product_id)
        tmp_cp = cp.with_suffix(".json.tmp")
        tmp_cp.write_text(json.dumps({"completed": manifest["completed"]}, indent=2))
        tmp_cp.replace(cp)

    def _mark_step_running(self, manifest: dict, step_name: str, config: dict) -> None:
        manifest["steps"][step_name] = {
            "status":          "running",
            "started_at":      _utcnow(),
            "finished_at":     None,
            "elapsed":         None,
            "outputs":         [],
            "input_paths":     _input_paths_from_config(self.config),
            "config_hash":     _dict_hash(config),
            "config_snapshot": config,
            "error":           None,
        }

    def _mark_step_success(
        self,
        manifest: dict,
        step_name: str,
        elapsed: float,
        outputs: list[str],
        step_return: dict | None = None,
    ) -> None:
        entry = manifest["steps"].setdefault(step_name, {})
        entry.update({
            "status":      "success",
            "finished_at": _utcnow(),
            "elapsed":     round(elapsed, 3),
            "outputs":     outputs,
            "error":       None,
        })
        if step_return:
            entry["step_return"] = {
                k: v for k, v in step_return.items()
                if isinstance(v, (str, int, float, bool, list, dict, type(None)))
            }

    def _mark_step_failed(
        self,
        manifest: dict,
        step_name: str,
        elapsed: float,
        error: str,
    ) -> None:
        entry = manifest["steps"].setdefault(step_name, {})
        entry.update({
            "status":      "failed",
            "finished_at": _utcnow(),
            "elapsed":     round(elapsed, 3),
            "error":       error,
        })

    # ------------------------------------------------------------------
    # Resume validity
    # ------------------------------------------------------------------

    def _step_is_valid(self, step_name: str, manifest: dict) -> bool:
        entry = manifest.get("steps", {}).get(step_name, {})
        if entry.get("status") != "success":
            return False

        stored_hash = entry.get("config_hash")
        if stored_hash is None:
            log.debug("[resume] %s: no config_hash (migrated checkpoint) — will rerun", step_name)
            return False
        if stored_hash != _dict_hash(self.config.get(step_name, {})):
            log.info("[resume] %s: config changed — invalidating", step_name)
            return False

        stored_inputs = entry.get("input_paths")
        if stored_inputs is None:
            log.debug("[resume] %s: no input_paths (old manifest) — will rerun", step_name)
            return False
        if stored_inputs != _input_paths_from_config(self.config):
            log.info("[resume] %s: input paths changed — invalidating", step_name)
            return False

        step_return = entry.get("step_return", {}) or {}
        if step_return.get("geo_skipped"):
            log.info(
                "[resume] %s: previously soft-skipped (%s) — invalidating "
                "so it can retry",
                step_name,
                step_return.get("geo_skip_reason", "unknown reason"),
            )
            return False

        for out_path in entry.get("outputs", []) or []:
            if out_path and not Path(out_path).exists():
                log.info(
                    "[resume] %s: recorded output missing (%s) — invalidating",
                    step_name, out_path,
                )
                return False

        return True

    def _invalidate_from(self, step_name: str, manifest: dict) -> None:
        order = self._step_order()
        try:
            start = order.index(step_name)
        except ValueError:
            return

        for name in order[start:]:
            entry = manifest.get("steps", {}).get(name)
            if entry and entry.get("status") == "success":
                log.info("[resume] Invalidating downstream step %s (config or inputs drifted)", name)
                entry["status"] = "invalidated"

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(
        self,
        product_id: str,
        only: list[str] | None = None,
        resume: bool = True,
        tile_class: str | None = None,
        steps_routed: list[str] | None = None,
    ) -> list[Result]:
        self._assert_no_duplicate_step_names()

        ctx = Context(
            product_id=product_id,
            working_dir=self.working_dir / Path(product_id).name,
            config=self.config,
        )
        ctx.working_dir.mkdir(parents=True, exist_ok=True)
        self._metrics = MetricsCollector(product_id)

        manifest = self._load_manifest(product_id) if resume else self._empty_manifest(product_id)

        if tile_class is not None:
            manifest["tile_class"] = tile_class
        if steps_routed is not None:
            manifest["steps_routed"] = steps_routed

        if resume:
            self._invalidate_drifted_steps(manifest, only)

        manifest["pipeline_version"] = self._version
        manifest["config_hash"]      = _dict_hash(self.config)
        manifest["input_paths"]      = _input_paths_from_config(self.config)
        if manifest.get("started_at") is None:
            manifest["started_at"] = _utcnow()

        completed: set[str] = {
            name for name, s in manifest.get("steps", {}).items()
            if s.get("status") == "success"
        }

        results: list[Result] = []
        upstream_failed = False

        for cls in self.steps:
            is_always_run = cls.name == _ALWAYS_RUN_STEP

            # Respect --steps filter, except for the always-run step
            if only and cls.name not in only and not is_always_run:
                continue

            if upstream_failed and not is_always_run:
                continue

            step_config = self.config.get(cls.name, {})
            step = cls(step_config)

            if cls.name in completed:
                log.info("⏭ %-30s skipped (checkpoint)", cls.name)
                prior = manifest.get("steps", {}).get(cls.name, {})

                step_return = prior.get("step_return") or {}
                if step_return:
                    ctx.data.update(step_return)

                if hasattr(step, "restore_context"):
                    try:
                        step.restore_context(ctx)
                    except Exception as exc:
                        log.warning(
                            "[resume] %s.restore_context failed (%s) — downstream may refetch",
                            cls.name, exc,
                        )

                results.append(Result(
                    cls.name,
                    success=True,
                    elapsed=float(prior.get("elapsed") or 0.0),
                    outputs=list(prior.get("outputs") or []),
                ))
                continue

            self._mark_step_running(manifest, cls.name, step_config)
            self._save_manifest(product_id, manifest)
            log.info(
                "▶ %-30s starting%s",
                cls.name, "  [bypasses --steps]" if is_always_run else "",
            )

            t0 = time.perf_counter()
            try:
                step_outputs = step.run(ctx)
                ctx.data.update(step_outputs or {})
                elapsed = time.perf_counter() - t0

                output_paths = _outputs_from_ctx_data(cls, ctx.data)
                self._mark_step_success(
                    manifest, cls.name, elapsed, output_paths, step_return=step_outputs,
                )
                self._save_manifest(product_id, manifest)

                log.info("✓ %-30s %.2fs", cls.name, elapsed)
                results.append(Result(cls.name, success=True, elapsed=elapsed, outputs=output_paths))
                completed.add(cls.name)
                self._push_step_metrics(cls.name, elapsed, "success", len(output_paths), ctx)

            except Exception as exc:
                elapsed = time.perf_counter() - t0
                error_msg = str(exc)
                self._mark_step_failed(manifest, cls.name, elapsed, error_msg)
                self._save_manifest(product_id, manifest)

                log.error("✗ %-30s %.2fs — %s", cls.name, elapsed, exc)
                results.append(Result(cls.name, success=False, elapsed=elapsed, error=error_msg))
                if self._metrics:
                    self._metrics.push_step(cls.name, elapsed, "failed")

                upstream_failed = True
                # Don't break — let the always-run step have a chance.

        manifest["finished_at"] = _utcnow()
        self._save_manifest(product_id, manifest)

        if self._metrics:
            n_ok = sum(1 for r in results if r.success)
            n_fail = sum(1 for r in results if not r.success)
            total = sum(r.elapsed for r in results)
            self._metrics.push_pipeline_complete(total, n_ok, n_fail)

        log.info("[manifest] Written → %s", self._manifest_path(product_id))
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _assert_no_duplicate_step_names(self) -> None:
        seen: set[str] = set()
        for cls in self.steps:
            if cls.name in seen:
                raise ValueError(
                    f"Duplicate step name '{cls.name}' detected. "
                    "Each step class must have a unique `name` attribute."
                )
            seen.add(cls.name)

    def _invalidate_drifted_steps(self, manifest: dict, only: list[str] | None) -> None:
        for cls in self.steps:
            if only and cls.name not in only:
                continue
            status = manifest.get("steps", {}).get(cls.name, {}).get("status")
            if status == "success" and not self._step_is_valid(cls.name, manifest):
                self._invalidate_from(cls.name, manifest)
                return  # _invalidate_from cascades; first drift is enough

    def _push_step_metrics(
        self,
        step_name: str,
        elapsed: float,
        status: str,
        n_outputs: int,
        ctx: Context,
    ) -> None:
        if not self._metrics:
            return

        self._metrics.push_step(step_name, elapsed, status, n_outputs)

        if step_name == "valid_pixel_mask":
            vf = ctx.data.get("mask_stats", {}).get("valid_fraction")
            if vf is not None:
                self._metrics.push_valid_pixel_fraction(vf)

        if step_name == "brdf_adjustment":
            nbar_dir = ctx.data.get("nbar_ls_dir")
            sbaf_dir = ctx.data.get("sbaf_dir")
            if nbar_dir and sbaf_dir:
                deltas = _compute_brdf_deltas(sbaf_dir, nbar_dir)
                if deltas:
                    try:
                        self._metrics.push_brdf_deltas(deltas)
                    except Exception as exc:
                        log.debug("[metrics] BRDF delta push failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# BRDF stats — single source of truth
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BRDFBandStats:
    """Pre/post BRDF reflectance statistics for one band, on shared valid pixels."""
    band:               str
    mean_pre:           float
    mean_post:          float
    mean_delta:         float
    median_rel_change:  float
    n_valid:            int


def _compute_brdf_band_stats(
    sbaf_dir: str | Path,
    nbar_dir: str | Path,
    *,
    bright_threshold: float = 0.05,
    sample_cap: int = 500_000,
    seed: int = 42,
) -> dict[str, BRDFBandStats]:
    """Compute pre/post BRDF stats for every band shared by the two dirs."""
    sbaf_p = Path(str(sbaf_dir))
    nbar_p = Path(str(nbar_dir))
    out: dict[str, BRDFBandStats] = {}
    rng = np.random.default_rng(seed=seed)

    for band, (sbaf_pat, nbar_name) in NBAR_BAND_MAP.items():
        sbaf_files = list(sbaf_p.glob(sbaf_pat))
        nbar_file = nbar_p / nbar_name
        if not sbaf_files or not nbar_file.exists():
            continue

        try:
            with rasterio.open(sbaf_files[0]) as src:
                pre = src.read(1).astype(np.float32)
                nd_val = _nodata_or_default(src.nodata)
            with rasterio.open(nbar_file) as src:
                post = src.read(1).astype(np.float32)

            valid = (pre != nd_val) & (post != nd_val) & (pre != 0)
            if not valid.any():
                continue

            pre_v = pre[valid]
            post_v = post[valid]
            n_valid_full = int(valid.sum())

            # Subsample large rasters for stable, fast stats.
            if len(pre_v) > sample_cap:
                idx = rng.integers(0, len(pre_v), sample_cap)
                pre_v = pre_v[idx]
                post_v = post_v[idx]

            bright = pre_v > bright_threshold
            if bright.any():
                rel = np.abs(post_v[bright] - pre_v[bright]) / pre_v[bright]
                median_rel_change = float(np.median(rel))
            else:
                median_rel_change = 0.0

            out[band] = BRDFBandStats(
                band               = band,
                mean_pre           = float(pre_v.mean()),
                mean_post          = float(post_v.mean()),
                mean_delta         = float(post_v.mean() - pre_v.mean()),
                median_rel_change  = median_rel_change,
                n_valid            = n_valid_full,
            )
        except Exception as exc:
            log.debug("[brdf] stats failed for %s (non-fatal): %s", band, exc)

    return out


def _compute_brdf_deltas(sbaf_dir: str | Path, nbar_dir: str | Path) -> dict[str, float]:
    """Per-band mean (post - pre) reflectance, for metrics emission."""
    return {b: s.mean_delta for b, s in _compute_brdf_band_stats(sbaf_dir, nbar_dir).items()}


# ---------------------------------------------------------------------------
# Step 1 — Geometric Processing
# ---------------------------------------------------------------------------

class GeometricProcessingStep:
    name = "geometric_processing"
    output_keys = ("geo_ls", "geo_s2")

    def __init__(self, config: dict):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        scene_dir = Path(ctx.config["landsat_path"]).name
        geo_ls_dir = ctx.working_dir / "geo" / scene_dir
        if geo_ls_dir.exists():
            ctx.data["geo_ls"] = geo_ls_dir
        ctx.data["geo_s2"] = None

    def run(self, ctx: Context) -> dict:
        try:
            return self._do_geometric_processing(ctx)
        except Exception as exc:
            log.warning(
                "[geo] Co-registration failed (%s: %s) — skipping geometric "
                "processing. Downstream steps will operate on the original "
                "Landsat scene.",
                type(exc).__name__, exc,
            )
            return {
                "geo_ls":          None,
                "geo_s2":          None,
                "geo_skipped":     True,
                "geo_skip_reason": f"{type(exc).__name__}: {exc}",
            }

    def _do_geometric_processing(self, ctx: Context) -> dict:
        from Geometric_Processing.pipelineGRI import GRIConfig, load_gri, process_scene

        gri_path = self._resolve_gri_path(ctx)

        ls_path = Path(ctx.config["landsat_path"])
        _validate_gri_overlap(gri_path, ls_path)

        output_dir = ctx.working_dir / "geo"
        output_dir.mkdir(parents=True, exist_ok=True)

        cfg = GRIConfig(
            gri_path=gri_path,
            target_resolution=self.config.get("resolution", 10.0),
            max_shift_pixels=self.config.get("max_shift", 50.0),
            output_dir=output_dir,
            sentinel2_bands=self.config.get("s2_bands", ["B02", "B03", "B04", "B08"]),
            landsat_optical_bands=self.config.get("ls_bands", ["B2", "B3", "B4", "B5", "B6", "B7"]),
            dem_path=Path(ctx.config["dem_path"]) if ctx.config.get("dem_path") else None,
            do_orthorectify=bool(ctx.config.get("dem_path")),
        )

        gri = load_gri(cfg)

        geo_ls_dir = Path(process_scene(ls_path, gri, cfg))
        log.info("[geo] LS8 → %s", geo_ls_dir)

        if not geo_ls_dir.exists():
            raise RuntimeError(f"[geo] Output directory does not exist: {geo_ls_dir}")
        geo_tifs = list(geo_ls_dir.glob("*_B*.TIF"))
        if not geo_tifs:
            raise RuntimeError(f"[geo] No band TIF files found in {geo_ls_dir}.")
        log.info("[geo] Found %d band TIF(s): %s", len(geo_tifs), [f.name for f in geo_tifs])

        _copy_mtl_files(ls_path, geo_ls_dir)

        geo_s2_dir = None
        s2_path = ctx.config.get("s2_path")
        if s2_path and Path(s2_path).exists():
            geo_s2_dir = process_scene(Path(s2_path), gri, cfg)
            log.info("[geo] S2  → %s", geo_s2_dir)
        else:
            log.info("[geo] S2 skipped (s2_path is None or does not exist)")

        return {"geo_ls": geo_ls_dir, "geo_s2": geo_s2_dir}

    def _resolve_gri_path(self, ctx: Context) -> Path:
        cache_dir = Path(ctx.config.get(
            "gri_cache_dir",
            str(_BASE / "Geometric_Processing" / "gri"),
        ))

        # Try the MGRS-keyed cache/fetch path first.
        try:
            from Geometric_Processing.gri_fetch import derive_mgrs_tile, get_or_fetch_gri
            mgrs_tile = derive_mgrs_tile(ctx.config["landsat_path"])
            if mgrs_tile:
                return get_or_fetch_gri(mgrs_tile, cache_dir)
            log.warning("[geo] Could not derive MGRS tile — trying legacy gri_path")
        except ImportError as exc:
            log.warning("[geo] gri_fetch module unavailable (%s) — trying legacy gri_path", exc)
        except Exception as exc:
            log.warning(
                "[geo] GRI fetch/cache failed (%s) — trying legacy gri_path",
                exc,
            )

        # Fallback: legacy static path (still supported for backward compat).
        legacy = ctx.config.get("gri_path")
        if legacy:
            legacy_path = Path(str(legacy))
            if legacy_path.exists():
                log.info("[geo] Using legacy gri_path: %s", legacy_path)
                return legacy_path

        raise FileNotFoundError(
            "No GRI available: MGRS-keyed cache/fetch did not succeed and "
            f"legacy gri_path ({legacy}) is not usable."
        )


# ---------------------------------------------------------------------------
# Step 2 — Atmospheric Correction
# ---------------------------------------------------------------------------

class AtmosphericCorrectionStep:
    name = "atmospheric_correction"
    output_keys = ("atm_dir", "toa_path")

    def __init__(self, config: dict):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        atm_dir = ctx.working_dir / "atm_corr"
        if atm_dir.exists():
            ctx.data["atm_dir"] = atm_dir
            ctx.data["bands_are_reflectance"] = True
            toa_path = atm_dir / "toa_reflectance.tif"
            if toa_path.exists():
                ctx.data["toa_path"] = toa_path

    def run(self, ctx: Context) -> dict:
        atm = _atm_mod
        if atm is None:
            raise RuntimeError("[atm] Atmospheric correction module not initialised.")

        geo_ls = ctx.data.get("geo_ls")
        if geo_ls is not None and Path(str(geo_ls)).exists():
            input_dir = str(geo_ls)
        else:
            input_dir = str(ctx.config["landsat_path"])
            log.warning("[atm] geo_ls absent or missing (%s), falling back to landsat_path", geo_ls)

        scene_dir = Path(ctx.config["landsat_path"]).name
        lut_path = ctx.config.get("lut_path", str(_BASE / "lut" / "lut_6s_L8.json"))
        output_dir = ctx.working_dir / "atm_corr"
        output_dir.mkdir(parents=True, exist_ok=True)

        log.info("[atm] Input dir: %s", input_dir)
        _copy_mtl_files(Path(ctx.config["landsat_path"]), Path(input_dir))

        dn_cube, band_names, profile = atm.load_l8_l1(input_dir, scene_dir)
        meta = atm.read_l8_mtl(input_dir, scene_dir)
        toa_cube, sun_zenith = atm.l8_dn_to_toa(dn_cube, band_names, meta)
        log.info("[atm] TOA computed, sun_zenith=%.2f°", sun_zenith)

        toa_path = output_dir / "toa_reflectance.tif"
        profile_toa = {**profile, "count": len(band_names), "dtype": "float32"}
        with rasterio.open(toa_path, "w", **profile_toa) as dst:
            dst.write(toa_cube)
            for i, name in enumerate(band_names, start=1):
                dst.update_tags(i, band_id=name)
        log.info("[atm] TOA saved → %s", toa_path)

        os.makedirs(os.path.dirname(lut_path), exist_ok=True)
        log.info("[atm] Loading/building LUT: %s", lut_path)

        sensor_tag = ctx.config.get("atm_sensor_tag", "L8")
        lut = atm.build_or_load_lut(lut_path, atm.LANDSAT8_BANDS, sensor_tag)

        log.info("[atm] Applying 6S correction…")
        spark = None
        if not _running_inside_spark_executor() and _use_spark(len(band_names)):
            spark = _get_spark()
            log.info("[atm] Using Spark for 6S (%d bands ≥ threshold)", len(band_names))
        else:
            log.info("[atm] Using sequential path for 6S (%d bands)", len(band_names))

        boa_cube = atm.apply_6s_correction(
            toa_cube, band_names, atm.LANDSAT8_BANDS, lut, sun_zenith,
            toa_path=toa_path,
            output_dir=output_dir,
            scene_id=scene_dir,
            spark=spark,
        )

        atm_dir = atm.write_boa_bands(
            boa_cube, band_names, profile, output_dir, scene_dir, input_dir,
            nodata=PIPELINE_NODATA,
        )
        if atm_dir is None:
            atm_dir = output_dir
            log.warning("[atm] write_boa_bands returned None — using output_dir")

        atm_dir = Path(str(atm_dir))
        if not atm_dir.exists():
            raise RuntimeError(f"[atm] atm_dir does not exist: {atm_dir}")

        _copy_mtl_files(Path(ctx.config["landsat_path"]), atm_dir)

        sr_tifs = list(atm_dir.glob("*_SR_B*.TIF"))
        if not sr_tifs:
            raise RuntimeError(f"[atm] No SR band TIF files found in {atm_dir}.")
        log.info("[atm] BOA bands saved → %s", atm_dir)

        return {
            "toa_path":              toa_path,
            "atm_dir":               atm_dir,
            "band_names":            band_names,
            "sun_zenith":            sun_zenith,
            "bands_are_reflectance": True,
        }


# ---------------------------------------------------------------------------
# Step 3 — SBAF Spectral Adjustment
# ---------------------------------------------------------------------------

class SBAFStep:
    name = "sbaf"
    output_keys = ("sbaf_dir",)

    def __init__(self, config: dict):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        sbaf_dir = ctx.working_dir / "sbaf"
        if sbaf_dir.exists():
            ctx.data["sbaf_dir"] = sbaf_dir
            ctx.data["bands_are_reflectance"] = True

    def run(self, ctx: Context) -> dict:
        from SBAF.sbaf import SbafProcessor

        atm_dir = ctx.data.get("atm_dir")
        if atm_dir is not None and Path(str(atm_dir)).exists():
            input_dir = Path(str(atm_dir))
        else:
            input_dir = Path(ctx.config["landsat_path"])
            log.warning("[sbaf] atm_dir absent or missing (%s), falling back to landsat_path", atm_dir)

        output_dir = ctx.working_dir / "sbaf"
        output_dir.mkdir(parents=True, exist_ok=True)
        log.info("[sbaf] Input dir: %s", input_dir)

        sr_tifs = list(input_dir.glob("*_SR_B*.TIF"))
        if not sr_tifs:
            raise RuntimeError(f"[sbaf] No SR band TIFs (*_SR_B*.TIF) found in {input_dir}.")
        log.info("[sbaf] SR TIFs found: %s", [f.name for f in sr_tifs])

        mission      = self.config.get("mission",      ctx.config.get("mission", "LANDSAT_8"))
        s2_target    = self.config.get("s2_target",    "Sentinel-2A")
        adaptive     = self.config.get("adaptive",     True)
        chunks       = self.config.get("chunks",       1024)
        output_scale = self.config.get("output_scale", 1.0)
        bands        = self.config.get("bands",        None)

        processor = SbafProcessor(
            mission=mission,
            s2_target=s2_target,
            adaptive=adaptive,
            chunks=chunks,
            output_scale=output_scale,
            input_is_reflectance=ctx.data.get("bands_are_reflectance", False),
            nodata=PIPELINE_NODATA,
        )

        log.info("[sbaf] Adjusting %s → %s (adaptive=%s)…", mission, s2_target, adaptive)
        outputs = processor.process_scene(
            scene_dir=input_dir,
            output_dir=output_dir,
            bands=bands,
            in_executor=_running_inside_spark_executor(),
        )
        if not outputs:
            raise RuntimeError(f"[sbaf] SbafProcessor produced no output files in {output_dir}.")
        log.info(
            "[sbaf] Done → %s  (%d files: %s)",
            output_dir, len(outputs), [p.name for p in outputs.values()],
        )

        _copy_mtl_files(Path(ctx.config["landsat_path"]), output_dir)

        return {
            "sbaf_dir":              output_dir,
            "sbaf_outputs":          outputs,
            "bands_are_reflectance": True,
        }


# ---------------------------------------------------------------------------
# Step 4 — Valid Pixel Mask
# ---------------------------------------------------------------------------

class ValidPixelMaskStep:
    name = "valid_pixel_mask"
    output_keys = ("mask_path",)

    def __init__(self, config: dict):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        scene_dir = Path(ctx.config["landsat_path"]).name
        mask_path = ctx.working_dir / "mask" / f"{scene_dir}_VALID_PIXEL_MASK.TIF"
        if mask_path.exists():
            ctx.data["mask_path"] = mask_path

    def run(self, ctx: Context) -> dict:
        from Valid_Pixel_Mask.valid_pixel_mask import (
            MaskConfig,
            build_valid_pixel_mask,
            find_landsat_qa_files,
            validate_mask,
            write_mask,
            print_validation_report,
        )

        qa_source_dir = Path(ctx.config["landsat_path"])
        log.info("[mask] QA source dir: %s", qa_source_dir)

        try:
            qa_files = find_landsat_qa_files(qa_source_dir)
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"[mask] Could not locate QA files in {qa_source_dir}: {exc}"
            ) from exc

        output_dir = ctx.working_dir / "mask"
        output_dir.mkdir(parents=True, exist_ok=True)
        scene_dir = Path(ctx.config["landsat_path"]).name
        mask_path = output_dir / f"{scene_dir}_VALID_PIXEL_MASK.TIF"

        cfg = MaskConfig(
            cloud_dilation_radius  = self.config.get("cloud_dilation_radius",  3),
            shadow_dilation_radius = self.config.get("shadow_dilation_radius", 3),
            use_cirrus             = self.config.get("use_cirrus",        True),
            use_dilated_cloud      = self.config.get("use_dilated_cloud", True),
            exclude_water          = self.config.get("exclude_water",     False),
            chunk_size             = self.config.get("chunk_size",        1024),
        )

        log.info(
            "[mask] Building mask (cloud_dil=%d, shadow_dil=%d, cirrus=%s, exclude_water=%s)…",
            cfg.cloud_dilation_radius, cfg.shadow_dilation_radius,
            cfg.use_cirrus, cfg.exclude_water,
        )
        mask_np, profile = build_valid_pixel_mask(
            qa_pixel_path      = qa_files["qa_pixel"],
            qa_radsat_path     = qa_files["qa_radsat"],
            sr_qa_aerosol_path = qa_files["sr_qa_aerosol"],
            cfg                = cfg,
        )

        write_mask(mask_np, profile, mask_path)
        log.info("[mask] Mask written → %s", mask_path)

        stats = validate_mask(mask_np, profile)
        print_validation_report(stats)

        for w in stats.get("warnings", []):
            log.warning("[mask] %s", w)

        log.info(
            "[mask] Valid fraction: %.2f%%  (clear pixels: %d / %d)",
            stats["valid_fraction"] * 100,
            stats["flags"]["clear"]["count"],
            stats["total_pixels"],
        )

        return {"mask_path": mask_path, "mask_stats": stats}


# ---------------------------------------------------------------------------
# Step 5 — BRDF Adjustment
# ---------------------------------------------------------------------------

class BRDFAdjustmentStep:
    name = "brdf_adjustment"
    output_keys = ("nbar_ls_dir", "nbar_s2_dir")

    def __init__(self, config: dict):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        nbar_dir = ctx.working_dir / "nbar" / "landsat"
        if nbar_dir.exists():
            ctx.data["nbar_ls_dir"] = str(nbar_dir)
        ctx.data["nbar_s2_dir"] = None

    def run(self, ctx: Context) -> dict:
        from BRDF_Adjustment.nbar import ROY_COEFS

        workers  = self.config.get("workers",  4)
        bands    = self.config.get("bands",    list(ROY_COEFS.keys()))
        clip_min = self.config.get("clip_min", 0.8)
        clip_max = self.config.get("clip_max", 1.2)

        ls_input = self._resolve_ls_input(ctx)
        ls_outdir = str(ctx.working_dir / "nbar" / "landsat")
        Path(ls_outdir).mkdir(parents=True, exist_ok=True)

        self._verify_ls_inputs(ls_input, ctx)

        completed_bands = self._run_ls8_nbar(
            bands, ls_input, ls_outdir, clip_min, clip_max, workers,
        )
        log.info("[brdf] Completed NBAR bands: %s", completed_bands)

        nbar_files = list(Path(ls_outdir).glob("NBAR_*.tif"))
        if not nbar_files:
            raise RuntimeError(f"[brdf] process_ls8 produced no NBAR_*.tif files in {ls_outdir}.")
        log.info(
            "[brdf] LS8 NBAR → %s  (%d files: %s)",
            ls_outdir, len(nbar_files), [f.name for f in nbar_files],
        )

        s2_outdir = self._run_s2_nbar_if_available(
            ctx, bands, clip_min, clip_max, workers,
        )
        return {"nbar_ls_dir": ls_outdir, "nbar_s2_dir": s2_outdir}

    # ---- internal helpers ----

    @staticmethod
    def _resolve_ls_input(ctx: Context) -> str:
        sbaf_dir = ctx.data.get("sbaf_dir")
        atm_dir  = ctx.data.get("atm_dir")
        if sbaf_dir and Path(str(sbaf_dir)).exists():
            log.info("[brdf] Input dir (SBAF): %s", sbaf_dir)
            return str(sbaf_dir)
        if atm_dir and Path(str(atm_dir)).exists():
            log.warning("[brdf] sbaf_dir absent, falling back to atm_dir")
            return str(atm_dir)
        log.warning("[brdf] Both sbaf_dir and atm_dir absent — falling back to landsat_path")
        return str(ctx.config["landsat_path"])

    @staticmethod
    def _verify_ls_inputs(ls_input: str, ctx: Context) -> None:
        ls_input_path = Path(ls_input)
        sr_tifs = (
            list(ls_input_path.glob("*_SR_B*.TIF"))
            + list(ls_input_path.glob("*_SBAF_B*.TIF"))
        )
        if not sr_tifs:
            raise RuntimeError(f"[brdf] No SR/SBAF band TIFs found in {ls_input}.")

        mtl_files = (
            list(ls_input_path.glob("*_MTL.json"))
            + list(ls_input_path.glob("*_MTL.txt"))
            + list(ls_input_path.glob("*_MTL.xml"))
        )
        if not mtl_files:
            _copy_mtl_files(Path(ctx.config["landsat_path"]), ls_input_path)
            mtl_files = (
                list(ls_input_path.glob("*_MTL.json"))
                + list(ls_input_path.glob("*_MTL.txt"))
                + list(ls_input_path.glob("*_MTL.xml"))
            )
            if not mtl_files:
                raise RuntimeError(f"[brdf] MTL file still missing in {ls_input}.")

        log.info("[brdf] SR/SBAF TIFs found: %s", [f.name for f in sr_tifs])
        log.info("[brdf] MTL files found: %s",    [f.name for f in mtl_files])

    def _run_ls8_nbar(
        self, bands, ls_input, ls_outdir, clip_min, clip_max, workers,
    ) -> list[str]:
        """Run NBAR on all Landsat-8 bands, threaded or via Spark."""
        from BRDF_Adjustment.nbar import process_ls8

        def _run_band_local(band: str) -> str:
            process_ls8(ls_input, [band], ls_outdir, clip_min=clip_min, clip_max=clip_max)
            return band

        # Cap at min(workers, len(bands)): never exceed the user's
        # requested concurrency, and never more threads than there is work.
        effective_workers = min(workers, len(bands))

        if not _use_spark(len(bands)):
            log.info(
                "[brdf] Running threaded NBAR on Landsat-8 (%d bands, %d workers)…",
                len(bands), effective_workers,
            )
            return _run_parallel_threads(_run_band_local, bands, effective_workers)

        log.info(
            "[brdf] Running Spark NBAR on Landsat-8 (%d bands, workers=%d)…",
            len(bands), effective_workers,
        )
        return self._spark_run_ls8(
            bands, ls_input, ls_outdir, clip_min, clip_max, effective_workers,
        )

    @staticmethod
    def _spark_run_ls8(bands, ls_input, ls_outdir, clip_min, clip_max, workers) -> list[str]:
        spark = _get_spark(workers=workers)
        base_str = str(_BASE)
        _ls, _out, _cmin, _cmax = ls_input, ls_outdir, clip_min, clip_max

        def _spark_band(band: str) -> str:
            import sys as _sys
            _sys.path.insert(0, base_str + "/BRDF_Adjustment")
            from BRDF_Adjustment.nbar import process_ls8 as _p
            _p(_ls, [band], _out, clip_min=_cmin, clip_max=_cmax)
            return band

        return (
            spark.sparkContext
            .parallelize(bands, numSlices=len(bands))
            .map(_spark_band)
            .collect()
        )

    def _run_s2_nbar_if_available(
        self, ctx: Context, bands, clip_min, clip_max, workers,
    ) -> str | None:
        from BRDF_Adjustment.nbar import process_s2

        geo_s2 = ctx.data.get("geo_s2")
        s2_input = (
            str(geo_s2) if geo_s2 and Path(str(geo_s2)).exists()
            else str(ctx.config.get("s2_path", ""))
        )
        if not s2_input or not Path(s2_input).exists():
            log.info("[brdf] S2 skipped")
            return None

        s2_out = ctx.working_dir / "nbar" / "sentinel2"
        s2_out.mkdir(parents=True, exist_ok=True)
        s2_outdir = str(s2_out)
        _cmin, _cmax = clip_min, clip_max

        effective_workers = min(workers, len(bands))

        def _run_band_local(band: str) -> str:
            process_s2(s2_input, [band], s2_outdir, clip_min=_cmin, clip_max=_cmax)
            return band

        if not _use_spark(len(bands)):
            log.info("[brdf] Running threaded NBAR on Sentinel-2 (%d workers)…", effective_workers)
            _run_parallel_threads(_run_band_local, bands, effective_workers)
        else:
            base_str = str(_BASE)
            _s2 = s2_input
            _out = s2_outdir

            def _spark_band(band: str) -> str:
                import sys as _sys
                _sys.path.insert(0, base_str + "/BRDF_Adjustment")
                from BRDF_Adjustment.nbar import process_s2 as _p
                _p(_s2, [band], _out, clip_min=_cmin, clip_max=_cmax)
                return band

            log.info("[brdf] Running Spark NBAR on Sentinel-2 (workers=%d)…", effective_workers)
            spark = _get_spark(workers=effective_workers)
            spark.sparkContext.parallelize(bands, numSlices=len(bands)).map(_spark_band).collect()

        log.info("[brdf] S2 NBAR → %s", s2_outdir)
        return s2_outdir


# ---------------------------------------------------------------------------
# Step 6 — Data Fusion / Upsampling
# ---------------------------------------------------------------------------

class DataFusionStep:
    name = "data_fusion"
    output_keys = ("fused_dir",)

    _NBAR_LOGICAL_TO_BAND = {
        "Blue":  "B2", "Green": "B3", "Red": "B4",
        "NIR":   "B5", "SWIR1": "B6", "SWIR2": "B7",
    }

    def __init__(self, config: dict):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        fused_dir = ctx.working_dir / "fusion"
        if fused_dir.exists():
            ctx.data["fused_dir"] = str(fused_dir)
        nbar_dir = ctx.working_dir / "nbar" / "landsat"
        if nbar_dir.exists():
            ctx.data["nbar_ls_dir"] = str(nbar_dir)

    def run(self, ctx: Context) -> dict:
        from data_fusion.upsampling import (
            find_landsat_file,
            find_sentinel_jp2,
            process_band,
            LANDSAT_BANDS,
            SENTINEL_BANDS,
            _maybe_extract,
        )

        workers = self.config.get("workers", os.cpu_count() or 4)
        output_dir = str(ctx.working_dir / "fusion")
        os.makedirs(output_dir, exist_ok=True)

        jobs = self._build_jobs(
            ctx, output_dir,
            find_landsat_file, find_sentinel_jp2,
            LANDSAT_BANDS, SENTINEL_BANDS, _maybe_extract,
        )
        if jobs:
            self._process_jobs(jobs, process_band, workers)

        fused_files = sorted(Path(output_dir).glob("*_10m.TIF"))
        if not fused_files:
            raise RuntimeError(f"[fusion] No *_10m.TIF files produced in {output_dir}.")
        log.info(
            "[fusion] Done → %s  (%d files: %s)",
            output_dir, len(fused_files), [f.name for f in fused_files],
        )

        mask_path = ctx.data.get("mask_path")
        if mask_path and Path(str(mask_path)).exists():
            _apply_mask_to_fusion(output_dir, fused_files, mask_path)
        else:
            log.warning("[fusion] mask_path not available — cloud masking skipped")

        _write_metadata(ctx, output_dir, fused_files)
        return {"fused_dir": output_dir}

    # ---- internal helpers ----

    def _build_jobs(
        self, ctx, output_dir,
        find_landsat_file, find_sentinel_jp2,
        LANDSAT_BANDS, SENTINEL_BANDS, _maybe_extract,
    ) -> list[tuple]:
        jobs: list[tuple] = []
        nbar_ls_dir = ctx.data.get("nbar_ls_dir")

        if nbar_ls_dir and Path(str(nbar_ls_dir)).exists():
            log.info("[fusion] Input dir (NBAR): %s", nbar_ls_dir)
            for logical, band in self._NBAR_LOGICAL_TO_BAND.items():
                fpath = os.path.join(str(nbar_ls_dir), f"NBAR_{logical}.tif")
                if not os.path.exists(fpath):
                    log.warning("[fusion] NBAR file not found for band %s — skipping", band)
                    continue
                jobs.append((fpath, band, output_dir, 1, "landsat"))
            if not jobs:
                raise RuntimeError(f"[fusion] No NBAR_*.tif files matched in {nbar_ls_dir}.")
        else:
            log.warning("[fusion] nbar_ls_dir absent — running standalone on raw Landsat")
            ls_input = _maybe_extract(str(ctx.config["landsat_path"]))
            for band in LANDSAT_BANDS:
                fpath = find_landsat_file(ls_input, band)
                if fpath is None:
                    log.warning("[fusion] Landsat band %s not found — skipping", band)
                    continue
                if band == "B8":
                    shutil.copy2(fpath, os.path.join(output_dir, f"{band}_10m.TIF"))
                    continue
                jobs.append((fpath, band, output_dir, 3, "landsat"))

        geo_s2 = ctx.data.get("geo_s2")
        s2_input = (
            str(geo_s2) if geo_s2 and Path(str(geo_s2)).exists()
            else str(ctx.config.get("s2_path", ""))
        )
        if s2_input and Path(s2_input).exists():
            for band, native_res in SENTINEL_BANDS.items():
                fpath = find_sentinel_jp2(s2_input, band)
                if fpath is None:
                    log.warning("[fusion] S2 band %s not found — skipping", band)
                    continue
                scale = native_res // 10
                if scale == 1:
                    shutil.copy2(fpath, os.path.join(output_dir, f"{band}_10m.TIF"))
                    continue
                jobs.append((fpath, band, output_dir, scale, "sentinel2"))
        else:
            log.info("[fusion] S2 skipped")

        log.info("[fusion] %d bands to process", len(jobs))
        return jobs

    def _process_jobs(self, jobs, process_band, workers: int) -> None:
        def _run_band(job) -> tuple[str, str | None]:
            fpath, band, outdir, scale, sensor = job
            try:
                process_band(fpath, band, outdir, scale, sensor)
                return (band, None)
            except Exception as exc:
                return (band, str(exc))

        if not _use_spark(len(jobs)):
            log.info("[fusion] Running threaded band processing (workers=%d)…", workers)
            results = _run_parallel_threads(_run_band, jobs, workers)
        else:
            log.info("[fusion] Running Spark band processing (workers=%d)…", workers)
            spark = _get_spark(workers=workers)
            base_str = str(_BASE)

            def _spark_band(job):
                fpath, band, outdir, scale, sensor = job
                import sys as _sys
                _sys.path.insert(0, base_str + "/data_fusion")
                from data_fusion.upsampling import process_band as _pb
                try:
                    _pb(fpath, band, outdir, scale, sensor)
                    return (band, None)
                except Exception as exc:
                    return (band, str(exc))

            results = (
                spark.sparkContext
                .parallelize(jobs, numSlices=min(workers, len(jobs)))
                .map(_spark_band)
                .collect()
            )

        failures = [(band, err) for band, err in results if err]
        for band, err in failures:
            log.error("[fusion] band %s failed: %s", band, err)

        if failures:
            failed_bands = [band for band, _ in failures]
            raise RuntimeError(
                f"[fusion] {len(failures)}/{len(jobs)} bands failed: {failed_bands}. "
                "Refusing to proceed with a degraded product."
            )


# ---------------------------------------------------------------------------
# Helper — apply valid pixel mask to fusion output TIFs
# ---------------------------------------------------------------------------

def _apply_mask_to_fusion(output_dir, fused_files, mask_path) -> None:
    from rasterio.enums import Resampling
    from rasterio.warp import reproject
    from Valid_Pixel_Mask.valid_pixel_mask import MaskBits

    cache_path = Path(output_dir) / "_mask_reproj_cache.npy"
    cache_meta_path = Path(output_dir) / "_mask_reproj_cache.json"
    mask_mtime = Path(str(mask_path)).stat().st_mtime

    ref_file = fused_files[0]
    with rasterio.open(ref_file) as ref:
        dst_crs = ref.crs
        dst_trans = ref.transform
        dst_shape = (ref.height, ref.width)
        dst_profile = ref.profile.copy()

    cache_key = {
        "mask_path":   str(mask_path),
        "mask_mtime":  mask_mtime,
        "dst_shape":   list(dst_shape),
        "dst_crs":     dst_crs.to_string() if dst_crs else None,
        "dst_transform": list(dst_trans)[:6],
    }

    cache_valid = False
    if cache_path.exists() and cache_meta_path.exists():
        try:
            stored_key = json.loads(cache_meta_path.read_text())
            cache_valid = (stored_key == cache_key)
            if not cache_valid:
                log.info("[fusion] Mask cache key mismatch — regenerating")
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("[fusion] Mask cache meta unreadable (%s) — regenerating", exc)

    if cache_valid:
        log.info("[fusion] Loading cached reprojected mask → %s", cache_path)
        binary_mask = np.load(cache_path)
    else:
        log.info("[fusion] Writing validity mask sidecar (sen2like style) → %s", mask_path)
        with rasterio.open(mask_path) as mask_src:
            mask_data = mask_src.read(1)
            mask_crs = mask_src.crs
            mask_trans = mask_src.transform

        mask_reproj = np.zeros(dst_shape, dtype=np.uint8)
        reproject(
            source        = mask_data,
            destination   = mask_reproj,
            src_transform = mask_trans,
            src_crs       = mask_crs,
            dst_transform = dst_trans,
            dst_crs       = dst_crs,
            resampling    = Resampling.nearest,
        )
        binary_mask = ((mask_reproj & (1 << MaskBits.CLEAR)) != 0).astype(np.uint8)
        np.save(cache_path, binary_mask)
        cache_meta_path.write_text(json.dumps(cache_key, indent=2))
        log.info("[fusion] Reprojected mask cached → %s", cache_path)

    valid_frac = float(binary_mask.sum()) / binary_mask.size
    log.info(
        "[fusion] Mask valid fraction after reproject to MGRS tile: %.2f%% "
        "(computed on MGRS grid — not directly comparable to the %% reported "
        "by valid_pixel_mask, which is on the full Landsat raster including fill)",
        valid_frac * 100,
    )
    if valid_frac < 0.01:
        log.warning("[fusion] ⚠ <1%% valid pixels — check mask encoding!")

    mask_out_path = Path(output_dir) / "FUSION_VALIDITY_MASK.TIF"
    sidecar_profile = dst_profile.copy()
    sidecar_profile.update(dtype="uint8", count=1, nodata=255)
    with rasterio.open(mask_out_path, "w", **sidecar_profile) as dst:
        dst.write(binary_mask, 1)
    log.info("[fusion] Validity mask written → %s", mask_out_path)


# ---------------------------------------------------------------------------
# Helper — write metadata sidecar JSON
# ---------------------------------------------------------------------------

def _write_metadata(ctx: Context, output_dir: str, fused_files: list[Path]) -> None:
    mask_stats = ctx.data.get("mask_stats", {})
    meta = {
        "product_id":      ctx.product_id,
        "processed_at":    _utcnow(),
        "pipeline_nodata": PIPELINE_NODATA,
        "steps_applied": [
            "geometric_processing", "atmospheric_correction", "sbaf",
            "valid_pixel_mask", "brdf_adjustment", "data_fusion",
        ],
        "config": {
            "gri_path": str(ctx.config.get("gri_path", "")),
            "dem_path": str(ctx.config.get("dem_path", "")),
            "s2_path":  str(ctx.config.get("s2_path",  "")),
            "sbaf":     ctx.config.get("sbaf", {}),
            "brdf":     ctx.config.get("brdf_adjustment", {}),
        },
        "valid_pixel_mask": {
            "valid_fraction": mask_stats.get("valid_fraction"),
            "flags":          mask_stats.get("flags", {}),
        },
        "output_bands": [f.name for f in fused_files],
    }
    meta_path = Path(output_dir) / "metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2, default=str))
    log.info("[meta] Metadata written → %s", meta_path)


# ---------------------------------------------------------------------------
# Helper — validate GRI spatial overlap
# ---------------------------------------------------------------------------

class GRIOverlapError(RuntimeError):
    """Raised when the GRI and scene bounds do not intersect."""


def _validate_gri_overlap(gri_path: Path, scene_path: Path) -> None:
    from rasterio.warp import transform_bounds

    scene_tifs = list(scene_path.glob("*_B2.TIF")) or list(scene_path.glob("*_B4.TIF"))
    if not scene_tifs:
        log.warning("[geo] GRI overlap check skipped — no reference TIF found in scene")
        return

    with rasterio.open(gri_path) as gri_src:
        gri_bounds = gri_src.bounds
        gri_crs = gri_src.crs

    with rasterio.open(scene_tifs[0]) as scene_src:
        scene_bounds = transform_bounds(scene_src.crs, gri_crs, *scene_src.bounds)

    overlap = (
        scene_bounds[0] < gri_bounds[2] and scene_bounds[2] > gri_bounds[0]
        and scene_bounds[1] < gri_bounds[3] and scene_bounds[3] > gri_bounds[1]
    )
    if not overlap:
        raise GRIOverlapError(
            f"[geo] GRI extent {tuple(round(x) for x in gri_bounds)} does NOT overlap "
            f"scene extent {tuple(round(x) for x in scene_bounds)}. "
            "Co-registration will produce garbage shifts. "
            "Rebuild the GRI from a Sentinel-2 scene covering this Landsat path/row."
        )

    log.info("[geo] GRI overlap check passed ✓")


# ---------------------------------------------------------------------------
# Helper — copy MTL files (idempotent, SHA-256 dedup)
# ---------------------------------------------------------------------------

def _sha256_file(path: Path, bufsize: int = 65536) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(bufsize), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_mtl_files(src_dir: Path, dst_dir: Path) -> int:
    if not src_dir.exists():
        log.warning("[mtl_copy] Source dir does not exist: %s", src_dir)
        return 0

    copied = 0
    for pattern in ("*_MTL.json", "*_MTL.txt", "*_MTL.xml"):
        for src_file in src_dir.glob(pattern):
            dst_file = dst_dir / src_file.name
            if dst_file.exists() and _sha256_file(dst_file) == _sha256_file(src_file):
                log.debug("[mtl_copy] Already identical, skipping: %s", dst_file.name)
                continue
            shutil.copy2(src_file, dst_file)
            log.info("[mtl_copy] %s → %s", src_file.name, dst_dir)
            copied += 1

    if copied == 0:
        log.debug("[mtl_copy] No new MTL files copied from %s to %s", src_dir, dst_dir)
    return copied


# ---------------------------------------------------------------------------
# Step 7 — Cleanup
# ---------------------------------------------------------------------------

_CLEANUP_DIRS_BY_MODE: dict[str, list[str]] = {
    "light":      [],
    "medium":     ["geo", "atm_corr"],
    "aggressive": ["sbaf", "nbar"],
    "strict":     ["fusion"],   
}

_CLEANUP_MODE_ORDER = ["none", "light", "medium", "aggressive", "strict"]


def _cleanup_mode_gte(mode: str, threshold: str) -> bool:
    try:
        return _CLEANUP_MODE_ORDER.index(mode) >= _CLEANUP_MODE_ORDER.index(threshold)
    except ValueError:
        return False


class CleanupStep:
    name = "cleanup"
    output_keys = ()

    def __init__(self, config: dict):
        self.config = config
        self.mode = config.get("mode", "none").lower()
        self.require_safe = config.get("require_safe", True)
        self.dry_run = config.get("dry_run", False)

        if self.mode not in _CLEANUP_MODE_ORDER:
            raise ValueError(
                f"[cleanup] Unknown mode '{self.mode}'. Valid modes: {_CLEANUP_MODE_ORDER}"
            )

    def restore_context(self, ctx: Context) -> None:
        pass

    def run(self, ctx: Context) -> dict:
        log.info(
            "[cleanup] mode=%s  dry_run=%s  require_safe=%s",
            self.mode, self.dry_run, self.require_safe,
        )

        if self.mode == "none":
            log.info("[cleanup] mode=none — nothing to do.")
            return {"cleanup_mode": self.mode, "cleanup_deleted": []}

        skip = self._gate_checks(ctx)
        if skip is not None:
            return skip

        to_delete = self._collect_targets(ctx)
        if not to_delete:
            log.info("[cleanup] Nothing to delete for mode=%s.", self.mode)
            return {"cleanup_mode": self.mode, "cleanup_deleted": []}

        deleted = self._delete_targets(to_delete)
        action = "Would have deleted" if self.dry_run else "Deleted"
        log.info("[cleanup] %s %d item(s): %s", action, len(deleted), deleted)

        return {
            "cleanup_mode":    self.mode,
            "cleanup_deleted": deleted,
            "cleanup_dry_run": self.dry_run,
        }

    def _gate_checks(self, ctx: Context) -> dict | None:
        if self.require_safe:
            safe_dir = ctx.data.get("safe_dir")
            safe_path = Path(str(safe_dir)) if safe_dir else None
            if not safe_path or not safe_path.exists():
                log.warning(
                    "[cleanup] SKIPPED — safe_dir not found (%s). "
                    "Set require_safe=False to override.", safe_dir,
                )
                return {"cleanup_mode": self.mode, "cleanup_deleted": [], "cleanup_skipped": True}

            contents = list(safe_path.iterdir())
            if not contents:
                log.warning("[cleanup] SKIPPED — safe_dir exists but is empty (%s).", safe_path)
                return {"cleanup_mode": self.mode, "cleanup_deleted": [], "cleanup_skipped": True}

            log.info("[cleanup] safe_dir OK → %s (%d items)", safe_path, len(contents))

        if _cleanup_mode_gte(self.mode, "strict") and not ctx.data.get("packaging_validated"):
            log.warning(
                "[cleanup] mode=strict REFUSED — packaging_validated is not True. "
                "Run ValidationStep first."
            )
            return {
                "cleanup_mode":        self.mode,
                "cleanup_deleted":     [],
                "cleanup_skipped":     True,
                "cleanup_skip_reason": "packaging_validated not set",
            }

        return None

    def _collect_targets(self, ctx: Context) -> list[Path]:
        to_delete: list[Path] = []

        if _cleanup_mode_gte(self.mode, "light"):
            toa_path = ctx.data.get("toa_path")
            if toa_path:
                p = Path(str(toa_path))
                if p.exists():
                    to_delete.append(p)

        for level in ("medium", "aggressive", "strict"):
            if _cleanup_mode_gte(self.mode, level):
                for dir_name in _CLEANUP_DIRS_BY_MODE[level]:
                    candidate = ctx.working_dir / dir_name
                    if candidate.exists():
                        to_delete.append(candidate)

        return to_delete

    def _delete_targets(self, targets: list[Path]) -> list[str]:
        deleted: list[str] = []
        verb = "Would delete" if self.dry_run else "Deleting"

        for target in targets:
            log.info("[cleanup] %s: %s", verb, target)
            if self.dry_run:
                deleted.append(str(target))
                continue
            try:
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
                deleted.append(str(target))
                log.info("[cleanup] ✓ removed %s", target)
            except OSError as exc:
                log.error("[cleanup] Failed to remove %s: %s", target, exc)

        return deleted


# ---------------------------------------------------------------------------
# Step 8 — Validation
# ---------------------------------------------------------------------------

class ValidationStep:
    name = "validation"
    output_keys = ()

    _DEFAULTS = dict(
        BOA_MIN          =  0.0,
        BOA_MAX          =  1.0,
        BOA_WARN_LOW     =  0.0,
        BOA_WARN_HIGH    =  0.8,
        NODATA_WARN_FRAC =  0.5,
        VALID_WARN_FRAC  =  0.3,
        BRDF_WARN_RATIO  =  0.10,
        BRDF_NOOP_FLOOR  =  0.001,
        SHIFT_WARN_PX    = 10.0,
    )

    def __init__(self, config: dict):
        self.config = config
        for attr, default in self._DEFAULTS.items():
            setattr(self, attr, config.get(attr, default))

    def restore_context(self, ctx: Context) -> None:
        pass

    def run(self, ctx: Context) -> dict:
        issues: list[tuple[str, str]] = []
        passed: list[str] = []

        log.info("=" * 60)
        log.info("VALIDATION REPORT — %s", ctx.product_id)
        log.info("=" * 60)

        self._check_reflectance(ctx, issues, passed)
        self._check_cloud_mask(ctx, issues, passed)
        self._check_brdf_effect(ctx, issues, passed)
        self._check_geometry(ctx, issues, passed)
        self._check_fusion_output(ctx, issues, passed)

        log.info("-" * 60)
        n_warn = sum(1 for lvl, _ in issues if lvl == "WARN")
        n_fail = sum(1 for lvl, _ in issues if lvl == "FAIL")
        log.info("Checks passed : %d", len(passed))
        log.info("Warnings      : %d", n_warn)
        log.info("Failures      : %d", n_fail)
        if issues:
            log.info("Issues:")
            for lvl, msg in issues:
                (log.warning if lvl == "WARN" else log.error)("  [%s] %s", lvl, msg)
        else:
            log.info("All checks passed — product looks healthy.")
        log.info("=" * 60)

        packaging_validated = (n_fail == 0)
        return {
            "validation_passed":   packaging_validated,
            "validation_warnings": n_warn,
            "validation_failures": n_fail,
            "validation_issues":   issues,
            "packaging_validated": packaging_validated,
        }

    # ---- internal checks ----

    def _check_reflectance(self, ctx, issues, passed):
        fused_dir = ctx.data.get("fused_dir")
        if not fused_dir or not Path(str(fused_dir)).exists():
            issues.append(("WARN", "reflectance check skipped — fused_dir not available"))
            return

        band_files = [
            f for f in sorted(Path(str(fused_dir)).glob("*_10m.TIF"))
            if "VALIDITY" not in f.name.upper()
        ]
        if not band_files:
            issues.append(("FAIL", "reflectance check — no *_10m.TIF files found in fused_dir"))
            return

        log.info("[validation] Reflectance range check (%d bands):", len(band_files))

        boa_min, boa_max = self.BOA_MIN, self.BOA_MAX

        def _band_stats(fpath_str: str):
            import rasterio as _rio
            import numpy as _np
            fname = Path(fpath_str).name
            try:
                with _rio.open(fpath_str) as src:
                    data = src.read(1).astype(_np.float32)
                    nd_val = _nodata_or_default(src.nodata)
                valid = data != nd_val
                n_total = int(data.size)
                n_valid = int(valid.sum())
                nodata_frac = (n_total - n_valid) / n_total if n_total > 0 else 0.0

                if n_valid == 0:
                    return (fname, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, True, "100% nodata")

                vals = data[valid]
                if len(vals) > 1_000_000:
                    rng = _np.random.default_rng(seed=42)
                    vals = rng.choice(vals, size=1_000_000, replace=False)

                oor_frac = float(((vals < boa_min) | (vals > boa_max)).sum()) / len(vals)
                return (
                    fname,
                    float(vals.mean()), float(vals.std()),
                    float(vals.min()), float(vals.max()),
                    oor_frac, nodata_frac, False, "",
                )
            except Exception as exc:
                return (fname, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, True, str(exc))

        paths = [str(f) for f in band_files]
        rows = _run_parallel_threads(_band_stats, paths, workers=min(len(paths), 8))

        for (fname, vmean, vstd, vmin, vmax, oor_frac, nodata_frac, error, errmsg) in rows:
            if error:
                issues.append(("FAIL", f"{fname} — {errmsg}"))
                continue
            log.info(
                "  %-22s  mean=%.4f  std=%.4f  min=%.4f  max=%.4f  "
                "nodata=%.1f%%  out-of-range=%.2f%%",
                fname, vmean, vstd, vmin, vmax, nodata_frac * 100, oor_frac * 100,
            )
            if nodata_frac > self.NODATA_WARN_FRAC:
                issues.append(("WARN",
                    f"{fname} — {nodata_frac:.0%} nodata (>{self.NODATA_WARN_FRAC:.0%})"))
            if oor_frac > 0.01:
                issues.append(("WARN",
                    f"{fname} — {oor_frac:.2%} pixels outside [{self.BOA_MIN}, {self.BOA_MAX}]"))
            if vmean < self.BOA_WARN_LOW:
                issues.append(("WARN",
                    f"{fname} — suspiciously low mean reflectance ({vmean:.4f})"))
            if vmean > self.BOA_WARN_HIGH:
                issues.append(("WARN",
                    f"{fname} — suspiciously high mean reflectance ({vmean:.4f})"))

        passed.append("reflectance_range")

    def _check_cloud_mask(self, ctx, issues, passed):
        from Valid_Pixel_Mask.valid_pixel_mask import MaskBits

        mask_stats = ctx.data.get("mask_stats")
        mask_path = ctx.data.get("mask_path")
        log.info("[validation] Cloud mask check:")

        if mask_stats:
            valid_frac = mask_stats.get("valid_fraction")
            flags = mask_stats.get("flags", {})
            if valid_frac is not None:
                fill_pct = flags.get("fill", {}).get("pct", 0.0)
                log.info(
                    "  Valid (clear) fraction : %.2f%%  "
                    "(of full Landsat raster; %.1f%% of that raster is fill)",
                    valid_frac * 100, fill_pct,
                )
                total = mask_stats.get("total_pixels", 1)
                for flag_name, flag_data in flags.items():
                    count = flag_data.get("count", 0)
                    log.info("  %-24s : %d px  (%.2f%%)", flag_name, count, 100.0 * count / total)
                if valid_frac < self.VALID_WARN_FRAC:
                    issues.append(("WARN",
                        f"Only {valid_frac:.1%} of scene is cloud-free "
                        f"(threshold {self.VALID_WARN_FRAC:.0%}) — results may be unreliable"))
                else:
                    passed.append("cloud_mask_coverage")
            else:
                issues.append(("WARN", "mask_stats present but valid_fraction is None"))

        elif mask_path and Path(str(mask_path)).exists():
            with rasterio.open(mask_path) as src:
                mask_data = src.read(1)
            n_total = mask_data.size
            n_valid = int(((mask_data & (1 << MaskBits.CLEAR)) != 0).sum())
            valid_frac = n_valid / n_total if n_total > 0 else 0.0
            log.info("  Valid (clear) fraction : %.2f%%  (recomputed from file)", valid_frac * 100)
            if valid_frac < self.VALID_WARN_FRAC:
                issues.append(("WARN", f"Only {valid_frac:.1%} of scene is cloud-free"))
            else:
                passed.append("cloud_mask_coverage")
        else:
            issues.append(("WARN", "cloud mask check skipped — mask_path and mask_stats both absent"))

    def _check_brdf_effect(self, ctx, issues, passed):
        """Validate that BRDF correction actually changed the reflectance values."""
        sbaf_dir = ctx.data.get("sbaf_dir")
        nbar_ls_dir = ctx.data.get("nbar_ls_dir")
        log.info("[validation] BRDF correction effect (pre vs post NBAR):")

        if not sbaf_dir or not nbar_ls_dir:
            issues.append(("WARN", "BRDF check skipped — sbaf_dir or nbar_ls_dir not available"))
            return

        stats = _compute_brdf_band_stats(sbaf_dir, nbar_ls_dir)
        if not stats:
            issues.append(("WARN", "BRDF check — no bands could be compared"))
            return

        max_change = max(s.median_rel_change for s in stats.values())
        if max_change < self.BRDF_NOOP_FLOOR:
            issues.append((
                "FAIL",
                f"BRDF correction appears to have done nothing — max median "
                f"relative change across {len(stats)} bands was {max_change:.2%} "
                f"(floor {self.BRDF_NOOP_FLOOR:.2%}). "
                "Check sun angles in the MTL and the NBAR step's logs.",
            ))

        for s in stats.values():
            log.info(
                "  %-6s : pre_mean=%.4f  post_mean=%.4f  delta=%+.4f  "
                "median_rel_change=%.2f%%  (on pixels where pre>0.05)",
                s.band, s.mean_pre, s.mean_post, s.mean_delta, s.median_rel_change * 100,
            )
            if s.median_rel_change > self.BRDF_WARN_RATIO:
                issues.append((
                    "WARN",
                    f"BRDF correction for {s.band} changed reflectance by "
                    f"{s.median_rel_change:.1%} (threshold {self.BRDF_WARN_RATIO:.0%})",
                ))

        passed.append("brdf_effect")

    def _check_geometry(self, ctx, issues, passed):
        geo_ls = ctx.data.get("geo_ls")
        log.info("[validation] Geometry check:")

        if not geo_ls or not Path(str(geo_ls)).exists():
            issues.append(("WARN", "geometry check skipped — geo_ls not available"))
            return

        geo_path = Path(str(geo_ls))
        tifs = sorted(geo_path.glob("*_B*.TIF"))
        if not tifs:
            issues.append(("FAIL", f"geometry check — no band TIFs found in {geo_path}"))
            return

        target_res = ctx.config.get("geometric_processing", {}).get("resolution", 10.0)
        log.info("  Band TIFs found: %d", len(tifs))

        res_ok = True
        crs_ok = True
        for fpath in tifs[:3]:
            with rasterio.open(fpath) as src:
                crs = src.crs
                pixel_w = abs(src.transform.a)
                pixel_h = abs(src.transform.e)

            if not (crs and crs.is_projected):
                issues.append(("WARN",
                    f"{fpath.name} — CRS is not projected (got {crs}); expected UTM"))
                crs_ok = False

            res_err_w = abs(pixel_w - target_res) / target_res
            res_err_h = abs(pixel_h - target_res) / target_res
            if res_err_w > 0.01 or res_err_h > 0.01:
                issues.append(("WARN",
                    f"{fpath.name} — pixel size {pixel_w:.2f}×{pixel_h:.2f}m "
                    f"deviates >1% from target {target_res}m"))
                res_ok = False
            else:
                log.info(
                    "  %-30s  res=%.2f×%.2fm  crs=%s",
                    fpath.name, pixel_w, pixel_h,
                    crs.to_epsg() if crs else "unknown",
                )

        if crs_ok and res_ok:
            passed.append("geometry")

    def _check_fusion_output(self, ctx, issues, passed):
        fused_dir = ctx.data.get("fused_dir")
        log.info("[validation] Fusion output completeness:")

        if not fused_dir or not Path(str(fused_dir)).exists():
            issues.append(("FAIL", "fusion output check — fused_dir not available"))
            return

        fused_path = Path(str(fused_dir))
        band_files = [
            f for f in sorted(fused_path.glob("*_10m.TIF"))
            if "VALIDITY" not in f.name.upper()
        ]

        EXPECTED_LS_BANDS = {"B2", "B3", "B4", "B5", "B6", "B7"}
        found_bands: set[str] = set()

        for fpath in band_files:
            band = fpath.stem.split("_")[0].upper()
            found_bands.add(band)

            if fpath.stat().st_size < 1024:
                issues.append(("FAIL",
                    f"{fpath.name} — file is suspiciously small ({fpath.stat().st_size} bytes)"))
                continue
            try:
                with rasterio.open(fpath) as src:
                    src.read(1, window=rasterio.windows.Window(0, 0, 256, 256))
                log.info("  %-30s  %.1f MB  OK", fpath.name, fpath.stat().st_size / 1e6)
            except Exception as exc:
                issues.append(("FAIL", f"{fpath.name} — unreadable: {exc}"))

        missing = EXPECTED_LS_BANDS - found_bands
        if missing:
            # Missing bands produce a degraded product — treat as FAIL so
            # packaging_validated becomes False and strict cleanup is blocked.
            issues.append(("FAIL",
                f"Expected Landsat bands not found in fusion output: {sorted(missing)}"))
        else:
            log.info("  All expected Landsat bands present: %s", sorted(found_bands))
            passed.append("fusion_completeness")

        mask_sidecar = fused_path / "FUSION_VALIDITY_MASK.TIF"
        if mask_sidecar.exists():
            log.info("  FUSION_VALIDITY_MASK.TIF — present")
            passed.append("validity_mask_sidecar")
        else:
            issues.append(("WARN", "FUSION_VALIDITY_MASK.TIF not found in fused_dir"))


_POST_PROCESSING_STEPS = ("validation", "cleanup")


def _register_all_steps(pipeline: "Pipeline", packaging_cls: type) -> "Pipeline":
    return pipeline.register(
        GeometricProcessingStep,
        AtmosphericCorrectionStep,
        SBAFStep,
        ValidPixelMaskStep,
        BRDFAdjustmentStep,
        DataFusionStep,
        packaging_cls,
        ValidationStep,
        CleanupStep,
    )


def _extend_routed_with_post_processing(
    routed_steps: list[str],
    user_only: list[str] | None,
) -> list[str]:
    for always_step in _POST_PROCESSING_STEPS:
        user_excluded = user_only is not None and always_step not in user_only
        if not user_excluded and always_step not in routed_steps:
            routed_steps.append(always_step)
    return routed_steps


# ---------------------------------------------------------------------------
# run_many — Spark edition (with adaptive tile routing)
# ---------------------------------------------------------------------------

def run_many(
    product_ids: list[str],
    config: dict,
    working_dir: str,
    workers: int = 1,
    only: list[str] | None = None,
    resume: bool = True,
    fallback_on_router_error: bool = False,  
) -> None:
    """Process many Landsat scenes in parallel via Spark."""
    spark = _get_spark(workers=workers)
    sc = spark.sparkContext

    bc_config = sc.broadcast(config)
    bc_working_dir = sc.broadcast(working_dir)
    bc_only = sc.broadcast(only)
    bc_resume = sc.broadcast(resume)
    bc_base = sc.broadcast(str(_BASE))
    bc_fallback = sc.broadcast(fallback_on_router_error)  

    def _process_product(landsat_path: str) -> list[dict]:
        import os as _os
        import sys as _sys

        _os.environ[_IN_SPARK_EXECUTOR_ENV] = "1"

        this_mod = _sys.modules[__name__]
        current_base = (
            str(getattr(this_mod, "_BASE", ""))
            if getattr(this_mod, "_bootstrap_done", False) else ""
        )
        if current_base and current_base != bc_base.value:
            _bootstrap(bc_base.value, force=True)
        elif not getattr(this_mod, "_bootstrap_done", False):
            _bootstrap(bc_base.value)

        tile_class_value, routed_steps = _route_landsat_scene(
            landsat_path,
            user_only=bc_only.value,
            fallback_on_error=bc_fallback.value,
        )
        if routed_steps is None:
            return [{
                "step": "__routing__", "success": True, "elapsed": 0.0,
                "error": "", "skipped": True, "tile_class": tile_class_value,
            }]

        routed_steps = _extend_routed_with_post_processing(routed_steps, bc_only.value)

        from Packaging.PackagingStep import PackagingStep as _PackagingStep

        product_config = {**bc_config.value, "landsat_path": landsat_path}
        pipeline = _register_all_steps(
            Pipeline(product_config, bc_working_dir.value),
            _PackagingStep,
        )
        results = pipeline.run(
            landsat_path,
            only         = routed_steps,
            resume       = bc_resume.value,
            tile_class   = tile_class_value,
            steps_routed = routed_steps,
        )
        return [
            {"step": r.step, "success": r.success, "elapsed": r.elapsed, "error": r.error}
            for r in results
        ]

    all_results = (
        sc.parallelize(product_ids, numSlices=len(product_ids))
        .map(_process_product)
        .collect()
    )

    for pid, results in zip(product_ids, all_results):
        if len(results) == 1 and results[0].get("skipped"):
            log.info("Product %s SKIPPED by router (class=%s).", pid, results[0].get("tile_class"))
            continue
        failed = [r for r in results if not r["success"]]
        if failed:
            log.error("Product %s FAILED at: %s — %s", pid, failed[0]["step"], failed[0]["error"])
        else:
            log.info("Product %s completed successfully.", pid)

    try:
        from report.generate_multi_report import generate_multi_report
        product_out_dirs = [Path(working_dir) / Path(pid).name for pid in product_ids]
        report_path = Path(working_dir) / "report.html"
        generate_multi_report(product_out_dirs, report_path)
        log.info("[report] Multi-product report → %s", report_path)
    except Exception as exc:
        log.warning("[report] Could not generate multi-product report: %s", exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_default_config(args: argparse.Namespace) -> dict:
    spacecraft = _detect_spacecraft(args.products[0])
    sensor_tag = _spacecraft_short_tag(spacecraft)   # "L8" or "L9"

    return {
        "landsat_path": args.products[0],
        "s2_path":      args.s2_path,
        "gri_path":     str(_BASE / "Geometric_Processing" / "gri" / "GRI_T31UDQ.tif"),
        "gri_cache_dir": str(_BASE / "Geometric_Processing" / "gri"),
        "dem_path":     None,
        "lut_path":     str(_BASE / "lut" / f"lut_6s_{sensor_tag}.json"),

        "atm_sensor_tag": sensor_tag,

        "geometric_processing": {
            "resolution": 10.0,
            "max_shift":  200.0,
            "s2_bands":   ["B02", "B03", "B04", "B08"],
            "ls_bands":   ["B2",  "B3",  "B4",  "B5",  "B6",  "B7"],
        },
        "atmospheric_correction": {},

        "sbaf": {
            "mission":      spacecraft,         # "LANDSAT_8" or "LANDSAT_9"
            "s2_target":    "Sentinel-2A",
            "adaptive":     True,
            "chunks":       1024,
            "output_scale": 1.0,
        },

        "valid_pixel_mask": {
            "cloud_dilation_radius":  3,
            "shadow_dilation_radius": 3,
            "use_cirrus":             True,
            "use_dilated_cloud":      True,
            "exclude_water":          args.exclude_water,
            "chunk_size":             1024,
        },

        "brdf_adjustment": {
            "workers":  args.workers,
            "clip_min": 0.8,
            "clip_max": 1.2,
        },

        "data_fusion": {
            "workers": args.workers,
        },

        "packaging": {
            "product_level":        "L2F",
            "processing_baseline":  "05.00",
            "quantification_value": 10000,
            "output_dtype":         "uint16",
            "compress":             "deflate",
            "cog":                  True,
            "nodata_out":           0,
        },

        "cleanup": {
            "mode":         args.cleanup_mode,
            "require_safe": True,
            "dry_run":      args.cleanup_dry_run,
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="sen2like pipeline — geometric → atm → SBAF → mask → BRDF → fusion"
    )
    parser.add_argument("products", nargs="+",
                        help="Path(s) to Landsat scene directory/directories")
    parser.add_argument("--working-dir", default="./output_pipeline")
    parser.add_argument(
        "--steps", nargs="+",
        choices=[
            "geometric_processing", "atmospheric_correction", "sbaf",
            "valid_pixel_mask", "brdf_adjustment", "data_fusion",
            "packaging", "validation", "cleanup",
        ],
        help="Run only these steps. Packaging always runs.",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--s2-path", default=None)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--no-routing", action="store_true")
    parser.add_argument("--base-dir", default=None)
    parser.add_argument("--exclude-water", action="store_true")
    parser.add_argument("--cleanup-mode", default="none", choices=_CLEANUP_MODE_ORDER)
    parser.add_argument("--cleanup-dry-run", action="store_true")
    parser.add_argument(
        "--router-fallback-ok",
        action="store_true",
        help="If routing fails, fall back to running the full pipeline. "
             "Default is to fail loudly so router bugs are visible.",
    )
    return parser.parse_args()


def _stop_spark_if_active() -> None:
    try:
        from pyspark.sql import SparkSession
        active = SparkSession.getActiveSession()
        if active:
            active.stop()
    except Exception:
        pass


def main() -> int:
    _install_excepthook()
    args = _parse_args()
    _bootstrap(args.base_dir)

    from Packaging.PackagingStep import PackagingStep

    config = _build_default_config(args)
    resume = not args.no_resume

    if len(args.products) == 1:
        if args.no_routing:
            routed_steps = args.steps
            tile_class_value = "MIXED"
            log.info("[router] Routing disabled — running %s", routed_steps or "all steps")
        else:
            tile_class_value, routed_steps = _route_landsat_scene(
                args.products[0],
                user_only=args.steps,
                fallback_on_error=args.router_fallback_ok, 
            )
            if routed_steps is None:
                log.warning("[router] Product classified as SKIP — nothing to process.")
                return 0
            routed_steps = _extend_routed_with_post_processing(routed_steps, args.steps)

        pipeline = _register_all_steps(
            Pipeline(config, args.working_dir),
            PackagingStep,
        )
        pipeline.run(
            args.products[0],
            only         = routed_steps,
            resume       = resume,
            tile_class   = tile_class_value,
            steps_routed = routed_steps,
        )

        try:
            from report.generate_report import generate_report
            generate_report(Path(args.working_dir) / Path(args.products[0]).name)
        except Exception as exc:
            log.warning("[report] Could not generate HTML report: %s", exc)

        _stop_spark_if_active()
    else:
        run_many(
            args.products,
            config,
            args.working_dir,
            workers=args.workers,
            only=args.steps,
            resume=resume,
            fallback_on_router_error=args.router_fallback_ok,   
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())