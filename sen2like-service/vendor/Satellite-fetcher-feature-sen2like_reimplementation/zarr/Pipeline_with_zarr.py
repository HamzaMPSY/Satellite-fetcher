from __future__ import annotations

import hashlib
import importlib
import json
import logging
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from Monitoring.metrics import MetricsCollector
import numpy as np
import rasterio

# ---------------------------------------------------------------------------
# Pipeline-wide nodata sentinel
# ---------------------------------------------------------------------------
PIPELINE_NODATA: float = -9999.0

_BASE: Path
atm_mod: object | None = None
_bootstrap_done: bool = False


def _bootstrap(base_dir: str | Path | None = None) -> None:
    global _BASE, atm_mod, _bootstrap_done

    resolved = Path(
        str(base_dir) if base_dir is not None
        else os.environ.get("LANDSAT_UPSAMPLING_BASE",
                             str(Path(__file__).resolve().parent))
    ).resolve()

    if _bootstrap_done:
        if resolved != _BASE:
            raise RuntimeError(
                f"_bootstrap() already called with base={_BASE}; "
                f"cannot reinitialise to {resolved}."
            )
        return

    _BASE = resolved

    for _subdir in [
        _BASE / "Geometric_Processing",
        _BASE / "atmospheric_correction",
        _BASE / "SBAF",
        _BASE / "Valid_Pixel_Mask",
        _BASE / "BRDF_Adjustment",
        _BASE / "data_fusion",
    ]:
        _s = str(_subdir)
        if _s not in sys.path:
            sys.path.insert(0, _s)

    module_name = "atmospheric_correction.atmospheric_correction_pipeline"
    try:
        atm_mod = importlib.import_module(module_name)
    except ImportError as exc:
        raise ImportError(
            f"Cannot load atmospheric correction module '{module_name}'."
        ) from exc
    _bootstrap_done = True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sen2like")

_IN_SPARK_EXECUTOR_ENV = "IN_SPARK_EXECUTOR"


def _running_inside_spark_executor() -> bool:
    return os.environ.get(_IN_SPARK_EXECUTOR_ENV, "") == "1"


def _get_spark(app_name: str = "sen2like", workers: int = 4):
    from pyspark.sql import SparkSession
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    master = os.environ.get("SPARK_MASTER", f"local[{workers}]")
    spark = (
        SparkSession.builder.master(master).appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


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


def _pipeline_version() -> str:
    import subprocess
    try:
        r = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                           capture_output=True, text=True, timeout=3,
                           cwd=str(_BASE))
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    return "version 1"


def _outputs_from_ctx_data(step_name: str, ctx_data: dict) -> list[str]:
    STEP_OUTPUT_KEYS = {
        "zarr_ingestion":         ["zarr_imagery_dir", "zarr_ancillary_dir"],
        "geometric_processing":   ["geo_ls", "geo_s2"],
        "atmospheric_correction": ["atm_dir", "toa_path"],
        "sbaf":                   ["sbaf_dir"],
        "valid_pixel_mask":       ["mask_path"],
        "brdf_adjustment":        ["nbar_ls_dir", "nbar_s2_dir"],
        "data_fusion":            ["fused_dir"],
        "validation":             [],
    }
    paths = []
    for key in STEP_OUTPUT_KEYS.get(step_name, []):
        val = ctx_data.get(key)
        if val is not None:
            p = Path(str(val))
            if p.exists():
                paths.append(str(p))
    return paths


def _input_paths_from_config(config: dict) -> dict[str, str]:
    keys = ["landsat_path", "s2_path", "gri_path", "dem_path", "lut_path", "zarr_path"]
    return {k: str(config[k]) for k in keys if config.get(k)}


def _using_zarr(config: dict) -> bool:
    return bool(config.get("zarr_path"))


# ---------------------------------------------------------------------------
# Helper — resolve landsat_path safely (may be absent in Zarr mode)
# ---------------------------------------------------------------------------

def _landsat_path(config: dict) -> Path | None:
    lp = config.get("landsat_path")
    if not lp:
        return None
    p = Path(str(lp))
    return p if p.exists() else None


def _scene_id(config: dict, ctx: "Context") -> str:
    """Return a scene identifier even when landsat_path is absent."""
    lp = config.get("landsat_path")
    if lp:
        return Path(str(lp)).name
    # Fall back to product_id or Zarr store name
    zp = config.get("zarr_path")
    if zp:
        return Path(str(zp)).stem
    return ctx.product_id


# ---------------------------------------------------------------------------
# Pipeline class
# ---------------------------------------------------------------------------

class Pipeline:
    def __init__(self, config: dict, working_dir: str | Path):
        self.config = config
        self.working_dir = Path(working_dir)
        self.steps: list = []
        self._version = _pipeline_version()
        self._metrics: MetricsCollector | None = None

    def _step_order(self) -> list[str]:
        return [cls.name for cls in self.steps]

    def register(self, *step_classes):
        self.steps.extend(step_classes)
        return self

    def _manifest_path(self, product_id: str) -> Path:
        return self.working_dir / Path(product_id).name / "manifest.json"

    def _checkpoint_path(self, product_id: str) -> Path:
        return self.working_dir / Path(product_id).name / "checkpoint.json"

    def _load_manifest(self, product_id: str) -> dict:
        mp = self._manifest_path(product_id)
        if not mp.exists():
            cp = self._checkpoint_path(product_id)
            if cp.exists():
                try:
                    old       = json.loads(cp.read_text())
                    completed = old.get("completed", [])
                    return self._seed_manifest_from_completed(product_id, completed)
                except (json.JSONDecodeError, OSError):
                    pass
            return self._empty_manifest(product_id)
        try:
            data = json.loads(mp.read_text())
            completed = [n for n, s in data.get("steps", {}).items()
                         if s.get("status") == "success"]
            if completed:
                log.info("[manifest] Resuming — completed: %s", sorted(completed))
            return data
        except (json.JSONDecodeError, OSError):
            corrupt = mp.with_suffix(".json.corrupt")
            log.warning("[manifest] Corrupt manifest — renaming to %s", corrupt.name)
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
            "steps":            {},
            "completed":        [],
        }

    def _seed_manifest_from_completed(self, product_id: str, completed: list[str]) -> dict:
        manifest = self._empty_manifest(product_id)
        for name in completed:
            manifest["steps"][name] = {
                "status": "success", "started_at": None,
                "finished_at": None, "elapsed": None,
                "outputs": [], "input_paths": {},
                "config_hash": None, "config_snapshot": {},
                "error": None, "migrated_from_checkpoint": True,
            }
        manifest["completed"] = sorted(completed)
        return manifest

    def _save_manifest(self, product_id: str, manifest: dict) -> None:
        mp = self._manifest_path(product_id)
        mp.parent.mkdir(parents=True, exist_ok=True)
        manifest["completed"] = sorted(
            n for n, s in manifest.get("steps", {}).items()
            if s.get("status") == "success"
        )
        tmp = mp.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(manifest, indent=2, default=str))
        tmp.replace(mp)
        cp     = self._checkpoint_path(product_id)
        tmp_cp = cp.with_suffix(".json.tmp")
        tmp_cp.write_text(json.dumps({"completed": manifest["completed"]}, indent=2))
        tmp_cp.replace(cp)

    def _mark_step_running(self, manifest, step_name, config):
        manifest["steps"][step_name] = {
            "status": "running", "started_at": _utcnow(),
            "finished_at": None, "elapsed": None,
            "outputs": [], "input_paths": _input_paths_from_config(self.config),
            "config_hash": _dict_hash(config), "config_snapshot": config,
            "error": None,
        }

    def _mark_step_success(self, manifest, step_name, elapsed, outputs):
        s = manifest["steps"].setdefault(step_name, {})
        s.update({"status": "success", "finished_at": _utcnow(),
                  "elapsed": round(elapsed, 3), "outputs": outputs, "error": None})

    def _mark_step_failed(self, manifest, step_name, elapsed, error):
        s = manifest["steps"].setdefault(step_name, {})
        s.update({"status": "failed", "finished_at": _utcnow(),
                  "elapsed": round(elapsed, 3), "error": error})

    def _mark_step_skipped(self, manifest, step_name):
        if step_name not in manifest["steps"]:
            manifest["steps"][step_name] = {
                "status": "skipped", "started_at": None,
                "finished_at": None, "elapsed": None,
                "outputs": [], "input_paths": {},
                "config_hash": None, "config_snapshot": {}, "error": None,
            }

    def _step_is_valid(self, step_name: str, manifest: dict) -> bool:
        entry = manifest.get("steps", {}).get(step_name, {})
        if entry.get("status") != "success":
            return False
        stored_hash = entry.get("config_hash")
        if stored_hash is None:
            return False
        if stored_hash != _dict_hash(self.config.get(step_name, {})):
            log.info("[resume] %s: config changed — invalidating", step_name)
            return False
        stored_inputs = entry.get("input_paths")
        if stored_inputs is None:
            return False
        if stored_inputs != _input_paths_from_config(self.config):
            log.info("[resume] %s: input paths changed — invalidating", step_name)
            return False
        return True

    def _invalidate_from(self, step_name: str, manifest: dict) -> None:
        order = self._step_order()
        try:
            start = order.index(step_name)
        except ValueError:
            return
        for name in order[start:]:
            if name in manifest.get("steps", {}):
                manifest["steps"][name]["status"] = "invalidated"

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(
        self,
        product_id: str,
        only: list[str] | None = None,
        resume: bool = True,
    ) -> list[Result]:
        seen: set[str] = set()
        for cls in self.steps:
            if cls.name in seen:
                raise ValueError(f"Duplicate step name '{cls.name}'.")
            seen.add(cls.name)

        ctx = Context(
            product_id=product_id,
            working_dir=self.working_dir / Path(product_id).name,
            config=self.config,
        )
        ctx.working_dir.mkdir(parents=True, exist_ok=True)
        self._metrics = MetricsCollector(product_id)

        manifest = self._load_manifest(product_id) if resume else self._empty_manifest(product_id)

        if resume:
            for cls in self.steps:
                if only and cls.name not in only:
                    continue
                if (manifest.get("steps", {}).get(cls.name, {}).get("status") == "success"
                        and not self._step_is_valid(cls.name, manifest)):
                    self._invalidate_from(cls.name, manifest)
                    break

        manifest["pipeline_version"] = self._version
        manifest["config_hash"]      = _dict_hash(self.config)
        manifest["input_paths"]      = _input_paths_from_config(self.config)
        if manifest.get("started_at") is None:
            manifest["started_at"] = _utcnow()

        completed = {n for n, s in manifest.get("steps", {}).items()
                     if s.get("status") == "success"}
        results: list[Result] = []

        for cls in self.steps:
            if only and cls.name not in only:
                continue

            step_config = self.config.get(cls.name, {})

            if cls.name in completed:
                log.info("⏭ %-30s skipped (checkpoint)", cls.name)
                self._mark_step_skipped(manifest, cls.name)
                results.append(Result(cls.name, success=True, elapsed=0.0))
                step = cls(step_config)
                if hasattr(step, "restore_context"):
                    step.restore_context(ctx)
                continue

            step = cls(step_config)
            self._mark_step_running(manifest, cls.name, step_config)
            self._save_manifest(product_id, manifest)
            log.info("▶ %-30s starting", cls.name)

            t0 = time.perf_counter()
            try:
                step_outputs = step.run(ctx)
                ctx.data.update(step_outputs or {})
                elapsed = time.perf_counter() - t0

                output_paths = _outputs_from_ctx_data(cls.name, ctx.data)
                self._mark_step_success(manifest, cls.name, elapsed, output_paths)
                self._save_manifest(product_id, manifest)
                log.info("✓ %-30s %.2fs", cls.name, elapsed)
                results.append(Result(cls.name, success=True, elapsed=elapsed,
                                      outputs=output_paths))
                completed.add(cls.name)

                if self._metrics:
                    self._metrics.push_step(cls.name, elapsed, "success", len(output_paths))
                    if cls.name == "valid_pixel_mask":
                        vf = (ctx.data.get("mask_stats") or {}).get("valid_fraction")
                        if vf is not None:
                            self._metrics.push_valid_pixel_fraction(vf)
                    if cls.name == "brdf_adjustment":
                        if ctx.data.get("nbar_ls_dir") and ctx.data.get("sbaf_dir"):
                            _push_brdf_deltas(self._metrics,
                                              ctx.data["sbaf_dir"],
                                              ctx.data["nbar_ls_dir"])

            except Exception as e:
                elapsed = time.perf_counter() - t0
                error_msg = str(e)
                self._mark_step_failed(manifest, cls.name, elapsed, error_msg)
                self._save_manifest(product_id, manifest)
                log.error("✗ %-30s %.2fs — %s", cls.name, elapsed, e)
                results.append(Result(cls.name, success=False,
                                      elapsed=elapsed, error=error_msg))
                if self._metrics:
                    self._metrics.push_step(cls.name, elapsed, "failed")
                break

        manifest["finished_at"] = _utcnow()
        self._save_manifest(product_id, manifest)

        if self._metrics:
            n_ok   = sum(1 for r in results if r.success)
            n_fail = sum(1 for r in results if not r.success)
            self._metrics.push_pipeline_complete(
                sum(r.elapsed for r in results), n_ok, n_fail)

        log.info("[manifest] Written → %s", self._manifest_path(product_id))
        return results


def _push_brdf_deltas(mc, sbaf_dir, nbar_dir) -> None:
    try:
        from pathlib import Path as _P
        BANDS = {
            "Blue":  ("*_SBAF_B2.TIF", "NBAR_Blue.tif"),
            "Green": ("*_SBAF_B3.TIF", "NBAR_Green.tif"),
            "Red":   ("*_SBAF_B4.TIF", "NBAR_Red.tif"),
            "NIR":   ("*_SBAF_B5.TIF", "NBAR_NIR.tif"),
            "SWIR1": ("*_SBAF_B6.TIF", "NBAR_SWIR1.tif"),
            "SWIR2": ("*_SBAF_B7.TIF", "NBAR_SWIR2.tif"),
        }
        deltas = {}
        for band, (pat, nbar_name) in BANDS.items():
            sbaf_files = list(_P(str(sbaf_dir)).glob(pat))
            nbar_file  = _P(str(nbar_dir)) / nbar_name
            if not sbaf_files or not nbar_file.exists():
                continue
            with rasterio.open(sbaf_files[0]) as src:
                pre = src.read(1).astype(np.float32); nd = src.nodata
            with rasterio.open(nbar_file) as src:
                post = src.read(1).astype(np.float32)
            nd_val = float(nd) if nd is not None else -9999.0
            valid  = (pre != nd_val) & (post != nd_val) & (pre != 0)
            if valid.any():
                deltas[band] = float((post[valid] - pre[valid]).mean())
        if deltas:
            mc.push_brdf_deltas(deltas)
    except Exception as exc:
        log.debug("[metrics] BRDF delta push failed (non-fatal): %s", exc)


# ===========================================================================
# Step 0 — Zarr Ingestion
# ===========================================================================

class ZarrIngestionStep:
    """
    Ingests imagery, ancillary, QA bands and MTL metadata from a Nimbus Zarr
    store.  Registered automatically when config['zarr_path'] is set.
    After this step runs, no downstream step requires landsat_path.
    """
    name = "zarr_ingestion"

    def __init__(self, config: dict) -> None:
        self.config    = config
        self._ingestor: Any = None

    def restore_context(self, ctx: Context) -> None:
        # Temp dirs are gone after restart — mark as needing re-run.
        log.info("[zarr] restore_context: ephemeral dirs gone, step will re-run.")

    def run(self, ctx: Context) -> dict:
        from .zarr_ingestor import ZarrIngestor, populate_context_from_zarr

        # Create persistent output dirs directly in working_dir.
        # The ingestor writes GeoTIFFs here — no temp-dir copy needed.
        zarr_imagery_dir = ctx.working_dir / "zarr_imagery"
        zarr_anc_dir     = ctx.working_dir / "zarr_ancillary"
        zarr_qa_dir_path = ctx.working_dir / "zarr_qa"
        for d in [zarr_imagery_dir, zarr_anc_dir, zarr_qa_dir_path]:
            d.mkdir(parents=True, exist_ok=True)

        # Pass persistent dirs via config so ingestor writes directly there.
        patched = {
            **ctx.config,
            "_zarr_imagery_out_dir":   zarr_imagery_dir,
            "_zarr_ancillary_out_dir": zarr_anc_dir,
            "_zarr_qa_out_dir":        zarr_qa_dir_path,
        }

        self._ingestor = ZarrIngestor(patched)
        result = self._ingestor.ingest()
        populate_context_from_zarr(ctx, result)
        ctx.data["_zarr_ingestor"] = self._ingestor

        # MTL: copy small JSON file from temp to working_dir.
        zarr_mtl_path = result.mtl_path
        if zarr_mtl_path is not None:
            dst_mtl = ctx.working_dir / Path(str(zarr_mtl_path)).name
            if not dst_mtl.exists():
                shutil.copy2(zarr_mtl_path, dst_mtl)
            zarr_mtl_path = dst_mtl

        out: dict[str, Any] = {
            "geo_ls":              result.imagery_dir,
            "geo_s2":              None,
            "zarr_imagery_dir":    result.imagery_dir,
            "zarr_imagery_meta":   result.imagery_meta,
            "zarr_ancillary_dir":  result.ancillary_dir,
            "zarr_ancillary_meta": result.ancillary_meta,
            "zarr_store_meta":     result.store_meta,
            "zarr_qa_dir":         result.qa_dir,
            "zarr_mtl_path":       zarr_mtl_path,
            "zarr_ingested":       True,
        }

        log.info("[zarr] Ingestion complete — geo_ls=%s  qa=%s  mtl=%s",
                 result.imagery_dir, result.qa_dir, zarr_mtl_path)
        return out


# ===========================================================================
# Step 1 — Geometric Processing
# ===========================================================================

class GeometricProcessingStep:
    name = "geometric_processing"

    def __init__(self, config):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        if ctx.data.get("zarr_ingested"):
            d = ctx.working_dir / "zarr_imagery"
            if d.exists():
                ctx.data["geo_ls"] = d
            ctx.data["geo_s2"] = None
            return
        lp = _landsat_path(ctx.config)
        if lp:
            geo_ls_dir = ctx.working_dir / "geo" / lp.name
            if geo_ls_dir.exists():
                ctx.data["geo_ls"] = geo_ls_dir
        ctx.data["geo_s2"] = None

    def run(self, ctx: Context) -> dict:
        # ── Zarr fast-path ────────────────────────────────────────────────
        if ctx.data.get("zarr_ingested"):
            geo_ls = ctx.data.get("geo_ls")
            if geo_ls and Path(str(geo_ls)).exists():
                log.info("[geo] Zarr source — skipping GRI co-registration (%s)", geo_ls)
                return {"geo_ls": Path(str(geo_ls)), "geo_s2": None}
            raise RuntimeError(
                "[geo] zarr_ingested=True but geo_ls is missing. "
                "Ensure ZarrIngestionStep ran first."
            )

        # ── Original GeoTIFF path ─────────────────────────────────────────
        from Geometric_Processing.pipelineGRI import GRIConfig, load_gri, process_scene

        lp = _landsat_path(ctx.config)
        if lp is None:
            raise RuntimeError(
                "[geo] landsat_path is required for GeoTIFF mode but is not set."
            )

        gri_path = Path(ctx.config["gri_path"])
        if not gri_path.exists():
            raise FileNotFoundError(f"GRI not found: {gri_path}")

        _validate_gri_overlap(gri_path, lp)
        output_dir = ctx.working_dir / "geo"
        output_dir.mkdir(parents=True, exist_ok=True)

        cfg = GRIConfig(
            gri_path=gri_path,
            target_resolution=self.config.get("resolution", 10.0),
            max_shift_pixels=self.config.get("max_shift", 50.0),
            output_dir=output_dir,
            sentinel2_bands=self.config.get("s2_bands", ["B02", "B03", "B04", "B08"]),
            landsat_optical_bands=self.config.get("ls_bands",
                                                   ["B2", "B3", "B4", "B5", "B6", "B7"]),
            dem_path=Path(ctx.config["dem_path"]) if ctx.config.get("dem_path") else None,
            do_orthorectify=bool(ctx.config.get("dem_path")),
        )
        gri = load_gri(cfg)
        geo_ls_dir = Path(process_scene(lp, gri, cfg))
        if not geo_ls_dir.exists():
            raise RuntimeError(f"[geo] Output dir missing: {geo_ls_dir}")
        if not list(geo_ls_dir.glob("*_B*.TIF")):
            raise RuntimeError(f"[geo] No band TIFs in {geo_ls_dir}")
        _copy_mtl_files(lp, geo_ls_dir)

        geo_s2_dir = None
        s2_path = ctx.config.get("s2_path")
        if s2_path and Path(s2_path).exists():
            geo_s2_dir = process_scene(Path(s2_path), gri, cfg)
        return {"geo_ls": geo_ls_dir, "geo_s2": geo_s2_dir}


# ===========================================================================
# Step 2 — Atmospheric Correction
# ===========================================================================

class AtmosphericCorrectionStep:
    name = "atmospheric_correction"

    def __init__(self, config):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        atm_dir = ctx.working_dir / "atm_corr"
        if atm_dir.exists():
            ctx.data["atm_dir"] = atm_dir
            ctx.data["bands_are_reflectance"] = True

    def run(self, ctx: Context) -> dict:
        if ctx.data.get("zarr_ingested"):
            return self._run_zarr(ctx)
        return self._run_geotiff(ctx)

    # ------------------------------------------------------------------
    # Zarr branch — BOA already available, just rename files
    # ------------------------------------------------------------------

    def _run_zarr(self, ctx: Context) -> dict:
        geo_ls = ctx.data.get("geo_ls")
        if geo_ls is None or not Path(str(geo_ls)).exists():
            raise RuntimeError(
                "[atm/zarr] geo_ls missing. Ensure ZarrIngestionStep ran first."
            )

        source_dir  = Path(str(geo_ls))
        output_dir  = ctx.working_dir / "atm_corr"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Scene prefix for output filenames.
        scene_prefix = _scene_id(ctx.config, ctx)

        band_map: dict[str, str] = dict(ctx.config.get("zarr_atm_band_map") or {})
        imagery_meta: dict[str, Any] = ctx.data.get("zarr_imagery_meta") or {}
        band_names: list[str] = list(imagery_meta.get("band_names") or
                                     [f.stem for f in sorted(source_dir.glob("*.TIF"))])

        written: list[Path] = []
        for band_name in band_names:
            src = source_dir / f"{band_name}.TIF"
            if not src.exists():
                log.warning("[atm/zarr] Source TIF missing for '%s' — skipping", band_name)
                continue
            dst_stem = band_map.get(band_name) or _zarr_band_to_sr_stem(band_name, scene_prefix)
            dst = output_dir / f"{dst_stem}.TIF"
            shutil.copy2(src, dst)
            written.append(dst)

        if not written:
            raise RuntimeError(
                f"[atm/zarr] No bands staged in {output_dir}."
            )

        # Copy MTL from Zarr extraction if available, else from landsat_path.
        mtl_src = ctx.data.get("zarr_mtl_path")
        if mtl_src and Path(str(mtl_src)).exists():
            shutil.copy2(mtl_src, output_dir / Path(str(mtl_src)).name)
        else:
            lp = _landsat_path(ctx.config)
            if lp:
                _copy_mtl_files(lp, output_dir)

        log.info("[atm/zarr] Staged %d BOA bands → %s", len(written), output_dir)
        return {
            "toa_path":              None,
            "atm_dir":               output_dir,
            "band_names":            [f.stem for f in written],
            "sun_zenith":            None,
            "bands_are_reflectance": True,
        }

    # ------------------------------------------------------------------
    # GeoTIFF branch — full 6S correction
    # ------------------------------------------------------------------

    def _run_geotiff(self, ctx: Context) -> dict:
        if atm_mod is None:
            raise RuntimeError("[atm] Atmospheric correction module not initialised.")
        _atm = atm_mod
        load_l8_l1          = _atm.load_l8_l1
        read_l8_mtl         = _atm.read_l8_mtl
        l8_dn_to_toa        = _atm.l8_dn_to_toa
        build_or_load_lut   = _atm.build_or_load_lut
        apply_6s_correction = _atm.apply_6s_correction
        write_boa_bands     = _atm.write_boa_bands
        LANDSAT8_BANDS      = _atm.LANDSAT8_BANDS

        geo_ls = ctx.data.get("geo_ls")
        lp     = _landsat_path(ctx.config)
        if geo_ls is not None and Path(str(geo_ls)).exists():
            input_dir = str(geo_ls)
        elif lp:
            input_dir = str(lp)
        else:
            raise RuntimeError("[atm] No input directory available.")

        scene_dir  = _scene_id(ctx.config, ctx)
        lut_path   = ctx.config.get("lut_path", str(_BASE / "lut" / "lut_6s_L8.json"))
        output_dir = ctx.working_dir / "atm_corr"
        output_dir.mkdir(parents=True, exist_ok=True)

        if lp:
            _copy_mtl_files(lp, Path(input_dir))

        dn_cube, band_names, profile = load_l8_l1(input_dir, scene_dir)
        meta = read_l8_mtl(input_dir, scene_dir)
        toa_cube, sun_zenith = l8_dn_to_toa(dn_cube, band_names, meta)

        toa_path    = output_dir / "toa_reflectance.tif"
        profile_toa = {**profile, "count": len(band_names), "dtype": "float32"}
        with rasterio.open(toa_path, "w", **profile_toa) as dst:
            dst.write(toa_cube)
            for i, name in enumerate(band_names, 1):
                dst.update_tags(i, band_id=name)

        os.makedirs(os.path.dirname(lut_path), exist_ok=True)
        lut     = build_or_load_lut(lut_path, LANDSAT8_BANDS, "L8")
        spark   = None if _running_inside_spark_executor() else _get_spark()
        boa_cube = apply_6s_correction(
            toa_cube, band_names, LANDSAT8_BANDS, lut, sun_zenith,
            toa_path=toa_path, output_dir=output_dir,
            scene_id=scene_dir, spark=spark,
        )
        atm_dir = write_boa_bands(boa_cube, band_names, profile,
                                  output_dir, scene_dir, input_dir,
                                  nodata=PIPELINE_NODATA) or output_dir
        atm_dir = Path(str(atm_dir))
        if not atm_dir.exists():
            raise RuntimeError(f"[atm] atm_dir missing: {atm_dir}")
        if lp:
            _copy_mtl_files(lp, atm_dir)
        if not list(atm_dir.glob("*_SR_B*.TIF")):
            raise RuntimeError(f"[atm] No SR TIFs in {atm_dir}")
        return {
            "toa_path": toa_path, "atm_dir": atm_dir,
            "band_names": band_names, "sun_zenith": sun_zenith,
            "bands_are_reflectance": True,
        }


def _zarr_band_to_sr_stem(band_name: str, scene_prefix: str) -> str:
    import re
    if re.match(r".*_SR_B\d+$", band_name):
        return band_name
    m = re.match(r"^SR_B0?(\d+)$", band_name, re.IGNORECASE)
    if m:
        return f"{scene_prefix}_SR_B{m.group(1)}"
    m = re.match(r"^B0?(\d+)$", band_name, re.IGNORECASE)
    if m:
        return f"{scene_prefix}_SR_B{m.group(1)}"
    return f"{scene_prefix}_SR_{band_name}"


# ===========================================================================
# Step 3 — SBAF
# ===========================================================================

class SBAFStep:
    name = "sbaf"

    def __init__(self, config):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        sbaf_dir = ctx.working_dir / "sbaf"
        if sbaf_dir.exists():
            ctx.data["sbaf_dir"] = sbaf_dir
            ctx.data["bands_are_reflectance"] = True

    def run(self, ctx: Context) -> dict:
        from SBAF.sbaf import SbafProcessor

        atm_dir = ctx.data.get("atm_dir")
        lp      = _landsat_path(ctx.config)

        if atm_dir is not None and Path(str(atm_dir)).exists():
            input_dir = Path(str(atm_dir))
        elif lp:
            input_dir = lp
        else:
            raise RuntimeError("[sbaf] No input directory available.")

        output_dir = ctx.working_dir / "sbaf"
        output_dir.mkdir(parents=True, exist_ok=True)

        sr_tifs = list(input_dir.glob("*_SR_B*.TIF"))
        if not sr_tifs:
            raise RuntimeError(f"[sbaf] No SR TIFs in {input_dir}.")

        processor = SbafProcessor(
            mission              = self.config.get("mission",      ctx.config.get("mission", "LANDSAT_8")),
            s2_target            = self.config.get("s2_target",    "Sentinel-2A"),
            adaptive             = self.config.get("adaptive",     True),
            chunks               = self.config.get("chunks",       1024),
            output_scale         = self.config.get("output_scale", 1.0),
            input_is_reflectance = ctx.data.get("bands_are_reflectance", False),
            nodata               = PIPELINE_NODATA,
        )
        outputs = processor.process_scene(
            scene_dir=input_dir, output_dir=output_dir,
            bands=self.config.get("bands"),
            in_executor=_running_inside_spark_executor(),
        )
        if not outputs:
            raise RuntimeError(f"[sbaf] No output files in {output_dir}.")

        # Copy MTL: prefer Zarr-extracted, fall back to landsat_path.
        mtl_src = ctx.data.get("zarr_mtl_path")
        if mtl_src and Path(str(mtl_src)).exists():
            shutil.copy2(mtl_src, output_dir / Path(str(mtl_src)).name)
        elif lp:
            _copy_mtl_files(lp, output_dir)

        return {"sbaf_dir": output_dir, "sbaf_outputs": outputs,
                "bands_are_reflectance": True}


# ===========================================================================
# Step 4 — Valid Pixel Mask
# ===========================================================================

class ValidPixelMaskStep:
    name = "valid_pixel_mask"

    def __init__(self, config):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        scene_dir = _scene_id(ctx.config, ctx)
        mask_path = ctx.working_dir / "mask" / f"{scene_dir}_VALID_PIXEL_MASK.TIF"
        if mask_path.exists():
            ctx.data["mask_path"] = mask_path

    def run(self, ctx: Context) -> dict:
        from Valid_Pixel_Mask.valid_pixel_mask import (
            MaskConfig, build_valid_pixel_mask, find_landsat_qa_files,
            validate_mask, write_mask, print_validation_report,
        )

        # ── Resolve QA source ─────────────────────────────────────────────
        # Priority: Zarr-extracted QA dir → landsat_path directory
        zarr_qa_dir = ctx.data.get("zarr_qa_dir")
        lp          = _landsat_path(ctx.config)

        if zarr_qa_dir and Path(str(zarr_qa_dir)).exists():
            qa_source_dir = Path(str(zarr_qa_dir))
            log.info("[mask] Using Zarr-extracted QA dir: %s", qa_source_dir)
        elif lp:
            qa_source_dir = lp
            log.info("[mask] Using Landsat QA dir: %s", qa_source_dir)
        else:
            log.warning(
                "[mask] No QA source available (zarr_qa_dir and landsat_path both absent) — "
                "skipping ValidPixelMaskStep."
            )
            return {"mask_path": None, "mask_stats": {}}

        try:
            qa_files = find_landsat_qa_files(qa_source_dir)
        except FileNotFoundError as exc:
            log.warning("[mask] QA files not found (%s) — skipping mask step.", exc)
            return {"mask_path": None, "mask_stats": {}}

        scene_id   = _scene_id(ctx.config, ctx)
        output_dir = ctx.working_dir / "mask"
        output_dir.mkdir(parents=True, exist_ok=True)
        mask_path  = output_dir / f"{scene_id}_VALID_PIXEL_MASK.TIF"

        cfg = MaskConfig(
            cloud_dilation_radius  = self.config.get("cloud_dilation_radius",  3),
            shadow_dilation_radius = self.config.get("shadow_dilation_radius", 3),
            use_cirrus             = self.config.get("use_cirrus",        True),
            use_dilated_cloud      = self.config.get("use_dilated_cloud", True),
            exclude_water          = self.config.get("exclude_water",     False),
            chunk_size             = self.config.get("chunk_size",        1024),
        )
        mask_np, profile = build_valid_pixel_mask(
            qa_pixel_path      = qa_files["qa_pixel"],
            qa_radsat_path     = qa_files["qa_radsat"],
            sr_qa_aerosol_path = qa_files["sr_qa_aerosol"],
            cfg                = cfg,
        )
        write_mask(mask_np, profile, mask_path)
        stats = validate_mask(mask_np, profile)
        print_validation_report(stats)
        for w in stats.get("warnings", []):
            log.warning("[mask] %s", w)
        log.info("[mask] Valid fraction: %.2f%%", stats["valid_fraction"] * 100)
        return {"mask_path": mask_path, "mask_stats": stats}


# ===========================================================================
# Helper — guarantee a MTL file with CORNER lat/lon in the BRDF input dir
# ===========================================================================

def _ensure_brdf_mtl(ls_input_path: Path, ctx: "Context") -> None:
    """
    Make sure ls_input_path contains a MTL JSON that has the
    CORNER_UL/LR/UR/LL_LAT/LON_PRODUCT keys that nbar.ls8_angles() needs.

    Strategy (in order):
      1. If a real *_MTL.json already exists there and has the corner keys → done.
      2. If zarr_mtl_path exists, copy it as ZARR_MTL.json and patch in corners.
      3. If landsat_path exists, copy its MTL.
      4. Derive corners from the Zarr store metadata and write a minimal MTL.
    """
    import json as _json

    scene_name = _scene_id(ctx.config, ctx)
    target_filename = f"{scene_name}_MTL.json"

    # ── 1. Check existing MTL files for corner keys ───────────────────────
    for pattern in ("*_MTL.json", "ZARR_MTL.json"):
        for existing in ls_input_path.glob(pattern):
            try:
                data = _json.loads(existing.read_text())
                # Navigate to IMAGE_ATTRIBUTES wherever it lives.
                pa = _mtl_image_attrs(data)
                if pa and "CORNER_UL_LAT_PRODUCT" in pa:
                    if existing.name != target_filename:
                        target = ls_input_path / target_filename
                        try:
                            existing.replace(target)
                            existing = target
                        except Exception as exc:
                            log.debug("[brdf] Could not rename %s → %s (%s)",
                                      existing.name, target_filename, exc)
                    log.debug("[brdf] MTL with corners found: %s", existing.name)
                    return  # already good
                # Has MTL but no corners — patch it in place.
                corners = _derive_corners_from_ctx(ctx, ls_input_path)
                if corners:
                    _inject_corner_metadata(data, corners)
                    target_path = ls_input_path / target_filename
                    if existing.name != target_filename:
                        target_path.write_text(_json.dumps(data, indent=2, default=str))
                        existing.unlink(missing_ok=True)
                    else:
                        existing.write_text(_json.dumps(data, indent=2, default=str))
                    log.info("[brdf] Patched corner keys into %s", existing.name)
                    return
            except Exception:
                continue

    # ── 2. Copy Zarr MTL and patch ────────────────────────────────────────
    zarr_mtl = ctx.data.get("zarr_mtl_path")
    if zarr_mtl and Path(str(zarr_mtl)).exists():
        dst = ls_input_path / target_filename
        shutil.copy2(zarr_mtl, dst)
        try:
            data    = _json.loads(dst.read_text())
            pa      = _mtl_image_attrs(data)
            corners = _derive_corners_from_ctx(ctx, ls_input_path)
            if corners:
                _inject_corner_metadata(data, corners)
                dst.write_text(_json.dumps(data, indent=2, default=str))
                log.info("[brdf] Zarr MTL copied + corners patched → %s", dst)
                return
        except Exception as exc:
            log.debug("[brdf] Could not patch Zarr MTL: %s", exc)
        return  # copied at least

    # ── 3. Copy from landsat_path ─────────────────────────────────────────
    lp = _landsat_path(ctx.config)
    if lp:
        copied = _copy_mtl_files(lp, ls_input_path)
        if copied:
            # Recurse once to patch the newly copied file.
            _ensure_brdf_mtl(ls_input_path, ctx)
            return

    # ── 4. Write minimal MTL from Zarr store meta ─────────────────────────
    corners  = _derive_corners_from_ctx(ctx, ls_input_path)
    store_meta = ctx.data.get("zarr_store_meta") or {}
    acq_dt   = store_meta.get("acquisition_datetime") or ""
    sun_el   = store_meta.get("sun_elevation")
    sun_az   = store_meta.get("sun_azimuth")
    sc_id    = store_meta.get("spacecraft_id") or "LANDSAT_8"
    sensor   = store_meta.get("sensor_id")     or "OLI_TIRS"
    scene_id = store_meta.get("scene_id") or store_meta.get("product_id") or "ZARR_SCENE"

    img_attrs: dict[str, Any] = {
        "SPACECRAFT_ID":     sc_id,
        "SENSOR_ID":         sensor,
        "DATE_ACQUIRED":     acq_dt[:10] if acq_dt else "",
        "SCENE_CENTER_TIME": acq_dt[11:] if len(acq_dt) > 10 else "00:00:00Z",
        "SUN_ELEVATION":     float(sun_el) if sun_el is not None else 45.0,
        "SUN_AZIMUTH":       float(sun_az) if sun_az is not None else 135.0,
    }
    if not corners:
        raise RuntimeError(
            "[brdf] Cannot derive corner lat/lon for MTL — "
            "provide landsat_path or embed 'crs'/'transform'/'shape' in the Zarr store."
        )

    img_attrs.update(corners)

    proj_attrs: dict[str, Any] = {
        "MAP_PROJECTION": store_meta.get("map_projection", "UTM"),
        "DATUM":          store_meta.get("datum",          "WGS84"),
        "UTM_ZONE":       store_meta.get("utm_zone",       0),
    }
    proj_attrs.update(corners)

    synthetic = {
        "LANDSAT_METADATA_FILE": {
            "PRODUCT_CONTENTS": {},
            "IMAGE_ATTRIBUTES": img_attrs,
            "PROJECTION_ATTRIBUTES": proj_attrs,
            "LEVEL2_PROCESSING_RECORD": {
                "PROCESSING_LEVEL": store_meta.get("processing_level", "L2SP"),
                "LANDSAT_SCENE_ID": scene_id,
            },
        }
    }
    dst = ls_input_path / target_filename
    dst.write_text(_json.dumps(synthetic, indent=2, default=str))
    log.info("[brdf] Wrote synthetic MTL with corners → %s", dst)


def _mtl_image_attrs(data: dict) -> dict | None:
    """
    Navigate a parsed MTL dict and return the IMAGE_ATTRIBUTES sub-dict,
    regardless of nesting level.  Returns None when not found.
    """
    # Standard Landsat JSON MTL structure.
    lmf = data.get("LANDSAT_METADATA_FILE") or data.get("L1_METADATA_FILE")
    if isinstance(lmf, dict):
        pa = lmf.get("IMAGE_ATTRIBUTES") or lmf.get("PRODUCT_METADATA")
        if isinstance(pa, dict):
            return pa
    # Flat dict (old TXT-converted format).
    if "CORNER_UL_LAT_PRODUCT" in data or "SUN_ELEVATION" in data:
        return data
    return None


def _inject_corner_metadata(data: dict, corners: dict[str, float]) -> None:
    """
    Update all relevant metadata sections (IMAGE_ATTRIBUTES, PROJECTION_ATTRIBUTES,
    PRODUCT_METADATA) with the provided corner latitude/longitude values.
    """
    if not corners:
        return

    lm = data.get("LANDSAT_METADATA_FILE") or data.get("L1_METADATA_FILE")
    if isinstance(lm, dict):
        for key in ("IMAGE_ATTRIBUTES", "PROJECTION_ATTRIBUTES", "PRODUCT_METADATA"):
            section = lm.get(key)
            if isinstance(section, dict):
                section.update(corners)

    if isinstance(data, dict):
        data.update({k: corners.get(k, data.get(k)) for k in corners})


def _derive_corners_from_ctx(
    ctx: "Context",
    ls_input_path: Path | None = None,
) -> dict[str, float] | None:
    """
    Derive CORNER_UL/LR/UR/LL_LAT/LON_PRODUCT from the Zarr store metadata
    stored in ctx.data.  Returns None when the required fields are absent.
    """
    try:
        from rasterio.transform import Affine, array_bounds
        from rasterio.warp import transform_bounds

        store_meta = ctx.data.get("zarr_store_meta") or {}
        imagery_meta = ctx.data.get("zarr_imagery_meta") or {}

        # Prefer imagery_meta (already at output resolution).
        tr = imagery_meta.get("transform") or store_meta.get("transform")
        crs = imagery_meta.get("crs") or store_meta.get("crs")
        h = imagery_meta.get("height") or (store_meta.get("shape") or [0, 0, 0, 0])[-2]
        w = imagery_meta.get("width") or (store_meta.get("shape") or [0, 0, 0, 0])[-1]

        if tr and crs and h and w:
            aff = Affine(*[float(v) for v in tr[:6]])
            l, b, r, t = array_bounds(int(h), int(w), aff)
            wl, sb, er, nt = transform_bounds(crs, "EPSG:4326", l, b, r, t)

            return {
                "CORNER_UL_LAT_PRODUCT": round(float(nt), 6),
                "CORNER_UL_LON_PRODUCT": round(float(wl), 6),
                "CORNER_UR_LAT_PRODUCT": round(float(nt), 6),
                "CORNER_UR_LON_PRODUCT": round(float(er), 6),
                "CORNER_LL_LAT_PRODUCT": round(float(sb), 6),
                "CORNER_LL_LON_PRODUCT": round(float(wl), 6),
                "CORNER_LR_LAT_PRODUCT": round(float(sb), 6),
                "CORNER_LR_LON_PRODUCT": round(float(er), 6),
            }

        # Fallback: derive from an existing raster file if metadata is incomplete.
        if ls_input_path is not None:
            candidate = next(
                (path for path in ls_input_path.glob("*.TIF") if path.is_file()),
                None,
            )
            if candidate:
                with rasterio.open(candidate) as src:
                    l, b, r, t = src.bounds
                    wl, sb, er, nt = transform_bounds(src.crs, "EPSG:4326", l, b, r, t)
                return {
                    "CORNER_UL_LAT_PRODUCT": round(float(nt), 6),
                    "CORNER_UL_LON_PRODUCT": round(float(wl), 6),
                    "CORNER_UR_LAT_PRODUCT": round(float(nt), 6),
                    "CORNER_UR_LON_PRODUCT": round(float(er), 6),
                    "CORNER_LL_LAT_PRODUCT": round(float(sb), 6),
                    "CORNER_LL_LON_PRODUCT": round(float(wl), 6),
                    "CORNER_LR_LAT_PRODUCT": round(float(sb), 6),
                    "CORNER_LR_LON_PRODUCT": round(float(er), 6),
                }

        return None
    except Exception as exc:
        log.debug("[brdf] Corner derivation failed: %s", exc)
        return None


# ===========================================================================
# Step 5 — BRDF Adjustment
# ===========================================================================

class BRDFAdjustmentStep:
    name = "brdf_adjustment"

    def __init__(self, config):
        self.config = config

    def restore_context(self, ctx: Context) -> None:
        nbar_dir = ctx.working_dir / "nbar" / "landsat"
        if nbar_dir.exists():
            ctx.data["nbar_ls_dir"] = str(nbar_dir)
        ctx.data["nbar_s2_dir"] = None

    def run(self, ctx: Context) -> dict:
        from BRDF_Adjustment.nbar import process_ls8, process_s2, ROY_COEFS

        workers  = self.config.get("workers",  4)
        bands    = self.config.get("bands",    list(ROY_COEFS.keys()))
        clip_min = self.config.get("clip_min", 0.8)
        clip_max = self.config.get("clip_max", 1.2)

        sbaf_dir = ctx.data.get("sbaf_dir")
        atm_dir  = ctx.data.get("atm_dir")
        lp       = _landsat_path(ctx.config)

        if sbaf_dir and Path(str(sbaf_dir)).exists():
            ls_input = str(sbaf_dir)
        elif atm_dir and Path(str(atm_dir)).exists():
            ls_input = str(atm_dir)
            log.warning("[brdf] sbaf_dir absent, falling back to atm_dir")
        elif lp:
            ls_input = str(lp)
            log.warning("[brdf] sbaf_dir and atm_dir absent, falling back to landsat_path")
        else:
            raise RuntimeError("[brdf] No input directory available.")

        ls_outdir = str(ctx.working_dir / "nbar" / "landsat")
        Path(ls_outdir).mkdir(parents=True, exist_ok=True)
        ls_input_path = Path(ls_input)

        sr_tifs = (list(ls_input_path.glob("*_SR_B*.TIF")) +
                   list(ls_input_path.glob("*_SBAF_B*.TIF")))
        if not sr_tifs:
            raise RuntimeError(f"[brdf] No SR/SBAF TIFs in {ls_input}.")

        # MTL: ensure a MTL JSON with CORNER lat/lon fields exists in ls_input_path.
        # nbar.py's ls8_angles() needs CORNER_UL_LAT_PRODUCT / CORNER_LR_LAT_PRODUCT.
        _ensure_brdf_mtl(ls_input_path, ctx)

        in_executor = _running_inside_spark_executor()

        def _run_ls8_band_local(band: str) -> str:
            process_ls8(ls_input, [band], ls_outdir,
                        clip_min=clip_min, clip_max=clip_max)
            return band

        if in_executor:
            completed_bands = _run_parallel_threads(_run_ls8_band_local, bands, workers)
        else:
            spark = _get_spark(workers=workers)
            sc    = spark.sparkContext
            _base_str = str(_BASE)
            _ls_input, _ls_outdir = ls_input, ls_outdir
            _clip_min, _clip_max  = clip_min, clip_max

            def _spark_ls8_band(band: str) -> str:
                import sys as _sys
                _sys.path.insert(0, _base_str + "/BRDF_Adjustment")
                from BRDF_Adjustment.nbar import process_ls8 as _p
                _p(_ls_input, [band], _ls_outdir,
                   clip_min=_clip_min, clip_max=_clip_max)
                return band

            completed_bands = (
                sc.parallelize(bands, numSlices=len(bands))
                .map(_spark_ls8_band).collect()
            )

        nbar_files = list(Path(ls_outdir).glob("NBAR_*.tif"))
        if not nbar_files:
            raise RuntimeError(f"[brdf] No NBAR_*.tif files in {ls_outdir}.")

        # Sentinel-2 (optional — only in GeoTIFF mode)
        s2_outdir = None
        geo_s2    = ctx.data.get("geo_s2")
        s2_input  = (str(geo_s2) if geo_s2 and Path(str(geo_s2)).exists()
                     else str(ctx.config.get("s2_path", "")))
        if s2_input and Path(s2_input).exists():
            s2_out = ctx.working_dir / "nbar" / "sentinel2"
            s2_out.mkdir(parents=True, exist_ok=True)
            s2_outdir = str(s2_out)

            _s2_input, _s2_outdir = s2_input, s2_outdir
            _base_str2 = str(_BASE)
            _c_min, _c_max = clip_min, clip_max

            def _run_s2_local(band: str) -> str:
                process_s2(_s2_input, [band], _s2_outdir,
                            clip_min=_c_min, clip_max=_c_max)
                return band

            if in_executor:
                _run_parallel_threads(_run_s2_local, bands, workers)
            else:
                def _spark_s2_band(band: str) -> str:
                    import sys as _sys
                    _sys.path.insert(0, _base_str2 + "/BRDF_Adjustment")
                    from BRDF_Adjustment.nbar import process_s2 as _p
                    _p(_s2_input, [band], _s2_outdir,
                       clip_min=_c_min, clip_max=_c_max)
                    return band
                sc.parallelize(bands, numSlices=len(bands)).map(_spark_s2_band).collect()

        return {"nbar_ls_dir": ls_outdir, "nbar_s2_dir": s2_outdir}


# ===========================================================================
# Step 6 — Data Fusion
# ===========================================================================

class DataFusionStep:
    name = "data_fusion"

    def __init__(self, config):
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
            find_landsat_file, find_sentinel_jp2, process_band,
            LANDSAT_BANDS, SENTINEL_BANDS, _maybe_extract,
        )

        workers    = self.config.get("workers", os.cpu_count() or 4)
        output_dir = str(ctx.working_dir / "fusion")
        os.makedirs(output_dir, exist_ok=True)
        jobs: list[tuple] = []

        nbar_ls_dir = ctx.data.get("nbar_ls_dir")
        lp          = _landsat_path(ctx.config)

        if nbar_ls_dir and Path(str(nbar_ls_dir)).exists():
            ls_dir = str(nbar_ls_dir)
            NBAR_TO_BAND = {
                "Blue": "B2", "Green": "B3", "Red": "B4",
                "NIR":  "B5", "SWIR1": "B6", "SWIR2": "B7",
            }
            for logical, band in NBAR_TO_BAND.items():
                fpath = os.path.join(ls_dir, f"NBAR_{logical}.tif")
                if os.path.exists(fpath):
                    jobs.append((fpath, band, output_dir, 1, "landsat"))
                else:
                    log.warning("[fusion] NBAR_%s.tif missing — skipping %s", logical, band)
            if not jobs:
                raise RuntimeError(f"[fusion] No NBAR_*.tif matched in {ls_dir}.")
        elif lp:
            log.warning("[fusion] nbar_ls_dir absent — standalone Landsat mode")
            ls_input = _maybe_extract(str(lp))
            for band in LANDSAT_BANDS:
                fpath = find_landsat_file(ls_input, band)
                if fpath is None:
                    continue
                if band == "B8":
                    shutil.copy2(fpath, os.path.join(output_dir, f"{band}_10m.TIF"))
                    continue
                jobs.append((fpath, band, output_dir, 3, "landsat"))
        else:
            raise RuntimeError(
                "[fusion] No input available (nbar_ls_dir absent and landsat_path not set)."
            )

        geo_s2   = ctx.data.get("geo_s2")
        s2_input = (str(geo_s2) if geo_s2 and Path(str(geo_s2)).exists()
                    else str(ctx.config.get("s2_path", "")))
        if s2_input and Path(s2_input).exists():
            for band, native_res in SENTINEL_BANDS.items():
                fpath = find_sentinel_jp2(s2_input, band)
                if fpath is None:
                    continue
                scale = native_res // 10
                if scale == 1:
                    shutil.copy2(fpath, os.path.join(output_dir, f"{band}_10m.TIF"))
                    continue
                jobs.append((fpath, band, output_dir, scale, "sentinel2"))

        if not jobs:
            log.warning("[fusion] No band jobs to process.")
        else:
            in_executor = _running_inside_spark_executor()

            def _process(job_tuple):
                fpath, band, outdir, scale, sensor = job_tuple
                try:
                    process_band(fpath, band, outdir, scale, sensor)
                    return (band, None)
                except Exception as exc:
                    return (band, str(exc))

            if in_executor:
                results = _run_parallel_threads(_process, jobs, workers)
            else:
                spark = _get_spark(workers=workers)
                sc    = spark.sparkContext
                _base_str = str(_BASE)

                def _spark_process(job_tuple):
                    fpath, band, outdir, scale, sensor = job_tuple
                    import sys as _sys
                    _sys.path.insert(0, _base_str + "/data_fusion")
                    from data_fusion.upsampling import process_band as _pb
                    try:
                        _pb(fpath, band, outdir, scale, sensor)
                        return (band, None)
                    except Exception as exc:
                        return (band, str(exc))

                results = (
                    sc.parallelize(jobs, numSlices=min(workers, len(jobs)))
                    .map(_spark_process).collect()
                )

            for band, err in results:
                if err:
                    log.error("[fusion] band %s failed: %s", band, err)

        fused_files = sorted(Path(output_dir).glob("*_10m.TIF"))
        if not fused_files:
            raise RuntimeError(f"[fusion] No *_10m.TIF produced in {output_dir}.")

        mask_path = ctx.data.get("mask_path")
        if mask_path and Path(str(mask_path)).exists():
            _apply_mask_to_fusion(output_dir, fused_files, mask_path)
        else:
            log.warning("[fusion] mask_path not available — cloud masking skipped")

        _write_metadata(ctx, output_dir, fused_files)
        return {"fused_dir": output_dir}


# ===========================================================================
# Helpers
# ===========================================================================

def _run_parallel_threads(fn, items, workers: int) -> list:
    effective = min(workers, len(items), 32)
    results = []
    with ThreadPoolExecutor(max_workers=effective) as pool:
        futures = {pool.submit(fn, item): item for item in items}
        for f in as_completed(futures):
            results.append(f.result())
    return results


def _apply_mask_to_fusion(output_dir, fused_files, mask_path):
    from rasterio.enums import Resampling
    from rasterio.warp import reproject
    from Valid_Pixel_Mask.valid_pixel_mask import MaskBits

    with rasterio.open(mask_path) as ms:
        mask_data  = ms.read(1)
        mask_crs   = ms.crs
        mask_trans = ms.transform

    with rasterio.open(fused_files[0]) as ref:
        dst_crs     = ref.crs
        dst_trans   = ref.transform
        dst_shape   = (ref.height, ref.width)
        dst_profile = ref.profile.copy()

    mask_reproj = np.zeros(dst_shape, dtype=np.uint8)
    reproject(source=mask_data, destination=mask_reproj,
              src_transform=mask_trans, src_crs=mask_crs,
              dst_transform=dst_trans, dst_crs=dst_crs,
              resampling=Resampling.nearest)

    binary_mask = ((mask_reproj & (1 << MaskBits.CLEAR)) != 0).astype(np.uint8)
    valid_frac  = float(binary_mask.sum()) / binary_mask.size
    log.info("[fusion] Mask valid fraction: %.2f%%", valid_frac * 100)
    if valid_frac < 0.01:
        log.warning("[fusion] ⚠ <1%% valid pixels — check mask encoding!")

    mask_out = Path(output_dir) / "FUSION_VALIDITY_MASK.TIF"
    p = dst_profile.copy()
    p.update(dtype="uint8", count=1, nodata=255)
    with rasterio.open(mask_out, "w", **p) as dst:
        dst.write(binary_mask, 1)


def _write_metadata(ctx: Context, output_dir: str, fused_files: list[Path]) -> None:
    mask_stats = ctx.data.get("mask_stats", {})
    zarr_meta: dict[str, Any] = {}
    if ctx.data.get("zarr_ingested"):
        zarr_meta = {
            "zarr_path":           ctx.config.get("zarr_path"),
            "zarr_time_index":     ctx.config.get("zarr_time_index", 0),
            "zarr_store_meta":     ctx.data.get("zarr_store_meta", {}),
            "zarr_imagery_meta":   ctx.data.get("zarr_imagery_meta", {}),
            "zarr_ancillary_meta": ctx.data.get("zarr_ancillary_meta"),
        }
    meta = {
        "product_id":      ctx.product_id,
        "processed_at":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pipeline_nodata": PIPELINE_NODATA,
        "input_source":    "zarr" if ctx.data.get("zarr_ingested") else "geotiff",
        "steps_applied": [
            s for s in [
                "zarr_ingestion" if ctx.data.get("zarr_ingested") else None,
                "geometric_processing", "atmospheric_correction",
                "sbaf", "valid_pixel_mask", "brdf_adjustment", "data_fusion",
            ] if s
        ],
        "config": {
            "zarr_path":         ctx.config.get("zarr_path", ""),
            "landsat_path":      ctx.config.get("landsat_path", ""),
            "s2_path":           ctx.config.get("s2_path",  ""),
            "resolution_policy": ctx.config.get("resolution_policy"),
            "sbaf":              ctx.config.get("sbaf", {}),
            "brdf":              ctx.config.get("brdf_adjustment", {}),
        },
        "valid_pixel_mask": {
            "valid_fraction": (mask_stats or {}).get("valid_fraction"),
            "flags":          (mask_stats or {}).get("flags", {}),
        },
        "output_bands": [f.name for f in fused_files],
    }
    if zarr_meta:
        meta["zarr_ingestion"] = zarr_meta
    meta_path = Path(output_dir) / "metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2, default=str))
    log.info("[meta] Metadata → %s", meta_path)


def _validate_gri_overlap(gri_path: Path, scene_path: Path) -> None:
    try:
        scene_tifs = list(scene_path.glob("*_B2.TIF")) or list(scene_path.glob("*_B4.TIF"))
        if not scene_tifs:
            log.warning("[geo] GRI overlap check skipped — no reference TIF found")
            return
        with rasterio.open(gri_path) as gs:
            from rasterio.warp import transform_bounds
            gb, gc = gs.bounds, gs.crs
        with rasterio.open(scene_tifs[0]) as ss:
            sb = transform_bounds(ss.crs, gc, *ss.bounds)
        overlap = (sb[0] < gb[2] and sb[2] > gb[0] and
                   sb[1] < gb[3] and sb[3] > gb[1])
        if not overlap:
            raise RuntimeError(
                f"[geo] GRI {tuple(round(x) for x in gb)} does NOT overlap "
                f"scene {tuple(round(x) for x in sb)}."
            )
        log.info("[geo] GRI overlap check passed ✓")
    except RuntimeError:
        raise
    except Exception as exc:
        log.warning("[geo] GRI overlap check failed (non-fatal): %s", exc)


def _copy_mtl_files(src_dir: Path, dst_dir: Path) -> int:
    if not src_dir or not Path(str(src_dir)).exists():
        return 0

    def _sha256(path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    copied = 0
    for pattern in ("*_MTL.json", "*_MTL.txt", "*_MTL.xml"):
        for src_file in Path(str(src_dir)).glob(pattern):
            dst_file = Path(str(dst_dir)) / src_file.name
            if dst_file.exists() and _sha256(dst_file) == _sha256(src_file):
                continue
            shutil.copy2(src_file, dst_file)
            copied += 1
    return copied


# ===========================================================================
# Step 7 — Validation
# ===========================================================================

class ValidationStep:
    name = "validation"
    _DEFAULTS = dict(
        BOA_MIN=0.0, BOA_MAX=1.0, BOA_WARN_LOW=0.0, BOA_WARN_HIGH=0.8,
        NODATA_WARN_FRAC=0.5, VALID_WARN_FRAC=0.3,
        BRDF_WARN_RATIO=0.05, SHIFT_WARN_PX=10.0,
    )

    def __init__(self, config):
        self.config = config
        for attr, default in self._DEFAULTS.items():
            setattr(self, attr, config.get(attr, default))

    def restore_context(self, ctx: Context) -> None:
        pass

    def run(self, ctx: Context) -> dict:
        issues, passed = [], []
        log.info("=" * 60)
        log.info("VALIDATION REPORT — %s", ctx.product_id)
        log.info("=" * 60)
        self._check_reflectance(ctx, issues, passed)
        self._check_cloud_mask(ctx, issues, passed)
        self._check_brdf_effect(ctx, issues, passed)
        self._check_geometry(ctx, issues, passed)
        self._check_fusion_output(ctx, issues, passed)
        n_warn = sum(1 for lvl, _ in issues if lvl == "WARN")
        n_fail = sum(1 for lvl, _ in issues if lvl == "FAIL")
        log.info("Passed:%d  Warnings:%d  Failures:%d", len(passed), n_warn, n_fail)
        for lvl, msg in issues:
            (log.warning if lvl == "WARN" else log.error)("  [%s] %s", lvl, msg)
        return {
            "validation_passed":   n_fail == 0,
            "validation_warnings": n_warn,
            "validation_failures": n_fail,
            "validation_issues":   issues,
        }

    def _check_reflectance(self, ctx, issues, passed):
        fused_dir = ctx.data.get("fused_dir")
        if not fused_dir or not Path(str(fused_dir)).exists():
            issues.append(("WARN", "reflectance check skipped — fused_dir unavailable"))
            return
        band_files = [f for f in sorted(Path(str(fused_dir)).glob("*_10m.TIF"))
                      if "VALIDITY" not in f.name.upper()]
        if not band_files:
            issues.append(("FAIL", "no *_10m.TIF found")); return

        boa_min, boa_max, nd = self.BOA_MIN, self.BOA_MAX, PIPELINE_NODATA

        def _stats(fpath_str):
            import rasterio as _r, numpy as _np
            from pathlib import Path as _P
            fname = _P(fpath_str).name
            try:
                with _r.open(fpath_str) as src:
                    data = src.read(1).astype(_np.float32); nodata = src.nodata
                nd_val = float(nodata) if nodata is not None else nd
                valid  = data != nd_val
                n_tot  = data.size; n_val = int(valid.sum())
                nodata_frac = (n_tot - n_val) / n_tot
                if n_val == 0:
                    return (fname, 0., 0., 0., 0., 0., 1., True, "100% nodata")
                vals = data[valid]
                if len(vals) > 1_000_000:
                    vals = _np.random.default_rng(42).choice(vals, 1_000_000, replace=False)
                oor = float(((vals < boa_min) | (vals > boa_max)).sum()) / len(vals)
                return (fname, float(vals.mean()), float(vals.std()),
                        float(vals.min()), float(vals.max()), oor, nodata_frac, False, "")
            except Exception as exc:
                return (fname, 0., 0., 0., 0., 0., 1., True, str(exc))

        paths = [str(f) for f in band_files]
        rows  = (_run_parallel_threads(_stats, paths, len(paths))
                 if _running_inside_spark_executor()
                 else _get_spark().sparkContext
                     .parallelize(paths, numSlices=len(paths))
                     .map(_stats).collect())

        for (fname, vmean, vstd, vmin, vmax, oor, nodata_frac, err, msg) in rows:
            if err:
                issues.append(("FAIL", f"{fname} — {msg}")); continue
            if nodata_frac > self.NODATA_WARN_FRAC:
                issues.append(("WARN", f"{fname} — {nodata_frac:.0%} nodata"))
            if oor > 0.01:
                issues.append(("WARN", f"{fname} — {oor:.2%} out of range"))
        passed.append("reflectance_range")

    def _check_cloud_mask(self, ctx, issues, passed):
        from Valid_Pixel_Mask.valid_pixel_mask import MaskBits
        mask_stats = ctx.data.get("mask_stats")
        mask_path  = ctx.data.get("mask_path")
        if mask_stats:
            vf = mask_stats.get("valid_fraction")
            if vf is not None:
                if vf < self.VALID_WARN_FRAC:
                    issues.append(("WARN", f"Only {vf:.1%} cloud-free"))
                else:
                    passed.append("cloud_mask_coverage")
            else:
                issues.append(("WARN", "mask_stats present but valid_fraction is None"))
        elif mask_path and Path(str(mask_path)).exists():
            with rasterio.open(mask_path) as src:
                md = src.read(1)
            vf = int(((md & (1 << MaskBits.CLEAR)) != 0).sum()) / md.size
            if vf < self.VALID_WARN_FRAC:
                issues.append(("WARN", f"Only {vf:.1%} cloud-free (recomputed)"))
            else:
                passed.append("cloud_mask_coverage")
        else:
            issues.append(("WARN", "cloud mask check skipped — no mask available"))

    def _check_brdf_effect(self, ctx, issues, passed):
        sbaf_dir    = ctx.data.get("sbaf_dir")
        nbar_ls_dir = ctx.data.get("nbar_ls_dir")
        if not sbaf_dir or not nbar_ls_dir:
            issues.append(("WARN", "BRDF check skipped")); return

        PAIRS = {
            "Blue": ("*_SBAF_B2.TIF", "NBAR_Blue.tif"),
            "Green":("*_SBAF_B3.TIF", "NBAR_Green.tif"),
            "Red":  ("*_SBAF_B4.TIF", "NBAR_Red.tif"),
            "NIR":  ("*_SBAF_B5.TIF", "NBAR_NIR.tif"),
            "SWIR1":("*_SBAF_B6.TIF", "NBAR_SWIR1.tif"),
            "SWIR2":("*_SBAF_B7.TIF", "NBAR_SWIR2.tif"),
        }
        triples = []
        for lg, (pat, nbar) in PAIRS.items():
            sbaf_f = list(Path(str(sbaf_dir)).glob(pat))
            nbar_f = Path(str(nbar_ls_dir)) / nbar
            if sbaf_f and nbar_f.exists():
                triples.append((lg, str(sbaf_f[0]), str(nbar_f)))
        if not triples:
            issues.append(("WARN", "BRDF check — no bands compared")); return

        nd = PIPELINE_NODATA
        def _brdf_stats(triple):
            lg, s, n = triple
            import rasterio as _r, numpy as _np
            try:
                with _r.open(s) as src: pre = src.read(1).astype(_np.float32); nodata = src.nodata
                with _r.open(n) as src: post = src.read(1).astype(_np.float32)
                nd_val = float(nodata) if nodata is not None else nd
                valid  = (pre != nd_val) & (post != nd_val) & (pre != 0)
                if not valid.any(): return (lg, None, None, None, None, "no valid pixels")
                pv, qv = pre[valid], post[valid]
                rel = _np.abs(qv - pv) / (_np.abs(pv) + 1e-6)
                return (lg, float(pv.mean()), float(qv.mean()),
                        float(qv.mean() - pv.mean()), float(rel.mean()), "")
            except Exception as exc:
                return (lg, None, None, None, None, str(exc))

        rows = (_run_parallel_threads(_brdf_stats, triples, len(triples))
                if _running_inside_spark_executor()
                else _get_spark().sparkContext
                    .parallelize(triples, numSlices=len(triples))
                    .map(_brdf_stats).collect())

        any_ok = False
        for (lg, _, _, _, rel, msg) in rows:
            if msg: continue
            if rel is not None and rel > self.BRDF_WARN_RATIO:
                issues.append(("WARN", f"BRDF {lg} changed {rel:.1%}"))
            any_ok = True
        if any_ok:
            passed.append("brdf_effect")
        else:
            issues.append(("WARN", "BRDF check — no bands compared"))

    def _check_geometry(self, ctx, issues, passed):
        geo_ls = ctx.data.get("geo_ls")
        if not geo_ls or not Path(str(geo_ls)).exists():
            issues.append(("WARN", "geometry check skipped")); return
        geo_path = Path(str(geo_ls))
        tifs = sorted(geo_path.glob("*_B*.TIF")) or sorted(geo_path.glob("*.TIF"))
        if not tifs:
            issues.append(("FAIL", f"no TIFs in {geo_path}")); return
        target_res = ctx.config.get("geometric_processing", {}).get("resolution", 10.0)
        ok = True
        for fpath in tifs[:3]:
            with rasterio.open(fpath) as src:
                pw, ph = abs(src.transform.a), abs(src.transform.e)
                crs    = src.crs
            if not (crs and crs.is_projected):
                issues.append(("WARN", f"{fpath.name} — CRS not projected")); ok = False
            if max(abs(pw - target_res), abs(ph - target_res)) / target_res > 0.01:
                issues.append(("WARN", f"{fpath.name} — pixel size {pw:.2f}×{ph:.2f}m")); ok = False
        if ok:
            passed.append("geometry")

    def _check_fusion_output(self, ctx, issues, passed):
        fused_dir = ctx.data.get("fused_dir")
        if not fused_dir or not Path(str(fused_dir)).exists():
            issues.append(("FAIL", "fused_dir unavailable")); return
        band_files = [f for f in sorted(Path(str(fused_dir)).glob("*_10m.TIF"))
                      if "VALIDITY" not in f.name.upper()]
        found = set()
        for f in band_files:
            band = f.stem.split("_")[0].upper()
            found.add(band)
            if f.stat().st_size < 1024:
                issues.append(("FAIL", f"{f.name} — suspiciously small")); continue
            try:
                with rasterio.open(f) as src:
                    src.read(1, window=rasterio.windows.Window(0, 0, 256, 256))
            except Exception as e:
                issues.append(("FAIL", f"{f.name} — unreadable: {e}"))
        missing = {"B2","B3","B4","B5","B6","B7"} - found
        if missing:
            issues.append(("WARN", f"Missing bands: {sorted(missing)}"))
        else:
            passed.append("fusion_completeness")
        if (Path(str(fused_dir)) / "FUSION_VALIDITY_MASK.TIF").exists():
            passed.append("validity_mask_sidecar")
        else:
            issues.append(("WARN", "FUSION_VALIDITY_MASK.TIF missing"))


# ===========================================================================
# run_many — Spark edition
# ===========================================================================

def run_many(
    product_ids: list[str],
    config: dict,
    working_dir: str,
    workers: int = 1,
    only: list[str] | None = None,
    resume: bool = True,
):
    spark = _get_spark(workers=workers)
    sc    = spark.sparkContext
    bc_config      = sc.broadcast(config)
    bc_working_dir = sc.broadcast(working_dir)
    bc_only        = sc.broadcast(only)
    bc_resume      = sc.broadcast(resume)
    bc_base        = sc.broadcast(str(_BASE))

    def _process_product(product_id: str):
        import os as _os, sys as _sys, importlib as _importlib
        global atm_mod
        _os.environ[_IN_SPARK_EXECUTOR_ENV] = "1"
        base = bc_base.value
        for d in [f"{base}/Geometric_Processing", f"{base}/atmospheric_correction",
                  f"{base}/SBAF", f"{base}/Valid_Pixel_Mask",
                  f"{base}/BRDF_Adjustment", f"{base}/data_fusion"]:
            if d not in _sys.path:
                _sys.path.insert(0, d)
        if atm_mod is None:
            atm_mod = _importlib.import_module(
                "atmospheric_correction.atmospheric_correction_pipeline"
            )

        pcfg = {**bc_config.value}
        # Only override landsat_path if the product_id looks like a path
        # (backward-compat for GeoTIFF mode).
        if not pcfg.get("zarr_path"):
            pcfg["landsat_path"] = product_id

        use_zarr = bool(pcfg.get("zarr_path"))
        steps    = ([ZarrIngestionStep] if use_zarr else []) + [
            GeometricProcessingStep, AtmosphericCorrectionStep,
            SBAFStep, ValidPixelMaskStep, BRDFAdjustmentStep, DataFusionStep,
        ]
        results = (
            Pipeline(pcfg, bc_working_dir.value)
            .register(*steps)
            .run(product_id, only=bc_only.value, resume=bc_resume.value)
        )
        return [{"step": r.step, "success": r.success,
                 "elapsed": r.elapsed, "error": r.error}
                for r in results]

    all_results = (
        sc.parallelize(product_ids, numSlices=len(product_ids))
        .map(_process_product).collect()
    )
    for pid, results in zip(product_ids, all_results):
        failed = [r for r in results if not r["success"]]
        if failed:
            log.error("Product %s FAILED at %s — %s",
                      pid, failed[0]["step"], failed[0]["error"])
        else:
            log.info("Product %s completed.", pid)

    try:
        from report.generate_multi_report import generate_multi_report
        generate_multi_report(
            [Path(working_dir) / Path(p).name for p in product_ids],
            Path(working_dir) / "report.html",
        )
    except Exception as exc:
        log.warning("[report] Multi-product report failed: %s", exc)


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="sen2like pipeline — Zarr or GeoTIFF input"
    )
    parser.add_argument(
        "products", nargs="*", default=[],
        help="Landsat scene path(s).  Optional when --zarr-path is set.",
    )
    parser.add_argument("--working-dir", default="./output_pipeline")
    parser.add_argument("--steps", nargs="+", choices=[
        "zarr_ingestion", "geometric_processing", "atmospheric_correction",
        "sbaf", "valid_pixel_mask", "brdf_adjustment", "data_fusion",
    ])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--s2-path", default=None)
    parser.add_argument("--zarr-path", default=None, dest="zarr_path",
                        help="URI or local path of a Nimbus Zarr store.")
    parser.add_argument("--zarr-time-index", type=int, default=0,
                        dest="zarr_time_index")
    parser.add_argument("--zarr-band-names", nargs="+", default=None,
                        dest="zarr_band_names")
    parser.add_argument("--zarr-ancillary-band-names", nargs="+", default=None,
                        dest="zarr_ancillary_band_names")
    parser.add_argument("--resolution-policy-collection", default=None,
                        dest="resolution_policy_collection")
    parser.add_argument("--target-pixel-size", type=float, default=None,
                        dest="target_pixel_size")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--base-dir", default=None)
    parser.add_argument("--exclude-water", action="store_true")
    args = parser.parse_args()

    # Validate: at least one of zarr_path or products must be provided.
    if not args.zarr_path and not args.products:
        parser.error("Provide at least one product path or --zarr-path.")

    _bootstrap(args.base_dir)

    _BUILTIN_RESOLUTION_POLICY: dict[str, dict] = {
        "landsat_ot_c2_l1": {"strategy": "collection_target_grid",
                              "reference_band": "B4",
                              "target_pixel_size_meters": 10.0},
        "landsat_ot_c2_l2": {"strategy": "collection_target_grid",
                              "reference_band": "SR_B4",
                              "target_pixel_size_meters": 10.0},
        "SENTINEL-2":        {"strategy": "collection_target_grid",
                              "reference_band": "B04",
                              "target_pixel_size_meters": 10.0},
        "SENTINEL-1":        {"strategy": "native_reference_grid",
                              "reference_band": "first_available_polarization",
                              "target_pixel_size_meters": None},
    }

    resolution_policy: dict | None = None
    if args.resolution_policy_collection:
        resolution_policy = _BUILTIN_RESOLUTION_POLICY.get(
            args.resolution_policy_collection)
    elif args.target_pixel_size is not None:
        resolution_policy = {"strategy": "custom", "reference_band": None,
                             "target_pixel_size_meters": args.target_pixel_size}

    # product_id: use first product path or Zarr store stem as identifier.
    product_id = (args.products[0] if args.products
                  else Path(args.zarr_path).stem)

    config = {
        # landsat_path is optional — set only when provided.
        "landsat_path": args.products[0] if args.products else None,
        "s2_path":      args.s2_path,
        "gri_path":     str(_BASE / "Geometric_Processing" / "gri" / "GRI_T31UDQ.tif"),
        "dem_path":     None,
        "lut_path":     str(_BASE / "lut" / "lut_6s_L8.json"),

        # Zarr config
        "zarr_path":                 args.zarr_path,
        "zarr_time_index":           args.zarr_time_index,
        "zarr_band_names":           args.zarr_band_names,
        "zarr_ancillary_band_names": args.zarr_ancillary_band_names,
        "resolution_policy":         resolution_policy,

        "geometric_processing": {
            "resolution": 10.0, "max_shift": 200.0,
            "s2_bands": ["B02","B03","B04","B08"],
            "ls_bands": ["B2","B3","B4","B5","B6","B7"],
        },
        "atmospheric_correction": {},
        "sbaf": {
            "mission": "LANDSAT_8", "s2_target": "Sentinel-2A",
            "adaptive": True, "chunks": 1024, "output_scale": 1.0,
        },
        "valid_pixel_mask": {
            "cloud_dilation_radius": 3, "shadow_dilation_radius": 3,
            "use_cirrus": True, "use_dilated_cloud": True,
            "exclude_water": args.exclude_water, "chunk_size": 1024,
        },
        "brdf_adjustment": {"workers": args.workers,
                            "clip_min": 0.8, "clip_max": 1.2},
        "data_fusion":     {"workers": args.workers},
    }

    use_zarr = bool(args.zarr_path)
    step_classes = ([ZarrIngestionStep] if use_zarr else []) + [
        GeometricProcessingStep, AtmosphericCorrectionStep,
        SBAFStep, ValidPixelMaskStep, BRDFAdjustmentStep,
        DataFusionStep, ValidationStep,
    ]
    resume = not args.no_resume

    if len(args.products) <= 1:
        Pipeline(config, args.working_dir).register(
            *step_classes
        ).run(product_id, only=args.steps, resume=resume)

        try:
            from report.generate_report import generate_report
            generate_report(Path(args.working_dir) / Path(product_id).name)
        except Exception as exc:
            log.warning("[report] HTML report failed: %s", exc)
    else:
        run_many(args.products, config, args.working_dir,
                 workers=args.workers, only=args.steps, resume=resume)
