
from __future__ import annotations
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
import numpy as np

log = logging.getLogger("sen2like.zarr_ingestor")

# ---------------------------------------------------------------------------
# Default QA layer name candidates (tried in order)
# ---------------------------------------------------------------------------
_QA_PIXEL_CANDIDATES   = ["QA_PIXEL",    "qa_pixel",    "PIXEL_QA"]
_QA_RADSAT_CANDIDATES  = ["QA_RADSAT",   "qa_radsat",   "RADSAT_QA"]
_QA_AEROSOL_CANDIDATES = ["SR_QA_AEROSOL","sr_qa_aerosol","AEROSOL_QA"]

# Default MTL attribute keys tried in the Zarr root attrs
_MTL_ATTR_CANDIDATES = ["mtl", "MTL", "landsat_mtl", "scene_metadata"]


# ---------------------------------------------------------------------------
# Public errors
# ---------------------------------------------------------------------------

class ZarrIngestionError(RuntimeError):
    """Raised when the Zarr store cannot be ingested."""


# ---------------------------------------------------------------------------
# Main ingestor
# ---------------------------------------------------------------------------

class ZarrIngestor:

    def __init__(self, config: dict[str, Any]) -> None:
        self._config   = config
        self._tmp_dirs: list[TemporaryDirectory[str]] = []

    def __enter__(self) -> "ZarrIngestionResult":
        self._result = self.ingest()
        return self._result

    def __exit__(self, *_: Any) -> None:
        self.cleanup()

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def ingest(self) -> "ZarrIngestionResult":
        zarr_path = self._config.get("zarr_path")
        if not zarr_path:
            raise ZarrIngestionError("config['zarr_path'] is not set.")

        time_index          = int(self._config.get("zarr_time_index", 0))
        requested_bands     = self._config.get("zarr_band_names")
        requested_anc_bands = self._config.get("zarr_ancillary_band_names")
        resolution_policy   = self._config.get("resolution_policy") or {}
        target_px           = resolution_policy.get("target_pixel_size_meters")
        ref_band            = resolution_policy.get("reference_band")

        log.info("[zarr] Opening store: %s", zarr_path)
        root, store_meta = _open_zarr_root(zarr_path)


        for _key in ("_zarr_imagery_out_dir", "_zarr_ancillary_out_dir", "_zarr_qa_out_dir"):
            _val = self._config.get(_key)
            if _val is not None:
                store_meta[_key] = str(_val)

        # ── Imagery ───────────────────────────────────────────────────────
        imagery_dir, imagery_meta = _materialise_array(
            root=root, store_meta=store_meta,
            array_name="imagery", coord_name="band",
            time_index=time_index, requested_bands=requested_bands,
            target_px=target_px, ref_band=ref_band,
            tmp_dirs=self._tmp_dirs, label="imagery",
        )
        log.info("[zarr] Imagery → %s (%d bands)", imagery_dir, len(imagery_meta["band_names"]))

        # ── Ancillary layers (SCL, CLDPRB …) ─────────────────────────────
        ancillary_dir:  Path | None           = None
        ancillary_meta: dict[str, Any] | None = None
        if "ancillary" in root:
            try:
                ancillary_dir, ancillary_meta = _materialise_array(
                    root=root, store_meta=store_meta,
                    array_name="ancillary", coord_name="ancillary_layer",
                    time_index=time_index, requested_bands=requested_anc_bands,
                    target_px=target_px, ref_band=ref_band,
                    tmp_dirs=self._tmp_dirs, label="ancillary",
                )
                log.info("[zarr] Ancillary → %s (%d layers)",
                         ancillary_dir, len(ancillary_meta["band_names"]))
            except Exception as exc:
                log.warning("[zarr] Ancillary extraction failed (non-fatal): %s", exc)

        # ── QA bands ──────────────────────────────────────────────────────
        qa_dir = self._extract_qa(root, store_meta, time_index, imagery_meta)

        # ── MTL metadata ──────────────────────────────────────────────────
        mtl_path = self._extract_mtl(root, store_meta)

        return ZarrIngestionResult(
            imagery_dir=imagery_dir,
            imagery_meta=imagery_meta,
            ancillary_dir=ancillary_dir,
            ancillary_meta=ancillary_meta,
            store_meta=store_meta,
            qa_dir=qa_dir,
            mtl_path=mtl_path,
        )

    # ------------------------------------------------------------------
    # QA extraction
    # ------------------------------------------------------------------

    def _extract_qa(
        self,
        root: Any,
        store_meta: dict[str, Any],
        time_index: int,
        imagery_meta: dict[str, Any],
    ) -> Path | None:
        qa_band_map: dict[str, Any] = dict(self._config.get("zarr_qa_band_map") or {})

        qa_targets: dict[str, list[str]] = {
            "qa_pixel":      [qa_band_map.get("qa_pixel")]    if qa_band_map.get("qa_pixel")    else _QA_PIXEL_CANDIDATES,
            "qa_radsat":     [qa_band_map.get("qa_radsat")]   if qa_band_map.get("qa_radsat")   else _QA_RADSAT_CANDIDATES,
            "sr_qa_aerosol": [qa_band_map.get("sr_qa_aerosol")] if qa_band_map.get("sr_qa_aerosol") else _QA_AEROSOL_CANDIDATES,
        }

        # found maps output_key → (zarr_array, band_index)
        found: dict[str, tuple[Any, int]] = {}

        def _search_array(arr: Any, layer_names: list[str]) -> None:
            name_to_idx = {n: i for i, n in enumerate(layer_names)}
            for out_key, candidates in qa_targets.items():
                if out_key in found:
                    continue
                for cand in candidates:
                    idx = name_to_idx.get(cand)
                    if idx is not None:
                        found[out_key] = (arr, idx)
                        break

        # -- 1. Dedicated qa array ----------------------------------------
        if "qa" in root:
            qa_coord = root["qa_layer"] if "qa_layer" in root else None
            if qa_coord is not None:
                qa_names = [str(v) for v in qa_coord[:]]
            else:
                qa_names = list(
                    store_meta.get("qa_layer_names") or
                    store_meta.get("qa_band_names") or []
                )
            _search_array(root["qa"], qa_names)

        # -- 2. QA layers inside ancillary --------------------------------
        if "ancillary" in root and len(found) < len(qa_targets):
            anc_coord = root["ancillary_layer"] if "ancillary_layer" in root else None
            if anc_coord is not None:
                anc_names = [str(v) for v in anc_coord[:]]
            else:
                anc_names = list(
                    store_meta.get("ancillary_layer_names") or
                    store_meta.get("ancillary_band_names") or []
                )
            _search_array(root["ancillary"], anc_names)

        if not found:
            log.warning(
                "[zarr] No QA bands found in store — "
                "ValidPixelMaskStep will be skipped or degraded."
            )
            return None

        # ── Write GeoTIFFs ────────────────────────────────────────────────
        try:
            import rasterio
            from rasterio.transform import Affine, array_bounds, from_origin
            from rasterio.warp import reproject
            from rasterio.enums import Resampling
        except ImportError as exc:
            raise ZarrIngestionError("rasterio is required for QA materialisation.") from exc

        crs, transform, _ = _crs_and_transform_from_meta(root, store_meta)
        affine = Affine(*transform) if transform else Affine.identity()
        ref_h  = imagery_meta["height"]
        ref_w  = imagery_meta["width"]

        _pd_qa = self._config.get("_zarr_qa_out_dir")
        if _pd_qa is not None:
            out_dir = Path(str(_pd_qa))
            out_dir.mkdir(parents=True, exist_ok=True)
        else:
            tmp = TemporaryDirectory(prefix="nimbus_qa_tif_")
            self._tmp_dirs.append(tmp)
            out_dir = Path(tmp.name)

        scene_prefix = (
    store_meta.get("scene_id")
    or store_meta.get("product_id")
    or Path(str(self._config.get("zarr_path", "zarr_scene"))).stem
)

        fname_map = {
            "qa_pixel":      f"{scene_prefix}_QA_PIXEL.TIF",
            "qa_radsat":     f"{scene_prefix}_QA_RADSAT.TIF",
            "sr_qa_aerosol": f"{scene_prefix}_SR_QA_AEROSOL.TIF",
        }

        for out_key, (arr, band_idx) in found.items():
            shape = arr.shape
            if len(shape) == 4:
                data = np.asarray(arr[time_index, band_idx, :, :])
            elif len(shape) == 3:
                data = np.asarray(arr[band_idx, :, :])
            else:
                log.warning("[zarr] QA array unexpected shape %s — skipping %s", shape, out_key)
                continue

            # Resample to imagery grid if shapes differ.
            if data.shape != (ref_h, ref_w):
                src_h, src_w = data.shape
                left, bottom, right, top = array_bounds(src_h, src_w, affine)
                dst_affine = from_origin(left, top,
                                         (right - left) / ref_w,
                                         (top   - bottom) / ref_h)
                resampled = np.zeros((ref_h, ref_w), dtype=data.dtype)
                reproject(
                    source=data, destination=resampled,
                    src_transform=affine, src_crs=crs or "EPSG:4326",
                    dst_transform=dst_affine, dst_crs=crs or "EPSG:4326",
                    resampling=Resampling.nearest,
                )
                data = resampled

            out_path = out_dir / fname_map[out_key]
            with rasterio.open(out_path, "w", driver="GTiff",
                               dtype=str(data.dtype), width=ref_w, height=ref_h,
                               count=1, crs=crs, transform=affine,
                               compress="deflate", tiled=True,
                               blockxsize=256, blockysize=256) as dst:
                dst.write(data, 1)
            log.debug("[zarr] QA written: %s", out_path.name)

        log.info("[zarr] QA dir → %s  (%d files)", out_dir,
                 len(list(out_dir.glob("*.TIF"))))
        return out_dir

    # ------------------------------------------------------------------
    # MTL extraction
    # ------------------------------------------------------------------

    def _extract_mtl(self, root: Any, store_meta: dict[str, Any]) -> Path | None:

        import json as _json

        # -- 1. Full MTL already embedded ---------------------------------
        for key in _MTL_ATTR_CANDIDATES:
            raw = store_meta.get(key)
            if isinstance(raw, dict) and raw:
                tmp = TemporaryDirectory(prefix="nimbus_mtl_")
                self._tmp_dirs.append(tmp)
                mtl_path = Path(tmp.name) / "ZARR_MTL.json"
                mtl_path.write_text(_json.dumps(raw, indent=2, default=str))
                log.info("[zarr] MTL from attr '%s' → %s", key, mtl_path)
                return mtl_path

        # -- 2. Assemble minimal MTL from individual attributes -----------
        acq_dt = store_meta.get("acquisition_datetime") or ""
        sun_el = store_meta.get("sun_elevation")
        sun_az = store_meta.get("sun_azimuth")
        sc_id  = (store_meta.get("spacecraft_id") or
                  store_meta.get("platform")      or "LANDSAT_8")
        sensor = (store_meta.get("sensor_id")     or
                  store_meta.get("instrument")    or "OLI_TIRS")
        scene_id = (store_meta.get("scene_id")    or
                    store_meta.get("product_id")  or "ZARR_SCENE")

        if not any([sun_el, sun_az, acq_dt]):
            log.warning(
                "[zarr] No MTL data found in store — "
                "steps requiring MTL will be skipped or degraded."
            )
            return None

        corner_attrs: dict[str, Any] = {}
        try:
            from rasterio.transform import Affine, array_bounds
            from rasterio.warp import transform_bounds as _tb
            _tr  = store_meta.get("transform")
            _crs = store_meta.get("crs")
            _shp = (store_meta.get("shape") or
                    store_meta.get("ancillary_shape") or [1, 1, 0, 0])
            _h   = int(_shp[-2]) if len(_shp) >= 2 else 0
            _w   = int(_shp[-1]) if len(_shp) >= 1 else 0
            if _tr and len(_tr) >= 6 and _crs and _h > 0 and _w > 0:
                _aff = Affine(*[float(v) for v in _tr[:6]])
                _l, _b, _r, _t = array_bounds(_h, _w, _aff)
                _wl, _sb, _er, _nt = _tb(_crs, "EPSG:4326", _l, _b, _r, _t)
                corner_attrs = {
                    "CORNER_UL_LAT_PRODUCT": round(_nt, 6),
                    "CORNER_UL_LON_PRODUCT": round(_wl, 6),
                    "CORNER_UR_LAT_PRODUCT": round(_nt, 6),
                    "CORNER_UR_LON_PRODUCT": round(_er, 6),
                    "CORNER_LL_LAT_PRODUCT": round(_sb, 6),
                    "CORNER_LL_LON_PRODUCT": round(_wl, 6),
                    "CORNER_LR_LAT_PRODUCT": round(_sb, 6),
                    "CORNER_LR_LON_PRODUCT": round(_er, 6),
                }
                log.debug("[zarr] MTL corners derived: UL=(%.4f,%.4f) LR=(%.4f,%.4f)",
                          _nt, _wl, _sb, _er)
        except Exception as _ce:
            log.debug("[zarr] Could not derive MTL corners: %s", _ce)

        synthetic_mtl = {
            "LANDSAT_METADATA_FILE": {
                "PRODUCT_CONTENTS": {},
                "IMAGE_ATTRIBUTES": {
                    "SPACECRAFT_ID":     sc_id,
                    "SENSOR_ID":         sensor,
                    "DATE_ACQUIRED":     acq_dt[:10] if acq_dt else "",
                    "SCENE_CENTER_TIME": acq_dt[11:] if len(acq_dt) > 10 else "00:00:00Z",
                    "SUN_ELEVATION":     float(sun_el) if sun_el is not None else 45.0,
                    "SUN_AZIMUTH":       float(sun_az) if sun_az is not None else 135.0,
                    **corner_attrs,
                },
                "PROJECTION_ATTRIBUTES": {
                    "MAP_PROJECTION": store_meta.get("map_projection", "UTM"),
                    "DATUM":          store_meta.get("datum",          "WGS84"),
                    "UTM_ZONE":       store_meta.get("utm_zone",       0),
                },
                "LEVEL2_PROCESSING_RECORD": {
                    "PROCESSING_LEVEL": store_meta.get("processing_level", "L2SP"),
                    "LANDSAT_SCENE_ID": scene_id,
                },
            }
        }

        # Embed per-band rescaling coefficients if stored.
        band_meta = store_meta.get("band_metadata") or {}
        level1: dict[str, Any] = {}
        for band_name, bm in band_meta.items():
            mult = bm.get("reflectance_mult") or bm.get("REFLECTANCE_MULT")
            add  = bm.get("reflectance_add")  or bm.get("REFLECTANCE_ADD")
            if mult is not None:
                level1[f"REFLECTANCE_MULT_{band_name}"] = float(mult)
            if add is not None:
                level1[f"REFLECTANCE_ADD_{band_name}"]  = float(add)
        if level1:
            synthetic_mtl["LANDSAT_METADATA_FILE"][
                "LEVEL1_RADIOMETRIC_RESCALING"] = level1

        tmp = TemporaryDirectory(prefix="nimbus_mtl_")
        self._tmp_dirs.append(tmp)
        mtl_path = Path(tmp.name) / "ZARR_MTL.json"
        mtl_path.write_text(_json.dumps(synthetic_mtl, indent=2, default=str))
        log.info("[zarr] Synthetic MTL → %s", mtl_path)
        return mtl_path

    def cleanup(self) -> None:
        while self._tmp_dirs:
            td = self._tmp_dirs.pop()
            try:
                td.cleanup()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------

class ZarrIngestionResult:
    __slots__ = (
        "imagery_dir", "imagery_meta",
        "ancillary_dir", "ancillary_meta",
        "store_meta", "qa_dir", "mtl_path",
    )

    def __init__(
        self,
        imagery_dir:    Path,
        imagery_meta:   dict[str, Any],
        ancillary_dir:  Path | None,
        ancillary_meta: dict[str, Any] | None,
        store_meta:     dict[str, Any],
        qa_dir:         Path | None,
        mtl_path:       Path | None,
    ) -> None:
        self.imagery_dir    = imagery_dir
        self.imagery_meta   = imagery_meta
        self.ancillary_dir  = ancillary_dir
        self.ancillary_meta = ancillary_meta
        self.store_meta     = store_meta
        self.qa_dir         = qa_dir
        self.mtl_path       = mtl_path

    @property
    def band_names(self) -> list[str]:
        return list(self.imagery_meta.get("band_names") or [])

    @property
    def ancillary_band_names(self) -> list[str]:
        return list((self.ancillary_meta or {}).get("band_names") or [])

    @property
    def crs(self) -> str | None:
        return self.imagery_meta.get("crs")

    @property
    def pixel_size(self) -> list[float] | None:
        return self.imagery_meta.get("pixel_size")

    @property
    def acquisition_datetime(self) -> str | None:
        return self.store_meta.get("acquisition_datetime")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _open_zarr_root(zarr_path: str) -> tuple[Any, dict[str, Any]]:
    try:
        import zarr
    except ImportError as exc:
        raise ZarrIngestionError("zarr is not installed.  Run: pip install zarr") from exc

    try:
        from nimbuschain_zarr_service.oci_storage import is_oci_uri, OCIStore
        if is_oci_uri(zarr_path):
            store_obj, parsed = OCIStore.from_uri(zarr_path)
            mapper = store_obj.get_mapper(parsed.path, create=False)
            root   = zarr.open_consolidated(mapper, mode="r")
        else:
            local = Path(zarr_path).expanduser().resolve()
            if not local.exists():
                raise ZarrIngestionError(f"Zarr store not found: {local}")
            root = zarr.open_consolidated(str(local), mode="r")
    except Exception as exc:
        try:
            import zarr as _z
            root = _z.open_group(zarr_path, mode="r")
            log.debug("[zarr] Opened without consolidated metadata (%s)", exc)
        except Exception as exc2:
            raise ZarrIngestionError(f"Cannot open Zarr store '{zarr_path}': {exc2}") from exc2

    return root, dict(root.attrs)


def _band_names_from_root(
    root: Any, coord_name: str, requested: list[str] | None,
) -> list[str]:
    if requested:
        return list(requested)
    if coord_name in root:
        try:
            return [str(v) for v in root[coord_name][:]]
        except Exception:
            pass
    attr_key = "band_names" if coord_name == "band" else f"{coord_name}_names"
    names = root.attrs.get(attr_key)
    if names:
        return list(names)
    raise ZarrIngestionError(
        f"Cannot determine band names for coord '{coord_name}'. "
        f"Set config['zarr_band_names'] or add a '{coord_name}' coord array."
    )


def _crs_and_transform_from_meta(
    root: Any, store_meta: dict[str, Any],
) -> tuple[str | None, list[float] | None, list[float] | None]:
    crs        = store_meta.get("crs")
    transform  = store_meta.get("transform")
    pixel_size = store_meta.get("reference_pixel_size") or store_meta.get("pixel_size")

    if (crs is None or transform is None) and "imagery" in root:
        try:
            img = dict(root["imagery"].attrs)
            crs        = crs       or img.get("crs")
            transform  = transform or img.get("transform")
            pixel_size = pixel_size or img.get("pixel_size")
        except Exception:
            pass

    transform  = [float(v) for v in transform[:6]]  if transform  and len(transform)  >= 6 else None
    pixel_size = [float(v) for v in pixel_size[:2]] if pixel_size and len(pixel_size) >= 2 else None
    return crs, transform, pixel_size


def _materialise_array(
    *, root: Any, store_meta: dict[str, Any],
    array_name: str, coord_name: str,
    time_index: int, requested_bands: list[str] | None,
    target_px: float | None, ref_band: str | None,
    tmp_dirs: list[TemporaryDirectory[str]], label: str,
) -> tuple[Path, dict[str, Any]]:
    try:
        import rasterio
        from rasterio.transform import Affine, array_bounds, from_origin
        from rasterio.warp import reproject
        from rasterio.enums import Resampling
    except ImportError as exc:
        raise ZarrIngestionError("rasterio required for materialisation.") from exc

    if array_name not in root:
        raise ZarrIngestionError(
            f"Array '{array_name}' not found.  Available: {sorted(root.keys())}"
        )

    arr   = root[array_name]
    shape = arr.shape
    if len(shape) != 4:
        raise ZarrIngestionError(
            f"Expected 4-D (time,{coord_name},y,x) for '{array_name}', got {shape}."
        )

    n_time, n_b, n_y, n_x = shape
    if time_index >= n_time:
        raise ZarrIngestionError(
            f"zarr_time_index={time_index} out of range (store has {n_time} steps)."
        )

    band_names   = _band_names_from_root(root, coord_name, requested_bands)
    store_names  = _band_names_from_root(root, coord_name, None)
    store_index  = {n: i for i, n in enumerate(store_names)}
    valid_pairs  = [(n, store_index[n]) for n in band_names
                    if store_index.get(n) is not None and store_index[n] < n_b]

    if not valid_pairs:
        raise ZarrIngestionError(f"No requested bands found in '{array_name}'.")

    crs, transform, pixel_size = _crs_and_transform_from_meta(root, store_meta)
    affine = Affine(*transform) if transform else Affine.identity()
    out_affine, out_h, out_w = affine, n_y, n_x
    need_resample = False

    if target_px is not None and pixel_size is not None:
        native = (abs(pixel_size[0]) + abs(pixel_size[1])) / 2.0
        if abs(native - target_px) / max(native, 1e-9) > 0.01:
            need_resample = True
            l, b, r, t = array_bounds(n_y, n_x, affine)
            out_w      = max(1, int(np.ceil((r - l) / target_px)))
            out_h      = max(1, int(np.ceil((t - b) / target_px)))
            out_affine = from_origin(l, t, target_px, target_px)
            log.info("[zarr] Resample %s %.2f→%.2fm (%dx%d→%dx%d)",
                     label, native, target_px, n_x, n_y, out_w, out_h)

    dtype  = np.dtype(arr.dtype)
    nodata = float("nan") if np.issubdtype(dtype, np.floating) else 0

    # Determine read chunk size from Zarr chunk spec (fall back to 1024 rows).
    try:
        zarr_chunks = arr.chunks  # tuple (t_chunk, b_chunk, y_chunk, x_chunk)
        row_chunk   = int(zarr_chunks[2]) if zarr_chunks and len(zarr_chunks) >= 3 else 1024
    except Exception:
        row_chunk = 1024
    row_chunk = max(256, min(row_chunk, 2048))  # clamp 256–2048

    # Use persistent output dir if injected by ZarrIngestionStep (avoids copy).
    _pd_key = f"_zarr_{label}_out_dir"
    _pd_val = store_meta.get(_pd_key)
    if _pd_val is not None:
        out_dir = Path(str(_pd_val))
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        tmp = TemporaryDirectory(prefix=f"nimbus_{label}_tif_")
        tmp_dirs.append(tmp)
        out_dir = Path(tmp.name)
    written: list[str] = []

    for band_name, band_idx in valid_pairs:
        log.info("[zarr] Writing %s/%s  (%dx%d px)…", label, band_name, out_w, out_h)
        out_path = out_dir / f"{band_name}.TIF"

        if not need_resample:
            # ── Chunked write: never load more than row_chunk rows at once ──
            with rasterio.open(out_path, "w", driver="GTiff",
                               dtype=str(dtype), width=out_w, height=out_h,
                               count=1, crs=crs, transform=out_affine, nodata=nodata,
                               compress="deflate", predictor=2,
                               tiled=True, blockxsize=256, blockysize=256) as dst:
                for y0 in range(0, out_h, row_chunk):
                    y1   = min(y0 + row_chunk, out_h)
                    chunk = np.asarray(
                        arr[time_index, band_idx, y0:y1, :]
                    ).astype(dtype)
                    window = rasterio.windows.Window(0, y0, out_w, y1 - y0)
                    dst.write(chunk, 1, window=window)
                dst.update_tags(1, band_id=band_name)
        else:
            # Resampling requires the full source array — load it once.
            alg  = (Resampling.nearest
                    if _is_categorical(band_name, store_meta)
                    else Resampling.bilinear)
            data = np.asarray(arr[time_index, band_idx, :, :]).astype(dtype)
            dst_arr = np.full((out_h, out_w), nodata, dtype=dtype)
            reproject(source=data, destination=dst_arr,
                      src_transform=affine, src_crs=crs or "EPSG:4326",
                      dst_transform=out_affine, dst_crs=crs or "EPSG:4326",
                      src_nodata=nodata, dst_nodata=nodata, resampling=alg)
            with rasterio.open(out_path, "w", driver="GTiff",
                               dtype=str(dtype), width=out_w, height=out_h,
                               count=1, crs=crs, transform=out_affine, nodata=nodata,
                               compress="deflate", predictor=2,
                               tiled=True, blockxsize=256, blockysize=256) as dst:
                dst.write(dst_arr, 1)
                dst.update_tags(1, band_id=band_name)
            del data, dst_arr

        written.append(band_name)

    log.info("[zarr] Materialised %d/%d → %s", len(written), len(valid_pairs), out_dir)
    return out_dir, {
        "band_names": written, "crs": crs,
        "transform":  [out_affine.a, out_affine.b, out_affine.c,
                       out_affine.d, out_affine.e, out_affine.f],
        "pixel_size": [abs(out_affine.a), abs(out_affine.e)],
        "height": out_h, "width": out_w, "dtype": str(dtype),
        "array_name": array_name, "coord_name": coord_name,
        "time_index": time_index,
    }


def _is_categorical(band_name: str, store_meta: dict[str, Any]) -> bool:
    if band_name in (store_meta.get("categorical_bands") or []):
        return True
    bm = (store_meta.get("band_metadata") or {}).get(band_name) or {}
    return bool(bm.get("categorical"))


# ---------------------------------------------------------------------------
# Context population
# ---------------------------------------------------------------------------

def populate_context_from_zarr(ctx: Any, result: ZarrIngestionResult) -> None:

    ctx.data["geo_ls"]              = result.imagery_dir
    ctx.data["geo_s2"]              = None
    ctx.data["zarr_imagery_meta"]   = result.imagery_meta
    ctx.data["zarr_ancillary_dir"]  = result.ancillary_dir
    ctx.data["zarr_ancillary_meta"] = result.ancillary_meta
    ctx.data["zarr_store_meta"]     = result.store_meta
    ctx.data["zarr_qa_dir"]         = result.qa_dir
    ctx.data["zarr_mtl_path"]       = result.mtl_path
    ctx.data["zarr_ingested"]       = True
    log.info("[zarr] Context → imagery=%s  qa=%s  mtl=%s",
             result.imagery_dir, result.qa_dir, result.mtl_path)