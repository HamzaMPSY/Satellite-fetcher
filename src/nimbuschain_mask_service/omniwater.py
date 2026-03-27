from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
from typing import Any
import importlib

from nimbuschain_mask_service.writers import write_water_mask_tiles_to_zarr
from nimbuschain_zarr_service.core import ConversionDependencyError, ConversionError, _open_existing_output_store


_S2_INPUT_BANDS = ["B04", "B03", "B02", "B08"]
_LANDSAT_L1_INPUT_BANDS = ["B4", "B3", "B2", "B5"]
_LANDSAT_L2_INPUT_BANDS = ["SR_B4", "SR_B3", "SR_B2", "SR_B5"]


@dataclass(frozen=True)
class OmniWaterPlan:
    required: bool
    supported: bool
    input_bands: list[str]
    reason: str | None = None


@dataclass(frozen=True)
class OmniWaterTile:
    path: Path
    row_start: int
    row_stop: int
    col_start: int
    col_stop: int


def omniwater_support_status() -> dict[str, Any]:
    spec = importlib.util.find_spec("omniwatermask")
    return {
        "available": spec is not None,
        "module": "omniwatermask",
    }


def apply_omniwatermask_to_zarr(
    *,
    job_id: str | None = None,
    zarr_uri: str,
    provider: str,
    collection: str,
    product_type: str | None,
    scene_id: str,
    acquisition_datetime: str | None,
    dataset_summary: dict[str, Any],
    fail_on_error: bool = False,
    stage_callback: Any = None,
) -> dict[str, Any]:
    scene_dir = _watermask_scene_dir(job_id=job_id, scene_id=scene_id)
    status_path = scene_dir / "water_mask_status.json"
    masked_zarr_uri = _masked_zarr_output_uri(
        source_zarr_uri=zarr_uri,
        scene_dir=scene_dir,
        scene_id=scene_id,
    )
    plan = _build_plan(
        provider=provider,
        collection=collection,
        product_type=product_type,
        dataset_summary=dataset_summary,
    )
    if not plan.supported:
        status = "failed" if plan.required else "skipped"
        result = {
            "status": status,
            "reason": plan.reason or "unsupported",
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": masked_zarr_uri,
            "storage_mode": "derived_zarr_copy",
            "input_bands": list(plan.input_bands),
            "mask_path": None,
            "artifact_uri": None,
            "status_path": str(status_path),
            "work_dir": str(scene_dir),
        }
        _persist_status_artifacts(scene_dir=scene_dir, status_path=status_path, payload=result)
        if fail_on_error and plan.required:
            raise ConversionError(
                "OmniWaterMask is required for this product, but the mask plan is not supported "
                f"({result['reason']})."
            )
        return result

    module_version: str | None = None
    make_water_mask: Any | None = None
    try:
        make_water_mask, _make_water_mask_debug, module_version = _load_make_water_mask()
    except ConversionDependencyError as exc:
        make_water_mask = None

    try:
        _prepare_scene_dir(scene_dir)
        running_payload = {
            "status": "running",
            "reason": None,
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": masked_zarr_uri,
            "storage_mode": "derived_zarr_copy",
            "input_bands": list(plan.input_bands),
            "mask_path": None,
            "artifact_uri": None,
            "status_path": str(status_path),
            "work_dir": str(scene_dir),
        }
        _persist_status_artifacts(scene_dir=scene_dir, status_path=status_path, payload=running_payload)
        prepared_output_zarr_uri = _prepare_masked_zarr_copy(source_zarr_uri=zarr_uri, output_zarr_uri=masked_zarr_uri)
        tiles_dir = scene_dir / "tiles"
        cache_dir = scene_dir / "cache"
        output_dir = scene_dir / "outputs"
        output_dir.mkdir(parents=True, exist_ok=True)
        cache_dir.mkdir(parents=True, exist_ok=True)
        tile_manifest = _export_rgbnir_tiles(
            zarr_uri=zarr_uri,
            tiles_dir=tiles_dir,
            dataset_summary=dataset_summary,
            input_bands=plan.input_bands,
        )
        tile_paths = [tile.path for tile in tile_manifest["tiles"]]
        if stage_callback is not None:
            stage_callback(
                "water_masking_started",
                {
                    "zarr_uri": zarr_uri,
                    "output_zarr_uri": prepared_output_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                    "work_dir": str(scene_dir),
                    "status_path": str(status_path),
                    "tile_count": len(tile_paths),
                },
            )
        mask_output, runtime_mode = _run_omniwater(
            make_water_mask=make_water_mask,
            scene_paths=tile_paths,
            output_dir=output_dir,
            cache_dir=cache_dir,
            scene_dir=scene_dir,
            tile_size=int(tile_manifest["tile_size"]),
        )
        mask_paths = _normalize_mask_outputs(
            mask_output,
            output_dir=output_dir,
            expected_count=len(tile_paths),
        )
        artifact_path = _write_mask_artifact(
            scene_dir=scene_dir,
            tiles=tile_manifest["tiles"],
            mask_paths=mask_paths,
            height=int(tile_manifest["height"]),
            width=int(tile_manifest["width"]),
            crs=tile_manifest["crs"],
            transform=tile_manifest["transform"],
        )
        result = write_water_mask_tiles_to_zarr(
            output_uri=prepared_output_zarr_uri,
            tiles=[
                {
                    "row_start": tile.row_start,
                    "row_stop": tile.row_stop,
                    "col_start": tile.col_start,
                    "col_stop": tile.col_stop,
                }
                for tile in tile_manifest["tiles"]
            ],
            mask_paths=[str(path) for path in mask_paths],
            height=int(tile_manifest["height"]),
            width=int(tile_manifest["width"]),
            acquisition_datetime=acquisition_datetime,
            model_name="omniwatermask" if runtime_mode == "model" else "omniwatermask_ndwi_fallback",
            model_version=module_version,
            input_bands=list(plan.input_bands),
            metadata={
                "provider": provider,
                "collection": collection,
                "product_type": product_type,
                "scene_id": scene_id,
                "source_mask_raster": str(artifact_path),
                "artifact_uri": str(artifact_path),
                "status_path": str(status_path),
                "work_dir": str(scene_dir),
                "tile_count": len(tile_paths),
                "runtime_mode": runtime_mode,
            },
        )
        payload = {
            "status": "written",
            "reason": None,
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": prepared_output_zarr_uri,
            "storage_mode": "derived_zarr_copy",
            "input_bands": list(plan.input_bands),
            "mask_path": result["mask_path"],
            "artifact_uri": str(artifact_path),
            "status_path": str(status_path),
            "work_dir": str(scene_dir),
            "shape": result["shape"],
            "dtype": result["dtype"],
            "classes": result["classes"],
            "model_name": result["model_name"],
            "model_version": result["model_version"],
            "written_at": result["written_at"],
            "runtime_mode": runtime_mode,
        }
        _persist_status_artifacts(scene_dir=scene_dir, status_path=status_path, payload=payload)
        _sync_zarr_mask_attrs(zarr_uri=prepared_output_zarr_uri, payload=payload)
        if stage_callback is not None:
            stage_callback(
                "water_masking_finished",
                {
                    "zarr_uri": zarr_uri,
                    "output_zarr_uri": prepared_output_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                    "water_mask": payload,
                },
            )
        return payload
    except Exception as exc:
        payload = {
            "status": "failed",
            "reason": str(exc),
            "input_zarr_uri": zarr_uri,
            "output_zarr_uri": masked_zarr_uri,
            "storage_mode": "derived_zarr_copy",
            "input_bands": list(plan.input_bands),
            "mask_path": None,
            "artifact_uri": None,
            "status_path": str(status_path),
            "work_dir": str(scene_dir),
        }
        _persist_status_artifacts(scene_dir=scene_dir, status_path=status_path, payload=payload)
        _cleanup_masked_zarr_copy(masked_zarr_uri)
        if stage_callback is not None:
            stage_callback(
                "water_masking_failed",
                {
                    "zarr_uri": zarr_uri,
                    "output_zarr_uri": masked_zarr_uri,
                    "scene_id": scene_id,
                    "provider": provider,
                    "collection": collection,
                    "product_type": product_type,
                    "water_mask": payload,
                },
            )
        if fail_on_error and plan.required:
            raise ConversionError(f"OmniWaterMask failed for scene '{scene_id}' ({exc}).") from exc
        return payload


def maybe_write_omniwater_mask(
    *,
    job_id: str | None = None,
    output_uri: str,
    provider: str,
    collection: str,
    product_type: str | None,
    scene_id: str,
    acquisition_datetime: str | None,
    dataset_summary: dict[str, Any],
    fail_on_error: bool = False,
) -> dict[str, Any]:
    return apply_omniwatermask_to_zarr(
        job_id=job_id,
        zarr_uri=output_uri,
        provider=provider,
        collection=collection,
        product_type=product_type,
        scene_id=scene_id,
        acquisition_datetime=acquisition_datetime,
        dataset_summary=dataset_summary,
        fail_on_error=fail_on_error,
    )


def _build_plan(
    *,
    provider: str,
    collection: str,
    product_type: str | None,
    dataset_summary: dict[str, Any],
) -> OmniWaterPlan:
    band_names = [str(value) for value in list(dataset_summary.get("band_names") or [])]
    normalized_collection = str(collection or "").strip().upper()
    normalized_product_type = str(product_type or "").strip().upper()

    if provider == "copernicus" and normalized_collection == "SENTINEL-2":
        if all(name in band_names for name in _S2_INPUT_BANDS):
            return OmniWaterPlan(required=True, supported=True, input_bands=list(_S2_INPUT_BANDS))
        return OmniWaterPlan(
            required=True,
            supported=False,
            input_bands=list(_S2_INPUT_BANDS),
            reason="required_sentinel2_rgbnir_bands_missing",
        )

    if provider == "usgs" and normalized_collection.startswith("LANDSAT"):
        if normalized_product_type.startswith("L2") or any(name.startswith("SR_B") for name in band_names):
            required = _LANDSAT_L2_INPUT_BANDS
            reason = "required_landsat_l2_rgbnir_bands_missing"
        else:
            required = _LANDSAT_L1_INPUT_BANDS
            reason = "required_landsat_l1_rgbnir_bands_missing"
        if all(name in band_names for name in required):
            return OmniWaterPlan(required=True, supported=True, input_bands=list(required))
        return OmniWaterPlan(required=True, supported=False, input_bands=list(required), reason=reason)

    return OmniWaterPlan(
        required=False,
        supported=False,
        input_bands=[],
        reason="unsupported_collection_for_omniwatermask",
    )


def is_omniwater_required(*, provider: str, collection: str) -> bool:
    normalized_provider = str(provider or "").strip().lower()
    normalized_collection = str(collection or "").strip().upper()
    if normalized_provider == "copernicus" and normalized_collection == "SENTINEL-2":
        return True
    if normalized_provider == "usgs" and normalized_collection.startswith("LANDSAT"):
        return True
    return False


def _load_make_water_mask() -> tuple[Any, Any | None, str | None]:
    try:
        module = importlib.import_module("omniwatermask")
    except ImportError as exc:
        raise ConversionDependencyError(
            "OmniWaterMask import failed "
            f"({exc}). Ensure omniwatermask is installed and OpenCV system libraries are present."
        ) from exc
    make_water_mask = getattr(module, "make_water_mask", None)
    if make_water_mask is None:
        raise ConversionDependencyError(
            "Installed omniwatermask package does not expose make_water_mask()."
        )
    return make_water_mask, getattr(module, "make_water_mask_debug", None), getattr(module, "__version__", None)


def _export_rgbnir_tiles(
    *,
    zarr_uri: str,
    tiles_dir: Path,
    dataset_summary: dict[str, Any],
    input_bands: list[str],
) -> dict[str, Any]:
    try:
        import numpy as np
        import rasterio
        from rasterio.transform import Affine
        from rasterio.windows import Window, transform as window_transform
        import zarr
    except ImportError as exc:
        raise ConversionDependencyError(
            "OmniWaterMask export dependencies are unavailable "
            f"({exc}). Ensure rasterio and zarr are installed."
        ) from exc

    output_store = _open_existing_output_store(zarr_uri)
    root = zarr.open_group(output_store, mode="r")
    imagery = root.get("imagery")
    if imagery is None:
        raise ConversionError("The target Zarr store does not contain an imagery array.")

    band_coord = root.get("band")
    if band_coord is None:
        raise ConversionError("The target Zarr store does not contain a band coordinate.")

    band_names = [_normalize_coord_value(value) for value in band_coord[:].tolist()]
    band_index = {name: index for index, name in enumerate(band_names)}
    missing = [name for name in input_bands if name not in band_index]
    if missing:
        raise ConversionError(
            f"Cannot export OmniWaterMask input because the Zarr store is missing bands: {missing}."
        )

    transform_values = list(dataset_summary.get("transform") or [])
    crs = dataset_summary.get("crs")
    if len(transform_values) < 6 or not crs:
        raise ConversionError("The Zarr summary is missing transform/crs for OmniWaterMask export.")

    height = int(dataset_summary["shape"][2])
    width = int(dataset_summary["shape"][3])
    dtype = str(dataset_summary.get("dtype") or imagery.dtype)
    transform = Affine(*transform_values[:6])
    tile_size = _watermask_tile_size()
    tiles: list[OmniWaterTile] = []
    tiles_dir.mkdir(parents=True, exist_ok=True)
    tile_index = 0
    for row_start in range(0, height, tile_size):
        row_stop = min(height, row_start + tile_size)
        for col_start in range(0, width, tile_size):
            col_stop = min(width, col_start + tile_size)
            tile_index += 1
            tile_path = tiles_dir / f"rgbnir_tile_{tile_index:04d}.tif"
            tile_window = Window(
                col_off=col_start,
                row_off=row_start,
                width=col_stop - col_start,
                height=row_stop - row_start,
            )
            with rasterio.open(
                tile_path,
                "w",
                driver="GTiff",
                height=row_stop - row_start,
                width=col_stop - col_start,
                count=4,
                dtype=np.dtype(dtype),
                crs=crs,
                transform=window_transform(tile_window, transform),
            ) as dataset:
                for output_band_index, band_name in enumerate(input_bands, start=1):
                    dataset.write(
                        imagery[0, band_index[band_name], row_start:row_stop, col_start:col_stop],
                        output_band_index,
                    )
            tiles.append(
                OmniWaterTile(
                    path=tile_path,
                    row_start=row_start,
                    row_stop=row_stop,
                    col_start=col_start,
                    col_stop=col_stop,
                )
            )
    return {
        "tiles": tiles,
        "tile_size": tile_size,
        "height": height,
        "width": width,
        "transform": transform,
        "crs": crs,
    }


def _run_omniwater(
    *,
    make_water_mask: Any | None,
    scene_paths: list[Path],
    output_dir: Path,
    cache_dir: Path,
    scene_dir: Path,
    tile_size: int,
) -> tuple[Any, str]:
    # If omniwatermask or its legacy model runtime is unavailable, fall back to a
    # deterministic NDWI mask instead of aborting the manual existing-Zarr flow.
    if _watermask_runtime_mode() != "model":
        return _run_internal_ndwi(scene_paths=scene_paths, output_dir=output_dir), "ndwi_fallback"
    kwargs: dict[str, Any] = {
        "scene_paths": scene_paths,
        "band_order": [1, 2, 3, 4],
        "output_dir": output_dir,
        "overwrite": True,
        "cache_dir": cache_dir,
        "batch_size": 1,
        "mosaic_device": "cpu",
        "inference_device": "cpu",
        "inference_patch_size": min(_watermask_inference_patch_size(), tile_size),
        "inference_overlap_size": min(_watermask_inference_overlap_size(), max(0, tile_size - 1)),
        "destination_model_dir": _watermask_model_dir(scene_dir),
        "model_download_source": "hugging_face",
    }
    signature = getattr(make_water_mask, "__signature__", None)
    if signature is None:
        try:
            import inspect

            signature = inspect.signature(make_water_mask)
        except Exception:
            signature = None
    accepted = set(signature.parameters.keys()) if signature is not None else set()
    optional_kwargs = {
        "use_cache": False,
        "use_osm_water": False,
        "use_osm_building": False,
        "use_osm_roads": False,
        "optimise_model": False,
        "use_model": True,
        "use_ndwi": True,
    }
    for key, value in optional_kwargs.items():
        if not accepted or key in accepted:
            kwargs[key] = value
    if make_water_mask is None:
        return _run_internal_ndwi(scene_paths=scene_paths, output_dir=output_dir), "ndwi_fallback"
    try:
        return make_water_mask(**kwargs), "model"
    except Exception as exc:
        if not _is_legacy_model_dependency_error(exc):
            raise
        return _run_internal_ndwi(scene_paths=scene_paths, output_dir=output_dir), "ndwi_fallback"


def _run_internal_ndwi(*, scene_paths: list[Path], output_dir: Path) -> list[Path]:
    try:
        import numpy as np
        import rasterio
    except ImportError as exc:
        raise ConversionDependencyError(
            f"Internal NDWI fallback requires numpy and rasterio ({exc})."
        ) from exc

    outputs: list[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for scene_path in scene_paths:
        mask_path = output_dir / f"{scene_path.stem}_water_mask.tif"
        with rasterio.open(scene_path) as src:
            green = src.read(2).astype(np.float32)
            nir = src.read(4).astype(np.float32)
            denom = green + nir
            ndwi = np.divide(
                green - nir,
                denom,
                out=np.zeros_like(green, dtype=np.float32),
                where=np.abs(denom) > 1e-6,
            )
            mask = (ndwi > 0.0).astype(np.uint8)
            profile = src.profile.copy()
            profile.update(count=1, dtype="uint8")
            with rasterio.open(mask_path, "w", **profile) as dst:
                dst.write(mask, 1)
        outputs.append(mask_path)
    return outputs


def _normalize_mask_outputs(result: Any, *, output_dir: Path, expected_count: int) -> list[Path]:
    outputs: list[Path] = []
    if isinstance(result, (str, Path)):
        candidate = Path(result)
        if candidate.exists():
            outputs.append(candidate)
    elif isinstance(result, list):
        for item in result:
            if isinstance(item, (str, Path)):
                candidate = Path(item)
                if candidate.exists():
                    outputs.append(candidate)
    if len(outputs) != expected_count:
        tif_candidates = sorted(output_dir.rglob("*.tif")) + sorted(output_dir.rglob("*.tiff"))
        outputs = [candidate for candidate in tif_candidates if candidate.exists()]
    if len(outputs) != expected_count:
        raise ConversionError(
            f"OmniWaterMask produced {len(outputs)} readable output raster(s), expected {expected_count}."
        )
    return outputs


def _read_mask(mask_path: Path) -> Any:
    try:
        import rasterio
    except ImportError as exc:
        raise ConversionDependencyError(
            "OmniWaterMask mask-read dependencies are unavailable "
            f"({exc}). Ensure rasterio is installed."
        ) from exc

    with rasterio.open(mask_path) as src:
        return src.read(1)


def _stitch_masks(
    *,
    tiles: list[OmniWaterTile],
    mask_paths: list[Path],
    height: int,
    width: int,
) -> Any:
    try:
        import numpy as np
    except ImportError as exc:
        raise ConversionDependencyError(f"numpy is required to stitch tile masks ({exc}).") from exc

    stitched = np.zeros((height, width), dtype=np.uint8)
    for tile, mask_path in zip(tiles, mask_paths, strict=True):
        tile_mask = _read_mask(mask_path)
        expected_shape = (tile.row_stop - tile.row_start, tile.col_stop - tile.col_start)
        if tuple(tile_mask.shape) != expected_shape:
            raise ConversionError(
                f"Tile mask shape mismatch for '{mask_path.name}': expected {expected_shape}, got {tuple(tile_mask.shape)}."
            )
        stitched[tile.row_start:tile.row_stop, tile.col_start:tile.col_stop] = tile_mask
    return stitched


def _normalize_coord_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _watermask_work_root() -> str | None:
    configured_root = str(os.getenv("NIMBUS_WATERMASK_DIR") or "").strip()
    data_root = str(os.getenv("NIMBUS_DATA_DIR") or "").strip()
    candidates: list[str] = []
    if configured_root:
        candidates.append(configured_root)
    if data_root:
        candidates.append(str(Path(data_root) / "watermask"))
    candidates.extend(
        [
            "/data/downloads/watermask",
            str(Path.cwd() / "data" / "downloads" / "watermask"),
        ]
    )
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        return str(path)
    return None


def _masked_zarr_root() -> str | None:
    configured_root = str(os.getenv("NIMBUS_ZARRMASK_DIR") or "").strip()
    data_root = str(os.getenv("NIMBUS_DATA_DIR") or "").strip()
    candidates: list[str] = []
    if configured_root:
        candidates.append(configured_root)
    if data_root:
        candidates.append(str(Path(data_root) / "zarrmask"))
    candidates.extend(
        [
            "/data/downloads/zarrmask",
            str(Path.cwd() / "data" / "downloads" / "zarrmask"),
            str(Path.cwd() / "download" / "zarrmask"),
        ]
    )
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        return str(path)
    return None


def _watermask_scene_dir(*, job_id: str | None, scene_id: str) -> Path:
    root = Path(_watermask_work_root() or "/tmp")
    safe_job_id = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(job_id or "").strip()).strip("._")
    safe_job_id = safe_job_id or "standalone"
    safe_scene_id = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in scene_id).strip("._")
    safe_scene_id = safe_scene_id or "unknown_scene"
    path = root / safe_job_id / safe_scene_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _masked_zarr_output_uri(*, source_zarr_uri: str, scene_dir: Path, scene_id: str) -> str:
    source_path = Path(str(source_zarr_uri).strip())
    safe_scene_id = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in scene_id).strip("._")
    safe_scene_id = safe_scene_id or "unknown_scene"
    if source_path.suffix == ".zarr":
        source_parent = source_path.parent
        if source_parent.name.lower() == "zarr":
            root = source_parent.parent / "zarrmask"
        else:
            root = source_parent / "zarrmask"
        root.mkdir(parents=True, exist_ok=True)
        return str(root / f"{source_path.stem}__watermask.zarr")
    root = Path(_masked_zarr_root() or scene_dir)
    return str(root / f"{safe_scene_id}__watermask.zarr")


def _prepare_masked_zarr_copy(*, source_zarr_uri: str, output_zarr_uri: str) -> str:
    source_path = Path(str(source_zarr_uri).strip())
    output_path = Path(str(output_zarr_uri).strip())
    if not source_path.exists():
        raise ConversionError(f"Source Zarr store not found: {source_zarr_uri}")
    if not source_path.is_dir():
        raise ConversionError(f"Source Zarr store must be a directory: {source_zarr_uri}")
    if source_path.resolve() == output_path.resolve():
        raise ConversionError("Masked Zarr output must differ from the source Zarr store.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        shutil.rmtree(output_path)
    shutil.copytree(source_path, output_path)
    return str(output_path)


def _cleanup_masked_zarr_copy(output_zarr_uri: str) -> None:
    target = Path(str(output_zarr_uri).strip())
    try:
        if target.exists():
            shutil.rmtree(target)
    except OSError:
        return


def _prepare_scene_dir(scene_dir: Path) -> None:
    for candidate in (scene_dir / "cache", scene_dir / "outputs", scene_dir / "tiles"):
        if candidate.exists():
            shutil.rmtree(candidate, ignore_errors=True)
    for candidate in (
        scene_dir / "water_mask.tif",
        scene_dir / "water_mask_status.json",
    ):
        if candidate.exists():
            candidate.unlink()

def _write_mask_artifact(
    *,
    scene_dir: Path,
    tiles: list[OmniWaterTile],
    mask_paths: list[Path],
    height: int,
    width: int,
    crs: Any,
    transform: Any,
) -> Path:
    try:
        import numpy as np
        import rasterio
        from rasterio.windows import Window
    except ImportError as exc:
        raise ConversionDependencyError(
            f"rasterio is required to persist the final water-mask artifact ({exc})."
        ) from exc
    artifact_path = scene_dir / "water_mask.tif"
    with rasterio.open(
        artifact_path,
        "w",
        driver="GTiff",
        height=int(height),
        width=int(width),
        count=1,
        dtype="uint8",
        crs=crs,
        transform=transform,
    ) as dst:
        for tile, mask_path in zip(tiles, mask_paths, strict=True):
            with rasterio.open(mask_path) as src:
                tile_mask = src.read(1)
            expected_shape = (tile.row_stop - tile.row_start, tile.col_stop - tile.col_start)
            if tuple(tile_mask.shape) != expected_shape:
                raise ConversionError(
                    f"Tile mask shape mismatch for '{mask_path.name}': expected {expected_shape}, got {tuple(tile_mask.shape)}."
                )
            dst.write(
                np.asarray(tile_mask, dtype=np.uint8),
                1,
                window=Window(
                    col_off=tile.col_start,
                    row_off=tile.row_start,
                    width=tile.col_stop - tile.col_start,
                    height=tile.row_stop - tile.row_start,
                ),
            )
    return artifact_path


def _is_legacy_model_dependency_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "fastai is not installed" in message
        or "legacy model support" in message
        or "versions 1-3" in message
        or "must enable use_model" in message
    )


def _watermask_tile_size() -> int:
    raw = str(os.getenv("NIMBUS_WATERMASK_TILE_SIZE") or "").strip()
    try:
        value = int(raw) if raw else 2048
    except ValueError:
        value = 2048
    return max(256, value)


def _watermask_runtime_mode() -> str:
    raw = str(os.getenv("NIMBUS_WATERMASK_RUNTIME_MODE") or "").strip().lower()
    return "model" if raw == "model" else "ndwi"


def _watermask_model_dir(scene_dir: Path) -> Path:
    configured = str(os.getenv("NIMBUS_WATERMASK_MODEL_DIR") or "").strip()
    target = Path(configured) if configured else scene_dir.parent / "_models"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _watermask_inference_patch_size() -> int:
    raw = str(os.getenv("NIMBUS_WATERMASK_INFERENCE_PATCH_SIZE") or "").strip()
    try:
        value = int(raw) if raw else 512
    except ValueError:
        value = 512
    return max(128, value)


def _watermask_inference_overlap_size() -> int:
    raw = str(os.getenv("NIMBUS_WATERMASK_INFERENCE_OVERLAP_SIZE") or "").strip()
    try:
        value = int(raw) if raw else 128
    except ValueError:
        value = 128
    return max(0, value)


def _persist_status_artifacts(*, scene_dir: Path, status_path: Path, payload: dict[str, Any]) -> None:
    scene_dir.mkdir(parents=True, exist_ok=True)
    serializable_payload = dict(payload)
    serializable_payload.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
    status_path.write_text(json.dumps(serializable_payload, indent=2, sort_keys=True))


def _sync_zarr_mask_attrs(*, zarr_uri: str, payload: dict[str, Any]) -> None:
    try:
        import zarr
    except ImportError:
        return

    try:
        output_store = _open_existing_output_store(zarr_uri)
    except ConversionError:
        return
    root = zarr.open_group(output_store, mode="a", zarr_format=2)
    root.attrs["water_mask_status"] = str(payload.get("status") or "")
    root.attrs["water_mask_reason"] = str(payload.get("reason") or "")
    root.attrs["water_mask_status_path"] = str(payload.get("status_path") or "")
    root.attrs["water_mask_artifact_uri"] = str(payload.get("artifact_uri") or "")
    root.attrs["water_mask_work_dir"] = str(payload.get("work_dir") or "")
    if str(payload.get("status") or "").strip().lower() != "written":
        root.attrs["water_mask_written"] = False
    zarr.consolidate_metadata(output_store)
