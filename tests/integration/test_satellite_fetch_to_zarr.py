from __future__ import annotations

import logging
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest

from nimbuschain_fetch.client import NimbusFetcherClient
from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import JobCreateRequest, JobResultResponse, JobState, SearchDownloadRequest
from nimbuschain_fetch.settings import get_settings
from nimbuschain_fetch_ui.zarr_utils import recent_source_candidates
from nimbuschain_zarr_service.core import PreparedSource, prepare_source, resolve_local_path
from nimbuschain_zarr_service.service import ZarrConversionService


LOGGER = logging.getLogger(__name__)
REPO_DOWNLOADS_DIR = Path(__file__).resolve().parents[2] / "data" / "downloads"

pytestmark = [pytest.mark.integration, pytest.mark.slow]

FINAL_JOB_STATES = {
    JobState.succeeded.value,
    JobState.failed.value,
    JobState.cancelled.value,
}
SENTINEL2_L1C_BANDS = {
    "B01",
    "B02",
    "B03",
    "B04",
    "B05",
    "B06",
    "B07",
    "B08",
    "B8A",
    "B09",
    "B10",
    "B11",
    "B12",
}
SENTINEL2_L2A_BANDS = {
    "B01",
    "B02",
    "B03",
    "B04",
    "B05",
    "B06",
    "B07",
    "B08",
    "B8A",
    "B09",
    "B11",
    "B12",
}
LANDSAT_L1_BANDS = {f"B{index}" for index in range(1, 12)}
LANDSAT_L2SP_BANDS = {
    "SR_B1",
    "SR_B2",
    "SR_B3",
    "SR_B4",
    "SR_B5",
    "SR_B6",
    "SR_B7",
    "ST_B10",
}
LANDSAT_L2SR_BANDS = {
    "SR_B1",
    "SR_B2",
    "SR_B3",
    "SR_B4",
    "SR_B5",
    "SR_B6",
    "SR_B7",
}
SENTINEL1_ALLOWED_BANDS = {"VV", "VH", "HH", "HV"}


@dataclass(frozen=True)
class PipelineCase:
    key: str
    provider: str
    collection: str
    default_product_type: str
    expected_data_family: str
    expected_resolution_m: float | None

    @property
    def env_prefix(self) -> str:
        return f"NIMBUS_IT_{self.key.upper()}"

    @property
    def effective_product_type(self) -> str:
        return os.getenv(f"{self.env_prefix}_PRODUCT_TYPE", self.default_product_type).strip()


@dataclass(frozen=True)
class RawProduct:
    source_mode: str
    raw_uri: str
    scene_id: str
    job_id: str | None = None
    fetch_result: JobResultResponse | None = None


CASES = (
    PipelineCase(
        key="sentinel1",
        provider="copernicus",
        collection="SENTINEL-1",
        default_product_type="GRD",
        expected_data_family="sar",
        expected_resolution_m=None,
    ),
    PipelineCase(
        key="sentinel2_toa",
        provider="copernicus",
        collection="SENTINEL-2",
        default_product_type="S2MSI1C",
        expected_data_family="optical",
        expected_resolution_m=10.0,
    ),
    PipelineCase(
        key="sentinel2_boa",
        provider="copernicus",
        collection="SENTINEL-2",
        default_product_type="S2MSI2A",
        expected_data_family="optical",
        expected_resolution_m=10.0,
    ),
    PipelineCase(
        key="landsat_l1",
        provider="usgs",
        collection="landsat_ot_c2_l1",
        default_product_type="L1TP",
        expected_data_family="optical",
        expected_resolution_m=30.0,
    ),
    PipelineCase(
        key="landsat_l2",
        provider="usgs",
        collection="landsat_ot_c2_l2",
        default_product_type="L2SP",
        expected_data_family="optical",
        expected_resolution_m=30.0,
    ),
)


@pytest.fixture(scope="session")
def zarr_service() -> ZarrConversionService:
    return ZarrConversionService()


@pytest.fixture(scope="session")
def integration_runtime(tmp_path_factory: pytest.TempPathFactory) -> tuple[NimbusFetcherClient, Path]:
    runtime_root = tmp_path_factory.mktemp("nimbus_it_runtime")
    data_dir = runtime_root / "downloads"
    db_path = runtime_root / "nimbus_it.db"
    settings = get_settings().model_copy(
        update={
            "nimbus_runtime_role": "all",
            "nimbus_db_backend": "sqlite",
            "nimbus_db_path": db_path,
            "nimbus_data_dir": data_dir,
        }
    )
    fetcher = NimbusFetcher(settings=settings)
    with NimbusFetcherClient(mode="direct", fetcher=fetcher) as client:
        yield client, data_dir


def _env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _live_fetch_enabled() -> bool:
    return _env("NIMBUS_IT_ENABLE_LIVE_FETCH") in {"1", "true", "TRUE", "yes", "YES"}


def _case_search_request(case: PipelineCase, output_dir: str) -> JobCreateRequest | None:
    prefix = case.env_prefix
    aoi_wkt = _env(f"{prefix}_AOI_WKT")
    start_date = _env(f"{prefix}_START_DATE")
    end_date = _env(f"{prefix}_END_DATE")
    if not (aoi_wkt and start_date and end_date):
        return None
    tile_id = _env(f"{prefix}_TILE_ID")
    return SearchDownloadRequest.model_validate(
        {
            "job_type": "search_download",
            "provider": case.provider,
            "collection": case.collection,
            "product_type": case.effective_product_type,
            "start_date": start_date,
            "end_date": end_date,
            "aoi": {"wkt": aoi_wkt},
            "tile_id": tile_id,
            "output_dir": output_dir,
        }
    )


def _wait_for_job(client: NimbusFetcherClient, job_id: str) -> JobResultResponse:
    timeout_seconds = float(_env("NIMBUS_IT_JOB_TIMEOUT_SECONDS") or "3600")
    poll_seconds = float(_env("NIMBUS_IT_JOB_POLL_SECONDS") or "2.0")
    deadline = __import__("time").monotonic() + timeout_seconds

    while True:
        status = client.get_job(job_id)
        if status.state.value in FINAL_JOB_STATES:
            if status.state.value != JobState.succeeded.value:
                pytest.fail(
                    f"Fetch job {job_id} finished in state={status.state.value}, errors={status.errors}"
                )
            return client.get_result(job_id)
        if __import__("time").monotonic() >= deadline:
            pytest.fail(f"Fetch job {job_id} did not finish within {timeout_seconds:.0f}s")
        __import__("time").sleep(max(0.2, poll_seconds))


def _choose_raw_product_path(case: PipelineCase, result: JobResultResponse) -> str:
    payload_paths = [path for path in result.paths if Path(path).name != "manifest.json"]
    if not payload_paths:
        pytest.fail(f"Job {result.job_id} succeeded but no raw product paths were returned.")

    for path in payload_paths:
        if _matches_case_candidate(case, path):
            return path

    pytest.fail(
        f"Unable to identify a raw product for case={case.key} in returned paths={payload_paths}"
    )


def _matches_case_candidate(case: PipelineCase, raw_uri: str) -> bool:
    name = Path(str(raw_uri)).name.upper()
    product_type = case.effective_product_type.upper()
    if case.provider == "copernicus":
        if case.collection == "SENTINEL-1":
            return name.startswith("S1") and product_type.split("_")[0] in name
        if product_type == "S2MSI1C":
            return "MSIL1C" in name
        if product_type == "S2MSI2A":
            return "MSIL2A" in name
        return name.startswith("S2")

    if case.collection == "landsat_ot_c2_l1":
        return "_L1" in name or "L1TP" in name or "L1GT" in name or "L1GS" in name
    return "_L2" in name or "L2SP" in name or "L2SR" in name


def _discover_local_raw(case: PipelineCase) -> RawProduct | None:
    explicit = _env(f"{case.env_prefix}_RAW_URI")
    if explicit:
        explicit_path = resolve_local_path(explicit)
        if not explicit_path.exists():
            pytest.fail(
                f"Configured raw fixture does not exist for {case.key}: {explicit_path}"
            )
        scene_id = _env(f"{case.env_prefix}_SCENE_ID") or _infer_scene_id(explicit_path)
        return RawProduct(source_mode="local_fixture", raw_uri=str(explicit_path), scene_id=scene_id)

    seen: set[str] = set()
    for candidate in recent_source_candidates(limit=500):
        if _matches_case_candidate(case, candidate):
            path = resolve_local_path(candidate)
            if path.exists():
                return RawProduct(
                    source_mode="local_fixture",
                    raw_uri=str(path),
                    scene_id=_infer_scene_id(path),
                )
            seen.add(str(path))

    if REPO_DOWNLOADS_DIR.exists():
        for path in REPO_DOWNLOADS_DIR.rglob("*"):
            if not path.exists() or str(path) in seen:
                continue
            if ".zarr" in path.parts or path.name.endswith(".zarr"):
                continue
            if _matches_case_candidate(case, str(path)):
                return RawProduct(
                    source_mode="local_fixture",
                    raw_uri=str(path.resolve()),
                    scene_id=_infer_scene_id(path),
                )
    return None


def _obtain_raw_product(case: PipelineCase, client: NimbusFetcherClient) -> RawProduct:
    live_request = _case_search_request(case, output_dir=f"it/{case.key}/{uuid.uuid4().hex}")
    if _live_fetch_enabled() and live_request is not None:
        LOGGER.info("Starting live fetch for case=%s", case.key)
        job_id = client.submit_job(live_request)
        result = _wait_for_job(client, job_id)
        raw_uri = _choose_raw_product_path(case, result)
        raw_path = resolve_local_path(raw_uri)
        return RawProduct(
            source_mode="live_fetch",
            raw_uri=str(raw_path),
            scene_id=_infer_scene_id(raw_path),
            job_id=job_id,
            fetch_result=result,
        )

    local = _discover_local_raw(case)
    if local is not None:
        LOGGER.info("Using pre-fetched local raw product for case=%s: %s", case.key, local.raw_uri)
        return local

    missing = f"{case.env_prefix}_AOI_WKT, {case.env_prefix}_START_DATE, {case.env_prefix}_END_DATE"
    pytest.skip(
        "No integration input available for "
        f"{case.key}. Enable live fetch with NIMBUS_IT_ENABLE_LIVE_FETCH=1 and configure {missing}, "
        f"or provide {case.env_prefix}_RAW_URI / a matching pre-fetched product in data/downloads."
    )


@contextmanager
def _prepared(raw_uri: str, *, label: str) -> Iterator[PreparedSource]:
    prepared = prepare_source(raw_uri, label=label)
    try:
        yield prepared
    finally:
        if prepared.cleanup is not None:
            prepared.cleanup.cleanup()


def _assert_raw_structure(case: PipelineCase, raw: RawProduct) -> None:
    label = "sentinel" if case.provider == "copernicus" else "landsat"
    with _prepared(raw.raw_uri, label=label) as prepared:
        if case.provider == "copernicus":
            manifest_paths = list(prepared.root.rglob("manifest.safe"))
            assert manifest_paths, (
                f"{case.key}: expected a SAFE-style Sentinel bundle with manifest.safe, got {raw.raw_uri}"
            )
            if case.collection == "SENTINEL-2":
                raster_files = list(prepared.root.rglob("*.jp2")) + list(prepared.root.rglob("*.tif"))
                assert raster_files, f"{case.key}: Sentinel-2 bundle contains no raster files"
            else:
                measurement_files = [
                    path for path in prepared.root.rglob("*")
                    if path.is_file() and "measurement" in str(path.parent).lower()
                ]
                assert measurement_files, (
                    f"{case.key}: Sentinel-1 bundle contains no measurement files"
                )
        else:
            mtl_files = [path for path in prepared.root.rglob("*_MTL.txt")]
            tif_files = list(prepared.root.rglob("*.TIF")) + list(prepared.root.rglob("*.tif"))
            assert mtl_files, f"{case.key}: Landsat bundle contains no *_MTL.txt metadata file"
            assert tif_files, f"{case.key}: Landsat bundle contains no TIF bands"


def _infer_scene_id(path: Path) -> str:
    name = path.name
    upper_name = name.upper()
    if upper_name.endswith(".SAFE.ZIP"):
        return name[:-4]
    if upper_name.endswith(".SAFE"):
        return name
    if path.suffix:
        return path.stem
    return name


def _expected_band_subset(case: PipelineCase) -> set[str]:
    product_type = case.effective_product_type.upper()
    if case.key == "sentinel1":
        return set()
    if case.key == "sentinel2_toa":
        return set(SENTINEL2_L1C_BANDS)
    if case.key == "sentinel2_boa":
        return set(SENTINEL2_L2A_BANDS)
    if case.key == "landsat_l1":
        return set(LANDSAT_L1_BANDS)
    if product_type == "L2SR":
        return set(LANDSAT_L2SR_BANDS)
    return set(LANDSAT_L2SP_BANDS)


def _assert_conversion_outputs(
    case: PipelineCase,
    raw: RawProduct,
    written_uri: str,
    data_family: str,
    normalization_summary: dict[str, Any],
    dataset_summary: dict[str, Any],
) -> None:
    pytest.importorskip("zarr")
    import zarr

    zarr_path = Path(written_uri)
    assert zarr_path.exists() and zarr_path.is_dir(), f"{case.key}: Zarr output was not created: {zarr_path}"
    assert (zarr_path / ".zgroup").exists() or (zarr_path / "zarr.json").exists(), (
        f"{case.key}: Zarr root metadata file missing in {zarr_path}"
    )

    assert data_family == case.expected_data_family, (
        f"{case.key}: expected data_family={case.expected_data_family}, got {data_family}"
    )
    assert dataset_summary["dimensions"] == ["time", "band", "y", "x"], (
        f"{case.key}: unexpected dimensions {dataset_summary['dimensions']}"
    )
    assert dataset_summary["shape"][0] == 1, f"{case.key}: expected single-scene time dimension"

    band_names = set(dataset_summary["band_names"])
    expected_subset = _expected_band_subset(case)
    if case.key == "sentinel1":
        assert band_names, f"{case.key}: no Sentinel-1 bands were written"
        assert band_names.issubset(SENTINEL1_ALLOWED_BANDS), (
            f"{case.key}: unexpected Sentinel-1 polarizations {sorted(band_names - SENTINEL1_ALLOWED_BANDS)}"
        )
        assert len(band_names) == len(set(dataset_summary["band_names"])), (
            f"{case.key}: duplicate Sentinel-1 polarization entries were written"
        )
    else:
        missing = sorted(expected_subset - band_names)
        assert not missing, (
            f"{case.key}: converted Zarr is missing expected bands {missing}. "
            f"Got {sorted(band_names)}"
        )
        assert len(band_names) == len(expected_subset), (
            f"{case.key}: expected exactly {len(expected_subset)} imagery layers, got {len(band_names)}"
        )

    pixel_size = dataset_summary.get("pixel_size") or normalization_summary.get("grid", {}).get("pixel_size")
    if case.expected_resolution_m is not None:
        assert pixel_size == [case.expected_resolution_m, case.expected_resolution_m], (
            f"{case.key}: expected target grid {case.expected_resolution_m} m, got {pixel_size}"
        )

    group = zarr.open_group(str(zarr_path), mode="r")
    keys = set(group.array_keys())
    assert {"imagery", "band", "time"}.issubset(keys), (
        f"{case.key}: expected Zarr arrays imagery/band/time, got {sorted(keys)}"
    )
    assert tuple(group["imagery"].shape) == tuple(dataset_summary["shape"]), (
        f"{case.key}: imagery shape mismatch between summary and Zarr store"
    )
    ancillary_layer_names = list(dataset_summary.get("ancillary_layer_names") or [])
    if ancillary_layer_names:
        assert "ancillary" in keys, f"{case.key}: ancillary layers declared but ancillary array missing"
        assert "ancillary_layer" in group, f"{case.key}: ancillary coordinate missing"
        assert tuple(group["ancillary"].shape) == tuple(dataset_summary["ancillary_shape"]), (
            f"{case.key}: ancillary shape mismatch between summary and Zarr store"
        )
        assert len(ancillary_layer_names) == int(group["ancillary"].shape[1]), (
            f"{case.key}: ancillary layer count mismatch"
        )

    assert normalization_summary["raw_path"], f"{case.key}: normalization summary missing raw_path"
    assert Path(resolve_local_path(raw.raw_uri)).exists(), (
        f"{case.key}: raw product path no longer exists after conversion"
    )


@pytest.mark.parametrize("case", CASES, ids=[case.key for case in CASES])
def test_satellite_fetch_to_zarr_pipeline(
    case: PipelineCase,
    integration_runtime: tuple[NimbusFetcherClient, Path],
    zarr_service: ZarrConversionService,
    tmp_path: Path,
) -> None:
    client, _runtime_download_dir = integration_runtime
    raw = _obtain_raw_product(case, client)

    if raw.source_mode == "live_fetch":
        assert raw.job_id, f"{case.key}: live fetch path did not retain job_id"
        assert raw.fetch_result is not None, f"{case.key}: live fetch path did not retain fetch result"
        assert raw.fetch_result.paths, f"{case.key}: fetch result contains no downloaded paths"

    _assert_raw_structure(case, raw)

    output_uri = str((tmp_path / f"{case.key}-{uuid.uuid4().hex}.zarr").resolve())
    written_uri, data_family, normalization_summary, dataset_summary = zarr_service.convert(
        provider=case.provider,
        collection=case.collection,
        scene_id=raw.scene_id,
        raw_uri=raw.raw_uri,
        output_uri=output_uri,
        product_type=case.effective_product_type,
    )

    _assert_conversion_outputs(
        case=case,
        raw=raw,
        written_uri=written_uri,
        data_family=data_family,
        normalization_summary=normalization_summary,
        dataset_summary=dataset_summary,
    )
