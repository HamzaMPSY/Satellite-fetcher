import json
import re
import datetime as dt
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

from nimbuschain_fetch_ui.constants import DOWNLOADS_DIR, ZARR_STORES_DIR
from nimbuschain_fetch_ui.jobs_helpers import _api_request


def _is_remote_uri(path_value: str | Path) -> bool:
    parsed = urlparse(str(path_value or "").strip())
    return bool(parsed.scheme and parsed.scheme.lower() not in {"", "file"})


def _container_to_host_path(path_value: str) -> Path:
    value = str(path_value or "").strip()
    if value.startswith("/data/"):
        return Path(".") / value.lstrip("/")
    if value == "/data":
        return Path("./data")
    return Path(value)


def _host_to_container_path(path: Path) -> str:
    try:
        resolved = path.resolve()
        downloads_root = DOWNLOADS_DIR.resolve()
        if resolved == downloads_root or downloads_root in resolved.parents:
            suffix = resolved.relative_to(downloads_root)
            return str(Path("/data/downloads") / suffix)
    except OSError:
        pass
    return str(path)


def _candidate_runtime_path(path_value: str | Path) -> tuple[str, Optional[Path]]:
    raw_value = str(path_value or "").strip()
    if not raw_value:
        return "", None
    if _is_remote_uri(raw_value):
        return raw_value, None
    if raw_value.startswith("/data/") or raw_value == "/data":
        host_hint = _container_to_host_path(raw_value)
        return raw_value, host_hint
    host_path = Path(raw_value)
    return _host_to_container_path(host_path), host_path


def _path_exists_in_runtime(path_value: str | Path) -> bool:
    if _is_remote_uri(path_value):
        return True
    runtime_value, host_path = _candidate_runtime_path(path_value)
    if runtime_value.startswith("/data/"):
        host_hint = _container_to_host_path(runtime_value)
        if host_hint.exists():
            return True
    if host_path is not None and host_path.exists():
        return True
    return False


def _looks_like_scene_name(name: str) -> bool:
    upper_name = str(name or "").upper()
    if not upper_name:
        return False
    if upper_name.endswith(".SAFE") or upper_name.endswith(".SAFE.ZIP"):
        return True
    if upper_name.startswith(("S1", "S2")) and "_" in upper_name:
        return True
    if upper_name.startswith(("LC08_", "LC09_", "LE07_", "LT05_", "LT04_", "LM05_")):
        return True
    return False


def _manifest_source_candidates(limit: int = 200) -> List[str]:
    if not DOWNLOADS_DIR.exists():
        return []

    candidates: Dict[str, float] = {}
    for manifest_path in DOWNLOADS_DIR.rglob("manifest.json"):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        for raw_path_value in list(payload.get("paths", [])):
            try:
                runtime_value, host_path = _candidate_runtime_path(str(raw_path_value))
                if not runtime_value:
                    continue
                if _is_remote_uri(runtime_value):
                    candidates[runtime_value] = manifest_path.stat().st_mtime
                    continue
                stat_path = host_path or _container_to_host_path(runtime_value)
                if not stat_path.exists():
                    continue
                candidates[runtime_value] = stat_path.stat().st_mtime
            except OSError:
                continue

        parent_dir = manifest_path.parent
        try:
            for child in parent_dir.iterdir():
                if _looks_like_scene_name(child.name):
                    candidates[_host_to_container_path(child)] = child.stat().st_mtime
        except OSError:
            continue

    return [
        item[0]
        for item in sorted(candidates.items(), key=lambda pair: pair[1], reverse=True)[: max(1, limit)]
    ]


def recent_source_candidates(limit: int = 200) -> List[str]:
    if not DOWNLOADS_DIR.exists():
        return []

    def _is_supported_raw_file(path: Path) -> bool:
        lower_name = path.name.lower()
        if lower_name.endswith(".safe.zip"):
            return True
        if path.suffix.lower() in {".zip", ".tar", ".gz", ".tgz", ".nc", ".tif", ".tiff"}:
            return True
        if _looks_like_scene_name(path.name):
            try:
                return tarfile.is_tarfile(path)
            except (OSError, tarfile.TarError):
                return False
        return False

    def _is_supported_raw_dir(path: Path) -> bool:
        lower_name = path.name.lower()
        if lower_name.endswith(".safe") or lower_name.endswith(".zarr"):
            return lower_name.endswith(".safe")
        if _looks_like_scene_name(path.name):
            return True
        try:
            children = list(path.iterdir())
        except OSError:
            return False
        if any(child.is_file() and child.name.lower() == "manifest.safe" for child in children):
            return True
        if any(child.is_file() and child.name.upper().endswith("_MTL.TXT") for child in children):
            return True
        if any(child.is_file() and child.suffix.lower() == ".nc" for child in children):
            return True
        tif_count = sum(
            1 for child in children if child.is_file() and child.suffix.lower() in {".tif", ".tiff", ".jp2"}
        )
        return tif_count >= 3

    candidates: Dict[str, float] = {}
    for path in DOWNLOADS_DIR.rglob("*"):
        try:
            if path.is_dir():
                if _is_supported_raw_dir(path):
                    candidates[_host_to_container_path(path)] = path.stat().st_mtime
            elif path.is_file() and _is_supported_raw_file(path):
                candidates[_host_to_container_path(path)] = path.stat().st_mtime
        except OSError:
            continue

    for manifest_candidate in _manifest_source_candidates(limit=max(limit, 200)):
        try:
            runtime_value, host_path = _candidate_runtime_path(manifest_candidate)
            if not runtime_value:
                continue
            stat_path = host_path or _container_to_host_path(runtime_value)
            if stat_path.exists():
                candidates[runtime_value] = stat_path.stat().st_mtime
        except OSError:
            continue

    return [
        item[0]
        for item in sorted(candidates.items(), key=lambda pair: pair[1], reverse=True)[: max(1, limit)]
    ]


def container_to_host_path_hint(path_value: str) -> str:
    if not path_value:
        return ""
    if _is_remote_uri(path_value):
        return ""
    if path_value.startswith("/data/") or path_value == "/data":
        return str(_container_to_host_path(path_value))
    return ""


def available_zarr_stores(limit: int = 120) -> List[Dict[str, Any]]:
    if not ZARR_STORES_DIR.exists():
        return []
    stores: List[Dict[str, Any]] = []
    for path in ZARR_STORES_DIR.rglob("*.zarr"):
        if not path.is_dir():
            continue
        try:
            stat = path.stat()
            stores.append(
                {
                    "path": str(path),
                    "name": path.name,
                    "mtime": stat.st_mtime,
                    "host_hint": container_to_host_path_hint(str(path)),
                    "entries": sorted(
                        [child.name for child in path.iterdir() if child.is_dir() or child.is_file()]
                    )[:12],
                }
            )
        except OSError:
            continue
    stores.sort(key=lambda item: item["mtime"], reverse=True)
    return stores[: max(1, limit)]


def list_artifacts(
    api_url: str,
    api_key: str,
    *,
    artifact_type: Optional[str] = None,
    provider: Optional[str] = None,
    collection: Optional[str] = None,
    scene_id: Optional[str] = None,
    job_id: Optional[str] = None,
    uri_query: Optional[str] = None,
    include_local: bool = False,
    page: int = 1,
    page_size: int = 100,
) -> Tuple[List[Dict[str, Any]], int]:
    params: Dict[str, Any] = {
        "page": page,
        "page_size": page_size,
        "include_local": str(include_local).lower(),
    }
    if artifact_type:
        params["artifact_type"] = artifact_type
    if provider:
        params["provider"] = provider
    if collection:
        params["collection"] = collection
    if scene_id:
        params["scene_id"] = scene_id
    if job_id:
        params["job_id"] = job_id
    if uri_query:
        params["uri_query"] = uri_query
    try:
        response = _api_request("GET", api_url, "/v1/artifacts", api_key=api_key, params=params, timeout=30)
        if not response.ok:
            return [], 0
        body = response.json()
        return list(body.get("items", [])), int(body.get("total", 0) or 0)
    except Exception:
        return [], 0


def _path_size_bytes(path_value: str) -> Optional[int]:
    if not path_value:
        return None
    if _is_remote_uri(path_value):
        return None
    path = _container_to_host_path(path_value) if str(path_value).startswith("/data/") else Path(path_value)
    if not path.exists():
        return None
    try:
        if path.is_file():
            return int(path.stat().st_size)
        total = 0
        file_count = 0
        for child in path.rglob("*"):
            if not child.is_file():
                continue
            total += int(child.stat().st_size)
            file_count += 1
            if file_count > 5000:
                return None
        return total
    except OSError:
        return None


def register_zarr_artifact(
    api_url: str,
    api_key: str,
    *,
    convert_response: Dict[str, Any],
    raw_uri: str,
    provider: str,
    collection: str,
    scene_id: str,
) -> bool:
    zarr_uri = str(convert_response.get("zarr_uri", "")).strip()
    if not zarr_uri:
        return False
    summary = convert_response.get("normalization_summary", {}) or {}
    zarr_summary = summary.get("zarr_summary", {}) or {}
    metadata = {
        "normalization_summary": summary,
        "registered_via": "streamlit_ui",
    }
    payload = {
        "artifact_type": "zarr",
        "artifact_uri": zarr_uri,
        "provider": provider,
        "collection": collection,
        "scene_id": scene_id,
        "source_uri": raw_uri,
        "created_by_job_id": str(convert_response.get("job_id", "")).strip() or None,
        "data_family": convert_response.get("data_family"),
        "band_names": list(convert_response.get("band_names") or zarr_summary.get("band_names") or []),
        "dimensions": list(convert_response.get("dimensions") or zarr_summary.get("dimensions") or []),
        "shape": list(zarr_summary.get("shape") or []),
        "size_bytes": _path_size_bytes(zarr_uri),
        "metadata": metadata,
    }
    try:
        response = _api_request("POST", api_url, "/v1/artifacts", api_key=api_key, payload=payload, timeout=30)
        return bool(response.ok)
    except Exception:
        return False


def artifact_visibility_status(item: Dict[str, Any]) -> str:
    metadata = dict(item.get("metadata") or {})
    if metadata.get("registered_via") or metadata.get("normalization_summary"):
        return "current"
    if not metadata.get("discovered_local"):
        return "current"

    provider = str(item.get("provider") or "").lower()
    collection = str(item.get("collection") or "").upper()
    dimensions = [str(v) for v in (item.get("dimensions") or [])]
    band_names = [str(v) for v in (item.get("band_names") or [])]
    artifact_uri = str(item.get("artifact_uri") or "")
    source_uri = str(item.get("source_uri") or "")

    if dimensions and dimensions != ["time", "band", "y", "x"]:
        return "legacy"
    if provider == "copernicus" and collection == "SENTINEL-2" and band_names and len(band_names) < 10:
        return "legacy"
    if artifact_uri.startswith("/data/") and not source_uri and metadata.get("discovered_local"):
        return "legacy"
    return "local"


def filter_visible_artifacts(
    artifacts: List[Dict[str, Any]],
    *,
    include_legacy: bool = False,
) -> Tuple[List[Dict[str, Any]], int]:
    filtered: List[Dict[str, Any]] = []
    hidden_legacy = 0
    for item in artifacts:
        status = artifact_visibility_status(item)
        enriched = {**item, "_visibility_status": status}
        if status == "legacy" and not include_legacy:
            hidden_legacy += 1
            continue
        filtered.append(enriched)
    return filtered, hidden_legacy


def human_size(size_bytes: Any) -> str:
    try:
        value = int(size_bytes)
    except Exception:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}"


def guess_scene_id(raw_uri: str) -> str:
    parsed = urlparse(str(raw_uri or "").strip())
    name = Path(parsed.path).name if parsed.scheme else Path(raw_uri).name
    for suffix in (".SAFE.zip", ".SAFE", ".tar.gz", ".tgz", ".tar", ".zip", ".nc", ".tif", ".tiff"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem


def guess_zarr_provider(raw_uri: str) -> str:
    lower_name = str(raw_uri or "").lower()
    if any(token in lower_name for token in ("landsat", "lc08", "lc09", "le07", "lt05")):
        return "usgs"
    return "copernicus"


def guess_zarr_collection(provider_api: str, scene_id: str) -> str:
    scene_upper = str(scene_id or "").upper()
    if provider_api == "usgs":
        if "_L1" in scene_upper:
            return "landsat_ot_c2_l1"
        return "landsat_ot_c2_l2"
    if scene_upper.startswith("S1"):
        return "SENTINEL-1"
    return "SENTINEL-2"


def default_zarr_output(scene_id: str) -> str:
    safe_scene = re.sub(r"[^A-Za-z0-9._-]+", "_", (scene_id or "scene")).strip("._-")
    if not safe_scene:
        safe_scene = "scene"
    return f"/data/downloads/zarr/{safe_scene}.zarr"


def guess_zarr_product_type(provider_api: str, collection: str, scene_id: str) -> str:
    scene_upper = str(scene_id or "").upper()
    if provider_api == "usgs":
        match = re.search(r"_((?:L1|L2)[A-Z0-9]{2})_", scene_upper)
        return match.group(1) if match else ("L1TP" if collection.endswith("_l1") else "L2SP")
    if scene_upper.startswith("S2"):
        if "MSIL1C" in scene_upper:
            return "S2MSI1C"
        if "MSIL2A" in scene_upper:
            return "S2MSI2A"
        return "S2MSI2A"
    token = guess_s1_product_type_from_scene(scene_upper)
    return token or "GRD"


def guess_s1_product_type_from_scene(scene_upper: str) -> str:
    parts = str(scene_upper).split("_")
    mode = parts[1] if len(parts) > 1 else ""
    token = parts[2] if len(parts) > 2 else ""
    if token.startswith("RAW"):
        return "RAW"
    if token.startswith("GRD"):
        return "GRD"
    if token.startswith("SLC"):
        return "IW_SLC__1S" if mode == "IW" else "SLC"
    return ""


def guess_raw_source_format(raw_uri: str) -> str:
    lower = str(raw_uri or "").lower()
    if lower.endswith(".zip"):
        return "zip"
    if lower.endswith(".tar") or lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        return "tar"
    if lower.endswith(".nc"):
        return "netcdf"
    if _is_remote_uri(raw_uri):
        parsed = urlparse(str(raw_uri or ""))
        remote_name = Path(parsed.path).name.lower()
        if remote_name.endswith(".zip"):
            return "zip"
        if remote_name.endswith(".tar") or remote_name.endswith(".tar.gz") or remote_name.endswith(".tgz"):
            return "tar"
        if remote_name.endswith(".nc"):
            return "netcdf"
        return "directory"
    if raw_uri:
        path = Path(raw_uri)
        if path.exists() and path.is_dir():
            return "directory"
    return "directory"


def zarr_service_schema(service_url: str) -> dict[str, Any]:
    service_url = str(service_url or "").strip()
    if not service_url:
        return {}
    try:
        response = _http_session().get(f"{service_url.rstrip('/')}/schema", timeout=30)
        if response.ok:
            return response.json()
    except Exception:
        return {}
    return {}


def zarr_supported_collections(schema: dict[str, Any], provider_api: str) -> list[str]:
    collections = ((schema.get("converter_config", {}) or {}).get(provider_api, {}) or {})
    if collections:
        return list(collections.keys())
    return list((schema.get("supported_collections", {}) or {}).get(provider_api, []))


def zarr_supported_product_types(schema: dict[str, Any], collection: str) -> list[str]:
    config = (schema.get("converter_config", {}) or {})
    if collection in ((config.get("copernicus") or {})):
        return list((((config.get("copernicus") or {}).get(collection)) or {}).keys())
    if collection in ((config.get("usgs") or {})):
        return list((((config.get("usgs") or {}).get(collection)) or {}).keys())
    return list((schema.get("supported_product_types", {}) or {}).get(collection, []))


def _http_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def list_artifacts_with_local(
    api_url: str,
    api_key: str,
    *,
    artifact_type: Optional[str] = None,
    provider: Optional[str] = None,
    collection: Optional[str] = None,
    scene_id: Optional[str] = None,
    job_id: Optional[str] = None,
    uri_query: Optional[str] = None,
    include_local: bool = False,
    page: int = 1,
    page_size: int = 100,
) -> Tuple[List[Dict[str, Any]], int]:
    return list_artifacts(
        api_url,
        api_key,
        artifact_type=artifact_type,
        provider=provider,
        collection=collection,
        scene_id=scene_id,
        job_id=job_id,
        uri_query=uri_query,
        include_local=include_local,
        page=page,
        page_size=page_size,
    )
