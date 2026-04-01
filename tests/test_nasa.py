from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
import types


NASA_PATH = Path(__file__).resolve().parents[1] / "providers" / "nasa.py"

providers_pkg = types.ModuleType("providers")
providers_pkg.__path__ = [str(NASA_PATH.parent)]
provider_base_module = types.ModuleType("providers.provider_base")
provider_base_module.ProviderBase = type("ProviderBase", (), {})
utilities_module = types.ModuleType("utilities")
utilities_module.ConfigLoader = type("ConfigLoader", (), {})
utilities_module.DownloadManager = type("DownloadManager", (), {})
utilities_module.OCIFSManager = type("OCIFSManager", (), {})

sys.modules.setdefault("providers", providers_pkg)
sys.modules["providers.provider_base"] = provider_base_module
sys.modules["utilities"] = utilities_module

NASA_SPEC = spec_from_file_location("nasa_provider_under_test", NASA_PATH)
NASA_MODULE = module_from_spec(NASA_SPEC)
assert NASA_SPEC.loader is not None
NASA_SPEC.loader.exec_module(NASA_MODULE)
Nasa = NASA_MODULE.Nasa


def test_matches_tile_id_handles_optional_t_prefix():
    granule_id = "HLS.S30.T35MRT.2024007T081229.v2.0"

    assert Nasa._matches_tile_id(granule_id, "35MRT")
    assert Nasa._matches_tile_id(granule_id, "T35MRT")
    assert not Nasa._matches_tile_id(granule_id, "35MQT")


def test_build_relative_path_groups_assets_under_granule_folder():
    granule_id = "HLS.S30.T35MRT.2024007T081229.v2.0"
    url = "https://example.com/path/HLS.S30.T35MRT.2024007T081229.v2.0.B04.tif"

    assert Nasa._build_relative_path(granule_id, url) == (
        "HLS.S30.T35MRT.2024007T081229.v2.0/"
        "HLS.S30.T35MRT.2024007T081229.v2.0.B04.tif"
    )


def test_is_downloadable_asset_rejects_query_style_non_tiff_links():
    assert Nasa._is_downloadable_asset(
        "https://example.com/granules?p=C2021957295-LPCLOUD"
    ) is False
    assert Nasa._is_downloadable_asset(
        "https://example.com/path/HLS.S30.T35MRT.2024007T081229.v2.0.B04.tif"
    ) is True
