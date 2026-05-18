from __future__ import annotations

from pathlib import Path

from nimbuschain_fetch_ui.downloads import count_downloaded_products
from nimbuschain_fetch_ui.results_tab import _download_file_rows


def test_download_file_rows_skip_files_that_disappear_during_scan(
    tmp_path: Path,
    monkeypatch,
) -> None:
    keep = tmp_path / "keep.txt"
    keep.write_text("ok", encoding="utf-8")
    disappearing = tmp_path / "gone.partial"
    disappearing.write_text("partial", encoding="utf-8")

    original_stat = Path.stat

    def flaky_stat(self: Path, *args, **kwargs):
        if self == disappearing:
            raise FileNotFoundError
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_stat)

    rows = _download_file_rows(tmp_path)

    assert rows == [
        {
            "path": "keep.txt",
            "size_MB": 0.0,
            "modified": rows[0]["modified"],
        }
    ]


def test_count_downloaded_products_skips_files_that_disappear_during_scan(
    tmp_path: Path,
    monkeypatch,
) -> None:
    keep = tmp_path / "keep.txt"
    keep.write_text("ok", encoding="utf-8")
    disappearing = tmp_path / "gone.partial"
    disappearing.write_text("partial", encoding="utf-8")

    count_downloaded_products.clear()
    original_stat = Path.stat

    def flaky_stat(self: Path, *args, **kwargs):
        if self == disappearing:
            raise FileNotFoundError
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_stat)

    count, total_mb = count_downloaded_products(str(tmp_path))

    assert count == 1
    assert total_mb > 0.0
    count_downloaded_products.clear()
