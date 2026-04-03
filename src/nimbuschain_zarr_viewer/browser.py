from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import streamlit as st

from nimbuschain_zarr_viewer.cli import (
    build_launch_command,
    default_zarr_root,
    discover_local_zarr_stores,
    summarize_store,
)


PRESET_LABELS = {
    "rgb": "RGB only",
    "rgb-masks": "RGB + masks",
    "all-bands": "All bands + masks",
    "masks-only": "Masks only",
}


def _format_shape(shape: tuple[int, ...]) -> str:
    if not shape:
        return "-"
    return " x ".join(str(int(item)) for item in shape)


def _load_store_catalog(root: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for store_path in discover_local_zarr_stores(root):
        try:
            summary = summarize_store(store_path)
            stat = store_path.stat()
        except Exception as exc:
            entries.append(
                {
                    "path": str(store_path),
                    "name": store_path.name,
                    "updated_at": "",
                    "band_count": 0,
                    "ancillary_count": 0,
                    "mask_layers": [],
                    "shape": (),
                    "error": str(exc),
                }
            )
            continue
        entries.append(
            {
                "path": str(summary.path),
                "name": summary.path.name,
                "updated_at": dt.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "band_count": len(summary.band_names),
                "ancillary_count": len(summary.ancillary_names),
                "mask_layers": list(summary.mask_layers),
                "shape": summary.imagery_shape,
                "error": "",
            }
        )
    return entries


def _option_label(entry: dict[str, Any]) -> str:
    masks = ", ".join(entry.get("mask_layers") or []) or "none"
    return (
        f"{entry.get('name', '-')}"
        f" | bands={entry.get('band_count', 0)}"
        f" | ancillary={entry.get('ancillary_count', 0)}"
        f" | masks={masks}"
    )


def _launch_store(path: str, *, preset: str, step: int, grid: bool) -> list[str]:
    command = build_launch_command(
        path,
        preset=preset,
        step=step or None,
        grid=grid,
        python_executable=sys.executable,
    )
    env = dict(os.environ)
    current_pythonpath = env.get("PYTHONPATH") or ""
    repo_src = str(Path(__file__).resolve().parents[2] / "src")
    env["PYTHONPATH"] = repo_src if not current_pythonpath else f"{repo_src}:{current_pythonpath}"
    subprocess.Popen(
        command,
        cwd=str(Path(__file__).resolve().parents[2]),
        env=env,
        start_new_session=True,
    )
    return command


def main() -> None:
    st.set_page_config(page_title="Nimbus Zarr Viewer", layout="wide")
    st.title("Nimbus Zarr Viewer")
    st.caption("Standalone Napari launcher for local Zarr stores.")

    top_left, top_mid, top_right = st.columns([3, 2, 1])
    with top_left:
        zarr_root = st.text_input(
            "Local Zarr root",
            value=str(default_zarr_root()),
            help="Directory scanned for local `.zarr` stores.",
        ).strip()
    with top_mid:
        query = st.text_input(
            "Filter stores",
            value="",
            placeholder="scene name or path fragment",
        ).strip().lower()
    with top_right:
        refresh = st.button("Refresh catalog", type="primary", width="stretch")

    if refresh:
        st.cache_data.clear()

    catalog = st.cache_data(show_spinner=False)(_load_store_catalog)(zarr_root)
    if query:
        catalog = [
            item
            for item in catalog
            if query in str(item.get("name", "")).lower() or query in str(item.get("path", "")).lower()
        ]

    if not catalog:
        st.warning("No local Zarr store found under the selected root.")
        return

    overview_cols = st.columns(4)
    with overview_cols[0]:
        st.metric("Local Zarr stores", len(catalog))
    with overview_cols[1]:
        st.metric("With masks", sum(1 for item in catalog if item.get("mask_layers")))
    with overview_cols[2]:
        st.metric("With ancillary", sum(1 for item in catalog if int(item.get("ancillary_count", 0)) > 0))
    with overview_cols[3]:
        st.metric("With errors", sum(1 for item in catalog if item.get("error")))

    table_rows = [
        {
            "name": item.get("name", "-"),
            "bands": item.get("band_count", 0),
            "ancillary": item.get("ancillary_count", 0),
            "masks": ", ".join(item.get("mask_layers") or []) or "none",
            "shape": _format_shape(tuple(item.get("shape") or ())),
            "updated_at": str(item.get("updated_at") or "-")[:19].replace("T", " "),
            "error": item.get("error", ""),
        }
        for item in catalog
    ]
    st.dataframe(table_rows, width="stretch", hide_index=True)

    selected_path = st.selectbox(
        "Choose Zarr store",
        options=[str(item["path"]) for item in catalog],
        format_func=lambda value: next(
            (_option_label(item) for item in catalog if str(item.get("path")) == value),
            value,
        ),
    )
    selected_entry = next(item for item in catalog if str(item.get("path")) == selected_path)

    preset = st.radio(
        "Viewer preset",
        options=["rgb", "rgb-masks", "all-bands", "masks-only"],
        index=2,
        horizontal=True,
        format_func=lambda key: PRESET_LABELS[key],
    )
    st.caption(
        "`RGB only` opens the RGB composite only. "
        "`RGB + masks` opens RGB plus cloud/water layers. "
        "`All bands + masks` opens imagery bands, RGB, ancillary, and masks. "
        "`Masks only` opens cloud/water layers and probabilities."
    )
    step = int(
        st.number_input(
            "Preview step",
            min_value=0,
            value=0,
            step=1,
            help="0 = auto. Increase to downsample large scenes before opening Napari.",
        )
    )
    grid = st.checkbox("Enable Napari grid", value=False)

    details_lines = [
        f"Path: {selected_entry.get('path', '-')}",
        f"Bands: {selected_entry.get('band_count', 0)}",
        f"Ancillary layers: {selected_entry.get('ancillary_count', 0)}",
        f"Masks: {', '.join(selected_entry.get('mask_layers') or []) or '-'}",
        f"Shape: {_format_shape(tuple(selected_entry.get('shape') or ()))}",
        f"Updated: {str(selected_entry.get('updated_at') or '-')[:19].replace('T', ' ')}",
    ]
    if selected_entry.get("error"):
        details_lines.append(f"Error: {selected_entry['error']}")
    st.code("\n".join(details_lines), language="text")

    command_preview = build_launch_command(
        selected_path,
        preset=preset,
        step=step or None,
        grid=grid,
        python_executable=sys.executable,
    )
    st.caption("Launch command")
    st.code(" ".join(command_preview), language="bash")

    if st.button("Open in Napari", type="primary", width="stretch", disabled=bool(selected_entry.get("error"))):
        command = _launch_store(selected_path, preset=preset, step=step, grid=grid)
        st.success(f"Napari launched for `{Path(selected_path).name}`.")
        st.code(" ".join(command), language="bash")


if __name__ == "__main__":
    main()
