from __future__ import annotations

from pathlib import Path

import streamlit as st


def render_settings_tab(
    *,
    skey: str,
    api_url: str,
    downloads_dir: Path,
    map_center: list[float],
    map_zoom: int,
) -> None:
    st.markdown(
        '<div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;"><span>🔧</span><span style="font-weight:600;font-size:.94rem;">Settings</span></div>',
        unsafe_allow_html=True,
    )
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Center", f"{map_center[0]:.4f}, {map_center[1]:.4f}")
    with col2:
        st.metric("Zoom", map_zoom)
    with col3:
        st.metric("System", skey)
    st.markdown("---")
    st.code(f"API URL: {api_url}\nDownloads dir: {downloads_dir}", language="text")
    st.markdown("---")

    st.markdown("**Converter / Zarr settings**")
    colz1, colz2, colz3 = st.columns(3)
    with colz1:
        st.session_state["zarr_chunk_time"] = st.number_input(
            "Chunk time",
            min_value=1,
            value=int(st.session_state.get("zarr_chunk_time", 1)),
            step=1,
        )
        st.session_state["zarr_clear_encodings"] = st.checkbox(
            "Clear encodings",
            value=bool(st.session_state.get("zarr_clear_encodings", True)),
        )
        st.session_state["zarr_prefetch"] = st.checkbox(
            "Prefetch remote",
            value=bool(st.session_state.get("zarr_prefetch", True)),
        )
    with colz2:
        st.session_state["zarr_chunk_y"] = st.number_input(
            "Chunk y",
            min_value=1,
            value=int(st.session_state.get("zarr_chunk_y", 512)),
            step=64,
        )
        st.session_state["zarr_append_mode"] = st.checkbox(
            "Append mode (time)",
            value=bool(st.session_state.get("zarr_append_mode", False)),
        )
        st.session_state["zarr_cache_remote"] = st.checkbox(
            "Cache remote",
            value=bool(st.session_state.get("zarr_cache_remote", True)),
        )
    with colz3:
        st.session_state["zarr_chunk_x"] = st.number_input(
            "Chunk x",
            min_value=1,
            value=int(st.session_state.get("zarr_chunk_x", 512)),
            step=64,
        )
        st.session_state["zarr_output_base"] = st.text_input(
            "Output base",
            value=st.session_state.get("zarr_output_base", "/data/downloads/zarr"),
        )
        st.session_state["zarr_cleanup_remote"] = st.checkbox(
            "Cleanup temp",
            value=bool(st.session_state.get("zarr_cleanup_remote", True)),
        )

    st.session_state["zarr_band_config_path"] = st.text_input(
        "Band config YAML",
        value=st.session_state.get("zarr_band_config_path"),
        help="Path to converter/config/bands.yml",
    )
    st.session_state["zarr_log_level"] = st.selectbox(
        "Converter log level",
        options=["info", "debug"],
        index=["info", "debug"].index(str(st.session_state.get("zarr_log_level", "info"))),
    )

    st.markdown("**Runtime notes**")
    st.markdown(
        """- Legacy map/tile UX preserved.
- Downloads go through FastAPI jobs (`/v1/jobs`) and worker service.
- Zarr conversions use the new reader/cube/writer pipeline (bands variable, fsspec remote, consolidated metadata).
- Reset/Unlock only clear UI runtime state, not downloaded files."""
    )
