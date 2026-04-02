from __future__ import annotations

from pathlib import Path
from typing import Any

import streamlit as st

from nimbuschain_fetch_ui.provider_auth import provider_action_guidance, provider_auth_state_label


def render_settings_tab(
    *,
    skey: str,
    api_url: str,
    downloads_dir: Path,
    map_center: list[float],
    map_zoom: int,
    provider_status_snapshot: dict[str, Any] | None,
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

    st.markdown("**Provider auth runtime**")
    providers = []
    if isinstance(provider_status_snapshot, dict):
        providers = [
            item
            for item in list(provider_status_snapshot.get("providers") or [])
            if isinstance(item, dict)
        ]
    if not providers:
        st.caption("Provider auth status unavailable. Refresh service status from Connection.")
    else:
        for item in providers:
            provider_name = str(item.get("provider") or "").upper()
            state_label = provider_auth_state_label(item)
            guidance = provider_action_guidance(str(item.get("provider") or ""), {"providers": [item]})
            st.markdown(
                "<div style='background:#111827;border:1px solid rgba(56,120,200,0.10);border-radius:10px;padding:12px;margin-bottom:8px;'>"
                f"<div style='display:flex;justify-content:space-between;align-items:center;gap:8px;'><span style='font-size:.78rem;color:#94a3b8;font-weight:600;'>{provider_name}</span>"
                f"<span style='font-size:.72rem;color:{'#22c55e' if state_label == 'valid' else '#ef4444' if state_label in {'missing', 'credentials invalid', 'credentials missing'} else '#f59e0b'};font-weight:700;text-transform:uppercase;'>{state_label}</span></div>"
                f"<div style='font-size:.72rem;color:#cbd5e1;margin-top:6px;'>{str(item.get('message') or '-')}</div>"
                f"<div style='font-size:.65rem;color:#64748b;margin-top:4px;'>Credential source: runtime env · username present: {'yes' if item.get('username_present') else 'no'} · token present: {'yes' if item.get('token_present') or item.get('password_present') else 'no'}</div>"
                "</div>",
                unsafe_allow_html=True,
            )
            detail = str(item.get("detail") or "").strip()
            if detail:
                with st.expander(f"{provider_name} auth details", expanded=False):
                    st.code(detail, language="text")
            if guidance:
                st.caption(guidance)
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
