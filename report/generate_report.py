from __future__ import annotations
import json
import math
import os
import hashlib
from pathlib import Path


# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------
_BG        = "#f8f7f5"
_SURFACE   = "#ffffff"
_BORDER    = "#e4e1db"
_BORDER_ST = "#ccc8c0"
_TEXT      = "#1a1916"
_TEXT2     = "#6b6860"
_TEXT3     = "#a09d97"
_GREEN     = "#2d6a4f"
_GREEN_BG  = "#edf6f0"
_AMBER     = "#8a5c1a"
_AMBER_BG  = "#fdf3e0"
_RED       = "#8b2020"
_RED_BG    = "#fdf0f0"

_STEP_COLORS = {
    "geometric_processing":   "#b0ada6",
    "atmospheric_correction": "#b0ada6",
    "sbaf":                   "#b0ada6",
    "valid_pixel_mask":       "#b0ada6",
    "brdf_adjustment":        "#6b6860",
    "data_fusion":            "#b0ada6",
    "packaging":              "#a09d97",
    "validation":             "#b0ada6",
    "cleanup":                "#b0ada6",
}

_STATUS = {
    "success":     ("✓", _GREEN,  _GREEN_BG),
    "failed":      ("✗", _RED,    _RED_BG),
    "skipped":     ("↷", _AMBER,  _AMBER_BG),
    "running":     ("◌", _TEXT2,  _BORDER),
    "invalidated": ("⊘", _TEXT3,  _BORDER),
}

# Tile class metadata — label + color for each routing class
_TILE_CLASS_META = {
    "SKIP":             ("Skip",             _RED,    "Tile was too cloudy or nodata-heavy to process"),
    "WATER":            ("Water",            "#4a6fa8","Open water surface — SBAF and BRDF skipped"),
    "DENSE_VEGETATION": ("Dense vegetation", _GREEN,  "High NDVI canopy — full pipeline"),
    "BARE_SOIL":        ("Bare soil",        _AMBER,  "Low NDVI, low NDWI — full pipeline with BRDF"),
    "URBAN":            ("Urban",            "#7b6fa0","High brightness variance — SBAF critical"),
    "MIXED":            ("Mixed",            _TEXT2,  "Ambiguous or partially cloudy — full pipeline"),
}

# All known pipeline steps in canonical order
_FULL_STEP_ORDER = [
    "geometric_processing",
    "atmospheric_correction",
    "sbaf",
    "valid_pixel_mask",
    "brdf_adjustment",
    "data_fusion",
    "packaging",
    "validation",
    "cleanup",
]


# ---------------------------------------------------------------------------
# System resource snapshot
# ---------------------------------------------------------------------------

def _collect_system_resources() -> dict:
    try:
        import psutil
        cpu_pct   = psutil.cpu_percent(interval=0.5)
        cpu_count = psutil.cpu_count(logical=True)
        cpu_freq  = psutil.cpu_freq()
        freq_ghz  = round(cpu_freq.current / 1000, 2) if cpu_freq else None
        vm        = psutil.virtual_memory()
        disk      = psutil.disk_usage("/")
        return {
            "available":  True,
            "cpu_pct":    cpu_pct,
            "cpu_count":  cpu_count,
            "freq_ghz":   freq_ghz,
            "ram_total":  vm.total,
            "ram_used":   vm.used,
            "ram_avail":  vm.available,
            "ram_pct":    vm.percent,
            "disk_total": disk.total,
            "disk_used":  disk.used,
            "disk_free":  disk.free,
            "disk_pct":   disk.percent,
        }
    except ImportError:
        print("[report] psutil not installed — system resource section will be skipped.")
        return {"available": False}
    except Exception as exc:
        print(f"[report] System resource collection failed: {exc}")
        return {"available": False}


def _fmt_bytes(n: int) -> str:
    if n >= 1 << 30:
        return f"{n / (1 << 30):.1f} GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.0f} MB"
    return f"{n / (1 << 10):.0f} KB"


def _resource_color(pct: float) -> str:
    if pct >= 90: return _RED
    if pct >= 70: return _AMBER
    return _GREEN


def _gauge_svg(pct: float, color: str, size: int = 80) -> str:
    import math as _m
    r = size / 2 - 8; cx = cy = size / 2; stroke = max(6, size // 12)
    def arc_point(deg):
        rad = _m.radians(deg)
        return cx + r * _m.cos(rad), cy + r * _m.sin(rad)
    end_deg = 135 + 270 * min(pct, 100) / 100
    x1, y1 = arc_point(135); x2, y2 = arc_point(end_deg)
    bx1, by1 = arc_point(135); bx2, by2 = arc_point(405)
    large = 1 if (end_deg - 135) > 180 else 0
    bg_d = f"M{bx1:.2f},{by1:.2f} A{r},{r} 0 1,1 {bx2:.2f},{by2:.2f}"
    fg_d = f"M{x1:.2f},{y1:.2f} A{r},{r} 0 {large},1 {x2:.2f},{y2:.2f}"
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" xmlns="http://www.w3.org/2000/svg">'
        f'<path d="{bg_d}" fill="none" stroke="{_BORDER_ST}" stroke-width="{stroke}" stroke-linecap="round"/>'
        f'<path d="{fg_d}" fill="none" stroke="{color}" stroke-width="{stroke}" stroke-linecap="round"/>'
        f'<text x="{cx}" y="{cy+5}" text-anchor="middle" font-family="\'DM Mono\',monospace" '
        f'font-size="{size//5}" font-weight="500" fill="{_TEXT}">{pct:.0f}%</text>'
        f'</svg>'
    )


def _sec_system_resources(res: dict) -> str:
    if not res.get("available"):
        return _empty_card("system", "System Resources",
                           "psutil not installed — run: pip install psutil")
    cpu_col  = _resource_color(res["cpu_pct"])
    ram_col  = _resource_color(res["ram_pct"])
    disk_col = _resource_color(res["disk_pct"])
    freq_row = (
        f'<div class="res-detail"><span class="res-k">Frequency</span>'
        f'<span class="res-v">{res["freq_ghz"]} GHz</span></div>'
        if res.get("freq_ghz") else ""
    )
    return f"""
<section class="card" id="system">
  <div class="card-head">
    <h2>System Resources</h2><span class="head-rule"></span>
    <span class="head-tag">Captured at report generation time</span>
  </div>
  <div class="res-grid">
    <div class="res-cell">
      <div class="res-gauge">{_gauge_svg(res["cpu_pct"], cpu_col, 90)}</div>
      <div class="res-title">CPU</div>
      <div class="res-details">
        <div class="res-detail"><span class="res-k">Usage</span><span class="res-v" style="color:{cpu_col}">{res['cpu_pct']:.1f}%</span></div>
        <div class="res-detail"><span class="res-k">Cores</span><span class="res-v">{res['cpu_count']}</span></div>
        {freq_row}
      </div>
    </div>
    <div class="res-divider"></div>
    <div class="res-cell">
      <div class="res-gauge">{_gauge_svg(res["ram_pct"], ram_col, 90)}</div>
      <div class="res-title">RAM</div>
      <div class="res-details">
        <div class="res-detail"><span class="res-k">Used</span><span class="res-v" style="color:{ram_col}">{_fmt_bytes(res['ram_used'])}</span></div>
        <div class="res-detail"><span class="res-k">Available</span><span class="res-v">{_fmt_bytes(res['ram_avail'])}</span></div>
        <div class="res-detail"><span class="res-k">Total</span><span class="res-v">{_fmt_bytes(res['ram_total'])}</span></div>
      </div>
    </div>
    <div class="res-divider"></div>
    <div class="res-cell">
      <div class="res-gauge">{_gauge_svg(res["disk_pct"], disk_col, 90)}</div>
      <div class="res-title">Disk</div>
      <div class="res-details">
        <div class="res-detail"><span class="res-k">Used</span><span class="res-v" style="color:{disk_col}">{_fmt_bytes(res['disk_used'])}</span></div>
        <div class="res-detail"><span class="res-k">Free</span><span class="res-v">{_fmt_bytes(res['disk_free'])}</span></div>
        <div class="res-detail"><span class="res-k">Total</span><span class="res-v">{_fmt_bytes(res['disk_total'])}</span></div>
      </div>
    </div>
  </div>
</section>"""


# ---------------------------------------------------------------------------
# Tile routing section  ← NEW
# ---------------------------------------------------------------------------

def _sec_routing(manifest: dict) -> str:
    """Show the adaptive tile routing decision: class, profile stats, steps routed vs skipped."""
    tile_class   = manifest.get("tile_class")
    steps_routed = manifest.get("steps_routed")

    if not tile_class:
        return _empty_card("routing", "Adaptive Tile Routing",
                           "Routing metadata not present — pipeline may have run without tile_router.py, "
                           "or this manifest predates the routing integration.")

    cls_label, cls_color, cls_desc = _TILE_CLASS_META.get(
        tile_class, (tile_class, _TEXT2, ""))

    # Build step pills: routed = green, skipped = muted
    all_processing_steps = [s for s in _FULL_STEP_ORDER
                            if s not in ("validation", "cleanup")]
    routed_set = set(steps_routed or [])

    pills = ""
    for step in all_processing_steps:
        in_route = step in routed_set
        col   = _GREEN  if in_route else _TEXT3
        bg    = _GREEN_BG if in_route else _BORDER
        icon  = "✓" if in_route else "—"
        label = step.replace("_", " ").title()
        pills += (f'<span class="route-pill" style="background:{bg};color:{col}">'
                  f'{icon} {_e(label)}</span>')

    n_run    = len([s for s in routed_set if s in all_processing_steps])
    n_skip   = len(all_processing_steps) - n_run
    savings  = f"{n_skip}/{len(all_processing_steps)} steps skipped"

    return f"""
<section class="card" id="routing">
  <div class="card-head"><h2>Adaptive Tile Routing</h2><span class="head-rule"></span>
    <span class="head-tag" style="color:{cls_color}">{_e(cls_label)}</span>
  </div>
  <div class="route-class-row">
    <span class="route-class-badge" style="background:{_hex_bg(cls_color)};color:{cls_color}">
      {_e(tile_class)}
    </span>
    <span class="route-class-desc">{_e(cls_desc)}</span>
    <span class="route-savings">{_e(savings)}</span>
  </div>
  <div class="route-pills">{pills}</div>
  <p class="note">Steps marked — were skipped for this tile class, reducing processing time without loss of output quality.</p>
</section>"""


def _hex_bg(hex_col: str) -> str:
    """Return a very light tint of a hex color for badge backgrounds."""
    h = hex_col.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    r2 = min(255, r + int((255 - r) * 0.85))
    g2 = min(255, g + int((255 - g) * 0.85))
    b2 = min(255, b + int((255 - b) * 0.85))
    return f"#{r2:02x}{g2:02x}{b2:02x}"


# ---------------------------------------------------------------------------
# Before / After imagery
# ---------------------------------------------------------------------------

_LANDSAT_RGB_BANDS = [("R", "B4"), ("G", "B3"), ("B", "B2")]
_FUSION_RGB_BANDS  = [("R", "B4_10m.TIF"), ("G", "B3_10m.TIF"), ("B", "B2_10m.TIF")]


def _find_raw_rgb_bands(landsat_scene_id: str, product_out_dir: Path) -> dict | None:
    scene_path = Path(landsat_scene_id)
    scene_id   = scene_path.name
    def _candidate_dirs():
        dirs = []
        # Try the explicit directory first, then its parent.
        if scene_path.exists():
            dirs += [scene_path, scene_path.parent]
        env_dir = os.environ.get("LANDSAT_INPUT_DIR")
        if env_dir:
            dirs += [Path(env_dir) / scene_id, Path(env_dir)]
        for up in range(1, 5):
            p = product_out_dir
            for _ in range(up): p = p.parent
            dirs += [p / scene_id, p]
        dirs += [Path.cwd() / scene_id, Path.cwd()]
        return dirs
    result = {}
    for channel, band_suffix in _LANDSAT_RGB_BANDS:
        b_name = f"{scene_id}_{band_suffix}.TIF"
        found  = next((d / b_name for d in _candidate_dirs() if (d / b_name).exists()), None)
        if found is None:
            print(f"[report] Raw {band_suffix} not found for scene '{scene_id}'")
            return None
        result[channel] = found
    return result


def _array_to_base64_png(arr) -> str:
    import io, base64, zlib, struct
    try:
        from PIL import Image
        buf  = io.BytesIO()
        mode = "L" if arr.ndim == 2 else ("RGBA" if arr.shape[2] == 4 else "RGB")
        Image.fromarray(arr, mode=mode).save(buf, format="PNG", optimize=True)
        return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
    except ImportError:
        pass
    if arr.ndim != 2:
        raise RuntimeError("Pillow is required to encode RGB/RGBA PNGs.")
    def _chunk(tag, data):
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)
    h, w = arr.shape
    raw  = b"".join(b"\x00" + arr[y].tobytes() for y in range(h))
    ihdr = struct.pack(">IIBBBBB", w, h, 8, 0, 0, 0, 0)
    png  = (b"\x89PNG\r\n\x1a\n"
            + _chunk(b"IHDR", ihdr)
            + _chunk(b"IDAT", zlib.compress(raw, 6))
            + _chunk(b"IEND", b""))
    return "data:image/png;base64," + base64.b64encode(png).decode()


def _stretch(arr, nodata_val=None):
    import numpy as np
    a = arr.astype(np.float32)
    if nodata_val is not None:
        a = np.where(a == nodata_val, np.nan, a)
    valid = a[np.isfinite(a)]
    if valid.size == 0:
        return np.zeros(arr.shape, dtype=np.uint8)
    lo, hi = np.percentile(valid, [2, 98])
    if hi <= lo: hi = lo + 1
    return np.clip(np.where(np.isfinite(a), (a - lo) / (hi - lo) * 255, 0), 0, 255).astype(np.uint8)


def _reproject_to_grid(src_path, dst_transform, dst_crs, dst_w, dst_h):
    import numpy as np, rasterio
    from rasterio.warp import reproject, Resampling
    out = np.zeros((dst_h, dst_w), dtype=np.float32)
    with rasterio.open(src_path) as src:
        reproject(source=rasterio.band(src, 1), destination=out,
                  src_transform=src.transform, src_crs=src.crs,
                  dst_transform=dst_transform, dst_crs=dst_crs,
                  resampling=Resampling.nearest, dst_nodata=0.0)
    return out


def _resize_band(arr2d, out_w):
    import numpy as np, rasterio
    from rasterio.transform import from_bounds
    from rasterio.warp import reproject, Resampling
    h2, w2 = arr2d.shape
    out_h  = max(1, int(h2 * out_w / w2))
    dst    = np.zeros((out_h, out_w), dtype=np.float32)
    src_t  = from_bounds(0, 0, w2, h2, w2, h2)
    dst_t  = from_bounds(0, 0, w2, h2, out_w, out_h)
    reproject(source=arr2d, destination=dst,
              src_transform=src_t, src_crs="EPSG:4326",
              dst_transform=dst_t, dst_crs="EPSG:4326",
              resampling=Resampling.bilinear)
    return dst


def _build_rgb_thumbnail(r_arr, g_arr, b_arr, nodata_val=0.0, thumb_w=800):
    import numpy as np
    r_u8 = _stretch(_resize_band(r_arr, thumb_w), nodata_val)
    g_u8 = _stretch(_resize_band(g_arr, thumb_w), nodata_val)
    b_u8 = _stretch(_resize_band(b_arr, thumb_w), nodata_val)
    return _array_to_base64_png(np.stack([r_u8, g_u8, b_u8], axis=-1))


def _make_aligned_rgb_thumbnails(raw_band_paths, proc_band_paths, thumb_w=800):
    try:
        import numpy as np, rasterio
        with rasterio.open(proc_band_paths["R"]) as ref:
            proc_crs, proc_transform = ref.crs, ref.transform
            proc_w, proc_h = ref.width, ref.height
            proc_nodata    = ref.nodata
        nd_proc = float(proc_nodata) if proc_nodata is not None else 0.0
        proc_bands, raw_bands = {}, {}
        for ch in ("R","G","B"):
            with rasterio.open(proc_band_paths[ch]) as src:
                proc_bands[ch] = src.read(1).astype(np.float32)
            raw_bands[ch] = _reproject_to_grid(
                raw_band_paths[ch], proc_transform, proc_crs, proc_w, proc_h)
        def _valid(arr, nd):
            return (arr != nd) & np.isfinite(arr) & (arr != 0)
        shared = (_valid(proc_bands["R"], nd_proc) & _valid(proc_bands["G"], nd_proc)
                  & _valid(proc_bands["B"], nd_proc) & _valid(raw_bands["R"], 0.0)
                  & _valid(raw_bands["G"], 0.0) & _valid(raw_bands["B"], 0.0))
        rows = np.any(shared, axis=1); cols = np.any(shared, axis=0)
        if rows.any() and cols.any():
            r0 = int(rows.argmax()); r1 = int(len(rows) - rows[::-1].argmax())
            c0 = int(cols.argmax()); c1 = int(len(cols) - cols[::-1].argmax())
            mr = max(1, int((r1-r0)*0.05)); mc = max(1, int((c1-c0)*0.05))
            r0 = max(0, r0-mr); r1 = min(proc_h, r1+mr)
            c0 = max(0, c0-mc); c1 = min(proc_w, c1+mc)
            def crop(arr): return arr[r0:r1, c0:c1]
        else:
            def crop(arr): return arr
        proc_c = {ch: crop(proc_bands[ch]) for ch in ("R","G","B")}
        raw_c  = {ch: crop(raw_bands[ch])  for ch in ("R","G","B")}
        b64_before = _build_rgb_thumbnail(raw_c["R"],  raw_c["G"],  raw_c["B"],  0.0,     thumb_w)
        b64_after  = _build_rgb_thumbnail(proc_c["R"], proc_c["G"], proc_c["B"], nd_proc, thumb_w)
        return b64_before, b64_after
    except Exception as exc:
        print(f"[report] RGB thumbnail generation failed: {exc}")
        return None, None


def _sec_imagery(product_out_dir: Path, input_paths: dict) -> str:
    landsat_scene  = input_paths.get("landsat_path", "")
    raw_band_paths = _find_raw_rgb_bands(landsat_scene, product_out_dir)
    fusion_dir     = product_out_dir / "fusion"
    proc_band_paths, missing_proc = {}, []
    for channel, fname in _FUSION_RGB_BANDS:
        p = fusion_dir / fname
        if p.exists(): proc_band_paths[channel] = p
        else: missing_proc.append(fname)
    missing = []
    if raw_band_paths is None:
        missing.append(f"raw B2/B3/B4 not found for scene '{Path(landsat_scene).name}'")
    if missing_proc:
        missing.append(f"processed RGB bands missing: {', '.join(missing_proc)}")
    if missing:
        return _empty_card("imagery", "Before / After — Natural Colour RGB (B4/B3/B2)",
                           f"Imagery unavailable: {' · '.join(missing)}")
    b64_before, b64_after = _make_aligned_rgb_thumbnails(raw_band_paths, proc_band_paths, 800)
    if not b64_before or not b64_after:
        return _empty_card("imagery", "Before / After — Natural Colour RGB (B4/B3/B2)",
                           "Could not render thumbnails — check rasterio / numpy / Pillow.")
    scene_name = Path(landsat_scene).name
    return f"""
<section class="card" id="imagery">
  <div class="card-head">
    <h2>Before / After — Natural Colour RGB (B4/B3/B2)</h2>
    <span class="head-rule"></span><span class="head-tag">Drag to compare</span>
  </div>
  <div class="viewer-wrap" id="viewer">
    <div class="viewer-img-wrap">
      <img class="viewer-img viewer-img-before" id="imgBefore" src="{b64_before}" alt="Raw L1 RGB">
      <img class="viewer-img viewer-img-after"  id="imgAfter"  src="{b64_after}"  alt="sen2like RGB">
    </div>
    <div class="viewer-label left">Raw L1 · 30 m · {_e(scene_name)}</div>
    <div class="viewer-label right">sen2like · 10 m</div>
    <div class="divider-line" id="divLine" style="left:50%"></div>
    <div class="divider-handle" id="divHandle" style="left:calc(50% - 13px)">
      <span class="divider-arrows">&#9664; &#9654;</span>
    </div>
  </div>
  <p class="viewer-hint">&#8592; drag the slider to compare &#8594;</p>
</section>"""


# ---------------------------------------------------------------------------
# Improvement metrics — derived from real pipeline data  ← FIXED
# ---------------------------------------------------------------------------

def _derive_improvement_metrics(product_out_dir: Path, brdf_deltas: list[dict],
                                  ordered_steps: list) -> list[dict]:
    """
    Build improvement cards from actual pipeline outputs.
    Falls back to baseline estimates where live data is unavailable.
    """
    metrics = []
    steps_dict = dict(ordered_steps)

    # 1. Resolution  — fixed physical fact of the pipeline
    metrics.append({
        "label":        "Output resolution",
        "before":       30.0, "after": 10.0,
        "unit":         "m GSD",
        "lower_better": True,
        "source":       "pipeline spec",
    })

    # 2. Cloud masking — from mask stats in metadata.json
    meta_path = product_out_dir / "fusion" / "metadata.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
            vf   = meta.get("valid_pixel_mask", {}).get("valid_fraction")
            if vf is not None:
                masked_pct = round((1 - vf) * 100, 1)
                metrics.append({
                    "label":        "Cloud masked coverage",
                    "before":       0.0,
                    "after":        masked_pct,
                    "unit":         "% pixels flagged",
                    "lower_better": False,
                    "source":       "mask stats",
                })
        except Exception:
            pass

    # 3. BRDF correction effect — from real band deltas
    if brdf_deltas:
        mean_abs_pct = sum(abs(d["pct"]) for d in brdf_deltas) / len(brdf_deltas)
        # Express as improvement in BRDF consistency (0 before correction, mean_abs_pct adjusted)
        metrics.append({
            "label":        "BRDF mean correction",
            "before":       0.0,
            "after":        round(mean_abs_pct, 2),
            "unit":         "% mean Δ reflectance",
            "lower_better": False,
            "note":         f"Across {len(brdf_deltas)} bands",
            "source":       "computed from NBAR vs SBAF",
        })

    # 4. Spectral adjustment — confirm SBAF ran
    sbaf_step = steps_dict.get("sbaf", {})
    if sbaf_step.get("status") == "success":
        metrics.append({
            "label":        "Spectral adjustment (SBAF)",
            "before":       "Not applied",
            "after":        "Applied",
            "unit":         "LS8 → S2A normalised",
            "lower_better": None,   # not a numeric comparison
            "source":       "step status",
        })

    # 5. Geometric correction — confirm step ran
    geo_step = steps_dict.get("geometric_processing", {})
    if geo_step.get("status") == "success":
        metrics.append({
            "label":        "Geometric co-registration",
            "before":       "Not applied",
            "after":        "Applied",
            "unit":         "GRI-based sub-pixel shift",
            "lower_better": None,
            "source":       "step status",
        })

    return metrics


def _sec_improvement(product_out_dir: Path, brdf_deltas: list[dict],
                      ordered_steps: list) -> str:
    metrics = _derive_improvement_metrics(product_out_dir, brdf_deltas, ordered_steps)
    if not metrics:
        return _empty_card("improvement", "Pipeline Improvement",
                           "No improvement metrics available.")

    cards = ""
    for m in metrics:
        label = _e(m["label"])
        unit  = _e(m["unit"])
        src   = _e(m.get("source", ""))
        note  = _e(m.get("note", ""))

        if m["lower_better"] is None:
            # Qualitative — before/after text cards
            cards += f"""
<div class="imp-card">
  <div class="imp-label">{label}</div>
  <div class="imp-bars">
    <div class="imp-bar-row">
      <span class="imp-tag before-tag">Before</span>
      <span class="imp-val before-val" style="min-width:auto">{_e(str(m['before']))}</span>
    </div>
    <div class="imp-bar-row">
      <span class="imp-tag after-tag">After</span>
      <span class="imp-val after-val" style="color:{_GREEN};min-width:auto">{_e(str(m['after']))}</span>
    </div>
  </div>
  <div class="imp-delta" style="color:{_TEXT3};font-size:.62rem">{unit}{' · ' + note if note else ''}</div>
</div>"""
        else:
            before = float(m["before"]); after = float(m["after"])
            low    = m["lower_better"]
            mx     = max(before, after) or 1
            bar_b  = min(100, int(before / mx * 100))
            bar_a  = min(100, int(after  / mx * 100))
            pct_imp = (before - after if low else after - before) / max(abs(before), 0.001) * 100
            arrow  = "↓" if low else "↑"
            good   = pct_imp > 0
            col    = _GREEN if good else (_RED if pct_imp < -5 else _TEXT3)

            # For "% masked" the "improvement" framing is different — just show value
            if before == 0.0 and not low:
                delta_txt = f"{after:g} {unit}"
            else:
                delta_txt = f"{arrow} {abs(pct_imp):.0f}% improvement"

            cards += f"""
<div class="imp-card">
  <div class="imp-label">{label}</div>
  <div class="imp-bars">
    <div class="imp-bar-row">
      <span class="imp-tag before-tag">Before</span>
      <div class="imp-bar-track"><div class="imp-bar-fill" style="width:{bar_b}%;background:{_BORDER_ST}"></div></div>
      <span class="imp-val before-val">{before:g} <small>{unit}</small></span>
    </div>
    <div class="imp-bar-row">
      <span class="imp-tag after-tag">After</span>
      <div class="imp-bar-track"><div class="imp-bar-fill" style="width:{bar_a}%;background:{col}"></div></div>
      <span class="imp-val after-val" style="color:{col}">{after:g} <small>{unit}</small></span>
    </div>
  </div>
  <div class="imp-delta" style="color:{col}">{_e(delta_txt)}{' · ' + note if note else ''}</div>
</div>"""

    return f"""
<section class="card" id="improvement">
  <div class="card-head"><h2>Pipeline Improvement</h2><span class="head-rule"></span>
    <span class="head-tag">Derived from pipeline outputs</span></div>
  <div class="imp-cards-col">{cards}</div>
</section>"""


# ---------------------------------------------------------------------------
# Performance chart
# ---------------------------------------------------------------------------

def _sec_performance(ordered_steps: list, total_elapsed: float) -> str:
    if total_elapsed <= 0:
        return ""
    rows   = [(n, s) for n, s in ordered_steps]
    n      = len(rows)
    bar_h  = 26; gap = 8; pad_l = 210; pad_r = 90; pad_t = 16; pad_b = 22
    chart_w = 820; bar_w = chart_w - pad_l - pad_r
    chart_h = pad_t + n * (bar_h + gap) + pad_b
    grid = ""
    for j in range(6):
        gx = pad_l + int(j / 5 * bar_w); pct = int(j / 5 * 100)
        grid += (f'<line x1="{gx}" y1="{pad_t}" x2="{gx}" y2="{chart_h-pad_b+4}" '
                 f'stroke="{_BORDER}" stroke-width="1"/>'
                 f'<text x="{gx}" y="{chart_h-5}" text-anchor="middle" '
                 f'font-size="9" fill="{_TEXT3}" font-family="\'DM Mono\',monospace">{pct}%</text>')
    bars = ""
    for i, (name, step) in enumerate(rows):
        el = step.get("elapsed") or 0
        frac = el / total_elapsed if total_elapsed else 0
        w    = max(3, int(frac * bar_w))
        y    = pad_t + i * (bar_h + gap)
        col  = _STEP_COLORS.get(name, _TEXT3)
        ts   = f"{el:.1f}s" if el else "skipped"
        op   = "0.3" if step.get("status") == "skipped" else "1"
        st   = step.get("status", "")
        tick = "✓" if st == "success" else ("✗" if st == "failed" else "↷")
        tc   = _GREEN if st == "success" else (_RED if st == "failed" else _AMBER)
        label = name.replace("_", " ").title()
        bars += (f'<text x="{pad_l-10}" y="{y+bar_h//2+4}" text-anchor="end" font-size="11" '
                 f'fill="{_TEXT2}" font-family="\'DM Sans\',sans-serif">{_e(label)}</text>'
                 f'<rect x="{pad_l}" y="{y}" width="{w}" height="{bar_h}" fill="{col}" rx="2" opacity="{op}"/>'
                 f'<text x="{pad_l+w+8}" y="{y+bar_h//2+4}" font-size="10" fill="{_TEXT2}" '
                 f'font-family="\'DM Mono\',monospace">{_e(ts)}</text>'
                 f'<text x="{pad_l-26}" y="{y+bar_h//2+4}" font-size="11" fill="{tc}">{tick}</text>')
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 {chart_w} {chart_h}" '
           f'style="max-width:{chart_w}px;display:block">{grid}{bars}</svg>')
    bands_per_min = 6 / (total_elapsed / 60) if total_elapsed else 0
    n_ok = len([s for _, s in ordered_steps if s.get("status") == "success"])
    return f"""
<section class="card" id="performance">
  <div class="card-head"><h2>Processing Performance</h2><span class="head-rule"></span></div>
  <div class="perf-layout">
    <div class="chart-wrap">{svg}</div>
    <div class="perf-stats">
      <div class="pstat"><span class="pstat-n">{total_elapsed:.0f}s</span><span class="pstat-l">Total runtime</span></div>
      <div class="pstat"><span class="pstat-n">{bands_per_min:.1f}</span><span class="pstat-l">Bands / minute</span></div>
      <div class="pstat"><span class="pstat-n">{n_ok}/{len(ordered_steps)}</span><span class="pstat-l">Steps succeeded</span></div>
      <div class="pstat"><span class="pstat-n">10 m</span><span class="pstat-l">Output resolution</span></div>
    </div>
  </div>
  <p class="note">Bar width proportional to share of total wall-clock time.</p>
</section>"""


# ---------------------------------------------------------------------------
# Step results table
# ---------------------------------------------------------------------------

def _sec_steps(ordered_steps: list) -> str:
    max_el = max((s.get("elapsed") or 0) for _, s in ordered_steps) or 1
    rows = ""
    for i, (name, step) in enumerate(ordered_steps):
        st   = step.get("status", "—")
        icon, col, bg = _STATUS.get(st, ("?", _TEXT2, _BORDER))
        el   = step.get("elapsed")
        el_s = f"{el:.2f}s" if el is not None else "—"
        n_out = len(step.get("outputs", []))
        err  = step.get("error") or ""
        label = name.replace("_", " ").title()
        bar_pct = int((el or 0) / max_el * 100)
        err_cell = (f'<span class="err-txt" title="{_e(err)}">{_e(err[:80])}{"…" if len(err)>80 else ""}</span>'
                    if err else '<span class="dash">—</span>')
        rows += f"""
<tr class="step-tr" style="animation-delay:{i*60}ms">
  <td class="sname">{_e(label)}</td>
  <td><span class="sbadge" style="background:{bg};color:{col}">{icon} {_e(st)}</span></td>
  <td><div class="el-wrap"><div class="el-bar" style="width:{bar_pct}%;background:{_TEXT3}"></div><code class="el-num">{el_s}</code></div></td>
  <td class="tc">{n_out}</td>
  <td class="errcol">{err_cell}</td>
</tr>"""
    return f"""
<section class="card" id="steps">
  <div class="card-head"><h2>Step-by-Step Results</h2><span class="head-rule"></span></div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead><tr><th>Step</th><th>Status</th><th>Elapsed</th><th class="tc">Outputs</th><th>Error</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"""


# ---------------------------------------------------------------------------
# Valid pixel mask
# ---------------------------------------------------------------------------

_MASK_CLASSES = {
    0:  ("No data",  "#d0cdc8"),
    1:  ("Clear",    "#2d6a4f"),
    2:  ("Water",    "#4a6fa8"),
    4:  ("Cloud",    "#8b2020"),
    8:  ("Shadow",   "#6b6860"),
    16: ("Snow/Ice", "#a8cce0"),
    32: ("Other",    "#ccc8c0"),
}


def _hex_to_rgb(h):
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _find_mask_tif(product_out_dir):
    env_path = os.environ.get("MASK_TIF")
    if env_path and Path(env_path).exists():
        return Path(env_path)
    candidates = (sorted((product_out_dir/"mask").glob("*_VALID_PIXEL_MASK.TIF"))
                  + sorted((product_out_dir/"mask").glob("*_VALID_PIXEL_MASK.tif"))
                  + sorted(product_out_dir.glob("*_VALID_PIXEL_MASK.TIF")))
    return next((c for c in candidates if c.exists()), None)


def _render_mask_thumbnail(mask_tif, thumb_w=680):
    try:
        import numpy as np, rasterio
        from rasterio.warp import reproject, Resampling
        from rasterio.transform import from_bounds
        with rasterio.open(mask_tif) as src:
            arr = src.read(1); nodata = src.nodata; h, w = arr.shape
        lut_size = max(256, int(arr.max()) + 2)
        lut      = np.zeros((lut_size, 4), dtype=np.uint8)
        for val, (_, hex_col) in _MASK_CLASSES.items():
            if val < lut_size:
                r, g, b = _hex_to_rgb(hex_col)
                lut[val] = [r, g, b, 220]
        lut[0] = [0, 0, 0, 0]
        if nodata is not None:
            nd = int(nodata)
            if 0 <= nd < lut_size: lut[nd] = [0, 0, 0, 0]
        out_h = max(1, int(h * thumb_w / w))
        dst   = np.zeros((out_h, thumb_w), dtype=arr.dtype)
        reproject(source=arr, destination=dst,
                  src_transform=from_bounds(0,0,w,h,w,h), src_crs="EPSG:4326",
                  dst_transform=from_bounds(0,0,w,h,thumb_w,out_h), dst_crs="EPSG:4326",
                  resampling=Resampling.nearest)
        flat = np.clip(dst.ravel().astype(np.int64), 0, lut_size-1)
        return _array_to_base64_png(lut[flat].reshape(out_h, thumb_w, 4))
    except Exception as exc:
        print(f"[report] Mask thumbnail failed: {exc}")
        return None


def _sec_mask(product_out_dir: Path) -> str:
    meta_path = product_out_dir / "fusion" / "metadata.json"
    vf = None; flags = {}
    if meta_path.exists():
        try:
            meta  = json.loads(meta_path.read_text())
            vpm   = meta.get("valid_pixel_mask", {})
            flags = vpm.get("flags", {})
            vf    = vpm.get("valid_fraction")
        except Exception: pass

    hcol = _GREEN if (vf or 0) >= 0.3 else (_AMBER if (vf or 0) >= 0.1 else _RED)
    htxt = ("Good coverage" if (vf or 0) >= 0.3
            else "Marginal coverage" if (vf or 0) >= 0.1
            else "Heavily obscured" if vf is not None else "")

    mask_tif = _find_mask_tif(product_out_dir)
    mask_b64 = _render_mask_thumbnail(mask_tif, 680) if mask_tif else None
    mask_name = mask_tif.name if mask_tif else ""

    palette = [("clear","#2d6a4f","Clear"),("cloud","#8b2020","Cloud"),
               ("shadow",_TEXT2,"Shadow"),("water","#4a6fa8","Water"),("other",_BORDER_ST,"Other")]

    donut_html = ""; legend_html = ""
    if vf is not None and flags:
        slices = [(lbl, col, flags.get(k, {}).get("count", 0)) for k, col, lbl in palette]
        total_px = sum(c for _, _, c in slices) or 1
        # Use the clear pixel fraction from flags for the donut center so it
        # matches the legend percentage — valid_fraction also excludes water
        # which makes it differ from the "Clear" legend entry.
        clear_count = flags.get("clear", {}).get("count", 0)
        clear_pct_str = f"{clear_count / total_px:.0%}" if total_px else "—"
        donut_html  = _donut([(l, c, n) for l, c, n in slices if n > 0],
                             total_px, r=60, hole=38, mid=clear_pct_str, sub="clear")
        legend_html = "".join(
            f'<div class="leg-row"><span class="leg-dot" style="background:{col}"></span>'
            f'<span class="leg-lbl">{_e(lbl)}</span>'
            f'<span class="leg-pct">{cnt/total_px:.1%}</span></div>'
            for lbl, col, cnt in slices if cnt > 0)
    else:
        legend_html = "".join(
            f'<div class="leg-row"><span class="leg-dot" style="background:{col}"></span>'
            f'<span class="leg-lbl">{_e(lbl)}</span></div>'
            for _, (lbl, col) in _MASK_CLASSES.items() if lbl != "No data")

    mask_img_html = (
        f'<div class="mask-img-wrap"><img class="mask-img" src="{mask_b64}" alt="Valid Pixel Mask">'
        f'<div class="mask-img-caption">{_e(mask_name)}</div></div>'
        if mask_b64 else '<p class="empty">Mask TIF not found.</p>')

    status_tag = f'<span class="head-tag" style="color:{hcol}">{_e(htxt)}</span>' if htxt else ""
    if vf is not None and flags:
        clear_count = flags.get("clear", {}).get("count", 0)
        total_px_for_stat = sum(flags.get(k, {}).get("count", 0)
                                for k in ("clear", "cloud", "shadow", "water", "other")) or 1
        clear_pct = clear_count / total_px_for_stat
        obscured_pct = 1 - clear_pct
        stats_row = (
            f'<div class="mask-stats-row">'
            f'<div class="mask-stat"><span class="mask-stat-n" style="color:{hcol}">{clear_pct:.1%}</span>'
            f'<span class="mask-stat-l">Clear fraction</span></div>'
            f'<div class="mask-stat"><span class="mask-stat-n">{obscured_pct:.1%}</span>'
            f'<span class="mask-stat-l">Obscured</span></div></div>'
            f'<p class="note" style="margin-top:.4rem">Valid fraction (excl. water): {vf:.1%}</p>'
        )
    else:
        stats_row = ""
    donut_block = f'<div class="mask-donut">{donut_html}</div>' if donut_html else ""

    return f"""
<section class="card" id="mask">
  <div class="card-head"><h2>Valid Pixel Mask</h2><span class="head-rule"></span>{status_tag}</div>
  {mask_img_html}
  <div class="mask-bottom">
    {donut_block}
    <div class="mask-right">{stats_row}<div class="legend-block">{legend_html}</div></div>
  </div>
</section>"""


def _donut(slices, total, r=70, hole=44, mid="", sub=""):
    cx = cy = r + 12; sz = (r + 12) * 2; start = -math.pi / 2; paths = ""
    for label, col, cnt in slices:
        if not cnt: continue
        ang = 2 * math.pi * cnt / total; end = start + ang
        x1o = cx + r    * math.cos(start); y1o = cy + r    * math.sin(start)
        x2o = cx + r    * math.cos(end);   y2o = cy + r    * math.sin(end)
        x1i = cx + hole * math.cos(end);   y1i = cy + hole * math.sin(end)
        x2i = cx + hole * math.cos(start); y2i = cy + hole * math.sin(start)
        lg  = 1 if ang > math.pi else 0
        d   = (f"M{x1o:.2f},{y1o:.2f} A{r},{r} 0 {lg},1 {x2o:.2f},{y2o:.2f} "
               f"L{x1i:.2f},{y1i:.2f} A{hole},{hole} 0 {lg},0 {x2i:.2f},{y2i:.2f} Z")
        paths += (f'<path d="{d}" fill="{col}" stroke="{_SURFACE}" stroke-width="2" opacity="0.75">'
                  f'<title>{_e(label)}: {cnt/total:.1%}</title></path>')
        start = end
    centre = ""
    if mid:
        centre = (f'<text x="{cx}" y="{cy-4}" text-anchor="middle" font-family="\'DM Mono\',monospace" '
                  f'font-size="18" font-weight="500" fill="{_TEXT}">{_e(mid)}</text>'
                  f'<text x="{cx}" y="{cy+13}" text-anchor="middle" font-family="\'DM Sans\',sans-serif" '
                  f'font-size="10" fill="{_TEXT3}">{_e(sub)}</text>')
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{sz}" height="{sz}" '
            f'viewBox="0 0 {sz} {sz}">{paths}{centre}</svg>')


# ---------------------------------------------------------------------------
# BRDF delta table
# ---------------------------------------------------------------------------

def _compute_brdf_deltas(product_out_dir: Path) -> list[dict]:
    nbar_dir = product_out_dir / "nbar" / "landsat"
    sbaf_dir = product_out_dir / "sbaf"
    if not nbar_dir.exists() or not sbaf_dir.exists():
        return []
    BANDS = [("Blue","*_SBAF_B2.TIF","#4a8db5"),("Green","*_SBAF_B3.TIF","#4a7c59"),
             ("Red","*_SBAF_B4.TIF","#8b2020"),("NIR","*_SBAF_B5.TIF","#7b6fa0"),
             ("SWIR1","*_SBAF_B6.TIF","#6b6860"),("SWIR2","*_SBAF_B7.TIF","#a09d97")]
    results = []
    for logi, pat, col in BANDS:
        sbaf_f = list(sbaf_dir.glob(pat))
        nbar_f = nbar_dir / f"NBAR_{logi}.tif"
        if not sbaf_f or not nbar_f.exists(): continue
        try:
            import numpy as np, rasterio
            with rasterio.open(sbaf_f[0]) as src:
                pre = src.read(1).astype(np.float32); nd = src.nodata
            with rasterio.open(nbar_f) as src:
                post = src.read(1).astype(np.float32)
            nd_v  = float(nd) if nd is not None else -9999.0
            valid = (pre != nd_v) & (post != nd_v) & (pre != 0)
            if not valid.any(): continue
            pv = pre[valid]; qv = post[valid]
            if len(pv) > 500_000:
                rng = np.random.default_rng(
                    seed=int(hashlib.sha256(logi.encode()).hexdigest()[:8], 16) % 2**31)
                idx = rng.integers(0, len(pv), 500_000)
                pv = pv[idx]; qv = qv[idx]
            pre_m = float(pv.mean()); post_m = float(qv.mean())
            delta = post_m - pre_m;  pct = delta / (abs(pre_m) + 1e-6) * 100
            results.append({"band": logi, "col": col, "pre": pre_m,
                            "post": post_m, "delta": delta, "pct": pct})
        except Exception: continue
    return results


def _sec_brdf_table(brdf_deltas: list[dict]) -> str:
    if not brdf_deltas:
        return _empty_card("brdf", "BRDF Correction", "No band data available.")
    max_abs_pct = max(abs(d["pct"]) for d in brdf_deltas) or 1
    rows = ""
    for d in brdf_deltas:
        pct     = d["pct"]
        col_bar = _TEXT3 if abs(pct) < 2 else (_AMBER if abs(pct) < 5 else _RED)
        bar_w   = min(100, abs(pct) / max_abs_pct * 100)
        d_sign  = "+" if d["delta"] >= 0 else ""; p_sign = "+" if pct >= 0 else ""
        rows += f"""
<tr>
  <td><span class="bdot" style="background:{d['col']}"></span>{_e(d['band'])}</td>
  <td class="mono">{d['pre']:.4f}</td>
  <td class="mono">{d['post']:.4f}</td>
  <td class="mono" style="color:{_GREEN if d['delta']>=0 else _RED}">{d_sign}{d['delta']:.4f}</td>
  <td><div class="pbar-wrap"><div class="pbar-fill" style="width:{bar_w:.0f}%;background:{col_bar}"></div>
  <code class="pbar-lbl" style="color:{col_bar}">{p_sign}{pct:.2f}%</code></div></td>
</tr>"""
    return f"""
<section class="card" id="brdf">
  <div class="card-head"><h2>BRDF Normalisation Effect</h2><span class="head-rule"></span>
    <span class="head-tag">Pre-NBAR vs Post-NBAR mean reflectance</span></div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead><tr><th>Band</th><th>Pre-NBAR μ</th><th>Post-NBAR μ</th><th>Δ abs</th><th>Δ relative</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <p class="note">&lt;2% — nominal &nbsp;·&nbsp; 2–5% — moderate &nbsp;·&nbsp; &gt;5% — review recommended</p>
</section>"""


# ---------------------------------------------------------------------------
# Fusion file inventory
# ---------------------------------------------------------------------------

def _sec_fusion(product_out_dir: Path) -> str:
    fusion_dir = product_out_dir / "fusion"
    if not fusion_dir.exists():
        return _empty_card("fusion", "Fusion Output", "Fusion directory not found.")
    candidates = (sorted(fusion_dir.glob("*_10m.TIF"))
                  + [fusion_dir / "FUSION_VALIDITY_MASK.TIF"]
                  + sorted(fusion_dir.glob("*.json")))
    seen, files = set(), []
    for f in candidates:
        if f not in seen and f.exists():
            seen.add(f); files.append(f)
    if not files:
        return _empty_card("fusion", "Fusion Output", "No output files found.")
    total_mb = sum(f.stat().st_size for f in files) / 1e6
    max_mb   = max(f.stat().st_size for f in files) / 1e6
    rows = ""
    for fp in files:
        mb = fp.stat().st_size / 1e6; bw = max(2, int(mb / max_mb * 100))
        rows += (f'<tr><td><code class="fname">{_e(fp.name)}</code></td>'
                 f'<td><div class="szbar-wrap"><div class="szbar" style="width:{bw}%"></div>'
                 f'<code class="sznum">{mb:.2f} MB</code></div></td></tr>')
    return f"""
<section class="card" id="fusion">
  <div class="card-head"><h2>Fusion Output Files</h2><span class="head-rule"></span>
    <span class="head-tag">{len(files)} files · {total_mb:.1f} MB</span></div>
  <div class="tbl-wrap">
    <table class="dtbl"><thead><tr><th>File</th><th>Size</th></tr></thead><tbody>{rows}</tbody></table>
  </div>
</section>"""


# ---------------------------------------------------------------------------
# Validation  ← FIXED: shows actual issues list
# ---------------------------------------------------------------------------

def _sec_validation(ordered_steps: list) -> str:
    val    = dict(ordered_steps).get("validation", {})
    status = val.get("status", "")
    if not status:
        return _empty_card("validation", "Validation", "Validation step was not run.")

    icon, col, bg = _STATUS.get(status, ("?", _TEXT2, _BORDER))
    err     = val.get("error") or ""
    elapsed = val.get("elapsed")
    el_s    = f"{elapsed:.2f}s" if elapsed is not None else "—"

    # Read validation results — new runs write to step_return, old runs used config_snapshot
    snap        = val.get("step_return") or val.get("config_snapshot", {})
    issues      = snap.get("validation_issues", [])
    n_warn      = snap.get("validation_warnings", 0)
    n_fail      = snap.get("validation_failures", 0)
    val_passed  = snap.get("validation_passed")

    err_blk = (f'<div class="err-blk"><pre>{_e(err)}</pre></div>' if err else "")

    # Summary badges
    summary = ""
    if val_passed is not None:
        res_col  = _GREEN if val_passed else _RED
        res_txt  = "Passed" if val_passed else "Failed"
        res_bg   = _GREEN_BG if val_passed else _RED_BG
        summary  = (f'<span class="sbadge" style="background:{res_bg};color:{res_col}">'
                    f'{"✓" if val_passed else "✗"} {res_txt}</span>&nbsp;')
    if n_warn:
        summary += (f'<span class="sbadge" style="background:{_AMBER_BG};color:{_AMBER}">'
                    f'⚠ {n_warn} warning{"s" if n_warn!=1 else ""}</span>&nbsp;')
    if n_fail:
        summary += (f'<span class="sbadge" style="background:{_RED_BG};color:{_RED}">'
                    f'✗ {n_fail} failure{"s" if n_fail!=1 else ""}</span>')

    # Issues table
    issues_html = ""
    if issues:
        rows = ""
        for lvl, msg in issues:
            lcol = _AMBER if lvl == "WARN" else _RED
            lbg  = _AMBER_BG if lvl == "WARN" else _RED_BG
            rows += (f'<tr><td><span class="sbadge" style="background:{lbg};color:{lcol}">'
                     f'{_e(lvl)}</span></td>'
                     f'<td style="font-size:.78rem;color:{_TEXT2}">{_e(msg)}</td></tr>')
        issues_html = f"""
<div class="tbl-wrap" style="margin-top:.8rem">
  <table class="dtbl">
    <thead><tr><th>Level</th><th>Issue</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""

    return f"""
<section class="card" id="validation">
  <div class="card-head"><h2>Validation</h2><span class="head-rule"></span></div>
  <div class="val-row">
    <span class="sbadge large" style="background:{bg};color:{col}">{icon} {_e(status)}</span>
    <span class="val-el">Runtime: {el_s}</span>
  </div>
  <div style="margin:.4rem 0 .6rem">{summary}</div>
  {err_blk}
  {issues_html}
  <p class="note">WARN = data may be unreliable · FAIL = output should not be used</p>
</section>"""


# ---------------------------------------------------------------------------
# Reproducibility fingerprint
# ---------------------------------------------------------------------------

def _sec_fingerprint(version, cfg_hash, input_paths):
    ip_rows = "".join(
        f'<div class="fp-row"><span class="fpk">{_e(k)}</span>'
        f'<code class="fpv">{_e(str(v))}</code></div>'
        for k, v in input_paths.items())
    return f"""
<section class="card" id="fingerprint">
  <div class="card-head"><h2>Reproducibility Fingerprint</h2><span class="head-rule"></span></div>
  <div class="fp-grid">
    <div class="fp-cell"><div class="fp-lbl">Pipeline version</div><code class="fp-hash">{_e(version)}</code></div>
    <div class="fp-cell"><div class="fp-lbl">Config SHA-256</div><code class="fp-hash brk">{_e(cfg_hash)}</code></div>
  </div>
  {ip_rows}
  <p class="note">Identical pipeline version + config hash + inputs → bit-for-bit reproducible outputs.</p>
</section>"""


# ---------------------------------------------------------------------------
# Config snapshot
# ---------------------------------------------------------------------------

def _sec_config(ordered_steps: list) -> str:
    snaps = {n: s.get("config_snapshot", {}) for n, s in ordered_steps if s.get("config_snapshot")}
    if not snaps: return ""
    return f"""
<section class="card" id="config">
  <div class="card-head"><h2>Config Snapshots</h2><span class="head-rule"></span></div>
  <details class="cfg-details">
    <summary>Expand full configuration</summary>
    <pre class="cfg-pre">{_e(json.dumps(snaps, indent=2, default=str))}</pre>
  </details>
</section>"""


# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------

def _sec_hero(product_id, started_at, finished_at, version, cfg_hash,
              input_paths, n_ok, n_fail, n_skip, elapsed):
    status_word  = "FAILED" if n_fail else "COMPLETE"
    status_color = _RED if n_fail else _GREEN
    short_id     = Path(product_id).name
    meta_rows    = "".join(
        f'<tr><td class="mk">{_e(k)}</td><td class="mv"><code>{_e(str(v))}</code></td></tr>'
        for k, v in input_paths.items())
    fail_color = _RED if n_fail else _TEXT3
    return f"""
<section class="hero-section">
  <div class="hero-inner">
    <div class="hero-left">
      <p class="overline">sen2like · Processing Report</p>
      <h1 class="hero-title">{_e(short_id)}</h1>
      <span class="status-pill" style="color:{status_color};border-color:{status_color}">
        <span class="status-dot" style="background:{status_color}"></span>{status_word}
      </span>
      <div class="hero-meta-table">
        <table>
          <tr><td class="mk">Started</td><td class="mv">{_e(started_at)}</td></tr>
          <tr><td class="mk">Finished</td><td class="mv">{_e(finished_at)}</td></tr>
          <tr><td class="mk">Version</td><td class="mv"><code>{_e(version)}</code></td></tr>
          <tr><td class="mk">Config hash</td><td class="mv"><code>{_e(cfg_hash[:14])}…</code></td></tr>
          {meta_rows}
        </table>
      </div>
    </div>
  </div>
  <div class="kpi-strip">
    <div class="kpi-cell"><span class="kpi-n" style="color:{_GREEN}">{n_ok}</span><span class="kpi-l">Steps complete</span></div>
    <div class="kpi-sep"></div>
    <div class="kpi-cell"><span class="kpi-n" style="color:{fail_color}">{n_fail}</span><span class="kpi-l">Failures</span></div>
    <div class="kpi-sep"></div>
    <div class="kpi-cell"><span class="kpi-n">{n_skip}</span><span class="kpi-l">Skipped</span></div>
    <div class="kpi-sep"></div>
    <div class="kpi-cell"><span class="kpi-n">{elapsed:.0f}s</span><span class="kpi-l">Wall-clock</span></div>
  </div>
</section>"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_card(sid, title, msg):
    return f"""
<section class="card" id="{sid}">
  <div class="card-head"><h2>{_e(title)}</h2><span class="head-rule"></span></div>
  <p class="empty">{_e(msg)}</p>
</section>"""


def _e(s):
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate_report(product_out_dir: str | Path) -> Path:
    product_out_dir = Path(product_out_dir)
    manifest_path   = product_out_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in {product_out_dir}.")
    manifest = json.loads(manifest_path.read_text())
    html     = _render(manifest, product_out_dir)
    report   = product_out_dir / "report.html"
    report.write_text(html, encoding="utf-8")
    print(f"[report] Written → {report}")
    return report


# ---------------------------------------------------------------------------
# Top-level render
# ---------------------------------------------------------------------------

def _render(manifest: dict, product_out_dir: Path) -> str:
    product_id  = manifest.get("product_id", "unknown")
    started_at  = manifest.get("started_at",  "—")
    finished_at = manifest.get("finished_at", "—")
    version     = manifest.get("pipeline_version", "unknown")
    cfg_hash    = manifest.get("config_hash", "—")
    input_paths = manifest.get("input_paths", {})
    steps       = manifest.get("steps", {})

    # Build ordered step list — canonical order, then anything extra
    ordered_steps = [(n, steps[n]) for n in _FULL_STEP_ORDER if n in steps]
    ordered_steps += [(n, s) for n, s in steps.items() if n not in _FULL_STEP_ORDER]

    total_elapsed = sum((s.get("elapsed") or 0) for _, s in ordered_steps)
    n_success = sum(1 for _, s in ordered_steps if s.get("status") == "success")
    n_failed  = sum(1 for _, s in ordered_steps if s.get("status") == "failed")
    n_skipped = sum(1 for _, s in ordered_steps if s.get("status") == "skipped")

    brdf_deltas = _compute_brdf_deltas(product_out_dir)
    sys_res     = _collect_system_resources()

    sections = "\n".join([
        _sec_hero(product_id, started_at, finished_at, version,
                  cfg_hash, input_paths, n_success, n_failed, n_skipped, total_elapsed),
        _sec_system_resources(sys_res),
        _sec_routing(manifest),                                          # NEW
        _sec_imagery(product_out_dir, input_paths),
        _sec_improvement(product_out_dir, brdf_deltas, ordered_steps),  # FIXED
        _sec_performance(ordered_steps, total_elapsed),
        _sec_steps(ordered_steps),
        _sec_mask(product_out_dir),
        _sec_brdf_table(brdf_deltas),
        _sec_fusion(product_out_dir),
        _sec_validation(ordered_steps),                                  # FIXED
        _sec_fingerprint(version, cfg_hash, input_paths),
        _sec_config(ordered_steps),
    ])

    return _page(product_id, sections)


# ---------------------------------------------------------------------------
# HTML page shell
# ---------------------------------------------------------------------------

def _page(title: str, body: str) -> str:
    nav_items = [
        ("system",      "System Resources"),
        ("routing",     "Tile Routing"),        # NEW
        ("imagery",     "Before / After"),
        ("improvement", "Improvement"),
        ("performance", "Performance"),
        ("steps",       "Steps"),
        ("mask",        "Pixel Mask"),
        ("brdf",        "BRDF"),
        ("fusion",      "Fusion"),
        ("validation",  "Validation"),
        ("fingerprint", "Fingerprint"),
        ("config",      "Config"),
    ]
    nav = "".join(f'<a class="nl" href="#{i}">{_e(l)}</a>' for i, l in nav_items)

    viewer_js = r"""
(function(){
  var wrap=document.getElementById('viewer'),after=document.getElementById('imgAfter'),
      line=document.getElementById('divLine'),handle=document.getElementById('divHandle');
  if(!wrap||!after) return;
  var dragging=false;
  function setSplit(cx){
    var rect=wrap.getBoundingClientRect(),pct=Math.max(0,Math.min(100,(cx-rect.left)/rect.width*100));
    after.style.clipPath='inset(0 '+(100-pct).toFixed(2)+'% 0 0)';
    var ps=pct.toFixed(2)+'%';
    line.style.left=ps; handle.style.left='calc('+ps+' - 13px)';
  }
  wrap.addEventListener('mousedown',function(e){dragging=true;setSplit(e.clientX);e.preventDefault();});
  wrap.addEventListener('mousemove',function(e){if(dragging)setSplit(e.clientX);});
  window.addEventListener('mouseup',function(){dragging=false;});
  wrap.addEventListener('touchstart',function(e){dragging=true;setSplit(e.touches[0].clientX);},{passive:true});
  wrap.addEventListener('touchmove',function(e){if(dragging)setSplit(e.touches[0].clientX);},{passive:true});
  window.addEventListener('touchend',function(){dragging=false;});
})();
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>sen2like Report — {_e(title)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:{_BG};--surface:{_SURFACE};--border:{_BORDER};--border-s:{_BORDER_ST};
  --text:{_TEXT};--text2:{_TEXT2};--text3:{_TEXT3};
  --green:{_GREEN};--green-bg:{_GREEN_BG};--amber:{_AMBER};--red:{_RED};
  --sans:'DM Sans',system-ui,sans-serif;--mono:'DM Mono','Courier New',monospace;
  --nav-w:220px;
}}
html{{scroll-behavior:smooth}}
body{{font-family:var(--sans);background:var(--bg);color:var(--text);display:flex;min-height:100vh;font-size:14px;line-height:1.6;font-weight:300;}}
.sidebar{{position:fixed;top:0;left:0;width:var(--nav-w);height:100vh;background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;padding:2rem 0 1.5rem;z-index:100;overflow-y:auto;}}
.sb-logo{{padding:0 1.5rem 1.5rem;border-bottom:1px solid var(--border);margin-bottom:1rem}}
.sb-name{{font-family:var(--mono);font-size:.85rem;font-weight:500;color:var(--text)}}
.sb-sub{{font-size:.68rem;color:var(--text3);margin-top:.2rem;letter-spacing:.08em;text-transform:uppercase}}
.nl{{display:block;padding:.38rem 1.5rem;font-size:.78rem;color:var(--text2);text-decoration:none;border-left:2px solid transparent;transition:all .12s;}}
.nl:hover,.nl.active{{color:var(--text);border-left-color:var(--text);background:rgba(0,0,0,.03)}}
.main{{margin-left:var(--nav-w);flex:1;padding:3rem 3.5rem;max-width:1080px}}
.hero-section{{margin-bottom:2rem;border-bottom:1px solid var(--border);padding-bottom:2rem}}
.overline{{font-family:var(--mono);font-size:.68rem;color:var(--text3);letter-spacing:.14em;text-transform:uppercase;margin-bottom:.6rem}}
.hero-title{{font-family:var(--mono);font-size:1rem;font-weight:500;color:var(--text);margin-bottom:.9rem;word-break:break-all;line-height:1.4}}
.status-pill{{display:inline-flex;align-items:center;gap:.4rem;font-family:var(--mono);font-size:.68rem;font-weight:500;letter-spacing:.08em;padding:.22rem .65rem;border:1px solid;border-radius:2px;margin-bottom:1.2rem;}}
.status-dot{{width:5px;height:5px;border-radius:50%}}
.hero-meta-table table{{border-collapse:collapse}}
.mk{{font-family:var(--mono);font-size:.7rem;color:var(--text3);padding:.15rem 1rem .15rem 0;white-space:nowrap;vertical-align:top}}
.mv{{font-family:var(--mono);font-size:.7rem;color:var(--text2);padding:.15rem 0;word-break:break-all}}
.kpi-strip{{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-top:1.5rem;background:var(--surface);}}
.kpi-cell{{padding:1rem .5rem;text-align:center;border-right:1px solid var(--border)}}
.kpi-cell:last-child{{border-right:none}}
.kpi-n{{display:block;font-family:var(--mono);font-size:1.5rem;font-weight:500;line-height:1;margin-bottom:.25rem;color:var(--text)}}
.kpi-l{{font-size:.65rem;color:var(--text3);letter-spacing:.07em;text-transform:uppercase;font-family:var(--mono)}}
.kpi-sep{{display:none}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:1.6rem 1.8rem;margin-bottom:1.6rem;animation:rise .35s ease both;}}
@keyframes rise{{from{{opacity:0;transform:translateY(10px)}}to{{opacity:1;transform:translateY(0)}}}}
.card-head{{display:flex;align-items:center;gap:.8rem;margin-bottom:1.2rem;padding-bottom:.7rem;border-bottom:1px solid var(--border);flex-wrap:wrap;}}
.card-head h2{{font-size:.85rem;font-weight:500;color:var(--text);flex-shrink:0}}
.head-rule{{flex:1;height:1px;background:var(--border)}}
.head-tag{{font-family:var(--mono);font-size:.68rem;color:var(--text3);white-space:nowrap}}
.res-grid{{display:flex;gap:0;align-items:stretch;border:1px solid var(--border);border-radius:4px;overflow:hidden;}}
.res-cell{{flex:1;display:flex;flex-direction:column;align-items:center;padding:1.2rem 1rem;gap:.6rem;}}
.res-divider{{width:1px;background:var(--border);flex-shrink:0}}
.res-gauge{{flex-shrink:0}}
.res-title{{font-family:var(--mono);font-size:.72rem;font-weight:500;color:var(--text);letter-spacing:.08em;text-transform:uppercase}}
.res-details{{display:flex;flex-direction:column;gap:.3rem;width:100%;}}
.res-detail{{display:flex;justify-content:space-between;align-items:center;font-size:.75rem;}}
.res-k{{color:var(--text3);font-family:var(--mono);font-size:.68rem}}
.res-v{{font-family:var(--mono);font-size:.75rem;font-weight:500;color:var(--text)}}
.route-class-row{{display:flex;align-items:center;gap:1rem;margin-bottom:1rem;flex-wrap:wrap;}}
.route-class-badge{{font-family:var(--mono);font-size:.8rem;font-weight:500;padding:.3rem .8rem;border-radius:3px;letter-spacing:.06em;}}
.route-class-desc{{font-size:.8rem;color:var(--text2);flex:1}}
.route-savings{{font-family:var(--mono);font-size:.72rem;color:var(--text3);white-space:nowrap}}
.route-pills{{display:flex;flex-wrap:wrap;gap:.45rem;margin-bottom:.4rem}}
.route-pill{{font-family:var(--mono);font-size:.68rem;padding:.22rem .6rem;border-radius:3px;white-space:nowrap;}}
.viewer-wrap{{position:relative;border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-bottom:.8rem;cursor:ew-resize;user-select:none;}}
.viewer-img-wrap{{position:relative;width:100%;}}
.viewer-img-before{{display:block;width:100%;height:auto;}}
.viewer-img-after{{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover;clip-path:inset(0 50% 0 0);}}
.viewer-label{{position:absolute;bottom:.7rem;font-family:var(--mono);font-size:.65rem;letter-spacing:.07em;text-transform:uppercase;padding:.18rem .45rem;background:rgba(255,255,255,.88);border-radius:2px;color:var(--text2);pointer-events:none;z-index:10;}}
.viewer-label.left{{left:.7rem}}.viewer-label.right{{right:.7rem;color:var(--green)}}
.divider-line{{position:absolute;top:0;bottom:0;width:2px;background:white;pointer-events:none;z-index:9}}
.divider-handle{{position:absolute;top:50%;transform:translateY(-50%);width:26px;height:26px;background:white;border-radius:50%;border:1px solid var(--border-s);display:flex;align-items:center;justify-content:center;pointer-events:none;z-index:10;}}
.divider-arrows{{font-size:9px;color:var(--text2);letter-spacing:-1px}}
.viewer-hint{{font-size:.68rem;color:var(--text3);font-family:var(--mono);text-align:center;margin-bottom:1.4rem}}
.imp-cards-col{{display:flex;flex-direction:column;gap:.75rem}}
.imp-card{{background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:.8rem .95rem}}
.imp-label{{font-size:.78rem;font-weight:500;color:var(--text);margin-bottom:.55rem}}
.imp-bars{{display:flex;flex-direction:column;gap:.28rem}}
.imp-bar-row{{display:flex;align-items:center;gap:.55rem}}
.imp-tag{{font-family:var(--mono);font-size:.6rem;letter-spacing:.05em;text-transform:uppercase;min-width:36px;color:var(--text3)}}
.after-tag{{color:var(--green);font-weight:500}}
.imp-bar-track{{flex:1;height:3px;background:var(--bg);border-radius:2px;overflow:hidden}}
.imp-bar-fill{{height:100%;border-radius:2px;transition:width 1s cubic-bezier(.4,0,.2,1)}}
.imp-val{{font-family:var(--mono);font-size:.7rem;min-width:90px;text-align:right;color:var(--text2)}}
.imp-val small{{font-size:.65rem;color:var(--text3)}}
.after-val{{font-weight:500}}
.imp-delta{{font-family:var(--mono);font-size:.65rem;color:var(--green);margin-top:.35rem}}
.perf-layout{{display:flex;gap:2rem;align-items:flex-start;flex-wrap:wrap}}
.chart-wrap{{flex:1;overflow-x:auto;min-width:260px}}
.perf-stats{{display:flex;flex-direction:column;gap:.6rem;min-width:120px}}
.pstat{{background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:.65rem .8rem;text-align:center}}
.pstat-n{{display:block;font-family:var(--mono);font-size:1.15rem;font-weight:500;color:var(--text);line-height:1.1}}
.pstat-l{{display:block;font-size:.62rem;color:var(--text3);text-transform:uppercase;letter-spacing:.07em;font-family:var(--mono);margin-top:.15rem}}
.tbl-wrap{{overflow-x:auto}}
.dtbl{{width:100%;border-collapse:collapse;font-size:.8rem}}
.dtbl th{{font-family:var(--mono);font-size:.62rem;letter-spacing:.09em;text-transform:uppercase;color:var(--text3);padding:.45rem .75rem;text-align:left;border-bottom:1px solid var(--border);}}
.dtbl td{{padding:.5rem .75rem;border-bottom:1px solid var(--border);vertical-align:middle;color:var(--text2)}}
.dtbl tr:last-child td{{border-bottom:none}}
.step-tr{{animation:rise .3s ease both}}
.sname{{color:var(--text);font-weight:400}}
.sbadge{{display:inline-flex;align-items:center;gap:.25rem;font-family:var(--mono);font-size:.65rem;font-weight:500;padding:.15rem .45rem;border-radius:2px;white-space:nowrap;}}
.sbadge.large{{font-size:.8rem;padding:.28rem .8rem}}
.tc{{text-align:center}}.mono{{font-family:var(--mono);font-size:.75rem}}.dash{{color:var(--border-s)}}
.el-wrap{{display:flex;align-items:center;gap:.45rem;min-width:100px}}
.el-bar{{height:3px;border-radius:2px;min-width:2px;opacity:.45;flex-shrink:0}}
.el-num{{font-family:var(--mono);font-size:.7rem;color:var(--text3)}}
.err-txt{{color:var(--red);font-size:.72rem;font-family:var(--mono)}}.errcol{{max-width:240px}}
.err-blk{{background:{_RED_BG};border:1px solid #e8b4b4;border-radius:4px;padding:.6rem .8rem;margin-bottom:.8rem}}
.err-blk pre{{font-family:var(--mono);font-size:.72rem;color:var(--red);white-space:pre-wrap;word-break:break-word}}
.legend-block{{display:flex;flex-direction:column;gap:.4rem}}
.leg-row{{display:flex;align-items:center;gap:.5rem;font-size:.78rem;color:var(--text2)}}
.leg-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0;opacity:.75}}.leg-lbl{{flex:1}}
.leg-pct{{font-family:var(--mono);font-size:.7rem;color:var(--text3);padding-left:1rem}}
.bdot{{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:.4rem;vertical-align:middle;opacity:.8}}
.pbar-wrap{{display:flex;align-items:center;gap:.5rem}}
.pbar-fill{{height:3px;border-radius:2px;min-width:2px;opacity:.6}}
.pbar-lbl{{font-family:var(--mono);font-size:.7rem}}
.fname{{font-size:.76rem}}
.szbar-wrap{{display:flex;align-items:center;gap:.5rem}}
.szbar{{height:3px;border-radius:2px;min-width:2px;opacity:.4;background:var(--text3)}}
.sznum{{font-family:var(--mono);font-size:.68rem;color:var(--text3)}}
.val-row{{display:flex;align-items:center;gap:1rem;margin-bottom:.7rem}}
.val-el{{font-family:var(--mono);font-size:.75rem;color:var(--text3)}}
.fp-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:.65rem;margin-bottom:.7rem}}
.fp-cell{{background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:.7rem .85rem}}
.fp-lbl{{font-family:var(--mono);font-size:.62rem;letter-spacing:.09em;text-transform:uppercase;color:var(--text3);margin-bottom:.3rem}}
.fp-hash{{font-family:var(--mono);font-size:.7rem;color:var(--text);word-break:break-all}}.brk{{word-break:break-all}}
.fp-row{{display:flex;gap:.7rem;padding:.32rem .65rem;border-bottom:1px solid var(--border)}}
.fp-row:last-child{{border-bottom:none}}
.fpk{{font-family:var(--mono);color:var(--text3);min-width:100px;font-size:.65rem}}
.fpv{{font-family:var(--mono);color:var(--text2);word-break:break-all;font-size:.68rem}}
.cfg-details summary{{cursor:pointer;font-size:.78rem;color:var(--text2);padding:.25rem 0;outline:none}}
.cfg-pre{{background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:.85rem;font-family:var(--mono);font-size:.7rem;color:var(--text2);overflow-x:auto;max-height:360px;margin-top:.55rem;white-space:pre;}}
.mask-img-wrap{{position:relative;border:1px solid var(--border);border-radius:4px;overflow:hidden;margin-bottom:1.1rem;background:var(--bg);text-align:center;}}
.mask-img{{display:block;width:100%;height:auto;max-height:340px;object-fit:contain;object-position:center;image-rendering:pixelated;}}
.mask-img-caption{{font-family:var(--mono);font-size:.62rem;color:var(--text3);padding:.3rem .6rem;text-align:left;border-top:1px solid var(--border);letter-spacing:.04em;}}
.mask-bottom{{display:flex;gap:1.5rem;align-items:flex-start;flex-wrap:wrap;margin-top:.2rem}}
.mask-donut{{flex-shrink:0}}
.mask-right{{display:flex;flex-direction:column;gap:.9rem;flex:1}}
.mask-stats-row{{display:flex;gap:1rem}}
.mask-stat{{background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:.55rem .85rem;text-align:center;min-width:90px;}}
.mask-stat-n{{display:block;font-family:var(--mono);font-size:1.1rem;font-weight:500;line-height:1.1;color:var(--text)}}
.mask-stat-l{{display:block;font-size:.6rem;color:var(--text3);text-transform:uppercase;letter-spacing:.07em;font-family:var(--mono);margin-top:.1rem}}
code{{font-family:var(--mono);font-size:.82em;color:var(--text2)}}
.note{{font-size:.7rem;color:var(--text3);margin-top:.65rem;line-height:1.6;padding-left:.55rem;border-left:1.5px solid var(--border);font-style:italic;}}
.empty{{font-size:.8rem;color:var(--text3);font-style:italic;padding:.25rem 0}}
::-webkit-scrollbar{{width:4px;height:4px}}
::-webkit-scrollbar-track{{background:transparent}}
::-webkit-scrollbar-thumb{{background:var(--border);border-radius:2px}}
@media(max-width:660px){{.sidebar{{display:none}}.main{{margin-left:0;padding:1.2rem 1rem}}.kpi-strip{{grid-template-columns:repeat(2,1fr)}}.res-grid{{flex-direction:column}}}}
</style>
</head>
<body>
<nav class="sidebar">
  <div class="sb-logo"><div class="sb-name">sen2like</div><div class="sb-sub">Pipeline Report</div></div>
  {nav}
</nav>
<main class="main">{body}</main>
<script>
(function(){{
  document.querySelectorAll('.card').forEach(function(el,i){{el.style.animationDelay=(i*55)+'ms';}});
  var links=Array.from(document.querySelectorAll('.nl'));
  var ids=links.map(function(l){{return l.getAttribute('href').slice(1)}});
  var secs=ids.map(function(id){{return document.getElementById(id)}});
  var io=new IntersectionObserver(function(entries){{
    entries.forEach(function(e){{
      if(e.isIntersecting){{
        links.forEach(function(l){{l.classList.remove('active')}});
        var i=secs.indexOf(e.target);
        if(i>-1) links[i].classList.add('active');
      }}
    }});
  }},{{threshold:0.2,rootMargin:'-5% 0px -65% 0px'}});
  secs.forEach(function(s){{if(s) io.observe(s);}});
}})();
</script>
<script>{viewer_js}</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python generate_report.py <product_out_dir>")
        sys.exit(1)
    generate_report(sys.argv[1])
