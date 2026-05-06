
from __future__ import annotations
import json
import math
from pathlib import Path
from report.generate_report import (
    _e, _page, _empty_card, _donut, _compute_brdf_deltas,
    _BG, _SURFACE, _BORDER, _BORDER_ST, _TEXT, _TEXT2, _TEXT3,
    _GREEN, _GREEN_BG, _AMBER, _AMBER_BG, _RED, _RED_BG,
    _STATUS,
)

def generate_multi_report(product_out_dirs: list, report_path) -> "Path":
    from pathlib import Path as _Path
    report_path = _Path(report_path)

    products = []
    for d in product_out_dirs:
        d = _Path(d)
        mp = d / "manifest.json"
        if not mp.exists():
            print(f"[multi-report] Skipping {d.name} — no manifest.json")
            continue
        try:
            manifest = json.loads(mp.read_text())
            manifest["_out_dir"] = d
            products.append(manifest)
        except Exception as exc:
            print(f"[multi-report] Could not read manifest for {d.name}: {exc}")

    if not products:
        raise FileNotFoundError(
            f"No valid manifests found in any of: {[str(p) for p in product_out_dirs]}"
        )

    html = _render_multi(products)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(html, encoding="utf-8")
    print(f"[report] Multi-product report → {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# Multi-report renderer
# ---------------------------------------------------------------------------

def _render_multi(products: list) -> str:
    n_products  = len(products)
    n_ok        = sum(1 for p in products if not any(
        s.get("status") == "failed" for s in p.get("steps", {}).values()))
    n_fail      = n_products - n_ok
    total_el    = sum(
        sum((s.get("elapsed") or 0) for s in p.get("steps", {}).values())
        for p in products
    )

    step_order = [
        "geometric_processing", "atmospheric_correction", "sbaf",
        "valid_pixel_mask", "brdf_adjustment", "data_fusion", "validation",
    ]

    sections = "\n".join([
        _multi_hero(products, n_ok, n_fail, total_el),
        _multi_summary_table(products, step_order),
        _multi_performance(products),
        _multi_steps(products),
        _multi_mask_stats(products),
        _multi_brdf(products),
        _multi_fusion(products),
        _multi_validation(products),
        _multi_fingerprint(products),
        _multi_config(products),
    ])

    title = f"{n_products} products"
    return _page_multi(title, sections)


def _multi_hero(products, n_ok, n_fail, total_el):
    status_color = _RED if n_fail else _GREEN
    status_word  = "PARTIAL FAILURE" if n_fail else "ALL COMPLETE"
    fail_color   = _RED if n_fail else _TEXT3

    return f"""
<section class="hero-section">
  <div class="hero-inner">
    <div class="hero-left">
      <p class="overline">sen2like · Multi-Product Report</p>
      <h1 class="hero-title">{len(products)} Products Processed</h1>
      <span class="status-pill" style="color:{status_color};border-color:{status_color}">
        <span class="status-dot" style="background:{status_color}"></span>{status_word}
      </span>
    </div>
  </div>
  <div class="kpi-strip">
    <div class="kpi-cell">
      <span class="kpi-n">{len(products)}</span>
      <span class="kpi-l">Total products</span>
    </div>
    <div class="kpi-sep"></div>
    <div class="kpi-cell">
      <span class="kpi-n" style="color:{_GREEN}">{n_ok}</span>
      <span class="kpi-l">Succeeded</span>
    </div>
    <div class="kpi-sep"></div>
    <div class="kpi-cell">
      <span class="kpi-n" style="color:{fail_color}">{n_fail}</span>
      <span class="kpi-l">Failed</span>
    </div>
    <div class="kpi-sep"></div>
    <div class="kpi-cell">
      <span class="kpi-n">{total_el:.0f}s</span>
      <span class="kpi-l">Total wall-clock</span>
    </div>
  </div>
</section>"""


def _multi_summary_table(products, step_order):
    rows = ""
    for p in products:
        pid    = Path(p.get("product_id", "unknown")).name
        steps  = p.get("steps", {})
        failed = [n for n, s in steps.items() if s.get("status") == "failed"]
        elapsed = sum((s.get("elapsed") or 0) for s in steps.values())

        overall_ok = len(failed) == 0
        row_col    = _GREEN if overall_ok else _RED
        row_icon   = "✓" if overall_ok else "✗"
        row_bg     = _GREEN_BG if overall_ok else _RED_BG

        step_cells = ""
        for sname in step_order:
            s    = steps.get(sname, {})
            st   = s.get("status", "—")
            icon, col, bg = _STATUS.get(st, ("·", _TEXT3, _BORDER))
            if st == "—":
                icon, col, bg = "—", _TEXT3, _BG
            step_cells += (
                f'<td class="tc"><span class="sbadge" '
                f'style="background:{bg};color:{col}">{icon}</span></td>'
            )

        err_str = ", ".join(failed) if failed else "—"
        rows += f"""
<tr>
  <td><span style="color:{row_col};font-family:var(--mono);font-size:1rem">{row_icon}</span></td>
  <td><code class="fname" style="font-size:.7rem">{_e(pid)}</code></td>
  {step_cells}
  <td class="mono" style="color:var(--text3)">{elapsed:.0f}s</td>
  <td style="color:{'var(--red)' if failed else 'var(--text3)'};font-size:.7rem;font-family:var(--mono)">{_e(err_str)}</td>
</tr>"""

    step_headers = "".join(
        f'<th class="tc" title="{_e(s)}">{_e(s[:4])}…</th>'
        for s in step_order
    )

    return f"""
<section class="card" id="summary">
  <div class="card-head">
    <h2>Product Summary</h2>
    <span class="head-rule"></span>
  </div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead>
        <tr>
          <th></th>
          <th>Product</th>
          {step_headers}
          <th>Elapsed</th>
          <th>Failed steps</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <p class="note">Step abbreviations: geom, atm_, sbaf, mask, brdf, fuse, vali</p>
</section>"""


def _multi_performance(products):
    max_el = max(
        sum((s.get("elapsed") or 0) for s in p.get("steps", {}).values())
        for p in products
    ) or 1

    bar_h   = 28
    gap     = 10
    pad_l   = 260
    pad_r   = 80
    pad_t   = 16
    pad_b   = 22
    chart_w = 820
    bar_w   = chart_w - pad_l - pad_r
    n       = len(products)
    chart_h = pad_t + n * (bar_h + gap) + pad_b

    grid = ""
    for j in range(6):
        gx  = pad_l + int(j / 5 * bar_w)
        pct = int(j / 5 * 100)
        grid += (
            f'<line x1="{gx}" y1="{pad_t}" x2="{gx}" y2="{chart_h - pad_b + 4}" '
            f'stroke="{_BORDER}" stroke-width="1"/>'
            f'<text x="{gx}" y="{chart_h - 5}" text-anchor="middle" '
            f'font-size="9" fill="{_TEXT3}" font-family="\'DM Mono\',monospace">{pct}%</text>'
        )

    bars = ""
    for i, p in enumerate(products):
        pid    = Path(p.get("product_id", "unknown")).name[:32]
        steps  = p.get("steps", {})
        elapsed = sum((s.get("elapsed") or 0) for s in steps.values())
        failed  = any(s.get("status") == "failed" for s in steps.values())
        frac    = elapsed / max_el
        w       = max(3, int(frac * bar_w))
        y       = pad_t + i * (bar_h + gap)
        col     = _RED if failed else _GREEN
        ts      = f"{elapsed:.0f}s"

        bars += (
            f'<text x="{pad_l - 10}" y="{y + bar_h // 2 + 4}" text-anchor="end" '
            f'font-size="10" fill="{_TEXT2}" font-family="\'DM Sans\',sans-serif">{_e(pid)}</text>'
            f'<rect x="{pad_l}" y="{y}" width="{w}" height="{bar_h}" '
            f'fill="{col}" rx="2" opacity="0.35"/>'
            f'<text x="{pad_l + w + 8}" y="{y + bar_h // 2 + 4}" '
            f'font-size="10" fill="{_TEXT2}" font-family="\'DM Mono\',monospace">{_e(ts)}</text>'
        )

    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="100%" '
        f'viewBox="0 0 {chart_w} {chart_h}" style="max-width:{chart_w}px;display:block">'
        + grid + bars + "</svg>"
    )

    return f"""
<section class="card" id="performance">
  <div class="card-head">
    <h2>Processing Performance</h2>
    <span class="head-rule"></span>
    <span class="head-tag">Total elapsed per product · bar width proportional</span>
  </div>
  <div class="chart-wrap">{svg}</div>
</section>"""


def _multi_mask_stats(products):
    rows = ""
    any_data = False
    for p in products:
        out_dir  = p.get("_out_dir")
        pid      = Path(p.get("product_id", "unknown")).name
        meta_path = Path(out_dir) / "fusion" / "metadata.json" if out_dir else None
        vf = None
        if meta_path and meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                vf   = meta.get("valid_pixel_mask", {}).get("valid_fraction")
            except Exception:
                pass

        if vf is not None:
            any_data = True
            col = _GREEN if vf >= 0.3 else (_AMBER if vf >= 0.1 else _RED)
            bar = int(vf * 100)
            rows += f"""
<tr>
  <td><code class="fname" style="font-size:.7rem">{_e(pid)}</code></td>
  <td>
    <div class="el-wrap">
      <div class="el-bar" style="width:{bar}%;background:{col};height:5px;opacity:.6"></div>
      <code class="el-num" style="color:{col}">{vf:.1%}</code>
    </div>
  </td>
  <td style="color:var(--text3);font-family:var(--mono);font-size:.72rem">
    {'Good' if vf >= 0.3 else 'Marginal' if vf >= 0.1 else 'Heavy cloud'}
  </td>
</tr>"""
        else:
            rows += f"""
<tr>
  <td><code class="fname" style="font-size:.7rem">{_e(pid)}</code></td>
  <td colspan="2"><span class="dash">—</span></td>
</tr>"""

    if not any_data:
        return _empty_card("mask", "Valid Pixel Mask — Coverage", "No mask metadata found across products.")

    return f"""
<section class="card" id="mask">
  <div class="card-head">
    <h2>Valid Pixel Mask — Coverage</h2>
    <span class="head-rule"></span>
    <span class="head-tag">Clear pixel fraction per product</span>
  </div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead><tr><th>Product</th><th>Clear fraction</th><th>Assessment</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"""


def _multi_brdf(products):
    # One table per product, stacked
    sections_html = ""
    for p in products:
        out_dir = p.get("_out_dir")
        pid     = Path(p.get("product_id", "unknown")).name
        if not out_dir:
            continue
        deltas = _compute_brdf_deltas(Path(out_dir))
        if not deltas:
            sections_html += f'<p class="note" style="margin-bottom:.7rem">{_e(pid)}: no BRDF data</p>'
            continue

        rows = ""
        for d in deltas:
            d_sign = "+" if d["delta"] >= 0 else ""
            p_sign = "+" if d["pct"]   >= 0 else ""
            col_bar = _TEXT3 if abs(d["pct"]) < 2 else (_AMBER if abs(d["pct"]) < 5 else _RED)
            bar_w = min(100, abs(d["pct"]) / (max(abs(x["pct"]) for x in deltas) or 1) * 100)
            rows += f"""
<tr>
  <td><span class="bdot" style="background:{d['col']}"></span>{_e(d['band'])}</td>
  <td class="mono">{d['pre']:.4f}</td>
  <td class="mono">{d['post']:.4f}</td>
  <td class="mono" style="color:{'var(--green)' if d['delta']>=0 else 'var(--red)'}">
    {d_sign}{d['delta']:.4f}
  </td>
  <td>
    <div class="pbar-wrap">
      <div class="pbar-fill" style="width:{bar_w:.0f}%;background:{col_bar}"></div>
      <code class="pbar-lbl" style="color:{col_bar}">{p_sign}{d['pct']:.2f}%</code>
    </div>
  </td>
</tr>"""

        sections_html += f"""
<div style="margin-bottom:1.2rem">
  <div style="font-family:var(--mono);font-size:.72rem;color:var(--text3);
              margin-bottom:.4rem;padding-bottom:.3rem;border-bottom:1px solid var(--border)">
    {_e(pid)}
  </div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead>
        <tr><th>Band</th><th>Pre-NBAR μ</th><th>Post-NBAR μ</th><th>Δ abs</th><th>Δ rel</th></tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>"""

    if not sections_html:
        return _empty_card("brdf", "BRDF Normalisation", "No BRDF data found.")

    return f"""
<section class="card" id="brdf">
  <div class="card-head">
    <h2>BRDF Normalisation Effect</h2>
    <span class="head-rule"></span>
    <span class="head-tag">Pre-NBAR vs Post-NBAR mean reflectance · per product</span>
  </div>
  {sections_html}
  <p class="note">&lt;2% — nominal &nbsp;·&nbsp; 2–5% — moderate &nbsp;·&nbsp; &gt;5% — review recommended</p>
</section>"""


def _multi_config(products):
    snaps = {}
    for p in products:
        pid   = Path(p.get("product_id", "unknown")).name
        steps = p.get("steps", {})
        snap  = {n: s.get("config_snapshot", {}) for n, s in steps.items() if s.get("config_snapshot")}
        if snap:
            snaps[pid] = snap
    if not snaps:
        return ""
    pretty = json.dumps(snaps, indent=2, default=str)
    return f"""
<section class="card" id="config">
  <div class="card-head">
    <h2>Config Snapshots</h2>
    <span class="head-rule"></span>
  </div>
  <details class="cfg-details">
    <summary>Expand full configuration for all products</summary>
    <pre class="cfg-pre">{_e(pretty)}</pre>
  </details>
</section>"""



def _multi_steps(products):
    blocks = ""
    for p in products:
        pid   = Path(p.get("product_id", "unknown")).name
        steps = p.get("steps", {})

        step_order = [
            "geometric_processing", "atmospheric_correction", "sbaf",
            "valid_pixel_mask", "brdf_adjustment", "data_fusion", "validation",
        ]
        ordered = [(n, steps[n]) for n in step_order if n in steps]
        ordered += [(n, s) for n, s in steps.items() if n not in step_order]

        if not ordered:
            continue

        max_el = max((s.get("elapsed") or 0) for _, s in ordered) or 1
        rows = ""
        for i, (name, step) in enumerate(ordered):
            st   = step.get("status", "—")
            icon, col, bg = _STATUS.get(st, ("?", _TEXT2, _BORDER))
            el   = step.get("elapsed")
            el_s = f"{el:.2f}s" if el is not None else "—"
            n_out = len(step.get("outputs", []))
            err  = step.get("error") or ""
            label = name.replace("_", " ").title()
            bar_pct = int((el or 0) / max_el * 100)
            err_cell = (
                f'<span class="err-txt" title="{_e(err)}">{_e(err[:80])}{"…" if len(err)>80 else ""}</span>'
                if err else '<span class="dash">—</span>'
            )
            rows += f"""
<tr class="step-tr" style="animation-delay:{i*40}ms">
  <td class="sname">{_e(label)}</td>
  <td><span class="sbadge" style="background:{bg};color:{col}">{icon} {_e(st)}</span></td>
  <td><div class="el-wrap">
    <div class="el-bar" style="width:{bar_pct}%;background:{_TEXT3}"></div>
    <code class="el-num">{el_s}</code>
  </div></td>
  <td class="tc">{n_out}</td>
  <td class="errcol">{err_cell}</td>
</tr>"""

        blocks += f"""
<div class="multi-block">
  <div class="multi-block-head">{_e(pid)}</div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead><tr>
        <th>Step</th><th>Status</th><th>Elapsed</th>
        <th class="tc">Outputs</th><th>Error</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>"""

    if not blocks:
        return _empty_card("steps", "Step Results", "No step data available.")

    return f"""
<section class="card" id="steps">
  <div class="card-head">
    <h2>Step-by-Step Results</h2>
    <span class="head-rule"></span>
    <span class="head-tag">Per product</span>
  </div>
  {blocks}
</section>"""


def _multi_fusion(products):
    blocks = ""
    for p in products:
        pid       = Path(p.get("product_id", "unknown")).name
        out_dir   = p.get("_out_dir")
        if not out_dir:
            continue
        fusion_dir = Path(out_dir) / "fusion"
        if not fusion_dir.exists():
            blocks += f'<p class="note" style="margin-bottom:.6rem">{_e(pid)}: fusion dir not found</p>'
            continue

        candidates = (
            sorted(fusion_dir.glob("*_10m.TIF"))
            + [fusion_dir / "FUSION_VALIDITY_MASK.TIF"]
            + sorted(fusion_dir.glob("*.json"))
        )
        seen, files = set(), []
        for f in candidates:
            if f not in seen and f.exists():
                seen.add(f); files.append(f)

        if not files:
            blocks += f'<p class="note" style="margin-bottom:.6rem">{_e(pid)}: no fusion files found</p>'
            continue

        total_mb = sum(f.stat().st_size for f in files) / 1e6
        max_mb   = max(f.stat().st_size for f in files) / 1e6 or 1
        rows = "".join(
            f'<tr><td><code class="fname">{_e(fp.name)}</code></td>'
            f'<td><div class="szbar-wrap">'
            f'<div class="szbar" style="width:{max(2,int(fp.stat().st_size/1e6/max_mb*100))}%"></div>'
            f'<code class="sznum">{fp.stat().st_size/1e6:.2f} MB</code></div></td></tr>'
            for fp in files
        )
        blocks += f"""
<div class="multi-block">
  <div class="multi-block-head">{_e(pid)} <span class="multi-block-sub">{len(files)} files · {total_mb:.1f} MB</span></div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead><tr><th>File</th><th>Size</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>"""

    if not blocks:
        return _empty_card("fusion", "Fusion Output Files", "No fusion data found.")

    return f"""
<section class="card" id="fusion">
  <div class="card-head">
    <h2>Fusion Output Files</h2>
    <span class="head-rule"></span>
    <span class="head-tag">Per product</span>
  </div>
  {blocks}
</section>"""


def _multi_validation(products):
    rows = ""
    any_val = False
    for p in products:
        pid   = Path(p.get("product_id", "unknown")).name
        steps = p.get("steps", {})
        val   = steps.get("validation", {})
        st    = val.get("status", "")
        if not st:
            rows += f'<tr><td><code class="fname" style="font-size:.7rem">{_e(pid)}</code></td><td colspan="3"><span class="dash">not run</span></td></tr>'
            continue
        any_val = True
        icon, col, bg = _STATUS.get(st, ("?", _TEXT2, _BORDER))
        elapsed = val.get("elapsed")
        el_s    = f"{elapsed:.2f}s" if elapsed is not None else "—"
        err     = val.get("error") or ""
        err_cell = (
            f'<span class="err-txt" title="{_e(err)}">{_e(err[:100])}{"…" if len(err)>100 else ""}</span>'
            if err else '<span class="dash">—</span>'
        )
        rows += f"""
<tr>
  <td><code class="fname" style="font-size:.7rem">{_e(pid)}</code></td>
  <td><span class="sbadge" style="background:{bg};color:{col}">{icon} {_e(st)}</span></td>
  <td class="mono" style="color:var(--text3)">{el_s}</td>
  <td class="errcol">{err_cell}</td>
</tr>"""

    if not any_val:
        return _empty_card("validation", "Validation", "Validation was not run for any product.")

    return f"""
<section class="card" id="validation">
  <div class="card-head">
    <h2>Validation</h2>
    <span class="head-rule"></span>
  </div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead><tr><th>Product</th><th>Status</th><th>Elapsed</th><th>Error</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <p class="note">Re-run in single-product mode for the full WARN/FAIL breakdown.</p>
</section>"""


def _multi_fingerprint(products):
    rows = ""
    for p in products:
        pid      = Path(p.get("product_id", "unknown")).name
        version  = p.get("pipeline_version", "unknown")
        cfg_hash = p.get("config_hash", "—")
        ip       = p.get("input_paths", {})
        ip_str   = "  ".join(f"{k}: {v}" for k, v in ip.items())
        rows += f"""
<tr>
  <td><code class="fname" style="font-size:.7rem">{_e(pid)}</code></td>
  <td><code style="font-family:var(--mono);font-size:.68rem">{_e(version)}</code></td>
  <td><code style="font-family:var(--mono);font-size:.65rem;word-break:break-all">{_e(cfg_hash[:20])}…</code></td>
  <td style="font-family:var(--mono);font-size:.62rem;color:var(--text3);word-break:break-all">{_e(ip_str)}</td>
</tr>"""

    return f"""
<section class="card" id="fingerprint">
  <div class="card-head">
    <h2>Reproducibility Fingerprint</h2>
    <span class="head-rule"></span>
  </div>
  <div class="tbl-wrap">
    <table class="dtbl">
      <thead><tr><th>Product</th><th>Version</th><th>Config hash</th><th>Input paths</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <p class="note">Identical version + config hash + inputs → bit-for-bit reproducible outputs.</p>
</section>"""


def _page_multi(title: str, body: str) -> str:
    nav_items = [
        ("summary",     "Summary"),
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


    dummy = _page("__MULTI__", "")
    # Extract everything up to and including </style>, replace nav and body.
    style_end = dummy.index("</style>") + len("</style>")
    head_css  = dummy[:style_end]

    return f"""{head_css}
</head>
<body>
<nav class="sidebar">
  <div class="sb-logo">
    <div class="sb-name">sen2like</div>
    <div class="sb-sub">Multi-Product Report</div>
  </div>
  {nav}
</nav>
<main class="main">
{body}
</main>
<script>
(function(){{
  document.querySelectorAll('.card').forEach(function(el,i){{
    el.style.animationDelay=(i*55)+'ms';
  }});
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
<style>
.multi-block{{margin-bottom:1.4rem;border:1px solid var(--border);border-radius:4px;overflow:hidden}}
.multi-block-head{{font-family:var(--mono);font-size:.72rem;color:var(--text2);font-weight:500;
  padding:.5rem .85rem;background:var(--bg);border-bottom:1px solid var(--border)}}
.multi-block-sub{{font-weight:400;color:var(--text3);margin-left:.6rem}}
.multi-block .dtbl td{{padding:.4rem .75rem}}
</style>
</body>
</html>"""



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    if not args:
        print("Usage: python generate_multi_report.py <dir1> [<dir2> ...] [--out report.html]")
        sys.exit(1)
    out_path = None
    if "--out" in args:
        i        = args.index("--out")
        out_path = args[i + 1]
        args     = args[:i] + args[i + 2:]
    out = out_path or str(Path(args[0]).parent / "report.html")
    generate_multi_report(args, out)