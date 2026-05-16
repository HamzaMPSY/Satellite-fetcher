from __future__ import annotations

from pathlib import Path
from typing import Annotated
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse

from nimbuschain_rgb_viewer_service import __version__
from nimbuschain_rgb_viewer_service.presets import preset_catalog
from nimbuschain_rgb_viewer_service.renderer import (
    inspect_zarr_scene,
    list_zarr_scenes,
    render_rgb_preview_png,
    resolve_scene_uri,
)


def create_router(*, zarr_root: Path) -> APIRouter:
    router = APIRouter()

    @router.get("/")
    def root() -> dict[str, str]:
        return {
            "service": "rgb-viewer-service",
            "status": "ok",
            "version": __version__,
            "gallery": "/gallery",
        }

    @router.get("/health")
    def health() -> dict[str, str]:
        return {
            "service": "rgb-viewer-service",
            "status": "ok",
            "version": __version__,
        }

    @router.get("/v1/configs")
    def configs() -> dict[str, object]:
        return {
            "status": "ok",
            "presets": preset_catalog(),
        }

    @router.get("/v1/scenes")
    def scenes(job_id: str | None = None) -> dict[str, object]:
        return {
            "status": "ok",
            "zarr_root": str(zarr_root),
            "job_id": job_id,
            "items": list_zarr_scenes(zarr_root, job_id=job_id),
        }

    @router.get("/v1/scene")
    def scene(
        uri: Annotated[str, Query(description="Local .zarr path or path relative to zarr_root")],
    ) -> dict[str, object]:
        try:
            return {
                "status": "ok",
                "item": inspect_zarr_scene(resolve_scene_uri(uri, zarr_root=zarr_root)),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/v1/preview")
    def preview(
        uri: Annotated[str, Query(description="Local .zarr path or path relative to zarr_root")],
        time_index: int = 0,
        max_size: int = 1024,
        bands: str | None = None,
    ) -> Response:
        try:
            resolved_bands = [item.strip() for item in str(bands or "").split(",") if item.strip()]
            png, metadata = render_rgb_preview_png(
                resolve_scene_uri(uri, zarr_root=zarr_root),
                time_index=time_index,
                max_size=max_size,
                bands=resolved_bands or None,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        headers = {
            "X-Nimbus-RGB-Bands": ",".join(metadata["rgb_bands"]),
            "X-Nimbus-RGB-Preset": str(metadata["preset"]),
            "X-Nimbus-Preview-Stride": str(metadata["stride"]),
        }
        return Response(content=png, media_type="image/png", headers=headers)

    @router.get("/gallery", response_class=HTMLResponse)
    def gallery(request: Request, job_id: str | None = None) -> HTMLResponse:
        items = list_zarr_scenes(zarr_root, job_id=job_id)
        return HTMLResponse(_gallery_html(items, request=request, job_id=job_id))

    return router


def _gallery_html(items: list[dict[str, object]], *, request: Request, job_id: str | None) -> str:
    rows = []
    for item in items:
        path = str(item.get("path") or "")
        scene_id = _escape(str(item.get("scene_id") or Path(path).stem))
        kind = _escape(str(item.get("kind") or "scene"))
        collection = _escape(str(item.get("collection") or ""))
        product_type = _escape(str(item.get("product_type") or ""))
        shape = list(item.get("shape") or [])
        time_count = int(shape[0]) if shape else 0
        recommended = item.get("recommended_rgb") if isinstance(item.get("recommended_rgb"), dict) else {}
        bands = ", ".join(str(value) for value in list(recommended.get("bands") or []))
        preview_url = f"/v1/preview?uri={quote(path)}&max_size=768"
        detail_url = f"/v1/scene?uri={quote(path)}"
        rows.append(
            f"""
            <article class="scene">
              <img src="{preview_url}" alt="{scene_id}" loading="lazy" />
              <div class="meta">
                <h2>{scene_id}</h2>
                <p><span class="pill">{kind}</span> {collection} {product_type}</p>
                <p>{time_count} time slice{"s" if time_count != 1 else ""}</p>
                <p>RGB: {_escape(bands)}</p>
                <a href="{detail_url}">metadata</a>
              </div>
            </article>
            """
        )

    empty = "<p class=\"empty\">No local .zarr scenes found.</p>" if not rows else ""
    base_url = str(request.base_url).rstrip("/")
    filter_label = f"job: {_escape(job_id)}" if job_id else "all local Zarr stores"
    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Nimbus RGB Viewer</title>
        <style>
          :root {{
            color-scheme: dark;
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            background: #0b1118;
            color: #edf4f8;
          }}
          body {{
            margin: 0;
            padding: 28px;
          }}
          header {{
            display: flex;
            justify-content: space-between;
            gap: 16px;
            align-items: end;
            margin-bottom: 24px;
          }}
          .subtitle {{
            margin-top: 6px;
            color: #9bb0bd;
            font-size: 13px;
          }}
          h1 {{
            margin: 0;
            font-size: 24px;
            font-weight: 700;
          }}
          .endpoint {{
            color: #9bb0bd;
            font-size: 13px;
          }}
          main {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 18px;
          }}
          .scene {{
            border: 1px solid #263442;
            border-radius: 8px;
            overflow: hidden;
            background: #111a23;
          }}
          .scene img {{
            display: block;
            width: 100%;
            aspect-ratio: 1.25;
            object-fit: contain;
            background: #05080c;
          }}
          .meta {{
            padding: 12px 14px 14px;
          }}
          h2 {{
            margin: 0 0 8px;
            font-size: 14px;
            overflow-wrap: anywhere;
          }}
          p {{
            margin: 4px 0;
            color: #a9bac5;
            font-size: 13px;
          }}
          .pill {{
            display: inline-block;
            margin-right: 6px;
            border: 1px solid #38556b;
            border-radius: 999px;
            padding: 1px 7px 2px;
            color: #b7f5ff;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
          }}
          a {{
            color: #81d4fa;
            font-size: 13px;
          }}
          .empty {{
            color: #a9bac5;
          }}
        </style>
      </head>
      <body>
        <header>
          <div>
            <h1>Nimbus RGB Viewer</h1>
            <div class="subtitle">{filter_label} · {len(items)} output{"s" if len(items) != 1 else ""}</div>
          </div>
          <div class="endpoint">{_escape(base_url)}</div>
        </header>
        {empty}
        <main>
          {''.join(rows)}
        </main>
      </body>
    </html>
    """


def _escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
