"""Export print-ready A4 review PDFs per organization via HTML -> PDF."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from io import BytesIO
from pathlib import Path

import httpx
import mapbox_vector_tile
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape
from PIL import Image, ImageDraw
from playwright.sync_api import sync_playwright

from benefind.classify import read_org_artifact
from benefind.config import PROJECT_ROOT


@dataclass
class ExportStats:
    total_rows: int = 0
    selected_rows: int = 0
    exported: int = 0
    failed: int = 0
    map_ok: int = 0
    map_missing: int = 0


@dataclass
class MunicipalityRegistry:
    key_by_name: dict[str, str]
    key_by_alias: dict[str, str]
    lonlat_by_key: dict[str, tuple[float, float]]
    map_meta: dict[str, float] | None = None


@dataclass(frozen=True)
class MapConfig:
    style_url: str
    default_zoom: float
    base_zoom_for_coverage: float
    width_px: int
    height_px: int
    max_render_dim_px: int
    max_render_pixels: int
    pad_lon: float
    pad_lat: float
    render_wait_ms: int
    render_timeout_ms: int
    probe_vector_tile: bool
    projection_meta_version: int
    marker_outer_radius_px: int
    marker_inner_radius_px: int
    marker_outer_color: str
    marker_inner_color: str


@dataclass(frozen=True)
class PdfConfig:
    map: MapConfig
    fallbacks: dict[str, str]


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value or "").strip()


def _fallback_text(value: object, fallback: str) -> str:
    text = _clean_text(value)
    return text if text else fallback


def _normalize_location_name(value: str) -> str:
    text = str(value or "").strip().lower()
    replacements = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


@lru_cache(maxsize=1)
def _load_review_pdf_raw_config() -> dict[str, object]:
    path = PROJECT_ROOT / "config" / "review_pdf.toml"
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("review_pdf config must be a TOML table")
    return raw


def _cfg_get(path: str, default: object = None) -> object:
    cursor: object = _load_review_pdf_raw_config()
    for token in path.split("."):
        if not isinstance(cursor, dict):
            return default
        if token not in cursor:
            return default
        cursor = cursor[token]
    return cursor


@lru_cache(maxsize=1)
def _load_pdf_config() -> PdfConfig:
    map_cfg = _cfg_get("map", {})
    if not isinstance(map_cfg, dict):
        map_cfg = {}
    marker_cfg = map_cfg.get("marker", {})
    if not isinstance(marker_cfg, dict):
        marker_cfg = {}

    fallbacks = _cfg_get("texts.fallbacks", {})
    if not isinstance(fallbacks, dict):
        fallbacks = {}

    return PdfConfig(
        map=MapConfig(
            style_url=str(map_cfg.get("style_url", "")).strip(),
            default_zoom=float(map_cfg.get("default_zoom", 12.8)),
            base_zoom_for_coverage=float(map_cfg.get("base_zoom_for_coverage", 9.6)),
            width_px=int(map_cfg.get("width_px", 900)),
            height_px=int(map_cfg.get("height_px", 500)),
            max_render_dim_px=int(map_cfg.get("max_render_dim_px", 10000)),
            max_render_pixels=int(map_cfg.get("max_render_pixels", 60_000_000)),
            pad_lon=float(map_cfg.get("pad_lon", 0.03)),
            pad_lat=float(map_cfg.get("pad_lat", 0.02)),
            render_wait_ms=int(map_cfg.get("render_wait_ms", 350)),
            render_timeout_ms=int(map_cfg.get("render_timeout_ms", 20_000)),
            probe_vector_tile=bool(map_cfg.get("probe_vector_tile", True)),
            projection_meta_version=int(map_cfg.get("projection_meta_version", 2)),
            marker_outer_radius_px=int(marker_cfg.get("outer_radius_px", 30)),
            marker_inner_radius_px=int(marker_cfg.get("inner_radius_px", 20)),
            marker_outer_color=str(marker_cfg.get("outer_color", "#ea580c")).strip(),
            marker_inner_color=str(marker_cfg.get("inner_color", "#9a3412")).strip(),
        ),
        fallbacks={str(k): str(v) for k, v in fallbacks.items()},
    )


def _load_question_payload(org_id: str, question_id: str) -> dict[str, object]:
    ask_path = PROJECT_ROOT / "data" / "orgs" / org_id / "classify" / question_id / "ask.json"
    payload = read_org_artifact(ask_path)
    manual = payload.get("manual_override", {})
    if isinstance(manual, dict):
        normalized = manual.get("normalized", {})
        if isinstance(normalized, dict) and normalized:
            return normalized
    normalized = payload.get("normalized", {})
    if isinstance(normalized, dict):
        return normalized
    return {}


def _load_municipality_registry() -> MunicipalityRegistry:
    path = PROJECT_ROOT / "config" / "municipality_coordinates.toml"
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    lonlat_by_key: dict[str, tuple[float, float]] = {}
    key_by_name: dict[str, str] = {}

    municipalities = raw.get("municipalities", [])
    if not isinstance(municipalities, list) or not municipalities:
        raise ValueError("municipality registry has no municipalities")

    for row in municipalities:
        if not isinstance(row, dict):
            continue
        key = _normalize_location_name(str(row.get("key", "") or ""))
        name = _normalize_location_name(str(row.get("name", "") or ""))
        if not key or not name:
            continue
        lon = float(row.get("lon"))
        lat = float(row.get("lat"))
        lonlat_by_key[key] = (lon, lat)
        key_by_name[name] = key

    key_by_alias: dict[str, str] = {}
    aliases = raw.get("aliases", [])
    if isinstance(aliases, list):
        for row in aliases:
            if not isinstance(row, dict):
                continue
            alias = _normalize_location_name(str(row.get("alias", "") or ""))
            mapped = _normalize_location_name(str(row.get("maps_to_key", "") or ""))
            if not alias or not mapped:
                continue
            if mapped not in lonlat_by_key:
                raise ValueError(f"Alias target not found in registry: {mapped}")
            key_by_alias[alias] = mapped

    if not lonlat_by_key:
        raise ValueError("municipality registry points are empty")

    return MunicipalityRegistry(
        key_by_name=key_by_name,
        key_by_alias=key_by_alias,
        lonlat_by_key=lonlat_by_key,
    )


def _resolve_municipality_key(location_text: str, registry: MunicipalityRegistry) -> str:
    normalized = _normalize_location_name(location_text)
    if not normalized:
        return ""
    if normalized in registry.key_by_name:
        return registry.key_by_name[normalized]
    if normalized in registry.key_by_alias:
        return registry.key_by_alias[normalized]
    for name_key, muni_key in registry.key_by_name.items():
        if normalized == name_key or normalized in name_key or name_key in normalized:
            return muni_key
    for alias_key, muni_key in registry.key_by_alias.items():
        if normalized == alias_key or normalized in alias_key or alias_key in normalized:
            return muni_key
    return ""


def _deg2num(lon: float, lat: float, zoom: float) -> tuple[float, float]:
    lat_rad = math.radians(lat)
    n = 2.0**zoom
    xtile = (lon + 180.0) / 360.0 * n
    ytile = (1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi) / 2.0 * n
    return xtile, ytile


def _fetch_vector_tile_probe(timeout_seconds: int = 20) -> bool:
    url = "https://vectortiles0.geo.admin.ch/tiles/ch.swisstopo.base.vt/v1.0.0/10/536/357.pbf"
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.content
        decoded = mapbox_vector_tile.decode(payload)
        return bool(decoded)
    except Exception:
        return False


def _fetch_district_boundary_rings_lonlat(
    timeout_seconds: int = 20,
) -> list[list[tuple[float, float]]]:
    default_layer = "ch.swisstopo.swissboundaries3d-bezirk-flaeche.fill"
    layer_id = str(_cfg_get("map.boundary.layer_id", default_layer))
    feature_id = int(_cfg_get("map.boundary.feature_id", 110))
    sr = int(_cfg_get("map.boundary.sr", 4326))
    url = f"https://api3.geo.admin.ch/rest/services/api/MapServer/{layer_id}/{feature_id}"
    params = {"geometryFormat": "geojson", "sr": str(sr)}
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
    except Exception:
        return []

    feature = data.get("feature", {}) if isinstance(data, dict) else {}
    geometry = feature.get("geometry", {}) if isinstance(feature, dict) else {}
    if not isinstance(geometry, dict):
        return []

    gtype = str(geometry.get("type", "") or "").strip()
    coords = geometry.get("coordinates", [])
    rings: list[list[tuple[float, float]]] = []

    if gtype == "Polygon" and isinstance(coords, list):
        polygons = [coords]
    elif gtype == "MultiPolygon" and isinstance(coords, list):
        polygons = coords
    else:
        polygons = []

    for polygon in polygons:
        if not isinstance(polygon, list):
            continue
        for ring in polygon:
            if not isinstance(ring, list):
                continue
            parsed = [
                (float(point[0]), float(point[1]))
                for point in ring
                if isinstance(point, list) and len(point) >= 2
            ]
            if len(parsed) >= 3:
                rings.append(parsed)
    return rings


def _apply_boundary_mask(
    image: Image.Image,
    *,
    center_lon: float,
    center_lat: float,
    zoom: float,
    rings_lonlat: list[list[tuple[float, float]]],
) -> Image.Image:
    if not rings_lonlat:
        return image
    width, height = image.size
    center_xw, center_yw = _deg2num(center_lon, center_lat, zoom)
    center_xw *= 256.0
    center_yw *= 256.0
    mask = Image.new("L", (width, height), 0)
    draw = ImageDraw.Draw(mask)

    for idx, ring in enumerate(rings_lonlat):
        pixels: list[tuple[int, int]] = []
        for lon, lat in ring:
            xtf, ytf = _deg2num(lon, lat, zoom)
            xw = xtf * 256.0
            yw = ytf * 256.0
            x = int(round((xw - center_xw) + (width / 2.0)))
            y = int(round((yw - center_yw) + (height / 2.0)))
            pixels.append((x, y))
        if len(pixels) < 3:
            continue
        fill = 255 if idx == 0 else 0
        draw.polygon(pixels, fill=fill)

    masked = image.convert("RGBA")
    masked.putalpha(mask)
    return masked


def _fetch_base_map_bytes(
    registry: MunicipalityRegistry,
    cfg: MapConfig,
    timeout_seconds: int = 20,
) -> bytes | None:
    if cfg.probe_vector_tile and not _fetch_vector_tile_probe(timeout_seconds=timeout_seconds):
        return None

    coords = list(registry.lonlat_by_key.values())
    if not coords:
        return None
    lons = [lon for lon, _ in coords]
    lats = [lat for _, lat in coords]
    min_lon = min(lons) - cfg.pad_lon
    max_lon = max(lons) + cfg.pad_lon
    min_lat = min(lats) - cfg.pad_lat
    max_lat = max(lats) + cfg.pad_lat

    center_lon = (min_lon + max_lon) / 2.0
    center_lat = (min_lat + max_lat) / 2.0
    zoom = cfg.default_zoom

    zoom_delta = max(0.0, zoom - cfg.base_zoom_for_coverage)
    target_scale = 2.0**zoom_delta
    max_scale_dim = min(
        cfg.max_render_dim_px / float(cfg.width_px),
        cfg.max_render_dim_px / float(cfg.height_px),
    )
    max_scale_px = math.sqrt(cfg.max_render_pixels / float(cfg.width_px * cfg.height_px))
    render_scale = max(1.0, min(target_scale, max_scale_dim, max_scale_px))
    render_width = max(1, int(round(cfg.width_px * render_scale)))
    render_height = max(1, int(round(cfg.height_px * render_scale)))

    html_doc = f"""<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <link href='https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css' rel='stylesheet'>
  <style>
    html, body, #map {{ margin: 0; padding: 0; width: 100%; height: 100%; background: #fff; }}
    .maplibregl-control-container {{ display: none; }}
  </style>
</head>
<body>
  <div id='map'></div>
  <script src='https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js'></script>
  <script>
    window.__renderDone = false;
    const map = new maplibregl.Map({{
      container: 'map',
      style: {json.dumps(cfg.style_url)},
      center: [{center_lon:.9f}, {center_lat:.9f}],
      zoom: {zoom:.4f},
      attributionControl: false,
      interactive: false,
      preserveDrawingBuffer: true,
      fadeDuration: 0,
    }});
    map.once('idle', () =>
      setTimeout(() => {{ window.__renderDone = true; }}, {cfg.render_wait_ms})
    );
  </script>
</body>
</html>
"""

    screenshot_bytes: bytes | None = None
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(viewport={"width": render_width, "height": render_height})
            page.set_content(html_doc, wait_until="load")
            page.wait_for_function("window.__renderDone === true", timeout=cfg.render_timeout_ms)
            screenshot_bytes = page.screenshot(type="png")
            browser.close()
    except Exception:
        return None

    if not screenshot_bytes:
        return None

    try:
        composed = Image.open(BytesIO(screenshot_bytes)).convert("RGBA")
    except Exception:
        return None

    boundary_rings = _fetch_district_boundary_rings_lonlat(timeout_seconds=timeout_seconds)
    composed = _apply_boundary_mask(
        composed,
        center_lon=center_lon,
        center_lat=center_lat,
        zoom=zoom,
        rings_lonlat=boundary_rings,
    )

    full_width, full_height = composed.size
    alpha_bbox = composed.getchannel("A").getbbox()
    if alpha_bbox:
        composed = composed.crop(alpha_bbox)
    crop_x0, crop_y0 = alpha_bbox[:2] if alpha_bbox else (0, 0)
    crop_width = float(alpha_bbox[2] - alpha_bbox[0]) if alpha_bbox else float(full_width)
    crop_height = float(alpha_bbox[3] - alpha_bbox[1]) if alpha_bbox else float(full_height)

    uniform_scale = (
        min(cfg.width_px / crop_width, cfg.height_px / crop_height)
        if crop_width and crop_height
        else 1.0
    )
    resized_w = max(1, int(round(crop_width * uniform_scale)))
    resized_h = max(1, int(round(crop_height * uniform_scale)))
    if composed.size != (resized_w, resized_h):
        composed = composed.resize((resized_w, resized_h), resample=Image.Resampling.LANCZOS)

    canvas_img = Image.new("RGBA", (cfg.width_px, cfg.height_px), (0, 0, 0, 0))
    pad_x = (cfg.width_px - resized_w) // 2
    pad_y = (cfg.height_px - resized_h) // 2
    canvas_img.paste(composed, (pad_x, pad_y), composed)

    registry.map_meta = {
        "meta_version": float(cfg.projection_meta_version),
        "center_lon": center_lon,
        "center_lat": center_lat,
        "zoom": zoom,
        "width": float(cfg.width_px),
        "height": float(cfg.height_px),
        "crop_x0": float(crop_x0),
        "crop_y0": float(crop_y0),
        "full_width": float(full_width),
        "full_height": float(full_height),
        "scale_x": float(uniform_scale),
        "scale_y": float(uniform_scale),
        "pad_x": float(pad_x),
        "pad_y": float(pad_y),
    }

    buffer = BytesIO()
    canvas_img.save(buffer, format="PNG")
    return buffer.getvalue()


def _is_usable_map_image(image_bytes: bytes) -> bool:
    try:
        with Image.open(BytesIO(image_bytes)) as img:
            rgba = img.convert("RGBA")
            bbox = rgba.getbbox()
            if bbox is None:
                return False
            alpha_extrema = rgba.getchannel("A").getextrema()
            if isinstance(alpha_extrema, tuple) and alpha_extrema[1] == 0:
                return False
            rgb = rgba.convert("RGB")
            values = list(rgb.getdata())
            if not values:
                return False
            near_white = sum(1 for r, g, b in values if r >= 245 and g >= 245 and b >= 245)
            return near_white / len(values) <= 0.995
    except Exception:
        return False


def _render_marker_map(
    base_image_bytes: bytes,
    registry: MunicipalityRegistry,
    lonlat: tuple[float, float],
    cfg: PdfConfig,
) -> bytes | None:
    try:
        with Image.open(BytesIO(base_image_bytes)) as base_img:
            img = base_img.convert("RGBA")
            draw = ImageDraw.Draw(img)
            meta = registry.map_meta
            if not isinstance(meta, dict):
                return None

            zoom = float(meta.get("zoom", 0.0))
            center_lon = float(meta.get("center_lon", 0.0))
            center_lat = float(meta.get("center_lat", 0.0))
            width, height = img.size
            crop_x0 = float(meta.get("crop_x0", 0.0))
            crop_y0 = float(meta.get("crop_y0", 0.0))
            scale_x = float(meta.get("scale_x", 1.0))
            scale_y = float(meta.get("scale_y", 1.0))
            pad_x = float(meta.get("pad_x", 0.0))
            pad_y = float(meta.get("pad_y", 0.0))

            lon, lat = lonlat
            xtf, ytf = _deg2num(float(lon), float(lat), zoom)
            cx_tf, cy_tf = _deg2num(center_lon, center_lat, zoom)
            full_x = (xtf - cx_tf) * 256.0 + (float(meta.get("full_width", width)) / 2.0)
            full_y = (ytf - cy_tf) * 256.0 + (float(meta.get("full_height", height)) / 2.0)
            x = int(round((full_x - crop_x0) * scale_x + pad_x))
            y = int(round((full_y - crop_y0) * scale_y + pad_y))
            x = max(0, min(width - 1, x))
            y = max(0, min(height - 1, y))

            marker_outer = cfg.map.marker_outer_color
            marker_inner = cfg.map.marker_inner_color
            draw.ellipse(
                [
                    x - cfg.map.marker_outer_radius_px,
                    y - cfg.map.marker_outer_radius_px,
                    x + cfg.map.marker_outer_radius_px,
                    y + cfg.map.marker_outer_radius_px,
                ],
                fill=marker_outer,
            )
            draw.ellipse(
                [
                    x - cfg.map.marker_inner_radius_px,
                    y - cfg.map.marker_inner_radius_px,
                    x + cfg.map.marker_inner_radius_px,
                    y + cfg.map.marker_inner_radius_px,
                ],
                fill=marker_inner,
            )

            buffer = BytesIO()
            img.save(buffer, format="PNG")
            return buffer.getvalue()
    except Exception:
        return None


def _load_map_meta(cache_meta_path: Path, required_version: int) -> dict[str, float] | None:
    if not cache_meta_path.exists():
        return None
    try:
        raw = json.loads(cache_meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    normalized: dict[str, float] = {}
    for key, value in raw.items():
        try:
            normalized[str(key)] = float(value)
        except Exception:
            return None
    if float(normalized.get("meta_version", 0.0)) < float(required_version):
        return None
    return normalized if normalized else None


def _save_map_meta(cache_meta_path: Path, map_meta: dict[str, float] | None) -> None:
    if not isinstance(map_meta, dict) or not map_meta:
        return
    payload = {key: float(value) for key, value in map_meta.items()}
    cache_meta_path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _base_map_cache_fingerprint(cfg: MapConfig) -> str:
    payload = {
        "style_url": cfg.style_url,
        "default_zoom": cfg.default_zoom,
        "base_zoom_for_coverage": cfg.base_zoom_for_coverage,
        "width_px": cfg.width_px,
        "height_px": cfg.height_px,
        "max_render_dim_px": cfg.max_render_dim_px,
        "max_render_pixels": cfg.max_render_pixels,
        "pad_lon": cfg.pad_lon,
        "pad_lat": cfg.pad_lat,
        "render_wait_ms": cfg.render_wait_ms,
        "render_timeout_ms": cfg.render_timeout_ms,
        "probe_vector_tile": cfg.probe_vector_tile,
        "projection_meta_version": cfg.projection_meta_version,
        "boundary_layer": str(_cfg_get("map.boundary.layer_id", "")),
        "boundary_feature_id": int(_cfg_get("map.boundary.feature_id", 110)),
        "boundary_sr": int(_cfg_get("map.boundary.sr", 4326)),
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _marker_cache_fingerprint(cfg: MapConfig) -> str:
    payload = {
        "outer_radius_px": cfg.marker_outer_radius_px,
        "inner_radius_px": cfg.marker_inner_radius_px,
        "outer_color": cfg.marker_outer_color.strip().lower(),
        "inner_color": cfg.marker_inner_color.strip().lower(),
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]


def _img_data_uri(image_bytes: bytes) -> str:
    encoded = base64.b64encode(image_bytes).decode("ascii")
    return f"data:image/png;base64,{encoded}"


@lru_cache(maxsize=1)
def _template_environment() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(PROJECT_ROOT)),
        autoescape=select_autoescape(enabled_extensions=("html", "xml"), default=True),
    )


def _build_packet_html(
    *,
    cfg: PdfConfig,
    org_id: str,
    org_name: str,
    location: str,
    map_data_uri: str | None,
    map_status: str,
    generated_at: str,
    payloads: dict[str, dict[str, object]],
) -> str:
    context = _build_packet_context(
        org_id=org_id,
        org_name=org_name,
        location=location,
        map_data_uri=map_data_uri,
        map_status=map_status,
        generated_at=generated_at,
        payloads=payloads,
    )
    rel = str(_cfg_get("template.html_file", "config/review_pdf_template.html"))
    return _template_environment().get_template(rel).render(context)


def _build_packet_context(
    *,
    org_id: str,
    org_name: str,
    location: str,
    map_data_uri: str | None,
    map_status: str,
    generated_at: str,
    payloads: dict[str, dict[str, object]],
) -> dict[str, object]:
    return {
        "org": {
            "id": org_id,
            "name": org_name,
            "location": location or "-",
        },
        "map": {
            "status": map_status,
            "data_uri": map_data_uri,
            "has_image": bool(map_data_uri),
        },
        "classify_payloads": payloads,
        "meta": {
            "generated_at": generated_at,
        },
    }


def _render_html_to_pdf(html_doc: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.set_content(html_doc, wait_until="networkidle")
        page.emulate_media(media="print")
        page.pdf(
            path=str(output_path),
            format="A4",
            print_background=True,
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
        )
        browser.close()


def export_review_pdfs(
    df: pd.DataFrame,
    *,
    output_dir: Path,
    org_id_filter: str | None,
    limit: int | None,
    map_cache_dir: Path,
) -> tuple[ExportStats, pd.DataFrame]:
    cfg = _load_pdf_config()
    stats = ExportStats(total_rows=int(len(df)))
    registry = _load_municipality_registry()

    base = df.copy()
    if "_excluded_reason" in base.columns:
        base = base.loc[base["_excluded_reason"].astype(str).str.strip() == ""].copy()
    if org_id_filter:
        target = org_id_filter.strip()
        base = base[base["_org_id"].astype(str).str.strip() == target]
    if limit is not None and limit > 0:
        base = base.head(limit)

    stats.selected_rows = int(len(base))
    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    manifest_rows: list[dict[str, str]] = []

    map_cache_dir.mkdir(parents=True, exist_ok=True)
    base_fp = _base_map_cache_fingerprint(cfg.map)
    base_map_cache = map_cache_dir / f"_base_map_winterthur_masked_{base_fp}.png"
    base_map_meta_cache = map_cache_dir / f"_base_map_winterthur_masked_{base_fp}.meta.json"
    base_map_bytes: bytes | None = None
    if base_map_cache.exists():
        cached = base_map_cache.read_bytes()
        if _is_usable_map_image(cached):
            base_map_bytes = cached
            cached_meta = _load_map_meta(base_map_meta_cache, cfg.map.projection_meta_version)
            if cached_meta:
                registry.map_meta = cached_meta
    if base_map_bytes is None:
        fetched = _fetch_base_map_bytes(registry, cfg.map)
        if fetched and _is_usable_map_image(fetched):
            base_map_bytes = fetched
            base_map_cache.write_bytes(fetched)
            _save_map_meta(base_map_meta_cache, registry.map_meta)
    elif not isinstance(registry.map_meta, dict):
        fetched = _fetch_base_map_bytes(registry, cfg.map)
        if fetched and _is_usable_map_image(fetched):
            base_map_bytes = fetched
            base_map_cache.write_bytes(fetched)
            _save_map_meta(base_map_meta_cache, registry.map_meta)
    elif isinstance(registry.map_meta, dict):
        _save_map_meta(base_map_meta_cache, registry.map_meta)

    for _, row in base.iterrows():
        org_id = _clean_text(row.get("_org_id", ""))
        if not org_id:
            stats.failed += 1
            manifest_rows.append(
                {
                    "_org_id": "",
                    "status": "failed",
                    "pdf_path": "",
                    "map_status": "missing_org_id",
                }
            )
            continue

        org_name = _fallback_text(
            row.get("Name", row.get("Bezeichnung", "")),
            cfg.fallbacks.get("org_title", "Unbekannte Organisation"),
        )
        location = _clean_text(row.get("Sitzort", row.get("Sitz", "")))

        payloads = {
            "q04_primary_target_group": _load_question_payload(org_id, "q04_primary_target_group"),
            "q05_founded_year": _load_question_payload(org_id, "q05_founded_year"),
            "q06_financials_manual": _load_question_payload(org_id, "q06_financials_manual"),
        }

        map_status = "ok"
        map_bytes: bytes | None = None
        muni_key = _resolve_municipality_key(location, registry)
        marker_fp = _marker_cache_fingerprint(cfg.map)
        cache_path = map_cache_dir / f"{org_id}_marker_{marker_fp}.png"
        if base_map_bytes is None:
            map_status = "base_map_missing"
        elif cache_path.exists():
            cached = cache_path.read_bytes()
            if _is_usable_map_image(cached):
                map_bytes = cached
            else:
                cache_path.unlink(missing_ok=True)

        if map_bytes is None and base_map_bytes is not None:
            if not muni_key:
                map_status = "municipality_not_mapped"
                map_bytes = base_map_bytes
            else:
                marker = registry.lonlat_by_key.get(muni_key)
                if marker:
                    rendered = _render_marker_map(base_map_bytes, registry, marker, cfg)
                    if rendered and _is_usable_map_image(rendered):
                        map_bytes = rendered
                        cache_path.write_bytes(rendered)
                    else:
                        map_status = "marker_render_failed"
                else:
                    map_status = "municipality_missing_point"

        if map_bytes:
            stats.map_ok += 1
        else:
            stats.map_missing += 1

        output_path = output_dir / f"{org_id}.pdf"
        try:
            html_doc = _build_packet_html(
                cfg=cfg,
                org_id=org_id,
                org_name=org_name,
                location=location,
                map_data_uri=_img_data_uri(map_bytes) if map_bytes else None,
                map_status=map_status,
                generated_at=generated_at,
                payloads=payloads,
            )
            _render_html_to_pdf(html_doc, output_path)
            stats.exported += 1
            manifest_rows.append(
                {
                    "_org_id": org_id,
                    "status": "ok",
                    "pdf_path": str(output_path),
                    "map_status": map_status,
                    "municipality_key": muni_key,
                    "error": "",
                }
            )
        except Exception as exc:
            stats.failed += 1
            manifest_rows.append(
                {
                    "_org_id": org_id,
                    "status": "failed",
                    "pdf_path": str(output_path),
                    "map_status": map_status,
                    "municipality_key": muni_key,
                    "error": str(exc),
                }
            )

    return stats, pd.DataFrame(manifest_rows)


def build_review_pdf_preview_context(
    df: pd.DataFrame,
    *,
    map_cache_dir: Path,
    org_id_filter: str | None = None,
) -> dict[str, object]:
    cfg = _load_pdf_config()
    registry = _load_municipality_registry()
    base = df.copy()
    if "_excluded_reason" in base.columns:
        base = base.loc[base["_excluded_reason"].astype(str).str.strip() == ""].copy()
    if org_id_filter:
        target = org_id_filter.strip()
        base = base[base["_org_id"].astype(str).str.strip() == target]
    if base.empty:
        raise ValueError("No matching row found for preview context")

    row = base.iloc[0]
    org_id = _clean_text(row.get("_org_id", ""))
    if not org_id:
        raise ValueError("Selected row has no _org_id")
    org_name = _fallback_text(
        row.get("Name", row.get("Bezeichnung", "")),
        cfg.fallbacks.get("org_title", "Unbekannte Organisation"),
    )
    location = _clean_text(row.get("Sitzort", row.get("Sitz", "")))

    map_cache_dir.mkdir(parents=True, exist_ok=True)
    base_fp = _base_map_cache_fingerprint(cfg.map)
    base_map_cache = map_cache_dir / f"_base_map_winterthur_masked_{base_fp}.png"
    base_map_meta_cache = map_cache_dir / f"_base_map_winterthur_masked_{base_fp}.meta.json"
    base_map_bytes: bytes | None = None
    if base_map_cache.exists():
        cached = base_map_cache.read_bytes()
        if _is_usable_map_image(cached):
            base_map_bytes = cached
            cached_meta = _load_map_meta(base_map_meta_cache, cfg.map.projection_meta_version)
            if cached_meta:
                registry.map_meta = cached_meta
    if base_map_bytes is None or not isinstance(registry.map_meta, dict):
        fetched = _fetch_base_map_bytes(registry, cfg.map)
        if fetched and _is_usable_map_image(fetched):
            base_map_bytes = fetched
            base_map_cache.write_bytes(fetched)
            _save_map_meta(base_map_meta_cache, registry.map_meta)

    map_status = "ok"
    map_bytes: bytes | None = None
    muni_key = _resolve_municipality_key(location, registry)
    if base_map_bytes is None:
        map_status = "base_map_missing"
    elif not muni_key:
        map_status = "municipality_not_mapped"
        map_bytes = base_map_bytes
    else:
        marker = registry.lonlat_by_key.get(muni_key)
        if marker:
            rendered = _render_marker_map(base_map_bytes, registry, marker, cfg)
            if rendered and _is_usable_map_image(rendered):
                map_bytes = rendered
            else:
                map_status = "marker_render_failed"
        else:
            map_status = "municipality_missing_point"

    payloads = {
        "q04_primary_target_group": _load_question_payload(org_id, "q04_primary_target_group"),
        "q05_founded_year": _load_question_payload(org_id, "q05_founded_year"),
        "q06_financials_manual": _load_question_payload(org_id, "q06_financials_manual"),
    }
    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    return _build_packet_context(
        org_id=org_id,
        org_name=org_name,
        location=location,
        map_data_uri=_img_data_uri(map_bytes) if map_bytes else None,
        map_status=map_status,
        generated_at=generated_at,
        payloads=payloads,
    )
