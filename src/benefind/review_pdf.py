"""Export print-ready A4 review PDFs per organization."""

from __future__ import annotations

import json
import math
import os
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
import qrcode
from PIL import Image, ImageDraw
from playwright.sync_api import sync_playwright
from reportlab.graphics.barcode import code128
from reportlab.lib.colors import Color
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

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
class PdfFontRoles:
    label: str
    content: str

@lru_cache(maxsize=1)
def _load_review_pdf_config() -> dict[str, object]:
    path = PROJECT_ROOT / "config" / "review_pdf.toml"
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("review_pdf config must be a TOML table")
    return raw


def _cfg(path: str, default: object = None) -> object:
    cursor: object = _load_review_pdf_config()
    for token in path.split("."):
        if not isinstance(cursor, dict):
            return default
        if token not in cursor:
            return default
        cursor = cursor[token]
    return cursor


@lru_cache(maxsize=1)
def _resolve_pdf_fonts() -> PdfFontRoles:
    font_files = _cfg("typography.font_files", {})
    if not isinstance(font_files, dict):
        font_files = {}

    def _find_font_path(candidates: list[str]) -> Path | None:
        for rel_path in candidates:
            if not rel_path:
                continue
            path = PROJECT_ROOT / rel_path
            if path.exists():
                return path
        return None

    def register_if_present(alias: str, key: str, fallback_candidates: list[str]) -> str | None:
        configured = str(font_files.get(key, "")).strip()
        candidates = [configured] if configured else []
        candidates.extend(fallback_candidates)
        path = _find_font_path(candidates)
        if path is None:
            return None
        try:
            pdfmetrics.registerFont(TTFont(alias, str(path)))
            return alias
        except Exception:
            return None

    unna_alias = register_if_present(
        "Unna",
        "unna",
        [
            "assets/fonts/Unna-Regular.ttf",
            "assets/fonts/unna/Unna-Regular.ttf",
            "fonts/Unna-Regular.ttf",
        ],
    )
    dmsans_alias = register_if_present(
        "DM Sans",
        "dm_sans",
        [
            "assets/fonts/DMSans-Regular.ttf",
            "assets/fonts/DM Sans Regular.ttf",
            "assets/fonts/dm-sans/DMSans-Regular.ttf",
            "assets/fonts/dm-sans/static/DMSans-Regular.ttf",
            "assets/fonts/dm-sans/DMSans-VariableFont_opsz,wght.ttf",
            "fonts/DMSans-Regular.ttf",
        ],
    )

    fallback_serif_alias = register_if_present(
        "Source Serif 4",
        "fallback_serif",
        [
            "assets/fonts/SourceSerif4-Regular.ttf",
            "assets/fonts/source-serif-4/SourceSerif4-Regular.ttf",
            "assets/fonts/source-serif-4/static/SourceSerif4-Regular.ttf",
            "fonts/SourceSerif4-Regular.ttf",
        ],
    )
    fallback_sans_alias = register_if_present(
        "Source Sans 3",
        "fallback_sans",
        [
            "assets/fonts/SourceSans3-Regular.ttf",
            "assets/fonts/source-sans-3/SourceSans3-Regular.ttf",
            "assets/fonts/source-sans-3/static/SourceSans3-Regular.ttf",
            "fonts/SourceSans3-Regular.ttf",
        ],
    )

    label_font = str(_cfg("typography.font_label", "Unna")).strip() or "Unna"
    content_font = str(_cfg("typography.font_content", "DM Sans")).strip() or "DM Sans"
    serif_fallback = fallback_serif_alias or "Times-Roman"
    sans_fallback = fallback_sans_alias or "Helvetica"

    if label_font == "Unna" and not unna_alias:
        label_font = serif_fallback
    elif label_font == "DM Sans" and not dmsans_alias:
        label_font = sans_fallback

    if content_font == "DM Sans" and not dmsans_alias:
        content_font = sans_fallback
    elif content_font == "Unna" and not unna_alias:
        content_font = serif_fallback

    return PdfFontRoles(label=label_font, content=content_font)


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value or "").strip()


def _fallback_text(value: object, fallback: str) -> str:
    text = _clean_text(value)
    return text if text else fallback


def _format_chf(value: object) -> str:
    none_text = str(_cfg("texts.fallbacks.none", "Keine Angabe"))
    text = _clean_text(value)
    if not text:
        return none_text
    try:
        amount = float(text)
    except ValueError:
        return none_text
    rounded = int(round(amount))
    chunks = []
    digits = str(abs(rounded))
    while digits:
        chunks.append(digits[-3:])
        digits = digits[:-3]
    joined = "'".join(reversed(chunks))
    if rounded < 0:
        joined = f"-{joined}"
    return f"{joined} CHF"


def _load_question_payload(org_id: str, question_id: str) -> dict[str, object]:
    ask_path = PROJECT_ROOT / "data" / "orgs" / \
        org_id / "classify" / question_id / "ask.json"
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


def _normalize_location_name(value: str) -> str:
    text = str(value or "").strip().lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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
            mapped = _normalize_location_name(
                str(row.get("maps_to_key", "") or ""))
            if not alias or not mapped:
                continue
            if mapped not in lonlat_by_key:
                raise ValueError(
                    f"Alias target not found in registry: {mapped}")
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
    ytile = (1.0 - math.log(math.tan(lat_rad) +
             (1.0 / math.cos(lat_rad))) / math.pi) / 2.0 * n
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
    layer_id = str(_cfg("map.boundary.layer_id", default_layer))
    feature_id = int(_cfg("map.boundary.feature_id", 110))
    sr = int(_cfg("map.boundary.sr", 4326))
    url = (
        "https://api3.geo.admin.ch/rest/services/api/MapServer/"
        f"{layer_id}/{feature_id}"
    )
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
        for ring in coords:
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

    if gtype == "MultiPolygon" and isinstance(coords, list):
        for polygon in coords:
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
    timeout_seconds: int = 20,
    width: int = 900,
    height: int = 500,
    zoom_override: float | None = None,
) -> bytes | None:
    should_probe = bool(_cfg("map.probe_vector_tile", True))
    if should_probe and not _fetch_vector_tile_probe(timeout_seconds=timeout_seconds):
        return None

    width = int(_cfg("map.width_px", width))
    height = int(_cfg("map.height_px", height))

    coords = list(registry.lonlat_by_key.values())
    if not coords:
        return None
    lons = [lon for lon, _ in coords]
    lats = [lat for _, lat in coords]
    pad_lon = float(_cfg("map.pad_lon", 0.03))
    pad_lat = float(_cfg("map.pad_lat", 0.02))
    min_lon = min(lons) - pad_lon
    max_lon = max(lons) + pad_lon
    min_lat = min(lats) - pad_lat
    max_lat = max(lats) + pad_lat

    center_lon = (min_lon + max_lon) / 2.0
    center_lat = (min_lat + max_lat) / 2.0
    default_zoom = float(_cfg("map.default_zoom", 12.8))
    zoom = zoom_override if zoom_override is not None else default_zoom
    # Keep geographic coverage equivalent to the historical full-district frame
    # while allowing higher detail rendering at higher zoom levels.
    base_zoom = float(_cfg("map.base_zoom_for_coverage", 9.6))
    max_render_dim = int(_cfg("map.max_render_dim_px", 10000))
    max_render_pixels = int(_cfg("map.max_render_pixels", 60_000_000))
    zoom_delta = max(0.0, zoom - base_zoom)
    target_scale = 2.0**zoom_delta
    max_scale_dim = min(max_render_dim / float(width),
                        max_render_dim / float(height))
    max_scale_px = math.sqrt(max_render_pixels / float(width * height))
    max_scale = min(max_scale_dim, max_scale_px)
    render_scale = max(1.0, min(target_scale, max_scale))
    render_width = max(1, int(round(width * render_scale)))
    render_height = max(1, int(round(height * render_scale)))

    screenshot_bytes: bytes | None = None
    html = """<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <link href='https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css' rel='stylesheet'>
  <style>
    html, body, #map { margin: 0; padding: 0; width: 100%; height: 100%; background: #ffffff; }
    .maplibregl-control-container { display: none; }
  </style>
</head>
<body>
  <div id='map'></div>
  <script src='https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js'></script>
  <script>
    window.__renderDone = false;
    const map = new maplibregl.Map({
      container: 'map',
      style: '__STYLE_URL__',
      center: [__CENTER_LON__, __CENTER_LAT__],
      zoom: __ZOOM__,
      attributionControl: false,
      interactive: false,
      fadeDuration: 0,
      pitchWithRotate: false,
      dragRotate: false,
      touchZoomRotate: false,
      preserveDrawingBuffer: true,
    });
    map.once('idle', () => {
      setTimeout(() => { window.__renderDone = true; }, __RENDER_WAIT_MS__);
    });
  </script>
</body>
</html>
"""
    html = (
        html.replace("__CENTER_LON__", f"{center_lon:.9f}")
        .replace("__CENTER_LAT__", f"{center_lat:.9f}")
        .replace("__ZOOM__", f"{zoom:.4f}")
        .replace("__STYLE_URL__", str(_cfg("map.style_url", "")))
        .replace("__RENDER_WAIT_MS__", str(int(_cfg("map.render_wait_ms", 350))))
    )

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(
                viewport={"width": render_width, "height": render_height})
            page.set_content(html, wait_until="load")
            page.wait_for_function(
                "window.__renderDone === true",
                timeout=int(_cfg("map.render_timeout_ms", 20000)),
            )
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

    boundary_rings = _fetch_district_boundary_rings_lonlat(
        timeout_seconds=timeout_seconds)
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

    crop_width = float(alpha_bbox[2] - alpha_bbox[0]
                       ) if alpha_bbox else float(full_width)
    crop_height = float(alpha_bbox[3] - alpha_bbox[1]
                        ) if alpha_bbox else float(full_height)

    target_w = int(width)
    target_h = int(height)
    if target_w <= 0 or target_h <= 0:
        return None

    if crop_width > 0 and crop_height > 0:
        uniform_scale = min(target_w / crop_width, target_h / crop_height)
    else:
        uniform_scale = 1.0
    resized_w = max(1, int(round(crop_width * uniform_scale)))
    resized_h = max(1, int(round(crop_height * uniform_scale)))

    if composed.size != (resized_w, resized_h):
        composed = composed.resize(
            (resized_w, resized_h), resample=Image.Resampling.LANCZOS)

    canvas_img = Image.new("RGBA", (target_w, target_h), (0, 0, 0, 0))
    pad_x = (target_w - resized_w) // 2
    pad_y = (target_h - resized_h) // 2
    canvas_img.paste(composed, (pad_x, pad_y), composed)
    composed = canvas_img

    output_width = float(target_w)
    output_height = float(target_h)

    effective_zoom = zoom - math.log2(render_scale)
    scale_x = uniform_scale
    scale_y = uniform_scale

    registry.map_meta = {
        "center_lon": center_lon,
        "center_lat": center_lat,
        "zoom": effective_zoom,
        "width": output_width,
        "height": output_height,
        "crop_x0": float(crop_x0),
        "crop_y0": float(crop_y0),
        "full_width": float(full_width),
        "full_height": float(full_height),
        "scale_x": float(scale_x),
        "scale_y": float(scale_y),
        "pad_x": float(pad_x),
        "pad_y": float(pad_y),
        "render_scale": float(render_scale),
    }

    buffer = BytesIO()
    composed.save(buffer, format="PNG")
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
            near_white = sum(1 for r, g, b in values if r >=
                             245 and g >= 245 and b >= 245)
            if near_white / len(values) > 0.995:
                return False
            return True
    except Exception:
        return False


def _render_marker_map(
    base_image_bytes: bytes,
    registry: MunicipalityRegistry,
    lonlat: tuple[float, float],
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
            full_x = (xtf - cx_tf) * 256.0 + \
                (float(meta.get("full_width", width)) / 2.0)
            full_y = (ytf - cy_tf) * 256.0 + \
                (float(meta.get("full_height", height)) / 2.0)
            x = int(round((full_x - crop_x0) * scale_x + pad_x))
            y = int(round((full_y - crop_y0) * scale_y + pad_y))
            x = max(0, min(width - 1, x))
            y = max(0, min(height - 1, y))

            radius_outer = int(_cfg("map.marker.outer_radius_px", 30))
            radius_inner = int(_cfg("map.marker.inner_radius_px", 20))
            outer_color = _theme_color_for("map_marker_outer", "100")
            inner_color = _theme_color_for("map_marker_inner", "700")
            outer_rgba = (
                int(round(outer_color.red * 255)),
                int(round(outer_color.green * 255)),
                int(round(outer_color.blue * 255)),
                255,
            )
            inner_rgba = (
                int(round(inner_color.red * 255)),
                int(round(inner_color.green * 255)),
                int(round(inner_color.blue * 255)),
                255,
            )
            draw.ellipse(
                [x - radius_outer, y - radius_outer,
                    x + radius_outer, y + radius_outer],
                fill=outer_rgba,
            )
            draw.ellipse(
                [x - radius_inner, y - radius_inner,
                    x + radius_inner, y + radius_inner],
                fill=inner_rgba,
            )

            buffer = BytesIO()
            img.save(buffer, format="PNG")
            return buffer.getvalue()
    except Exception:
        return None


def _make_qr_image(payload: dict[str, str]) -> ImageReader:
    qr = qrcode.QRCode(version=2, box_size=8, border=1)
    qr.add_data(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").get_image()
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return ImageReader(buffer)


def _crop_transparent_edges(image_bytes: bytes) -> bytes:
    try:
        with Image.open(BytesIO(image_bytes)) as raw:
            img = raw.convert("RGBA")
            alpha_bbox = img.getchannel("A").getbbox()
            if not alpha_bbox:
                return image_bytes
            cropped = img.crop(alpha_bbox)
            buffer = BytesIO()
            cropped.save(buffer, format="PNG")
            return buffer.getvalue()
    except Exception:
        return image_bytes


def _load_map_meta(cache_meta_path: Path) -> dict[str, float] | None:
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
    return normalized if normalized else None


def _save_map_meta(cache_meta_path: Path, map_meta: dict[str, float] | None) -> None:
    if not isinstance(map_meta, dict) or not map_meta:
        return
    payload = {key: float(value) for key, value in map_meta.items()}
    cache_meta_path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _draw_corner_markers(c: canvas.Canvas, width: float, height: float) -> None:
    offset = float(_cfg("layout.corner_markers.offset", 16))
    size = float(_cfg("layout.corner_markers.size", 16))
    stroke = float(_cfg("layout.corner_markers.stroke", 1.5))
    dot_radius = float(_cfg("layout.corner_markers.dot_radius", 1.8))
    dot_inset = float(_cfg("layout.corner_markers.dot_inset", 5.0))
    marker_color = _theme_color_for("corner_marker", "600")
    dot_fill_color = _theme_color_for("corner_dot_fill", "600")

    c.setStrokeColor(marker_color)
    c.setFillColor(dot_fill_color)
    c.setLineWidth(stroke)
    c.setLineCap(1)
    c.setLineJoin(1)

    def draw_corner(x: float, y: float, dx: float, dy: float) -> None:
        path = c.beginPath()
        path.moveTo(x + (dx * size), y)
        path.lineTo(x, y)
        path.lineTo(x, y + (dy * size))
        c.drawPath(path, stroke=1, fill=0)

        dot_x = x + (dx * dot_inset)
        dot_y = y + (dy * dot_inset)
        c.setFillColor(dot_fill_color)
        c.circle(dot_x, dot_y, dot_radius, stroke=0, fill=1)
        c.setLineWidth(stroke)

    draw_corner(offset, height - offset, 1.0, -1.0)
    draw_corner(width - offset, height - offset, -1.0, -1.0)
    draw_corner(offset, offset, 1.0, 1.0)
    draw_corner(width - offset, offset, -1.0, 1.0)


def _srgb_channel_from_linear(channel: float) -> float:
    if channel <= 0.0031308:
        return 12.92 * channel
    return 1.055 * (channel ** (1.0 / 2.4)) - 0.055


def _oklch_to_color(lightness: float, chroma: float, hue: float) -> Color:
    h_rad = math.radians(hue)
    a = chroma * math.cos(h_rad)
    b = chroma * math.sin(h_rad)

    l_ = lightness + 0.3963377774 * a + 0.2158037573 * b
    m_ = lightness - 0.1055613458 * a - 0.0638541728 * b
    s_ = lightness - 0.0894841775 * a - 1.2914855480 * b

    l_lin = l_ * l_ * l_
    m_lin = m_ * m_ * m_
    s_lin = s_ * s_ * s_

    r_lin = +4.0767416621 * l_lin - 3.3077115913 * m_lin + 0.2309699292 * s_lin
    g_lin = -1.2684380046 * l_lin + 2.6097574011 * m_lin - 0.3413193965 * s_lin
    b_lin = -0.0041960863 * l_lin - 0.7034186147 * m_lin + 1.7076147010 * s_lin

    r = max(0.0, min(1.0, _srgb_channel_from_linear(max(0.0, r_lin))))
    g = max(0.0, min(1.0, _srgb_channel_from_linear(max(0.0, g_lin))))
    bl = max(0.0, min(1.0, _srgb_channel_from_linear(max(0.0, b_lin))))
    return Color(r, g, bl)


@lru_cache(maxsize=1)
def _load_review_theme_tokens() -> dict[str, str]:
    rel = str(_cfg("theme.palette_file", "config/review_pdf_theme.css"))
    path = PROJECT_ROOT / rel
    if not path.exists():
        return {}
    css = path.read_text(encoding="utf-8")
    matches = re.findall(r"--([a-z0-9-]+)\s*:\s*([^;]+);", css, flags=re.IGNORECASE)
    return {name.strip().lower(): value.strip() for name, value in matches}


def _color_from_token_value(value: str) -> Color:
    raw = value.strip().lower()
    if raw.startswith("#"):
        hex_value = raw[1:]
        if len(hex_value) == 3:
            hex_value = "".join(ch * 2 for ch in hex_value)
        if len(hex_value) == 6:
            r = int(hex_value[0:2], 16) / 255.0
            g = int(hex_value[2:4], 16) / 255.0
            b = int(hex_value[4:6], 16) / 255.0
            return Color(r, g, b)
    match = re.match(
        r"oklch\(\s*([0-9.]+)%\s+([0-9.]+)\s+([0-9.]+)\s*\)",
        raw,
        flags=re.IGNORECASE,
    )
    if match:
        lightness = float(match.group(1)) / 100.0
        chroma = float(match.group(2))
        hue = float(match.group(3))
        return _oklch_to_color(lightness, chroma, hue)
    return Color(71.0 / 255.0, 85.0 / 255.0, 105.0 / 255.0)


def _theme_color_for(element: str, default_shade: str) -> Color:
    family = str(_cfg("theme.family", "slate")).strip().lower() or "slate"
    shade = str(_cfg(f"theme.shades.{element}", default_shade)).strip().lower() or default_shade
    token_name = f"color-{family}-{shade}"
    tokens = _load_review_theme_tokens()
    if token_name in tokens:
        return _color_from_token_value(tokens[token_name])
    fallback_name = f"color-slate-{default_shade}"
    if fallback_name in tokens:
        return _color_from_token_value(tokens[fallback_name])
    return Color(71.0 / 255.0, 85.0 / 255.0, 105.0 / 255.0)


def _theme_named_color(name: str, fallback: Color) -> Color:
    tokens = _load_review_theme_tokens()
    token_name = f"color-{name.strip().lower()}"
    if token_name in tokens:
        return _color_from_token_value(tokens[token_name])
    return fallback


def _draw_label_box(
    c: canvas.Canvas,
    *,
    text: str,
    x: float,
    y_top: float,
    font_name: str,
) -> float:
    cfg = _cfg("layout.labels", {})
    if not isinstance(cfg, dict):
        cfg = {}
    font_size = float(cfg.get("font_size", 8))
    h_pad = float(cfg.get("padding_x", 6))
    v_pad = float(cfg.get("padding_y", 2.5))
    radius = float(cfg.get("radius", 3))

    label = text.strip().upper()
    text_width = c.stringWidth(label, font_name, font_size)
    box_w = text_width + (2.0 * h_pad)
    box_h = font_size + (2.0 * v_pad)
    y = y_top - box_h

    c.setFillColor(_theme_color_for("label_bg", "700"))
    c.roundRect(x, y, box_w, box_h, radius, stroke=0, fill=1)

    c.setFillColor(_theme_named_color("white", Color(1, 1, 1)))
    c.setFont(font_name, font_size)
    c.drawString(x + h_pad, y + v_pad + 0.4, label)
    return y


def _draw_wrapped_text(
    c: canvas.Canvas,
    text: str,
    x: float,
    y: float,
    width: float,
    font_name: str,
    font_size: float,
    leading: float,
    max_lines: int,
) -> float:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        proposal = word if not current else f"{current} {word}"
        if c.stringWidth(proposal, font_name, font_size) <= width:
            current = proposal
            continue
        if current:
            lines.append(current)
        current = word
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)

    c.setFont(font_name, font_size)
    cursor_y = y
    for line in lines[:max_lines]:
        c.drawString(x, cursor_y, line)
        cursor_y -= leading
    return cursor_y


def _draw_kv(
    c: canvas.Canvas,
    x: float,
    y: float,
    label: str,
    value: str,
    value_width: float,
) -> float:
    font_bold = str(_cfg("typography.font_bold", "Helvetica-Bold"))
    font_regular = str(_cfg("typography.font_regular", "Helvetica"))
    meta_size = int(_cfg("typography.meta_size", 9))
    c.setFont(font_bold, meta_size)
    c.drawString(x, y, f"{label}:")
    c.setFont(font_regular, meta_size)
    value_offset_x = float(_cfg("layout.kv.value_offset_x", 110))
    leading = float(_cfg("layout.kv.leading", 11))
    max_lines = int(_cfg("layout.kv.max_lines", 3))
    return _draw_wrapped_text(
        c,
        value,
        x + value_offset_x,
        y,
        value_width,
        font_regular,
        meta_size,
        leading,
        max_lines,
    )


def _export_single_pdf(
    output_path: Path,
    org_row: pd.Series,
    classify_payloads: dict[str, dict[str, object]],
    map_image_bytes: bytes | None,
    map_status: str,
    generated_at: str,
) -> None:
    fonts = _resolve_pdf_fonts()
    fallbacks = _cfg("texts.fallbacks", {})
    org_id = _fallback_text(org_row.get("_org_id", ""), str(fallbacks.get("org_id", "unknown")))
    org_name = _fallback_text(
        org_row.get("Name", org_row.get("Bezeichnung", "")),
        str(fallbacks.get("org_title", "Unbekannte Organisation")),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(output_path), pagesize=A4)
    page_w, page_h = A4

    _draw_corner_markers(c, page_w, page_h)

    margin = float(_cfg("layout.page.margin", 36))
    title_y = float(_cfg("layout.page.title_y", 50))
    title_size = int(_cfg("typography.title_size", 17))
    title_leading = float(_cfg("typography.title_leading", title_size * 1.18))
    label_gap = float(_cfg("layout.labels.gap_below", 8))
    title_left_offset = float(_cfg("layout.page.title_left_offset", 18))
    title_width_ratio = float(_cfg("layout.page.title_width_ratio", 0.67))
    configured_title_font = str(_cfg("typography.font_title", "Unna")).strip() or "Unna"
    if configured_title_font in pdfmetrics.getRegisteredFontNames():
        title_font = configured_title_font
    elif "Unna" in pdfmetrics.getRegisteredFontNames():
        title_font = "Unna"
    else:
        title_font = fonts.label
    title_text = org_name
    available_width = page_w - (2.0 * margin)
    title_width = max(120.0, available_width * max(0.2, min(1.0, title_width_ratio)))
    title_x = margin + title_left_offset
    label_top_y = page_h - title_y
    label_font = fonts.content
    label_bottom_y = _draw_label_box(
        c,
        text="Name",
        x=title_x,
        y_top=label_top_y,
        font_name=label_font,
    )
    title_top_y = label_bottom_y - label_gap

    map_cfg = _cfg("layout.map_box", {})
    if not isinstance(map_cfg, dict):
        map_cfg = {}
    map_w = float(map_cfg.get("width", 200))
    map_h = float(map_cfg.get("height", 118))
    map_top_offset = float(map_cfg.get("top_offset", 0))
    map_x = page_w - margin - map_w
    map_y = (page_h - title_y - map_top_offset) - map_h

    if map_image_bytes:
        inner_padding = float(map_cfg.get("inner_padding", 3))
        map_bytes_for_pdf = _crop_transparent_edges(map_image_bytes)
        c.drawImage(
            ImageReader(BytesIO(map_bytes_for_pdf)),
            map_x + inner_padding,
            map_y + inner_padding,
            width=map_w - (2 * inner_padding),
            height=map_h - (2 * inner_padding),
            preserveAspectRatio=True,
            mask="auto",
        )

    words = title_text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        proposal = word if not current else f"{current} {word}"
        if c.stringWidth(proposal, title_font, title_size) <= title_width:
            current = proposal
            continue
        if current:
            lines.append(current)
            current = word
        else:
            lines.append(word)
            current = ""
    if current:
        lines.append(current)
    if not lines:
        lines = [title_text]

    c.setFont(title_font, title_size)
    c.setFillColor(_theme_color_for("title", "900"))
    line_y = title_top_y
    for line in lines:
        c.drawString(title_x, line_y, line)
        line_y -= title_leading

    corner_cfg = _cfg("layout.corner_markers", {})
    if not isinstance(corner_cfg, dict):
        corner_cfg = {}
    marker_offset = float(corner_cfg.get("offset", 16))
    marker_size = float(corner_cfg.get("size", 20))

    barcode_cfg = _cfg("layout.barcode", {})
    if not isinstance(barcode_cfg, dict):
        barcode_cfg = {}
    module_width = float(barcode_cfg.get("module_width", 0.42))
    side_gap = float(barcode_cfg.get("side_gap", 18))

    bar_height = marker_size
    y = marker_offset

    target_width = max(40.0, page_w - (2.0 * (marker_offset + marker_size + side_gap)))
    marker_color = _theme_color_for("barcode", "600")
    barcode = code128.Code128(
        org_id,
        barHeight=bar_height,
        barWidth=module_width,
        barFillColor=marker_color,
    )
    if barcode.width > 0:
        fitted_module_width = module_width * (target_width / barcode.width)
        module_width = max(0.28, min(1.2, fitted_module_width))
        barcode = code128.Code128(
            org_id,
            barHeight=bar_height,
            barWidth=module_width,
            barFillColor=marker_color,
        )

    x = (page_w - barcode.width) / 2.0
    barcode.drawOn(c, x, y)

    c.showPage()
    c.save()


def export_review_pdfs(
    df: pd.DataFrame,
    *,
    output_dir: Path,
    org_id_filter: str | None,
    limit: int | None,
    map_cache_dir: Path,
) -> tuple[ExportStats, pd.DataFrame]:
    stats = ExportStats(total_rows=int(len(df)))
    registry = _load_municipality_registry()

    base = df.copy()
    if "_excluded_reason" in base.columns:
        active_mask = base["_excluded_reason"].astype(str).str.strip() == ""
        base = base.loc[active_mask].copy()
    if org_id_filter:
        target = org_id_filter.strip()
        base = base[base["_org_id"].astype(str).str.strip() == target]
    if limit is not None and limit > 0:
        base = base.head(limit)

    stats.selected_rows = int(len(base))
    generated_at = datetime.now(UTC).isoformat(timespec="seconds")

    manifest_rows: list[dict[str, str]] = []
    map_cache_dir.mkdir(parents=True, exist_ok=True)

    default_zoom = float(_cfg("map.default_zoom", 12.8))
    zoom_env = str(os.getenv("BENEFIND_REVIEW_MAP_ZOOM", "")).strip()
    zoom_override: float | None = default_zoom
    if zoom_env:
        try:
            zoom_override = float(zoom_env)
        except ValueError:
            zoom_override = default_zoom

    zoom_cache_suffix = f"_z{zoom_override:.2f}" if zoom_override is not None else ""
    base_map_cache = map_cache_dir / \
        f"_base_map_winterthur_masked{zoom_cache_suffix}.png"
    base_map_meta_cache = map_cache_dir / \
        f"_base_map_winterthur_masked{zoom_cache_suffix}.meta.json"
    base_map_bytes: bytes | None = None
    if base_map_cache.exists():
        cached_base = base_map_cache.read_bytes()
        if _is_usable_map_image(cached_base):
            base_map_bytes = cached_base
    if base_map_bytes is not None and not isinstance(registry.map_meta, dict):
        cached_meta = _load_map_meta(base_map_meta_cache)
        if cached_meta:
            registry.map_meta = cached_meta
    if base_map_bytes is not None and not isinstance(registry.map_meta, dict):
        fetched_for_meta = _fetch_base_map_bytes(
            registry,
            width=int(_cfg("map.width_px", 900)),
            height=int(_cfg("map.height_px", 500)),
            zoom_override=zoom_override,
        )
        if fetched_for_meta and _is_usable_map_image(fetched_for_meta):
            base_map_bytes = fetched_for_meta
            base_map_cache.write_bytes(fetched_for_meta)
            _save_map_meta(base_map_meta_cache, registry.map_meta)
    if base_map_bytes is None:
        fetched_base = _fetch_base_map_bytes(
            registry,
            width=int(_cfg("map.width_px", 900)),
            height=int(_cfg("map.height_px", 500)),
            zoom_override=zoom_override,
        )
        if fetched_base and _is_usable_map_image(fetched_base):
            base_map_bytes = fetched_base
            base_map_cache.write_bytes(fetched_base)
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

        payloads = {
            "q04_primary_target_group": _load_question_payload(org_id, "q04_primary_target_group"),
            "q05_founded_year": _load_question_payload(org_id, "q05_founded_year"),
            "q06_financials_manual": _load_question_payload(org_id, "q06_financials_manual"),
            "q07_org_summary_de": _load_question_payload(org_id, "q07_org_summary_de"),
        }

        location = _clean_text(row.get("Sitzort", row.get("Sitz", "")))
        marker_outer = str(_cfg("theme.shades.map_marker_outer", "100")).strip()
        marker_inner = str(_cfg("theme.shades.map_marker_inner", "700")).strip()
        marker_family = str(_cfg("theme.family", "slate")).strip().lower() or "slate"
        marker_cache_suffix = f"_{marker_family}_{marker_outer}_{marker_inner}"
        cache_path = map_cache_dir / f"{org_id}{marker_cache_suffix}.png"
        map_bytes: bytes | None = None
        map_status = "ok"

        muni_key = _resolve_municipality_key(location, registry)
        if base_map_bytes is None:
            map_status = "base_map_missing"
        elif not muni_key:
            map_status = "municipality_not_mapped"
            map_bytes = base_map_bytes
        elif cache_path.exists():
            cached = cache_path.read_bytes()
            if _is_usable_map_image(cached):
                cropped_cached = _crop_transparent_edges(cached)
                map_bytes = cropped_cached
                if cropped_cached != cached:
                    cache_path.write_bytes(cropped_cached)
            else:
                marker = registry.lonlat_by_key.get(muni_key)
                if marker:
                    rendered = _render_marker_map(
                        base_map_bytes, registry, marker)
                    if rendered and _is_usable_map_image(rendered):
                        cropped_rendered = _crop_transparent_edges(rendered)
                        map_bytes = cropped_rendered
                        cache_path.write_bytes(cropped_rendered)
                    else:
                        map_status = "marker_render_failed"
                else:
                    map_status = "municipality_missing_point"
        else:
            marker = registry.lonlat_by_key.get(muni_key)
            if marker:
                rendered = _render_marker_map(base_map_bytes, registry, marker)
                if rendered and _is_usable_map_image(rendered):
                    cropped_rendered = _crop_transparent_edges(rendered)
                    map_bytes = cropped_rendered
                    cache_path.write_bytes(cropped_rendered)
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
            _export_single_pdf(
                output_path=output_path,
                org_row=row,
                classify_payloads=payloads,
                map_image_bytes=map_bytes,
                map_status=map_status,
                generated_at=generated_at,
            )
            stats.exported += 1
            manifest_rows.append(
                {
                    "_org_id": org_id,
                    "status": "ok",
                    "pdf_path": str(output_path),
                    "map_status": map_status,
                    "municipality_key": muni_key,
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
            continue

        manifest_rows[-1]["error"] = ""

    manifest_df = pd.DataFrame(manifest_rows)
    return stats, manifest_df
