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
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
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
            outer_rgba = tuple(_cfg("map.marker.outer_rgba", [255, 255, 255, 255]))
            inner_rgba = tuple(_cfg("map.marker.inner_rgba", [220, 25, 32, 255]))
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


def _draw_corner_markers(c: canvas.Canvas, width: float, height: float) -> None:
    offset = float(_cfg("layout.corner_markers.offset", 16))
    size = float(_cfg("layout.corner_markers.size", 16))
    stroke = float(_cfg("layout.corner_markers.stroke", 1.5))
    c.setStrokeColorRGB(0, 0, 0)
    c.setLineWidth(stroke)

    c.line(offset, height - offset, offset + size, height - offset)
    c.line(offset, height - offset, offset, height - offset - size)

    c.line(width - offset, height - offset,
           width - offset - size, height - offset)
    c.line(width - offset, height - offset,
           width - offset, height - offset - size)

    c.line(offset, offset, offset + size, offset)
    c.line(offset, offset, offset, offset + size)

    c.line(width - offset, offset, width - offset - size, offset)
    c.line(width - offset, offset, width - offset, offset + size)


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
    labels = _cfg("labels", {})
    fallbacks = _cfg("texts.fallbacks", {})
    map_box_cfg = _cfg("layout.map_box", {})
    typo = _cfg("typography", {})

    font_bold = (
        str(typo.get("font_bold", "Helvetica-Bold"))
        if isinstance(typo, dict)
        else "Helvetica-Bold"
    )
    font_regular = (
        str(typo.get("font_regular", "Helvetica"))
        if isinstance(typo, dict)
        else "Helvetica"
    )
    title_size = int(typo.get("title_size", 17)) if isinstance(typo, dict) else 17
    meta_size = int(typo.get("meta_size", 9)) if isinstance(typo, dict) else 9
    section_title_size = int(typo.get("section_title_size", 11)) if isinstance(typo, dict) else 11
    small_size = int(typo.get("small_size", 8)) if isinstance(typo, dict) else 8

    output_path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(output_path), pagesize=A4)
    page_w, page_h = A4

    _draw_corner_markers(c, page_w, page_h)

    margin = float(_cfg("layout.page.margin", 36))
    c.setTitle(_fallback_text(org_row.get("_org_id", ""), "org"))

    org_id = _fallback_text(org_row.get("_org_id", ""), str(fallbacks.get("org_id", "unknown")))
    org_name = _fallback_text(
        org_row.get("Name", org_row.get("Bezeichnung", "")),
        str(fallbacks.get("org_title", "Unbekannte Organisation")),
    )
    location = _fallback_text(
        org_row.get("Sitzort", org_row.get("Sitz", "")),
        str(fallbacks.get("location", "Unbekannt")),
    )
    website = _fallback_text(
        org_row.get("_website_url_final", org_row.get("_website_url", "")),
        str(fallbacks.get("website", "Keine Website erfasst")),
    )

    q04 = classify_payloads.get("q04_primary_target_group", {})
    q05 = classify_payloads.get("q05_founded_year", {})
    q06 = classify_payloads.get("q06_financials_manual", {})
    q07 = classify_payloads.get("q07_org_summary_de", {})

    group_key = _clean_text(q04.get("category", "")).lower()
    group_map = _cfg("mappings.target_group", {})
    fallback_unknown = str(fallbacks.get("unknown", "Unbekannt"))
    if isinstance(group_map, dict):
        group_label = str(group_map.get(group_key, fallback_unknown))
    else:
        group_label = fallback_unknown
    subgroup_value = q04.get("subgroup_labels", [])
    subgroup_text = (
        ", ".join(str(item).strip()
                  for item in subgroup_value if str(item).strip())
        if isinstance(subgroup_value, list)
        else ""
    )
    subgroup_text = subgroup_text or str(fallbacks.get("none", "Keine Angabe"))

    finance_status_key = _clean_text(q06.get("information_status", "")).lower()
    finance_map = _cfg("mappings.financial_status", {})
    if isinstance(finance_map, dict):
        finance_status = str(finance_map.get(finance_status_key, fallback_unknown))
    else:
        finance_status = fallback_unknown
    fiscal_year = _fallback_text(
        q06.get("fiscal_year", ""),
        str(fallbacks.get("none", "Keine Angabe")),
    )
    total_earnings = _format_chf(q06.get("total_earnings_chf", ""))
    donated_amount = _format_chf(q06.get("donated_amount_chf", ""))
    founded_year = _fallback_text(
        q05.get("founded_year", ""),
        str(fallbacks.get("none", "Keine Angabe")),
    )

    reliable_none = str(fallbacks.get("reliable_none", "Keine verlaessliche Angabe"))
    legal_form = _fallback_text(org_row.get("_legal_form_final", ""), reliable_none)
    zefix_uid = _fallback_text(
        org_row.get("_zefix_uid", ""),
        str(fallbacks.get("none", "Keine Angabe")),
    )
    zefix_status = _fallback_text(
        org_row.get("_zefix_status", ""),
        str(fallbacks.get("none", "Keine Angabe")),
    )
    zefix_purpose = _fallback_text(org_row.get("_zefix_purpose", ""), reliable_none)
    summary_de = _fallback_text(q07.get("summary_de", ""), reliable_none)

    title_max_chars = int(_cfg("layout.page.title_max_chars", 120))
    title_y = float(_cfg("layout.page.title_y", 50))
    meta_org_id_y = float(_cfg("layout.page.meta_org_id_y", 66))
    meta_export_y = float(_cfg("layout.page.meta_export_y", 78))
    c.setFont(font_bold, title_size)
    c.drawString(margin, page_h - title_y, org_name[:title_max_chars])
    c.setFont(font_regular, meta_size)
    c.drawString(margin, page_h - meta_org_id_y, f"{labels.get('org_id', 'Org ID')}: {org_id}")
    c.drawString(
        margin,
        page_h - meta_export_y,
        f"{labels.get('export', 'Export')}: {generated_at}",
    )

    qr_size = float(_cfg("layout.qr.size", 72))
    qr_y = float(_cfg("layout.qr.y", 86))
    qr_reader = _make_qr_image(
        {"org_id": org_id, "v": str(_cfg("texts.qr_version", "review_pdf_v1"))}
    )
    c.drawImage(
        qr_reader,
        page_w - margin - qr_size,
        page_h - qr_y,
        width=qr_size,
        height=qr_size,
        mask="auto",
    )

    top_y = page_h - float(_cfg("layout.page.top_y", 104))
    section_cfg = _cfg("layout.sections", {})
    if not isinstance(section_cfg, dict):
        section_cfg = {}
    master_height = float(section_cfg.get("master_height", 156))
    master_title_y = float(section_cfg.get("master_title_y", 18))
    master_start_y = float(section_cfg.get("master_start_y", 36))
    target_gap = float(section_cfg.get("target_gap_from_master", 180))
    target_height = float(section_cfg.get("target_height", 102))
    target_title_y = float(section_cfg.get("target_title_y", 20))
    target_start_y = float(section_cfg.get("target_start_y", 38))
    finance_gap = float(section_cfg.get("finance_gap_from_target", 124))
    finance_height = float(section_cfg.get("finance_height", 94))
    finance_title_y = float(section_cfg.get("finance_title_y", 20))
    finance_start_y = float(section_cfg.get("finance_start_y", 38))
    summary_gap = float(section_cfg.get("summary_gap_from_finance", 116))
    summary_height = float(section_cfg.get("summary_height", 114))
    summary_title_y = float(section_cfg.get("summary_title_y", 20))
    summary_start_y = float(section_cfg.get("summary_start_y", 38))
    manual_gap = float(section_cfg.get("manual_gap_from_summary", 136))
    manual_height = float(section_cfg.get("manual_height", 86))
    manual_title_y = float(section_cfg.get("manual_title_y", 20))
    manual_start_y = float(section_cfg.get("manual_start_y", 38))
    manual_step_y = float(section_cfg.get("manual_check_step_y", 14))

    kv_cfg = _cfg("layout.kv", {})
    if not isinstance(kv_cfg, dict):
        kv_cfg = {}
    gap_after = float(kv_cfg.get("line_gap_after", 3))
    gap_after_compact = float(kv_cfg.get("line_gap_after_compact", 2))
    master_value_width = float(kv_cfg.get("master_value_width", 380))
    target_value_width = float(kv_cfg.get("target_value_width", 420))
    finance_value_width = float(kv_cfg.get("finance_value_width", 420))

    c.rect(
        margin,
        top_y - (master_height + 12),
        page_w - 2 * margin,
        master_height,
        stroke=1,
        fill=0,
    )
    c.setFont(font_bold, section_title_size)
    c.drawString(margin + 8, top_y - master_title_y, str(labels.get("master", "Stammdaten")))
    y = top_y - master_start_y
    y = _draw_kv(
        c, margin + 8, y, str(labels.get("loc", "Ort")), location, master_value_width
    ) - gap_after
    y = _draw_kv(
        c,
        margin + 8,
        y,
        str(labels.get("website", "Website")),
        website,
        master_value_width,
    ) - gap_after
    y = _draw_kv(
        c,
        margin + 8,
        y,
        str(labels.get("legal_form", "Rechtsform")),
        legal_form,
        master_value_width,
    ) - gap_after
    y = _draw_kv(
        c,
        margin + 8,
        y,
        str(labels.get("zefix_uid", "ZEFIX UID")),
        zefix_uid,
        master_value_width,
    ) - gap_after
    _draw_kv(
        c,
        margin + 8,
        y,
        str(labels.get("zefix_status", "ZEFIX Status")),
        zefix_status,
        master_value_width,
    )

    map_x = page_w - margin - float(map_box_cfg.get("x_offset_from_right", 210))
    map_y = top_y - float(map_box_cfg.get("y_offset_from_top", 156))
    map_w = float(map_box_cfg.get("width", 200))
    map_h = float(map_box_cfg.get("height", 118))
    c.rect(map_x, map_y, map_w, map_h, stroke=1, fill=0)
    c.setFont(font_bold, meta_size)
    map_title_y = map_y + map_h - float(map_box_cfg.get("title_offset_y", 12))
    c.drawString(map_x + 4, map_title_y, str(labels.get("map", "Standort")))
    if map_image_bytes:
        inner_padding = float(map_box_cfg.get("inner_padding", 3))
        image_top_padding = float(map_box_cfg.get("image_top_padding", 20))
        c.drawImage(
            ImageReader(BytesIO(map_image_bytes)),
            map_x + inner_padding,
            map_y + inner_padding,
            width=map_w - (2 * inner_padding),
            height=map_h - image_top_padding,
            preserveAspectRatio=True,
            mask="auto",
        )
    else:
        c.setFont(font_regular, small_size)
        c.drawString(map_x + 6, map_y + 12,
                     f"{fallbacks.get('map_unavailable', 'Karte nicht verfuegbar')} ({map_status})")

    sec2_top = top_y - target_gap
    c.rect(
        margin,
        sec2_top - (target_height + 10),
        page_w - 2 * margin,
        target_height,
        stroke=1,
        fill=0,
    )
    c.setFont(font_bold, section_title_size)
    c.drawString(
        margin + 8,
        sec2_top - target_title_y,
        str(labels.get("target", "Zielgruppe und Zweck")),
    )
    y2 = sec2_top - target_start_y
    y2 = _draw_kv(
        c,
        margin + 8,
        y2,
        str(labels.get("target_main", "Hauptzielgruppe")),
        group_label,
        420,
    ) - gap_after_compact
    y2 = _draw_kv(
        c,
        margin + 8,
        y2,
        str(labels.get("target_sub", "Untergruppen")),
        subgroup_text,
        420,
    ) - gap_after_compact
    _draw_kv(
        c,
        margin + 8,
        y2,
        str(labels.get("purpose", "Verifizierter Zweck")),
        zefix_purpose,
        target_value_width,
    )

    sec3_top = sec2_top - finance_gap
    c.rect(
        margin,
        sec3_top - (finance_height + 10),
        page_w - 2 * margin,
        finance_height,
        stroke=1,
        fill=0,
    )
    c.setFont(font_bold, section_title_size)
    c.drawString(margin + 8, sec3_top - finance_title_y, str(labels.get("finance", "Finanzen")))
    y3 = sec3_top - finance_start_y
    y3 = _draw_kv(c, margin + 8, y3, str(labels.get("finance_status", "Informationsstatus")),
                  finance_status, finance_value_width) - gap_after_compact
    y3 = _draw_kv(
        c,
        margin + 8,
        y3,
        str(labels.get("fiscal_year", "Geschaeftsjahr")),
        fiscal_year,
        finance_value_width,
    ) - gap_after_compact
    y3 = _draw_kv(
        c,
        margin + 8,
        y3,
        str(labels.get("earnings", "Jahresertrag")),
        total_earnings,
        finance_value_width,
    ) - gap_after_compact
    y3 = _draw_kv(
        c,
        margin + 8,
        y3,
        str(labels.get("donations", "Spendenbetrag")),
        donated_amount,
        finance_value_width,
    ) - gap_after_compact
    _draw_kv(
        c,
        margin + 8,
        y3,
        str(labels.get("founded", "Gruendungsjahr")),
        founded_year,
        finance_value_width,
    )

    sec4_top = sec3_top - summary_gap
    c.rect(
        margin,
        sec4_top - (summary_height + 10),
        page_w - 2 * margin,
        summary_height,
        stroke=1,
        fill=0,
    )
    c.setFont(font_bold, section_title_size)
    c.drawString(
        margin + 8,
        sec4_top - summary_title_y,
        str(labels.get("summary", "Kurzbeschreibung")),
    )
    _draw_wrapped_text(
        c,
        summary_de,
        margin + 8,
        sec4_top - summary_start_y,
        page_w - 2 * margin - 16,
        font_regular,
        meta_size,
        11,
        8,
    )

    sec5_top = sec4_top - manual_gap
    c.rect(
        margin,
        sec5_top - (manual_height + 10),
        page_w - 2 * margin,
        manual_height,
        stroke=1,
        fill=0,
    )
    c.setFont(font_bold, section_title_size)
    c.drawString(
        margin + 8,
        sec5_top - manual_title_y,
        str(labels.get("manual_review", "Manuelle Endpruefung")),
    )
    c.setFont(font_regular, meta_size)
    checks = labels.get("manual_review_checks", []) if isinstance(labels, dict) else []
    if not checks:
        checks = [
            "Daten plausibel",
            "Zielgruppe korrekt",
            "Finanzangaben korrekt/erganzt",
            "Weiterer Abklaerungsbedarf",
        ]
    check_y = sec5_top - manual_start_y
    for item in checks:
        c.rect(margin + 8, check_y - 3, 8, 8, stroke=1, fill=0)
        c.drawString(margin + 22, check_y - 1, item)
        check_y -= manual_step_y

    c.setFont(font_regular, small_size)
    footer_y = float(_cfg("layout.footer.y", 24))
    c.drawString(margin, footer_y, str(_cfg("texts.packet_version", "benefind review packet v1")))
    c.drawRightString(page_w - margin, footer_y, f"Map: {map_status}")

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
    base_map_bytes: bytes | None = None
    if base_map_cache.exists():
        cached_base = base_map_cache.read_bytes()
        if _is_usable_map_image(cached_base):
            base_map_bytes = cached_base
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
        cache_path = map_cache_dir / f"{org_id}.png"
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
                map_bytes = cached
            else:
                marker = registry.lonlat_by_key.get(muni_key)
                if marker:
                    rendered = _render_marker_map(
                        base_map_bytes, registry, marker)
                    if rendered and _is_usable_map_image(rendered):
                        map_bytes = rendered
                        cache_path.write_bytes(rendered)
                    else:
                        map_status = "marker_render_failed"
                else:
                    map_status = "municipality_missing_point"
        else:
            marker = registry.lonlat_by_key.get(muni_key)
            if marker:
                rendered = _render_marker_map(base_map_bytes, registry, marker)
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
