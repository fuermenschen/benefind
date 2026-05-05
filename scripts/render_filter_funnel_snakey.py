"""Render filter-funnel snakey visuals (SVG + PNG + PDF) from metadata.

Orchestration only: loads inputs, delegates domain mapping to
benefind.diagram.filter_funnel, runs layout + render, exports raster/vector variants.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import tomllib
from dataclasses import fields, replace
from pathlib import Path

from benefind.diagram.filter_funnel import build_model
from benefind.diagram.snakey import (
    LayoutConfig,
    PageLayoutConfig,
    SnakeyStyle,
    layout_snakey,
    render_html,
    render_svg,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = REPO_ROOT / "data" / "meta" / "filter_funnel_meta.json"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "meta" / "filter_funnel_snakey.svg"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _coerce_dataclass_value(raw: object, target_type: object) -> object:
    if target_type is int:
        return int(raw)  # type: ignore[arg-type]
    if target_type is float:
        return float(raw)  # type: ignore[arg-type]
    if target_type is str:
        return str(raw)
    return raw


def _apply_overrides(dataclass_obj: object, raw_overrides: dict[str, object]) -> object:
    allowed = {field.name: field.type for field in fields(dataclass_obj)}  # type: ignore[arg-type]
    updates: dict[str, object] = {}
    for key, value in raw_overrides.items():
        if key not in allowed:
            continue
        updates[key] = _coerce_dataclass_value(value, allowed[key])
    return replace(dataclass_obj, **updates)  # type: ignore[arg-type]


def _load_raw_toml(path: Path) -> dict[str, object]:
    """Parse a TOML or JSON file and return the raw dict."""
    with path.open("rb") as f:
        if path.suffix.lower() == ".json":
            raw: object = json.loads(f.read().decode())
        else:
            raw = tomllib.load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"Config root must be an object/table: {path}")
    return raw  # type: ignore[return-value]


def _load_config_file(
    path: Path | None,
    *,
    _seen: frozenset[Path] | None = None,
) -> tuple[LayoutConfig, SnakeyStyle, PageLayoutConfig]:
    layout = LayoutConfig()
    style = SnakeyStyle()
    page = PageLayoutConfig()
    if path is None:
        return layout, style, page

    path = path.resolve()
    seen = (_seen or frozenset()) | {path}
    raw = _load_raw_toml(path)

    # --- resolve extends (parent config) first, then overlay child keys ---
    extends_val = raw.get("extends")
    if extends_val is not None:
        if not isinstance(extends_val, str):
            raise ValueError(
                f"'extends' must be a string filename, got {type(extends_val).__name__!r}: {path}"
            )
        parent_path = (path.parent / extends_val).resolve()
        if not parent_path.exists():
            raise FileNotFoundError(
                f"Config 'extends' parent not found: {parent_path} (referenced from {path})"
            )
        if parent_path in seen:
            chain = " -> ".join(str(p) for p in [*seen, parent_path])
            raise ValueError(f"Cyclic 'extends' detected: {chain}")
        layout, style, page = _load_config_file(parent_path, _seen=seen)

    if isinstance(raw.get("layout"), dict):
        layout = _apply_overrides(layout, raw["layout"])  # type: ignore[arg-type]
    if isinstance(raw.get("style"), dict):
        style = _apply_overrides(style, raw["style"])  # type: ignore[arg-type]
    if isinstance(raw.get("page"), dict):
        page = _apply_overrides(page, raw["page"])  # type: ignore[arg-type]
    return layout, style, page


def _paper_size_px(size: str, orientation: str) -> tuple[int, int]:
    # CSS reference pixels at 96 DPI.
    mm_to_px = 96.0 / 25.4
    presets_mm: dict[str, tuple[float, float]] = {
        "A5": (148.0, 210.0),
        "A4": (210.0, 297.0),
        "Letter": (215.9, 279.4),
    }
    w_mm, h_mm = presets_mm[size]
    if orientation == "landscape":
        w_mm, h_mm = h_mm, w_mm
    return int(round(w_mm * mm_to_px)), int(round(h_mm * mm_to_px))


def _resolve_page_size(
    page: PageLayoutConfig,
    scene_width: int,
    scene_height: int,
) -> tuple[int, int]:
    if page.size_mode == "content":
        pad = max(page.page_padding_px, 0)
        return scene_width + (pad * 2), scene_height + (pad * 2)
    if page.size_mode == "paper":
        return _paper_size_px(page.paper_size, page.paper_orientation)
    width = max(int(page.page_width_px), 1)
    height = max(int(page.page_height_px), 1)
    return width, height


def _load_step_context(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("Comments file must be a JSON object.")
    by_step = raw.get("step_context", raw)
    if not isinstance(by_step, dict):
        raise ValueError("step_context must be a JSON object.")
    return {str(k): str(v) for k, v in by_step.items() if str(v).strip()}


# ---------------------------------------------------------------------------
# PNG / PDF export via Playwright
# ---------------------------------------------------------------------------
# Both functions load the generated .html file (which references the .svg by
# filename) so that the title/subtitle are rendered by the browser.


def _export_png_with_playwright(
    html_path: Path,
    svg_path: Path,
    png_path: Path,
    page_width: int,
    page_height: int,
    scale: int,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        tmp_html = tmp_path / html_path.name
        tmp_svg = tmp_path / svg_path.name
        tmp_png = tmp_path / "out.png"
        tmp_html.write_text(html_path.read_text(encoding="utf-8"), encoding="utf-8")
        tmp_svg.write_text(svg_path.read_text(encoding="utf-8"), encoding="utf-8")

        py = (
            "from playwright.sync_api import sync_playwright\n"
            "with sync_playwright() as p:\n"
            "    browser = p.chromium.launch()\n"
            "    page = browser.new_page("
            f"viewport={{'width': {page_width}, 'height': {page_height}}}, "
            f"device_scale_factor={scale})\n"
            f"    page.goto('file://{tmp_html.as_posix()}')\n"
            "    page.wait_for_timeout(250)\n"
            f"    page.screenshot(path='{tmp_png.as_posix()}', full_page=True)\n"
            "    browser.close()\n"
        )
        try:
            subprocess.run(["uv", "run", "python", "-c", py], check=True, cwd=REPO_ROOT)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "PNG export failed. Ensure Playwright browsers are installed: "
                "`uv run playwright install chromium`."
            ) from exc

        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_bytes(tmp_png.read_bytes())


def _export_pdf_with_playwright(
    html_path: Path,
    svg_path: Path,
    pdf_path: Path,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        tmp_html = tmp_path / html_path.name
        tmp_svg = tmp_path / svg_path.name
        tmp_pdf = tmp_path / "out.pdf"
        tmp_html.write_text(html_path.read_text(encoding="utf-8"), encoding="utf-8")
        tmp_svg.write_text(svg_path.read_text(encoding="utf-8"), encoding="utf-8")

        py = (
            "from playwright.sync_api import sync_playwright\n"
            "with sync_playwright() as p:\n"
            "    browser = p.chromium.launch()\n"
            "    page = browser.new_page(viewport={'width': 2000, 'height': 2000})\n"
            f"    page.goto('file://{tmp_html.as_posix()}')\n"
            "    page.wait_for_load_state('networkidle')\n"
            "    page.wait_for_timeout(250)\n"
            "    root = page.locator('#page-root')\n"
            "    box = root.bounding_box()\n"
            "    if box is None:\n"
            "        raise RuntimeError('Could not measure HTML page bounds for PDF export.')\n"
            "    page_height = max(int(round(box['height'])), 1)\n"
            "    page_width = max(int(round(box['width'])), 1)\n"
            "    page.pdf("
            f"path='{tmp_pdf.as_posix()}', "
            "print_background=True, "
            "width=f'{page_width}px', "
            "height=f'{page_height}px', "
            "margin={'top': '0', 'right': '0', 'bottom': '0', 'left': '0'}, "
            "prefer_css_page_size=False)\n"
            "    browser.close()\n"
        )
        try:
            subprocess.run(["uv", "run", "python", "-c", py], check=True, cwd=REPO_ROOT)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "PDF export failed. Ensure Playwright browsers are installed: "
                "`uv run playwright install chromium`."
            ) from exc

        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(tmp_pdf.read_bytes())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--config", type=Path, default=None, help="Layout/style TOML or JSON")
    parser.add_argument("--comments", type=Path, default=None, help="Step context JSON")
    parser.add_argument("--format", choices=["svg", "png", "pdf", "both", "all"], default="both")
    parser.add_argument("--orientation", choices=["top_down", "left_right"], default=None)
    parser.add_argument("--branch-side", choices=["right", "left", "alternate"], default=None)
    parser.add_argument("--stage-label-side", choices=["right", "left", "alternate"],
                        default=None)
    parser.add_argument("--width", type=int, default=None)
    parser.add_argument("--height", type=int, default=None)
    parser.add_argument("--page-size-mode", choices=["content", "pixels", "paper"], default=None)
    parser.add_argument("--page-fit", choices=["contain", "none"], default=None)
    parser.add_argument("--page-align-x", choices=["left", "center", "right"], default=None)
    parser.add_argument("--page-align-y", choices=["top", "center", "bottom"], default=None)
    parser.add_argument("--page-paper", choices=["A5", "A4", "Letter"], default=None)
    parser.add_argument("--page-orientation", choices=["portrait", "landscape"], default=None)
    parser.add_argument("--page-padding", type=int, default=None)
    parser.add_argument("--scale", type=int, default=2)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    with args.input.open("r", encoding="utf-8") as f:
        meta = json.load(f)

    config_base, style, page_base = _load_config_file(args.config)
    step_context = _load_step_context(args.comments)

    model = build_model(meta, step_context)
    overrides: dict[str, object] = {}
    if args.orientation is not None:
        overrides["orientation"] = args.orientation
    if args.branch_side is not None:
        overrides["branch_side_policy"] = args.branch_side
    if args.stage_label_side is not None:
        overrides["stage_label_side_policy"] = args.stage_label_side
    config = replace(config_base, **overrides) if overrides else config_base
    page_overrides: dict[str, object] = {}
    if args.page_size_mode is not None:
        page_overrides["size_mode"] = args.page_size_mode
    if args.page_fit is not None:
        page_overrides["fit_mode"] = args.page_fit
    if args.page_align_x is not None:
        page_overrides["align_x"] = args.page_align_x
    if args.page_align_y is not None:
        page_overrides["align_y"] = args.page_align_y
    if args.page_paper is not None:
        page_overrides["paper_size"] = args.page_paper
    if args.page_orientation is not None:
        page_overrides["paper_orientation"] = args.page_orientation
    if args.width is not None:
        page_overrides["page_width_px"] = args.width
    if args.height is not None:
        page_overrides["page_height_px"] = args.height
    if args.page_padding is not None:
        page_overrides["page_padding_px"] = args.page_padding
    page = replace(page_base, **page_overrides) if page_overrides else page_base

    scene = layout_snakey(model, config, style)
    page_width, page_height = _resolve_page_size(page, scene.width, scene.height)

    svg_path = args.output
    html_path = args.output.with_suffix(".html")

    render_svg(scene, svg_path)
    print(f"Wrote SVG:  {svg_path}")
    render_html(
        scene,
        html_path,
        page=page,
        page_width=page_width,
        page_height=page_height,
    )
    print(f"Wrote HTML: {html_path}")

    if args.format in {"png", "both", "all"}:
        png_path = svg_path.with_suffix(".png")
        _export_png_with_playwright(
            html_path,
            svg_path,
            png_path,
            page_width,
            page_height,
            args.scale,
        )
        print(f"Wrote PNG:  {png_path}")

    if args.format in {"pdf", "both", "all"}:
        pdf_path = svg_path.with_suffix(".pdf")
        _export_pdf_with_playwright(html_path, svg_path, pdf_path)
        print(f"Wrote PDF:  {pdf_path}")


if __name__ == "__main__":
    main()
