"""Render filter-funnel snakey visuals (SVG + PNG) from metadata.

Orchestration only: loads inputs, delegates domain mapping to
benefind.diagram.filter_funnel, runs layout + render, exports PNG.
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
from benefind.diagram.snakey import LayoutConfig, SnakeyStyle, layout_snakey, render_svg

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
) -> tuple[LayoutConfig, SnakeyStyle]:
    layout = LayoutConfig()
    style = SnakeyStyle()
    if path is None:
        return layout, style

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
        layout, style = _load_config_file(parent_path, _seen=seen)

    if isinstance(raw.get("layout"), dict):
        layout = _apply_overrides(layout, raw["layout"])  # type: ignore[arg-type]
    if isinstance(raw.get("style"), dict):
        style = _apply_overrides(style, raw["style"])  # type: ignore[arg-type]
    return layout, style


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
# PNG export
# ---------------------------------------------------------------------------


def _export_png_with_playwright(
    svg_path: Path,
    png_path: Path,
    width: int,
    height: int,
    scale: int,
) -> None:
    html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<style>body{margin:0;background:#eaeef3;display:flex;justify-content:center;"
        "align-items:flex-start;padding:20px;}img{width:100%;height:auto;}</style></head>"
        f"<body><img src='{svg_path.name}' alt='snakey'/></body></html>"
    )
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        html_path = tmp_path / "render.html"
        tmp_svg = tmp_path / svg_path.name
        tmp_png = tmp_path / "out.png"
        html_path.write_text(html, encoding="utf-8")
        tmp_svg.write_text(svg_path.read_text(encoding="utf-8"), encoding="utf-8")

        py = (
            "from playwright.sync_api import sync_playwright\n"
            "with sync_playwright() as p:\n"
            "    browser = p.chromium.launch()\n"
            f"    page = browser.new_page(viewport={{'width': {width}, 'height': {height}}}, "
            f"device_scale_factor={scale})\n"
            f"    page.goto('file://{html_path.as_posix()}')\n"
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--config", type=Path, default=None, help="Layout/style TOML or JSON")
    parser.add_argument("--comments", type=Path, default=None, help="Step context JSON")
    parser.add_argument("--format", choices=["svg", "png", "both"], default="both")
    parser.add_argument("--orientation", choices=["top_down", "left_right"], default=None)
    parser.add_argument("--branch-side", choices=["right", "left", "alternate"], default=None)
    parser.add_argument("--stage-label-side", choices=["right", "left", "alternate"],
                        default=None)
    parser.add_argument("--width", type=int, default=None)
    parser.add_argument("--height", type=int, default=None)
    parser.add_argument("--scale", type=int, default=2)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    with args.input.open("r", encoding="utf-8") as f:
        meta = json.load(f)

    config_base, style = _load_config_file(args.config)
    step_context = _load_step_context(args.comments)

    model = build_model(meta, step_context)
    overrides: dict[str, object] = {}
    if args.width is not None:
        overrides["width"] = args.width
    if args.height is not None:
        overrides["height"] = args.height
    if args.orientation is not None:
        overrides["orientation"] = args.orientation
    if args.branch_side is not None:
        overrides["branch_side_policy"] = args.branch_side
    if args.stage_label_side is not None:
        overrides["stage_label_side_policy"] = args.stage_label_side
    config = replace(config_base, **overrides) if overrides else config_base
    scene = layout_snakey(model, config, style)
    render_svg(scene, args.output)
    print(f"Wrote SVG: {args.output}")

    if args.format in {"png", "both"}:
        png_path = args.output.with_suffix(".png")
        _export_png_with_playwright(args.output, png_path, scene.width, scene.height, args.scale)
        print(f"Wrote PNG: {png_path}")


if __name__ == "__main__":
    main()
