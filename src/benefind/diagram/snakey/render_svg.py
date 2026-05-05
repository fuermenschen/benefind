from __future__ import annotations

import base64
import html
from pathlib import Path

from .types import NodeAnchor, Scene, SnakeyStyle, TextAnchor


def _font_mime_type(font_path: Path) -> str:
    ext = font_path.suffix.lower()
    if ext == ".woff2":
        return "font/woff2"
    if ext == ".woff":
        return "font/woff"
    if ext == ".otf":
        return "font/otf"
    return "font/ttf"


def _font_format_hint(font_path: Path) -> str:
    ext = font_path.suffix.lower()
    if ext == ".woff2":
        return "woff2"
    if ext == ".woff":
        return "woff"
    if ext == ".otf":
        return "opentype"
    return "truetype"


def _embedded_font_css(scene: Scene) -> str:
    style = scene.style
    repo_root = Path(__file__).resolve().parents[4]
    parts: list[str] = []
    for font_file in style.embedded_font_files:
        path = Path(font_file)
        if not path.is_absolute():
            cwd_candidate = Path.cwd() / path
            repo_candidate = repo_root / path
            path = cwd_candidate if cwd_candidate.exists() else repo_candidate
        if not path.exists() or not path.is_file():
            continue
        font_b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        mime = _font_mime_type(path)
        fmt = _font_format_hint(path)
        parts.append(
            "@font-face {"
            f"font-family: '{style.embedded_font_family}';"
            f"src: url('data:{mime};base64,{font_b64}') format('{fmt}');"
            "font-style: normal;"
            "font-weight: normal;"
            "}"
        )
    return "".join(parts)


def _bezier_path(x0: float, y0: float, x1: float, y1: float, bend: float = 0.5) -> str:
    """Simple single-segment cubic bezier."""
    dx = x1 - x0
    dy = y1 - y0
    if abs(dy) >= abs(dx):
        c = dy * bend
        return (
            f"M {x0:.2f} {y0:.2f} "
            f"C {x0:.2f} {y0 + c:.2f}, {x1:.2f} {y1 - c:.2f}, {x1:.2f} {y1:.2f}"
        )
    c = dx * bend
    return (
        f"M {x0:.2f} {y0:.2f} "
        f"C {x0 + c:.2f} {y0:.2f}, {x1 - c:.2f} {y1:.2f}, {x1:.2f} {y1:.2f}"
    )


def _elbow_path(
    x0: float, y0: float,
    wx: float, wy: float,
    x1: float, y1: float,
) -> str:
    """Two-segment cubic bezier via an elbow waypoint (wx, wy).

    Segment A (source → waypoint): departs horizontally/vertically, arrives at waypoint.
    Segment B (waypoint → target):  departs from waypoint tangent, arrives at target.

    The waypoint sits at the lane's secondary position but at an intermediate primary,
    so the branch sweeps laterally first, then curves to the final node position.
    This prevents branches from slicing through downstream labels.
    """
    # Control points: CP1 stays at source (depart along primary), CP2 at waypoint level.
    # Then second cubic departs from waypoint tangent and arrives smoothly at target.
    dx0 = wx - x0
    dy0 = wy - y0
    dx1 = x1 - wx
    dy1 = y1 - wy

    # Segment A: source → waypoint
    # CP1: stay at source x, move partway in y (or vice-versa for left-right)
    # CP2: arrive at waypoint from same direction
    if abs(dy0) >= abs(dx0):
        c0 = dy0 * 0.6
        cp1x, cp1y = x0, y0 + c0
        cp2x, cp2y = wx, wy - c0 * 0.2
    else:
        c0 = dx0 * 0.6
        cp1x, cp1y = x0 + c0, y0
        cp2x, cp2y = wx - c0 * 0.2, wy

    # Segment B: waypoint → target (smooth continuation)
    if abs(dy1) >= abs(dx1):
        c1 = dy1 * 0.6
        cp3x, cp3y = wx, wy + c1 * 0.2
        cp4x, cp4y = x1, y1 - c1
    else:
        c1 = dx1 * 0.6
        cp3x, cp3y = wx + c1 * 0.2, wy
        cp4x, cp4y = x1 - c1, y1

    return (
        f"M {x0:.2f} {y0:.2f} "
        f"C {cp1x:.2f} {cp1y:.2f}, {cp2x:.2f} {cp2y:.2f}, {wx:.2f} {wy:.2f} "
        f"C {cp3x:.2f} {cp3y:.2f}, {cp4x:.2f} {cp4y:.2f}, {x1:.2f} {y1:.2f}"
    )


def _spline_path(
    x0: float, y0: float,
    wx: float, wy: float,
    x1: float, y1: float,
    tension: float = 0.5,
    handle_scale: float = 0.4,
) -> str:
    """S-curve cubic bezier for top-down orientation.

    Single cubic — no handles at the midpoint. Handle directions lerp between:

    tension=0 (axis-aligned):
      P1 out: horizontal right
      P3 in:  horizontal left

    tension=1 (chord-directed):
      P1 out: toward P3
      P3 in:  toward P1

    handle_scale controls handle length as a fraction of the chord length.
    """
    dx, dy = x1 - x0, y1 - y0
    chord = (dx ** 2 + dy ** 2) ** 0.5
    if chord == 0:
        return f"M {x0:.2f} {y0:.2f} L {x1:.2f} {y1:.2f}"
    ux, uy = dx / chord, dy / chord

    hlen = chord * handle_scale

    # tension=0 directions (horizontal)
    p1_out_0 = (1.0, 0.0)
    p3_in_0 = (-1.0, 0.0)

    # tension=1 directions (chord-directed)
    p1_out_1 = (ux, uy)
    p3_in_1 = (-ux, -uy)

    def lerp_dir(d0: tuple[float, float], d1: tuple[float, float]) -> tuple[float, float]:
        vx = d0[0] + tension * (d1[0] - d0[0])
        vy = d0[1] + tension * (d1[1] - d0[1])
        mag = (vx ** 2 + vy ** 2) ** 0.5
        if mag == 0:
            return (0.0, 0.0)
        return (vx / mag, vy / mag)

    d1 = lerp_dir(p1_out_0, p1_out_1)
    d3 = lerp_dir(p3_in_0, p3_in_1)

    cp1x = x0 + d1[0] * hlen
    cp1y = y0 + d1[1] * hlen
    cp2x = x1 + d3[0] * hlen
    cp2y = y1 + d3[1] * hlen

    return (
        f"M {x0:.2f} {y0:.2f} "
        f"C {cp1x:.2f} {cp1y:.2f}, {cp2x:.2f} {cp2y:.2f}, {x1:.2f} {y1:.2f}"
    )


def _spline_path_lr(
    x0: float, y0: float,
    wx: float, wy: float,
    x1: float, y1: float,
) -> str:
    """Spline variant for left-right orientation."""
    cp1x = x0
    cp1y = wy
    cp2x = x1
    cp2y = wy
    return (
        f"M {x0:.2f} {y0:.2f} "
        f"C {cp1x:.2f} {cp1y:.2f}, {cp2x:.2f} {cp2y:.2f}, {x1:.2f} {y1:.2f}"
    )


def _line_style(kind: str, scene: Scene, highlight: bool = False) -> tuple[int, str, str]:
    style = scene.style
    if highlight and kind == "title":
        # Only the title line gets the accent color and heavier weight
        return (
            style.block_title_size,
            style.highlight_stage_label_title_color,
            style.highlight_stage_label_font_weight,
        )
    if kind == "title":
        return style.block_title_size, style.text_color, "600"
    if kind == "count":
        return style.block_count_size, style.text_subtle_color, "500"
    return style.block_context_size, style.context_color, "400"


def _render_text_anchor(parts: list[str], ta: TextAnchor, scene: Scene) -> None:
    """Emit SVG <text> elements for all lines in a TextAnchor.

    Coordinate model:
      ta.x, ta.y = top-left corner of the block bounding box.
      ta.align   = SVG text-anchor ("start" or "end").

    When align="start":  all lines share x = ta.x + padding_x
    When align="end":    all lines share x = ta.x + block.width - padding_x
      (so the right edge of text aligns with ta.x + block.width)
    """
    style = scene.style
    if not ta.block.lines:
        return

    if ta.align == "end":
        x_pos = ta.x + ta.block.width - style.block_padding_x
    else:
        x_pos = ta.x + style.block_padding_x

    y = ta.y + style.block_padding_y + style.block_line_height * 0.8

    for line in ta.block.lines:
        font_size, color, weight = _line_style(line.kind, scene, highlight=ta.highlight)
        parts.append(
            f'<text x="{x_pos:.2f}" y="{y:.2f}" '
            f'text-anchor="{ta.align}" '
            f'font-family="{style.font_family}" font-size="{font_size}" '
            f'font-weight="{weight}" fill="{color}">{line.text}</text>'
        )
        y += style.block_line_height


def _badge_offset(
    na: NodeAnchor,
    badge_index: int,
    use_ring: bool,
    stroke_half: float,
    style: SnakeyStyle,
    scene: Scene,
) -> tuple[float, float, str, float]:
    """Compute (bx, by, text_anchor, rotation_deg) for a badge near a highlighted node.

    Clearance = stroke_half + ring_extent + highlight_badge_gap, so the badge
    always sits outside the widest part of the trunk at that node.

    rotation_deg: degrees to rotate the SVG badge icon so it points toward the node.
      The default chevron path points downward (toward a node below it), so:
        above  → 180°  (flip to point up)
        below  →   0°  (default down)
        left   →  90°  (rotate left)
        right  → 270°  (rotate right)
    """
    ring_extent = (style.highlight_node_ring_width + 1.5) if use_ring else 0.0
    # For SVG icons the badge position is the icon centre, so add half the icon size
    # to ensure the nearest edge of the icon clears the ring/stroke by highlight_badge_gap.
    icon_half = style.highlight_badge_size / 2.0 if style.highlight_badge_svg else 0.0
    clearance = stroke_half + ring_extent + style.highlight_badge_gap + icon_half

    side = scene.config.highlight_badge_side
    orientation = scene.config.orientation

    # Resolve relative sides to absolute cardinal directions
    if side == "outward":
        abs_side = ("above" if badge_index == 0 else "below") if orientation == "top_down" \
                   else ("left" if badge_index == 0 else "right")
    elif side == "inward":
        abs_side = ("below" if badge_index == 0 else "above") if orientation == "top_down" \
                   else ("right" if badge_index == 0 else "left")
    elif side == "left_right":
        abs_side = "left" if badge_index == 0 else "right"
    elif side == "right_left":
        abs_side = "right" if badge_index == 0 else "left"
    else:
        abs_side = side  # absolute: "above"/"below"/"left"/"right"

    # SVG badge rotation: chevron points in the direction of flow, not at the node.
    # Default path points downward (0°). top_down flows down → 0°, left_right flows right → 270°.
    rotation_deg = 0.0 if orientation == "top_down" else 270.0

    if abs_side == "above":
        return na.x, na.y - clearance, "middle", rotation_deg
    if abs_side == "below":
        return na.x, na.y + clearance, "middle", rotation_deg
    if abs_side == "left":
        return na.x - clearance, na.y, "end", rotation_deg
    # right
    return na.x + clearance, na.y, "start", rotation_deg


def _render_node_anchor(
    parts: list[str],
    na: NodeAnchor,
    scene: Scene,
    badge_index: int | None = None,
    stroke_half: float = 0.0,
) -> None:
    """Render a node circle, with optional highlight treatment.

    badge_index: 0 = first highlighted node (start), 1 = last (end).
                 Only used when highlight_mode includes 'badge'.
    stroke_half: half the trunk stroke width at this node — used to push
                 the badge clear of the trunk ribbon.
    """
    style = scene.style
    mode = scene.config.highlight_mode

    if na.role == "main_trunk":
        base_fill = style.main_trunk_node_fill
    else:
        base_fill = style.exclusion_node_fill

    if not na.highlight:
        parts.append(
            f'<circle cx="{na.x:.2f}" cy="{na.y:.2f}" '
            f'r="{na.radius:.1f}" fill="{base_fill}"/>'
        )
        return

    # --- highlighted node: decompose mode into feature flags ---
    use_ring = "ring" in mode
    use_fill = "fill" in mode
    use_badge = "badge" in mode

    fill = style.highlight_node_fill if use_fill else base_fill

    # Outer ring (drawn before node so it sits behind the circle)
    if use_ring:
        ring_r = na.radius + style.highlight_node_ring_width + 1.5
        parts.append(
            f'<circle cx="{na.x:.2f}" cy="{na.y:.2f}" '
            f'r="{ring_r:.1f}" fill="none" '
            f'stroke="{style.highlight_node_ring_color}" '
            f'stroke-width="{style.highlight_node_ring_width:.1f}"/>'
        )

    # Node circle
    parts.append(
        f'<circle cx="{na.x:.2f}" cy="{na.y:.2f}" '
        f'r="{na.radius:.1f}" fill="{fill}"/>'
    )

    # Badge label
    if use_badge and badge_index is not None:
        bx, by, anchor, rot = _badge_offset(
            na, badge_index, use_ring, stroke_half, style, scene
        )
        svg_path = style.highlight_badge_svg
        if svg_path:
            # Inline SVG icon: scale from 24×24 to highlight_badge_size, rotate to point at node
            sz = style.highlight_badge_size
            scale = sz / 24.0
            # Translate so the icon is centred on (bx, by)
            tx = bx - sz / 2.0
            ty = by - sz / 2.0
            # Rotation transform around the icon centre
            parts.append(
                f'<g transform="translate({tx:.2f},{ty:.2f}) '
                f'rotate({rot:.0f},{sz/2:.1f},{sz/2:.1f}) '
                f'scale({scale:.4f})">'
                f'<path d="{svg_path}" fill="none" '
                f'stroke="{style.highlight_badge_color}" '
                f'stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>'
                f'</g>'
            )
        else:
            badge_texts = style.highlight_badge_text
            label = badge_texts[badge_index] if badge_index < len(badge_texts) else ""
            if label:
                parts.append(
                    f'<text x="{bx:.2f}" y="{by:.2f}" '
                    f'text-anchor="{anchor}" dominant-baseline="middle" '
                    f'font-family="{style.font_family}" '
                    f'font-size="{style.highlight_badge_font_size}" '
                    f'font-weight="700" '
                    f'fill="{style.highlight_badge_color}">{label}</text>'
                )


def render_svg(scene: Scene, output_path: Path) -> None:
    style = scene.style
    parts: list[str] = []

    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{scene.width}" '
        f'height="{scene.height}" viewBox="0 0 {scene.width} {scene.height}">'
    )

    # Definitions (font embedding for standalone SVG portability)
    parts.append("<defs>")
    font_css = _embedded_font_css(scene)
    if font_css:
        parts.append(f"<style>{font_css}</style>")
    parts.append("</defs>")

    # Edges — paths computed here from final (post-canvas-fit) node positions
    node_map = {na.key: na for na in scene.node_anchors}
    for er in scene.edge_routes:
        if er.value <= 0:
            continue
        src = node_map[er.source_key]
        tgt = node_map[er.target_key]
        if er.waypoint is not None:
            frac = scene.config.branch_waypoint_primary_frac
            orientation = scene.config.orientation
            if orientation == "top_down":
                wp_x = tgt.x
                wp_y = src.y + frac * (tgt.y - src.y)
            else:
                wp_x = src.x + frac * (tgt.x - src.x)
                wp_y = tgt.y
            if scene.config.branch_route_mode == "spline":
                if orientation == "top_down":
                    path = _spline_path(src.x, src.y, wp_x, wp_y, tgt.x, tgt.y,
                                        tension=scene.config.branch_spline_tension,
                                        handle_scale=scene.config.branch_spline_handle_scale)
                else:
                    path = _spline_path_lr(
                        src.x, src.y, wp_x, wp_y, tgt.x, tgt.y)
            else:
                path = _elbow_path(src.x, src.y, wp_x, wp_y, tgt.x, tgt.y)
        else:
            path = _bezier_path(src.x, src.y, tgt.x, tgt.y)
        if er.role == "main_trunk":
            color = style.main_trunk_color
            opacity = style.main_trunk_opacity
        else:
            color = style.exclusion_branch_color
            opacity = style.exclusion_branch_opacity
        parts.append(
            f'<path d="{path}" fill="none" stroke="{color}" '
            f'stroke-opacity="{opacity}" stroke-linecap="round" '
            f'stroke-width="{er.stroke_width:.2f}"/>'
        )

    # Node circles (drawn on top of edges)
    # Build a map of node_key → max trunk stroke_width for badge clearance.
    trunk_stroke: dict[str, float] = {}
    for er in scene.edge_routes:
        if er.role == "main_trunk":
            for key in (er.source_key, er.target_key):
                trunk_stroke[key] = max(trunk_stroke.get(key, 0.0), er.stroke_width)

    # Track highlighted nodes in order to assign badge_index (0=start, 1=end).
    highlighted_keys = [na.key for na in scene.node_anchors if na.highlight]
    for na in scene.node_anchors:
        badge_index: int | None = None
        if na.highlight and highlighted_keys:
            if na.key == highlighted_keys[0]:
                badge_index = 0
            elif na.key == highlighted_keys[-1]:
                badge_index = 1
        stroke_half = trunk_stroke.get(na.key, 0.0) * 0.5
        _render_node_anchor(parts, na, scene, badge_index, stroke_half)

    # Text labels
    for ta in scene.text_anchors:
        if ta.role == "main_trunk":
            continue
        _render_text_anchor(parts, ta, scene)

    parts.append("</svg>")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def render_html(
    scene: Scene,
    output_path: Path,
    *,
    page_width: int,
) -> None:
    """Write an HTML file that places the title/subtitle above the SVG diagram.

    The SVG is referenced by filename; both files must reside in the same
    directory.  Title and subtitle are laid out by the browser using native
    font metrics, so long strings wrap correctly without any manual estimation.
    """
    style = scene.style
    pad = scene.config.canvas_fit_padding
    svg_name = html.escape(output_path.with_suffix(".svg").name)

    # Reuse the same @font-face CSS already embedded in the SVG so the title
    # renders in the same typeface.
    font_css = _embedded_font_css(scene)

    title_html = f'<div class="title">{html.escape(scene.title)}</div>' if scene.title else ""
    subtitle_html = (
        f'<div class="subtitle">{html.escape(scene.subtitle)}</div>'
        if scene.subtitle
        else ""
    )

    content = f"""\
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    {font_css}
    :root {{
      --page-width: {page_width}px;
      --page-pad: {pad}px;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: linear-gradient(135deg, {style.background_start}, {style.background_end});
      display: flex;
      justify-content: center;
    }}
    .page {{
      width: var(--page-width);
      max-width: 100%;
      padding: var(--page-pad);
    }}
    .title-block {{
      width: 100%;
    }}
    .title {{
      font-family: {style.font_family};
      font-size: {style.title_size}px;
      font-weight: 700;
      color: {style.title_color};
      overflow-wrap: break-word;
      word-wrap: break-word;
    }}
    .subtitle {{
      font-family: {style.font_family};
      font-size: {style.subtitle_size}px;
      color: {style.subtitle_color};
      margin-top: {style.title_subtitle_gap}px;
      overflow-wrap: break-word;
      word-wrap: break-word;
    }}
    .diagram {{
      display: block;
      width: 100%;
      height: auto;
      margin-top: {style.title_block_margin}px;
    }}
    @page {{
      size: {page_width}px auto;
      margin: 0;
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="title-block">
      {title_html}
      {subtitle_html}
    </div>
    <img class="diagram" src="{svg_name}" alt="snakey diagram">
  </div>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
