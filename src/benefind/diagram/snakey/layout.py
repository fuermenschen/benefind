"""Layout engine for the snakey diagram.

Design goals
------------
- Fully general: no funnel-domain knowledge, only operates on SnakeyModel.
- Deterministic: same model + config → same scene every time.
- Geometry-first: circles and text blocks are always moved together; no desync.
- Clean separation:
    1. Spine placement (trunk nodes + organic drift).
    2. Branch lane assignment (deterministic, prevents most crossings before nudging).
    3. Stage label placement (trunk-thickness-aware clearance).
    4. Light text-block collision resolver (residual nudge, primary-axis only).
    5. Canvas fit (bounds from circles + text + stroke radius).
"""

from __future__ import annotations

import math
from dataclasses import replace

from .text import resolve_text_block, stroke_width_for
from .types import (
    BoundingBox,
    EdgeRoute,
    LayoutConfig,
    NodeAnchor,
    Scene,
    SidePolicy,
    SnakeyModel,
    SnakeyStyle,
    TextAnchor,
)

# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------


def _xy(orientation: str, primary: float, secondary: float) -> tuple[float, float]:
    """Convert (primary, secondary) layout axes to (x, y) SVG coordinates."""
    if orientation == "top_down":
        return secondary, primary
    return primary, secondary


def _primary(orientation: str, x: float, y: float) -> float:
    return y if orientation == "top_down" else x


def _secondary(orientation: str, x: float, y: float) -> float:
    return x if orientation == "top_down" else y


def _sign(policy: SidePolicy, index: int, default: int = 1) -> int:
    if policy == "right":
        return 1
    if policy == "left":
        return -1
    return default if index % 2 == 0 else -default


# ---------------------------------------------------------------------------
# Bezier path builder
# ---------------------------------------------------------------------------


def _bezier_path(x0: float, y0: float, x1: float, y1: float, bend: float = 0.5) -> str:
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


# ---------------------------------------------------------------------------
# Text block bounding box (top-left origin)
# ---------------------------------------------------------------------------


def _text_bbox(anchor: TextAnchor) -> BoundingBox:
    return BoundingBox(
        x=anchor.x,
        y=anchor.y,
        w=anchor.block.width,
        h=anchor.block.height,
        key=anchor.node_key,
    )


def _node_circle_bbox(anchor: NodeAnchor) -> BoundingBox:
    r = anchor.radius
    return BoundingBox(x=anchor.x - r, y=anchor.y - r, w=r * 2, h=r * 2, key=anchor.key)


# ---------------------------------------------------------------------------
# Collision resolver — primary-axis nudge only, on text anchors
# ---------------------------------------------------------------------------


def _overlaps(a: BoundingBox, b: BoundingBox, gap: float) -> bool:
    return (
        a.x < b.x1 + gap
        and a.x1 + gap > b.x
        and a.y < b.y1 + gap
        and a.y1 + gap > b.y
    )


def _shift_text_anchor_primary(
    anchor: TextAnchor, orientation: str, delta: float
) -> TextAnchor:
    """Nudge a text anchor along the primary flow axis."""
    if orientation == "top_down":
        return replace(anchor, y=anchor.y + delta)
    return replace(anchor, x=anchor.x + delta)


def _resolve_text_collisions(
    anchors: list[TextAnchor],
    orientation: str,
    gap: float,
    max_iterations: int = 20,
) -> list[TextAnchor]:
    resolved = list(anchors)
    for _ in range(max_iterations):
        changed = False
        for i in range(len(resolved)):
            for j in range(i + 1, len(resolved)):
                a = _text_bbox(resolved[i])
                b = _text_bbox(resolved[j])
                if _overlaps(a, b, gap):
                    overlap_y = (a.y1 + gap) - b.y
                    overlap_x = (a.x1 + gap) - b.x
                    if orientation == "top_down":
                        delta = overlap_y
                    else:
                        delta = overlap_x
                    resolved[j] = _shift_text_anchor_primary(resolved[j], orientation, delta)
                    changed = True
        if not changed:
            break
    return resolved


# ---------------------------------------------------------------------------
# Canvas fit
# ---------------------------------------------------------------------------


def _scene_bounds(
    node_anchors: list[NodeAnchor],
    text_anchors: list[TextAnchor],
    edge_routes: list[EdgeRoute],
) -> tuple[float, float, float, float]:
    """Return (min_x, min_y, max_x, max_y) over all scene geometry.

    Edge paths are not pre-computed, so we expand each edge endpoint by half
    the stroke width — sufficient to prevent circle/stroke clipping.
    """
    xs: list[float] = []
    ys: list[float] = []

    for na in node_anchors:
        xs += [na.x - na.radius, na.x + na.radius]
        ys += [na.y - na.radius, na.y + na.radius]

    for ta in text_anchors:
        bb = _text_bbox(ta)
        xs += [bb.x, bb.x1]
        ys += [bb.y, bb.y1]

    # For edges: expand each endpoint by half stroke so thick strokes don't clip.
    node_map = {na.key: na for na in node_anchors}
    for er in edge_routes:
        hw = er.stroke_width * 0.5
        for key in (er.source_key, er.target_key):
            na = node_map.get(key)
            if na:
                xs += [na.x - hw, na.x + hw]
                ys += [na.y - hw, na.y + hw]

    if not xs or not ys:
        return 0.0, 0.0, 0.0, 0.0
    return min(xs), min(ys), max(xs), max(ys)


def _fit_canvas(
    node_anchors: list[NodeAnchor],
    text_anchors: list[TextAnchor],
    edge_routes: list[EdgeRoute],
    config: LayoutConfig,
    title_block_height: float | None = None,
) -> tuple[list[NodeAnchor], list[TextAnchor], list[EdgeRoute], int, int]:
    pad = float(config.canvas_fit_padding)
    top_reserve = pad + (float(config.title_block_height) if title_block_height is None else title_block_height)

    min_x, min_y, max_x, max_y = _scene_bounds(node_anchors, text_anchors, edge_routes)

    # Shift everything so content starts at (pad, top_reserve)
    dx = pad - min_x
    dy = top_reserve - min_y

    shifted_nodes = [replace(na, x=na.x + dx, y=na.y + dy) for na in node_anchors]
    shifted_texts = [replace(ta, x=ta.x + dx, y=ta.y + dy) for ta in text_anchors]
    shifted_edges = [
        replace(
            er,
            waypoint=(er.waypoint[0] + dx, er.waypoint[1] + dy)
            if er.waypoint is not None
            else None,
        )
        for er in edge_routes
    ]

    # Recompute bounds after shift
    _, _, new_max_x, new_max_y = _scene_bounds(shifted_nodes, shifted_texts, edge_routes)

    w = int(math.ceil(new_max_x + pad))
    h = int(math.ceil(new_max_y + pad))

    if config.canvas_fit_mode == "fixed":
        w = max(w, config.width)
        h = max(h, config.height)

    return shifted_nodes, shifted_texts, shifted_edges, w, h


# ---------------------------------------------------------------------------
# Main layout function
# ---------------------------------------------------------------------------


def layout_snakey(model: SnakeyModel, config: LayoutConfig, style: SnakeyStyle) -> Scene:
    # ------------------------------------------------------------------
    # 1. Spine: place trunk nodes with organic secondary drift
    # ------------------------------------------------------------------
    trunk_positions: dict[str, tuple[float, float]] = {}  # key → (x, y)
    trunk_primary: dict[str, float] = {}
    trunk_secondary: dict[str, float] = {}

    node_anchors: list[NodeAnchor] = []
    edge_routes: list[EdgeRoute] = []

    primary = config.trunk_start_primary
    secondary = config.trunk_start_secondary

    max_value = max((tn.value for tn in model.trunk_nodes), default=1)

    for idx, trunk in enumerate(model.trunk_nodes):
        drift = (
            config.organic_amplitude_1 * math.sin(idx * config.organic_freq_1)
            + config.organic_amplitude_2 * math.sin(
                idx * config.organic_freq_2 + config.organic_phase_2
            )
        )
        sx = secondary + drift
        x, y = _xy(config.orientation, primary, sx)
        trunk_positions[trunk.key] = (x, y)
        trunk_primary[trunk.key] = primary
        trunk_secondary[trunk.key] = sx

        node_anchors.append(
            NodeAnchor(
                key=trunk.key,
                role="main_trunk",
                x=x,
                y=y,
                radius=config.trunk_node_radius,
                value=trunk.value,
                highlight=trunk.highlight,
            )
        )
        primary += config.trunk_gap_primary

    # Trunk edges
    for idx in range(1, len(model.trunk_nodes)):
        src = model.trunk_nodes[idx - 1]
        tgt = model.trunk_nodes[idx]
        sx, sy = trunk_positions[src.key]
        tx, ty = trunk_positions[tgt.key]
        sw = stroke_width_for(
            tgt.value, max_value,
            style.trunk_stroke_min, style.trunk_stroke_max,
            style.stroke_scale_exponent,
        )
        edge_routes.append(
            EdgeRoute(
                source_key=src.key,
                target_key=tgt.key,
                role="main_trunk",
                value=tgt.value,
                stroke_width=sw,
            )
        )

    # ------------------------------------------------------------------
    # 2. Branch lane assignment — deterministic, no crossings by construction
    #
    # For each exclusion node, we compute its final (x, y) from first principles:
    # - primary position: source trunk primary + small per-exclusion primary offset
    #   (spreads them slightly along the flow to reduce bunching)
    # - secondary offset from trunk: determined by mode + lane
    #
    # BranchLayoutMode semantics:
    #   same_side           – all at branch_offset_secondary, on branch_side_policy side
    #   alternate_sides     – alternate left/right, equal length
    #   same_side_staggered – all on one side, alternating short/long lengths
    # ------------------------------------------------------------------

    # We need trunk stroke widths at each trunk node to compute label clearance later.
    trunk_stroke: dict[str, float] = {
        tn.key: stroke_width_for(
            tn.value, max_value,
            style.trunk_stroke_min, style.trunk_stroke_max,
            style.stroke_scale_exponent,
        )
        for tn in model.trunk_nodes
    }

    exclusion_anchors: list[NodeAnchor] = []
    exclusion_text_anchors: list[TextAnchor] = []
    exclusion_edge_routes: list[EdgeRoute] = []
    # Remember side + source info per exclusion key for post-collision node sync
    excl_side: dict[str, int] = {}
    excl_src_key: dict[str, str] = {}
    excl_sec: dict[str, float] = {}  # final secondary of node (lane position)

    exclusions_by_source: dict[str, list[str]] = {}
    for excl in model.exclusion_nodes:
        exclusions_by_source.setdefault(excl.source_trunk_key, []).append(excl.key)

    exclusion_group_pos: dict[str, tuple[int, int]] = {}
    for keys in exclusions_by_source.values():
        total = len(keys)
        for idx, key in enumerate(keys):
            exclusion_group_pos[key] = (idx, total)

    for lane_idx, excl in enumerate(model.exclusion_nodes):
        src_x, src_y = trunk_positions[excl.source_trunk_key]
        src_pri = trunk_primary[excl.source_trunk_key]
        src_sec = trunk_secondary[excl.source_trunk_key]

        # Secondary offset: determined by layout mode
        mode = config.branch_layout_mode
        if mode == "same_side":
            side = _sign(config.branch_side_policy, 0)
            ex_sec_offset = side * config.branch_offset_secondary
        elif mode == "alternate_sides":
            side = _sign("alternate", lane_idx)
            ex_sec_offset = side * config.branch_offset_secondary
        else:  # same_side_staggered
            side = _sign(config.branch_side_policy, 0)
            length = (
                config.branch_stagger_short
                if lane_idx % 2 == 0
                else config.branch_stagger_long
            )
            ex_sec_offset = side * length

        ex_sec = src_sec + ex_sec_offset
        # Primary: centred spread among exclusions that share the same source
        # trunk segment, so branch_gap_primary controls local de-bunching
        # without distorting global stage alignment.
        group_idx, group_total = exclusion_group_pos[excl.key]
        centred_idx = group_idx - (group_total - 1) * 0.5
        ex_pri = src_pri + centred_idx * config.branch_gap_primary
        ex_x, ex_y = _xy(config.orientation, ex_pri, ex_sec)

        na = NodeAnchor(
            key=excl.key,
            role="exclusion_node",
            x=ex_x,
            y=ex_y,
            radius=config.exclusion_node_radius,
            value=excl.value,
        )
        exclusion_anchors.append(na)

        # Text block: placed outward from node circle edge
        text_offset = config.exclusion_node_radius + config.exclusion_text_margin
        text_sec = ex_sec + side * text_offset
        text_x, text_y = _xy(config.orientation, ex_pri, text_sec)
        block = resolve_text_block(excl.text, config.text_max_width_exclusion_node, style)
        ta_x = text_x if side > 0 else text_x - block.width
        ta_y = text_y - block.height * 0.5

        exclusion_text_anchors.append(
            TextAnchor(
                node_key=excl.key,
                role="exclusion_node",
                x=ta_x,
                y=ta_y,
                align="start" if side > 0 else "end",
                block=block,
                text=excl.text,
            )
        )

        sw_branch = stroke_width_for(
            excl.value, max_value,
            style.branch_stroke_min, style.branch_stroke_max,
            style.stroke_scale_exponent,
        )
        exclusion_edge_routes.append(
            EdgeRoute(
                source_key=excl.source_trunk_key,
                target_key=excl.key,
                role="exclusion_branch",
                value=excl.value,
                stroke_width=sw_branch,
                # waypoint filled in after collision resolution
            )
        )

        excl_side[excl.key] = side
        excl_src_key[excl.key] = excl.source_trunk_key
        excl_sec[excl.key] = ex_sec

    # ------------------------------------------------------------------
    # 3. Stage labels — trunk-thickness-aware clearance
    #
    # For each label (sits between source and target trunk nodes):
    #   - primary position: midpoint of source and target primary coords
    #   - secondary: hug the trunk side, clearing by:
    #       0.5 * interpolated_stroke_at_midpoint + stage_label_clearance
    # ------------------------------------------------------------------

    stage_text_anchors: list[TextAnchor] = []

    for lbl_idx, lbl in enumerate(model.stage_labels):
        src_pri = trunk_primary[lbl.source_trunk_key]
        tgt_pri = trunk_primary[lbl.target_trunk_key]
        src_sec = trunk_secondary[lbl.source_trunk_key]
        tgt_sec = trunk_secondary[lbl.target_trunk_key]

        mid_pri = (src_pri + tgt_pri) * 0.5
        mid_sec = (src_sec + tgt_sec) * 0.5

        # Stroke at midpoint: interpolate between source and target values
        src_sw = trunk_stroke.get(lbl.source_trunk_key, style.trunk_stroke_min)
        tgt_sw = trunk_stroke.get(lbl.target_trunk_key, style.trunk_stroke_min)
        mid_sw = (src_sw + tgt_sw) * 0.5
        clearance = mid_sw * 0.5 + config.stage_label_clearance

        block = resolve_text_block(lbl.text, config.text_max_width_stage_label, style)
        side = _sign(config.stage_label_side_policy, lbl_idx, -1)

        if side < 0:
            # Left side: right edge of block at (mid_sec - clearance)
            lbl_sec = mid_sec - clearance - block.width
        else:
            lbl_sec = mid_sec + clearance

        lbl_x, lbl_y = _xy(config.orientation, mid_pri, lbl_sec)
        # top-left of block
        ta_x = lbl_x
        ta_y = lbl_y - block.height * 0.5

        stage_text_anchors.append(
            TextAnchor(
                node_key=lbl.key,
                role="stage_label",
                x=ta_x,
                y=ta_y,
                align="start" if side > 0 else "end",
                block=block,
                text=lbl.text,
                highlight=lbl.highlight and config.highlight_stage_labels,
            )
        )

    # ------------------------------------------------------------------
    # 4. Main-trunk text labels
    #
    # Place trunk-node labels on the opposite side of stage labels by default.
    # This keeps the two annotation families separated while still allowing the
    # collision resolver to nudge residual overlaps.
    # ------------------------------------------------------------------
    trunk_text_anchors: list[TextAnchor] = []
    for idx, trunk in enumerate(model.trunk_nodes):
        pri = trunk_primary[trunk.key]
        sec = trunk_secondary[trunk.key]
        block = resolve_text_block(trunk.text, config.text_max_width_main_trunk, style)
        stage_side = _sign(config.stage_label_side_policy, idx, -1)
        text_side = -stage_side

        sw = trunk_stroke.get(trunk.key, style.trunk_stroke_min)
        clearance = (sw * 0.5) + config.stage_label_clearance + config.trunk_node_radius + 6.0
        text_sec = sec + text_side * clearance
        text_x, text_y = _xy(config.orientation, pri, text_sec)
        ta_x = text_x if text_side > 0 else text_x - block.width
        ta_y = text_y - block.height * 0.5

        trunk_text_anchors.append(
            TextAnchor(
                node_key=trunk.key,
                role="main_trunk",
                x=ta_x,
                y=ta_y,
                align="start" if text_side > 0 else "end",
                block=block,
                text=trunk.text,
                highlight=trunk.highlight and config.highlight_stage_labels,
            )
        )

    # ------------------------------------------------------------------
    # 5. Collision resolver — light residual nudge on text anchors only
    # ------------------------------------------------------------------
    visible_text_before_resolve = stage_text_anchors + exclusion_text_anchors
    resolved_visible_texts = _resolve_text_collisions(
        visible_text_before_resolve,
        config.orientation,
        config.min_block_gap,
    )
    # Keep trunk labels out of collision solving so they cannot perturb visible label flow.
    # They are retained in scene text anchors (for engine-level introspection/tests),
    # but are not rendered by the SVG renderer.
    resolved_texts = resolved_visible_texts + trunk_text_anchors

    # ------------------------------------------------------------------
    # 6. Post-collision sync: move each exclusion NodeAnchor to match
    #    the (possibly nudged) primary coord of its TextAnchor.
    #    The node circle should always sit visually adjacent to the label,
    #    not at the original pre-nudge position.
    # ------------------------------------------------------------------
    # Build a fast lookup from node_key → resolved TextAnchor
    resolved_excl_text: dict[str, TextAnchor] = {
        ta.node_key: ta for ta in resolved_visible_texts if ta.role == "exclusion_node"
    }

    synced_exclusion_anchors: list[NodeAnchor] = []
    for na in exclusion_anchors:
        ta = resolved_excl_text.get(na.key)
        if ta is None:
            synced_exclusion_anchors.append(na)
            continue
        side = excl_side[na.key]
        ex_sec = excl_sec[na.key]
        # Primary of text anchor centre
        if config.orientation == "top_down":
            text_centre_pri = ta.y + ta.block.height * 0.5
            new_x, new_y = _xy(config.orientation, text_centre_pri, ex_sec)
        else:
            text_centre_pri = ta.x + ta.block.width * 0.5
            new_x, new_y = _xy(config.orientation, text_centre_pri, ex_sec)
        synced_exclusion_anchors.append(replace(na, x=new_x, y=new_y))

    # ------------------------------------------------------------------
    # 7. Build waypoints for elbow routing (after node positions are final)
    # ------------------------------------------------------------------
    synced_node_map = {na.key: na for na in node_anchors + synced_exclusion_anchors}
    final_edge_routes: list[EdgeRoute] = list(edge_routes)

    for er in exclusion_edge_routes:
        if config.branch_route_mode == "direct":
            final_edge_routes.append(er)
            continue

        # Elbow: depart from source trunk node laterally (secondary axis only),
        # then curve to the final exclusion node position.
        # Waypoint sits at:
        #   primary = src_pri + frac * (tgt_pri - src_pri)   (along flow)
        #   secondary = tgt_sec                               (already at the lane)
        src_na = synced_node_map[er.source_key]
        tgt_na = synced_node_map[er.target_key]
        src_pri_v = _primary(config.orientation, src_na.x, src_na.y)
        tgt_pri_v = _primary(config.orientation, tgt_na.x, tgt_na.y)
        tgt_sec_v = _secondary(config.orientation, tgt_na.x, tgt_na.y)
        frac = config.branch_waypoint_primary_frac
        wp_pri = src_pri_v + frac * (tgt_pri_v - src_pri_v)
        wp_x, wp_y = _xy(config.orientation, wp_pri, tgt_sec_v)

        final_edge_routes.append(replace(er, waypoint=(wp_x, wp_y)))

    all_node_anchors = node_anchors + synced_exclusion_anchors

    # ------------------------------------------------------------------
    # 8. Canvas fit
    # ------------------------------------------------------------------
    fitted_nodes, fitted_texts, fitted_edges, w, h = _fit_canvas(
        all_node_anchors,
        resolved_texts,
        final_edge_routes,
        config,
    )

    return Scene(
        title=model.title,
        subtitle=model.subtitle,
        width=w,
        height=h,
        node_anchors=fitted_nodes,
        text_anchors=fitted_texts,
        edge_routes=fitted_edges,
        style=style,
        config=config,
    )
