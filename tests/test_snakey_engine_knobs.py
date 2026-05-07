from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, fields, replace
from pathlib import Path

import pytest

from benefind.diagram.snakey import (
    ExclusionNode,
    LayoutConfig,
    SnakeyModel,
    SnakeyStyle,
    StageLabel,
    TextBlock,
    TrunkNode,
    layout_snakey,
    render_svg,
)


@dataclass(frozen=True)
class KnobCase:
    domain: str
    field_name: str
    mutated_value: object
    expected_effect: str
    active_overrides: dict[str, object] | None = None
    inactive_overrides: dict[str, object] | None = None


def _make_model() -> SnakeyModel:
    return SnakeyModel(
        title="Funnel",
        subtitle="Engine knob coverage",
        trunk_nodes=[
            TrunkNode(
                "t0",
                TextBlock(title="Start Stage", count="1'000 organisations"),
                1000,
                highlight=True,
            ),
            TrunkNode("t1", TextBlock(title="Parse Scope", count="850 organisations"), 850),
            TrunkNode("t2", TextBlock(title="Policy Fit", count="640 organisations"), 640),
            TrunkNode(
                "t3", TextBlock(title="Website Discovery", count="390 organisations"), 390
            ),
            TrunkNode(
                "t4",
                TextBlock(title="Final Review", count="220 organisations"),
                220,
                highlight=True,
            ),
        ],
        stage_labels=[
            StageLabel(
                "s1", TextBlock(title="Stage 1", count="850 remain"), "t0", "t1", highlight=True
            ),
            StageLabel("s2", TextBlock(title="Stage 2", count="640 remain"), "t1", "t2"),
            StageLabel("s3", TextBlock(title="Stage 3", count="390 remain"), "t2", "t3"),
            StageLabel(
                "s4", TextBlock(title="Stage 4", count="220 remain"), "t3", "t4", highlight=True
            ),
        ],
        exclusion_nodes=[
            ExclusionNode(
                "e1",
                TextBlock(
                    title="Excluded: no policy page",
                    count="150",
                    context="Ambiguous eligibility and no target audience section",
                ),
                150,
                "t1",
            ),
            ExclusionNode(
                "e2",
                TextBlock(
                    title="Excluded: out of scope",
                    count="210",
                    context="Geography mismatch and legal-form mismatch",
                ),
                210,
                "t2",
            ),
            ExclusionNode(
                "e3",
                TextBlock(
                    title="Excluded: duplicate websites",
                    count="250",
                    context="Duplicate domains and parked pages requiring manual review",
                ),
                250,
                "t2",
            ),
        ],
    )


def _scene_fingerprint(config: LayoutConfig, style: SnakeyStyle) -> str:
    scene = layout_snakey(_make_model(), config, style)
    parts: list[str] = [f"{scene.width}|{scene.height}"]
    for na in scene.node_anchors:
        parts.append(
            f"N|{na.key}|{na.role}|{na.x:.3f}|{na.y:.3f}|{na.radius:.3f}|{na.value}|{int(na.highlight)}"
        )
    for ta in scene.text_anchors:
        parts.append(
            f"T|{ta.node_key}|{ta.role}|{ta.x:.3f}|{ta.y:.3f}|{ta.align}|"
            f"{ta.block.width:.3f}|{ta.block.height:.3f}|{int(ta.highlight)}"
        )
    for er in scene.edge_routes:
        wp = "none" if er.waypoint is None else f"{er.waypoint[0]:.3f},{er.waypoint[1]:.3f}"
        parts.append(
            f"E|{er.source_key}|{er.target_key}|{er.role}|{er.value}|{er.stroke_width:.3f}|{wp}"
        )
    payload = "\n".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _svg_fingerprint(config: LayoutConfig, style: SnakeyStyle, tmp_path: Path) -> str:
    scene = layout_snakey(_make_model(), config, style)
    out = tmp_path / "snakey.svg"
    render_svg(scene, out)
    svg = out.read_text(encoding="utf-8")
    normalized = re.sub(r"\s+", " ", svg).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _build_cases() -> list[KnobCase]:
    inactive_badge = {"highlight_mode": "ring_fill"}
    active_badge = {"highlight_mode": "ring_fill_badge"}
    active_badge_text = {
        "highlight_mode": "ring_fill_badge",
        "highlight_badge_svg": "",
        "highlight_badge_text": ("START", "END"),
    }
    cases = [KnobCase("layout", "orientation", "left_right", "scene"),
        KnobCase("layout", "stage_label_side_policy", "alternate", "scene"),
        KnobCase("layout", "stage_label_clearance", 42.0, "scene"),
        KnobCase("layout", "branch_route_mode", "direct", "scene"),
        KnobCase(
            "layout",
            "branch_waypoint_primary_frac",
            0.75,
            "scene",
            active_overrides={"branch_route_mode": "elbow"},
            inactive_overrides={"branch_route_mode": "direct"},
        ),
        KnobCase(
            "layout",
            "branch_spline_tension",
            0.9,
            "svg",
            active_overrides={"branch_route_mode": "spline", "orientation": "top_down"},
            inactive_overrides={"branch_route_mode": "elbow", "orientation": "top_down"},
        ),
        KnobCase(
            "layout",
            "branch_spline_handle_scale",
            0.75,
            "svg",
            active_overrides={"branch_route_mode": "spline", "orientation": "top_down"},
            inactive_overrides={"branch_route_mode": "elbow", "orientation": "top_down"},
        ),
        KnobCase("layout", "branch_layout_mode", "alternate_sides", "scene"),
        KnobCase("layout", "branch_side_policy", "left", "scene"),
        KnobCase(
            "layout",
            "branch_stagger_short",
            120.0,
            "scene",
            active_overrides={"branch_layout_mode": "same_side_staggered"},
            inactive_overrides={"branch_layout_mode": "same_side"},
        ),
        KnobCase(
            "layout",
            "branch_stagger_long",
            460.0,
            "scene",
            active_overrides={"branch_layout_mode": "same_side_staggered"},
            inactive_overrides={"branch_layout_mode": "same_side"},
        ),
        KnobCase(
            "layout",
            "branch_offset_secondary",
            360.0,
            "scene",
            active_overrides={"branch_layout_mode": "same_side"},
            inactive_overrides={"branch_layout_mode": "same_side_staggered"},
        ),
        KnobCase("layout", "trunk_gap_primary", 240.0, "scene"),
        KnobCase("layout", "branch_gap_primary", 120.0, "none"),
        KnobCase("layout", "trunk_start_primary", 260.0, "none"),
        KnobCase("layout", "trunk_start_secondary", 620.0, "none"),
        KnobCase("layout", "text_max_width_main_trunk", 160, "scene"),
        KnobCase("layout", "text_max_width_stage_label", 80, "scene"),
        KnobCase("layout", "text_max_width_exclusion_node", 220, "scene"),
        KnobCase(
            "layout",
            "min_block_gap",
            30.0,
            "scene",
            active_overrides={
                "trunk_gap_primary": 60.0,
                "branch_gap_primary": 20.0,
                "stage_label_clearance": 4.0,
            },
        ),
        KnobCase("layout", "exclusion_node_radius", 14.0, "scene"),
        KnobCase("layout", "exclusion_text_margin", 30.0, "scene"),
        KnobCase("layout", "trunk_node_radius", 10.0, "scene"),
        KnobCase("layout", "organic_amplitude_1", 60.0, "scene"),
        KnobCase("layout", "organic_freq_1", 1.1, "scene"),
        KnobCase("layout", "organic_amplitude_2", 22.0, "scene"),
        KnobCase("layout", "organic_freq_2", 2.2, "scene"),
        KnobCase("layout", "organic_phase_2", 2.4, "scene"),
        KnobCase("layout", "highlight_mode", "ring_badge", "svg"),
        KnobCase(
            "layout",
            "highlight_badge_side",
            "left_right",
            "svg",
            active_overrides=active_badge,
            inactive_overrides=inactive_badge,
        ),
        KnobCase("layout", "highlight_stage_labels", False, "svg"),
        KnobCase("style", "font_family", "Georgia, serif", "svg"),
        KnobCase("style", "embedded_font_family", "Georgia", "svg"),
        KnobCase("style", "embedded_font_files", (), "svg"),
        KnobCase("style", "background_start", "#111827", "none"),
        KnobCase("style", "background_end", "#1f2937", "none"),
        KnobCase("style", "title_color", "#ef4444", "none"),
        KnobCase("style", "subtitle_color", "#22c55e", "none"),
        KnobCase("style", "main_trunk_color", "#06b6d4", "svg"),
        KnobCase("style", "main_trunk_opacity", 0.55, "svg"),
        KnobCase("style", "exclusion_branch_color", "#e11d48", "svg"),
        KnobCase("style", "exclusion_branch_opacity", 0.4, "svg"),
        KnobCase("style", "main_trunk_node_fill", "#3b82f6", "svg"),
        KnobCase("style", "exclusion_node_fill", "#f97316", "svg"),
        KnobCase("style", "text_color", "#334155", "svg"),
        KnobCase("style", "text_subtle_color", "#0ea5e9", "svg"),
        KnobCase("style", "context_color", "#f43f5e", "svg"),
        KnobCase("style", "title_size", 44, "none"),
        KnobCase("style", "subtitle_size", 24, "none"),
        KnobCase("style", "title_subtitle_gap", 24, "none"),
        KnobCase("style", "title_block_margin", 48, "none"),
        KnobCase("style", "block_title_size", 22, "scene"),
        KnobCase("style", "block_count_size", 20, "scene"),
        KnobCase("style", "footer_size", 12, "none"),
        KnobCase("style", "block_context_size", 18, "scene"),
        KnobCase("style", "block_line_height", 26, "scene"),
        KnobCase("style", "block_padding_x", 20, "scene"),
        KnobCase("style", "block_padding_y", 14, "scene"),
        KnobCase("style", "trunk_stroke_min", 8.0, "scene"),
        KnobCase("style", "trunk_stroke_max", 90.0, "scene"),
        KnobCase("style", "branch_stroke_min", 6.0, "scene"),
        KnobCase("style", "branch_stroke_max", 52.0, "scene"),
        KnobCase("style", "stroke_scale_exponent", 0.45, "scene"),
        KnobCase(
            "style",
            "highlight_node_fill",
            "#f59e0b",
            "svg",
            active_overrides={"highlight_mode": "fill"},
        ),
        KnobCase(
            "style",
            "highlight_node_ring_color",
            "#000000",
            "svg",
            active_overrides={"highlight_mode": "ring"},
        ),
        KnobCase(
            "style",
            "highlight_node_ring_width",
            6.0,
            "svg",
            active_overrides={"highlight_mode": "ring"},
        ),
        KnobCase(
            "style",
            "highlight_badge_svg",
            "M2 2L22 22",
            "svg",
            active_overrides=active_badge,
            inactive_overrides=inactive_badge,
        ),
        KnobCase(
            "style",
            "highlight_badge_size",
            34.0,
            "svg",
            active_overrides=active_badge,
            inactive_overrides=inactive_badge,
        ),
        KnobCase(
            "style",
            "highlight_badge_text",
            ("BEGIN", "END"),
            "svg",
            active_overrides=active_badge_text,
            inactive_overrides=inactive_badge,
        ),
        KnobCase(
            "style",
            "highlight_badge_color",
            "#7c3aed",
            "svg",
            active_overrides=active_badge,
            inactive_overrides=inactive_badge,
        ),
        KnobCase(
            "style",
            "highlight_badge_font_size",
            24,
            "svg",
            active_overrides=active_badge_text,
            inactive_overrides=inactive_badge,
        ),
        KnobCase(
            "style",
            "highlight_badge_gap",
            22.0,
            "svg",
            active_overrides=active_badge,
            inactive_overrides=inactive_badge,
        ),
        KnobCase(
            "style",
            "highlight_stage_label_title_color",
            "#16a34a",
            "svg",
            active_overrides={"highlight_stage_labels": True},
            inactive_overrides={"highlight_stage_labels": False},
        ),
        KnobCase(
            "style",
            "highlight_stage_label_font_weight",
            "900",
            "svg",
            active_overrides={"highlight_stage_labels": True},
            inactive_overrides={"highlight_stage_labels": False},
        ),
    ]
    return cases


KNOB_CASES = _build_cases()


def _apply_knob(
    base_layout: LayoutConfig,
    base_style: SnakeyStyle,
    case: KnobCase,
    include_mutation: bool,
    scenario_overrides: dict[str, object] | None,
) -> tuple[LayoutConfig, SnakeyStyle]:
    layout = base_layout
    style = base_style
    if scenario_overrides:
        layout_updates = {k: v for k, v in scenario_overrides.items() if hasattr(layout, k)}
        style_updates = {k: v for k, v in scenario_overrides.items() if hasattr(style, k)}
        if layout_updates:
            layout = replace(layout, **layout_updates)
        if style_updates:
            style = replace(style, **style_updates)

    if include_mutation:
        if case.domain == "layout":
            layout = replace(layout, **{case.field_name: case.mutated_value})
        else:
            style = replace(style, **{case.field_name: case.mutated_value})

    return layout, style


@pytest.mark.parametrize("case", KNOB_CASES, ids=lambda c: f"{c.domain}.{c.field_name}")
def test_knob_effect_in_active_scenario(case: KnobCase, tmp_path: Path) -> None:
    base_layout = LayoutConfig()
    base_style = SnakeyStyle()

    layout_base, style_base = _apply_knob(
        base_layout,
        base_style,
        case,
        include_mutation=False,
        scenario_overrides=case.active_overrides,
    )
    layout_mut, style_mut = _apply_knob(
        base_layout,
        base_style,
        case,
        include_mutation=True,
        scenario_overrides=case.active_overrides,
    )

    scene_a = _scene_fingerprint(layout_base, style_base)
    scene_b = _scene_fingerprint(layout_mut, style_mut)
    svg_a = _svg_fingerprint(layout_base, style_base, tmp_path)
    svg_b = _svg_fingerprint(layout_mut, style_mut, tmp_path)

    if case.expected_effect == "scene":
        assert scene_a != scene_b, f"{case.domain}.{case.field_name} did not change scene"
    elif case.expected_effect == "svg":
        assert svg_a != svg_b, f"{case.domain}.{case.field_name} did not change svg"
    elif case.expected_effect == "both":
        assert scene_a != scene_b, f"{case.domain}.{case.field_name} did not change scene"
        assert svg_a != svg_b, f"{case.domain}.{case.field_name} did not change svg"
    else:
        assert scene_a == scene_b, f"{case.domain}.{case.field_name} unexpectedly changed scene"
        assert svg_a == svg_b, f"{case.domain}.{case.field_name} unexpectedly changed svg"


@pytest.mark.parametrize(
    "case",
    [case for case in KNOB_CASES if case.inactive_overrides is not None],
    ids=lambda c: f"inactive.{c.domain}.{c.field_name}",
)
def test_conditional_knob_inert_when_inactive(case: KnobCase, tmp_path: Path) -> None:
    base_layout = LayoutConfig()
    base_style = SnakeyStyle()

    layout_base, style_base = _apply_knob(
        base_layout,
        base_style,
        case,
        include_mutation=False,
        scenario_overrides=case.inactive_overrides,
    )
    layout_mut, style_mut = _apply_knob(
        base_layout,
        base_style,
        case,
        include_mutation=True,
        scenario_overrides=case.inactive_overrides,
    )

    scene_a = _scene_fingerprint(layout_base, style_base)
    scene_b = _scene_fingerprint(layout_mut, style_mut)
    svg_a = _svg_fingerprint(layout_base, style_base, tmp_path)
    svg_b = _svg_fingerprint(layout_mut, style_mut, tmp_path)

    assert scene_a == scene_b, f"{case.domain}.{case.field_name} changed scene while inactive"
    assert svg_a == svg_b, f"{case.domain}.{case.field_name} changed svg while inactive"


def test_knob_registry_covers_all_engine_fields() -> None:
    layout_fields = {f.name for f in fields(LayoutConfig)}
    style_fields = {f.name for f in fields(SnakeyStyle)}

    covered_layout = {c.field_name for c in KNOB_CASES if c.domain == "layout"}
    covered_style = {c.field_name for c in KNOB_CASES if c.domain == "style"}

    assert covered_layout == layout_fields
    assert covered_style == style_fields
