from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

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
from benefind.diagram.snakey.types import BoundingBox


def _load_render_script_module():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "render_filter_funnel_snakey.py"
    spec = importlib.util.spec_from_file_location("render_filter_funnel_snakey", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _text_bbox(ta) -> BoundingBox:
    return BoundingBox(x=ta.x, y=ta.y, w=ta.block.width, h=ta.block.height)


def _bboxes_overlap(a: BoundingBox, b: BoundingBox, gap: float = 2.0) -> bool:
    return (
        a.x < b.x + b.w + gap
        and a.x + a.w + gap > b.x
        and a.y < b.y + b.h + gap
        and a.y + a.h + gap > b.y
    )


def _make_model() -> SnakeyModel:
    return SnakeyModel(
        title="T",
        subtitle="S",
        trunk_nodes=[
            TrunkNode("t0", TextBlock(title="Start", count="1'000"), 1000),
            TrunkNode("t1", TextBlock(title="Step 1", count="800"), 800),
            TrunkNode("t2", TextBlock(title="Step 2", count="600"), 600),
            TrunkNode("t3", TextBlock(title="Step 3", count="400"), 400),
            TrunkNode("t4", TextBlock(title="Step 4", count="200"), 200),
        ],
        stage_labels=[
            StageLabel("s1", TextBlock(title="Stage 1", count="800"), "t0", "t1"),
            StageLabel("s2", TextBlock(title="Stage 2", count="600"), "t1", "t2"),
            StageLabel("s3", TextBlock(title="Stage 3", count="400"), "t2", "t3"),
            StageLabel("s4", TextBlock(title="Stage 4", count="200"), "t3", "t4"),
        ],
        exclusion_nodes=[
            ExclusionNode(
                "e1",
                TextBlock(title="Excl 1", count="200", context="Reason A | Reason B"),
                200, "t1",
            ),
            ExclusionNode(
                "e2",
                TextBlock(title="Excl 2", count="200", context="Reason C"),
                200, "t2",
            ),
            ExclusionNode(
                "e3",
                TextBlock(title="Excl 3", count="200", context="Reason D | Reason E long text"),
                200, "t3",
            ),
        ],
    )


def test_layout_is_deterministic() -> None:
    model = _make_model()
    config = LayoutConfig()
    style = SnakeyStyle()
    scene_a = layout_snakey(model, config, style)
    scene_b = layout_snakey(model, config, style)
    coords_a = [(na.key, na.x, na.y) for na in scene_a.node_anchors]
    coords_b = [(na.key, na.x, na.y) for na in scene_b.node_anchors]
    assert coords_a == coords_b


def test_text_anchors_no_overlap() -> None:
    model = _make_model()
    config = LayoutConfig()
    style = SnakeyStyle()
    scene = layout_snakey(model, config, style)

    texts = scene.text_anchors
    for i, a in enumerate(texts):
        for b in texts[i + 1 :]:
            bb_a = _text_bbox(a)
            bb_b = _text_bbox(b)
            assert not _bboxes_overlap(bb_a, bb_b), (
                f"Text blocks overlap: {a.node_key!r} and {b.node_key!r}"
            )


def test_exclusion_text_does_not_overlap_circle() -> None:
    """Text anchor must not overlap its own exclusion circle."""
    model = _make_model()
    config = LayoutConfig()
    style = SnakeyStyle()
    scene = layout_snakey(model, config, style)

    anchor_map = {na.key: na for na in scene.node_anchors}
    for ta in scene.text_anchors:
        if ta.role != "exclusion_node":
            continue
        na = anchor_map[ta.node_key]
        r = na.radius
        # text block bounding box
        tb = _text_bbox(ta)
        # circle bounding box
        cb = BoundingBox(x=na.x - r, y=na.y - r, w=r * 2, h=r * 2)
        assert not _bboxes_overlap(tb, cb, gap=0.0), (
            f"Exclusion text overlaps its circle for {ta.node_key!r}"
        )


def test_content_fit_mode_no_clipping() -> None:
    """In content fit mode, all text anchors must be within canvas bounds."""
    model = _make_model()
    config = LayoutConfig(canvas_fit_mode="content", canvas_fit_padding=60)
    style = SnakeyStyle()
    scene = layout_snakey(model, config, style)

    for ta in scene.text_anchors:
        assert ta.x >= 0, f"Text anchor {ta.node_key!r} clips left: x={ta.x:.1f}"
        assert ta.y >= 0, f"Text anchor {ta.node_key!r} clips top: y={ta.y:.1f}"
        assert ta.x + ta.block.width <= scene.width + 1, (
            f"Text anchor {ta.node_key!r} clips right: "
            f"x+w={ta.x + ta.block.width:.1f} > width={scene.width}"
        )


def test_stage_label_clearance_respects_stroke() -> None:
    """Stage labels on a thick trunk must be further from trunk than on a thin trunk.

    We compare two models: one where the stage segment carries almost the full max value
    (thick stroke) and one where it carries a tiny fraction (thin stroke).
    """
    style = SnakeyStyle()

    def _label_gap(source_value: int, target_value: int, global_max: int) -> float:
        """Return secondary gap from trunk midpoint to nearest label right edge."""
        trunk_nodes = [
            TrunkNode("t0", TextBlock(title="A", count=str(source_value)), source_value),
            TrunkNode("t1", TextBlock(title="B", count=str(target_value)), target_value),
        ]
        stage_labels = [
            StageLabel("s0", TextBlock(title="Stage", count=str(target_value)), "t0", "t1"),
        ]
        # Add a dummy max-value node so max_value is consistently global_max
        trunk_nodes_ext = [
            TrunkNode("t_max", TextBlock(title="Max", count=str(global_max)), global_max),
        ] + trunk_nodes
        m = SnakeyModel(
            title="T", subtitle="S",
            trunk_nodes=trunk_nodes_ext,
            stage_labels=stage_labels,
            exclusion_nodes=[],
        )
        cfg = LayoutConfig(
            canvas_fit_mode="content",
            stage_label_side_policy="left",
            organic_amplitude_1=0.0,
            organic_amplitude_2=0.0,
        )
        scene = layout_snakey(m, cfg, style)
        node_map = {na.key: na for na in scene.node_anchors}
        lbl = next(ta for ta in scene.text_anchors if ta.role == "stage_label")
        src_x = node_map["t0"].x
        tgt_x = node_map["t1"].x
        mid_x = (src_x + tgt_x) * 0.5
        return mid_x - (lbl.x + lbl.block.width)

    global_max = 1000
    # Thick segment: carries most of the flow
    gap_thick = _label_gap(source_value=900, target_value=800, global_max=global_max)
    # Thin segment: carries very little
    gap_thin = _label_gap(source_value=50, target_value=10, global_max=global_max)

    assert gap_thick > gap_thin, (
        f"Label not further on thick trunk: thick={gap_thick:.1f}, thin={gap_thin:.1f}"
    )


def test_same_side_staggered_alternates_lengths() -> None:
    """same_side_staggered: even-indexed exclusions are shorter than odd-indexed."""
    model = _make_model()
    config = LayoutConfig(
        branch_layout_mode="same_side_staggered",
        branch_side_policy="right",
        branch_stagger_short=150.0,
        branch_stagger_long=300.0,
        organic_amplitude_1=0.0,
        organic_amplitude_2=0.0,
        orientation="top_down",
    )
    style = SnakeyStyle()
    scene = layout_snakey(model, config, style)

    ex_nodes = [na for na in scene.node_anchors if na.role == "exclusion_node"]
    trunk_xs = {
        na.key: na.x for na in scene.node_anchors if na.role == "main_trunk"
    }
    # All exclusion nodes reference their source trunks by edge
    excl_sources = {excl.key: excl.source_trunk_key for excl in model.exclusion_nodes}

    for idx, excl_node in enumerate(ex_nodes):
        src_x = trunk_xs[excl_sources[excl_node.key]]
        dist = excl_node.x - src_x  # should be positive (right side)
        assert dist > 0, f"Exclusion {excl_node.key!r} not on right side"
        expected_short = idx % 2 == 0
        if expected_short:
            assert dist < config.branch_stagger_long * 0.9, (
                f"Even-indexed exclusion {excl_node.key!r} is not short: dist={dist:.1f}"
            )
        else:
            assert dist > config.branch_stagger_short * 1.1, (
                f"Odd-indexed exclusion {excl_node.key!r} is not long: dist={dist:.1f}"
            )


def test_config_and_comments_merge_into_model(tmp_path: Path) -> None:
    module = _load_render_script_module()
    config_path = tmp_path / "snakey_config.toml"
    config_path.write_text(
        "\n".join([
            "[layout]",
            "orientation = 'left_right'",
            "text_max_width_exclusion_node = 420",
            "",
            "[style]",
            "block_context_size = 15",
        ]) + "\n",
        encoding="utf-8",
    )
    comments_path = tmp_path / "comments.json"
    comments_path.write_text(
        json.dumps({"step_context": {"q01_target_focus": "Schritt Kontext Test"}}),
        encoding="utf-8",
    )

    layout_cfg, style_cfg = module._load_config_file(config_path)
    assert layout_cfg.orientation == "left_right"
    assert layout_cfg.text_max_width_exclusion_node == 420
    assert style_cfg.block_context_size == 15

    meta_path = Path(__file__).resolve().parents[1] / "data" / "meta" / "filter_funnel_meta_2026.json"
    with meta_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)

    step_context = module._load_step_context(comments_path)
    from benefind.diagram.filter_funnel import build_model
    model = build_model(meta, step_context)
    q01 = next(node for node in model.exclusion_nodes if node.key == "ex_q01_target_focus")
    assert "Schritt Kontext Test" in q01.text.context


def test_svg_embeds_font_face_by_default(tmp_path: Path) -> None:
    model = _make_model()
    scene = layout_snakey(model, LayoutConfig(), SnakeyStyle())
    out = tmp_path / "snakey.svg"
    render_svg(scene, out)
    svg = out.read_text(encoding="utf-8")
    assert "@font-face" in svg
    assert "data:font/ttf;base64," in svg


def test_parse_args_accepts_pdf_and_all(monkeypatch) -> None:
    module = _load_render_script_module()

    monkeypatch.setattr(sys, "argv", ["render_filter_funnel_snakey.py", "--format", "pdf"])
    args_pdf = module._parse_args()
    assert args_pdf.format == "pdf"

    monkeypatch.setattr(sys, "argv", ["render_filter_funnel_snakey.py", "--format", "all"])
    args_all = module._parse_args()
    assert args_all.format == "all"


# ---------------------------------------------------------------------------
# extends inheritance
# ---------------------------------------------------------------------------


def test_extends_child_overrides_parent(tmp_path: Path) -> None:
    """Child [style] keys override parent values; parent layout keys are preserved."""
    module = _load_render_script_module()

    parent = tmp_path / "base.toml"
    parent.write_text(
        "[layout]\norientation = 'left_right'\n\n[style]\nbackground_start = '#aaaaaa'\n",
        encoding="utf-8",
    )
    child = tmp_path / "theme.toml"
    child.write_text(
        'extends = "base.toml"\n\n[style]\nbackground_start = "#ff0000"\n',
        encoding="utf-8",
    )

    layout_cfg, style_cfg = module._load_config_file(child)
    assert layout_cfg.orientation == "left_right"      # inherited from parent
    assert style_cfg.background_start == "#ff0000"     # overridden by child


def test_extends_parent_value_kept_when_child_silent(tmp_path: Path) -> None:
    """Style keys set only in parent survive unchanged when child does not mention them."""
    module = _load_render_script_module()

    parent = tmp_path / "base.toml"
    parent.write_text("[style]\nbackground_start = '#base'\nbackground_end = '#base_end'\n",
                      encoding="utf-8")
    child = tmp_path / "theme.toml"
    child.write_text('extends = "base.toml"\n\n[style]\nbackground_end = "#child_end"\n',
                     encoding="utf-8")

    _, style_cfg = module._load_config_file(child)
    assert style_cfg.background_start == "#base"       # untouched by child
    assert style_cfg.background_end == "#child_end"    # overridden by child


def test_extends_no_parent_key_gives_defaults(tmp_path: Path) -> None:
    """A child without extends and no style keys falls back to SnakeyStyle defaults."""
    module = _load_render_script_module()
    from benefind.diagram.snakey import SnakeyStyle

    child = tmp_path / "minimal.toml"
    child.write_text("[layout]\norientation = 'top_down'\n", encoding="utf-8")

    _, style_cfg = module._load_config_file(child)
    assert style_cfg == SnakeyStyle()


def test_extends_missing_parent_raises(tmp_path: Path) -> None:
    """A missing parent file raises FileNotFoundError with a helpful message."""
    module = _load_render_script_module()
    import pytest

    child = tmp_path / "theme.toml"
    child.write_text('extends = "nonexistent_base.toml"\n', encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="nonexistent_base.toml"):
        module._load_config_file(child)


def test_extends_cycle_raises(tmp_path: Path) -> None:
    """Mutual extends (A -> B -> A) raises a ValueError naming the cycle."""
    module = _load_render_script_module()
    import pytest

    a = tmp_path / "a.toml"
    b = tmp_path / "b.toml"
    a.write_text('extends = "b.toml"\n', encoding="utf-8")
    b.write_text('extends = "a.toml"\n', encoding="utf-8")

    with pytest.raises(ValueError, match="Cyclic"):
        module._load_config_file(a)


def test_extends_non_string_raises(tmp_path: Path) -> None:
    """extends = <non-string> raises a ValueError."""
    module = _load_render_script_module()
    import pytest

    child = tmp_path / "bad.toml"
    child.write_text("extends = 42\n", encoding="utf-8")

    with pytest.raises(ValueError, match="extends"):
        module._load_config_file(child)


def test_extends_real_theme_configs() -> None:
    """Dark and light theme configs load cleanly via extends and carry expected palette keys."""
    module = _load_render_script_module()
    meta_dir = Path(__file__).resolve().parents[1] / "data" / "meta"

    dark_layout, dark_style = module._load_config_file(
        meta_dir / "filter_funnel_snakey_config_2026_dark.toml"
    )
    light_layout, light_style = module._load_config_file(
        meta_dir / "filter_funnel_snakey_config_2026_light.toml"
    )

    # Geometry is identical: both inherit base layout unchanged
    assert dark_layout.orientation == light_layout.orientation
    assert dark_layout.trunk_gap_primary == light_layout.trunk_gap_primary
    assert dark_layout.branch_offset_secondary == light_layout.branch_offset_secondary

    # Colors differ between themes
    assert dark_style.background_start != light_style.background_start
    assert dark_style.title_color != light_style.title_color

    # Both themes override the key palette fields
    for style_cfg, label in ((dark_style, "dark"), (light_style, "light")):
        assert style_cfg.background_start, f"{label}: background_start is empty/unset"
        assert style_cfg.title_color, f"{label}: title_color is empty/unset"
        assert style_cfg.main_trunk_color, f"{label}: main_trunk_color is empty/unset"
