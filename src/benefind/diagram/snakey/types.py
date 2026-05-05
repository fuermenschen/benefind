from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Orientation = Literal["top_down", "left_right"]
SidePolicy = Literal["right", "left", "alternate"]
# same_side_staggered: all exclusions on one side, alternating short/long branch lengths
BranchLayoutMode = Literal["same_side", "alternate_sides", "same_side_staggered"]
# direct: simple source→target bezier
# elbow:  two-segment cubic via a lateral waypoint — avoids crossing downstream labels
# spline: single smooth cubic that departs laterally then curves to target — softer than elbow
BranchRouteMode = Literal["direct", "elbow", "spline"]
PageSizeMode = Literal["content", "pixels", "paper"]
PageFitMode = Literal["contain", "none"]
PageAlignX = Literal["left", "center", "right"]
PageAlignY = Literal["top", "center", "bottom"]
PaperSize = Literal["A5", "A4", "Letter"]
PaperOrientation = Literal["portrait", "landscape"]
# Highlight mode controls how start/end trunk nodes are visually distinguished.
# Modes compose: use "_" to combine features:
#   fill          — different fill colour only
#   ring          — stroke ring around the node circle only
#   ring_fill     — ring + fill
#   badge         — text badge near the node (e.g. "▶ Start" / "End ◀")
#   ring_badge    — ring + badge
#   ring_fill_badge — ring + fill + badge
HighlightMode = Literal["fill", "ring", "ring_fill", "badge", "ring_badge", "ring_fill_badge"]
# Badge side controls which side of the node the badge label is placed on.
# Relative options (orientation-aware, mirrored between start and end nodes):
#   outward    — along flow: above start / below end (top_down); left start / right end (left_right)
#   inward     — along flow: below start / above end (top_down); right start / left end (left_right)
#   left_right — cross flow: start badge on left, end badge on right
#   right_left — cross flow: start badge on right, end badge on left
# Absolute options (same cardinal side for both nodes):
#   above / below / left / right
BadgeSide = Literal[
    "outward",
    "inward",
    "left_right",
    "right_left",
    "above",
    "below",
    "left",
    "right",
]


# ---------------------------------------------------------------------------
# Semantic model (input to layout)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TextBlock:
    title: str = ""
    count: str = ""
    context: str = ""


@dataclass(frozen=True)
class TrunkNode:
    key: str
    text: TextBlock
    value: int
    highlight: bool = False


@dataclass(frozen=True)
class StageLabel:
    key: str
    text: TextBlock
    source_trunk_key: str
    target_trunk_key: str
    highlight: bool = False


@dataclass(frozen=True)
class ExclusionNode:
    key: str
    text: TextBlock
    value: int
    source_trunk_key: str


@dataclass(frozen=True)
class SnakeyModel:
    title: str
    subtitle: str
    trunk_nodes: list[TrunkNode]
    stage_labels: list[StageLabel]
    exclusion_nodes: list[ExclusionNode]


# ---------------------------------------------------------------------------
# Layout configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LayoutConfig:
    # Orientation
    orientation: Orientation = "top_down"

    # Stage label side (left/right/alternate relative to trunk)
    stage_label_side_policy: SidePolicy = "left"

    # Extra gap between trunk edge and nearest label edge (pixels).
    # Actual clearance = 0.5 * stroke_at_stage + stage_label_clearance
    stage_label_clearance: float = 18.0

    # Branch routing
    # "direct"  — simple source→target bezier (fast, can cross labels)
    # "elbow"   — depart laterally to lane secondary first, then curve to target
    #             this keeps branches from cutting through downstream text
    branch_route_mode: BranchRouteMode = "elbow"
    # At what fraction along the primary axis the elbow reaches full secondary extent.
    # 0.0 = elbow bends immediately at the source; 1.0 = at the target. Default 0.35.
    branch_waypoint_primary_frac: float = 0.35
    # Spline S-curve tension: 0.0 = handles horizontal, 1.0 = handles toward other point.
    branch_spline_tension: float = 0.5
    # Spline handle length as a fraction of the chord length between the two nodes.
    branch_spline_handle_scale: float = 0.4
    branch_layout_mode: BranchLayoutMode = "same_side"
    # Which absolute side branches go to in same_side / same_side_staggered
    branch_side_policy: SidePolicy = "right"

    # In same_side_staggered: short/long offsets from trunk centre in secondary axis
    branch_stagger_short: float = 180.0
    branch_stagger_long: float = 340.0

    # In same_side / alternate_sides: fixed secondary offset
    branch_offset_secondary: float = 260.0

    # Primary-axis gap between trunk nodes
    trunk_gap_primary: float = 108.0

    # Primary-axis spacing between exclusion nodes (used to spread along primary axis)
    branch_gap_primary: float = 88.0

    # Starting position of trunk spine (primary, secondary axes)
    trunk_start_primary: float = 200.0
    trunk_start_secondary: float = 460.0

    # Text wrap limits
    text_max_width_main_trunk: int = 220
    text_max_width_stage_label: int = 190
    text_max_width_exclusion_node: int = 320

    # Collision resolution gap between text blocks
    min_block_gap: float = 14.0

    # Exclusion node circle radius and gap from circle edge to text
    exclusion_node_radius: float = 8.0
    exclusion_text_margin: float = 14.0

    # Trunk node circle radius (used for stroke clearance and canvas bounds)
    trunk_node_radius: float = 5.5

    # Organicness of trunk path (two overlaid sine waves on secondary axis).
    # Set both amplitudes to 0 to disable.
    organic_amplitude_1: float = 32.0
    organic_freq_1: float = 0.65
    organic_amplitude_2: float = 14.0
    organic_freq_2: float = 1.4
    organic_phase_2: float = 1.1

    # Highlight mode for start/end trunk nodes (nodes with highlight=True).
    # Controls visual treatment — see HighlightMode for options.
    highlight_mode: HighlightMode = "ring_fill"
    # Which side of the node the badge label is placed on — see BadgeSide.
    highlight_badge_side: BadgeSide = "outward"
    # Whether to also visually highlight the stage labels at the start/end nodes.
    highlight_stage_labels: bool = True


# ---------------------------------------------------------------------------
# Style
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SnakeyStyle:
    font_family: str = "Manrope"
    embedded_font_family: str = "Manrope"
    embedded_font_files: tuple[str, ...] = ("assets/fonts/manrope/Manrope-VariableFont_wght.ttf",)
    background_start: str = "#f8fafc"
    background_end: str = "#eef2f7"
    title_color: str = "#0f172a"
    subtitle_color: str = "#334155"
    main_trunk_color: str = "#0284c7"
    main_trunk_opacity: float = 1.0
    exclusion_branch_color: str = "#f59e0b"
    exclusion_branch_opacity: float = 1.0
    main_trunk_node_fill: str = "#0369a1"
    exclusion_node_fill: str = "#b45309"
    text_color: str = "#0f172a"
    text_subtle_color: str = "#334155"
    context_color: str = "#64748b"
    title_size: int = 36
    subtitle_size: int = 20
    # Gap between the title text block and the subtitle text block (pixels).
    title_subtitle_gap: int = 12
    # Extra whitespace below the subtitle and above the first trunk node (pixels).
    title_block_margin: int = 24
    block_title_size: int = 18
    block_count_size: int = 16
    block_context_size: int = 14
    block_line_height: int = 20
    block_padding_x: int = 10
    block_padding_y: int = 8
    trunk_stroke_min: float = 3.0
    trunk_stroke_max: float = 68.0
    branch_stroke_min: float = 2.8
    branch_stroke_max: float = 36.0
    stroke_scale_exponent: float = 0.7
    # Highlight style — applied to trunk nodes that have highlight=True
    highlight_node_fill: str = "#38bdf8"
    highlight_node_ring_color: str = "#ffffff"
    highlight_node_ring_width: float = 3.0
    # Badge: SVG icon rendered near start/end nodes when highlight_mode includes "badge".
    #
    # highlight_badge_svg  — SVG path `d=` string drawn on a 24×24 coordinate system.
    #   The path is scaled to highlight_badge_size px and rotated to point in the flow
    #   direction (down for top_down layouts, right for left_right layouts).
    #   Default: Tailwind ChevronDoubleDown — two downward chevron strokes.
    #   Set to "" to disable the SVG icon and use highlight_badge_text instead.
    #
    # highlight_badge_text — fallback text labels (index 0 = start, 1 = end).
    #   Only used when highlight_badge_svg = "".
    #   Set either element to "" to suppress that badge.
    #   Example: ["Beginn", "Ende"] for german labels.
    #
    # highlight_badge_side — where the badge is placed relative to the node.
    #   "outward"   : along flow axis, away from diagram centre (above start / below end
    #                 for top_down; left start / right end for left_right)  [recommended]
    #   "inward"    : along flow axis, toward diagram centre (mirrored from outward)
    #   "left_right": cross-flow — start badge on left,  end badge on right
    #   "right_left": cross-flow — start badge on right, end badge on left
    #   "above" / "below" / "left" / "right" : absolute side, same for both nodes
    #
    # highlight_badge_size — width = height of the SVG icon in pixels.
    # highlight_badge_font_size — font size used when falling back to text badges.
    # highlight_badge_color — stroke color (SVG icon) or fill color (text).
    # highlight_badge_gap  — extra space between ring/stroke edge and badge centre.
    highlight_badge_svg: str = "M19.5 5.25l-7.5 7.5-7.5-7.5m15 6l-7.5 7.5-7.5-7.5"
    highlight_badge_size: float = 24.0
    highlight_badge_text: tuple[str, str] = ("", "")
    highlight_badge_color: str = "#0f172a"
    highlight_badge_font_size: int = 18
    highlight_badge_gap: float = 14.0
    # Stage label highlight — overrides title color/weight for stage labels with highlight=True
    highlight_stage_label_title_color: str = "#0284c7"
    highlight_stage_label_font_weight: str = "700"


@dataclass(frozen=True)
class PageLayoutConfig:
    size_mode: PageSizeMode = "content"
    fit_mode: PageFitMode = "contain"
    align_x: PageAlignX = "center"
    align_y: PageAlignY = "top"
    paper_size: PaperSize = "A4"
    paper_orientation: PaperOrientation = "portrait"
    page_width_px: int = 1900
    page_height_px: int = 1160
    page_padding_px: int = 24


# ---------------------------------------------------------------------------
# Resolved text
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedTextLine:
    text: str
    kind: Literal["title", "count", "context"]


@dataclass(frozen=True)
class ResolvedBlock:
    lines: list[ResolvedTextLine]
    width: float
    height: float


# ---------------------------------------------------------------------------
# Scene geometry — the three primitives layout produces
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeAnchor:
    """Circle endpoint — used for edge routing and circle rendering."""
    key: str
    role: Literal["main_trunk", "exclusion_node"]
    x: float
    y: float
    radius: float
    value: int
    highlight: bool = False


@dataclass(frozen=True)
class TextAnchor:
    """Text block placement — always moves together with its associated circle."""
    node_key: str
    role: Literal["main_trunk", "stage_label", "exclusion_node"]
    # top-left corner of the text block
    x: float
    y: float
    align: Literal["start", "end"]
    block: ResolvedBlock
    text: TextBlock
    highlight: bool = False


@dataclass(frozen=True)
class EdgeRoute:
    """Logical edge — path is computed by the renderer from final node positions."""
    source_key: str
    target_key: str
    role: Literal["main_trunk", "exclusion_branch"]
    value: int
    stroke_width: float
    # Optional elbow waypoint in SVG coords; if set renderer uses a shaped cubic.
    waypoint: tuple[float, float] | None = None


@dataclass(frozen=True)
class Scene:
    title: str
    subtitle: str
    width: int
    height: int
    node_anchors: list[NodeAnchor]
    text_anchors: list[TextAnchor]
    edge_routes: list[EdgeRoute]
    style: SnakeyStyle
    config: LayoutConfig


# ---------------------------------------------------------------------------
# Internal helpers used across layout + canvas fit
# ---------------------------------------------------------------------------


@dataclass
class BoundingBox:
    x: float
    y: float
    w: float
    h: float
    key: str = field(default="")

    @property
    def x1(self) -> float:
        return self.x + self.w

    @property
    def y1(self) -> float:
        return self.y + self.h
