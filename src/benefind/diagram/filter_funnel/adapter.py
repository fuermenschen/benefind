"""Bridge between filter_funnel_meta.json and the generic snakey diagram tool.

All benefind-domain knowledge lives here:
- which steps exist and in what order
- which steps are aggregated (sanity_check)
- German label/reason translations
- how to combine breakdown context with manual comments
- trunk node sequence and naming
"""

from __future__ import annotations

from dataclasses import replace as _replace

from benefind.diagram.snakey import (
    ExclusionNode,
    SnakeyModel,
    StageLabel,
    TextBlock,
    TrunkNode,
)

_REASON_LABELS_DE: dict[str, str] = {
    "IRRELEVANT_PURPOSE": "Nicht relevanter Zweck",
    "NO_INFORMATION": "Zu wenig Information",
    "NOT_EXIST": "Nicht mehr existent",
    "IN_LIQUIDATION": "In Liquidation",
    "OTHER": "Andere",
    "NOT_CATEGORY_A": "Nicht ausschliesslich gemeinnützig tätig",
    "OUTSIDE_BEZIRK_WINTERTHUR": "Ausserhalb des Bezirks Winterthur",
}


def _reason_de(label: str) -> str:
    return _REASON_LABELS_DE.get(label, label)


# ---------------------------------------------------------------------------
# Steps aggregated into the synthetic "sanity_check" node
# ---------------------------------------------------------------------------

_SANITY_CHECK_STEP_IDS = [
    "q04_primary_target_group",
    "q05_founded_year",
    "manual_cleanup_or_unattributed",
]

_MANUAL_RELEVANCE_EXCLUDED = 27
_MANUAL_RELEVANCE_REMAINING = 25

# ---------------------------------------------------------------------------
# Stage definitions: order matters — determines trunk sequence
# ---------------------------------------------------------------------------

_STAGES = [
    {
        "key": "category_a",
        "label": "Nur teilweise steuerbefreit",
        "source_trunk": "trunk_all_orgs",
    },
    {
        "key": "location_winterthur",
        "label": "Restlicher Kanton",
        "source_trunk": "trunk_after_category_a",
    },
    {
        "key": "website_review_exclusion",
        "label": "Keine Website oder offensichtlich unpassend",
        "source_trunk": "trunk_after_location",
    },
    {
        "key": "q01_target_focus",
        "label": "Menschen nicht im Fokus",
        "source_trunk": "trunk_after_website_review",
    },
    {
        "key": "q02_regional_focus",
        "label": "National oder International tätig",
        "source_trunk": "trunk_after_q01",
    },
    {
        "key": "q03_donation_ask",
        "label": "Q03 Spendenaufruf",
        "source_trunk": "trunk_after_q02",
    },
    {
        "key": "sanity_check",
        "label": "Sanity Check",
        "source_trunk": "trunk_after_q03",
        "aggregated_from": _SANITY_CHECK_STEP_IDS,
    },
    {
        "key": "manual_relevance_review",
        "label": "Nicht relevante Organisationen",
        "source_trunk": "trunk_final_active",
        "manual_excluded": _MANUAL_RELEVANCE_EXCLUDED,
    },
]

# ---------------------------------------------------------------------------
# Helpers for reading metadata
# ---------------------------------------------------------------------------


def _format_int(value: int) -> str:
    return f"{value:,}".replace(",", "'")


def _step(steps: list[dict[str, object]], step_id: str) -> dict[str, object]:
    for row in steps:
        if str(row.get("id", "")) == step_id:
            return row
    raise ValueError(f"Step '{step_id}' not found in metadata.")


def _step_excluded(steps: list[dict[str, object]], step_id: str) -> int:
    return int(_step(steps, step_id).get("excluded", 0))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_model(
    meta: dict[str, object],
    step_context: dict[str, str] | None = None,
) -> SnakeyModel:
    """Transform filter_funnel_meta.json content into a SnakeyModel.

    Args:
        meta: Parsed contents of filter_funnel_meta.json.
        step_context: Optional mapping of step_id → free-text comment to
                      append to each exclusion node's context line.
    """
    if step_context is None:
        step_context = {}

    totals = meta.get("totals")
    steps = meta.get("steps")
    if not isinstance(totals, dict) or not isinstance(steps, list):
        raise ValueError(
            "Invalid metadata: expected 'totals' dict and 'steps' list.")

    parsed_all = int(totals["parsed_all"])
    after_category_a = int(totals["after_category_a"])
    after_location = int(totals["after_location_winterthur"])
    final_active = int(totals["final_active"])

    # Resolve each stage's excluded count
    resolved_stages = []
    for stage_def in _STAGES:
        manual_excluded = stage_def.get("manual_excluded")
        if manual_excluded is not None:
            excluded = int(manual_excluded)
        else:
            agg_ids: list[str] = stage_def.get(
                "aggregated_from", [stage_def["key"]])  # type: ignore[assignment]
            excluded = sum(_step_excluded(steps, sid) for sid in agg_ids)
        resolved_stages.append({**stage_def, "excluded": excluded})

    # Build trunk nodes
    # A dummy entry node is prepended so that "Alle Organisationen" receives a
    # stage label (the label mechanism annotates each edge with the target
    # node's text; the very first node would otherwise be unlabelled).
    trunk_nodes = [
        TrunkNode(
            "trunk_entry",
            TextBlock(title="", count=""),
            parsed_all,
        ),
        TrunkNode(
            "trunk_all_orgs",
            TextBlock(title="Alle Organisationen im Kanton Zürich",
                      count=_format_int(parsed_all)),
            parsed_all,
        ),
        TrunkNode(
            "trunk_after_category_a",
            TextBlock(title="Rein gemeinnützig tätig",
                      count=_format_int(after_category_a)),
            after_category_a,
        ),
        TrunkNode(
            "trunk_after_location",
            TextBlock(title="Organisationen im Bezirk Winterthur",
                      count=_format_int(after_location)),
            after_location,
        ),
    ]

    downstream_sequence = [
        ("trunk_after_website_review",
         "Nach Webseiten-Prüfung", "website_review_exclusion"),
        ("trunk_after_q01", "Menschen im Fokus", "q01_target_focus"),
        ("trunk_after_q02", "Regional oder lokal tätig", "q02_regional_focus"),
        ("trunk_after_q03", "Spendenaufruf vorhanden", "q03_donation_ask"),
        ("trunk_final_active", "Für finale manuelle Prüfung freigegeben", "sanity_check"),
        ("trunk_relevant_orgs", "Relevante Organisationen", "manual_relevance_review"),
    ]
    running = after_location
    computed_final_active: int | None = None
    for trunk_key, trunk_label, stage_key in downstream_sequence:
        stage = next(s for s in resolved_stages if s["key"] == stage_key)
        running -= stage["excluded"]
        if trunk_key == "trunk_final_active":
            computed_final_active = running
        trunk_nodes.append(
            TrunkNode(
                trunk_key,
                TextBlock(title=trunk_label, count=_format_int(running)),
                running,
            )
        )

    if computed_final_active is None or computed_final_active != final_active:
        raise ValueError(
            f"Consistency mismatch: computed final_active={computed_final_active}, "
            f"metadata final_active={final_active}."
        )

    if running != _MANUAL_RELEVANCE_REMAINING:
        raise ValueError(
            "Consistency mismatch: computed relevant organisations="
            f"{running}, expected={_MANUAL_RELEVANCE_REMAINING}."
        )

    # Mark the first and last trunk nodes as highlighted so the renderer can
    # visually distinguish the pipeline entry point and final output node.
    trunk_nodes[0] = _replace(trunk_nodes[0], highlight=True)
    trunk_nodes[-1] = _replace(trunk_nodes[-1], highlight=True)

    # Stage labels (one per trunk edge).
    # The label adjacent to a highlighted trunk node gets highlight=True so the
    # renderer can optionally emphasise it.
    # - Start node (trunk_entry): highlight the first label that has non-empty text
    #   (the entry node itself has blank text, so we use its *outgoing* label)
    # - End node: highlight the label whose target is the end node
    start_key = trunk_nodes[0].key  # trunk_entry — source of first label
    end_key = trunk_nodes[-1].key   # target of last label

    def _stage_highlight(prev: TrunkNode, nxt: TrunkNode) -> bool:
        # Highlight label departing from start node (if it has text content)
        if prev.key == start_key and bool(nxt.text.title):
            return True
        # Highlight label arriving at end node
        if nxt.key == end_key and bool(nxt.text.title):
            return True
        return False

    stage_labels = [
        StageLabel(
            key=f"stage_{nxt.key}",
            text=TextBlock(title=nxt.text.title,
                           count=nxt.text.count, context=""),
            source_trunk_key=prev.key,
            target_trunk_key=nxt.key,
            highlight=_stage_highlight(prev, nxt),
        )
        for prev, nxt in zip(trunk_nodes[:-1], trunk_nodes[1:], strict=False)
    ]

    # Exclusion nodes
    exclusion_nodes = [
        ExclusionNode(
            key=f"ex_{stage['key']}",
            text=TextBlock(
                title=f"Ausschluss: {stage['label']}",
                count=_format_int(stage["excluded"]),
                context=step_context.get(stage["key"], ""),
            ),
            value=stage["excluded"],
            source_trunk_key=stage["source_trunk"],
        )
        for stage in resolved_stages
    ]

    return SnakeyModel(
        title="Benefizpartner:innen-Suche 2026",
        subtitle="Strukturierte Analyse aller Organisationen, die in Frage kommen könnten. Basiert auf publizierter Liste steuerbefreiter Organisationen im Kanton Zürich.",
        trunk_nodes=trunk_nodes,
        stage_labels=stage_labels,
        exclusion_nodes=exclusion_nodes,
    )
