# Visualization

The filter-funnel Snakey diagram shows how many organizations remain after each
pipeline step, from the raw parsed list down to the final active cohort. It is
a Sankey-style diagram: the width of the trunk ribbon is proportional to the
surviving count, and each exclusion branches off to the side.

Run the diagram after `benefind classify` is complete. Two steps are required:
first build the meta JSON that aggregates funnel counts, then render it into
an SVG (and optionally a PNG).

## Workflow

### Step 1 — build the meta JSON

```bash
uv run python scripts/build_filter_funnel_meta.py
```

Reads the canonical pipeline artifacts from `data/` and writes a single JSON
file with step-level counts to `data/meta/filter_funnel_meta.json`.

Rerun this step whenever the underlying pipeline data changes (new exclusions,
updated classifications, etc.). For easier inspection of the JSON:

```bash
uv run python scripts/build_filter_funnel_meta.py --pretty
```

The script will fail with a clear message listing any missing input files if
the pipeline has not been run to completion yet.

### Step 2 — render the diagram

```bash
uv run python scripts/render_filter_funnel_snakey.py
```

Reads `data/meta/filter_funnel_meta.json` and writes
`data/meta/filter_funnel_snakey.svg` and `data/meta/filter_funnel_snakey.png`.

To use a specific visual config (see [Styling and layout](#styling-and-layout)):

```bash
uv run python scripts/render_filter_funnel_snakey.py \
  --config data/meta/filter_funnel_snakey_config_2026.toml
```

To render SVG only (skips the Playwright PNG step):

```bash
uv run python scripts/render_filter_funnel_snakey.py --format svg
```

Full options:

```
--input PATH      Meta JSON input (default: data/meta/filter_funnel_meta.json)
--output PATH     SVG output path (default: data/meta/filter_funnel_snakey.svg)
--config PATH     Layout/style TOML or JSON config file
--comments PATH   Step context JSON (see Annotating stages below)
--format          svg | png | both  (default: both)
--orientation     top_down | left_right  (default: top_down)
--branch-side     right | left | alternate  (default: right)
--stage-label-side  right | left | alternate  (default: left)
--width INT       Canvas width in px (default: 1900)
--height INT      Canvas height in px (default: 1160)
--scale INT       PNG device pixel ratio (default: 2)
```

CLI flags override anything set in the config file.


## Annotating stages

Each diagram stage can carry a short context line — displayed below the
surviving-count text. Edit `data/meta/filter_funnel_snakey_comments_2026.json`:

```json
{
  "step_context": {
    "category_a":             "Only category (a) organizations",
    "location_winterthur":    "Bezirk Winterthur only",
    "website_review_exclusion": "Excluded during website review",
    "q01_target_focus":       "Must have a clear target-group focus"
  }
}
```

Keys match the `id` values in `filter_funnel_meta_2026.json` → `steps`. Omit a key
to show no context for that stage. An example file with all step IDs is at
`data/meta/filter_funnel_snakey_comments_example.json`.

Pass the file to the renderer:

```bash
uv run python scripts/render_filter_funnel_snakey.py \
  --comments data/meta/filter_funnel_snakey_comments_2026.json
```


## Styling and layout

All visual and layout options are documented with inline comments in:

```
data/meta/filter_funnel_snakey_config_example.toml
```

The file is split into two tables:

- `[layout]` — canvas size, node spacing, branch routing, organic wobble,
  highlight mode
- `[style]` — colours, font sizes, stroke widths, badge appearance

Only the fields you want to override need to be set; everything else falls back
to the defaults shown in comments. The minimal working config is just an empty
file (all defaults apply).

The committed 2026 config is at `data/meta/filter_funnel_snakey_config_2026.toml`
and serves as a fully annotated working example. Use it as a starting point for
future years.


## Highlight modes

Start and end trunk nodes can be visually distinguished with
`highlight_mode` in `[layout]`. The six modes compose independently:

| Mode | What it adds |
|---|---|
| `fill` | Different fill colour on the node circle |
| `ring` | Stroke ring around the node circle |
| `ring_fill` | Ring + fill colour |
| `badge` | SVG icon (or text fallback) near the node |
| `ring_badge` | Ring + badge |
| `ring_fill_badge` | Ring + fill + badge |

The default badge is a Tailwind `ChevronDoubleDown` icon, automatically rotated
to point in the flow direction. To use a custom icon, set `highlight_badge_svg`
to any SVG path `d=` string drawn on a 24×24 grid. To use text labels instead
(e.g. German), set `highlight_badge_svg = ""` and
`highlight_badge_text = ["Beginn", "Ende"]`.

See the `# Badge` and `# Highlight` sections in
`filter_funnel_snakey_config_example.toml` for all badge options with defaults.


## PNG export

PNG rendering requires Playwright with a Chromium browser. Install once:

```bash
uv run playwright install chromium
```

The renderer opens the SVG in a headless browser and takes a full-page
screenshot. If Playwright is unavailable, render with `--format svg` instead.


## Re-running

| Changed | What to rerun |
|---|---|
| Pipeline data (new exclusions, classify results) | Both steps — `build_filter_funnel_meta.py` then `render_filter_funnel_snakey.py` |
| Visual config only (colours, layout, highlight) | `render_filter_funnel_snakey.py` only |
| Stage annotations only | `render_filter_funnel_snakey.py` only |
