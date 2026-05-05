from __future__ import annotations

from .types import ResolvedBlock, ResolvedTextLine, SnakeyStyle, TextBlock


def stroke_width_for(
    value: int,
    max_value: int,
    min_stroke: float,
    max_stroke: float,
    exponent: float,
) -> float:
    """Map a value in [0, max_value] to a stroke width in [min_stroke, max_stroke]."""
    if value <= 0 or max_value <= 0:
        return 0.0
    ratio = min(value / max_value, 1.0)
    return min_stroke + (max_stroke - min_stroke) * (ratio ** exponent)


def _line_width_estimate(text: str, font_size: int) -> float:
    if not text:
        return 0.0
    return len(text) * font_size * 0.56


def _wrap_line(text: str, max_width: float, font_size: int) -> list[str]:
    raw = text.strip()
    if not raw:
        return []
    words = raw.split()
    if not words:
        return []
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if _line_width_estimate(candidate, font_size) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def resolve_text_block(text: TextBlock, max_width: int, style: SnakeyStyle) -> ResolvedBlock:
    lines: list[ResolvedTextLine] = []
    title_lines = _wrap_line(text.title, max_width, style.block_title_size)
    count_lines = _wrap_line(text.count, max_width, style.block_count_size)
    context_lines = _wrap_line(text.context, max_width, style.block_context_size)

    for line in title_lines:
        lines.append(ResolvedTextLine(text=line, kind="title"))
    for line in count_lines:
        lines.append(ResolvedTextLine(text=line, kind="count"))
    for line in context_lines:
        lines.append(ResolvedTextLine(text=line, kind="context"))

    if not lines:
        return ResolvedBlock(lines=[], width=0.0, height=0.0)

    max_line_width = 0.0
    for line in lines:
        if line.kind == "title":
            size = style.block_title_size
        elif line.kind == "count":
            size = style.block_count_size
        else:
            size = style.block_context_size
        max_line_width = max(max_line_width, _line_width_estimate(line.text, size))

    content_height = len(lines) * style.block_line_height
    width = max_line_width + style.block_padding_x * 2
    height = content_height + style.block_padding_y * 2
    return ResolvedBlock(lines=lines, width=width, height=height)
