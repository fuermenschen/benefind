"""Shared UI components and styling for all benefind CLI interactions.

All commands should use the helpers here for consistent visual output.
Design principles:
- Rich panels for all structured output
- Single-key shortcuts for review loops
- Y/n confirmations via questionary
- Adaptive panel width to terminal
- Clickable URLs (degrades gracefully)
- Never use emojis – icons are ASCII / Unicode symbols only
"""

from __future__ import annotations

import select
import sys
import termios
import tty
from dataclasses import dataclass, field
from typing import Any

import questionary
from questionary import Choice
from questionary import Style as QStyle
from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ---------------------------------------------------------------------------
# Singleton console – import and use this everywhere instead of creating your own
# ---------------------------------------------------------------------------
console = Console()

# ---------------------------------------------------------------------------
# Questionary style matching the Rich colour palette
# ---------------------------------------------------------------------------
QUESTIONARY_STYLE = QStyle(
    [
        ("qmark", "fg:cyan bold"),
        ("question", "bold"),
        ("answer", "fg:green bold"),
        ("pointer", "fg:cyan bold"),
        ("highlighted", "fg:cyan bold"),
        ("selected", "fg:green"),
        ("separator", "fg:blue"),
        ("instruction", "fg:ansigray"),
    ]
)

# ---------------------------------------------------------------------------
# Colours (use as Rich markup strings)
# ---------------------------------------------------------------------------
C_PRIMARY = "bold cyan"
C_SECONDARY = "dim white"
C_URL = "blue underline"
C_SUCCESS = "bold green"
C_WARNING = "bold yellow"
C_ERROR = "bold red"
C_SCORE_HIGH = "green"
C_SCORE_MED = "yellow"
C_SCORE_LOW = "red"
C_KEY = "bold white on blue"
C_MUTED = "dim"
C_PROGRESS = "cyan"
C_BORDER = "blue"

# ---------------------------------------------------------------------------
# Unicode symbols (no emoji – pure Unicode that renders in any monospace font)
# ---------------------------------------------------------------------------
SYM_OK = "[bold green]✓[/bold green]"
SYM_SKIP = "[bold yellow]⊙[/bold yellow]"
SYM_FAIL = "[bold red]✗[/bold red]"
SYM_WARN = "[bold yellow]![/bold yellow]"
SYM_INFO = "[cyan]i[/cyan]"
SYM_URL = "[blue]→[/blue]"
SYM_LOC = "[cyan]@[/cyan]"

# ---------------------------------------------------------------------------
# Width helpers
# ---------------------------------------------------------------------------


def panel_width() -> int:
    """Adaptive panel width: fits terminal but caps at 90 for readability."""
    w = console.width
    if w < 50:
        return w
    return min(w - 2, 90)


# ---------------------------------------------------------------------------
# Panel builders
# ---------------------------------------------------------------------------


def make_panel(content: Any, title: str, *, border_style: str = C_BORDER) -> Panel:
    """Create a consistent titled panel."""
    return Panel(
        content,
        title=f"[bold]{title}[/bold]",
        title_align="left",
        border_style=border_style,
        width=panel_width(),
        padding=(0, 1),
    )


def make_kv_table(rows: list[tuple[str, Any]], *, show_header: bool = False) -> Table:
    """Create a two-column key→value table without visible borders."""
    table = Table(
        show_header=show_header,
        show_edge=False,
        box=None,
        pad_edge=False,
        expand=False,
    )
    table.add_column("key", style=C_SECONDARY, min_width=14, no_wrap=True)
    table.add_column("value")
    for label, value in rows:
        table.add_row(label, value if isinstance(value, str) else str(value))
    return table


def make_actions_table(actions: list[tuple[str, str]]) -> Table:
    """Render action keys as a compact multi-column layout.

    ``actions`` is a list of (key, description) pairs.
    The keys are highlighted; the descriptions follow inline.
    """
    # Compose each item as "[ k ] description"
    items = []
    for key, desc in actions:
        t = Text()
        t.append(f" {key} ", style=C_KEY)
        t.append(f" {desc}", style="white")
        items.append(t)

    # Lay them out in columns; Rich figures out the best column count
    return Columns(items, equal=False, expand=False)


# ---------------------------------------------------------------------------
# Screen management
# ---------------------------------------------------------------------------


def clear() -> None:
    """Clear the terminal screen."""
    console.clear()


def print_panel(content: Any, title: str, *, border_style: str = C_BORDER) -> None:
    """Convenience: print a titled panel directly to the console."""
    console.print(make_panel(content, title, border_style=border_style))


# ---------------------------------------------------------------------------
# Feedback helpers
# ---------------------------------------------------------------------------


def print_success(message: str) -> None:
    console.print(f"  {SYM_OK} [bold green]{message}[/bold green]")


def print_skip(message: str) -> None:
    console.print(f"  {SYM_SKIP} [bold yellow]{message}[/bold yellow]")


def print_warning(message: str) -> None:
    console.print(f"  {SYM_WARN} [bold yellow]{message}[/bold yellow]")


def print_error(message: str) -> None:
    console.print(f"  {SYM_FAIL} [bold red]{message}[/bold red]")


# ---------------------------------------------------------------------------
# URL formatting (clickable in modern terminals, plain text fallback)
# ---------------------------------------------------------------------------


def fmt_url(url: str) -> str:
    """Return a Rich-markup string for a URL with click link if supported."""
    if not url:
        return "[dim]-[/dim]"
    # Rich's link markup: [link=url]text[/link]
    return f"[{C_URL}][link={url}]{url}[/link][/{C_URL}]"


def fmt_score(score: int | str | None) -> str:
    """Return a colour-coded score string."""
    if score is None or str(score).strip() in ("", "nan"):
        return "[dim]-[/dim]"
    try:
        v = int(float(str(score)))
    except (ValueError, TypeError):
        return f"[dim]{score}[/dim]"
    color = C_SCORE_HIGH if v >= 40 else (C_SCORE_MED if v >= 20 else C_SCORE_LOW)
    return f"[{color}]{v}[/{color}]"


def fmt_confidence(conf: str | None) -> str:
    """Return a colour-coded confidence string."""
    c = (conf or "").strip().lower()
    if c == "high":
        return f"[{C_SCORE_HIGH}]{c}[/{C_SCORE_HIGH}]"
    if c == "medium":
        return f"[{C_SCORE_MED}]{c}[/{C_SCORE_MED}]"
    if c in ("low", "none", "excluded"):
        return f"[{C_SCORE_LOW}]{c}[/{C_SCORE_LOW}]"
    if c == "manual":
        return f"[cyan]{c}[/cyan]"
    return f"[dim]{conf or '-'}[/dim]"


# ---------------------------------------------------------------------------
# Single-key capture (Unix / macOS only – no Windows needed)
# ---------------------------------------------------------------------------


def _getch() -> str:
    """Read one keypress burst from stdin without waiting for Enter."""
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        chars = [sys.stdin.read(1)]
        while True:
            readable, _, _ = select.select([sys.stdin], [], [], 0)
            if not readable:
                break
            chars.append(sys.stdin.read(1))
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return "".join(chars)


def wait_for_key(
    valid_keys: list[str],
    *,
    prompt: str = "Action: ",
    show_prompt: bool = True,
) -> str:
    """Block until the user presses one of the valid keys (case-insensitive).

    Returns the matched key in lower-case.
    Ctrl-C raises KeyboardInterrupt so callers can handle quit gracefully.
    """
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print_error("Interactive review requires a TTY terminal.")
        raise KeyboardInterrupt

    valid_lower = {k.lower() for k in valid_keys}
    if show_prompt:
        console.print(f"\n[{C_SECONDARY}]{prompt}[/{C_SECONDARY}]", end="")
    while True:
        ch = _getch()
        if len(ch) > 1 and not ch.startswith("\x1b"):
            first = ch[0].lower()
            if first in valid_lower:
                console.print(f"[bold]{ch[0]}[/bold]")
                print_warning("Ignored extra buffered input; used first key only.")
                return first
            print_warning("Pasted text ignored in hotkey mode. Press one key.")
            continue
        if ch == "\x03":  # Ctrl-C
            raise KeyboardInterrupt
        if ch.startswith("\x1b"):  # Escape (and escape sequences) -> cancel/skip
            console.print()  # newline after prompt
            return "esc"
        key = ch.lower()
        if key in valid_lower:
            console.print(f"[bold]{ch}[/bold]")  # echo the pressed key
            return key


# ---------------------------------------------------------------------------
# Confirmation helpers
# ---------------------------------------------------------------------------


def confirm(message: str, *, default: bool = True) -> bool:
    """Show a Y/n confirmation via questionary."""
    result = questionary.confirm(
        message,
        default=default,
        style=QUESTIONARY_STYLE,
        qmark="",
    ).ask()
    return bool(result)


def ask_text(message: str, *, default: str = "") -> str:
    """Prompt for free text via questionary."""
    result = questionary.text(
        message,
        default=default,
        style=QUESTIONARY_STYLE,
        qmark="",
    ).ask()
    return (result or "").strip()


def ask_checkbox(
    message: str,
    choices: list[tuple[str, str]],
    *,
    default_values: set[str] | None = None,
) -> list[str]:
    """Prompt for multi-select values via questionary checkbox."""
    default_values = default_values or set()
    questionary_choices = [
        Choice(title=title, value=value, checked=value in default_values)
        for title, value in choices
    ]
    result = questionary.checkbox(
        message,
        choices=questionary_choices,
        style=QUESTIONARY_STYLE,
        qmark="",
    ).ask()
    return [str(value) for value in (result or [])]


# ---------------------------------------------------------------------------
# Progress tracking dataclass
# ---------------------------------------------------------------------------


@dataclass
class ReviewProgress:
    """Mutable progress state for a review session."""

    total: int
    current: int = 0
    accepted: int = 0
    skipped: int = 0
    excluded: int = 0
    extra: dict[str, int] = field(default_factory=dict)

    def advance(self) -> None:
        self.current += 1

    def mark_accepted(self) -> None:
        self.accepted += 1

    def mark_skipped(self) -> None:
        self.skipped += 1

    def mark_excluded(self) -> None:
        self.excluded += 1

    def as_panel(self, title: str = "Progress") -> Panel:
        bar = _progress_bar(self.current, self.total, width=30)
        line = Text()
        line.append(f"{self.current}/{self.total}  ", style=C_PROGRESS)
        line.append(bar + "  ")
        line.append(f"✓ {self.accepted} accepted  ", style=C_SCORE_HIGH)
        line.append(f"⊙ {self.skipped} skipped  ", style=C_WARNING)
        line.append(f"✗ {self.excluded} excluded", style=C_ERROR)
        return make_panel(line, title, border_style=C_BORDER)


def _progress_bar(current: int, total: int, *, width: int = 20) -> str:
    if total == 0:
        return "─" * width
    filled = round(current / total * width)
    return "█" * filled + "░" * (width - filled)


# ---------------------------------------------------------------------------
# Summary panel builder
# ---------------------------------------------------------------------------


def print_summary(title: str, rows: list[tuple[str, str | int]]) -> None:
    """Print a summary panel with key/value rows."""
    table = make_kv_table([(label, str(value)) for label, value in rows])
    console.print(make_panel(table, title))
