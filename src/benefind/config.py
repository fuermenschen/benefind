"""Configuration loading and validation.

Loads settings from TOML config files and provides them as typed dataclasses
to the rest of the application. Supports local overrides via settings.local.toml.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PdfConfig:
    source_url: str = ""
    filename: str = "steuerbefreite_jp.pdf"


@dataclass
class FilteringConfig:
    fuzzy_match_threshold: int = 85
    include_unknown_locations: bool = False
    use_category_filter: bool = True
    manual_review_warning_threshold: int = 50
    exact_match_only: bool = True


@dataclass
class ScrapingConfig:
    request_delay_seconds: float = 2.0
    max_pages_per_org: int = 10
    timeout_seconds: int = 30
    user_agent: str = "benefind/0.1 (nonprofit research; https://hfm-winti.ch)"
    respect_robots_txt: bool = True


@dataclass
class SearchConfig:
    provider: str = "brave"
    max_results: int = 10
    min_results_before_broad_search: int = 3
    fallback_score_threshold: int = 25
    fallback_min_score_gap: int = 8
    auto_accept_score: int = 40
    llm_verify_min_score: int = 20
    llm_verify_max_score: int = 39
    llm_verify_enabled: bool = True
    max_requests_per_second: float = 45.0
    max_workers: int = 20
    request_delay_seconds: float = 0.0
    timeout_seconds: int = 15
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0


@dataclass
class LlmConfig:
    model: str = "gpt-4o-mini"
    temperature: float = 0.1
    max_tokens: int = 1000
    confidence_threshold: float = 0.7


@dataclass
class PromptTemplate:
    id: str
    description: str
    question: str
    answer_type: str = "text"


@dataclass
class MunicipalityConfig:
    name: str = "Bezirk Winterthur"
    municipalities: list[str] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    excluded_municipalities: list[str] = field(default_factory=list)


@dataclass
class Settings:
    log_level: str = "INFO"
    pdf: PdfConfig = field(default_factory=PdfConfig)
    filtering: FilteringConfig = field(default_factory=FilteringConfig)
    scraping: ScrapingConfig = field(default_factory=ScrapingConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    llm: LlmConfig = field(default_factory=LlmConfig)
    municipalities: MunicipalityConfig = field(default_factory=MunicipalityConfig)
    prompts: list[PromptTemplate] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _load_toml(path: Path) -> dict:
    """Load a TOML file and return its contents as a dict."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def _merge_dicts(base: dict, override: dict) -> dict:
    """Recursively merge override into base."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_settings(config_dir: Path | None = None) -> Settings:
    """Load and merge all configuration files into a Settings object.

    Loads in order (later files override earlier):
    1. config/settings.toml
    2. config/settings.local.toml (if exists, gitignored)
    3. config/municipalities.toml
    4. config/prompts.toml
    """
    config_dir = config_dir or CONFIG_DIR

    # --- settings.toml ---
    settings_data = _load_toml(config_dir / "settings.toml")

    # --- settings.local.toml (optional override) ---
    local_path = config_dir / "settings.local.toml"
    if local_path.exists():
        local_data = _load_toml(local_path)
        settings_data = _merge_dicts(settings_data, local_data)

    # --- municipalities.toml ---
    muni_data = _load_toml(config_dir / "municipalities.toml")

    # --- prompts.toml ---
    prompts_data = _load_toml(config_dir / "prompts.toml")

    # Build Settings object
    general = settings_data.get("general", {})
    return Settings(
        log_level=general.get("log_level", "INFO"),
        pdf=PdfConfig(**settings_data.get("pdf", {})),
        filtering=FilteringConfig(**settings_data.get("filtering", {})),
        scraping=ScrapingConfig(**settings_data.get("scraping", {})),
        search=SearchConfig(**settings_data.get("search", {})),
        llm=LlmConfig(**settings_data.get("llm", {})),
        municipalities=MunicipalityConfig(
            **muni_data.get("bezirk_winterthur", {}),
        ),
        prompts=[PromptTemplate(**p) for p in prompts_data.get("prompts", [])],
    )
