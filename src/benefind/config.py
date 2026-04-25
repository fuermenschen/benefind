"""Configuration loading and validation.

Loads settings from TOML config files and provides them as typed dataclasses
to the rest of the application. Supports local overrides via settings.local.toml.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from string import Formatter

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
    prepare_include_subdomains: bool = False
    prepare_keep_ranked_urls_per_org: int = 80
    prepare_discovery_safety_cap: int = 2000
    prepare_stale_sitemap_days: int = 365
    prepare_section_cap_per_org: int = 20
    prepare_sitemap_max_files: int = 50
    prepare_sitemap_max_depth: int = 4
    prepare_fallback_max_visits: int = 120
    prepare_max_workers: int = 32
    clean_min_segment_chars: int = 80
    clean_min_duplicate_page_ratio: float = 0.6
    clean_retain_one_duplicate_copy: bool = True
    clean_min_usable_chars_per_org: int = 1


@dataclass
class SearchConfig:
    provider: str = "brave"
    review_search_engine: str = "duckduckgo"
    max_results: int = 10
    min_results_before_broad_search: int = 3
    fallback_score_threshold: int = 25
    fallback_min_score_gap: int = 8
    auto_accept_score: int = 40
    llm_verify_min_score: int = 20
    llm_verify_max_score: int = 39
    cross_provider_agree_min_score: int = 20
    llm_verify_enabled: bool = True
    max_requests_per_second: float = 45.0
    max_workers: int = 20
    request_delay_seconds: float = 0.0
    timeout_seconds: int = 15
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    firecrawl_enabled: bool = True
    firecrawl_max_results: int = 10
    firecrawl_timeout_seconds: int = 30
    firecrawl_max_retries: int = 2
    discover_verify_llm_enabled: bool = True
    discover_verify_llm_min_score: int = 25
    discover_verify_llm_auto_confirm_score: int = 80


@dataclass
class LlmConfig:
    model: str = "gpt-4o-mini"
    temperature: float = 0.1
    max_tokens: int = 1000
    confidence_threshold: float = 0.7


@dataclass
class ZefixConfig:
    timeout_seconds: int = 20
    max_retries: int = 2
    retry_backoff_seconds: float = 1.0
    max_requests_per_second: float = 4.0
    max_burst: int = 2
    max_workers: int = 8
    candidate_preview_limit: int = 8


@dataclass
class PromptDefinition:
    id: str
    description: str
    template: str
    placeholders: dict[str, str] = field(default_factory=dict)
    response_format: str = "text"
    response_required_keys: list[str] = field(default_factory=list)
    source_path: str = ""


@dataclass
class MunicipalityConfig:
    name: str = "Bezirk Winterthur"
    municipalities: list[str] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    excluded_municipalities: list[str] = field(default_factory=list)


@dataclass
class UrlScoringConfig:
    favor_tokens: list[str] = field(default_factory=list)
    favor_regexes: list[str] = field(default_factory=list)
    penalize_tokens: list[str] = field(default_factory=list)
    penalize_regexes: list[str] = field(default_factory=list)
    exclude_tokens: list[str] = field(default_factory=list)
    exclude_regexes: list[str] = field(default_factory=list)
    cms_scaffold_segments: list[str] = field(default_factory=list)
    technical_root_segments: list[str] = field(default_factory=list)
    technical_segment_pairs: list[list[str]] = field(default_factory=list)
    non_html_extensions: list[str] = field(default_factory=list)


@dataclass
class Settings:
    log_level: str = "INFO"
    pdf: PdfConfig = field(default_factory=PdfConfig)
    filtering: FilteringConfig = field(default_factory=FilteringConfig)
    scraping: ScrapingConfig = field(default_factory=ScrapingConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    llm: LlmConfig = field(default_factory=LlmConfig)
    zefix: ZefixConfig = field(default_factory=ZefixConfig)
    municipalities: MunicipalityConfig = field(default_factory=MunicipalityConfig)
    prompts: dict[str, PromptDefinition] = field(default_factory=dict)


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
    4. config/prompts/*.toml (prompt registry)
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

    prompt_registry = load_prompt_registry(config_dir)

    # Build Settings object
    general = settings_data.get("general", {})
    return Settings(
        log_level=general.get("log_level", "INFO"),
        pdf=PdfConfig(**settings_data.get("pdf", {})),
        filtering=FilteringConfig(**settings_data.get("filtering", {})),
        scraping=ScrapingConfig(**settings_data.get("scraping", {})),
        search=SearchConfig(**settings_data.get("search", {})),
        llm=LlmConfig(**settings_data.get("llm", {})),
        zefix=ZefixConfig(**settings_data.get("zefix", {})),
        municipalities=MunicipalityConfig(
            **muni_data.get("bezirk_winterthur", {}),
        ),
        prompts=prompt_registry,
    )


def _template_placeholders(template: str) -> set[str]:
    names: set[str] = set()
    for _, field_name, _, _ in Formatter().parse(template):
        if field_name:
            names.add(field_name)
    return names


def _validate_prompt_definition(prompt: PromptDefinition) -> None:
    prompt_id = str(prompt.id or "").strip()
    if not prompt_id:
        raise ValueError("Prompt id is required")

    template = str(prompt.template or "")
    if not template.strip():
        raise ValueError(f"Prompt '{prompt_id}' has an empty template")

    declared = set(prompt.placeholders.keys())
    discovered = _template_placeholders(template)
    missing = sorted(discovered - declared)
    unused = sorted(declared - discovered)
    if missing or unused:
        detail_parts: list[str] = []
        if missing:
            detail_parts.append(f"missing declarations: {', '.join(missing)}")
        if unused:
            detail_parts.append(f"unused declarations: {', '.join(unused)}")
        detail = "; ".join(detail_parts)
        raise ValueError(f"Prompt '{prompt_id}' placeholder mismatch ({detail})")

    if prompt.response_format == "json_object":
        if not prompt.response_required_keys:
            raise ValueError(
                f"Prompt '{prompt_id}' requires response.required_keys for json_object format"
            )


def load_prompt_registry(config_dir: Path | None = None) -> dict[str, PromptDefinition]:
    config_dir = config_dir or CONFIG_DIR
    prompt_dir = config_dir / "prompts"
    if not prompt_dir.exists() or not prompt_dir.is_dir():
        raise ValueError(f"Prompt directory not found: {prompt_dir}")

    prompt_files = sorted(path for path in prompt_dir.glob("*.toml") if path.is_file())
    if not prompt_files:
        raise ValueError(f"No prompt files found in {prompt_dir}")

    registry: dict[str, PromptDefinition] = {}
    for prompt_path in prompt_files:
        data = _load_toml(prompt_path)
        prompt_data = data.get("prompt", {})
        if not isinstance(prompt_data, dict) or not prompt_data:
            raise ValueError(f"Prompt file must define [prompt]: {prompt_path}")

        placeholders_raw = prompt_data.get("placeholders", {})
        if not isinstance(placeholders_raw, dict):
            raise ValueError(f"Prompt placeholders must be a table in {prompt_path}")
        placeholders = {
            str(key).strip(): str(value).strip() for key, value in placeholders_raw.items()
        }

        response_raw = prompt_data.get("response", {})
        if not isinstance(response_raw, dict):
            raise ValueError(f"Prompt response must be a table in {prompt_path}")
        response_format = str(response_raw.get("format", "text") or "text").strip()
        required_keys_raw = response_raw.get("required_keys", [])
        if required_keys_raw is None:
            required_keys_raw = []
        if not isinstance(required_keys_raw, list):
            raise ValueError(f"Prompt response.required_keys must be an array in {prompt_path}")
        required_keys = [str(value).strip() for value in required_keys_raw if str(value).strip()]

        prompt = PromptDefinition(
            id=str(prompt_data.get("id", "") or "").strip(),
            description=str(prompt_data.get("description", "") or "").strip(),
            template=str(prompt_data.get("template", "") or ""),
            placeholders=placeholders,
            response_format=response_format,
            response_required_keys=required_keys,
            source_path=str(prompt_path),
        )
        _validate_prompt_definition(prompt)

        if prompt.id in registry:
            existing_path = registry[prompt.id].source_path
            raise ValueError(
                f"Duplicate prompt id '{prompt.id}' in {prompt_path} and {existing_path}"
            )
        registry[prompt.id] = prompt

    return registry


def render_prompt_template(prompt: PromptDefinition, values: dict[str, object]) -> str:
    declared = set(prompt.placeholders.keys())
    provided = set(values.keys())
    missing = sorted(declared - provided)
    unexpected = sorted(provided - declared)
    if missing or unexpected:
        detail_parts: list[str] = []
        if missing:
            detail_parts.append(f"missing values: {', '.join(missing)}")
        if unexpected:
            detail_parts.append(f"unexpected values: {', '.join(unexpected)}")
        detail = "; ".join(detail_parts)
        raise ValueError(f"Prompt '{prompt.id}' render failed ({detail})")

    format_values = {key: str(value) for key, value in values.items()}
    return prompt.template.format(**format_values)


def load_url_scoring_config(config_dir: Path | None = None) -> UrlScoringConfig:
    """Load config/url_scoring.toml into a UrlScoringConfig object."""
    config_dir = config_dir or CONFIG_DIR
    scoring_data = _load_toml(config_dir / "url_scoring.toml")
    return UrlScoringConfig(**scoring_data.get("url_scoring", {}))
