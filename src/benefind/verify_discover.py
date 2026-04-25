"""Discover false-positive quality gate using rules + optional LLM verification."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

import pandas as pd

from benefind.config import DATA_DIR, Settings, render_prompt_template
from benefind.external_api import ExternalApiAccessError, classify_openai_access_error
from benefind.scrape_clean import load_latest_scrape_clean_summary

DISCOVER_VERIFY_TEXT_COLUMNS = [
    "_discover_verify_status",
    "_discover_verify_confidence",
    "_discover_verify_reason",
    "_discover_verify_stage",
    "_discover_verify_llm_reason",
    "_discover_verify_llm_evidence",
    "_discover_verified_at",
]

DISCOVER_VERIFY_BOOL_COLUMNS = [
    "_discover_verify_needs_review",
    "_discover_verify_rule_name_match",
    "_discover_verify_rule_location_match",
]

DISCOVER_VERIFY_BOOL_OPTIONAL_COLUMNS = ["_discover_verify_llm_belongs"]

DISCOVER_VERIFY_INT_COLUMNS = ["_discover_verify_score", "_discover_verify_llm_score"]


def _bool_or_none(value: object) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def ensure_discover_verify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure discover-verify columns exist with explicit, writable dtypes."""
    for column in DISCOVER_VERIFY_TEXT_COLUMNS:
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].astype(object).where(df[column].notna(), "")

    for column in DISCOVER_VERIFY_BOOL_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = df[column].map(_bool_or_none).astype("boolean")

    for column in DISCOVER_VERIFY_BOOL_OPTIONAL_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = df[column].map(_bool_or_none).astype("boolean")

    for column in DISCOVER_VERIFY_INT_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    return df


@dataclass
class DiscoverVerificationResult:
    status: str
    needs_review: bool
    confidence: str
    score: int
    reason: str
    decision_stage: str
    rule_name_match: bool
    rule_location_match: bool
    llm_belongs: bool | None = None
    llm_score: int | None = None
    llm_reason: str = ""
    llm_evidence: str = ""


def _normalize_ascii(text: str) -> str:
    value = str(text or "")
    return (
        value.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("Ä", "ae")
        .replace("Ö", "oe")
        .replace("Ü", "ue")
        .replace("ß", "ss")
    )


def _normalize_text(text: str) -> str:
    value = _normalize_ascii(text).lower()
    value = re.sub(r"[\[\](){}]", " ", value)
    value = re.sub(r"[^a-z0-9\s-]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _remove_bracket_content(text: str) -> str:
    value = re.sub(r"\([^)]*\)", " ", str(text or ""))
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = re.sub(r"\{[^}]*\}", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _name_variants(name: str) -> list[str]:
    candidates = [str(name or "").strip(), _remove_bracket_content(name)]
    variants: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = _normalize_text(candidate)
        if normalized and normalized not in seen:
            variants.append(normalized)
            seen.add(normalized)
    return variants


def _content_tokens(text: str) -> set[str]:
    return {token for token in _normalize_text(text).split() if len(token) >= 3}


def _name_exact_match(org_name: str, content: str) -> bool:
    normalized_content = f" {_normalize_text(content)} "
    for variant in _name_variants(org_name):
        if len(variant) < 4:
            continue
        if f" {variant} " in normalized_content:
            return True
    return False


def _name_token_match(org_name: str, content: str) -> bool:
    tokens = _content_tokens(content)
    if not tokens:
        return False
    for variant in _name_variants(org_name):
        parts = [p for p in variant.split() if len(p) >= 4]
        if len(parts) < 2:
            continue
        if all(part in tokens for part in parts):
            return True
    return False


def _location_match(location: str, content: str) -> bool:
    target = _normalize_text(location)
    if not target:
        return False
    return f" {target} " in f" {_normalize_text(content)} "


def _domain_name_hint(org_name: str, website_url: str) -> bool:
    host = urlsplit(str(website_url or "").strip()).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return False
    name_tokens = [token for token in _normalize_text(org_name).split() if len(token) >= 4]
    return any(token in host for token in name_tokens)


def collect_clean_content(org_id: str, *, max_files: int = 6, max_chars: int = 4000) -> str:
    pages_cleaned_dir = DATA_DIR / "orgs" / org_id / "pages_cleaned"
    if not pages_cleaned_dir.exists() or not pages_cleaned_dir.is_dir():
        return ""

    chunks: list[str] = []
    total_chars = 0
    for path in sorted(pages_cleaned_dir.glob("*.md"))[:max_files]:
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        if not text:
            continue
        remaining = max_chars - total_chars
        if remaining <= 0:
            break
        clipped = text[:remaining]
        chunks.append(clipped)
        total_chars += len(clipped)

    return "\n\n".join(chunks)


def _extract_json_object(text: str) -> dict:
    stripped = (text or "").strip()
    if not stripped:
        return {}
    try:
        value = json.loads(stripped)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        value = json.loads(stripped[start : end + 1])
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        return {}


def _llm_verify_discover_match(
    *,
    org_name: str,
    org_location: str,
    website_url: str,
    content: str,
    settings: Settings,
) -> tuple[bool | None, int | None, str, str]:
    try:
        from openai import OpenAI
    except Exception:
        return None, None, "llm_unavailable", ""

    if not os.environ.get("OPENAI_API_KEY", ""):
        raise ExternalApiAccessError(
            provider="OpenAI",
            reason="missing_api_key",
            details="OPENAI_API_KEY is not set",
        )

    prompt_def = settings.prompts.get("discover.mismatch_verify")
    if prompt_def is None:
        raise ValueError("Prompt 'discover.mismatch_verify' is missing from prompt registry")

    prompt = render_prompt_template(
        prompt_def,
        {
            "org_name": org_name,
            "org_location": org_location or "-",
            "website_url": website_url or "-",
            "content_snippets": content[:3000] or "-",
        },
    )

    try:
        client = OpenAI()
        response = client.responses.create(
            model=settings.llm.model,
            input=prompt,
            temperature=0,
            max_output_tokens=int(settings.llm.max_tokens),
        )
    except Exception as e:
        access_error = classify_openai_access_error(e)
        if access_error is not None:
            raise access_error
        return None, None, f"llm_error:{e}", ""

    payload = _extract_json_object(str(getattr(response, "output_text", "") or ""))
    belongs_raw = payload.get("belongs")
    if isinstance(belongs_raw, bool):
        belongs = belongs_raw
    else:
        belongs_text = str(belongs_raw or "").strip().lower()
        belongs = belongs_text in {"true", "1", "yes", "y"}

    try:
        score = int(float(payload.get("score", 0) or 0))
    except (TypeError, ValueError):
        score = 0
    score = max(0, min(100, score))

    reason = str(payload.get("reason", "") or "").strip()
    evidence = str(payload.get("evidence", "") or "").strip()
    return belongs, score, reason, evidence


def _build_rule_score(
    *,
    name_exact: bool,
    name_token: bool,
    location_ok: bool,
    domain_hint: bool,
) -> int:
    score = 0
    if name_exact:
        score += 65
    elif name_token:
        score += 40
    if location_ok:
        score += 25
    if domain_hint:
        score += 10
    return max(0, min(100, score))


def verify_discover_match(
    *,
    org_name: str,
    org_location: str,
    website_url: str,
    content: str,
    settings: Settings,
    llm_verify_enabled: bool,
) -> DiscoverVerificationResult:
    name_exact = _name_exact_match(org_name, content)
    name_token = _name_token_match(org_name, content)
    location_ok = _location_match(org_location, content)
    domain_hint = _domain_name_hint(org_name, website_url)
    rule_score = _build_rule_score(
        name_exact=name_exact,
        name_token=name_token,
        location_ok=location_ok,
        domain_hint=domain_hint,
    )

    if name_exact and location_ok:
        return DiscoverVerificationResult(
            status="confirmed",
            needs_review=False,
            confidence="high",
            score=rule_score,
            reason="rules:name_and_location_match",
            decision_stage="rules_auto_confirm",
            rule_name_match=True,
            rule_location_match=True,
        )

    if not llm_verify_enabled or rule_score < int(settings.search.discover_verify_llm_min_score):
        return DiscoverVerificationResult(
            status="review_required",
            needs_review=True,
            confidence="low" if rule_score < 40 else "medium",
            score=rule_score,
            reason="rules:insufficient_match",
            decision_stage="rules_review",
            rule_name_match=bool(name_exact or name_token),
            rule_location_match=location_ok,
        )

    llm_belongs, llm_score, llm_reason, llm_evidence = _llm_verify_discover_match(
        org_name=org_name,
        org_location=org_location,
        website_url=website_url,
        content=content,
        settings=settings,
    )
    auto_score = int(settings.search.discover_verify_llm_auto_confirm_score)
    if llm_belongs and (llm_score or 0) >= auto_score:
        return DiscoverVerificationResult(
            status="confirmed",
            needs_review=False,
            confidence="high",
            score=max(rule_score, llm_score or 0),
            reason=llm_reason or "llm:belongs_true",
            decision_stage="llm_auto_confirm",
            rule_name_match=bool(name_exact or name_token),
            rule_location_match=location_ok,
            llm_belongs=llm_belongs,
            llm_score=llm_score,
            llm_reason=llm_reason,
            llm_evidence=llm_evidence,
        )

    return DiscoverVerificationResult(
        status="review_required",
        needs_review=True,
        confidence="medium" if (llm_score or 0) >= 50 else "low",
        score=max(rule_score, llm_score or 0),
        reason=llm_reason or "llm:review_required",
        decision_stage="llm_review",
        rule_name_match=bool(name_exact or name_token),
        rule_location_match=location_ok,
        llm_belongs=llm_belongs,
        llm_score=llm_score,
        llm_reason=llm_reason,
        llm_evidence=llm_evidence,
    )


def load_clean_eligible_org_ids() -> set[str]:
    summary_df = load_latest_scrape_clean_summary()
    if summary_df.empty:
        return set()
    status = summary_df["_scrape_clean_status"].astype(str).str.strip().str.lower()
    usable = pd.to_numeric(summary_df["_scrape_clean_usable_chars"], errors="coerce").fillna(0)
    mask = (status == "ok") & (usable > 0)
    return {
        str(value).strip()
        for value in summary_df.loc[mask, "_org_id"].tolist()
        if str(value).strip()
    }
