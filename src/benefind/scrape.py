"""Web scraping: fetch prepared URLs and persist per-page artifacts.

This module consumes URL targets produced by `prepare-scraping` and writes:
- markdown pages under `data/orgs/<_org_id>/pages/`
- URL-level scrape manifest under `data/orgs/<_org_id>/scrape/manifest.csv`
- run metadata under `data/orgs/<_org_id>/scrape/run_meta.json`

Scraping is resumable at URL level: successful URLs are skipped on rerun unless
`refresh_existing=True` is requested.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from robotexclusionrulesparser import RobotExclusionRulesParser

try:
    import trafilatura
except Exception:  # pragma: no cover - optional dependency
    trafilatura = None

try:
    from readability import Document
except Exception:  # pragma: no cover - optional dependency
    Document = None

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - optional dependency
    sync_playwright = None

try:
    import pdfplumber
except Exception:  # pragma: no cover - optional dependency
    pdfplumber = None

from benefind.config import DATA_DIR, Settings

logger = logging.getLogger(__name__)

ACCEPTABLE_EXTRACTION_SCORE = 35

MANIFEST_COLUMNS = [
    "_org_id",
    "_scrape_run_id",
    "_prepared_url_order",
    "_prepared_url",
    "_page_attempt_count",
    "_page_status",
    "_page_failure_reason_code",
    "_page_failure_detail",
    "_http_status",
    "_content_type",
    "_fetch_mode",
    "_extractor_selected",
    "_extractor_score",
    "_extractor_score_static_best",
    "_extractor_score_render_best",
    "_content_quality",
    "_content_quality_reason",
    "_render_trigger_reason",
    "_final_url",
    "_metadata_title",
    "_metadata_description",
    "_metadata_canonical",
    "_metadata_lang",
    "_saved_markdown_path",
    "_saved_at",
]


@dataclass
class ScrapeOrgResult:
    org_dir: Path
    attempted_count: int
    success_count: int
    failed_count: int
    skipped_success_count: int
    failure_reason_counts: dict[str, int] = field(default_factory=dict)
    content_quality_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class PageScrapeResult:
    status: str
    failure_reason_code: str
    failure_detail: str
    http_status: int | None
    content_type: str
    content: str
    extractor_selected: str = ""
    extractor_score: int | None = None
    extractor_score_static_best: int | None = None
    extractor_score_render_best: int | None = None
    content_quality: str = ""
    content_quality_reason: str = ""
    fetch_mode: str = "static"
    render_trigger_reason: str = ""
    final_url: str = ""
    metadata_title: str = ""
    metadata_description: str = ""
    metadata_canonical: str = ""
    metadata_lang: str = ""


@dataclass
class PlaywrightRenderResult:
    status: str
    html: str
    final_url: str
    failure_detail: str


BOILERPLATE_TOKENS = {
    "home",
    "menu",
    "navigation",
    "kontakt",
    "contact",
    "impressum",
    "datenschutz",
    "privacy",
    "cookie",
    "login",
    "newsletter",
    "footer",
    "header",
    "sitemap",
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return slug or "unnamed"


def _org_paths(org_id: str) -> tuple[Path, Path, Path, Path, Path]:
    org_dir = DATA_DIR / "orgs" / org_id
    pages_dir = org_dir / "pages"
    scrape_dir = org_dir / "scrape"
    manifest_path = scrape_dir / "manifest.csv"
    run_meta_path = scrape_dir / "run_meta.json"
    return org_dir, pages_dir, scrape_dir, manifest_path, run_meta_path


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _load_manifest(manifest_path: Path) -> pd.DataFrame:
    if not manifest_path.exists():
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    try:
        df = pd.read_csv(manifest_path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    for column in MANIFEST_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df[MANIFEST_COLUMNS]


def _save_manifest(manifest_df: pd.DataFrame, manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
    manifest_df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(manifest_path)


def _classify_exception(exc: Exception) -> tuple[str, str]:
    if isinstance(exc, httpx.TimeoutException):
        return "network_timeout", str(exc)

    if isinstance(exc, httpx.ConnectError):
        detail = str(exc)
        lowered = detail.lower()
        if "ssl" in lowered or "certificate" in lowered:
            return "tls_error", detail
        return "network_dns_failure", detail

    return "unexpected_exception", f"{type(exc).__name__}: {exc}"


def _extract_markdown(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    markdown = md(str(soup), strip=["img"])
    markdown = re.sub(r"\n{3,}", "\n\n", markdown)
    return markdown.strip()


def _extract_metadata(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    if soup.title and soup.title.string:
        title = str(soup.title.string).strip()

    description = ""
    meta_description = soup.find("meta", attrs={"name": "description"})
    if meta_description and meta_description.get("content"):
        description = str(meta_description.get("content") or "").strip()

    canonical = ""
    canonical_tag = soup.find("link", attrs={"rel": "canonical"})
    if canonical_tag and canonical_tag.get("href"):
        canonical = str(canonical_tag.get("href") or "").strip()

    lang = ""
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        lang = str(html_tag.get("lang") or "").strip()

    return {
        "title": title,
        "description": description,
        "canonical": canonical,
        "lang": lang,
    }


def _extract_with_markdownify(html: str) -> str:
    return _extract_markdown(html)


def _extract_with_trafilatura(html: str) -> str:
    if trafilatura is None:
        return ""
    try:
        extracted = trafilatura.extract(
            html,
            output_format="markdown",
            include_links=True,
            include_formatting=True,
        )
    except Exception:
        return ""
    return str(extracted or "").strip()


def _extract_with_readability(html: str) -> str:
    if Document is None:
        return ""
    try:
        doc = Document(html)
        summary_html = doc.summary() or ""
        if not summary_html:
            return ""
        markdown = md(summary_html, strip=["img"])
    except Exception:
        return ""
    markdown = re.sub(r"\n{3,}", "\n\n", markdown)
    return markdown.strip()


def _paragraph_count(content: str) -> int:
    paragraphs = [chunk.strip() for chunk in re.split(r"\n\s*\n", content) if chunk.strip()]
    return len(paragraphs)


def _sentence_count(content: str) -> int:
    return len(re.findall(r"[.!?]+", content))


def _heading_count(content: str) -> int:
    return len(re.findall(r"(?m)^\s{0,3}#{1,6}\s+\S", content))


def _link_density(content: str) -> float:
    matches = re.findall(r"\[([^\]]+)\]\([^\)]+\)", content)
    link_chars = sum(len(str(match or "").strip()) for match in matches)
    return link_chars / max(len(content), 1)


def _duplicate_line_ratio(content: str) -> float:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if not lines:
        return 0.0
    duplicates = len(lines) - len(set(lines))
    return duplicates / len(lines)


def _boilerplate_token_ratio(content: str) -> float:
    tokens = re.findall(r"[a-z0-9]+", content.lower())
    if not tokens:
        return 0.0
    boilerplate_count = sum(1 for token in tokens if token in BOILERPLATE_TOKENS)
    return boilerplate_count / len(tokens)


def _length_score(char_count: int) -> int:
    if char_count >= 2000:
        return 35
    if char_count >= 1200:
        return 25
    if char_count >= 600:
        return 15
    if char_count >= 300:
        return 5
    return 0


def _score_extracted_content(content: str, metadata: dict[str, str]) -> int:
    text = str(content or "").strip()
    if not text:
        return 0

    score = 0

    score += _length_score(len(text))

    paragraphs = _paragraph_count(text)
    if paragraphs >= 5:
        score += 10
    elif paragraphs >= 2:
        score += 5

    if _heading_count(text) >= 1:
        score += 10

    if _sentence_count(text) >= 12:
        score += 5

    if str(metadata.get("title", "") or "").strip():
        score += 5
    if str(metadata.get("description", "") or "").strip():
        score += 5

    if _link_density(text) > 0.45:
        score -= 20
    if _duplicate_line_ratio(text) > 0.30:
        score -= 10
    if _boilerplate_token_ratio(text) > 0.35:
        score -= 10

    return max(0, min(100, int(score)))


def _quality_from_score(
    score: int | None,
    *,
    base_reason: str = "",
) -> tuple[str, str]:
    if score is None:
        return "", str(base_reason or "").strip()

    if int(score) >= ACCEPTABLE_EXTRACTION_SCORE:
        return "ok", ""

    reason = str(base_reason or "").strip()
    threshold_reason = f"score_below_threshold:{int(score)}<{ACCEPTABLE_EXTRACTION_SCORE}"
    if reason:
        return "low", f"{reason};{threshold_reason}"
    return "low", threshold_reason


def _minimal_content_from_metadata(
    metadata: dict[str, str],
    *,
    final_url: str,
    content_type: str,
    note: str,
) -> str:
    lines = ["# Low-content page"]
    if final_url:
        lines.append(f"- URL: {final_url}")
    if content_type:
        lines.append(f"- Content-Type: {content_type}")
    if note:
        lines.append(f"- Note: {note}")

    title = str(metadata.get("title", "") or "").strip()
    description = str(metadata.get("description", "") or "").strip()
    canonical = str(metadata.get("canonical", "") or "").strip()
    lang = str(metadata.get("lang", "") or "").strip()

    if title:
        lines.append(f"- Title: {title}")
    if description:
        lines.append(f"- Description: {description}")
    if canonical:
        lines.append(f"- Canonical: {canonical}")
    if lang:
        lines.append(f"- Language: {lang}")

    return "\n".join(lines).strip()


def _extract_pdf_text(content: bytes) -> str:
    if pdfplumber is None:
        return ""

    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            parts: list[str] = []
            for page in pdf.pages:
                text = str(page.extract_text() or "").strip()
                if text:
                    parts.append(text)
    except Exception:
        return ""

    if not parts:
        return ""

    body = "\n\n".join(parts).strip()
    if not body:
        return ""

    return f"# PDF content\n\n{body}".strip()


def _extract_non_html_content(response: httpx.Response) -> tuple[str, str, str]:
    content_type = str(response.headers.get("content-type", "") or "").lower()

    if "application/pdf" in content_type:
        extracted = _extract_pdf_text(response.content)
        if not extracted:
            return "", "", "pdf_text_extraction_failed"
        return extracted, "pdfplumber", "non_html:application_pdf"

    text_like_types = (
        "text/plain",
        "text/markdown",
        "text/xml",
        "application/json",
        "application/xml",
        "application/xhtml+xml",
    )
    if any(token in content_type for token in text_like_types):
        text = str(response.text or "").strip()
        if not text:
            return "", "", "text_like_content_empty"
        return text, "raw_text", f"non_html:{content_type.split(';', 1)[0]}"

    return "", "", f"non_html_unsupported:{content_type or 'unknown'}"


def _available_extractors() -> dict[str, Callable[[str], str]]:
    extractors: dict[str, Callable[[str], str]] = {"markdownify": _extract_with_markdownify}
    if trafilatura is not None:
        extractors["trafilatura"] = _extract_with_trafilatura
    if Document is not None:
        extractors["readability-lxml"] = _extract_with_readability
    return extractors


def _run_extractor(name: str, html: str) -> str:
    extractors = _available_extractors()
    func = extractors.get(name)
    if func is None:
        return ""
    return str(func(html) or "").strip()


def _select_best_extractor(
    html: str,
    metadata: dict[str, str],
    *,
    preferred_extractor: str | None,
) -> tuple[str, str, int]:
    extractors = _available_extractors()

    if preferred_extractor and preferred_extractor in extractors:
        preferred_content = _run_extractor(preferred_extractor, html)
        preferred_score = _score_extracted_content(preferred_content, metadata)
        if preferred_score >= ACCEPTABLE_EXTRACTION_SCORE:
            return preferred_extractor, preferred_content, preferred_score

    best_name = ""
    best_content = ""
    best_score = -1
    for name in extractors:
        content = _run_extractor(name, html)
        score = _score_extracted_content(content, metadata)
        if score > best_score:
            best_name = name
            best_content = content
            best_score = score

    if best_score < 0:
        return "", "", 0
    return best_name, best_content, best_score


def _render_page_playwright(
    url: str,
    *,
    user_agent: str,
    timeout_seconds: int,
    headless: bool = True,
) -> PlaywrightRenderResult:
    if sync_playwright is None:
        return PlaywrightRenderResult(
            status="failed",
            html="",
            final_url="",
            failure_detail="playwright_not_installed",
        )

    timeout_ms = max(int(timeout_seconds * 1000), 1000)

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=headless)
            try:
                context = browser.new_context(user_agent=user_agent or None)
                try:
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    try:
                        page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 5000))
                    except Exception:
                        pass

                    html = str(page.content() or "").strip()
                    final_url = str(page.url or "").strip()
                finally:
                    context.close()
            finally:
                browser.close()
    except Exception as exc:
        return PlaywrightRenderResult(
            status="failed",
            html="",
            final_url="",
            failure_detail=f"{type(exc).__name__}: {exc}",
        )

    if not html:
        return PlaywrightRenderResult(
            status="failed",
            html="",
            final_url=final_url,
            failure_detail="empty_rendered_html",
        )

    return PlaywrightRenderResult(
        status="success",
        html=html,
        final_url=final_url,
        failure_detail="",
    )


def _detect_render_markers(static_html: str, extracted_content: str) -> list[str]:
    html = str(static_html or "")
    text = str(extracted_content or "").strip()
    markers: list[str] = []

    checks = [
        (r'id\s*=\s*["\']__next["\']', "id___next"),
        (r"window\.__NUXT__", "window_nuxt"),
        (r"data-reactroot", "data_reactroot"),
        (r"hydrateRoot\s*\(", "hydrate_root"),
        (r"createRoot\s*\(", "create_root"),
    ]
    for pattern, label in checks:
        if re.search(pattern, html, flags=re.IGNORECASE):
            markers.append(label)

    noscript_text = ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        noscript_chunks = [
            str(node.get_text(" ", strip=True) or "") for node in soup.find_all("noscript")
        ]
        noscript_text = " ".join(chunk for chunk in noscript_chunks if chunk).lower()
        script_count = len(soup.find_all("script"))
        visible_text_length = len(soup.get_text(" ", strip=True))
    except Exception:
        script_count = 0
        visible_text_length = 0

    if (
        "javascript" in noscript_text
        and ("required" in noscript_text or "enable" in noscript_text)
    ):
        markers.append("noscript_js_required")

    if script_count >= 10 and visible_text_length < 500 and len(text) < 200:
        markers.append("script_heavy_low_text")

    deduped: list[str] = []
    for marker in markers:
        if marker not in deduped:
            deduped.append(marker)
    return deduped


def _build_render_trigger_reason(static_html: str, extracted_content: str) -> str:
    markers = _detect_render_markers(static_html, extracted_content)
    if not markers:
        return "poor_static_extraction"
    marker_summary = ",".join(markers)
    return f"poor_static_extraction:markers={marker_summary}"


def _is_playwright_infrastructure_failure(detail: str) -> bool:
    text = str(detail or "").lower()
    if not text:
        return False

    indicators = [
        "playwright_not_installed",
        "browser_type.launch",
        "executable doesn't exist",
        "please run the following command to download new browsers",
        "failed to launch",
    ]
    return any(indicator in text for indicator in indicators)


def _playwright_fallback_from_http_failure(
    *,
    url: str,
    user_agent: str,
    timeout_seconds: int,
    failure_reason_code: str,
    failure_detail: str,
    http_status: int | None,
    content_type: str,
    final_url: str,
    playwright_headless: bool,
) -> PageScrapeResult:
    render_result = _render_page_playwright(
        url,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        headless=playwright_headless,
    )
    if render_result.status != "success":
        detail_parts = [str(failure_detail or "").strip(), render_result.failure_detail]
        merged_detail = "; ".join(part for part in detail_parts if part)
        return PageScrapeResult(
            status="failed",
            failure_reason_code=failure_reason_code,
            failure_detail=merged_detail,
            http_status=http_status,
            content_type=content_type,
            content="",
            extractor_selected="",
            extractor_score=0,
            extractor_score_static_best=0,
            extractor_score_render_best=0,
            fetch_mode="playwright",
            render_trigger_reason=f"http_failure_fallback:{failure_reason_code}",
            final_url=final_url,
        )

    rendered_metadata = _extract_metadata(render_result.html)
    extractor_name, extracted_content, extractor_score = _select_best_extractor(
        render_result.html,
        rendered_metadata,
        preferred_extractor=None,
    )

    selected_content = extracted_content
    if not selected_content:
        selected_content = _minimal_content_from_metadata(
            rendered_metadata,
            final_url=render_result.final_url or final_url or url,
            content_type=content_type,
            note=f"http_failure_fallback:{failure_reason_code}",
        )

    quality, quality_reason = _quality_from_score(
        extractor_score,
        base_reason=f"http_failure_fallback:{failure_reason_code}",
    )
    return PageScrapeResult(
        status="success",
        failure_reason_code="",
        failure_detail="",
        http_status=http_status,
        content_type=content_type,
        content=selected_content,
        extractor_selected=extractor_name,
        extractor_score=extractor_score,
        extractor_score_static_best=0,
        extractor_score_render_best=extractor_score,
        content_quality=quality,
        content_quality_reason=quality_reason,
        fetch_mode="playwright",
        render_trigger_reason=f"http_failure_fallback:{failure_reason_code}",
        final_url=render_result.final_url or final_url,
        metadata_title=rendered_metadata.get("title", ""),
        metadata_description=rendered_metadata.get("description", ""),
        metadata_canonical=rendered_metadata.get("canonical", ""),
        metadata_lang=rendered_metadata.get("lang", ""),
    )


def _scrape_page_static(
    client: httpx.Client,
    url: str,
    *,
    preferred_extractor: str | None = None,
    playwright_headless: bool = True,
) -> PageScrapeResult:
    timeout_seconds = 30
    try:
        read_timeout = getattr(client.timeout, "read", None)
        if read_timeout is not None:
            timeout_seconds = int(read_timeout)
    except Exception:
        timeout_seconds = 30

    try:
        response = client.get(url)
    except Exception as exc:
        code, detail = _classify_exception(exc)
        return _playwright_fallback_from_http_failure(
            url=url,
            user_agent=str(client.headers.get("User-Agent", "") or ""),
            timeout_seconds=timeout_seconds,
            failure_reason_code=code,
            failure_detail=detail,
            http_status=None,
            content_type="",
            final_url=url,
            playwright_headless=playwright_headless,
        )

    status_code = int(response.status_code)
    content_type = str(response.headers.get("content-type", "") or "")
    response_final_url = str(response.url or "").strip()

    if status_code == 401:
        return _playwright_fallback_from_http_failure(
            url=url,
            user_agent=str(client.headers.get("User-Agent", "") or ""),
            timeout_seconds=timeout_seconds,
            failure_reason_code="http_401",
            failure_detail="",
            http_status=status_code,
            content_type=content_type,
            final_url=response_final_url,
            playwright_headless=playwright_headless,
        )

    if status_code == 403:
        return _playwright_fallback_from_http_failure(
            url=url,
            user_agent=str(client.headers.get("User-Agent", "") or ""),
            timeout_seconds=timeout_seconds,
            failure_reason_code="http_403",
            failure_detail="",
            http_status=status_code,
            content_type=content_type,
            final_url=response_final_url,
            playwright_headless=playwright_headless,
        )

    if status_code >= 500:
        return _playwright_fallback_from_http_failure(
            url=url,
            user_agent=str(client.headers.get("User-Agent", "") or ""),
            timeout_seconds=timeout_seconds,
            failure_reason_code="http_5xx",
            failure_detail="",
            http_status=status_code,
            content_type=content_type,
            final_url=response_final_url,
            playwright_headless=playwright_headless,
        )

    if status_code != 200:
        return _playwright_fallback_from_http_failure(
            url=url,
            user_agent=str(client.headers.get("User-Agent", "") or ""),
            timeout_seconds=timeout_seconds,
            failure_reason_code="http_other",
            failure_detail=f"http_{status_code}",
            http_status=status_code,
            content_type=content_type,
            final_url=response_final_url,
            playwright_headless=playwright_headless,
        )

    if "text/html" not in content_type.lower():
        non_html_content, non_html_extractor, non_html_reason = _extract_non_html_content(response)
        if non_html_content:
            score = _score_extracted_content(non_html_content, {})
            quality, quality_reason = _quality_from_score(score, base_reason=non_html_reason)
            return PageScrapeResult(
                status="success",
                failure_reason_code="",
                failure_detail="",
                http_status=status_code,
                content_type=content_type,
                content=non_html_content,
                extractor_selected=non_html_extractor,
                extractor_score=score,
                extractor_score_static_best=score,
                extractor_score_render_best=None,
                content_quality=quality,
                content_quality_reason=quality_reason,
                fetch_mode="static",
                render_trigger_reason=non_html_reason,
                final_url=response_final_url,
            )

        return PageScrapeResult(
            status="failed",
            failure_reason_code="non_html_response",
            failure_detail=non_html_reason,
            http_status=status_code,
            content_type=content_type,
            content="",
            extractor_selected="",
            extractor_score=0,
            extractor_score_static_best=0,
            extractor_score_render_best=None,
            final_url=response_final_url,
        )

    metadata = _extract_metadata(response.text)
    extractor_name, extracted_content, extractor_score = _select_best_extractor(
        response.text,
        metadata,
        preferred_extractor=preferred_extractor,
    )

    if extractor_score < ACCEPTABLE_EXTRACTION_SCORE:
        render_trigger_reason = _build_render_trigger_reason(response.text, extracted_content)
        if ":markers=" in render_trigger_reason:
            logger.debug(
                "Playwright escalation for %s due to static extraction score=%d (%s)",
                url,
                extractor_score,
                render_trigger_reason,
            )

        render_result = _render_page_playwright(
            url,
            user_agent=str(client.headers.get("User-Agent", "") or ""),
            timeout_seconds=timeout_seconds,
            headless=playwright_headless,
        )

        if render_result.status != "success":
            if extracted_content and _is_playwright_infrastructure_failure(
                render_result.failure_detail
            ):
                fallback_reason = (
                    f"{render_trigger_reason}:render_fallback={render_result.failure_detail}"
                )
                logger.warning(
                    "Playwright unavailable for %s; keeping static extraction as degraded success",
                    url,
                )
                return PageScrapeResult(
                    status="success",
                    failure_reason_code="",
                    failure_detail="",
                    http_status=status_code,
                    content_type=content_type,
                    content=extracted_content,
                    extractor_selected=extractor_name,
                    extractor_score=extractor_score,
                    extractor_score_static_best=extractor_score,
                    extractor_score_render_best=None,
                    content_quality="low",
                    content_quality_reason=(
                        f"low_score_static:{extractor_score};"
                        f"render_unavailable:{render_result.failure_detail}"
                    ),
                    fetch_mode="static",
                    render_trigger_reason=fallback_reason,
                    final_url=response_final_url,
                    metadata_title=metadata.get("title", ""),
                    metadata_description=metadata.get("description", ""),
                    metadata_canonical=metadata.get("canonical", ""),
                    metadata_lang=metadata.get("lang", ""),
                )

            if extracted_content:
                fallback_reason = (
                    f"{render_trigger_reason}:render_fallback={render_result.failure_detail}"
                )
                logger.warning(
                    (
                        "Playwright render failed for %s; "
                        "keeping static extraction as low-quality success"
                    ),
                    url,
                )
                return PageScrapeResult(
                    status="success",
                    failure_reason_code="",
                    failure_detail="",
                    http_status=status_code,
                    content_type=content_type,
                    content=extracted_content,
                    extractor_selected=extractor_name,
                    extractor_score=extractor_score,
                    extractor_score_static_best=extractor_score,
                    extractor_score_render_best=None,
                    content_quality="low",
                    content_quality_reason=(
                        f"low_score_static:{extractor_score};"
                        f"render_failed:{render_result.failure_detail}"
                    ),
                    fetch_mode="static",
                    render_trigger_reason=fallback_reason,
                    final_url=response_final_url,
                    metadata_title=metadata.get("title", ""),
                    metadata_description=metadata.get("description", ""),
                    metadata_canonical=metadata.get("canonical", ""),
                    metadata_lang=metadata.get("lang", ""),
                )

            return PageScrapeResult(
                status="failed",
                failure_reason_code="playwright_render_failed",
                failure_detail=render_result.failure_detail,
                http_status=status_code,
                content_type=content_type,
                content=extracted_content,
                extractor_selected=extractor_name,
                extractor_score=extractor_score,
                extractor_score_static_best=extractor_score,
                extractor_score_render_best=0,
                fetch_mode="playwright",
                render_trigger_reason=render_trigger_reason,
                final_url=render_result.final_url or response_final_url,
                metadata_title=metadata.get("title", ""),
                metadata_description=metadata.get("description", ""),
                metadata_canonical=metadata.get("canonical", ""),
                metadata_lang=metadata.get("lang", ""),
            )

        rendered_metadata = _extract_metadata(render_result.html)
        render_extractor_name, render_content, render_score = _select_best_extractor(
            render_result.html,
            rendered_metadata,
            preferred_extractor=None,
        )

        use_render = render_score > extractor_score
        selected_content = render_content if use_render else extracted_content
        selected_extractor = render_extractor_name if use_render else extractor_name
        selected_score = render_score if use_render else extractor_score
        selected_metadata = rendered_metadata if use_render else metadata
        selected_final_url = (
            render_result.final_url or response_final_url if use_render else response_final_url
        )
        selected_fetch_mode = "playwright" if use_render else "static"

        if not selected_content:
            selected_content = _minimal_content_from_metadata(
                selected_metadata,
                final_url=selected_final_url,
                content_type=content_type,
                note="extractor_empty_after_render",
            )

        quality, quality_reason = _quality_from_score(
            selected_score,
            base_reason=f"low_score_after_render:selected={selected_score}",
        )

        return PageScrapeResult(
            status="success",
            failure_reason_code="",
            failure_detail="",
            http_status=status_code,
            content_type=content_type,
            content=selected_content,
            extractor_selected=selected_extractor,
            extractor_score=selected_score,
            extractor_score_static_best=extractor_score,
            extractor_score_render_best=render_score,
            content_quality=quality,
            content_quality_reason=quality_reason,
            fetch_mode=selected_fetch_mode,
            render_trigger_reason=render_trigger_reason,
            final_url=selected_final_url,
            metadata_title=selected_metadata.get("title", ""),
            metadata_description=selected_metadata.get("description", ""),
            metadata_canonical=selected_metadata.get("canonical", ""),
            metadata_lang=selected_metadata.get("lang", ""),
        )

    quality, quality_reason = _quality_from_score(extractor_score)
    return PageScrapeResult(
        status="success",
        failure_reason_code="",
        failure_detail="",
        http_status=status_code,
        content_type=content_type,
        content=extracted_content,
        extractor_selected=extractor_name,
        extractor_score=extractor_score,
        extractor_score_static_best=extractor_score,
        extractor_score_render_best=None,
        content_quality=quality,
        content_quality_reason=quality_reason,
        fetch_mode="static",
        render_trigger_reason="",
        final_url=response_final_url,
        metadata_title=metadata.get("title", ""),
        metadata_description=metadata.get("description", ""),
        metadata_canonical=metadata.get("canonical", ""),
        metadata_lang=metadata.get("lang", ""),
    )


def _build_page_filename(order: int, url: str) -> str:
    parsed = urlparse(url)
    base = f"{parsed.netloc}-{parsed.path or '/'}"
    slug = _slugify(base)
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    return f"{order:03d}-{slug}-{digest}.md"


def _append_manifest_row(manifest_df: pd.DataFrame, row: dict) -> pd.DataFrame:
    append_row = {column: row.get(column, "") for column in MANIFEST_COLUMNS}
    return pd.concat([manifest_df, pd.DataFrame([append_row])], ignore_index=True)


def _existing_success_row(manifest_df: pd.DataFrame, url: str) -> pd.Series | None:
    matches = manifest_df[manifest_df["_prepared_url"].astype(str).str.strip() == url]
    if matches.empty:
        return None

    success_rows = matches[
        matches["_page_status"].astype(str).str.strip().str.lower() == "success"
    ]
    if success_rows.empty:
        return None
    return success_rows.iloc[-1]


def _attempt_count(manifest_df: pd.DataFrame, url: str) -> int:
    matches = manifest_df[manifest_df["_prepared_url"].astype(str).str.strip() == url]
    if matches.empty:
        return 0

    values = pd.to_numeric(matches["_page_attempt_count"], errors="coerce").fillna(0)
    if values.empty:
        return 0
    try:
        return int(values.max())
    except (TypeError, ValueError):
        return 0


def _org_calibrated_extractor(manifest_df: pd.DataFrame) -> str | None:
    success_rows = manifest_df[
        manifest_df["_page_status"].astype(str).str.strip().str.lower() == "success"
    ]
    if success_rows.empty:
        return None

    latest = success_rows.iloc[-1]
    extractor = str(latest.get("_extractor_selected", "") or "").strip()
    return extractor or None


def scrape_organization_urls(
    org_id: str,
    org_name: str,
    urls: list[str],
    settings: Settings,
    *,
    refresh_existing: bool = False,
    run_id: str | None = None,
    playwright_headless: bool = True,
) -> ScrapeOrgResult:
    """Scrape prepared URLs for one organization and maintain URL-level manifest."""
    org_id_norm = str(org_id or "").strip()
    if not org_id_norm:
        raise ValueError("org_id is required")

    org_dir, pages_dir, scrape_dir, manifest_path, run_meta_path = _org_paths(org_id_norm)
    pages_dir.mkdir(parents=True, exist_ok=True)
    scrape_dir.mkdir(parents=True, exist_ok=True)

    effective_run_id = run_id or _now_iso()
    user_agent = settings.scraping.user_agent
    timeout = settings.scraping.timeout_seconds
    delay = settings.scraping.request_delay_seconds
    max_pages = settings.scraping.max_pages_per_org

    manifest_df = _load_manifest(manifest_path)
    org_extractor = _org_calibrated_extractor(manifest_df)

    attempted_count = 0
    success_count = 0
    failed_count = 0
    skipped_success_count = 0
    failure_reason_counts: dict[str, int] = {}
    content_quality_counts: dict[str, int] = {}

    started_at = _now_iso()
    headers = {"User-Agent": user_agent}

    with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
        for order, raw_url in enumerate(urls[:max_pages], start=1):
            url = str(raw_url or "").strip()
            if not url:
                continue

            existing_success = _existing_success_row(manifest_df, url)
            if not refresh_existing and existing_success is not None:
                skipped_success_count += 1
                manifest_row = {
                    "_org_id": org_id_norm,
                    "_scrape_run_id": effective_run_id,
                    "_prepared_url_order": order,
                    "_prepared_url": url,
                    "_page_attempt_count": _attempt_count(manifest_df, url),
                    "_page_status": "skipped",
                    "_page_failure_reason_code": "",
                    "_page_failure_detail": "already_success",
                    "_http_status": existing_success.get("_http_status", ""),
                    "_content_type": existing_success.get("_content_type", ""),
                    "_fetch_mode": existing_success.get("_fetch_mode", "static"),
                    "_extractor_selected": existing_success.get("_extractor_selected", ""),
                    "_extractor_score": existing_success.get("_extractor_score", ""),
                    "_extractor_score_static_best": existing_success.get(
                        "_extractor_score_static_best", ""
                    ),
                    "_extractor_score_render_best": existing_success.get(
                        "_extractor_score_render_best", ""
                    ),
                    "_content_quality": existing_success.get("_content_quality", ""),
                    "_content_quality_reason": existing_success.get(
                        "_content_quality_reason", ""
                    ),
                    "_render_trigger_reason": existing_success.get("_render_trigger_reason", ""),
                    "_final_url": existing_success.get("_final_url", ""),
                    "_metadata_title": existing_success.get("_metadata_title", ""),
                    "_metadata_description": existing_success.get("_metadata_description", ""),
                    "_metadata_canonical": existing_success.get("_metadata_canonical", ""),
                    "_metadata_lang": existing_success.get("_metadata_lang", ""),
                    "_saved_markdown_path": existing_success.get("_saved_markdown_path", ""),
                    "_saved_at": _now_iso(),
                }
                manifest_df = _append_manifest_row(manifest_df, manifest_row)
                _save_manifest(manifest_df, manifest_path)
                continue

            attempted_count += 1
            if playwright_headless:
                page_result = _scrape_page_static(
                    client,
                    url,
                    preferred_extractor=org_extractor,
                )
            else:
                page_result = _scrape_page_static(
                    client,
                    url,
                    preferred_extractor=org_extractor,
                    playwright_headless=False,
                )
            attempt_count = _attempt_count(manifest_df, url) + 1
            saved_path = ""

            if (
                page_result.status == "success"
                and not org_extractor
                and page_result.extractor_selected
            ):
                org_extractor = page_result.extractor_selected

            if page_result.status == "success":
                filename = _build_page_filename(order, url)
                page_path = pages_dir / filename
                page_path.write_text(page_result.content, encoding="utf-8")
                saved_path = str(page_path)
                success_count += 1
                quality = str(page_result.content_quality or "").strip() or "unknown"
                content_quality_counts[quality] = content_quality_counts.get(quality, 0) + 1
            else:
                failed_count += 1
                reason = str(page_result.failure_reason_code or "").strip() or "unknown_failure"
                failure_reason_counts[reason] = failure_reason_counts.get(reason, 0) + 1

            manifest_row = {
                "_org_id": org_id_norm,
                "_scrape_run_id": effective_run_id,
                "_prepared_url_order": order,
                "_prepared_url": url,
                "_page_attempt_count": attempt_count,
                "_page_status": page_result.status,
                "_page_failure_reason_code": page_result.failure_reason_code,
                "_page_failure_detail": page_result.failure_detail,
                "_http_status": "" if page_result.http_status is None else page_result.http_status,
                "_content_type": page_result.content_type,
                "_fetch_mode": page_result.fetch_mode,
                "_extractor_selected": page_result.extractor_selected,
                "_extractor_score": ""
                if page_result.extractor_score is None
                else page_result.extractor_score,
                "_extractor_score_static_best": ""
                if page_result.extractor_score_static_best is None
                else page_result.extractor_score_static_best,
                "_extractor_score_render_best": ""
                if page_result.extractor_score_render_best is None
                else page_result.extractor_score_render_best,
                "_content_quality": page_result.content_quality,
                "_content_quality_reason": page_result.content_quality_reason,
                "_render_trigger_reason": page_result.render_trigger_reason,
                "_final_url": page_result.final_url,
                "_metadata_title": page_result.metadata_title,
                "_metadata_description": page_result.metadata_description,
                "_metadata_canonical": page_result.metadata_canonical,
                "_metadata_lang": page_result.metadata_lang,
                "_saved_markdown_path": saved_path,
                "_saved_at": _now_iso(),
            }
            manifest_df = _append_manifest_row(manifest_df, manifest_row)
            _save_manifest(manifest_df, manifest_path)

            if delay > 0:
                time.sleep(delay)

    finished_at = _now_iso()
    run_meta = {
        "_org_id": org_id_norm,
        "_org_name": org_name,
        "_scrape_run_id": effective_run_id,
        "_refresh_existing": bool(refresh_existing),
        "_org_extractor_selected": org_extractor or "",
        "_started_at": started_at,
        "_finished_at": finished_at,
        "_max_pages": int(max_pages),
        "_attempted_urls": attempted_count,
        "_successful_urls": success_count,
        "_failed_urls": failed_count,
        "_skipped_success_urls": skipped_success_count,
        "_failure_reason_counts": {
            key: failure_reason_counts[key] for key in sorted(failure_reason_counts)
        },
        "_content_quality_counts": {
            key: content_quality_counts[key] for key in sorted(content_quality_counts)
        },
    }
    _write_json_atomic(run_meta_path, run_meta)

    logger.info(
        "Scrape org %s (%s): attempted=%d success=%d failed=%d skipped=%d",
        org_name,
        org_id_norm,
        attempted_count,
        success_count,
        failed_count,
        skipped_success_count,
    )

    return ScrapeOrgResult(
        org_dir=org_dir,
        attempted_count=attempted_count,
        success_count=success_count,
        failed_count=failed_count,
        skipped_success_count=skipped_success_count,
        failure_reason_counts={
            key: failure_reason_counts[key] for key in sorted(failure_reason_counts)
        },
        content_quality_counts={
            key: content_quality_counts[key] for key in sorted(content_quality_counts)
        },
    )


def check_robots_txt(
    base_url: str,
    user_agent: str,
    timeout: int = 10,
) -> RobotExclusionRulesParser | None:
    """Legacy helper kept for compatibility with older call sites."""
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(robots_url)
            if response.status_code == 200:
                parser = RobotExclusionRulesParser()
                parser.parse(response.text)
                return parser
            return None
    except Exception:
        return None


def is_allowed(
    url: str,
    robots: RobotExclusionRulesParser | None,
    user_agent: str,
) -> bool:
    """Legacy helper kept for compatibility with older call sites."""
    if robots is None:
        return True
    return robots.is_allowed(user_agent, url)
