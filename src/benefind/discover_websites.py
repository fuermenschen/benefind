"""Website discovery: find the website URL for each organization.

Uses the Brave Search API to find the official website for each organization
in the filtered list.
"""

from __future__ import annotations

import html
import json
import logging
import os
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from benefind.config import Settings, render_prompt_template
from benefind.external_api import (
    ExternalApiAccessError,
    classify_http_access_error,
    classify_openai_access_error,
)

# TODO: check if async would make sense here

logger = logging.getLogger(__name__)

# Domains that are aggregators/registries, not the org's own website
DEPRIORITIZED_DOMAINS = {
    "zefix.ch",
    "moneyhouse.ch",
    "uid.admin.ch",
    "shabex.ch",
    "help.ch",
    "search.ch",
    "local.ch",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "wikipedia.org",
    "wikidata.org",
    "dnb.com",
    "northdata.com",
    "northdata.de",
    "stiftungschweiz.ch",
    "lixt.ch",
    "fundraiso.ch",
    "zhwin.ch",
}

NEAR_ROOT_PATH_KEYWORDS = {
    "about",
    "ueber-uns",
    "about-us",
    "portrait",
    "kontakt",
    "contact",
    "home",
}


@dataclass
class WebsiteResult:
    """Result of searching for an organization's website."""

    org_name: str
    url: str | None
    confidence: str  # "high", "medium", "low", "none"
    source: str  # how the URL was found
    needs_review: bool
    score: int | None = None
    score_gap: int | None = None
    llm_url: str | None = None
    llm_agrees: bool | None = None
    decision_stage: str = "none"
    llm_prompt: str | None = None
    llm_response: str | None = None


@dataclass
class WebsiteCandidate:
    """Scored candidate result for debug/inspection output."""

    score: int
    url: str
    title: str
    description: str


@dataclass
class _ScoredPageResult:
    score: int
    url: str
    title: str
    description: str
    domain: str


class _SearchRateLimiter:
    """Thread-safe global rate limiter for search API calls."""

    def __init__(self, requests_per_second: float) -> None:
        self._interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def wait_for_slot(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                sleep_seconds = self._next_allowed - now
                self._next_allowed += self._interval
            else:
                sleep_seconds = 0.0
                self._next_allowed = now + self._interval
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def _is_deprioritized(url: str) -> bool:
    """Check if a URL belongs to a deprioritized aggregator domain."""
    try:
        domain = urlparse(url).netloc.lower()
        # Strip www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return any(domain == d or domain.endswith("." + d) for d in DEPRIORITIZED_DOMAINS)
    except Exception:
        return False


def _score_result(url: str, title: str, org_name: str) -> int:
    """Score a search result by how likely it is the org's official website.

    Higher score = more likely to be the right site.
    """
    score = 0
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.lower()
    path = (parsed_url.path or "").strip("/")

    # Prefer .ch domains
    if domain.endswith(".ch"):
        score += 10

    # Penalize aggregator sites
    if _is_deprioritized(url):
        score -= 50

    # Bonus if org name appears in the domain
    org_words = [w.lower() for w in org_name.split() if len(w) > 3]
    for word in org_words:
        if word in domain:
            score += 15

    # Bonus if title contains the org name
    title_lower = title.lower()
    for word in org_words:
        if word in title_lower:
            score += 5

    # Prefer root / near-root pages over deep subpages.
    if not path:
        score += 10
    else:
        depth = len([part for part in path.split("/") if part])
        if depth == 1:
            score += 4
        elif depth >= 2:
            score -= 6

    return score


def _normalize_domain(url: str) -> str:
    domain = urlparse(url).netloc.lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def _sanitize_search_text(value: str) -> str:
    """Sanitize organization/location text before building search queries."""
    text = (value or "").strip()
    if not text:
        return ""

    text = re.sub(r"[\"'`“”„«»‚‘’]", "", text)
    text = re.sub(r"[^\w\s-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _registrable_domain(domain: str) -> str:
    parts = [part for part in domain.split(".") if part]
    if len(parts) <= 2:
        return domain

    multi_part_suffixes = {
        "co.uk",
        "org.uk",
        "gov.uk",
        "ac.uk",
        "com.au",
        "org.au",
        "net.au",
        "co.jp",
        "co.nz",
    }
    suffix2 = ".".join(parts[-2:])
    if suffix2 in multi_part_suffixes and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _canonical_page_priority(url: str) -> int:
    path = (urlparse(url).path or "").lower().strip("/")
    if not path:
        return 100

    preferred = list(NEAR_ROOT_PATH_KEYWORDS)
    if any(part in path for part in preferred):
        return 80

    less_preferred = ["spenden", "donate", "news", "blog", "events", "event"]
    if any(part in path for part in less_preferred):
        return 30

    return 60


def _pick_best_domain(scored_pages: list[_ScoredPageResult]) -> tuple[int, str, _ScoredPageResult]:
    by_domain: dict[str, list[_ScoredPageResult]] = {}
    for page in scored_pages:
        by_domain.setdefault(page.domain, []).append(page)

    best_domain = ""
    best_domain_score = -10_000
    best_page: _ScoredPageResult | None = None

    for domain, pages in by_domain.items():
        pages_sorted = sorted(
            pages,
            key=lambda page: (_canonical_page_priority(page.url), page.score),
            reverse=True,
        )
        best_page_for_domain = pages_sorted[0]
        max_page_score = max(page.score for page in pages)
        repeat_bonus = min(6, (len(pages) - 1) * 3)
        root_bonus = (
            3 if any((urlparse(page.url).path or "").strip("/") == "" for page in pages) else 0
        )
        domain_score = max_page_score + repeat_bonus + root_bonus

        if domain_score > best_domain_score:
            best_domain = domain
            best_domain_score = domain_score
            best_page = best_page_for_domain

    if best_page is None:
        raise ValueError("No scored pages available")

    return best_domain_score, best_domain, best_page


def _build_domain_candidates(
    scored_pages: list[_ScoredPageResult],
) -> list[tuple[int, str, _ScoredPageResult]]:
    by_domain: dict[str, list[_ScoredPageResult]] = {}
    for page in scored_pages:
        by_domain.setdefault(page.domain, []).append(page)

    candidates: list[tuple[int, str, _ScoredPageResult]] = []
    for domain, pages in by_domain.items():
        pages_sorted = sorted(
            pages,
            key=lambda page: (_canonical_page_priority(page.url), page.score),
            reverse=True,
        )
        canonical_page = pages_sorted[0]
        max_page_score = max(page.score for page in pages)
        repeat_bonus = min(6, (len(pages) - 1) * 3)
        root_bonus = (
            3 if any((urlparse(page.url).path or "").strip("/") == "" for page in pages) else 0
        )
        domain_score = max_page_score + repeat_bonus + root_bonus
        candidates.append((domain_score, domain, canonical_page))

    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates


def _score_gap(scored_pages: list[_ScoredPageResult]) -> int | None:
    candidates = _build_domain_candidates(scored_pages)
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0][0]
    return candidates[0][0] - candidates[1][0]


def _score_pages(results: list[dict], org_name: str) -> list[_ScoredPageResult]:
    return [
        _ScoredPageResult(
            score=_score_result(result["url"], result["title"], org_name),
            url=result["url"],
            title=result["title"],
            description=result["description"],
            domain=_registrable_domain(_normalize_domain(result["url"])),
        )
        for result in results
        if result.get("url")
    ]


def _rank_results(
    results: list[dict],
    org_name: str,
) -> tuple[list[_ScoredPageResult], int, str, _ScoredPageResult, int | None] | None:
    scored_pages = _score_pages(results, org_name)
    if not scored_pages:
        return None
    best_score, best_domain, best_page = _pick_best_domain(scored_pages)
    score_gap = _score_gap(scored_pages)
    return scored_pages, best_score, best_domain, best_page, score_gap


def _pages_to_candidates(pages: list[_ScoredPageResult]) -> list[WebsiteCandidate]:
    by_domain: dict[str, list[_ScoredPageResult]] = {}
    for page in pages:
        by_domain.setdefault(page.domain, []).append(page)

    out: list[WebsiteCandidate] = []
    for domain, domain_pages in by_domain.items():
        best_score = max(p.score for p in domain_pages)
        repeat_bonus = min(6, (len(domain_pages) - 1) * 3)
        root_bonus = (
            3 if any((urlparse(p.url).path or "").strip("/") == "" for p in domain_pages) else 0
        )
        domain_score = best_score + repeat_bonus + root_bonus
        canonical_page = sorted(
            domain_pages,
            key=lambda p: (_canonical_page_priority(p.url), p.score),
            reverse=True,
        )[0]
        out.append(
            WebsiteCandidate(
                score=domain_score,
                url=canonical_page.url,
                title=canonical_page.title,
                description=f"domain={domain}; supporting_results={len(domain_pages)}",
            )
        )
    out.sort(key=lambda c: c.score, reverse=True)
    return out


def _fetch_page_title(url: str, timeout: int = 10) -> str | None:
    """Fetch URL and return HTML title.

    Returns:
        - title string (possibly empty) when the URL is reachable
        - None when the URL is unreachable or non-HTML
    """
    headers = {
        "User-Agent": "benefind/0.1 (nonprofit research; https://hfm-winti.ch)",
    }
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
            response = client.get(url)
            if response.status_code >= 400:
                return None
            content_type = response.headers.get("content-type", "").lower()
            if "text/html" not in content_type:
                return None
            match = re.search(
                r"<title[^>]*>(.*?)</title>", response.text, flags=re.IGNORECASE | re.DOTALL
            )
            if not match:
                return ""
            title = html.unescape(match.group(1))
            return re.sub(r"\s+", " ", title).strip()
    except Exception:
        return None


def _merge_unique_results(base: list[dict], additional: list[dict], max_results: int) -> list[dict]:
    merged = list(base)
    seen_urls = {result.get("url", "") for result in merged}
    for candidate in additional:
        url = candidate.get("url", "")
        if url and url not in seen_urls:
            merged.append(candidate)
            seen_urls.add(url)
        if len(merged) >= max_results:
            break
    return merged


def _normalize_match_path(url: str) -> str:
    path = (urlparse(url).path or "").strip("/").lower()
    return re.sub(r"/+", "/", path)


def _is_near_root_path(path: str) -> bool:
    if not path:
        return True
    parts = [part for part in path.split("/") if part]
    if len(parts) > 1:
        return False
    return parts[0] in NEAR_ROOT_PATH_KEYWORDS


def _urls_agree(brave_url: str, llm_url: str) -> bool:
    brave_domain = _registrable_domain(_normalize_domain(brave_url))
    llm_domain = _registrable_domain(_normalize_domain(llm_url))
    if not brave_domain or not llm_domain or brave_domain != llm_domain:
        return False

    brave_path = _normalize_match_path(brave_url)
    llm_path = _normalize_match_path(llm_url)
    if brave_path == llm_path:
        return True

    if _is_near_root_path(brave_path) and _is_near_root_path(llm_path):
        return True

    return False


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


def _llm_web_verify(
    org_name: str,
    org_location: str,
    candidate_url: str,
    settings: Settings,
    stop_event: threading.Event | None = None,
) -> tuple[str | None, str, str, str]:
    """Ask an LLM web search tool for the official URL."""
    try:
        from openai import OpenAI
    except Exception as e:
        return None, f"llm_unavailable: {e}", "", ""

    openai_api_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_api_key:
        raise ExternalApiAccessError(
            provider="OpenAI",
            reason="missing_api_key",
            details="OPENAI_API_KEY is not set",
        )
    if stop_event is not None and stop_event.is_set():
        return None, "llm_skipped: batch_stopped", "", ""

    prompt_def = settings.prompts.get("discover.website_verify")
    if prompt_def is None:
        raise ValueError("Prompt 'discover.website_verify' is missing from prompt registry")
    prompt = render_prompt_template(
        prompt_def,
        {
            "org_name": org_name,
            "org_location": org_location or "-",
            "candidate_url": candidate_url or "-",
        },
    )

    try:
        client = OpenAI()
        response = client.responses.create(
            model=settings.llm.model,
            input=prompt,
            tools=[{"type": "web_search_preview"}],
            temperature=0,
        )
    except Exception as e:
        access_error = classify_openai_access_error(e)
        if access_error is not None:
            raise access_error
        logger.warning("LLM verification failed for '%s': %s", org_name, e)
        return None, f"llm_error: {e}", prompt, ""

    output_text = getattr(response, "output_text", "")
    payload = _extract_json_object(output_text)
    llm_url_raw = str(payload.get("url", "") or "").strip()
    llm_url = llm_url_raw if llm_url_raw.startswith(("http://", "https://")) else None
    if not llm_url:
        reason = str(payload.get("reason", "") or "").strip()
        return None, f"llm_no_url: {reason}", prompt, output_text

    reason = str(payload.get("reason", "") or "").strip()
    confidence = str(payload.get("confidence", "") or "").strip()
    note = f"llm_url={llm_url!r}, confidence={confidence!r}, reason={reason!r}"
    return llm_url, note, prompt, output_text


def _search_with_fallback(
    query_name: str,
    query_location: str,
    settings: Settings,
    rate_limiter: _SearchRateLimiter | None = None,
    stop_event: threading.Event | None = None,
) -> tuple[list[dict], str, int]:
    """Run unquoted search first; trigger quoted fallback based on score quality."""
    primary_query = f"{query_name} {query_location}".strip()
    quoted_query = f'"{query_name}" {query_location}'.strip()
    request_count = 0

    if not primary_query:
        return [], primary_query, request_count

    results = _brave_search(
        primary_query,
        max_results=settings.search.max_results,
        timeout=settings.search.timeout_seconds,
        max_retries=settings.search.max_retries,
        retry_backoff_seconds=settings.search.retry_backoff_seconds,
        rate_limiter=rate_limiter,
        stop_event=stop_event,
    )
    request_count += 1

    should_run_quoted_fallback = False
    fallback_reason = ""
    if quoted_query and quoted_query != primary_query:
        provisional_pages = _score_pages(results, query_name)
        if not provisional_pages:
            should_run_quoted_fallback = True
            fallback_reason = "no_usable_results"
        else:
            provisional_candidates = _build_domain_candidates(provisional_pages)
            provisional_score, provisional_domain, _ = provisional_candidates[0]
            top_domain_deprioritized = _is_deprioritized(f"https://{provisional_domain}/")
            provisional_gap = _score_gap(provisional_pages)

            if top_domain_deprioritized:
                should_run_quoted_fallback = True
                fallback_reason = "top_domain_deprioritized"
            elif provisional_score < settings.search.fallback_score_threshold:
                should_run_quoted_fallback = True
                fallback_reason = f"top_score<{settings.search.fallback_score_threshold}"
            elif (
                provisional_gap is not None
                and provisional_gap < settings.search.fallback_min_score_gap
            ):
                should_run_quoted_fallback = True
                fallback_reason = f"score_gap<{settings.search.fallback_min_score_gap}"

    if should_run_quoted_fallback:
        quoted_results = _brave_search(
            quoted_query,
            max_results=settings.search.max_results,
            timeout=settings.search.timeout_seconds,
            max_retries=settings.search.max_retries,
            retry_backoff_seconds=settings.search.retry_backoff_seconds,
            rate_limiter=rate_limiter,
            stop_event=stop_event,
        )
        request_count += 1
        results = _merge_unique_results(results, quoted_results, settings.search.max_results)
        strategy = f"{primary_query} (+quoted fallback:{fallback_reason or 'score_policy'})"
        return results, strategy, request_count

    return results, primary_query, request_count


def _brave_search(
    query: str,
    max_results: int = 5,
    timeout: int = 15,
    max_retries: int = 3,
    retry_backoff_seconds: float = 1.0,
    rate_limiter: _SearchRateLimiter | None = None,
    stop_event: threading.Event | None = None,
) -> list[dict]:
    """Execute a search query using the Brave Search API.

    Returns a list of result dicts with 'url', 'title', 'description' keys.
    """
    api_key = os.environ.get("BRAVE_API_KEY", "")
    if not api_key:
        raise ExternalApiAccessError(
            provider="Brave",
            reason="missing_api_key",
            details=(
                "BRAVE_API_KEY not set. Add it to your .env file. "
                "Get a key at: https://brave.com/search/api/"
            ),
        )

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": api_key,
    }
    params = {
        "q": query,
        "count": max_results,
        "search_lang": "de",
        "country": "CH",
    }

    retryable_status_codes = {429, 500, 502, 503, 504}
    with httpx.Client(timeout=timeout) as client:
        for attempt in range(max_retries + 1):
            try:
                if stop_event is not None and stop_event.is_set():
                    return []
                if rate_limiter is not None:
                    rate_limiter.wait_for_slot()
                response = client.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers=headers,
                    params=params,
                )

                body_text = response.text
                access_error = classify_http_access_error(
                    provider="Brave",
                    status_code=response.status_code,
                    body_text=body_text,
                    headers=dict(response.headers),
                )
                if access_error is not None:
                    raise access_error

                if response.status_code in retryable_status_codes and attempt < max_retries:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        retry_after_seconds = float(retry_after) if retry_after else 0.0
                    except ValueError:
                        retry_after_seconds = 0.0
                    backoff_seconds = retry_backoff_seconds * (2**attempt)
                    sleep_seconds = max(retry_after_seconds, backoff_seconds)
                    logger.warning(
                        "Search API returned status %d; retrying in %.2fs (attempt %d/%d)",
                        response.status_code,
                        sleep_seconds,
                        attempt + 1,
                        max_retries,
                    )
                    if stop_event is not None and stop_event.wait(sleep_seconds):
                        return []
                    if stop_event is None:
                        time.sleep(sleep_seconds)
                    continue

                response.raise_for_status()
                data = response.json()
                break
            except httpx.RequestError as e:
                if attempt >= max_retries:
                    raise
                sleep_seconds = retry_backoff_seconds * (2**attempt)
                logger.warning(
                    "Search request error for query %r: %s; retrying in %.2fs (attempt %d/%d)",
                    query,
                    e,
                    sleep_seconds,
                    attempt + 1,
                    max_retries,
                )
                if stop_event is not None and stop_event.wait(sleep_seconds):
                    return []
                if stop_event is None:
                    time.sleep(sleep_seconds)
        else:
            data = {}

    results = []
    for item in data.get("web", {}).get("results", []):
        results.append(
            {
                "url": item.get("url", ""),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
            }
        )

    return results


def _firecrawl_search(
    query: str,
    max_results: int = 10,
    timeout: int = 30,
    max_retries: int = 2,
    retry_backoff_seconds: float = 1.0,
    rate_limiter: _SearchRateLimiter | None = None,
    stop_event: threading.Event | None = None,
) -> list[dict]:
    """Execute a search query using the Firecrawl Search API.

    Uses Firecrawl's Google-based index as a fallback when Brave's index
    doesn't cover the target site. Returns the same format as _brave_search().
    """
    api_key = os.environ.get("FIRECRAWL_API_KEY", "")
    if not api_key:
        logger.debug("FIRECRAWL_API_KEY not set; skipping Firecrawl search.")
        return []

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "query": query,
        "limit": max_results,
        "country": "CH",
        "location": "Switzerland",
    }

    retryable_status_codes = {429, 500, 502, 503, 504}
    with httpx.Client(timeout=timeout) as client:
        for attempt in range(max_retries + 1):
            try:
                if stop_event is not None and stop_event.is_set():
                    return []
                if rate_limiter is not None:
                    rate_limiter.wait_for_slot()
                response = client.post(
                    "https://api.firecrawl.dev/v2/search",
                    headers=headers,
                    json=payload,
                )

                body_text = response.text
                access_error = classify_http_access_error(
                    provider="Firecrawl",
                    status_code=response.status_code,
                    body_text=body_text,
                    headers=dict(response.headers),
                )
                if access_error is not None:
                    raise access_error

                if response.status_code in retryable_status_codes and attempt < max_retries:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        retry_after_seconds = float(retry_after) if retry_after else 0.0
                    except ValueError:
                        retry_after_seconds = 0.0
                    backoff_seconds = retry_backoff_seconds * (2**attempt)
                    sleep_seconds = max(retry_after_seconds, backoff_seconds)
                    logger.warning(
                        "Firecrawl API returned status %d; retrying in %.2fs (attempt %d/%d)",
                        response.status_code,
                        sleep_seconds,
                        attempt + 1,
                        max_retries,
                    )
                    if stop_event is not None and stop_event.wait(sleep_seconds):
                        return []
                    if stop_event is None:
                        time.sleep(sleep_seconds)
                    continue

                response.raise_for_status()
                data = response.json()
                break
            except httpx.RequestError as e:
                if attempt >= max_retries:
                    logger.warning(
                        "Firecrawl search failed after retries for query %r: %s", query, e
                    )
                    return []
                sleep_seconds = retry_backoff_seconds * (2**attempt)
                logger.warning(
                    "Firecrawl request error for query %r: %s; retrying in %.2fs (attempt %d/%d)",
                    query,
                    e,
                    sleep_seconds,
                    attempt + 1,
                    max_retries,
                )
                if stop_event is not None and stop_event.wait(sleep_seconds):
                    return []
                if stop_event is None:
                    time.sleep(sleep_seconds)
        else:
            return []

    # Firecrawl returns results in data.web[] (search-only, no scrapeOptions).
    results: list[dict] = []
    web_results = data.get("data", {})
    if isinstance(web_results, dict):
        web_results = web_results.get("web", [])
    elif not isinstance(web_results, list):
        web_results = []
    for item in web_results:
        url = item.get("url", "")
        if url:
            results.append(
                {
                    "url": url,
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                }
            )

    return results


@dataclass
class _DiscoveryTrace:
    query_used: str
    request_count: int
    brave_candidates: list[WebsiteCandidate]
    llm_candidates: list[WebsiteCandidate] | None
    firecrawl_candidates: list[WebsiteCandidate] | None
    result: WebsiteResult


def _run_discovery_cascade(
    org_name: str,
    org_location: str,
    settings: Settings,
    rate_limiter: _SearchRateLimiter | None = None,
    llm_verify_enabled: bool | None = None,
    stop_event: threading.Event | None = None,
) -> _DiscoveryTrace:
    query_name = _sanitize_search_text(org_name)
    query_location = _sanitize_search_text(org_location)
    llm_enabled = (
        settings.search.llm_verify_enabled if llm_verify_enabled is None else llm_verify_enabled
    )

    results, query_used, request_count = _search_with_fallback(
        query_name,
        query_location,
        settings,
        rate_limiter=rate_limiter,
        stop_event=stop_event,
    )

    brave_rank = _rank_results(results, org_name)
    brave_candidates = _pages_to_candidates(brave_rank[0]) if brave_rank else []
    llm_candidates: list[WebsiteCandidate] | None = None
    firecrawl_candidates: list[WebsiteCandidate] | None = None

    brave_url = brave_rank[3].url if brave_rank else None
    llm_url: str | None = None
    llm_agrees: bool | None = None
    llm_prompt: str | None = None
    llm_response: str | None = None
    llm_note = "llm_not_used" if llm_enabled else "llm_skipped"
    cross_threshold = settings.search.cross_provider_agree_min_score

    def make_result(
        stage: str,
        rank: tuple[list[_ScoredPageResult], int, str, _ScoredPageResult, int | None] | None,
        *,
        needs_review: bool,
        confidence: str,
        source_prefix: str,
    ) -> WebsiteResult:
        if rank is None:
            return WebsiteResult(
                org_name=org_name,
                url=None,
                confidence="none",
                source=f"{source_prefix} (query={query_used!r}, no candidates, {llm_note})",
                needs_review=True,
                decision_stage=stage,
                llm_url=llm_url,
                llm_agrees=llm_agrees,
                llm_prompt=llm_prompt,
                llm_response=llm_response,
            )

        _, best_score, best_domain, best_page, score_gap = rank
        return WebsiteResult(
            org_name=org_name,
            url=best_page.url,
            confidence=confidence,
            source=(
                f"{source_prefix} (query={query_used!r}, domain={best_domain!r}, "
                f"score={best_score}, score_gap={score_gap}, title={best_page.title!r}, {llm_note})"
            ),
            needs_review=needs_review,
            score=best_score,
            score_gap=score_gap,
            llm_url=llm_url,
            llm_agrees=llm_agrees,
            decision_stage=stage,
            llm_prompt=llm_prompt,
            llm_response=llm_response,
        )

    # Tier 1: Brave only.
    if brave_rank is not None and brave_rank[1] >= settings.search.auto_accept_score:
        return _DiscoveryTrace(
            query_used=query_used,
            request_count=request_count,
            brave_candidates=brave_candidates,
            llm_candidates=None,
            firecrawl_candidates=None,
            result=make_result(
                "brave_auto",
                brave_rank,
                needs_review=False,
                confidence="high",
                source_prefix="brave_search",
            ),
        )

    working_results = list(results)
    working_rank = brave_rank

    # Tier 2: LLM web search candidate.
    if llm_enabled:
        candidate_url = brave_url or ""
        llm_url, llm_note, llm_prompt, llm_response = _llm_web_verify(
            org_name,
            org_location,
            candidate_url,
            settings,
            stop_event=stop_event,
        )
        if llm_url:
            fetched_title = _fetch_page_title(llm_url, timeout=settings.search.timeout_seconds)
            if fetched_title is None:
                llm_note = f"{llm_note}, url_unreachable"
                llm_url = None
            else:
                llm_result = [
                    {"url": llm_url, "title": fetched_title, "description": "llm_web_search"}
                ]
                working_results = _merge_unique_results(
                    working_results,
                    llm_result,
                    settings.search.max_results * 3,
                )
                working_rank = _rank_results(working_results, org_name)
                if working_rank is not None:
                    llm_candidates = _pages_to_candidates(working_rank[0])
                    llm_agrees = bool(brave_url and _urls_agree(brave_url, llm_url))

                    if working_rank[1] >= settings.search.auto_accept_score:
                        return _DiscoveryTrace(
                            query_used=query_used,
                            request_count=request_count,
                            brave_candidates=brave_candidates,
                            llm_candidates=llm_candidates,
                            firecrawl_candidates=None,
                            result=make_result(
                                "llm_auto",
                                working_rank,
                                needs_review=False,
                                confidence="high",
                                source_prefix="llm_plus_search",
                            ),
                        )

                    if llm_agrees and working_rank[1] >= cross_threshold:
                        return _DiscoveryTrace(
                            query_used=query_used,
                            request_count=request_count,
                            brave_candidates=brave_candidates,
                            llm_candidates=llm_candidates,
                            firecrawl_candidates=None,
                            result=make_result(
                                "llm_brave_agree",
                                working_rank,
                                needs_review=False,
                                confidence="high",
                                source_prefix="llm_plus_search",
                            ),
                        )

    # Tier 3: Firecrawl fallback.
    firecrawl_api_key = os.environ.get("FIRECRAWL_API_KEY", "")
    if settings.search.firecrawl_enabled and firecrawl_api_key:
        firecrawl_query = f"{query_name} {query_location}".strip()
        if firecrawl_query:
            try:
                fc_results = _firecrawl_search(
                    firecrawl_query,
                    max_results=settings.search.firecrawl_max_results,
                    timeout=settings.search.firecrawl_timeout_seconds,
                    max_retries=settings.search.firecrawl_max_retries,
                    rate_limiter=rate_limiter,
                    stop_event=stop_event,
                )
                request_count += 1
            except ExternalApiAccessError:
                raise
            except Exception as e:
                logger.warning("Firecrawl search failed for '%s': %s", org_name, e)
                fc_results = []

            if fc_results:
                working_results = _merge_unique_results(
                    working_results,
                    fc_results,
                    settings.search.max_results * 3,
                )
                working_rank = _rank_results(working_results, org_name)
                if working_rank is not None:
                    firecrawl_candidates = _pages_to_candidates(working_rank[0])
                    firecrawl_rank = _rank_results(fc_results, org_name)
                    firecrawl_best_url = (
                        firecrawl_rank[3].url if firecrawl_rank is not None else None
                    )
                    agrees_with_brave = bool(
                        firecrawl_best_url
                        and brave_url
                        and _urls_agree(firecrawl_best_url, brave_url)
                    )
                    agrees_with_llm = bool(
                        firecrawl_best_url and llm_url and _urls_agree(firecrawl_best_url, llm_url)
                    )

                    if working_rank[1] >= settings.search.auto_accept_score:
                        return _DiscoveryTrace(
                            query_used=query_used,
                            request_count=request_count,
                            brave_candidates=brave_candidates,
                            llm_candidates=llm_candidates,
                            firecrawl_candidates=firecrawl_candidates,
                            result=make_result(
                                "firecrawl_auto",
                                working_rank,
                                needs_review=False,
                                confidence="high",
                                source_prefix="firecrawl_plus_search",
                            ),
                        )

                    if (agrees_with_brave or agrees_with_llm) and working_rank[
                        1
                    ] >= cross_threshold:
                        llm_agrees = agrees_with_llm if llm_url else llm_agrees
                        return _DiscoveryTrace(
                            query_used=query_used,
                            request_count=request_count,
                            brave_candidates=brave_candidates,
                            llm_candidates=llm_candidates,
                            firecrawl_candidates=firecrawl_candidates,
                            result=make_result(
                                "firecrawl_agree",
                                working_rank,
                                needs_review=False,
                                confidence="high",
                                source_prefix="firecrawl_plus_search",
                            ),
                        )

    # Tier 4: Manual review with best available candidate.
    if working_rank is None:
        working_rank = _rank_results(working_results, org_name)

    confidence = "none"
    if working_rank is not None and working_rank[1] >= 0:
        confidence = "low"

    return _DiscoveryTrace(
        query_used=query_used,
        request_count=request_count,
        brave_candidates=brave_candidates,
        llm_candidates=llm_candidates,
        firecrawl_candidates=firecrawl_candidates,
        result=make_result(
            "manual_review",
            working_rank,
            needs_review=True,
            confidence=confidence,
            source_prefix="cascade_search",
        ),
    )


def find_website(
    org_name: str,
    org_location: str,
    settings: Settings,
    rate_limiter: _SearchRateLimiter | None = None,
    llm_verify_enabled: bool | None = None,
    stop_event: threading.Event | None = None,
) -> WebsiteResult:
    """Find the website for a given organization via cascade search."""
    try:
        trace = _run_discovery_cascade(
            org_name,
            org_location,
            settings,
            rate_limiter=rate_limiter,
            llm_verify_enabled=llm_verify_enabled,
            stop_event=stop_event,
        )
        return trace.result
    except ExternalApiAccessError:
        raise
    except Exception as e:
        logger.warning("Search failed for '%s': %s", org_name, e)
        return WebsiteResult(
            org_name=org_name,
            url=None,
            confidence="none",
            source=f"error: {e}",
            needs_review=True,
            decision_stage="search_error",
        )


def inspect_website_candidates(
    org_name: str,
    org_location: str,
    settings: Settings,
    llm_verify_enabled: bool | None = None,
) -> tuple[
    str,
    list[WebsiteCandidate],
    int,
    list[WebsiteCandidate] | None,
    list[WebsiteCandidate] | None,
    WebsiteResult,
]:
    """Run one discover query and return all scored candidates.

    Returns (
        query_used,
        brave_candidates,
        request_count,
        llm_candidates,
        firecrawl_candidates,
        decision_result,
    ).
    llm_candidates / firecrawl_candidates are None when those tiers were not used.
    """
    trace = _run_discovery_cascade(
        org_name,
        org_location,
        settings,
        llm_verify_enabled=llm_verify_enabled,
    )
    return (
        trace.query_used,
        trace.brave_candidates,
        trace.request_count,
        trace.llm_candidates,
        trace.firecrawl_candidates,
        trace.result,
    )


def find_websites_batch(
    organizations: list[dict],
    settings: Settings,
    name_column: str = "Bezeichnung",
    location_column: str = "Sitzort",
    llm_verify_enabled: bool | None = None,
    on_result: Callable[[int, WebsiteResult], None] | None = None,
) -> list[WebsiteResult]:
    """Find websites for a batch of organizations with rate limiting.

    Args:
        organizations: List of org dicts (from parsed/filtered data).
        settings: Application settings.
        name_column: Column name containing the org name.
        location_column: Column name containing the org location.

    Returns:
        List of WebsiteResult objects, one per organization.
    """
    if not organizations:
        return []

    requests_per_second = settings.search.max_requests_per_second
    if requests_per_second <= 0 and settings.search.request_delay_seconds > 0:
        requests_per_second = 1.0 / settings.search.request_delay_seconds

    rate_limiter = _SearchRateLimiter(requests_per_second)
    max_workers = max(1, settings.search.max_workers)
    indexed_results: list[tuple[int, WebsiteResult]] = []
    stop_event = threading.Event()

    def run_single(index: int, org: dict) -> tuple[int, WebsiteResult]:
        if stop_event.is_set():
            return index, WebsiteResult(
                org_name=str(org.get(name_column, "") or ""),
                url=None,
                confidence="none",
                source="skipped: batch stopped",
                needs_review=True,
                decision_stage="batch_stopped",
            )
        name = org.get(name_column, "")
        location = org.get(location_column, "")
        result = find_website(
            name,
            location,
            settings,
            rate_limiter=rate_limiter,
            llm_verify_enabled=llm_verify_enabled,
            stop_event=stop_event,
        )
        return index, result

    executor = ThreadPoolExecutor(max_workers=max_workers)
    futures = [executor.submit(run_single, index, org) for index, org in enumerate(organizations)]
    try:
        for future in as_completed(futures):
            try:
                index, result = future.result()
            except ExternalApiAccessError:
                stop_event.set()
                executor.shutdown(wait=True, cancel_futures=True)
                raise
            indexed_results.append((index, result))
            if on_result is not None:
                on_result(index, result)
    finally:
        executor.shutdown(wait=True)

    indexed_results.sort(key=lambda item: item[0])
    return [result for _, result in indexed_results]
