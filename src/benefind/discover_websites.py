"""Website discovery: find the website URL for each organization.

Uses the Brave Search API to find the official website for each organization
in the filtered list.
"""

from __future__ import annotations

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

from benefind.config import Settings

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

LOW_SCORE_BROADEN_THRESHOLD = 10


@dataclass
class WebsiteResult:
    """Result of searching for an organization's website."""

    org_name: str
    url: str | None
    confidence: str  # "high", "medium", "low", "none"
    source: str  # how the URL was found
    needs_review: bool


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

    preferred = [
        "about",
        "ueber-uns",
        "about-us",
        "portrait",
        "kontakt",
        "contact",
        "home",
    ]
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


def _search_with_fallback(
    query_name: str,
    query_location: str,
    settings: Settings,
    rate_limiter: _SearchRateLimiter | None = None,
) -> tuple[list[dict], str, int]:
    """Run unquoted search first; fall back to quoted search if needed."""
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
    )
    request_count += 1

    should_run_quoted_fallback = False
    if quoted_query and quoted_query != primary_query:
        if len(results) < settings.search.min_results_before_broad_search:
            should_run_quoted_fallback = True
        else:
            provisional_pages = _score_pages(results, query_name)
            if provisional_pages:
                provisional_score, provisional_domain, _ = _pick_best_domain(provisional_pages)
                top_domain_deprioritized = _is_deprioritized(f"https://{provisional_domain}/")
                if top_domain_deprioritized or provisional_score < LOW_SCORE_BROADEN_THRESHOLD:
                    should_run_quoted_fallback = True

    if should_run_quoted_fallback:
        quoted_results = _brave_search(
            quoted_query,
            max_results=settings.search.max_results,
            timeout=settings.search.timeout_seconds,
            max_retries=settings.search.max_retries,
            retry_backoff_seconds=settings.search.retry_backoff_seconds,
            rate_limiter=rate_limiter,
        )
        request_count += 1
        results = _merge_unique_results(results, quoted_results, settings.search.max_results)
        return results, f"{primary_query} (+quoted fallback)", request_count

    return results, primary_query, request_count


def _brave_search(
    query: str,
    max_results: int = 5,
    timeout: int = 15,
    max_retries: int = 3,
    retry_backoff_seconds: float = 1.0,
    rate_limiter: _SearchRateLimiter | None = None,
) -> list[dict]:
    """Execute a search query using the Brave Search API.

    Returns a list of result dicts with 'url', 'title', 'description' keys.
    """
    api_key = os.environ.get("BRAVE_API_KEY", "")
    if not api_key:
        raise ValueError(
            "BRAVE_API_KEY not set. Add it to your .env file. "
            "Get a key at: https://brave.com/search/api/"
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
                if rate_limiter is not None:
                    rate_limiter.wait_for_slot()
                response = client.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers=headers,
                    params=params,
                )

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


def find_website(
    org_name: str,
    org_location: str,
    settings: Settings,
    rate_limiter: _SearchRateLimiter | None = None,
) -> WebsiteResult:
    """Find the website for a given organization using Brave Search.

    Searches for the org name + location, scores the results, and returns
    the best candidate.
    """
    query_name = _sanitize_search_text(org_name)
    query_location = _sanitize_search_text(org_location)

    try:
        results, query_used, _ = _search_with_fallback(
            query_name,
            query_location,
            settings,
            rate_limiter=rate_limiter,
        )
    except ValueError as e:
        logger.error("Search config error: %s", e)
        return WebsiteResult(
            org_name=org_name,
            url=None,
            confidence="none",
            source="error: missing API key",
            needs_review=True,
        )
    except Exception as e:
        logger.warning("Search failed for '%s': %s", org_name, e)
        return WebsiteResult(
            org_name=org_name,
            url=None,
            confidence="none",
            source=f"error: {e}",
            needs_review=True,
        )

    if not results:
        logger.debug("No search results for: %s", org_name)
        return WebsiteResult(
            org_name=org_name,
            url=None,
            confidence="none",
            source="brave_search: no results",
            needs_review=True,
        )

    scored_pages = _score_pages(results, org_name)

    if not scored_pages:
        return WebsiteResult(
            org_name=org_name,
            url=None,
            confidence="none",
            source="brave_search: no usable results",
            needs_review=True,
        )

    best_score, best_domain, best_page = _pick_best_domain(scored_pages)
    url = best_page.url

    # Determine confidence
    if best_score >= 20:
        confidence = "high"
        needs_review = False
    elif best_score >= 5:
        confidence = "medium"
        needs_review = True
    elif best_score >= -10:
        confidence = "low"
        needs_review = True
    else:
        confidence = "none"
        needs_review = True
        url = None

    return WebsiteResult(
        org_name=org_name,
        url=url,
        confidence=confidence,
        source=(
            f"brave_search (query={query_used!r}, domain={best_domain!r}, "
            f"score={best_score}, title={best_page.title!r})"
        ),
        needs_review=needs_review,
    )


def inspect_website_candidates(
    org_name: str,
    org_location: str,
    settings: Settings,
) -> tuple[str, list[WebsiteCandidate], int]:
    """Run one discover query and return all scored candidates."""
    query_name = _sanitize_search_text(org_name)
    query_location = _sanitize_search_text(org_location)
    results, query_used, request_count = _search_with_fallback(
        query_name,
        query_location,
        settings,
    )

    scored_pages = _score_pages(results, org_name)

    by_domain: dict[str, list[_ScoredPageResult]] = {}
    for page in scored_pages:
        by_domain.setdefault(page.domain, []).append(page)

    candidates: list[WebsiteCandidate] = []
    for domain, pages in by_domain.items():
        best_score = max(page.score for page in pages)
        repeat_bonus = min(6, (len(pages) - 1) * 3)
        root_bonus = (
            3 if any((urlparse(page.url).path or "").strip("/") == "" for page in pages) else 0
        )
        domain_score = best_score + repeat_bonus + root_bonus
        canonical_page = sorted(
            pages,
            key=lambda page: (_canonical_page_priority(page.url), page.score),
            reverse=True,
        )[0]
        candidates.append(
            WebsiteCandidate(
                score=domain_score,
                url=canonical_page.url,
                title=canonical_page.title,
                description=f"domain={domain}; supporting_results={len(pages)}",
            )
        )

    candidates.sort(key=lambda candidate: candidate.score, reverse=True)
    return query_used, candidates, request_count


def find_websites_batch(
    organizations: list[dict],
    settings: Settings,
    name_column: str = "Bezeichnung",
    location_column: str = "Sitzort",
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

    def run_single(index: int, org: dict) -> tuple[int, WebsiteResult]:
        name = org.get(name_column, "")
        location = org.get(location_column, "")
        result = find_website(name, location, settings, rate_limiter=rate_limiter)
        return index, result

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(run_single, index, org) for index, org in enumerate(organizations)
        ]
        for future in as_completed(futures):
            index, result = future.result()
            indexed_results.append((index, result))
            if on_result is not None:
                on_result(index, result)

    indexed_results.sort(key=lambda item: item[0])
    return [result for _, result in indexed_results]
