"""Website discovery: find the website URL for each organization.

Uses the Brave Search API to find the official website for each organization
in the filtered list.
"""

from __future__ import annotations

import logging
import os
import time
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
    "stiftungschweiz.ch",
}


@dataclass
class WebsiteResult:
    """Result of searching for an organization's website."""

    org_name: str
    url: str | None
    confidence: str  # "high", "medium", "low", "none"
    source: str  # how the URL was found
    needs_review: bool


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
    domain = urlparse(url).netloc.lower()

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

    return score


def _brave_search(query: str, max_results: int = 5, timeout: int = 15) -> list[dict]:
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

    with httpx.Client(timeout=timeout) as client:
        response = client.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers=headers,
            params=params,
        )
        response.raise_for_status()
        data = response.json()

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
) -> WebsiteResult:
    """Find the website for a given organization using Brave Search.

    Searches for the org name + location, scores the results, and returns
    the best candidate.
    """
    query = f'"{org_name}" {org_location}'

    try:
        results = _brave_search(query, max_results=settings.search.max_results)
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
        logger.info("No search results for: %s", org_name)
        return WebsiteResult(
            org_name=org_name,
            url=None,
            confidence="none",
            source="brave_search: no results",
            needs_review=True,
        )

    # Score and sort results
    scored = []
    for r in results:
        score = _score_result(r["url"], r["title"], org_name)
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)

    best_score, best = scored[0]
    url = best["url"]

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
        source=f"brave_search (score={best_score}, title={best['title']!r})",
        needs_review=needs_review,
    )


def find_websites_batch(
    organizations: list[dict],
    settings: Settings,
    name_column: str = "Bezeichnung",
    location_column: str = "Sitz",
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
    results = []
    delay = settings.scraping.request_delay_seconds

    for i, org in enumerate(organizations):
        name = org.get(name_column, "")
        location = org.get(location_column, "")

        logger.info("[%d/%d] Searching for: %s", i + 1, len(organizations), name)
        result = find_website(name, location, settings)
        results.append(result)

        if i < len(organizations) - 1:
            time.sleep(delay)

    found = sum(1 for r in results if r.url)
    logger.info(
        "Found websites for %d/%d organizations.",
        found,
        len(organizations),
    )
    return results
