"""Website discovery: find the website URL for each organization.

Uses a search API or LLM with web search to find the official website
for each organization in the filtered list.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

from benefind.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class WebsiteResult:
    """Result of searching for an organization's website."""

    org_name: str
    url: str | None
    confidence: str  # "high", "medium", "low", "none"
    source: str  # how the URL was found
    needs_review: bool


def find_website(
    org_name: str,
    org_location: str,
    settings: Settings,
) -> WebsiteResult:
    """Find the website for a given organization.

    Tries a web search with the org name + location to find the most likely
    official website.
    """
    # TODO: Implement search provider integration (Google Custom Search, Brave, SerpAPI)
    # For now, this is a stub that will be implemented in a later step.
    logger.warning(
        "Website discovery not yet implemented. Org: %s",
        org_name,
    )
    return WebsiteResult(
        org_name=org_name,
        url=None,
        confidence="none",
        source="not_implemented",
        needs_review=True,
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
