"""Web scraping: respectfully scrape organization websites.

Downloads key pages from each organization's website, converts HTML to markdown,
and stores the content locally for later analysis. Respects robots.txt and
implements rate limiting.

Implementation maturity note:
This is a first-shot implementation based on the initial post-discovery schema.
Upstream website-discovery and manual-review fields evolved afterward. Before
relying on scrape outputs, verify that CSV columns and exclusion semantics still
align with current pipeline assumptions.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx

# TODO: check if async would make sense here
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from robotexclusionrulesparser import RobotExclusionRulesParser

from benefind.config import DATA_DIR, Settings

logger = logging.getLogger(__name__)

# Pages we typically want to scrape (paths relative to the site root)
DEFAULT_PAGES = [
    "/",
    "/about",
    "/ueber-uns",
    "/about-us",
    "/portrait",
    "/leitbild",
    "/projekte",
    "/projects",
    "/angebot",
    "/angebote",
    "/spenden",
    "/donate",
    "/kontakt",
    "/contact",
    "/jahresbericht",
    "/annual-report",
]


def check_robots_txt(
    base_url: str,
    user_agent: str,
    timeout: int = 10,
) -> RobotExclusionRulesParser | None:
    """Fetch and parse the robots.txt for a given site.

    Returns the parser object, or None if robots.txt is not found or cannot
    be parsed.
    """
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(robots_url)
            if response.status_code == 200:
                parser = RobotExclusionRulesParser()
                parser.parse(response.text)
                logger.info("Parsed robots.txt for %s", parsed.netloc)
                return parser
            else:
                logger.debug(
                    "No robots.txt found for %s (status %d)", parsed.netloc, response.status_code
                )
                return None
    except Exception as e:
        logger.debug("Could not fetch robots.txt for %s: %s", parsed.netloc, e)
        return None


def is_allowed(
    url: str,
    robots: RobotExclusionRulesParser | None,
    user_agent: str,
) -> bool:
    """Check if we are allowed to fetch a URL according to robots.txt."""
    if robots is None:
        return True
    return robots.is_allowed(user_agent, url)


def scrape_page(
    url: str,
    user_agent: str,
    timeout: int = 30,
) -> str | None:
    """Fetch a single page and convert its content to markdown.

    Returns the markdown content, or None if the page could not be fetched.
    """
    try:
        headers = {"User-Agent": user_agent}
        with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
            response = client.get(url)

            if response.status_code != 200:
                logger.debug("Got status %d for %s", response.status_code, url)
                return None

            content_type = response.headers.get("content-type", "")
            if "text/html" not in content_type:
                logger.debug("Skipping non-HTML content at %s: %s", url, content_type)
                return None

            # Parse HTML and extract main content
            soup = BeautifulSoup(response.text, "html.parser")

            # Remove script, style, nav, footer elements
            for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
                tag.decompose()

            # Convert to markdown
            markdown = md(str(soup), strip=["img"])

            # Clean up excessive whitespace
            markdown = re.sub(r"\n{3,}", "\n\n", markdown)
            markdown = markdown.strip()

            return markdown

    except Exception as e:
        logger.warning("Error scraping %s: %s", url, e)
        return None


def discover_pages(
    base_url: str,
    user_agent: str,
    timeout: int = 30,
) -> list[str]:
    """Discover actual pages on a website by checking which default paths exist.

    Also looks at the homepage for internal links to find relevant pages.
    Returns a list of URLs that exist and are worth scraping.
    """
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    found_urls = []

    # Check default paths
    for path in DEFAULT_PAGES:
        url = urljoin(base, path)
        try:
            headers = {"User-Agent": user_agent}
            with httpx.Client(
                timeout=timeout,
                follow_redirects=True,
                headers=headers,
            ) as client:
                response = client.head(url)
                if response.status_code == 200:
                    found_urls.append(url)
        except Exception:
            continue

    # If we only found the homepage, try to discover pages from links
    if len(found_urls) <= 1:
        try:
            headers = {"User-Agent": user_agent}
            with httpx.Client(
                timeout=timeout,
                follow_redirects=True,
                headers=headers,
            ) as client:
                response = client.get(base)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for link in soup.find_all("a", href=True):
                        href = link["href"]
                        full_url = urljoin(base, href)
                        if urlparse(full_url).netloc == parsed.netloc:
                            if full_url not in found_urls:
                                found_urls.append(full_url)
        except Exception:
            pass

    return found_urls


def scrape_organization(
    org_name: str,
    website_url: str,
    settings: Settings,
) -> Path | None:
    """Scrape an organization's website and store content locally.

    Creates a directory under data/orgs/<org_slug>/pages/ with one markdown
    file per scraped page.

    Returns the org directory path, or None if scraping failed entirely.
    """
    slug = _slugify(org_name)
    org_dir = DATA_DIR / "orgs" / slug
    pages_dir = org_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    user_agent = settings.scraping.user_agent
    timeout = settings.scraping.timeout_seconds
    delay = settings.scraping.request_delay_seconds
    max_pages = settings.scraping.max_pages_per_org

    # Check robots.txt
    robots = None
    if settings.scraping.respect_robots_txt:
        robots = check_robots_txt(website_url, user_agent, timeout)

    # Discover pages
    pages = discover_pages(website_url, user_agent, timeout)
    logger.info("Discovered %d pages for %s", len(pages), org_name)

    # Scrape pages
    scraped_count = 0
    for url in pages[:max_pages]:
        if not is_allowed(url, robots, user_agent):
            logger.info("Skipping %s (blocked by robots.txt)", url)
            continue

        content = scrape_page(url, user_agent, timeout)
        if content:
            page_slug = _slugify(urlparse(url).path or "index")
            page_path = pages_dir / f"{page_slug}.md"
            page_path.write_text(content, encoding="utf-8")
            scraped_count += 1

        time.sleep(delay)

    if scraped_count == 0:
        logger.warning("Could not scrape any pages for %s", org_name)
        return None

    logger.info("Scraped %d pages for %s -> %s", scraped_count, org_name, org_dir)
    return org_dir


def scrape_organization_urls(
    org_name: str,
    urls: list[str],
    settings: Settings,
) -> Path | None:
    """Scrape a precomputed list of URLs for one organization.

    Assumes URL planning (robots/scope/discovery) already happened in
    `prepare-scraping` and applies only fetch/convert/store behavior here.
    """
    slug = _slugify(org_name)
    org_dir = DATA_DIR / "orgs" / slug
    pages_dir = org_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    user_agent = settings.scraping.user_agent
    timeout = settings.scraping.timeout_seconds
    delay = settings.scraping.request_delay_seconds
    max_pages = settings.scraping.max_pages_per_org

    scraped_count = 0
    used_slugs: set[str] = set()

    for url in urls[:max_pages]:
        content = scrape_page(url, user_agent, timeout)
        if not content:
            if delay > 0:
                time.sleep(delay)
            continue

        page_slug = _slugify(f"{urlparse(url).netloc}-{urlparse(url).path or 'index'}")
        if page_slug in used_slugs:
            page_slug = _slugify(f"{page_slug}-{scraped_count + 1}")
        used_slugs.add(page_slug)

        page_path = pages_dir / f"{page_slug}.md"
        page_path.write_text(content, encoding="utf-8")
        scraped_count += 1

        if delay > 0:
            time.sleep(delay)

    if scraped_count == 0:
        logger.warning("Could not scrape any pages for %s", org_name)
        return None

    logger.info("Scraped %d pages for %s -> %s", scraped_count, org_name, org_dir)
    return org_dir


def _slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return slug or "unnamed"
