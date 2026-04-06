"""Prepare scraping targets with robots-aware, org-scoped URL discovery.

Builds a per-organization scraping plan before actual page scraping:
- determines robots.txt policy status for the organization website
- derives crawl scope from the discovered seed URL (host or path-prefix)
- discovers in-scope URLs sitemap-first, then local-link fallback

No content/relevance filtering is applied in this step.
"""

from __future__ import annotations

import gzip
import logging
import time
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup
from robotexclusionrulesparser import RobotExclusionRulesParser

from benefind.config import DATA_DIR, Settings

logger = logging.getLogger(__name__)


@dataclass
class ScopeDefinition:
    seed_url: str
    seed_origin: str
    seed_host: str
    scope_mode: str
    path_prefix: str
    include_subdomains: bool


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _normalize_url(url: str) -> str:
    parsed = urlsplit((url or "").strip())
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        return ""
    if not parsed.netloc:
        return ""
    normalized = parsed._replace(fragment="", query="")
    path = normalized.path or "/"
    normalized = normalized._replace(path=path)
    return urlunsplit(normalized)


def _normalize_host(host: str) -> str:
    return host.lower().strip().split(":", 1)[0]


def _is_host_in_scope(host: str, scope: ScopeDefinition) -> bool:
    host_norm = _normalize_host(host)
    seed_host = scope.seed_host
    if host_norm == seed_host:
        return True
    if scope.include_subdomains and host_norm.endswith("." + seed_host):
        return True
    return False


def _is_url_in_scope(url: str, scope: ScopeDefinition) -> bool:
    parsed = urlsplit(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        return False
    if not _is_host_in_scope(parsed.netloc, scope):
        return False

    if scope.scope_mode == "host":
        return True

    path = parsed.path or "/"
    prefix = scope.path_prefix
    prefix_base = prefix.rstrip("/") or "/"
    if path == prefix or path == prefix_base:
        return True
    if path.startswith(prefix_base + "/"):
        return True
    return False


def _build_scope(seed_url: str, include_subdomains: bool) -> ScopeDefinition | None:
    normalized = _normalize_url(seed_url)
    if not normalized:
        return None

    parsed = urlsplit(normalized)
    seed_host = _normalize_host(parsed.netloc)
    path = parsed.path or "/"
    segments = [segment for segment in path.split("/") if segment]

    if not segments:
        scope_mode = "host"
        path_prefix = "/"
    else:
        last_segment = segments[-1]
        last_is_file = "." in last_segment

        # Single-segment seeds (e.g. /kontakt or /kontakt.php) are usually
        # discover-result landing pages, not intentional narrow site sections.
        if len(segments) == 1:
            scope_mode = "host"
            path_prefix = "/"
        elif last_is_file:
            parent_segments = segments[:-1]
            if parent_segments:
                scope_mode = "path_prefix"
                path_prefix = "/" + "/".join(parent_segments) + "/"
            else:
                scope_mode = "host"
                path_prefix = "/"
        else:
            scope_mode = "path_prefix"
            path_prefix = "/" + "/".join(segments) + "/"

    return ScopeDefinition(
        seed_url=normalized,
        seed_origin=f"{parsed.scheme}://{parsed.netloc}",
        seed_host=seed_host,
        scope_mode=scope_mode,
        path_prefix=path_prefix,
        include_subdomains=include_subdomains,
    )


def _robots_is_allowed(
    robots_parser: RobotExclusionRulesParser | None,
    user_agent: str,
    url: str,
) -> bool:
    if robots_parser is None:
        return True
    try:
        return bool(robots_parser.is_allowed(user_agent, url))
    except Exception:
        return True


def _extract_robots_sitemaps(robots_text: str) -> list[str]:
    sitemap_urls: list[str] = []
    for line in robots_text.splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        if key.strip().lower() != "sitemap":
            continue
        normalized = _normalize_url(value.strip())
        if normalized:
            sitemap_urls.append(normalized)
    return sitemap_urls


def _parse_xml_bytes(payload: bytes) -> ET.Element | None:
    if not payload:
        return None
    try:
        return ET.fromstring(payload)
    except ET.ParseError:
        pass

    try:
        decompressed = gzip.decompress(payload)
    except OSError:
        return None

    try:
        return ET.fromstring(decompressed)
    except ET.ParseError:
        return None


def _tag_local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1].lower()
    return tag.lower()


def _collect_sitemap_urls(
    client: httpx.Client,
    scope: ScopeDefinition,
    robots_parser: RobotExclusionRulesParser | None,
    user_agent: str,
    timeout_seconds: int,
    request_delay_seconds: float,
    max_sitemaps: int,
    max_depth: int,
    max_urls: int,
) -> list[str]:
    robots_url = scope.seed_origin + "/robots.txt"
    robots_text = ""

    try:
        response = client.get(robots_url, timeout=timeout_seconds)
        if response.status_code == 200:
            robots_text = response.text
    except Exception:
        robots_text = ""

    initial_sitemaps = _extract_robots_sitemaps(robots_text)
    if not initial_sitemaps:
        initial_sitemaps = [scope.seed_origin + "/sitemap.xml"]

    queue: deque[tuple[str, int]] = deque((u, 0) for u in initial_sitemaps)
    visited_sitemaps: set[str] = set()
    found_urls: list[str] = []
    seen_urls: set[str] = set()

    while queue and len(visited_sitemaps) < max_sitemaps and len(found_urls) < max_urls:
        sitemap_url, depth = queue.popleft()
        normalized_sitemap = _normalize_url(sitemap_url)
        if not normalized_sitemap or normalized_sitemap in visited_sitemaps:
            continue

        if robots_parser is not None and not _robots_is_allowed(
            robots_parser,
            user_agent,
            normalized_sitemap,
        ):
            continue

        visited_sitemaps.add(normalized_sitemap)

        try:
            response = client.get(normalized_sitemap, timeout=timeout_seconds)
        except Exception:
            continue

        if response.status_code != 200:
            continue

        root = _parse_xml_bytes(response.content)
        if root is None:
            continue

        root_name = _tag_local_name(root.tag)
        if root_name == "sitemapindex":
            if depth >= max_depth:
                continue
            for node in root.iter():
                if _tag_local_name(node.tag) != "loc":
                    continue
                loc = _normalize_url((node.text or "").strip())
                if loc:
                    queue.append((loc, depth + 1))
            continue

        if root_name != "urlset":
            continue

        for node in root.iter():
            if _tag_local_name(node.tag) != "loc":
                continue
            loc = _normalize_url((node.text or "").strip())
            if not loc:
                continue
            if not _is_url_in_scope(loc, scope):
                continue
            if robots_parser is not None and not _robots_is_allowed(robots_parser, user_agent, loc):
                continue
            if loc in seen_urls:
                continue
            seen_urls.add(loc)
            found_urls.append(loc)
            if len(found_urls) >= max_urls:
                break

        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)

    return found_urls


def _collect_link_fallback_urls(
    client: httpx.Client,
    scope: ScopeDefinition,
    robots_parser: RobotExclusionRulesParser | None,
    user_agent: str,
    timeout_seconds: int,
    request_delay_seconds: float,
    max_visits: int,
    max_urls: int,
    already_seen: set[str],
) -> list[str]:
    queue: deque[str] = deque([scope.seed_url])
    visited: set[str] = set()
    found: list[str] = []

    while queue and len(visited) < max_visits and len(already_seen) < max_urls:
        current = queue.popleft()
        normalized_current = _normalize_url(current)
        if not normalized_current:
            continue
        if normalized_current in visited:
            continue
        if not _is_url_in_scope(normalized_current, scope):
            continue

        visited.add(normalized_current)
        if robots_parser is not None and not _robots_is_allowed(
            robots_parser,
            user_agent,
            normalized_current,
        ):
            continue

        try:
            response = client.get(normalized_current, timeout=timeout_seconds)
        except Exception:
            continue

        if response.status_code != 200:
            continue

        if normalized_current not in already_seen:
            found.append(normalized_current)
            already_seen.add(normalized_current)

        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        for link in soup.find_all("a", href=True):
            candidate = _normalize_url(urljoin(normalized_current, link["href"]))
            if not candidate:
                continue
            if candidate in visited:
                continue
            if not _is_url_in_scope(candidate, scope):
                continue
            if robots_parser is not None and not _robots_is_allowed(
                robots_parser, user_agent, candidate
            ):
                continue
            queue.append(candidate)
            if candidate not in already_seen:
                found.append(candidate)
                already_seen.add(candidate)
                if len(already_seen) >= max_urls:
                    break

        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)

    return found


def _fetch_robots(
    client: httpx.Client,
    scope: ScopeDefinition,
    timeout_seconds: int,
) -> tuple[RobotExclusionRulesParser | None, str, str]:
    robots_url = scope.seed_origin + "/robots.txt"
    try:
        response = client.get(robots_url, timeout=timeout_seconds)
    except Exception as e:
        return None, "fetch_error", str(e)

    if response.status_code == 404:
        return None, "missing", ""
    if response.status_code != 200:
        return None, f"http_{response.status_code}", ""

    parser = RobotExclusionRulesParser()
    try:
        parser.parse(response.text)
    except Exception as e:
        return None, "parse_error", str(e)

    return parser, "ok", ""


def _prepare_single_org(
    org: dict,
    settings: Settings,
    org_id_column: str,
    name_column: str,
    website_column: str,
) -> tuple[dict, list[dict]]:
    prepared_at = _now_iso()
    org_id = str(org.get(org_id_column, "") or "").strip()
    org_name = str(org.get(name_column, "") or "").strip() or "Unknown"
    website_url = str(org.get(website_column, "") or "").strip()

    if not website_url:
        summary = {
            "_org_id": org_id,
            "_org_name": org_name,
            "_website_url": "",
            "_scrape_scope_mode": "",
            "_scrape_scope_path_prefix": "",
            "_scrape_robots_policy": "no_website",
            "_scrape_robots_fetch": "not_checked",
            "_scrape_allowed": False,
            "_scrape_prepared_url_count": 0,
            "_scrape_prep_status": "no_website",
            "_scrape_prep_error": "",
            "_scrape_prepared_at": prepared_at,
            "_scrape_targets_file": "",
        }
        return summary, []

    include_subdomains = bool(settings.scraping.prepare_include_subdomains)
    scope = _build_scope(website_url, include_subdomains)
    if scope is None:
        summary = {
            "_org_id": org_id,
            "_org_name": org_name,
            "_website_url": website_url,
            "_scrape_scope_mode": "",
            "_scrape_scope_path_prefix": "",
            "_scrape_robots_policy": "invalid_url",
            "_scrape_robots_fetch": "not_checked",
            "_scrape_allowed": False,
            "_scrape_prepared_url_count": 0,
            "_scrape_prep_status": "invalid_url",
            "_scrape_prep_error": "unsupported_or_invalid_url",
            "_scrape_prepared_at": prepared_at,
            "_scrape_targets_file": "",
        }
        return summary, []

    timeout = int(settings.scraping.timeout_seconds)
    delay = float(settings.scraping.request_delay_seconds)

    with httpx.Client(
        headers={"User-Agent": settings.scraping.user_agent},
        follow_redirects=True,
    ) as client:
        robots_parser: RobotExclusionRulesParser | None = None
        robots_fetch = "not_checked"
        robots_policy = "allowed"
        prep_error = ""
        allowed = True

        if settings.scraping.respect_robots_txt:
            robots_parser, robots_fetch, robots_error = _fetch_robots(client, scope, timeout)
            if robots_error:
                prep_error = robots_error
            if robots_parser is not None:
                allowed = _robots_is_allowed(
                    robots_parser, settings.scraping.user_agent, scope.seed_url
                )
                robots_policy = "allowed" if allowed else "blocked"
            elif robots_fetch in {"missing", "http_403", "http_401"}:
                robots_policy = "unknown"
            else:
                robots_policy = "unknown"

        if not allowed:
            summary = {
                "_org_id": org_id,
                "_org_name": org_name,
                "_website_url": scope.seed_url,
                "_scrape_scope_mode": scope.scope_mode,
                "_scrape_scope_path_prefix": scope.path_prefix,
                "_scrape_robots_policy": robots_policy,
                "_scrape_robots_fetch": robots_fetch,
                "_scrape_allowed": False,
                "_scrape_prepared_url_count": 0,
                "_scrape_prep_status": "blocked",
                "_scrape_prep_error": prep_error,
                "_scrape_prepared_at": prepared_at,
                "_scrape_targets_file": "",
            }
            return summary, []

        max_urls = int(settings.scraping.prepare_max_urls_per_org)
        sitemap_urls = _collect_sitemap_urls(
            client=client,
            scope=scope,
            robots_parser=robots_parser,
            user_agent=settings.scraping.user_agent,
            timeout_seconds=timeout,
            request_delay_seconds=delay,
            max_sitemaps=int(settings.scraping.prepare_sitemap_max_files),
            max_depth=int(settings.scraping.prepare_sitemap_max_depth),
            max_urls=max_urls,
        )

        discovered_map: dict[str, str] = {url: "sitemap" for url in sitemap_urls}
        fallback_urls = _collect_link_fallback_urls(
            client=client,
            scope=scope,
            robots_parser=robots_parser,
            user_agent=settings.scraping.user_agent,
            timeout_seconds=timeout,
            request_delay_seconds=delay,
            max_visits=int(settings.scraping.prepare_fallback_max_visits),
            max_urls=max_urls,
            already_seen=set(discovered_map.keys()),
        )

        for url in fallback_urls:
            if url in discovered_map:
                if discovered_map[url] == "sitemap":
                    discovered_map[url] = "sitemap+links"
                continue
            discovered_map[url] = "links"

    ordered_urls = list(discovered_map.keys())
    truncated = False
    if len(ordered_urls) > max_urls:
        ordered_urls = ordered_urls[:max_urls]
        truncated = True

    prep_status = "ready" if ordered_urls else "no_urls"
    summary = {
        "_org_id": org_id,
        "_org_name": org_name,
        "_website_url": scope.seed_url,
        "_scrape_scope_mode": scope.scope_mode,
        "_scrape_scope_path_prefix": scope.path_prefix,
        "_scrape_robots_policy": robots_policy,
        "_scrape_robots_fetch": robots_fetch,
        "_scrape_allowed": True,
        "_scrape_prepared_url_count": len(ordered_urls),
        "_scrape_prep_status": prep_status,
        "_scrape_prep_error": "url_limit_truncated" if truncated else prep_error,
        "_scrape_prepared_at": prepared_at,
        "_scrape_targets_file": "",
    }

    targets: list[dict] = []
    for order, url in enumerate(ordered_urls, start=1):
        targets.append(
            {
                "_prepared_url": url,
                "_prepared_url_source": discovered_map.get(url, "unknown"),
                "_prepared_url_order": order,
            }
        )

    return summary, targets


SUMMARY_COLUMNS = [
    "_org_id",
    "_org_name",
    "_website_url",
    "_scrape_scope_mode",
    "_scrape_scope_path_prefix",
    "_scrape_robots_policy",
    "_scrape_robots_fetch",
    "_scrape_allowed",
    "_scrape_prepared_url_count",
    "_scrape_prep_status",
    "_scrape_prep_error",
    "_scrape_prepared_at",
    "_scrape_targets_file",
]

TARGET_COLUMNS = [
    "_prepared_url_order",
    "_prepared_url_source",
    "_prepared_url",
]


def targets_file_for_org(org_id: str) -> Path:
    """Return per-organization targets CSV path under data/orgs/<org_id>/scrape_prep/."""
    return DATA_DIR / "orgs" / org_id / "scrape_prep" / "sitemap_urls.csv"


def _write_csv_atomic(df, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(output_path)


def write_org_targets(org_id: str, targets: list[dict]) -> Path:
    """Persist one org's prepared URL list as CSV and return path."""
    import pandas as pd

    target_path = targets_file_for_org(org_id)
    target_df = pd.DataFrame(targets, columns=TARGET_COLUMNS)
    _write_csv_atomic(target_df, target_path)
    return target_path


def load_org_targets(targets_file: Path) -> list[str]:
    """Load prepared URLs from a per-organization target CSV."""
    import pandas as pd

    if not targets_file.exists():
        return []
    try:
        df = pd.read_csv(targets_file, encoding="utf-8-sig")
    except Exception:
        return []
    if "_prepared_url" not in df.columns:
        return []
    return [str(url).strip() for url in df["_prepared_url"].tolist() if str(url).strip()]


def load_prepare_summary(summary_path: Path) -> tuple[list[dict], set[str]]:
    """Load existing summary rows and return rows + known _org_id set."""
    import pandas as pd

    if not summary_path.exists():
        return [], set()

    try:
        existing_df = pd.read_csv(summary_path, encoding="utf-8-sig")
    except Exception:
        return [], set()

    if existing_df.empty or "_org_id" not in existing_df.columns:
        return [], set()

    existing_df = existing_df.drop_duplicates(subset="_org_id", keep="last")
    for column in SUMMARY_COLUMNS:
        if column not in existing_df.columns:
            existing_df[column] = ""
    existing_df = existing_df[SUMMARY_COLUMNS]
    rows = existing_df.to_dict("records")
    org_ids = {
        str(row.get("_org_id", "")).strip() for row in rows if str(row.get("_org_id", "")).strip()
    }
    return rows, org_ids


class PrepareCheckpointWriter:
    """Thread-safe checkpoint writer for prepare-scraping results."""

    def __init__(self, summary_path: Path, existing_rows: list[dict] | None = None) -> None:
        self.summary_path = summary_path
        self._rows_by_org_id: dict[str, dict] = {}
        if existing_rows:
            for row in existing_rows:
                org_id = str(row.get("_org_id", "")).strip()
                if org_id:
                    normalized = {column: row.get(column, "") for column in SUMMARY_COLUMNS}
                    self._rows_by_org_id[org_id] = normalized

    def upsert(self, summary: dict, targets: list[dict]) -> Path:
        """Persist one org's targets and summary row immediately."""
        import pandas as pd

        org_id = str(summary.get("_org_id", "")).strip()
        if not org_id:
            raise ValueError("Missing _org_id in prepare summary row")

        targets_path = write_org_targets(org_id, targets)
        summary["_scrape_targets_file"] = str(targets_path)

        normalized = {column: summary.get(column, "") for column in SUMMARY_COLUMNS}
        self._rows_by_org_id[org_id] = normalized

        output_rows = list(self._rows_by_org_id.values())
        summary_df = pd.DataFrame(output_rows, columns=SUMMARY_COLUMNS)
        _write_csv_atomic(summary_df, self.summary_path)
        return targets_path

    def rows(self) -> list[dict]:
        return list(self._rows_by_org_id.values())


def prepare_scraping_batch(
    organizations: list[dict],
    settings: Settings,
    *,
    org_id_column: str = "_org_id",
    name_column: str = "Bezeichnung",
    website_column: str = "_website_url",
    on_result=None,
) -> list[dict]:
    """Prepare scraping metadata for a batch and stream each result via callback."""
    if not organizations:
        return []

    total = len(organizations)
    max_workers = max(1, int(settings.scraping.prepare_max_workers))
    summaries: list[dict] = []

    if max_workers == 1 or total == 1:
        for index, org in enumerate(organizations, start=1):
            summary, org_targets = _prepare_single_org(
                org,
                settings,
                org_id_column=org_id_column,
                name_column=name_column,
                website_column=website_column,
            )
            if on_result is not None:
                on_result(summary, org_targets)
            summaries.append(summary)
            logger.info(
                "[%d/%d] prepared %s (%s, %d urls)",
                index,
                total,
                summary.get("_org_name", "Unknown"),
                summary.get("_scrape_prep_status", "unknown"),
                summary.get("_scrape_prepared_url_count", 0),
            )
        return summaries

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _prepare_single_org,
                org,
                settings,
                org_id_column,
                name_column,
                website_column,
            )
            for org in organizations
        ]

        completed = 0
        for future in as_completed(futures):
            summary, org_targets = future.result()
            if on_result is not None:
                on_result(summary, org_targets)
            summaries.append(summary)
            completed += 1
            logger.info(
                "[%d/%d] prepared %s (%s, %d urls)",
                completed,
                total,
                summary.get("_org_name", "Unknown"),
                summary.get("_scrape_prep_status", "unknown"),
                summary.get("_scrape_prepared_url_count", 0),
            )

    return summaries
