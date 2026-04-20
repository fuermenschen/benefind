"""Prepare scraping targets with robots-aware, org-scoped URL discovery.

Builds a per-organization scraping plan before actual page scraping:
- determines robots.txt policy status for the organization website
- normalizes the discovered website URL into the most plausible site base
- discovers in-scope URLs sitemap-first, then local-link fallback when needed
- scores and ranks discovered URLs so descriptive pages win over noisy listings
"""

from __future__ import annotations

import gzip
import logging
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote_plus, urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup
from robotexclusionrulesparser import RobotExclusionRulesParser

from benefind.config import DATA_DIR, Settings, load_url_scoring_config

logger = logging.getLogger(__name__)

LANGUAGE_SEGMENTS = {"de", "en", "fr", "it"}
GENERIC_ROOT_SEGMENTS = {"home", "homepage", "index", "start", "startseite"}
URL_SCORING = load_url_scoring_config()
FAVOR_TOKENS = set(URL_SCORING.favor_tokens)
FAVOR_REGEXES = tuple(URL_SCORING.favor_regexes)
PENALIZE_TOKENS = set(URL_SCORING.penalize_tokens)
PENALIZE_REGEXES = tuple(URL_SCORING.penalize_regexes)
EXCLUDE_TOKENS = set(URL_SCORING.exclude_tokens)
EXCLUDE_REGEXES = tuple(URL_SCORING.exclude_regexes)
TECHNICAL_ROOT_SEGMENTS = set(URL_SCORING.technical_root_segments)
CMS_SCAFFOLD_SEGMENTS = set(URL_SCORING.cms_scaffold_segments)
RAW_TECHNICAL_SEGMENT_PAIRS = URL_SCORING.technical_segment_pairs
NON_HTML_EXTENSIONS = set(URL_SCORING.non_html_extensions)
SCOPE_BOUNDARY_TOKENS = FAVOR_TOKENS | PENALIZE_TOKENS
PAGE_LIKE_SUFFIXES = (".html", ".htm", ".php", ".aspx", ".asp", ".jsp")


@dataclass
class PreparedUrlCandidate:
    url: str
    source: str
    priority: float | None = None
    lastmod: str = ""


@dataclass
class ScopeDefinition:
    seed_original_url: str
    seed_url: str
    seed_origin: str
    seed_host: str
    scope_mode: str
    path_prefix: str
    include_subdomains: bool
    scope_reason: str


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


def _url_identity_key(url: str) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    parsed = urlsplit(normalized)
    host = _normalize_host(parsed.netloc)
    path = parsed.path or "/"
    return f"{host}{path}"


def _prefer_https_url(first: str, second: str) -> str:
    first_url = _normalize_url(first)
    second_url = _normalize_url(second)
    if not first_url:
        return second_url
    if not second_url:
        return first_url

    first_scheme = urlsplit(first_url).scheme.lower()
    second_scheme = urlsplit(second_url).scheme.lower()
    if second_scheme == "https" and first_scheme != "https":
        return second_url
    return first_url


def _normalize_host(host: str) -> str:
    return host.lower().strip().split(":", 1)[0]


def _toggle_www_netloc(netloc: str) -> str:
    parsed = urlsplit(f"http://{netloc}")
    host = (parsed.hostname or "").strip()
    if not host:
        return ""

    if host.lower().startswith("www."):
        toggled_host = host[4:]
    else:
        toggled_host = f"www.{host}"

    if parsed.port is not None:
        return f"{toggled_host}:{parsed.port}"
    return toggled_host


def _build_seed_probe_candidates(seed_url: str) -> list[str]:
    normalized = _normalize_url(seed_url)
    if not normalized:
        return []

    parsed = urlsplit(normalized)
    host = parsed.netloc
    path = parsed.path or "/"

    candidates: list[str] = []
    for scheme in (parsed.scheme.lower(), "https", "http"):
        if scheme not in {"https", "http"}:
            continue
        candidates.append(urlunsplit((scheme, host, path, "", "")))

    toggled_netloc = _toggle_www_netloc(host)
    if toggled_netloc:
        for scheme in ("https", "http"):
            candidates.append(urlunsplit((scheme, toggled_netloc, path, "", "")))

    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized_candidate = _normalize_url(candidate)
        if not normalized_candidate or normalized_candidate in seen:
            continue
        seen.add(normalized_candidate)
        unique.append(normalized_candidate)
    return unique


def _probe_seed_candidate(
    client: httpx.Client,
    url: str,
    timeout_seconds: int,
) -> tuple[bool, str, str]:
    try:
        response = client.get(url, timeout=timeout_seconds)
    except Exception as e:
        return False, "", f"{type(e).__name__}: {e}"

    final_url = _normalize_url(str(response.url))
    status = int(response.status_code)
    if status < 500:
        return True, final_url, f"http_{status}"
    return False, final_url, f"http_{status}"


def _resolve_reachable_scope(
    client: httpx.Client,
    scope: ScopeDefinition,
    timeout_seconds: int,
) -> tuple[ScopeDefinition | None, str]:
    candidates = _build_seed_probe_candidates(scope.seed_url)
    if not candidates:
        return None, "seed_unreachable:no_probe_candidates"

    errors: list[str] = []
    for candidate in candidates:
        ok, final_url, status_note = _probe_seed_candidate(client, candidate, timeout_seconds)
        if not ok:
            errors.append(f"{candidate} ({status_note})")
            continue

        resolved_from = final_url or candidate
        resolved_scope = _build_scope(resolved_from, scope.include_subdomains)
        if resolved_scope is not None:
            return resolved_scope, ""

        errors.append(f"{candidate} (invalid_resolved_url)")

    preview = "; ".join(errors[:3]) if errors else "all_probe_attempts_failed"
    return None, f"seed_unreachable:{preview}"


def _normalize_text(value: str) -> str:
    normalized = unquote_plus((value or "").lower().strip())
    return (
        normalized.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )


def _canonicalize_segment(value: str) -> str:
    normalized = _normalize_text(value).replace("_", "-")
    normalized = re.sub(r"\s+", "-", normalized)
    for suffix in PAGE_LIKE_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized.strip("-")


def _segment_tokens(value: str) -> list[str]:
    normalized = _canonicalize_segment(value)
    return [token for token in re.split(r"[^a-z0-9]+", normalized) if token]


def _canonicalize_segment_pair_rules(
    pairs: list[list[str]] | list[tuple[str, str]],
) -> set[tuple[str, str]]:
    normalized_pairs: set[tuple[str, str]] = set()
    for pair in pairs:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        first = _canonicalize_segment(str(pair[0]))
        second = _canonicalize_segment(str(pair[1]))
        if first and second:
            normalized_pairs.add((first, second))
    return normalized_pairs


TECHNICAL_SEGMENT_PAIRS = _canonicalize_segment_pair_rules(RAW_TECHNICAL_SEGMENT_PAIRS)


def _path_segments(url: str) -> list[str]:
    parsed = urlsplit(url)
    return [segment for segment in (parsed.path or "/").split("/") if segment]


def _path_depth(url: str) -> int:
    return len(_path_segments(url))


def _extract_priority(raw_value: str) -> float | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    try:
        priority = float(value)
    except ValueError:
        return None
    return max(0.0, min(priority, 1.0))


def _parse_lastmod(raw_value: str) -> datetime | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(value[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _segment_matches(segment: str, tokens: set[str]) -> bool:
    segment_parts = set(_segment_tokens(segment))
    if not segment_parts:
        return False
    return bool(segment_parts & tokens)


def _regex_matches(value: str, patterns: tuple[str, ...]) -> list[str]:
    return [pattern for pattern in patterns if re.match(pattern, value)]


def _is_scaffold_only_path(segments: list[str]) -> bool:
    """Return True if every segment is a CMS scaffold or language prefix."""
    allowed = CMS_SCAFFOLD_SEGMENTS | LANGUAGE_SEGMENTS
    return all(_canonicalize_segment(s) in allowed for s in segments)


def _first_boundary_index(segments: list[str]) -> int | None:
    for index, segment in enumerate(segments):
        if _segment_matches(segment, SCOPE_BOUNDARY_TOKENS):
            return index
    return None


def _section_bucket(url: str) -> str:
    segments = _path_segments(url)
    if not segments:
        return "root"
    normalized = [_canonicalize_segment(segment) for segment in segments]
    if normalized[0] in LANGUAGE_SEGMENTS and len(normalized) > 1:
        return normalized[1]
    return normalized[0]


def _is_id_like_segment(segment: str) -> bool:
    normalized = _canonicalize_segment(segment)
    if re.fullmatch(r"[a-f0-9]{10,}", normalized):
        return True
    if re.fullmatch(r"[a-z0-9]{20,}", normalized):
        return True
    return False


def _looks_like_hostname_segment(segment: str) -> bool:
    normalized = _normalize_text(segment)
    return bool(re.fullmatch(r"(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+", normalized))


def _priority_bonus(priority: float | None) -> int:
    if priority is None:
        return 0
    return int(round((priority - 0.5) * 6))


def _technical_exclusion_reason(url: str) -> str:
    parsed = urlsplit(url)
    path = _normalize_text(parsed.path)
    for extension in NON_HTML_EXTENSIONS:
        if path.endswith(extension):
            return f"non_html_extension:{extension}"

    segments = [_canonicalize_segment(segment) for segment in _path_segments(url)]
    if not segments:
        return ""
    normalized_path = "/".join(segments)
    for segment in segments:
        if _looks_like_hostname_segment(segment):
            return f"embedded_hostname:{segment}"
        exclude_hits = sorted(set(_segment_tokens(segment)) & EXCLUDE_TOKENS)
        if exclude_hits:
            return f"exclude_tokens:{'|'.join(exclude_hits)}"
        regex_matches = _regex_matches(segment, EXCLUDE_REGEXES)
        if regex_matches:
            return f"exclude_regex:{segment}"
    path_matches = _regex_matches(normalized_path, EXCLUDE_REGEXES)
    if path_matches:
        return f"exclude_regex_path:{normalized_path}"
    if segments[0] in TECHNICAL_ROOT_SEGMENTS:
        return f"technical_root:{segments[0]}"
    if len(segments) >= 2 and (segments[0], segments[1]) in TECHNICAL_SEGMENT_PAIRS:
        return f"technical_pair:{segments[0]}/{segments[1]}"
    return ""


def _score_candidate(
    candidate: PreparedUrlCandidate,
    scope: ScopeDefinition,
    stale_cutoff: datetime,
) -> tuple[int, list[str], int, str]:
    score = 0
    reasons: list[str] = []
    segments = _path_segments(candidate.url)
    normalized_segments = [_canonicalize_segment(segment) for segment in segments]
    tokens: set[str] = set()
    for segment in segments:
        tokens.update(_segment_tokens(segment))

    depth = len(segments)
    section = _section_bucket(candidate.url)

    if candidate.url == scope.seed_url:
        score += 20
        reasons.append("normalized_seed")
    elif depth == 0:
        score += 18
        reasons.append("homepage")
    elif depth == 1 and normalized_segments[0] in LANGUAGE_SEGMENTS:
        score += 10
        reasons.append("language_root")

    favor_hits = sorted(tokens & FAVOR_TOKENS)
    if favor_hits:
        favor_bonus = min(24, 6 * len(favor_hits))
        score += favor_bonus
        reasons.append(f"favor_tokens:{'|'.join(favor_hits)}")

    regex_favor_matches: list[str] = []
    for index, segment in enumerate(normalized_segments):
        if segment in GENERIC_ROOT_SEGMENTS and index > 0:
            continue
        regex_favor_matches.extend(_regex_matches(segment, FAVOR_REGEXES))
    if regex_favor_matches:
        regex_bonus = min(12, 6 * len(set(regex_favor_matches)))
        score += regex_bonus
        reasons.append("favor_regex")

    penalty = 0
    penalize_hits = sorted(tokens & PENALIZE_TOKENS)
    if penalize_hits:
        penalty -= min(20, 7 * len(penalize_hits))
        reasons.append(f"penalize_tokens:{'|'.join(penalize_hits)}")

    regex_penalize_matches: list[str] = []
    for segment in normalized_segments:
        regex_penalize_matches.extend(_regex_matches(segment, PENALIZE_REGEXES))
    if regex_penalize_matches:
        penalty -= min(10, 5 * len(set(regex_penalize_matches)))
        reasons.append("penalize_regex")

    if re.search(r"/(20\d{2}|19\d{2})/", urlsplit(candidate.url).path):
        penalty -= 5
        reasons.append("penalize_archive_year")

    if penalty < -20:
        penalty = -20
    score += penalty

    if depth == 3:
        score -= 2
        reasons.append("depth:3")
    elif depth == 4:
        score -= 5
        reasons.append("depth:4")
    elif depth == 5:
        score -= 9
        reasons.append("depth:5")
    elif depth >= 6:
        score -= 14
        reasons.append("depth:6+")

    long_segment_count = sum(1 for segment in normalized_segments if len(segment) >= 40)
    if long_segment_count:
        segment_penalty = min(6, long_segment_count * 2)
        score -= segment_penalty
        reasons.append(f"long_segments:{long_segment_count}")

    id_like_count = sum(1 for segment in normalized_segments if _is_id_like_segment(segment))
    if id_like_count:
        id_penalty = min(8, id_like_count * 4)
        score -= id_penalty
        reasons.append(f"id_like_segments:{id_like_count}")

    priority_bonus = _priority_bonus(candidate.priority)
    if priority_bonus:
        score += priority_bonus
        reasons.append(f"priority:{candidate.priority:.2f}")

    lastmod_dt = _parse_lastmod(candidate.lastmod)
    if lastmod_dt is not None and lastmod_dt < stale_cutoff:
        score -= 2
        reasons.append("lastmod_stale")

    return score, reasons, depth, section


def _count_rankable_candidates(candidates: dict[str, PreparedUrlCandidate]) -> int:
    return sum(
        1
        for candidate in candidates.values()
        if not _technical_exclusion_reason(candidate.url)
    )


def _rank_candidates(
    candidates: dict[str, PreparedUrlCandidate],
    scope: ScopeDefinition,
    settings: Settings,
) -> tuple[list[dict], int, int]:
    keep_limit = max(1, int(settings.scraping.prepare_keep_ranked_urls_per_org))
    section_cap = max(1, int(settings.scraping.prepare_section_cap_per_org))
    stale_cutoff = datetime.now(UTC) - timedelta(
        days=int(settings.scraping.prepare_stale_sitemap_days)
    )

    excluded_count = 0
    ranked: list[dict] = []
    for candidate in candidates.values():
        exclusion_reason = _technical_exclusion_reason(candidate.url)
        if exclusion_reason:
            excluded_count += 1
            continue

        score, reasons, depth, section = _score_candidate(candidate, scope, stale_cutoff)
        ranked.append(
            {
                "_prepared_url": candidate.url,
                "_prepared_url_source": candidate.source,
                "_prepared_url_score": score,
                "_prepared_url_decision": "keep",
                "_prepared_url_reasons": " | ".join(reasons),
                "_prepared_url_depth": depth,
                "_prepared_url_priority": "" if candidate.priority is None else candidate.priority,
                "_prepared_url_lastmod": candidate.lastmod,
                "_prepared_url_section": section,
            }
        )

    ranked.sort(
        key=lambda row: (
            -int(row["_prepared_url_score"]),
            int(row["_prepared_url_depth"]),
            len(urlsplit(str(row["_prepared_url"])).path),
            str(row["_prepared_url"]),
        )
    )

    selected: list[dict] = []
    deferred: list[dict] = []
    section_counts: Counter[str] = Counter()
    for row in ranked:
        section = str(row["_prepared_url_section"])
        if section_counts[section] >= section_cap:
            deferred.append(row)
            continue
        section_counts[section] += 1
        selected.append(row)
        if len(selected) >= keep_limit:
            break

    if len(selected) < keep_limit:
        for row in deferred:
            row["_prepared_url_decision"] = "keep_after_section_cap"
            selected.append(row)
            if len(selected) >= keep_limit:
                break

    for order, row in enumerate(selected, start=1):
        row["_prepared_url_order"] = order

    return selected, len(candidates), excluded_count


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


def _url_with_path(parsed, path: str) -> str:
    normalized_path = path if path.startswith("/") else "/" + path
    normalized_path = normalized_path or "/"
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, "", ""))


def _build_scope(seed_url: str, include_subdomains: bool) -> ScopeDefinition | None:
    normalized = _normalize_url(seed_url)
    if not normalized:
        return None

    parsed = urlsplit(normalized)
    seed_host = _normalize_host(parsed.netloc)
    path = parsed.path or "/"
    segments = [segment for segment in path.split("/") if segment]
    normalized_path = "/"
    scope_reason = "root_seed"

    if segments:
        boundary_index = _first_boundary_index(segments)
        last_segment = segments[-1]
        last_is_file = "." in last_segment
        first_segment = _normalize_text(segments[0])

        # Do not promote multi-segment paths to host root when the boundary
        # token is already the first segment (e.g. /organisation/<slug>). On
        # shared hosts this would broaden scope too aggressively.
        if boundary_index is not None and not (boundary_index == 0 and len(segments) > 1):
            boundary_segment = _normalize_text(segments[boundary_index])
            parent_segments = segments[:boundary_index]
            if parent_segments:
                normalized_path = "/" + "/".join(parent_segments) + "/"
                scope_reason = f"promoted_before_boundary:{boundary_segment}"
            else:
                normalized_path = "/"
                scope_reason = f"promoted_to_host_root:{boundary_segment}"
        elif len(segments) == 1 and first_segment in LANGUAGE_SEGMENTS:
            normalized_path = "/" + segments[0] + "/"
            scope_reason = "language_root_seed"
        elif len(segments) == 1:
            normalized_path = "/"
            scope_reason = "single_leaf_promoted_to_host_root"
        elif last_is_file:
            parent_segments = segments[:-1]
            if parent_segments:
                normalized_path = "/" + "/".join(parent_segments) + "/"
                scope_reason = "file_leaf_promoted_to_parent"
            else:
                normalized_path = "/"
                scope_reason = "file_leaf_promoted_to_host_root"
        else:
            normalized_path = "/" + "/".join(segments) + "/"
            scope_reason = "kept_path_prefix"

        # Second pass: if the remaining prefix consists entirely of CMS
        # scaffold / language segments, it carries no org-specific meaning
        # and we can safely promote to host root.
        if normalized_path != "/":
            remaining = [s for s in normalized_path.split("/") if s]
            if remaining and _is_scaffold_only_path(remaining):
                scope_reason = f"scaffold_promoted_to_host_root:{scope_reason}"
                normalized_path = "/"

    normalized_seed = _url_with_path(parsed, normalized_path)
    if normalized_path == "/":
        scope_mode = "host"
        path_prefix = "/"
    else:
        scope_mode = "path_prefix"
        path_prefix = normalized_path

    return ScopeDefinition(
        seed_original_url=normalized,
        seed_url=normalized_seed,
        seed_origin=f"{parsed.scheme}://{parsed.netloc}",
        seed_host=seed_host,
        scope_mode=scope_mode,
        path_prefix=path_prefix,
        include_subdomains=include_subdomains,
        scope_reason=scope_reason,
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
) -> list[PreparedUrlCandidate]:
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
    found_urls: list[PreparedUrlCandidate] = []
    seen_url_keys: set[str] = set()

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
            for node in root:
                if _tag_local_name(node.tag) != "sitemap":
                    continue
                loc = ""
                for child in node:
                    if _tag_local_name(child.tag) != "loc":
                        continue
                    loc = _normalize_url((child.text or "").strip())
                    break
                if loc:
                    queue.append((loc, depth + 1))
            continue

        if root_name != "urlset":
            continue

        for node in root:
            if _tag_local_name(node.tag) != "url":
                continue
            loc = ""
            lastmod = ""
            priority = None
            for child in node:
                child_name = _tag_local_name(child.tag)
                if child_name == "loc":
                    loc = _normalize_url((child.text or "").strip())
                elif child_name == "lastmod":
                    lastmod = (child.text or "").strip()
                elif child_name == "priority":
                    priority = _extract_priority((child.text or "").strip())
            if not loc:
                continue
            if not _is_url_in_scope(loc, scope):
                continue
            if robots_parser is not None and not _robots_is_allowed(robots_parser, user_agent, loc):
                continue
            loc_key = _url_identity_key(loc)
            if not loc_key:
                continue
            if loc_key in seen_url_keys:
                continue
            seen_url_keys.add(loc_key)
            found_urls.append(
                PreparedUrlCandidate(
                    url=loc,
                    source="sitemap",
                    priority=priority,
                    lastmod=lastmod,
                )
            )
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
    already_seen_keys: set[str],
) -> list[PreparedUrlCandidate]:
    queue: deque[str] = deque([scope.seed_url])
    visited_keys: set[str] = set()
    found: list[PreparedUrlCandidate] = []

    while queue and len(visited_keys) < max_visits and len(already_seen_keys) < max_urls:
        current = queue.popleft()
        normalized_current = _normalize_url(current)
        if not normalized_current:
            continue
        current_key = _url_identity_key(normalized_current)
        if not current_key:
            continue
        if current_key in visited_keys:
            continue
        if not _is_url_in_scope(normalized_current, scope):
            continue

        visited_keys.add(current_key)
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

        if current_key not in already_seen_keys:
            found.append(PreparedUrlCandidate(url=normalized_current, source="links"))
            already_seen_keys.add(current_key)

        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        for link in soup.find_all("a", href=True):
            candidate = _normalize_url(urljoin(normalized_current, link["href"]))
            if not candidate:
                continue
            candidate_key = _url_identity_key(candidate)
            if not candidate_key:
                continue
            if candidate_key in visited_keys:
                continue
            if not _is_url_in_scope(candidate, scope):
                continue
            if robots_parser is not None and not _robots_is_allowed(
                robots_parser, user_agent, candidate
            ):
                continue
            queue.append(candidate)
            if candidate_key not in already_seen_keys:
                found.append(PreparedUrlCandidate(url=candidate, source="links"))
                already_seen_keys.add(candidate_key)
                if len(already_seen_keys) >= max_urls:
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


def _merge_candidate_source(existing: str, incoming: str) -> str:
    existing_parts = set(filter(None, str(existing or "").split("+")))
    incoming_parts = set(filter(None, str(incoming or "").split("+")))
    merged_parts = existing_parts | incoming_parts
    ordered = [label for label in ("seed", "sitemap", "links") if label in merged_parts]
    return "+".join(ordered) if ordered else "unknown"


def _merge_candidate(
    discovered: dict[str, PreparedUrlCandidate],
    candidate: PreparedUrlCandidate,
) -> None:
    normalized_url = _normalize_url(candidate.url)
    key = _url_identity_key(normalized_url)
    if not key:
        return

    existing = discovered.get(key)
    if existing is None:
        discovered[key] = PreparedUrlCandidate(
            url=normalized_url,
            source=candidate.source,
            priority=candidate.priority,
            lastmod=candidate.lastmod,
        )
        return

    existing.url = _prefer_https_url(existing.url, normalized_url)

    if candidate.priority is not None and existing.priority is None:
        existing.priority = candidate.priority

    existing_lastmod = _parse_lastmod(existing.lastmod)
    candidate_lastmod = _parse_lastmod(candidate.lastmod)
    if existing_lastmod is None and candidate_lastmod is not None:
        existing.lastmod = candidate.lastmod
    elif candidate.lastmod and (existing_lastmod is None or candidate_lastmod is None):
        if not existing.lastmod:
            existing.lastmod = candidate.lastmod
    elif candidate_lastmod is not None and (
        existing_lastmod is None or candidate_lastmod > existing_lastmod
    ):
        existing.lastmod = candidate.lastmod

    existing.source = _merge_candidate_source(existing.source, candidate.source)


def _latest_lastmod(candidates: list[PreparedUrlCandidate]) -> datetime | None:
    timestamps = [_parse_lastmod(candidate.lastmod) for candidate in candidates]
    valid = [timestamp for timestamp in timestamps if timestamp is not None]
    return max(valid) if valid else None


def _make_prepare_summary(
    *,
    org_id: str,
    org_name: str,
    website_url: str,
    prepared_at: str,
    scope: ScopeDefinition | None,
    robots_policy: str,
    robots_fetch: str,
    allowed: bool,
    prep_status: str,
    prep_error: str,
    candidate_count: int = 0,
    excluded_count: int = 0,
    kept_count: int = 0,
    sitemap_stale: bool = False,
) -> dict:
    return {
        "_org_id": org_id,
        "_org_name": org_name,
        "_website_url": website_url,
        "_scrape_seed_original": "" if scope is None else scope.seed_original_url,
        "_scrape_seed_normalized": "" if scope is None else scope.seed_url,
        "_scrape_scope_mode": "" if scope is None else scope.scope_mode,
        "_scrape_scope_path_prefix": "" if scope is None else scope.path_prefix,
        "_scrape_scope_reason": "" if scope is None else scope.scope_reason,
        "_scrape_robots_policy": robots_policy,
        "_scrape_robots_fetch": robots_fetch,
        "_scrape_allowed": allowed,
        "_scrape_sitemap_stale": sitemap_stale,
        "_scrape_prepared_candidate_count": candidate_count,
        "_scrape_prepared_excluded_count": excluded_count,
        "_scrape_prepared_kept_count": kept_count,
        "_scrape_prepared_url_count": kept_count,
        "_scrape_prep_status": prep_status,
        "_scrape_prep_error": prep_error,
        "_scrape_prepared_at": prepared_at,
        "_scrape_targets_file": "",
    }


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
        summary = _make_prepare_summary(
            org_id=org_id,
            org_name=org_name,
            website_url="",
            prepared_at=prepared_at,
            scope=None,
            robots_policy="no_website",
            robots_fetch="not_checked",
            allowed=False,
            prep_status="no_website",
            prep_error="",
        )
        return summary, []

    include_subdomains = bool(settings.scraping.prepare_include_subdomains)
    scope = _build_scope(website_url, include_subdomains)
    if scope is None:
        summary = _make_prepare_summary(
            org_id=org_id,
            org_name=org_name,
            website_url=website_url,
            prepared_at=prepared_at,
            scope=None,
            robots_policy="invalid_url",
            robots_fetch="not_checked",
            allowed=False,
            prep_status="invalid_url",
            prep_error="unsupported_or_invalid_url",
        )
        return summary, []

    timeout = int(settings.scraping.timeout_seconds)
    delay = float(settings.scraping.request_delay_seconds)

    with httpx.Client(
        headers={"User-Agent": settings.scraping.user_agent},
        follow_redirects=True,
    ) as client:
        resolved_scope, seed_probe_error = _resolve_reachable_scope(client, scope, timeout)
        if resolved_scope is None:
            summary = _make_prepare_summary(
                org_id=org_id,
                org_name=org_name,
                website_url=website_url,
                prepared_at=prepared_at,
                scope=scope,
                robots_policy="unknown",
                robots_fetch="seed_unreachable",
                allowed=True,
                prep_status="no_urls",
                prep_error=seed_probe_error,
            )
            return summary, []

        scope = resolved_scope
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
            summary = _make_prepare_summary(
                org_id=org_id,
                org_name=org_name,
                website_url=website_url,
                prepared_at=prepared_at,
                scope=scope,
                robots_policy=robots_policy,
                robots_fetch=robots_fetch,
                allowed=False,
                prep_status="blocked",
                prep_error=prep_error,
            )
            return summary, []

        max_discovery_urls = max(1, int(settings.scraping.prepare_discovery_safety_cap))
        sitemap_urls = _collect_sitemap_urls(
            client=client,
            scope=scope,
            robots_parser=robots_parser,
            user_agent=settings.scraping.user_agent,
            timeout_seconds=timeout,
            request_delay_seconds=delay,
            max_sitemaps=int(settings.scraping.prepare_sitemap_max_files),
            max_depth=int(settings.scraping.prepare_sitemap_max_depth),
            max_urls=max_discovery_urls,
        )

        discovered: dict[str, PreparedUrlCandidate] = {}
        _merge_candidate(discovered, PreparedUrlCandidate(url=scope.seed_url, source="seed"))
        for candidate in sitemap_urls:
            _merge_candidate(discovered, candidate)

        latest_sitemap_lastmod = _latest_lastmod(sitemap_urls)
        stale_cutoff = datetime.now(UTC) - timedelta(
            days=int(settings.scraping.prepare_stale_sitemap_days)
        )
        sitemap_stale = latest_sitemap_lastmod is None or latest_sitemap_lastmod < stale_cutoff
        rankable_discovered = _count_rankable_candidates(discovered)
        run_link_fallback = (
            not sitemap_urls
            or sitemap_stale
            or rankable_discovered < int(settings.scraping.prepare_keep_ranked_urls_per_org)
        )
        if run_link_fallback:
            fallback_urls = _collect_link_fallback_urls(
                client=client,
                scope=scope,
                robots_parser=robots_parser,
                user_agent=settings.scraping.user_agent,
                timeout_seconds=timeout,
                request_delay_seconds=delay,
                max_visits=int(settings.scraping.prepare_fallback_max_visits),
                max_urls=max_discovery_urls,
                already_seen_keys=set(discovered.keys()),
            )
            for candidate in fallback_urls:
                _merge_candidate(discovered, candidate)
        else:
            sitemap_stale = False

    targets, candidate_count, excluded_count = _rank_candidates(discovered, scope, settings)
    prep_status = "ready" if targets else "no_urls"
    summary = _make_prepare_summary(
        org_id=org_id,
        org_name=org_name,
        website_url=website_url,
        prepared_at=prepared_at,
        scope=scope,
        robots_policy=robots_policy,
        robots_fetch=robots_fetch,
        allowed=True,
        prep_status=prep_status,
        prep_error=prep_error,
        candidate_count=candidate_count,
        excluded_count=excluded_count,
        kept_count=len(targets),
        sitemap_stale=sitemap_stale,
    )

    return summary, targets


SUMMARY_COLUMNS = [
    "_org_id",
    "_org_name",
    "_website_url",
    "_scrape_seed_original",
    "_scrape_seed_normalized",
    "_scrape_scope_mode",
    "_scrape_scope_path_prefix",
    "_scrape_scope_reason",
    "_scrape_robots_policy",
    "_scrape_robots_fetch",
    "_scrape_allowed",
    "_scrape_sitemap_stale",
    "_scrape_prepared_candidate_count",
    "_scrape_prepared_excluded_count",
    "_scrape_prepared_kept_count",
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
    "_prepared_url_score",
    "_prepared_url_decision",
    "_prepared_url_reasons",
    "_prepared_url_depth",
    "_prepared_url_priority",
    "_prepared_url_lastmod",
    "_prepared_url_section",
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
    on_started=None,
    on_result=None,
    log_progress: bool = True,
) -> list[dict]:
    """Prepare scraping metadata for a batch and stream each result via callback."""
    if not organizations:
        return []

    total = len(organizations)
    max_workers = max(1, int(settings.scraping.prepare_max_workers))
    summaries: list[dict] = []

    if max_workers == 1 or total == 1:
        for index, org in enumerate(organizations, start=1):
            if on_started is not None:
                on_started(org)
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
            if log_progress:
                logger.info(
                    "[%d/%d] prepared %s (%s, %d urls)",
                    index,
                    total,
                    summary.get("_org_name", "Unknown"),
                    summary.get("_scrape_prep_status", "unknown"),
                    summary.get("_scrape_prepared_url_count", 0),
                )
        return summaries

    def _prepare_with_start(org: dict) -> tuple[dict, list[dict]]:
        if on_started is not None:
            on_started(org)
        return _prepare_single_org(
            org,
            settings,
            org_id_column=org_id_column,
            name_column=name_column,
            website_column=website_column,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _prepare_with_start,
                org,
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
            if log_progress:
                logger.info(
                    "[%d/%d] prepared %s (%s, %d urls)",
                    completed,
                    total,
                    summary.get("_org_name", "Unknown"),
                    summary.get("_scrape_prep_status", "unknown"),
                    summary.get("_scrape_prepared_url_count", 0),
                )

    return summaries
