"""ZEFIX enrichment helpers for legal form, UID, purpose, and status."""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from benefind.config import Settings
from benefind.external_api import ExternalApiAccessError, classify_http_access_error

logger = logging.getLogger(__name__)


class _TransientZefixError(RuntimeError):
    """Transient request failure that should be retried."""


class _GlobalRateLimiter:
    """Token-bucket rate limiter shared across ZEFIX batch workers."""

    def __init__(self, requests_per_second: float, burst: int) -> None:
        self._rate = float(max(0.0, requests_per_second))
        self._capacity = float(max(1, burst))
        self._tokens = self._capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def wait_for_slot(self) -> None:
        if self._rate <= 0:
            return

        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._last_refill)
                self._last_refill = now
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                wait_seconds = (1.0 - self._tokens) / self._rate

            time.sleep(max(0.01, wait_seconds))


@dataclass
class ZefixEnrichmentResult:
    org_name: str
    query_name_normalized: str
    match_status: str
    match_count: int
    match_uids: str
    match_names: str
    uid: str
    legal_form: str
    purpose: str
    status: str
    checked_at: str
    error: str


@dataclass
class ZefixUidLookupResult:
    uid: str
    name: str
    legal_form: str
    purpose: str
    status: str
    error: str


def normalize_org_name(value: str) -> str:
    """Normalize org names for straightforward exact matching."""
    text = str(value or "").strip().lower()
    if not text:
        return ""

    text = text.replace("_", " ")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat(timespec="seconds")


def _env_required(name: str) -> str:
    value = str(os.environ.get(name, "") or "").strip()
    if not value:
        raise ExternalApiAccessError(
            provider="ZEFIX",
            reason="missing_api_key",
            details=f"{name} is not set",
        )
    return value


def _zefix_request(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    settings: Settings,
    rate_limiter: _GlobalRateLimiter,
    json_payload: dict | None = None,
) -> object:
    attempts = max(1, int(settings.zefix.max_retries) + 1)
    backoff_base = max(0.1, float(settings.zefix.retry_backoff_seconds))

    retrying = Retrying(
        reraise=True,
        stop=stop_after_attempt(attempts),
        wait=wait_exponential_jitter(initial=backoff_base, max=20),
        retry=retry_if_exception_type(_TransientZefixError),
    )

    for attempt in retrying:
        with attempt:
            rate_limiter.wait_for_slot()
            try:
                response = client.request(method, path, json=json_payload)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                raise _TransientZefixError(f"request_failed:{type(exc).__name__}: {exc}") from exc
            except Exception as exc:
                raise RuntimeError(f"request_failed:{type(exc).__name__}: {exc}") from exc

            body_text = response.text or ""
            access_error = classify_http_access_error(
                "zefix",
                int(response.status_code),
                body_text,
                headers=dict(response.headers),
            )
            if access_error is not None:
                raise access_error

            if response.status_code == 404:
                return []

            if response.status_code == 429 or response.status_code >= 500:
                raise _TransientZefixError(f"http_{response.status_code}: {body_text[:250]}")

            if response.status_code >= 400:
                raise RuntimeError(f"http_{response.status_code}: {body_text[:250]}")

            try:
                return response.json()
            except Exception as exc:
                raise RuntimeError(f"invalid_json:{type(exc).__name__}") from exc

    return []


def _search_companies(
    client: httpx.Client,
    *,
    normalized_name: str,
    canton: str,
    active_only: bool,
    settings: Settings,
    rate_limiter: _GlobalRateLimiter,
) -> list[dict]:
    payload = {
        "name": normalized_name,
        "canton": canton,
        "activeOnly": bool(active_only),
    }
    data = _zefix_request(
        client,
        "POST",
        "/api/v1/company/search",
        settings=settings,
        rate_limiter=rate_limiter,
        json_payload=payload,
    )
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def _company_details_by_uid(
    client: httpx.Client,
    *,
    uid: str,
    settings: Settings,
    rate_limiter: _GlobalRateLimiter,
) -> dict:
    data = _zefix_request(
        client,
        "GET",
        f"/api/v1/company/uid/{uid}",
        settings=settings,
        rate_limiter=rate_limiter,
    )
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}


def _exact_name_matches(candidates: list[dict], normalized_query: str) -> list[dict]:
    out: list[dict] = []
    for candidate in candidates:
        candidate_name = normalize_org_name(str(candidate.get("name", "") or ""))
        if candidate_name and candidate_name == normalized_query:
            out.append(candidate)
    return out


def enrich_with_zefix(
    org_name: str,
    *,
    settings: Settings,
    canton: str = "ZH",
    active_only: bool = False,
    rate_limiter: _GlobalRateLimiter | None = None,
) -> ZefixEnrichmentResult:
    normalized_query = normalize_org_name(org_name)
    checked_at = _now_iso()

    if len(normalized_query) < 3:
        return ZefixEnrichmentResult(
            org_name=org_name,
            query_name_normalized=normalized_query,
            match_status="search_error",
            match_count=0,
            match_uids="",
            match_names="",
            uid="",
            legal_form="",
            purpose="",
            status="",
            checked_at=checked_at,
            error="normalized_name_too_short",
        )

    base_url = _env_required("ZEFIX_BASE_URL").rstrip("/")
    username = _env_required("ZEFIX_USERNAME")
    password = _env_required("ZEFIX_PASSWORD")

    timeout = max(1, int(settings.zefix.timeout_seconds))
    effective_rate_limiter = rate_limiter or _GlobalRateLimiter(
        settings.zefix.max_requests_per_second,
        settings.zefix.max_burst,
    )
    with httpx.Client(
        base_url=base_url,
        timeout=timeout,
        auth=(username, password),
        headers={"Accept": "application/json"},
    ) as client:
        try:
            search_rows = _search_companies(
                client,
                normalized_name=normalized_query,
                canton=canton,
                active_only=active_only,
                settings=settings,
                rate_limiter=effective_rate_limiter,
            )
        except ExternalApiAccessError:
            raise
        except Exception as exc:
            return ZefixEnrichmentResult(
                org_name=org_name,
                query_name_normalized=normalized_query,
                match_status="search_error",
                match_count=0,
                match_uids="",
                match_names="",
                uid="",
                legal_form="",
                purpose="",
                status="",
                checked_at=checked_at,
                error=f"search_failed:{exc}",
            )

        matches = _exact_name_matches(search_rows, normalized_query)
        preview_limit = max(1, int(settings.zefix.candidate_preview_limit))
        preview_rows = matches[:preview_limit]
        match_uids = "|".join(str(row.get("uid", "") or "").strip() for row in preview_rows)
        match_names = "|".join(str(row.get("name", "") or "").strip() for row in preview_rows)

        if len(matches) == 0:
            return ZefixEnrichmentResult(
                org_name=org_name,
                query_name_normalized=normalized_query,
                match_status="no_match",
                match_count=0,
                match_uids=match_uids,
                match_names=match_names,
                uid="",
                legal_form="",
                purpose="",
                status="",
                checked_at=checked_at,
                error="",
            )

        if len(matches) > 1:
            return ZefixEnrichmentResult(
                org_name=org_name,
                query_name_normalized=normalized_query,
                match_status="multiple_matches",
                match_count=len(matches),
                match_uids=match_uids,
                match_names=match_names,
                uid="",
                legal_form="",
                purpose="",
                status="",
                checked_at=checked_at,
                error="",
            )

        matched_uid = str(matches[0].get("uid", "") or "").strip()
        if not matched_uid:
            return ZefixEnrichmentResult(
                org_name=org_name,
                query_name_normalized=normalized_query,
                match_status="detail_error",
                match_count=1,
                match_uids="",
                match_names=match_names,
                uid="",
                legal_form="",
                purpose="",
                status="",
                checked_at=checked_at,
                error="missing_uid_in_match",
            )

        try:
            detail = _company_details_by_uid(
                client,
                uid=matched_uid,
                settings=settings,
                rate_limiter=effective_rate_limiter,
            )
        except ExternalApiAccessError:
            raise
        except Exception as exc:
            return ZefixEnrichmentResult(
                org_name=org_name,
                query_name_normalized=normalized_query,
                match_status="detail_error",
                match_count=1,
                match_uids=matched_uid,
                match_names=match_names,
                uid=matched_uid,
                legal_form="",
                purpose="",
                status="",
                checked_at=checked_at,
                error=f"detail_failed:{exc}",
            )

        if not detail:
            return ZefixEnrichmentResult(
                org_name=org_name,
                query_name_normalized=normalized_query,
                match_status="detail_error",
                match_count=1,
                match_uids=matched_uid,
                match_names=match_names,
                uid=matched_uid,
                legal_form="",
                purpose="",
                status="",
                checked_at=checked_at,
                error="detail_not_found",
            )

    legal_form = str(((detail.get("legalForm") or {}).get("name") or {}).get("de") or "").strip()
    purpose = str(detail.get("purpose", "") or "").strip()
    status = str(detail.get("status", "") or "").strip()

    return ZefixEnrichmentResult(
        org_name=org_name,
        query_name_normalized=normalized_query,
        match_status="matched",
        match_count=1,
        match_uids=matched_uid,
        match_names=match_names,
        uid=matched_uid,
        legal_form=legal_form,
        purpose=purpose,
        status=status,
        checked_at=checked_at,
        error="",
    )


def enrich_with_zefix_batch(
    organizations: list[dict],
    settings: Settings,
    *,
    name_column: str,
    canton: str = "ZH",
    active_only: bool = False,
    on_result: Callable[[int, ZefixEnrichmentResult], None] | None = None,
) -> list[ZefixEnrichmentResult]:
    if not organizations:
        return []

    max_workers = max(1, int(settings.zefix.max_workers))
    indexed_results: list[tuple[int, ZefixEnrichmentResult]] = []
    stop_event = threading.Event()
    rate_limiter = _GlobalRateLimiter(
        settings.zefix.max_requests_per_second,
        settings.zefix.max_burst,
    )

    def run_single(index: int, row: dict) -> tuple[int, ZefixEnrichmentResult]:
        if stop_event.is_set():
            org_name = str(row.get(name_column, "") or "")
            return index, ZefixEnrichmentResult(
                org_name=org_name,
                query_name_normalized=normalize_org_name(org_name),
                match_status="search_error",
                match_count=0,
                match_uids="",
                match_names="",
                uid="",
                legal_form="",
                purpose="",
                status="",
                checked_at=_now_iso(),
                error="skipped_batch_stopped",
            )

        org_name = str(row.get(name_column, "") or "")
        result = enrich_with_zefix(
            org_name,
            settings=settings,
            canton=canton,
            active_only=active_only,
            rate_limiter=rate_limiter,
        )
        return index, result

    executor = ThreadPoolExecutor(max_workers=max_workers)
    future_context: dict = {}
    for index, row in enumerate(organizations):
        future = executor.submit(run_single, index, row)
        org_name = str(row.get(name_column, "") or "")
        future_context[future] = (index, org_name)

    try:
        for future in as_completed(future_context):
            try:
                index, result = future.result()
            except ExternalApiAccessError:
                stop_event.set()
                executor.shutdown(wait=True, cancel_futures=True)
                raise
            except Exception as exc:
                index, org_name = future_context[future]
                logger.warning("ZEFIX batch worker failed for index=%s: %s", index, exc)
                result = ZefixEnrichmentResult(
                    org_name=org_name,
                    query_name_normalized=normalize_org_name(org_name),
                    match_status="search_error",
                    match_count=0,
                    match_uids="",
                    match_names="",
                    uid="",
                    legal_form="",
                    purpose="",
                    status="",
                    checked_at=_now_iso(),
                    error=f"batch_worker_error:{type(exc).__name__}: {exc}",
                )

            indexed_results.append((index, result))
            if on_result is not None:
                on_result(index, result)
    finally:
        executor.shutdown(wait=True)

    indexed_results.sort(key=lambda item: item[0])
    return [result for _, result in indexed_results]


def lookup_zefix_uid(uid: str, *, settings: Settings) -> ZefixUidLookupResult:
    """Fetch ZEFIX company details for one UID."""
    uid_norm = str(uid or "").strip()
    if not uid_norm:
        return ZefixUidLookupResult("", "", "", "", "", "uid_empty")

    base_url = _env_required("ZEFIX_BASE_URL").rstrip("/")
    username = _env_required("ZEFIX_USERNAME")
    password = _env_required("ZEFIX_PASSWORD")

    timeout = max(1, int(settings.zefix.timeout_seconds))
    rate_limiter = _GlobalRateLimiter(
        settings.zefix.max_requests_per_second,
        settings.zefix.max_burst,
    )

    with httpx.Client(
        base_url=base_url,
        timeout=timeout,
        auth=(username, password),
        headers={"Accept": "application/json"},
    ) as client:
        try:
            detail = _company_details_by_uid(
                client,
                uid=uid_norm,
                settings=settings,
                rate_limiter=rate_limiter,
            )
        except ExternalApiAccessError:
            raise
        except Exception as exc:
            return ZefixUidLookupResult(uid_norm, "", "", "", "", f"detail_failed:{exc}")

    if not detail:
        return ZefixUidLookupResult(uid_norm, "", "", "", "", "detail_not_found")

    name = str(detail.get("name", "") or "").strip()
    legal_form = str(((detail.get("legalForm") or {}).get("name") or {}).get("de") or "").strip()
    purpose = str(detail.get("purpose", "") or "").strip()
    status = str(detail.get("status", "") or "").strip()

    return ZefixUidLookupResult(uid_norm, name, legal_form, purpose, status, "")
