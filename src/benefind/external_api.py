"""Helpers for classifying external API access failures."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass


@dataclass(slots=True)
class ExternalApiAccessError(RuntimeError):
    """Unrecoverable external API access failure.

    Used for quota/billing exhaustion and authentication/configuration failures
    where continued retries or provider fallback should stop immediately.
    """

    provider: str
    reason: str
    details: str = ""
    status_code: int | None = None

    def __post_init__(self) -> None:
        status = f" status={self.status_code}" if self.status_code is not None else ""
        message = f"{self.provider}: {self.reason}{status}"
        if self.details:
            message = f"{message} ({self.details})"
        super().__init__(message)


def is_quota_exhausted_signal(text: str) -> bool:
    """Return True when provider response text indicates credit exhaustion."""
    value = (text or "").lower()
    if not value:
        return False

    markers = (
        "insufficient_quota",
        "out of credits",
        "credit balance",
        "exceeded your current quota",
        "quota exceeded",
        "usage limit reached",
        "billing hard limit",
    )
    return any(marker in value for marker in markers)


def _looks_like_non_billing_rate_limit(text: str) -> bool:
    value = (text or "").lower()
    if not value:
        return False

    rate_markers = (
        "rate limit",
        "too many requests",
        "requests per minute",
        "requests per day",
        "rate_limited",
        "rate_limit_exceeded",
    )
    billing_markers = (
        "quota",
        "credit",
        "billing",
        "payment",
        "subscription",
        "plan",
        "insufficient_quota",
    )

    return any(marker in value for marker in rate_markers) and not any(
        marker in value for marker in billing_markers
    )


def _extract_error_code(body_text: str) -> str:
    stripped = (body_text or "").strip()
    if not stripped:
        return ""

    code = ""
    try:
        payload = json.loads(stripped)
        if isinstance(payload, dict):
            direct = payload.get("code")
            if isinstance(direct, str):
                code = direct
            nested = payload.get("error")
            if not code and isinstance(nested, dict):
                nested_code = nested.get("code")
                if isinstance(nested_code, str):
                    code = nested_code
    except json.JSONDecodeError:
        code = ""

    if not code:
        match = re.search(r'"code"\s*:\s*"([A-Za-z0-9_\-]+)"', stripped)
        if match:
            code = match.group(1)

    return code.strip()


def _is_zero(value: str) -> bool:
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def _classify_brave_access_error(
    status_code: int,
    body_text: str,
    headers: dict[str, str] | None = None,
) -> ExternalApiAccessError | None:
    code = _extract_error_code(body_text).upper()
    remaining = ""
    if headers:
        remaining = headers.get("X-RateLimit-Remaining") or headers.get("x-ratelimit-remaining", "")

    if status_code in {401, 403}:
        return ExternalApiAccessError(
            provider="Brave",
            reason="unauthorized_or_forbidden",
            details=f"code={code or 'unknown'} body={(body_text or '')[:200]}",
            status_code=status_code,
        )

    if status_code == 429:
        if _looks_like_non_billing_rate_limit(body_text):
            return None
        if code == "QUOTA_LIMITED" and _is_zero(remaining):
            return ExternalApiAccessError(
                provider="Brave",
                reason="quota_exhausted",
                details=f"code={code} remaining={remaining!r}",
                status_code=status_code,
            )
        if code in {
            "QUOTA_LIMITED",
            "SUBSCRIPTION_TOKEN_INVALID",
            "SUBSCRIPTION_NOT_FOUND",
            "RESOURCE_NOT_ALLOWED",
            "OPTION_NOT_IN_PLAN",
        }:
            return ExternalApiAccessError(
                provider="Brave",
                reason="quota_or_plan_blocked",
                details=f"code={code}",
                status_code=status_code,
            )
        if is_quota_exhausted_signal(body_text):
            return ExternalApiAccessError(
                provider="Brave",
                reason="quota_exhausted",
                details=(body_text or "")[:200],
                status_code=status_code,
            )

        return ExternalApiAccessError(
            provider="Brave",
            reason="unknown_429",
            details=f"code={code or 'unknown'} remaining={remaining!r}",
            status_code=status_code,
        )

    return None


def _classify_firecrawl_access_error(
    status_code: int,
    body_text: str,
) -> ExternalApiAccessError | None:
    code = _extract_error_code(body_text).lower()

    if status_code == 402:
        return ExternalApiAccessError(
            provider="Firecrawl",
            reason="quota_exhausted",
            details=f"code={code or 'payment_required'}",
            status_code=status_code,
        )

    if status_code in {401, 403}:
        return ExternalApiAccessError(
            provider="Firecrawl",
            reason="unauthorized_or_forbidden",
            details=f"code={code or 'unknown'} body={(body_text or '')[:200]}",
            status_code=status_code,
        )

    if status_code == 429:
        if _looks_like_non_billing_rate_limit(body_text):
            return None
        if is_quota_exhausted_signal(body_text):
            return ExternalApiAccessError(
                provider="Firecrawl",
                reason="quota_exhausted",
                details=f"code={code or 'unknown'} body={(body_text or '')[:200]}",
                status_code=status_code,
            )
        return ExternalApiAccessError(
            provider="Firecrawl",
            reason="unknown_429",
            details=f"code={code or 'unknown'} body={(body_text or '')[:200]}",
            status_code=status_code,
        )

    return None


def classify_http_access_error(
    provider: str,
    status_code: int,
    body_text: str,
    headers: dict[str, str] | None = None,
) -> ExternalApiAccessError | None:
    """Classify unrecoverable access failures from HTTP status + body text."""
    provider_key = (provider or "").strip().lower()

    if provider_key == "brave":
        return _classify_brave_access_error(status_code, body_text, headers=headers)

    if provider_key == "firecrawl":
        return _classify_firecrawl_access_error(status_code, body_text)

    if status_code in {401, 403}:
        return ExternalApiAccessError(
            provider=provider,
            reason="unauthorized_or_forbidden",
            details=(body_text or "")[:200],
            status_code=status_code,
        )

    if status_code == 429 and is_quota_exhausted_signal(body_text):
        return ExternalApiAccessError(
            provider=provider,
            reason="quota_exhausted",
            details=(body_text or "")[:200],
            status_code=status_code,
        )

    return None


def classify_openai_access_error(exc: Exception) -> ExternalApiAccessError | None:
    """Classify unrecoverable OpenAI SDK/API access failures."""
    status_code = getattr(exc, "status_code", None)
    text = str(exc)
    exc_name = type(exc).__name__.lower()
    code = ""

    response = getattr(exc, "response", None)
    if response is not None:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                error = payload.get("error")
                if isinstance(error, dict):
                    value = error.get("code")
                    if isinstance(value, str):
                        code = value
        except Exception:
            code = ""

    if not code:
        body = getattr(exc, "body", None)
        if isinstance(body, dict):
            error = body.get("error", body)
            if isinstance(error, dict):
                value = error.get("code")
                if isinstance(value, str):
                    code = value

    if not code:
        code = _extract_error_code(text)
    code = code.lower()

    blocking_codes = {
        "insufficient_quota",
        "billing_hard_limit_reached",
        "account_deactivated",
        "access_terminated",
        "permission_denied",
        "invalid_api_key",
    }

    if status_code in {401, 403}:
        return ExternalApiAccessError(
            provider="OpenAI",
            reason="unauthorized_or_forbidden",
            details=f"code={code or 'unknown'} {text}",
            status_code=status_code,
        )

    if status_code == 429 and _looks_like_non_billing_rate_limit(text):
        return None

    if status_code == 429 and (code in blocking_codes or is_quota_exhausted_signal(text)):
        return ExternalApiAccessError(
            provider="OpenAI",
            reason="quota_exhausted",
            details=f"code={code or 'unknown'} {text}",
            status_code=status_code,
        )

    if status_code == 429:
        return ExternalApiAccessError(
            provider="OpenAI",
            reason="unknown_429",
            details=f"code={code or 'unknown'} {text}",
            status_code=status_code,
        )

    if code in blocking_codes:
        reason = (
            "quota_exhausted"
            if code in {"insufficient_quota", "billing_hard_limit_reached"}
            else "unauthorized_or_forbidden"
        )
        return ExternalApiAccessError(
            provider="OpenAI",
            reason=reason,
            details=f"code={code} {text}",
            status_code=status_code,
        )

    if "authentication" in exc_name or "permission" in exc_name:
        return ExternalApiAccessError(
            provider="OpenAI",
            reason="unauthorized_or_forbidden",
            details=text,
            status_code=status_code,
        )

    if "rate" in exc_name and is_quota_exhausted_signal(text):
        return ExternalApiAccessError(
            provider="OpenAI",
            reason="quota_exhausted",
            details=text,
            status_code=status_code,
        )

    return None
