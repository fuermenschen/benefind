from __future__ import annotations

import json
import threading

import pytest

from benefind.config import PromptTemplate, SearchConfig, Settings
from benefind.discover_websites import (
    _brave_search,
    _firecrawl_search,
    _llm_web_verify,
    find_websites_batch,
)
from benefind.evaluate import ask_llm, evaluate_organization
from benefind.external_api import ExternalApiAccessError, is_quota_exhausted_signal


class _FakeResponse:
    def __init__(self, status_code: int, *, text: str = "", payload: dict | None = None) -> None:
        self.status_code = status_code
        self.text = text
        self.headers: dict[str, str] = {}
        self._payload = payload or {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeHttpClient:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)

    def __enter__(self) -> _FakeHttpClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def get(self, *_args, **_kwargs) -> _FakeResponse:
        return self._responses.pop(0)

    def post(self, *_args, **_kwargs) -> _FakeResponse:
        return self._responses.pop(0)


class _CountingHttpClient(_FakeHttpClient):
    def __init__(self, responses: list[_FakeResponse], counters: dict[str, int]) -> None:
        super().__init__(responses)
        self._counters = counters

    def get(self, *_args, **_kwargs) -> _FakeResponse:
        self._counters["get"] = self._counters.get("get", 0) + 1
        return super().get(*_args, **_kwargs)


class _FakeOpenAIError(Exception):
    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class _FakeCompletions:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def create(self, **_kwargs):
        raise self._exc


class _FakeChat:
    def __init__(self, exc: Exception) -> None:
        self.completions = _FakeCompletions(exc)


class _FakeOpenAIClient:
    def __init__(self, exc: Exception) -> None:
        self.chat = _FakeChat(exc)


def test_brave_failfast_on_forbidden(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "bad-key")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient([_FakeResponse(403, text="forbidden")]),
    )

    with pytest.raises(ExternalApiAccessError) as exc_info:
        _brave_search("test org", max_retries=0)

    assert exc_info.value.provider == "Brave"
    assert exc_info.value.reason == "unauthorized_or_forbidden"
    assert exc_info.value.status_code == 403


def test_brave_failfast_on_quota_429(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "quota-key")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient(
            [_FakeResponse(429, text='{"error":"insufficient_quota"}')]
        ),
    )

    with pytest.raises(ExternalApiAccessError) as exc_info:
        _brave_search("test org", max_retries=0)

    assert exc_info.value.provider == "Brave"
    assert exc_info.value.reason == "quota_exhausted"
    assert exc_info.value.status_code == 429


def test_brave_does_not_failfast_on_transient_rate_limited_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "key")
    response = _FakeResponse(
        429,
        text='{"code":"RATE_LIMITED","message":"rate limit exceeded, try again later"}',
    )
    response.headers = {"X-RateLimit-Remaining": "1"}
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient([response]),
    )

    result = _brave_search("test org", max_retries=0)

    assert result == []


def test_brave_failfast_on_unknown_429(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "key")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient([_FakeResponse(429, text='{"code":"SOMETHING_NEW"}')]),
    )

    with pytest.raises(ExternalApiAccessError) as exc_info:
        _brave_search("test org", max_retries=0)

    assert exc_info.value.provider == "Brave"
    assert exc_info.value.reason == "unknown_429"
    assert exc_info.value.status_code == 429


def test_brave_does_not_failfast_on_transient_429_without_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "key")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient(
            [_FakeResponse(429, text="429 Too Many Requests: rate limit exceeded")]
        ),
    )

    result = _brave_search("test org", max_retries=0)

    assert result == []


def test_quota_signal_does_not_match_generic_quota_metric_phrase() -> None:
    message = "rate limit exceeded for quota metric read requests"
    assert is_quota_exhausted_signal(message) is False


def test_brave_search_stops_before_request_when_batch_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "key")
    counters: dict[str, int] = {}
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _CountingHttpClient(
            [_FakeResponse(200, payload={"web": {"results": []}})], counters
        ),
    )
    stop_event = threading.Event()
    stop_event.set()

    results = _brave_search("test org", max_retries=0, stop_event=stop_event)

    assert results == []
    assert counters.get("get", 0) == 0


def test_firecrawl_failfast_on_unauthorized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "bad-key")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient([_FakeResponse(401, text="unauthorized")]),
    )

    with pytest.raises(ExternalApiAccessError) as exc_info:
        _firecrawl_search("test org", max_retries=0)

    assert exc_info.value.provider == "Firecrawl"
    assert exc_info.value.reason == "unauthorized_or_forbidden"
    assert exc_info.value.status_code == 401


def test_firecrawl_failfast_on_payment_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "no-credits")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient([_FakeResponse(402, text='{"code":"payment_required"}')]),
    )

    with pytest.raises(ExternalApiAccessError) as exc_info:
        _firecrawl_search("test org", max_retries=0)

    assert exc_info.value.provider == "Firecrawl"
    assert exc_info.value.reason == "quota_exhausted"
    assert exc_info.value.status_code == 402


def test_firecrawl_does_not_failfast_on_transient_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "key")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient(
            [
                _FakeResponse(
                    429,
                    text=(
                        '{"error":{"code":"rate_limit_exceeded",'
                        '"message":"rate limit exceeded, retry later"}}'
                    ),
                )
            ]
        ),
    )

    result = _firecrawl_search("test org", max_retries=0)

    assert result == []


def test_firecrawl_does_not_failfast_on_transient_429_without_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "key")
    monkeypatch.setattr(
        "benefind.discover_websites.httpx.Client",
        lambda **_kwargs: _FakeHttpClient(
            [_FakeResponse(429, text="Too many requests: rate limit reached")]
        ),
    )

    result = _firecrawl_search("test org", max_retries=0)

    assert result == []


def test_llm_verify_fails_fast_when_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with pytest.raises(ExternalApiAccessError) as exc_info:
        _llm_web_verify("Org", "Winterthur", "", Settings())

    assert exc_info.value.provider == "OpenAI"
    assert exc_info.value.reason == "missing_api_key"


@pytest.mark.parametrize(
    ("status_code", "message", "expected_reason"),
    [
        (403, "Forbidden", "unauthorized_or_forbidden"),
        (429, "insufficient_quota", "quota_exhausted"),
    ],
)
def test_ask_llm_classifies_openai_access_errors(
    status_code: int,
    message: str,
    expected_reason: str,
) -> None:
    fake_client = _FakeOpenAIClient(_FakeOpenAIError(message, status_code=status_code))

    with pytest.raises(ExternalApiAccessError) as exc_info:
        ask_llm("hello", Settings(), client=fake_client)

    assert exc_info.value.provider == "OpenAI"
    assert exc_info.value.reason == expected_reason
    assert exc_info.value.status_code == status_code


def test_ask_llm_does_not_failfast_on_transient_openai_rate_limit() -> None:
    message = (
        '{"error":{"code":"rate_limit_exceeded","message":"rate limit exceeded, retry later"}}'
    )
    fake_client = _FakeOpenAIClient(_FakeOpenAIError(message, status_code=429))

    with pytest.raises(_FakeOpenAIError):
        ask_llm("hello", Settings(), client=fake_client)


def test_ask_llm_failfast_on_unknown_openai_429() -> None:
    fake_client = _FakeOpenAIClient(_FakeOpenAIError("429 throttled", status_code=429))

    with pytest.raises(ExternalApiAccessError) as exc_info:
        ask_llm("hello", Settings(), client=fake_client)

    assert exc_info.value.provider == "OpenAI"
    assert exc_info.value.reason == "unknown_429"
    assert exc_info.value.status_code == 429


def test_ask_llm_does_not_failfast_on_transient_openai_429_without_code() -> None:
    fake_client = _FakeOpenAIClient(
        _FakeOpenAIError("429 Too Many Requests: rate limit exceeded, retry later", status_code=429)
    )

    with pytest.raises(_FakeOpenAIError):
        ask_llm("hello", Settings(), client=fake_client)


def test_evaluate_saves_partial_results_on_failfast(tmp_path) -> None:
    org_dir = tmp_path / "org-one"
    pages_dir = org_dir / "pages"
    pages_dir.mkdir(parents=True)
    (pages_dir / "index.md").write_text("content", encoding="utf-8")

    settings = Settings(
        prompts=[
            PromptTemplate(id="q1", description="first", question="Q1 {org_name}"),
            PromptTemplate(id="q2", description="second", question="Q2 {org_name}"),
        ]
    )

    state = {"calls": 0}

    def _fake_ask_llm(_prompt, _settings, _client=None):
        state["calls"] += 1
        if state["calls"] == 1:
            return "ok"
        raise ExternalApiAccessError(provider="OpenAI", reason="quota_exhausted", status_code=429)

    import benefind.evaluate as evaluate_module

    original = evaluate_module.ask_llm
    evaluate_module.ask_llm = _fake_ask_llm
    try:
        with pytest.raises(ExternalApiAccessError):
            evaluate_organization("org_1", "Org", "Winterthur", "Purpose", org_dir, settings)
    finally:
        evaluate_module.ask_llm = original

    payload = json.loads((org_dir / "evaluation.json").read_text(encoding="utf-8"))
    assert payload["q1"]["answer"] == "ok"
    assert "q2" not in payload
    assert payload["_org_id"] == "org_1"
    assert payload["_org_name"] == "Org"


def test_discovery_batch_stops_on_access_error(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(search=SearchConfig(max_workers=1))

    def _raise_access_error(*_args, **_kwargs):
        raise ExternalApiAccessError(provider="Brave", reason="quota_exhausted", status_code=429)

    monkeypatch.setattr("benefind.discover_websites.find_website", _raise_access_error)

    with pytest.raises(ExternalApiAccessError):
        find_websites_batch(
            [{"Bezeichnung": "Org A", "Sitzort": "Winterthur"}],
            settings,
        )
