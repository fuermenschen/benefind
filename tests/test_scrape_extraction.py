from __future__ import annotations

import httpx
import pytest

from benefind import scrape


def test_score_extracted_content_rewards_structured_content() -> None:
    content = "\n\n".join(
        [
            "# Example Heading",
            "This is a long paragraph with enough words to count as meaningful. "
            "It has multiple sentences. Another sentence appears here.",
            "Second paragraph has additional details. It continues with more text. "
            "A third sentence is included.",
            "Third paragraph is also descriptive and talks about the organization mission.",
            "Fourth paragraph adds context around activities and local engagement.",
            "Fifth paragraph closes with contacts and supporting details.",
        ]
    )
    metadata = {"title": "Example", "description": "Desc", "canonical": "", "lang": "de"}

    score = scrape._score_extracted_content(content, metadata)
    assert score >= 35


def test_select_best_extractor_prefers_highest_score(monkeypatch: pytest.MonkeyPatch) -> None:
    html = (
        "<html><head><title>X</title><meta name='description' content='D'></head>"
        "<body>x</body></html>"
    )
    metadata = {"title": "X", "description": "D", "canonical": "", "lang": "en"}

    def low(_html: str) -> str:
        return "short"

    def high(_html: str) -> str:
        lines = ["# Heading"] + [
            (
                "Paragraph number "
                f"{index} with several sentences for extraction quality. "
                "Another sentence appears here. Third sentence appears here."
            )
            for index in range(8)
        ]
        return "\n\n".join(lines)

    monkeypatch.setattr(
        scrape,
        "_available_extractors",
        lambda: {
            "markdownify": low,
            "trafilatura": high,
            "readability-lxml": low,
        },
    )

    extractor, _content, score = scrape._select_best_extractor(
        html,
        metadata,
        preferred_extractor=None,
    )

    assert extractor == "trafilatura"
    assert score >= 35


def test_select_best_keeps_preferred_if_acceptable(monkeypatch: pytest.MonkeyPatch) -> None:
    html = (
        "<html><head><title>X</title><meta name='description' content='D'></head>"
        "<body>x</body></html>"
    )
    metadata = {"title": "X", "description": "D", "canonical": "", "lang": "en"}

    def acceptable(_html: str) -> str:
        return "\n\n".join(
            [
                "# Heading",
                (
                    "First paragraph with enough detail to count as meaningful extraction. "
                    "Another sentence appears. A third sentence appears."
                ),
                (
                    "Second paragraph has additional detail for the same organization page. "
                    "Another sentence appears. A third sentence appears."
                ),
                (
                    "Third paragraph includes context about services and eligibility details. "
                    "Another sentence appears. A third sentence appears."
                ),
                (
                    "Fourth paragraph captures opening hours and locations for local support. "
                    "Another sentence appears. A third sentence appears."
                ),
                (
                    "Fifth paragraph includes contact and next-step guidance for applicants. "
                    "Another sentence appears. A third sentence appears."
                ),
            ]
        )

    def better(_html: str) -> str:
        lines = ["# Better Heading"] + [
            (
                "Paragraph number "
                f"{index} with several sentences for extraction quality. "
                "Another sentence appears here. Third sentence appears here."
            )
            for index in range(10)
        ]
        return "\n\n".join(lines)

    monkeypatch.setattr(
        scrape,
        "_available_extractors",
        lambda: {
            "markdownify": acceptable,
            "trafilatura": better,
            "readability-lxml": better,
        },
    )

    extractor, _content, score = scrape._select_best_extractor(
        html,
        metadata,
        preferred_extractor="markdownify",
    )

    assert extractor == "markdownify"
    assert score >= 35


def test_scrape_page_escalates_to_playwright_after_poor_static(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            text="<html><head><title>Static</title></head><body>tiny</body></html>",
        )

    selection_results = iter(
        [
            ("markdownify", "tiny", 10),
            ("trafilatura", "# Rendered\n\nLonger rendered content", 60),
        ]
    )

    monkeypatch.setattr(
        scrape,
        "_select_best_extractor",
        lambda *_args, **_kwargs: next(selection_results),
    )
    monkeypatch.setattr(
        scrape,
        "_render_page_playwright",
        lambda *_args, **_kwargs: scrape.PlaywrightRenderResult(
            status="success",
            html="<html><head><title>Rendered</title></head><body>ok</body></html>",
            final_url="https://example.org/",
            failure_detail="",
        ),
    )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = scrape._scrape_page_static(client, "https://example.org/")

    assert result.status == "success"
    assert result.fetch_mode == "playwright"
    assert result.extractor_selected == "trafilatura"
    assert result.extractor_score_static_best == 10
    assert result.extractor_score_render_best == 60
    assert result.render_trigger_reason == "poor_static_extraction"


def test_scrape_page_falls_back_to_static_when_playwright_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            text="<html><head><title>Static</title></head><body>tiny</body></html>",
        )

    monkeypatch.setattr(
        scrape,
        "_select_best_extractor",
        lambda *_args, **_kwargs: ("markdownify", "tiny", 10),
    )
    monkeypatch.setattr(
        scrape,
        "_render_page_playwright",
        lambda *_args, **_kwargs: scrape.PlaywrightRenderResult(
            status="failed",
            html="",
            final_url="",
            failure_detail="playwright_not_installed",
        ),
    )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = scrape._scrape_page_static(client, "https://example.org/")

    assert result.status == "success"
    assert result.failure_reason_code == ""
    assert result.fetch_mode == "static"
    assert "render_fallback=playwright_not_installed" in result.render_trigger_reason


def test_scrape_page_render_error_still_fails_when_not_infra_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            text="<html><head><title>Static</title></head><body>tiny</body></html>",
        )

    monkeypatch.setattr(
        scrape,
        "_select_best_extractor",
        lambda *_args, **_kwargs: ("markdownify", "tiny", 10),
    )
    monkeypatch.setattr(
        scrape,
        "_render_page_playwright",
        lambda *_args, **_kwargs: scrape.PlaywrightRenderResult(
            status="failed",
            html="",
            final_url="",
            failure_detail="navigation timeout",
        ),
    )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = scrape._scrape_page_static(client, "https://example.org/")

    assert result.status == "failed"
    assert result.failure_reason_code == "playwright_render_failed"
    assert result.failure_detail == "navigation timeout"
    assert result.fetch_mode == "playwright"


def test_scrape_page_non_html_never_calls_playwright(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/pdf"},
            text="%PDF-1.7",
        )

    called = {"render": False}

    def fake_render(*_args, **_kwargs) -> scrape.PlaywrightRenderResult:
        called["render"] = True
        return scrape.PlaywrightRenderResult(
            status="failed",
            html="",
            final_url="",
            failure_detail="should_not_be_called",
        )

    monkeypatch.setattr(scrape, "_render_page_playwright", fake_render)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = scrape._scrape_page_static(client, "https://example.org/file.pdf")

    assert result.status == "failed"
    assert result.failure_reason_code == "non_html_response"
    assert called["render"] is False


def test_scrape_page_http_other_status_returns_http_other_code() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            headers={"content-type": "text/html"},
            text="<html><body>Not found</body></html>",
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = scrape._scrape_page_static(client, "https://example.org/missing")

    assert result.status == "failed"
    assert result.failure_reason_code == "http_other"
    assert result.failure_detail == "http_404"


def test_render_trigger_reason_includes_js_markers() -> None:
    html = (
        "<html><head><script>window.__NUXT__ = {};</script></head>"
        "<body><div id='__next'></div><noscript>JavaScript is required</noscript></body></html>"
    )

    reason = scrape._build_render_trigger_reason(html, "tiny")

    assert reason.startswith("poor_static_extraction:markers=")
    assert "id___next" in reason
    assert "window_nuxt" in reason
    assert "noscript_js_required" in reason


def test_scrape_page_http_403_never_calls_playwright(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            headers={"content-type": "text/html"},
            text="<html><body>Forbidden</body></html>",
        )

    called = {"render": False}

    def fake_render(*_args, **_kwargs) -> scrape.PlaywrightRenderResult:
        called["render"] = True
        return scrape.PlaywrightRenderResult(
            status="failed",
            html="",
            final_url="",
            failure_detail="should_not_be_called",
        )

    monkeypatch.setattr(scrape, "_render_page_playwright", fake_render)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = scrape._scrape_page_static(client, "https://example.org/private")

    assert result.status == "failed"
    assert result.failure_reason_code == "http_403"
    assert called["render"] is False
