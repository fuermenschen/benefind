from __future__ import annotations

import pytest

from benefind.config import Settings
from benefind.prepare_scraping import (
    _build_scope_from_final_url,
    _prepare_single_org,
    _resolve_reachable_scope,
    build_prepare_input_signature,
)


class _FakeHttpClient:
    def __enter__(self) -> _FakeHttpClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_build_scope_from_final_url_root_is_host_scope() -> None:
    scope = _build_scope_from_final_url("https://example.org/", include_subdomains=False)

    assert scope is not None
    assert scope.seed_url == "https://example.org/"
    assert scope.scope_mode == "host"
    assert scope.path_prefix == "/"
    assert scope.scope_reason == "final_url_root"


def test_build_scope_from_final_url_non_root_keeps_exact_path_prefix() -> None:
    scope = _build_scope_from_final_url("https://example.org/de/verein", include_subdomains=False)

    assert scope is not None
    assert scope.seed_url == "https://example.org/de/verein"
    assert scope.scope_mode == "path_prefix"
    assert scope.path_prefix == "/de/verein"
    assert scope.scope_reason == "final_url_path_prefix"


def test_resolve_reachable_scope_keeps_strict_scope_from_resolved_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial_scope = _build_scope_from_final_url("https://example.org/de/verein", False)
    assert initial_scope is not None

    monkeypatch.setattr(
        "benefind.prepare_scraping._build_seed_probe_candidates",
        lambda _seed_url: ["https://example.org/de/verein"],
    )
    monkeypatch.setattr(
        "benefind.prepare_scraping._probe_seed_candidate",
        lambda _client, _candidate, _timeout: (
            True,
            "https://www.example.org/partners/club",
            "http_200",
        ),
    )

    resolved_scope, error = _resolve_reachable_scope(object(), initial_scope, timeout_seconds=5)

    assert error == ""
    assert resolved_scope is not None
    assert resolved_scope.seed_url == "https://www.example.org/partners/club"
    assert resolved_scope.scope_mode == "path_prefix"
    assert resolved_scope.path_prefix == "/partners/club"
    assert resolved_scope.scope_reason == "final_url_path_prefix"


def test_prepare_single_org_uses_final_url_scope_without_heuristic_promotion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings()
    settings.scraping.respect_robots_txt = False

    monkeypatch.setattr(
        "benefind.prepare_scraping.httpx.Client",
        lambda **_kwargs: _FakeHttpClient(),
    )
    monkeypatch.setattr(
        "benefind.prepare_scraping._resolve_reachable_scope",
        lambda _client, scope, _timeout: (scope, ""),
    )
    monkeypatch.setattr("benefind.prepare_scraping._collect_sitemap_urls", lambda **_kwargs: [])
    monkeypatch.setattr(
        "benefind.prepare_scraping._collect_link_fallback_urls",
        lambda **_kwargs: [],
    )

    summary, targets = _prepare_single_org(
        {
            "_org_id": "org_test_1",
            "Bezeichnung": "Test Verein",
            "_website_url_final": "https://example.org/de/verein",
        },
        settings,
        org_id_column="_org_id",
        name_column="Bezeichnung",
        website_column="_website_url_final",
    )

    assert summary["_scrape_scope_mode"] == "path_prefix"
    assert summary["_scrape_scope_path_prefix"] == "/de/verein"
    assert summary["_scrape_scope_reason"] == "final_url_path_prefix"
    assert summary["_scrape_seed_normalized"] == "https://example.org/de/verein"
    assert summary["_scrape_prep_status"] == "ready"
    assert len(targets) == 1
    assert targets[0]["_prepared_url"] == "https://example.org/de/verein"


def test_prepare_signature_is_deterministic_and_changes_on_final_url() -> None:
    settings = Settings()
    org = {
        "_org_id": "org_test_1",
        "_website_url_final": "https://example.org/",
        "_excluded_reason": "",
    }

    sig_one = build_prepare_input_signature(org, settings)
    sig_two = build_prepare_input_signature(org, settings)
    assert sig_one == sig_two

    changed = dict(org)
    changed["_website_url_final"] = "https://example.org/about"
    sig_three = build_prepare_input_signature(changed, settings)
    assert sig_three != sig_one


def test_prepare_single_org_seed_unreachable_marks_readiness_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings()
    settings.scraping.respect_robots_txt = False

    monkeypatch.setattr(
        "benefind.prepare_scraping.httpx.Client",
        lambda **_kwargs: _FakeHttpClient(),
    )
    monkeypatch.setattr(
        "benefind.prepare_scraping._resolve_reachable_scope",
        lambda _client, _scope, _timeout: (None, "seed_unreachable:all_probe_attempts_failed"),
    )

    summary, targets = _prepare_single_org(
        {
            "_org_id": "org_test_2",
            "Bezeichnung": "Unreachable Verein",
            "_website_url_final": "https://example.org/",
        },
        settings,
        org_id_column="_org_id",
        name_column="Bezeichnung",
        website_column="_website_url_final",
    )

    assert summary["_scrape_prep_status"] == "no_urls"
    assert summary["_scrape_robots_fetch"] == "seed_unreachable"
    assert summary["_scrape_readiness_status"] == "pending"
    assert summary["_scrape_input_signature"]
    assert targets == []
