from __future__ import annotations

from pathlib import Path

import pytest

from benefind.config import load_prompt_registry, render_prompt_template


def _write_prompt(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_load_prompt_registry_reads_prompt_folder(tmp_path: Path) -> None:
    _write_prompt(
        tmp_path / "prompts" / "discover.website_verify.toml",
        """
[prompt]
id = "discover.website_verify"
description = "Verify website"
template = "Organization: {org_name}"

[prompt.placeholders]
org_name = "Organization"

[prompt.response]
format = "json_object"
required_keys = ["url"]
""".strip(),
    )

    registry = load_prompt_registry(tmp_path)
    assert "discover.website_verify" in registry
    prompt = registry["discover.website_verify"]
    rendered = render_prompt_template(prompt, {"org_name": "Org"})
    assert rendered == "Organization: Org"


def test_load_prompt_registry_rejects_placeholder_mismatch(tmp_path: Path) -> None:
    _write_prompt(
        tmp_path / "prompts" / "bad.toml",
        """
[prompt]
id = "bad.prompt"
description = "Bad"
template = "Organization: {org_name}"

[prompt.placeholders]
org_location = "Location"

[prompt.response]
format = "text"
required_keys = []
""".strip(),
    )

    with pytest.raises(ValueError, match="placeholder mismatch"):
        load_prompt_registry(tmp_path)


def test_load_prompt_registry_rejects_duplicate_ids(tmp_path: Path) -> None:
    content = """
[prompt]
id = "dup.id"
description = "Duplicate"
template = "{org_name}"

[prompt.placeholders]
org_name = "Organization"

[prompt.response]
format = "text"
required_keys = []
""".strip()
    _write_prompt(tmp_path / "prompts" / "a.toml", content)
    _write_prompt(tmp_path / "prompts" / "b.toml", content)

    with pytest.raises(ValueError, match="Duplicate prompt id"):
        load_prompt_registry(tmp_path)


def test_render_prompt_template_rejects_missing_values(tmp_path: Path) -> None:
    _write_prompt(
        tmp_path / "prompts" / "one.toml",
        """
[prompt]
id = "one"
description = "One"
template = "{org_name} in {org_location}"

[prompt.placeholders]
org_name = "Organization"
org_location = "Location"

[prompt.response]
format = "text"
required_keys = []
""".strip(),
    )

    registry = load_prompt_registry(tmp_path)
    prompt = registry["one"]
    with pytest.raises(ValueError, match="missing values"):
        render_prompt_template(prompt, {"org_name": "Org"})
