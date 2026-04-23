"""LLM-based evaluation: use AI to answer questions about each organization.

Loads scraped website content, fills in prompt templates, and sends them to
the OpenAI API. Stores structured answers per organization for later reporting.

Implementation maturity note:
This is a first-shot implementation based on earlier workflow assumptions.
Since discovery/review schema and decision columns changed over time, validate
that evaluate input/output expectations still align before treating results as
production-ready.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from openai import OpenAI

from benefind.config import DATA_DIR, PromptTemplate, Settings
from benefind.external_api import ExternalApiAccessError, classify_openai_access_error

logger = logging.getLogger(__name__)


def _save_evaluation(org_dir: Path, results: dict) -> Path:
    eval_path = org_dir / "evaluation.json"
    eval_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    return eval_path


def load_scraped_content(org_dir: Path, max_chars: int = 30_000) -> str:
    """Load all scraped markdown pages for an organization.

    Concatenates all .md files in the pages/ subdirectory. Truncates to
    max_chars to stay within LLM context limits.
    """
    candidate_dirs = [org_dir / "pages_cleaned", org_dir / "pages"]
    pages_dir: Path | None = None
    for candidate in candidate_dirs:
        if not candidate.exists():
            continue
        has_markdown = any(path.is_file() for path in candidate.glob("*.md"))
        if has_markdown:
            pages_dir = candidate
            break

    if pages_dir is None:
        return ""

    parts = []
    for md_file in sorted(pages_dir.glob("*.md")):
        content = md_file.read_text(encoding="utf-8")
        parts.append(f"--- Page: {md_file.stem} ---\n{content}")

    combined = "\n\n".join(parts)
    if len(combined) > max_chars:
        combined = combined[:max_chars] + "\n\n[... content truncated ...]"
        logger.debug("Truncated content for %s to %d chars", org_dir.name, max_chars)

    return combined


def fill_prompt(
    template: PromptTemplate,
    org_name: str,
    org_location: str,
    org_purpose: str,
    page_content: str,
) -> str:
    """Fill in a prompt template with organization-specific values."""
    return template.question.format(
        org_name=org_name,
        org_location=org_location,
        org_purpose=org_purpose,
        page_content=page_content,
    )


def ask_llm(
    prompt: str,
    settings: Settings,
    client: OpenAI | None = None,
) -> str:
    """Send a prompt to the LLM and return the response text."""
    if client is None:
        client = OpenAI()

    try:
        response = client.chat.completions.create(
            model=settings.llm.model,
            temperature=settings.llm.temperature,
            max_tokens=settings.llm.max_tokens,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a research assistant helping a Swiss nonprofit organization "
                        "evaluate potential charity partners. Be factual and concise. "
                        "If you are unsure about something, say so clearly. "
                        "Answer in the language of the question."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )
    except Exception as e:
        access_error = classify_openai_access_error(e)
        if access_error is not None:
            raise access_error
        raise

    return response.choices[0].message.content or ""


def evaluate_organization(
    org_id: str,
    org_name: str,
    org_location: str,
    org_purpose: str,
    org_dir: Path,
    settings: Settings,
    client: OpenAI | None = None,
) -> dict:
    """Run all configured prompt evaluations for a single organization.

    Returns a dict with prompt IDs as keys and answer dicts as values.
    Also saves the results as evaluation.json in the org directory.
    """
    page_content = load_scraped_content(org_dir)

    if not page_content:
        logger.warning("No scraped content for %s, evaluation will be limited.", org_name)

    results = {
        "_org_id": org_id,
        "_org_name": org_name,
        "_org_location": org_location,
        "_org_purpose": org_purpose,
        "_has_website_content": bool(page_content),
    }

    for template in settings.prompts:
        prompt = fill_prompt(template, org_name, org_location, org_purpose, page_content)

        try:
            answer = ask_llm(prompt, settings, client)
            results[template.id] = {
                "answer": answer,
                "answer_type": template.answer_type,
                "description": template.description,
            }
            logger.info("[%s] %s -> answered", org_name, template.id)
        except ExternalApiAccessError:
            eval_path = _save_evaluation(org_dir, results)
            logger.warning("Saved partial evaluation for %s to %s", org_name, eval_path)
            raise
        except Exception as e:
            logger.error("[%s] %s -> error: %s", org_name, template.id, e)
            results[template.id] = {
                "answer": f"ERROR: {e}",
                "answer_type": template.answer_type,
                "description": template.description,
                "_error": True,
            }

    # Save results
    eval_path = _save_evaluation(org_dir, results)
    logger.info("Saved evaluation for %s to %s", org_name, eval_path)

    return results


def evaluate_batch(
    organizations: list[dict],
    settings: Settings,
    name_column: str = "Bezeichnung",
    location_column: str = "Sitzort",
    purpose_column: str = "Zweck",
) -> list[dict]:
    """Evaluate a batch of organizations.

    Expects each org dict to have a '_website_url' key and an org directory
    already created by the scraper.
    """
    openai_api_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_api_key:
        raise ExternalApiAccessError(
            provider="OpenAI",
            reason="missing_api_key",
            details="OPENAI_API_KEY is not set",
        )

    client = OpenAI()
    results = []

    for i, org in enumerate(organizations):
        name = org.get(name_column, "")
        location = org.get(location_column, "")
        purpose = org.get(purpose_column, "")
        org_id = str(org.get("_org_id", "") or "").strip()
        org_dir = DATA_DIR / "orgs" / org_id if org_id else None

        logger.info("[%d/%d] Evaluating: %s", i + 1, len(organizations), name)

        if not org_id:
            logger.warning("Missing _org_id for %s, skipping evaluation.", name)
            results.append({"_org_name": name, "_error": "missing_org_id"})
            continue

        if not org_dir or not org_dir.exists():
            logger.warning("No scraped data for %s, skipping evaluation.", name)
            results.append({"_org_id": org_id, "_org_name": name, "_error": "no_scraped_data"})
            continue

        result = evaluate_organization(org_id, name, location, purpose, org_dir, settings, client)
        results.append(result)

    return results
