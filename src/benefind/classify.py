"""Classification workflow for multi-question LLM + review loops."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from benefind.cli_ui import (
    C_MUTED,
    ask_text,
    clear,
    confirm,
    console,
    make_actions_table,
    make_panel,
    print_skip,
    print_success,
    wait_for_key,
)
from benefind.config import CONFIG_DIR, DATA_DIR, Settings, render_prompt_template
from benefind.external_api import ExternalApiAccessError, classify_openai_access_error
from benefind.scrape_clean import load_latest_scrape_clean_summary

STATUS_WAITING_FOR_CLEAN_TEXT = "waiting_for_clean_text"


@dataclass(slots=True)
class QuestionSourceConfig:
    kind: str = "pages_cleaned"
    max_snippets: int = 18
    max_snippet_chars: int = 800
    min_snippet_chars: int = 120


@dataclass(slots=True)
class Rule:
    field: str
    op: str
    value: object = None


@dataclass(slots=True)
class PolicyRule:
    action: str
    priority: int
    mode: str
    conditions: list[Rule]


@dataclass(slots=True)
class ClassifyQuestion:
    id: str
    prompt_id: str
    enabled: bool
    order: int
    description: str
    source: QuestionSourceConfig
    policy_rules: list[PolicyRule]
    source_path: str
    fingerprint: str


@dataclass(slots=True)
class AskResult:
    payload: dict
    raw_response: str
    prompt: str
    route: str
    error: str


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    return cleaned.strip("_")


def _parse_rule(raw: dict, *, fallback_priority: int) -> PolicyRule:
    action = str(raw.get("action", "") or "").strip().lower()
    if action not in {"auto_accept", "auto_exclude", "review_needed"}:
        raise ValueError(f"Invalid classify policy action: {action!r}")
    mode = str(raw.get("mode", "all") or "all").strip().lower()
    if mode not in {"all", "any"}:
        raise ValueError(f"Invalid classify policy mode: {mode!r}")
    priority = int(raw.get("priority", fallback_priority) or fallback_priority)

    conditions_raw = raw.get("conditions", [])
    if not isinstance(conditions_raw, list) or not conditions_raw:
        raise ValueError("Classify policy rule requires non-empty conditions")

    conditions: list[Rule] = []
    for item in conditions_raw:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field", "") or "").strip()
        op = str(item.get("op", "") or "").strip().lower()
        if not field or not op:
            raise ValueError("Classify policy condition requires field and op")
        conditions.append(Rule(field=field, op=op, value=item.get("value")))

    if not conditions:
        raise ValueError("Classify policy rule has no usable conditions")
    return PolicyRule(action=action, priority=priority, mode=mode, conditions=conditions)


def load_classify_questions(config_dir: Path | None = None) -> list[ClassifyQuestion]:
    root = config_dir or CONFIG_DIR
    prompt_dir = root / "prompts"
    prompt_files = sorted(path for path in prompt_dir.glob("classify.*.toml") if path.is_file())
    if not prompt_files:
        raise ValueError(f"No classify prompt files found in {prompt_dir}")

    seen: set[str] = set()
    questions: list[ClassifyQuestion] = []
    for fallback_order, prompt_path in enumerate(prompt_files):
        raw_bytes = prompt_path.read_bytes()
        raw_data = tomllib.loads(raw_bytes.decode("utf-8"))
        fingerprint = hashlib.sha256(raw_bytes).hexdigest()

        prompt_raw = raw_data.get("prompt", {}) if isinstance(raw_data.get("prompt"), dict) else {}
        classify_raw = (
            raw_data.get("classify", {}) if isinstance(raw_data.get("classify"), dict) else {}
        )

        qid = str(classify_raw.get("id", "") or "").strip() or _slug(prompt_path.stem)
        if not qid:
            raise ValueError(f"Could not derive classify id from {prompt_path}")
        if qid in seen:
            raise ValueError(f"Duplicate classify question id: {qid}")
        seen.add(qid)

        source_raw = (
            classify_raw.get("source", {})
            if isinstance(classify_raw.get("source", {}), dict)
            else {}
        )
        policy_raw = (
            classify_raw.get("policy", {})
            if isinstance(classify_raw.get("policy", {}), dict)
            else {}
        )
        rules_raw_value = policy_raw.get("rules", [])
        rules_raw = rules_raw_value if isinstance(rules_raw_value, list) else []
        rules = [_parse_rule(raw, fallback_priority=index) for index, raw in enumerate(rules_raw)]

        source = QuestionSourceConfig(
            kind=str(source_raw.get("kind", "pages_cleaned") or "pages_cleaned").strip(),
            max_snippets=int(source_raw.get("max_snippets", 18) or 18),
            max_snippet_chars=int(source_raw.get("max_snippet_chars", 800) or 800),
            min_snippet_chars=int(source_raw.get("min_snippet_chars", 120) or 120),
        )

        questions.append(
            ClassifyQuestion(
                id=qid,
                prompt_id=str(prompt_raw.get("id", "") or "").strip(),
                enabled=bool(classify_raw.get("enabled", True)),
                order=int(classify_raw.get("order", fallback_order) or fallback_order),
                description=str(classify_raw.get("description", "") or "").strip(),
                source=source,
                policy_rules=rules,
                source_path=str(prompt_path),
                fingerprint=fingerprint,
            )
        )

    for question in questions:
        if not question.prompt_id:
            raise ValueError(f"Classify question '{question.id}' has no prompt.id")
        if not question.policy_rules:
            raise ValueError(f"Classify question '{question.id}' has no policy rules")

    questions.sort(key=lambda item: item.order)
    return [item for item in questions if item.enabled]


def question_columns(question_id: str) -> dict[str, str]:
    base = f"_classify_{_slug(question_id)}"
    return {
        "auto_result": f"{base}_auto_result",
        "auto_result_at": f"{base}_auto_result_at",
        "review_result": f"{base}_review_result",
        "review_result_at": f"{base}_review_result_at",
    }


def classify_lock_path() -> Path:
    path = DATA_DIR / "classify" / "registry_lock.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def build_registry_snapshot(questions: list[ClassifyQuestion]) -> dict[str, object]:
    return {
        "version": 1,
        "questions": [
            {
                "id": question.id,
                "order": question.order,
                "prompt_id": question.prompt_id,
                "source_path": question.source_path,
                "fingerprint": question.fingerprint,
            }
            for question in questions
        ],
    }


def load_registry_lock(path: Path | None = None) -> dict[str, object]:
    effective_path = path or classify_lock_path()
    if not effective_path.exists():
        return {}
    try:
        return json.loads(effective_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_registry_lock(questions: list[ClassifyQuestion], path: Path | None = None) -> None:
    effective_path = path or classify_lock_path()
    snapshot = build_registry_snapshot(questions)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    existing = load_registry_lock(effective_path)
    created_at = str(existing.get("created_at", "") or "").strip() or now
    snapshot["created_at"] = created_at
    snapshot["updated_at"] = now
    effective_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def registry_changes(
    questions: list[ClassifyQuestion],
    lock_payload: dict[str, object],
) -> dict[str, list[str]]:
    current = build_registry_snapshot(questions)
    current_rows = (
        current.get("questions", []) if isinstance(current.get("questions"), list) else []
    )
    locked_rows = (
        lock_payload.get("questions", []) if isinstance(lock_payload.get("questions"), list) else []
    )

    current_map = {
        str(row.get("id", "")).strip(): row
        for row in current_rows
        if isinstance(row, dict) and str(row.get("id", "")).strip()
    }
    locked_map = {
        str(row.get("id", "")).strip(): row
        for row in locked_rows
        if isinstance(row, dict) and str(row.get("id", "")).strip()
    }

    current_ids = [str(row.get("id", "")).strip() for row in current_rows if isinstance(row, dict)]
    locked_ids = [str(row.get("id", "")).strip() for row in locked_rows if isinstance(row, dict)]

    added = [qid for qid in current_ids if qid and qid not in locked_map]
    removed = [qid for qid in locked_ids if qid and qid not in current_map]

    reordered: list[str] = []
    for qid in current_ids:
        if not qid or qid not in locked_map:
            continue
        current_index = int(current_map[qid].get("order", -1) or -1)
        locked_index = int(locked_map[qid].get("order", -1) or -1)
        if current_index != locked_index:
            reordered.append(qid)

    fingerprint_changed: list[str] = []
    for qid in current_ids:
        if not qid or qid not in locked_map:
            continue
        locked_hash = str(locked_map[qid].get("fingerprint", "") or "").strip()
        current_hash = str(current_map[qid].get("fingerprint", "") or "").strip()
        if locked_hash != current_hash:
            fingerprint_changed.append(qid)

    return {
        "added": added,
        "removed": removed,
        "reordered": reordered,
        "fingerprint_changed": fingerprint_changed,
    }


def is_append_only_addition(
    questions: list[ClassifyQuestion],
    lock_payload: dict[str, object],
) -> bool:
    changes = registry_changes(questions, lock_payload)
    if changes["removed"] or changes["reordered"] or changes["fingerprint_changed"]:
        return False

    locked_rows = (
        lock_payload.get("questions", []) if isinstance(lock_payload.get("questions"), list) else []
    )
    locked_ids = [str(row.get("id", "")).strip() for row in locked_rows if isinstance(row, dict)]
    current_ids = [question.id for question in questions]
    return current_ids[: len(locked_ids)] == locked_ids


def progressed_question_ids(df: pd.DataFrame, questions: list[ClassifyQuestion]) -> set[str]:
    progressed: set[str] = set()
    for question in questions:
        cols = question_columns(question.id)
        auto_col = cols["auto_result"]
        review_col = cols["review_result"]
        if auto_col in df.columns and bool((df[auto_col].astype(str).str.strip() != "").any()):
            progressed.add(question.id)
        if review_col in df.columns and bool((df[review_col].astype(str).str.strip() != "").any()):
            progressed.add(question.id)
    return progressed


def changed_question_ids(change_map: dict[str, list[str]]) -> set[str]:
    changed: set[str] = set()
    for values in change_map.values():
        changed.update(values)
    return changed


def ensure_question_columns(df: pd.DataFrame, question_id: str) -> None:
    cols = question_columns(question_id)
    for value in cols.values():
        if value not in df.columns:
            df[value] = ""
        else:
            df[value] = df[value].astype(object).where(df[value].notna(), "")


def ensure_compact_classify_columns(df: pd.DataFrame) -> None:
    for col in ["_classify_version", "_classify_last_updated_at"]:
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].astype(object).where(df[col].notna(), "")


def cleanup_legacy_classify_columns(
    df: pd.DataFrame,
    questions: list[ClassifyQuestion],
) -> list[str]:
    keep = set()
    for question in questions:
        keep.update(question_columns(question.id).values())
    keep.update({"_classify_version", "_classify_last_updated_at"})

    remove: list[str] = []
    for col in list(df.columns):
        if not col.startswith("_classify_"):
            continue
        if col in keep:
            continue
        remove.append(col)
    if remove:
        df.drop(columns=remove, inplace=True, errors="ignore")
    return remove


def count_phase(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
) -> tuple[int, int]:
    cols = question_columns(question.id)
    active_mask = pd.Series(True, index=df.index)
    if "_excluded_reason" in df.columns:
        active_mask = df["_excluded_reason"].astype(str).str.strip() == ""

    org_ids = df["_org_id"].astype(str).str.strip()
    eligible_mask = org_ids.isin(eligible_org_ids)
    auto_result = df[cols["auto_result"]].astype(str).str.strip().str.lower()
    review_result = df[cols["review_result"]].astype(str).str.strip().str.lower()

    ask_pending = int((active_mask & eligible_mask & (auto_result == "")).sum())
    review_pending = int(
        (active_mask & (auto_result == "needs_review") & (review_result == "")).sum()
    )
    return ask_pending, review_pending


def load_eligible_org_ids() -> set[str]:
    summary_df = load_latest_scrape_clean_summary()
    if summary_df.empty:
        return set()
    status = summary_df["_scrape_clean_status"].astype(str).str.strip().str.lower()
    usable = pd.to_numeric(summary_df["_scrape_clean_usable_chars"], errors="coerce").fillna(0)
    mask = (status == "ok") & (usable > 0)
    return {
        str(value).strip()
        for value in summary_df.loc[mask, "_org_id"].tolist()
        if str(value).strip()
    }


def mark_ineligible_for_waiting(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
) -> int:
    cols = question_columns(question.id)
    org_ids = df["_org_id"].astype(str).str.strip()
    auto_result = df[cols["auto_result"]].astype(str).str.strip().str.lower()
    active_mask = (
        df["_excluded_reason"].astype(str).str.strip() == ""
        if "_excluded_reason" in df.columns
        else pd.Series(True, index=df.index)
    )
    mask = active_mask & ~org_ids.isin(eligible_org_ids) & (auto_result == "")
    if int(mask.sum()) == 0:
        return 0

    now = datetime.now(UTC).isoformat(timespec="seconds")
    df.loc[mask, cols["auto_result"]] = STATUS_WAITING_FOR_CLEAN_TEXT
    df.loc[mask, cols["auto_result_at"]] = now
    return int(mask.sum())


def restore_eligible_waiting_rows(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
) -> int:
    cols = question_columns(question.id)
    auto_result = df[cols["auto_result"]].astype(str).str.strip().str.lower()
    org_ids = df["_org_id"].astype(str).str.strip()
    active_mask = (
        df["_excluded_reason"].astype(str).str.strip() == ""
        if "_excluded_reason" in df.columns
        else pd.Series(True, index=df.index)
    )
    mask = (
        active_mask
        & org_ids.isin(eligible_org_ids)
        & (auto_result == STATUS_WAITING_FOR_CLEAN_TEXT)
    )
    if int(mask.sum()) == 0:
        return 0

    for key in [cols["auto_result"], cols["auto_result_at"]]:
        df.loc[mask, key] = ""
    return int(mask.sum())


def reset_question_rows(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
) -> int:
    cols = question_columns(question.id)
    active_mask = (
        df["_excluded_reason"].astype(str).str.strip() == ""
        if "_excluded_reason" in df.columns
        else pd.Series(True, index=df.index)
    )
    org_ids = df["_org_id"].astype(str).str.strip()
    mask = active_mask & org_ids.isin(eligible_org_ids)
    if int(mask.sum()) == 0:
        return 0

    for key in cols.values():
        df.loc[mask, key] = ""
    return int(mask.sum())


def _clean_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _snippet_id(index: int, path: Path) -> str:
    stem = _slug(path.stem)[:24] or "snippet"
    return f"s{index:02d}_{stem}"


def collect_evidence_snippets(org_id: str, question: ClassifyQuestion) -> list[dict[str, str]]:
    pages_dir = DATA_DIR / "orgs" / str(org_id).strip() / "pages_cleaned"
    if not pages_dir.exists() or not pages_dir.is_dir():
        return []

    snippets: list[dict[str, str]] = []
    files = sorted(path for path in pages_dir.glob("*.md") if path.is_file())
    for index, file_path in enumerate(files, start=1):
        if len(snippets) >= question.source.max_snippets:
            break
        try:
            raw_text = file_path.read_text(encoding="utf-8")
        except Exception:
            continue
        text = _clean_text(raw_text)
        if len(text) < question.source.min_snippet_chars:
            continue
        snippets.append(
            {
                "snippet_id": _snippet_id(index, file_path),
                "text": text[: question.source.max_snippet_chars],
            }
        )
    return snippets


def render_evidence_snippets(snippets: list[dict[str, str]]) -> str:
    if not snippets:
        return "- none"
    rows: list[str] = []
    for snippet in snippets:
        sid = str(snippet.get("snippet_id", "")).strip()
        text = str(snippet.get("text", "")).strip()
        rows.append(f"- snippet_id: {sid}\n  text: {text}")
    return "\n\n".join(rows)


def _extract_json_object(text: str) -> dict:
    stripped = (text or "").strip()
    if not stripped:
        return {}
    try:
        value = json.loads(stripped)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        value = json.loads(stripped[start : end + 1])
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        return {}


def _unique_text_list(values: object, *, limit: int) -> list[str]:
    if not isinstance(values, list):
        return []
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        if text.lower() in seen:
            continue
        seen.add(text.lower())
        output.append(text)
        if len(output) >= limit:
            break
    return output


def normalize_payload(payload: dict, *, allowed_snippet_ids: set[str]) -> dict:
    primary_focus = str(payload.get("primary_focus", "") or "").strip().lower()
    service_mode = str(payload.get("service_mode", "") or "").strip().lower()
    reason = str(payload.get("reason", "") or "").strip()
    try:
        confidence = float(payload.get("confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    try:
        primary_focus_confidence = float(payload.get("primary_focus_confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        primary_focus_confidence = 0.0
    primary_focus_confidence = max(0.0, min(1.0, primary_focus_confidence))

    secondary = [
        value.lower()
        for value in _unique_text_list(payload.get("secondary_focuses"), limit=3)
    ]
    subgroups = [
        value.lower() for value in _unique_text_list(payload.get("subgroup_labels"), limit=5)
    ]

    evidence_output: list[dict[str, str]] = []
    if isinstance(payload.get("evidence"), list):
        for item in payload.get("evidence"):
            if not isinstance(item, dict):
                continue
            snippet_id = str(item.get("snippet_id", "") or "").strip()
            quote = str(item.get("quote", "") or "").strip()
            if not snippet_id or not quote:
                continue
            if snippet_id not in allowed_snippet_ids:
                continue
            evidence_output.append({"snippet_id": snippet_id, "quote": quote})
            if len(evidence_output) >= 4:
                break

    return {
        "primary_focus": primary_focus,
        "primary_focus_confidence": primary_focus_confidence,
        "secondary_focuses": secondary,
        "subgroup_labels": subgroups,
        "service_mode": service_mode,
        "confidence": confidence,
        "evidence": evidence_output,
        "reason": reason,
    }


def validate_payload(payload: dict) -> None:
    required_keys = {
        "primary_focus",
        "primary_focus_confidence",
        "secondary_focuses",
        "subgroup_labels",
        "service_mode",
        "confidence",
        "evidence",
        "reason",
    }
    missing = sorted(required_keys - set(payload.keys()))
    if missing:
        raise ValueError(f"Missing required keys: {', '.join(missing)}")


def _field_value(payload: dict, field: str) -> object:
    cursor: object = payload
    for part in field.split("."):
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(part)
    return cursor


def _rule_match(rule: Rule, payload: dict) -> bool:
    value = _field_value(payload, rule.field)
    op = rule.op
    expected = rule.value

    if op == "eq":
        return value == expected
    if op == "neq":
        return value != expected
    if op == "in":
        if not isinstance(expected, list):
            return False
        return value in expected
    if op == "not_in":
        if not isinstance(expected, list):
            return False
        return value not in expected
    if op == "gte":
        try:
            return float(value) >= float(expected)
        except (TypeError, ValueError):
            return False
    if op == "lte":
        try:
            return float(value) <= float(expected)
        except (TypeError, ValueError):
            return False
    if op == "exists":
        return value is not None and str(value).strip() != ""
    if op == "contains":
        if isinstance(value, list):
            return expected in value
        return str(expected) in str(value)
    if op == "intersects":
        if not isinstance(expected, list) or not isinstance(value, list):
            return False
        value_set = {str(item).strip().lower() for item in value}
        expected_set = {str(item).strip().lower() for item in expected}
        return bool(value_set & expected_set)
    if op == "len_gte":
        try:
            return len(value) >= int(expected)  # type: ignore[arg-type]
        except Exception:
            return False
    if op == "len_eq":
        try:
            return len(value) == int(expected)  # type: ignore[arg-type]
        except Exception:
            return False
    return False


def decide_route(payload: dict, question: ClassifyQuestion) -> str:
    ordered_rules = sorted(question.policy_rules, key=lambda item: item.priority)
    for rule in ordered_rules:
        if rule.mode == "all":
            matched = all(_rule_match(cond, payload) for cond in rule.conditions)
        else:
            matched = any(_rule_match(cond, payload) for cond in rule.conditions)
        if not matched:
            continue
        if rule.action == "auto_accept":
            return "auto_accepted"
        if rule.action == "auto_exclude":
            return "auto_excluded"
        return "needs_review"
    return "needs_review"


def classify_once(
    org_name: str,
    org_location: str,
    snippets: list[dict[str, str]],
    question: ClassifyQuestion,
    settings: Settings,
) -> AskResult:
    prompt_def = settings.prompts.get(question.prompt_id)
    if prompt_def is None:
        raise ValueError(f"Prompt '{question.prompt_id}' is missing")

    prompt = render_prompt_template(
        prompt_def,
        {
            "org_name": org_name,
            "org_location": org_location or "-",
            "evidence_snippets": render_evidence_snippets(snippets),
        },
    )

    try:
        from openai import OpenAI
    except Exception as e:
        raise ValueError(f"OpenAI SDK unavailable: {e}") from e

    if not os.environ.get("OPENAI_API_KEY", ""):
        raise ExternalApiAccessError(
            provider="OpenAI",
            reason="missing_api_key",
            details="OPENAI_API_KEY is not set",
        )

    try:
        client = OpenAI()
        response = client.responses.create(
            model=settings.llm.model,
            input=prompt,
            temperature=float(settings.llm.temperature),
            max_output_tokens=int(settings.llm.max_tokens),
        )
    except Exception as e:
        access_error = classify_openai_access_error(e)
        if access_error is not None:
            raise access_error
        raise ValueError(f"LLM request failed: {e}") from e

    raw_response = str(getattr(response, "output_text", "") or "").strip()
    parsed = _extract_json_object(raw_response)
    if not parsed:
        raise ValueError("LLM did not return a JSON object")

    validate_payload(parsed)
    allowed = {str(item.get("snippet_id", "")).strip() for item in snippets}
    normalized = normalize_payload(parsed, allowed_snippet_ids=allowed)
    route = decide_route(normalized, question)
    return AskResult(
        payload=normalized,
        raw_response=raw_response,
        prompt=prompt,
        route=route,
        error="",
    )


def classify_org_dir(org_id: str, question_id: str) -> Path:
    path = DATA_DIR / "orgs" / str(org_id).strip() / "classify" / question_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_org_artifact(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_org_artifact(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def apply_auto_summary(
    df: pd.DataFrame,
    row_index: int,
    question: ClassifyQuestion,
    route: str,
) -> None:
    cols = question_columns(question.id)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    df.at[row_index, cols["auto_result"]] = route
    df.at[row_index, cols["auto_result_at"]] = now


def apply_review_summary(
    df: pd.DataFrame,
    row_index: int,
    question: ClassifyQuestion,
    decision: str,
) -> None:
    cols = question_columns(question.id)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    df.at[row_index, cols["review_result"]] = decision
    df.at[row_index, cols["review_result_at"]] = now


def update_classify_meta(df: pd.DataFrame) -> None:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    df["_classify_version"] = "v1"
    df["_classify_last_updated_at"] = now


def review_classifications(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    queue_indices: list[int],
    *,
    interactive: bool,
    save_callback=None,
) -> dict[str, int]:
    cols = question_columns(question.id)
    stats = {
        "accepted": 0,
        "excluded": 0,
        "skipped": 0,
        "remaining": 0,
    }

    if not queue_indices:
        return stats

    if not interactive:
        stats["remaining"] = len(queue_indices)
        return stats

    valid_keys = ["a", "x", "u", "s", "q"]
    for pos, idx in enumerate(queue_indices, start=1):
        row = df.loc[idx]
        org_id = str(row.get("_org_id", "") or "").strip()

        long_name_ascii = (
            "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnuetzigen Zwecken\n"
            "steuerbefreit sind"
        )
        long_name_utf8 = (
            "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnützigen Zwecken\n"
            "steuerbefreit sind"
        )
        org_name = str(
            row.get("Bezeichnung")
            or row.get("Name")
            or row.get("Institution")
            or row.get(long_name_ascii)
            or row.get(long_name_utf8)
            or "Unknown"
        )

        artifact_path = classify_org_dir(org_id, question.id) / "ask.json"
        ask_payload = read_org_artifact(artifact_path)
        normalized_payload = (
            ask_payload.get("normalized", {}) if isinstance(ask_payload, dict) else {}
        )

        auto_result = str(row.get(cols["auto_result"], "") or "-")
        payload_text = (
            json.dumps(normalized_payload, ensure_ascii=False) if normalized_payload else "-"
        )
        details = (
            f"[bold]{org_name}[/bold]\n"
            f"org_id: {org_id or '-'}\n"
            f"auto_result: {auto_result}\n"
            f"payload: {payload_text}"
        )

        while True:
            clear()
            header = f"[bold]Classify Review[/bold]  [dim]{pos}/{len(queue_indices)}[/dim]"
            console.print(make_panel(header, "Review Queue"))
            console.print(make_panel(details, question.id))
            console.print(
                make_actions_table(
                    [
                        ("a", "accept in scope"),
                        ("x", "exclude irrelevant"),
                        ("u", "keep pending"),
                        ("s", "skip"),
                        ("q", "quit"),
                    ]
                )
            )

            try:
                key = wait_for_key(valid_keys)
            except KeyboardInterrupt:
                stats["remaining"] += len(queue_indices) - pos + 1
                return stats

            if key == "q":
                stats["remaining"] += len(queue_indices) - pos + 1
                return stats
            if key == "s":
                stats["skipped"] += 1
                break
            if key == "u":
                note = ask_text("Optional note")
                review_payload = {
                    "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                    "question_id": question.id,
                    "org_id": org_id,
                    "decision": "pending",
                    "note": note,
                }
                review_path = classify_org_dir(org_id, question.id) / "review.json"
                write_org_artifact(review_path, review_payload)
                if save_callback is not None:
                    save_callback()
                stats["skipped"] += 1
                print_skip("Kept pending")
                break
            if key == "a":
                if not confirm("Accept as in-scope?", default=True):
                    continue
                apply_review_summary(df, idx, question, "accepted")
                write_org_artifact(
                    classify_org_dir(org_id, question.id) / "review.json",
                    {
                        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                        "question_id": question.id,
                        "org_id": org_id,
                        "decision": "accepted",
                    },
                )
                if save_callback is not None:
                    save_callback()
                stats["accepted"] += 1
                print_success("Accepted")
                break
            if key == "x":
                if not confirm("Exclude as IRRELEVANT_PURPOSE?", default=True):
                    continue
                apply_review_summary(df, idx, question, "excluded")
                write_org_artifact(
                    classify_org_dir(org_id, question.id) / "review.json",
                    {
                        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                        "question_id": question.id,
                        "org_id": org_id,
                        "decision": "excluded",
                    },
                )
                if save_callback is not None:
                    save_callback()
                stats["excluded"] += 1
                print_success("Excluded")
                break

    return stats


def format_debug_result(
    org_id: str,
    org_name: str,
    org_location: str,
    snippets: list[dict[str, str]],
    result: AskResult | None,
    error: str,
) -> None:
    console.print("[bold]Classify debug sample[/bold]")
    console.print(f"Org ID: {org_id or '-'}")
    console.print(f"Org: {org_name or '-'}")
    console.print(f"Location: {org_location or '-'}")
    console.print(f"Snippets: {len(snippets)}")
    for snippet in snippets[:4]:
        preview = str(snippet.get("text", ""))[:220]
        console.print(f"  - {snippet.get('snippet_id')}: {preview}")

    if error:
        console.print(f"[red]Error:[/red] {error}")
        return

    if result is None:
        console.print(f"[{C_MUTED}]No result.[/{C_MUTED}]")
        return

    console.print(f"Route: {result.route}")
    console.print(f"Payload: {json.dumps(result.payload, ensure_ascii=False)}")
    if result.raw_response:
        console.print("\n[bold]Raw response[/bold]")
        console.print(result.raw_response)
