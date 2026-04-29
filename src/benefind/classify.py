"""Classification workflow for multi-question LLM + review loops."""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import tomllib
import webbrowser
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd

from benefind.cli_ui import (
    C_MUTED,
    C_PRIMARY,
    C_SCORE_HIGH,
    C_SCORE_LOW,
    C_SCORE_MED,
    C_WARNING,
    ask_select,
    ask_text,
    clear,
    confirm,
    console,
    make_actions_table,
    make_kv_table,
    make_panel,
    print_success,
    print_warning,
    wait_for_key,
)
from benefind.config import CONFIG_DIR, DATA_DIR, Settings, render_prompt_template
from benefind.csv_io import ensure_text_columns
from benefind.exclusion_reasons import (
    EXCLUDE_REASON_OPTIONS,
    VALID_EXCLUDE_REASON_CODES,
    ExcludeReason,
)
from benefind.external_api import ExternalApiAccessError, classify_openai_access_error
from benefind.scrape_clean import load_latest_scrape_clean_summary

STATUS_WAITING_FOR_CLEAN_TEXT = "waiting_for_clean_text"
VERIFIED_PURPOSE_SNIPPET_ID = "__verified_purpose"


@dataclass(slots=True)
class QuestionSourceConfig:
    kind: str = "pages_cleaned"
    max_snippets: int = 18
    max_snippet_chars: int = 800
    max_total_snippet_chars: int | None = None
    min_snippet_chars: int = 0
    selection_mode: str = "file_order"
    keyword_priority_terms: list[str] = field(default_factory=list)
    keyword_priority_case_sensitive: bool = False
    keyword_priority_match_mode: str = "substring"
    keyword_priority_fill_mode: str = "fallback_file_order"

@dataclass(slots=True)
class Rule:
    field: str
    op: str
    value: object = None


@dataclass(slots=True)
class PolicyRule:
    rule_id: str
    action: str
    priority: int
    mode: str
    conditions: list[Rule]
    review_reason: str


@dataclass(slots=True)
class OutputFieldConfig:
    key: str
    kind: str
    required: bool = False
    lowercase: bool = False
    min_value: float | None = None
    max_value: float | None = None
    max_items: int | None = None
    unique_casefold: bool = False
    allowed: list[str] = field(default_factory=list)
    object_item_keys: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ReviewFieldConfig:
    label: str
    field: str
    format: str = "auto"
    confidence_field: str = ""


@dataclass(slots=True)
class ManualQuickAnswer:
    label: str
    values: dict[str, object]


@dataclass(slots=True)
class ClassifyQuestion:
    id: str
    prompt_id: str
    enabled: bool
    order: int
    description: str
    execution_mode: str
    source: QuestionSourceConfig
    policy_rules: list[PolicyRule]
    output_fields: list[OutputFieldConfig]
    manual_quick_answers: list[ManualQuickAnswer]
    conclude_apply_exclusion: bool
    strict_output_keys: bool
    ask_max_attempts: int
    review_fields: list[ReviewFieldConfig]
    source_path: str
    fingerprint: str


@dataclass(slots=True)
class AskResult:
    payload: dict
    raw_response: str
    prompt: str
    route: str
    route_reason: str
    error: str


@dataclass(slots=True)
class ManualAskOutcome:
    status: str
    result: AskResult | None = None
    payload: dict[str, object] = field(default_factory=dict)
    entry_method: str = "sequential"
    quick_answer_index: int = 0
    exclude_reason: ExcludeReason | None = None
    exclude_reason_note: str = ""


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    return cleaned.strip("_")


def _parse_rule(raw: dict, *, fallback_priority: int) -> PolicyRule:
    rule_id = str(raw.get("id", "") or "").strip() or f"rule_{fallback_priority}"
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
    return PolicyRule(
        rule_id=rule_id,
        action=action,
        priority=priority,
        mode=mode,
        conditions=conditions,
        review_reason=str(raw.get("review_reason", "") or "").strip(),
    )


def _parse_output_field(raw: dict) -> OutputFieldConfig:
    key = str(raw.get("key", "") or "").strip()
    kind = str(raw.get("kind", "") or "").strip().lower()
    if not key:
        raise ValueError("Classify output field requires key")
    if kind not in {"string", "number", "string_list", "object_list"}:
        raise ValueError(f"Classify output field '{key}' has invalid kind: {kind!r}")
    min_value = raw.get("min")
    max_value = raw.get("max")
    max_items = raw.get("max_items")
    allowed_raw = raw.get("allowed", [])
    if not isinstance(allowed_raw, list):
        raise ValueError(f"Classify output field '{key}' requires allowed to be a list")
    object_item_keys_raw = raw.get("object_item_keys", [])
    if not isinstance(object_item_keys_raw, list):
        raise ValueError(
            f"Classify output field '{key}' requires object_item_keys to be a list"
        )
    return OutputFieldConfig(
        key=key,
        kind=kind,
        required=bool(raw.get("required", False)),
        lowercase=bool(raw.get("lowercase", False)),
        min_value=float(min_value) if min_value is not None else None,
        max_value=float(max_value) if max_value is not None else None,
        max_items=int(max_items) if max_items is not None else None,
        unique_casefold=bool(raw.get("unique_casefold", False)),
        allowed=[str(item).strip() for item in allowed_raw if str(item).strip()],
        object_item_keys=[
            str(item).strip() for item in object_item_keys_raw if str(item).strip()
        ],
    )


def _parse_manual_quick_answer(raw: dict, *, question_id: str) -> ManualQuickAnswer:
    label = str(raw.get("label", "") or "").strip()
    if not label:
        raise ValueError(f"Classify question '{question_id}' manual quick answer requires label")
    values_raw = raw.get("values", {})
    if not isinstance(values_raw, dict) or not values_raw:
        raise ValueError(
            f"Classify question '{question_id}' manual quick answer '{label}' requires values table"
        )
    return ManualQuickAnswer(label=label, values=dict(values_raw))


def _parse_review_field(raw: dict) -> ReviewFieldConfig:
    label = str(raw.get("label", "") or "").strip()
    field = str(raw.get("field", "") or "").strip()
    if not label or not field:
        raise ValueError("Classify review field requires label and field")
    return ReviewFieldConfig(
        label=label,
        field=field,
        format=str(raw.get("format", "auto") or "auto").strip().lower(),
        confidence_field=str(raw.get("confidence_field", "") or "").strip(),
    )


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
        keyword_priority_raw = (
            source_raw.get("keyword_priority", {})
            if isinstance(source_raw.get("keyword_priority", {}), dict)
            else {}
        )
        policy_raw = (
            classify_raw.get("policy", {})
            if isinstance(classify_raw.get("policy", {}), dict)
            else {}
        )
        output_raw = (
            classify_raw.get("output", {})
            if isinstance(classify_raw.get("output", {}), dict)
            else {}
        )
        execution_raw = (
            classify_raw.get("execution", {})
            if isinstance(classify_raw.get("execution", {}), dict)
            else {}
        )
        manual_raw = (
            classify_raw.get("manual", {})
            if isinstance(classify_raw.get("manual", {}), dict)
            else {}
        )
        conclude_raw = (
            classify_raw.get("conclude", {})
            if isinstance(classify_raw.get("conclude", {}), dict)
            else {}
        )
        review_raw = (
            classify_raw.get("review", {})
            if isinstance(classify_raw.get("review", {}), dict)
            else {}
        )
        rules_raw_value = policy_raw.get("rules", [])
        rules_raw = rules_raw_value if isinstance(rules_raw_value, list) else []
        rules = [_parse_rule(raw, fallback_priority=index) for index, raw in enumerate(rules_raw)]
        output_fields_raw_value = output_raw.get("fields", [])
        output_fields_raw = (
            output_fields_raw_value if isinstance(output_fields_raw_value, list) else []
        )
        output_fields = [
            _parse_output_field(raw) for raw in output_fields_raw if isinstance(raw, dict)
        ]
        quick_answers_raw_value = manual_raw.get("quick_answers", [])
        quick_answers_raw = (
            quick_answers_raw_value if isinstance(quick_answers_raw_value, list) else []
        )
        manual_quick_answers = [
            _parse_manual_quick_answer(raw, question_id=qid)
            for raw in quick_answers_raw
            if isinstance(raw, dict)
        ]
        review_fields_raw_value = review_raw.get("fields", [])
        review_fields_raw = (
            review_fields_raw_value if isinstance(review_fields_raw_value, list) else []
        )
        review_fields = [
            _parse_review_field(raw) for raw in review_fields_raw if isinstance(raw, dict)
        ]
        terms_raw = keyword_priority_raw.get("terms", [])
        if not isinstance(terms_raw, list):
            raise ValueError(
                f"Classify question '{qid}' requires source.keyword_priority.terms to be a list"
            )

        source = QuestionSourceConfig(
            kind=str(source_raw.get("kind", "pages_cleaned") or "pages_cleaned").strip(),
            max_snippets=int(source_raw.get("max_snippets", 18) or 18),
            max_snippet_chars=int(source_raw.get("max_snippet_chars", 800) or 800),
            max_total_snippet_chars=(
                int(source_raw.get("max_total_snippet_chars"))
                if source_raw.get("max_total_snippet_chars") not in {None, ""}
                else None
            ),
            min_snippet_chars=int(source_raw.get("min_snippet_chars", 0) or 0),
            selection_mode=str(source_raw.get("selection_mode", "file_order") or "file_order")
            .strip()
            .lower(),
            keyword_priority_terms=[
                str(item).strip()
                for item in terms_raw
                if str(item).strip()
            ],
            keyword_priority_case_sensitive=bool(
                keyword_priority_raw.get("case_sensitive", False)
            ),
            keyword_priority_match_mode=str(
                keyword_priority_raw.get("match_mode", "substring") or "substring"
            )
            .strip()
            .lower(),
            keyword_priority_fill_mode=str(
                keyword_priority_raw.get("fill_mode", "fallback_file_order")
                or "fallback_file_order"
            )
            .strip()
            .lower(),
        )

        if source.selection_mode not in {"file_order", "keyword_priority"}:
            raise ValueError(
                f"Classify question '{qid}' has invalid source.selection_mode: "
                f"{source.selection_mode!r}"
            )
        if source.keyword_priority_match_mode not in {"substring", "word_boundary"}:
            raise ValueError(
                f"Classify question '{qid}' has invalid source.keyword_priority.match_mode: "
                f"{source.keyword_priority_match_mode!r}"
            )
        if source.keyword_priority_fill_mode not in {"fallback_file_order", "matches_only"}:
            raise ValueError(
                f"Classify question '{qid}' has invalid source.keyword_priority.fill_mode: "
                f"{source.keyword_priority_fill_mode!r}"
            )
        if source.max_total_snippet_chars is not None and source.max_total_snippet_chars <= 0:
            raise ValueError(
                f"Classify question '{qid}' has invalid source.max_total_snippet_chars: "
                f"{source.max_total_snippet_chars!r}"
            )
        if not output_fields:
            raise ValueError(f"Classify question '{qid}' requires classify.output.fields")
        output_keys = [field.key for field in output_fields]
        if len(set(output_keys)) != len(output_keys):
            raise ValueError(f"Classify question '{qid}' has duplicate output field keys")

        required_keys_raw = prompt_raw.get("response", {}).get("required_keys", [])
        if not isinstance(required_keys_raw, list):
            raise ValueError(
                f"Classify question '{qid}' prompt.response.required_keys must be a list"
            )
        required_keys = [str(item).strip() for item in required_keys_raw if str(item).strip()]
        missing_output_defs = sorted(set(required_keys) - set(output_keys))
        if missing_output_defs:
            raise ValueError(
                f"Classify question '{qid}' missing classify.output.fields definitions for: "
                f"{', '.join(missing_output_defs)}"
            )

        questions.append(
            ClassifyQuestion(
                id=qid,
                prompt_id=str(prompt_raw.get("id", "") or "").strip(),
                enabled=bool(classify_raw.get("enabled", True)),
                order=int(classify_raw.get("order", fallback_order) or fallback_order),
                description=str(classify_raw.get("description", "") or "").strip(),
                execution_mode=str(execution_raw.get("mode", "llm") or "llm").strip().lower(),
                source=source,
                policy_rules=rules,
                output_fields=output_fields,
                manual_quick_answers=manual_quick_answers,
                conclude_apply_exclusion=bool(conclude_raw.get("apply_exclusion", True)),
                strict_output_keys=bool(output_raw.get("strict_output_keys", False)),
                ask_max_attempts=(
                    int(classify_raw.get("ask_max_attempts"))
                    if classify_raw.get("ask_max_attempts") not in {None, ""}
                    else 2
                ),
                review_fields=review_fields,
                source_path=str(prompt_path),
                fingerprint=fingerprint,
            )
        )

    for question in questions:
        if question.execution_mode not in {"llm", "manual"}:
            raise ValueError(
                f"Classify question '{question.id}' has invalid execution.mode: "
                f"{question.execution_mode!r}"
            )
        if question.execution_mode == "llm" and not question.prompt_id:
            raise ValueError(f"Classify question '{question.id}' has no prompt.id")
        if question.execution_mode == "llm" and not question.policy_rules:
            raise ValueError(f"Classify question '{question.id}' has no policy rules")
        if question.ask_max_attempts <= 0:
            raise ValueError(f"Classify question '{question.id}' has invalid ask_max_attempts")

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
    ensure_text_columns(df, list(cols.values()))


def ensure_compact_classify_columns(df: pd.DataFrame) -> None:
    ensure_text_columns(df, ["_classify_version", "_classify_last_updated_at"])


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


def _keyword_match_count(
    text: str,
    terms: list[str],
    *,
    case_sensitive: bool,
    match_mode: str,
) -> int:
    if not terms or not text:
        return 0

    if match_mode == "substring":
        haystack = text if case_sensitive else text.lower()
        count = 0
        for term in terms:
            needle = term if case_sensitive else term.lower()
            if not needle:
                continue
            count += haystack.count(needle)
        return count

    if match_mode == "word_boundary":
        flags = 0 if case_sensitive else re.IGNORECASE
        count = 0
        for term in terms:
            token = str(term or "").strip()
            if not token:
                continue
            pattern = re.compile(rf"\b{re.escape(token)}\b", flags)
            count += len(pattern.findall(text))
        return count

    return 0


def collect_evidence_snippets(org_id: str, question: ClassifyQuestion) -> list[dict[str, str]]:
    pages_dir = DATA_DIR / "orgs" / str(org_id).strip() / "pages_cleaned"
    if not pages_dir.exists() or not pages_dir.is_dir():
        return []

    candidates: list[dict[str, object]] = []
    files = sorted(path for path in pages_dir.glob("*.md") if path.is_file())
    for index, file_path in enumerate(files, start=1):
        try:
            raw_text = file_path.read_text(encoding="utf-8")
        except Exception:
            continue
        text = _clean_text(raw_text)
        if len(text) < question.source.min_snippet_chars:
            continue
        candidates.append(
            {
                "snippet_id": _snippet_id(index, file_path),
                "text": text[: question.source.max_snippet_chars],
                "_file_order": index,
                "_match_count": _keyword_match_count(
                    text,
                    question.source.keyword_priority_terms,
                    case_sensitive=question.source.keyword_priority_case_sensitive,
                    match_mode=question.source.keyword_priority_match_mode,
                ),
            }
        )

    if question.source.selection_mode == "keyword_priority":
        matched = [row for row in candidates if int(row.get("_match_count", 0) or 0) > 0]
        unmatched = [row for row in candidates if int(row.get("_match_count", 0) or 0) <= 0]
        matched.sort(
            key=lambda row: (
                -int(row.get("_match_count", 0) or 0),
                int(row.get("_file_order", 0) or 0),
            )
        )
        unmatched.sort(key=lambda row: int(row.get("_file_order", 0) or 0))
        if question.source.keyword_priority_fill_mode == "matches_only":
            selected = matched
        else:
            selected = matched + unmatched
    else:
        selected = sorted(candidates, key=lambda row: int(row.get("_file_order", 0) or 0))

    snippets: list[dict[str, str]] = []
    total_chars = 0
    max_total = question.source.max_total_snippet_chars
    for row in selected:
        if len(snippets) >= question.source.max_snippets:
            break
        text = str(row.get("text", "") or "").strip()
        if not text:
            continue
        if max_total is not None:
            remaining = max_total - total_chars
            if remaining <= 0:
                break
            if len(text) > remaining:
                text = text[:remaining].strip()
                if not text:
                    break
        snippets.append(
            {
                "snippet_id": str(row.get("snippet_id", "") or "").strip(),
                "text": text,
            }
        )
        total_chars += len(text)
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


def count_words(text: str) -> int:
    return len(re.findall(r"\S+", str(text or "").strip()))


def _is_q07_word_count_error(error_text: str) -> bool:
    return str(error_text or "").strip().startswith("summary_de word count out of range:")


def _format_facts_list(facts: list[tuple[str, object]]) -> str:
    rows: list[str] = []
    for key, value in facts:
        if isinstance(value, list):
            cleaned = [str(item).strip() for item in value if str(item).strip()]
            text = ", ".join(cleaned) if cleaned else "-"
        elif value is None:
            text = "-"
        else:
            text = str(value).strip() or "-"
        rows.append(f"- {key}: {text}")
    return "\n".join(rows)


def _normalized_for_org_question(org_id: str, question_id: str) -> dict[str, object]:
    ask_path = classify_org_dir(org_id, question_id) / "ask.json"
    ask_payload = read_org_artifact(ask_path)
    normalized, _ = _effective_normalized_payload(ask_payload)
    return normalized if isinstance(normalized, dict) else {}


def build_org_facts_compact(
    *,
    org_id: str,
    org_name: str,
    org_location: str,
    website_url: str,
) -> str:
    q01 = _normalized_for_org_question(org_id, "q01_target_focus")
    q02 = _normalized_for_org_question(org_id, "q02_regional_focus")
    q03 = _normalized_for_org_question(org_id, "q03_donation_ask")
    q04 = _normalized_for_org_question(org_id, "q04_primary_target_group")
    q05 = _normalized_for_org_question(org_id, "q05_founded_year")
    q06 = _normalized_for_org_question(org_id, "q06_financials_manual")

    facts: list[tuple[str, object]] = [
        ("name", org_name),
        ("ort", org_location),
        ("website", website_url),
        ("q01_primary_focus", q01.get("primary_focus", "")),
        ("q01_service_mode", q01.get("service_mode", "")),
        ("q01_subgroups", q01.get("subgroup_labels", [])),
        ("q02_scope", q02.get("scope", "")),
        ("q02_locations", q02.get("extracted_locations", [])),
        ("q03_spendenaufruf", q03.get("asks_for_donations", "")),
        ("q04_primary_target_group", q04.get("category", "")),
        ("q04_subgroups", q04.get("subgroup_labels", [])),
        ("q05_founded_year", q05.get("founded_year", "")),
        ("q06_finanz_status", q06.get("information_status", "")),
        ("q06_fiscal_year", q06.get("fiscal_year", None)),
        ("q06_total_earnings_chf", q06.get("total_earnings_chf", None)),
        ("q06_donated_amount_chf", q06.get("donated_amount_chf", None)),
    ]
    return _format_facts_list(facts)


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


def _normalize_text_list(
    values: object,
    *,
    max_items: int | None,
    lowercase: bool,
    unique_casefold: bool,
) -> list[str]:
    if not isinstance(values, list):
        raise ValueError("Expected array value")
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        if lowercase:
            text = text.lower()
        key = text.casefold() if unique_casefold else text
        if unique_casefold and key in seen:
            continue
        seen.add(key)
        output.append(text)
        if max_items is not None and len(output) >= max_items:
            break
    return output


def validate_payload(
    payload: dict,
    *,
    question: ClassifyQuestion,
    required_keys: list[str],
) -> None:
    if not isinstance(payload, dict):
        raise ValueError("LLM output must be a JSON object")
    missing = sorted(set(required_keys) - set(payload.keys()))
    if missing:
        raise ValueError(f"Missing required keys: {', '.join(missing)}")
    if question.strict_output_keys:
        allowed_keys = {field.key for field in question.output_fields}
        extras = sorted(set(payload.keys()) - allowed_keys)
        if extras:
            raise ValueError(f"Unexpected keys: {', '.join(extras)}")


def normalize_payload(
    payload: dict,
    *,
    question: ClassifyQuestion,
    allowed_snippet_ids: set[str],
) -> dict:
    output: dict[str, object] = {}
    for field_cfg in question.output_fields:
        raw_value = payload.get(field_cfg.key)
        if field_cfg.kind == "string":
            text = str(raw_value or "").strip()
            if field_cfg.lowercase:
                text = text.lower()
            if field_cfg.allowed and text not in field_cfg.allowed:
                raise ValueError(
                    f"Invalid value for '{field_cfg.key}': {text!r}. "
                    f"Allowed: {field_cfg.allowed}"
                )
            output[field_cfg.key] = text
            continue

        if field_cfg.kind == "number":
            if raw_value in {None, ""}:
                if field_cfg.required:
                    raise ValueError(f"Missing numeric value for '{field_cfg.key}'")
                output[field_cfg.key] = None
                continue
            try:
                number = float(raw_value)
            except (TypeError, ValueError) as e:
                raise ValueError(f"Invalid numeric value for '{field_cfg.key}'") from e
            if field_cfg.min_value is not None:
                number = max(field_cfg.min_value, number)
            if field_cfg.max_value is not None:
                number = min(field_cfg.max_value, number)
            output[field_cfg.key] = number
            continue

        if field_cfg.kind == "string_list":
            values = _normalize_text_list(
                raw_value,
                max_items=field_cfg.max_items,
                lowercase=field_cfg.lowercase,
                unique_casefold=field_cfg.unique_casefold,
            )
            if field_cfg.allowed:
                invalid = [item for item in values if item not in field_cfg.allowed]
                if invalid:
                    raise ValueError(
                        f"Invalid values for '{field_cfg.key}': {invalid!r}. "
                        f"Allowed: {field_cfg.allowed}"
                    )
            output[field_cfg.key] = values
            continue

        if field_cfg.kind == "object_list":
            if not isinstance(raw_value, list):
                raise ValueError(f"Expected array value for '{field_cfg.key}'")
            objects: list[dict[str, str]] = []
            for item in raw_value:
                if not isinstance(item, dict):
                    continue
                if field_cfg.object_item_keys:
                    keys = sorted(str(key).strip() for key in item.keys())
                    expected = sorted(field_cfg.object_item_keys)
                    if keys != expected:
                        raise ValueError(
                            f"Invalid object keys for '{field_cfg.key}': {keys!r}, "
                            f"expected {expected!r}"
                        )
                obj: dict[str, str] = {}
                for key in field_cfg.object_item_keys:
                    obj[key] = str(item.get(key, "") or "").strip()
                if "snippet_id" in obj and obj["snippet_id"] not in allowed_snippet_ids:
                    continue
                if "snippet_id" in obj and not obj["snippet_id"]:
                    continue
                if "quote" in obj and not obj["quote"]:
                    continue
                objects.append(obj)
                if field_cfg.max_items is not None and len(objects) >= field_cfg.max_items:
                    break
            output[field_cfg.key] = objects
            continue

        raise ValueError(f"Unsupported output field kind: {field_cfg.kind!r}")

    return output


def required_output_keys(question: ClassifyQuestion) -> list[str]:
    return [field.key for field in question.output_fields if field.required]


def validate_required_output_fields(payload: dict[str, object], question: ClassifyQuestion) -> None:
    for field_cfg in question.output_fields:
        if not field_cfg.required:
            continue
        value = payload.get(field_cfg.key)
        if field_cfg.kind == "string":
            if str(value or "").strip() == "":
                raise ValueError(f"Required field is empty: {field_cfg.key}")
            continue
        if field_cfg.kind == "number":
            if value is None or str(value).strip() == "":
                raise ValueError(f"Required field is empty: {field_cfg.key}")
            continue
        if field_cfg.kind in {"string_list", "object_list"}:
            if not isinstance(value, list) or not value:
                raise ValueError(f"Required field has no items: {field_cfg.key}")
    if question.id == "q07_org_summary_de":
        summary = str(payload.get("summary_de", "") or "").strip()
        words = count_words(summary)
        if words < 100 or words > 150:
            raise ValueError(f"summary_de word count out of range: {words} (expected 100-150)")


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


def decide_route(payload: dict, question: ClassifyQuestion) -> tuple[str, str]:
    ordered_rules = sorted(question.policy_rules, key=lambda item: item.priority)
    for rule in ordered_rules:
        if rule.mode == "all":
            matched = all(_rule_match(cond, payload) for cond in rule.conditions)
        else:
            matched = any(_rule_match(cond, payload) for cond in rule.conditions)
        if not matched:
            continue
        if rule.action == "auto_accept":
            return "auto_accepted", rule.review_reason or f"Matched policy rule {rule.rule_id}."
        if rule.action == "auto_exclude":
            return "auto_excluded", rule.review_reason or f"Matched policy rule {rule.rule_id}."
        return "needs_review", rule.review_reason or f"Matched policy rule {rule.rule_id}."
    return "needs_review", "No policy rule matched; default manual review."


def classify_once(
    org_name: str,
    org_location: str,
    verified_purpose: str,
    snippets: list[dict[str, str]],
    org_facts_compact: str,
    question: ClassifyQuestion,
    settings: Settings,
) -> AskResult:
    prompt_def = settings.prompts.get(question.prompt_id)
    if prompt_def is None:
        raise ValueError(f"Prompt '{question.prompt_id}' is missing")

    template_values: dict[str, object] = {}
    for placeholder in prompt_def.placeholders:
        if placeholder == "org_name":
            template_values[placeholder] = org_name
        elif placeholder == "org_location":
            template_values[placeholder] = org_location or "-"
        elif placeholder == "verified_purpose":
            template_values[placeholder] = verified_purpose or "-"
        elif placeholder == "evidence_snippets":
            template_values[placeholder] = render_evidence_snippets(snippets)
        elif placeholder == "org_facts_compact":
            template_values[placeholder] = org_facts_compact or "-"
        else:
            raise ValueError(
                f"Prompt '{prompt_def.id}' uses unsupported classify placeholder: {placeholder!r}"
            )

    prompt = render_prompt_template(prompt_def, template_values)

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

    required_keys = list(prompt_def.response_required_keys)
    allowed = {str(item.get("snippet_id", "")).strip() for item in snippets}
    if str(verified_purpose or "").strip():
        allowed.add(VERIFIED_PURPOSE_SNIPPET_ID)
    client = OpenAI()
    last_error = ""
    first_failed_summary: str = ""
    first_failed_count: int = 0
    first_failed_captured = False
    for attempt in range(1, question.ask_max_attempts + 1):
        attempt_prompt = prompt
        if attempt > 1 and last_error:
            attempt_prompt = (
                f"{prompt}\n\n"
                "Validation feedback from previous attempt:\n"
                f"- {last_error}\n"
                "Please fix this and return one valid JSON object only."
            )
            if (
                question.id == "q07_org_summary_de"
                and first_failed_captured
                and first_failed_summary
            ):
                attempt_prompt = (
                    f"{attempt_prompt}\n\n"
                    "Zusatz fuer Korrektur (automatisch gemessen):\n"
                    f"- Erster Entwurf hatte {first_failed_count} Woerter.\n"
                    "- Erster Entwurf:\n"
                    f"{first_failed_summary}\n"
                    "Bitte liefere eine ueberarbeitete Fassung mit 100 bis 150 Woertern."
                )
        try:
            response = client.responses.create(
                model=settings.llm.model,
                input=attempt_prompt,
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
            last_error = "LLM did not return a JSON object"
            continue

        normalized: dict[str, object] | None = None
        try:
            validate_payload(parsed, question=question, required_keys=required_keys)
            normalized = normalize_payload(parsed, question=question, allowed_snippet_ids=allowed)
            validate_required_output_fields(normalized, question)
        except ValueError as e:
            last_error = str(e)
            is_q07_wc_error = (
                question.id == "q07_org_summary_de"
                and normalized is not None
                and _is_q07_word_count_error(last_error)
            )

            if is_q07_wc_error and isinstance(normalized, dict) and not first_failed_captured:
                summary = str(normalized.get("summary_de", "") or "").strip()
                if summary:
                    first_failed_summary = summary
                    first_failed_count = count_words(summary)
                    first_failed_captured = True

            if attempt < question.ask_max_attempts and (
                (question.id != "q07_org_summary_de") or is_q07_wc_error
            ):
                continue

            if is_q07_wc_error and isinstance(normalized, dict):
                summary = str(normalized.get("summary_de", "") or "").strip()
                words = count_words(summary)
                return AskResult(
                    payload=normalized,
                    raw_response=raw_response,
                    prompt=prompt,
                    route="needs_review",
                    route_reason=(
                        "Summary generated but word count validation failed "
                        f"({words} words; expected 100-150)."
                    ),
                    error="",
                )
            raise

        route, route_reason = decide_route(normalized, question)
        return AskResult(
            payload=normalized,
            raw_response=raw_response,
            prompt=prompt,
            route=route,
            route_reason=route_reason,
            error="",
        )

    raise ValueError(last_error or "Classify ask failed after retries")


def _manual_default_value(field_cfg: OutputFieldConfig) -> object:
    if field_cfg.kind == "string":
        return ""
    if field_cfg.kind == "number":
        return None
    if field_cfg.kind in {"string_list", "object_list"}:
        return []
    return ""


def _manual_seed_payload(
    question: ClassifyQuestion,
    current_payload: dict[str, object],
) -> dict[str, object]:
    seeded: dict[str, object] = {}
    for field_cfg in question.output_fields:
        if field_cfg.key in current_payload:
            seeded[field_cfg.key] = current_payload[field_cfg.key]
            continue
        seeded[field_cfg.key] = _manual_default_value(field_cfg)
    return seeded


def _manual_field_order(question: ClassifyQuestion) -> list[OutputFieldConfig]:
    required = [field for field in question.output_fields if field.required]
    optional = [field for field in question.output_fields if not field.required]
    return required + optional


def _manual_format_summary_value(value: object, kind: str) -> str:
    if kind == "number":
        if value is None or str(value).strip() == "":
            return f"[{C_MUTED}]-[/{C_MUTED}]"
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value)
        if number.is_integer():
            return str(int(number))
        return f"{number:.4f}".rstrip("0").rstrip(".")
    if kind in {"string_list", "object_list"}:
        if not isinstance(value, list) or not value:
            return f"[{C_MUTED}]-[/{C_MUTED}]"
        if kind == "string_list":
            return ", ".join(str(item) for item in value)
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    text = str(value or "").strip()
    return text if text else f"[{C_MUTED}]-[/{C_MUTED}]"


def _manual_parse_field_value(raw_text: str, field_cfg: OutputFieldConfig) -> object:
    text = str(raw_text or "").strip()
    if field_cfg.kind == "number":
        if not text:
            if field_cfg.required:
                raise ValueError(f"Required field is empty: {field_cfg.key}")
            return None
        try:
            return float(text)
        except ValueError as e:
            raise ValueError(f"Invalid number: {text!r}") from e
    return _parse_edit_value(text, field_cfg)


def _manual_pick_quick_answer(question: ClassifyQuestion) -> tuple[ManualQuickAnswer, int] | None:
    if not question.manual_quick_answers:
        print_warning("No quick answers configured for this question.")
        return None
    options = question.manual_quick_answers[:9]
    lines = ["Press 1-9 to apply a predefined answer."]
    for index, quick_answer in enumerate(options, start=1):
        lines.append(f"{index}. {quick_answer.label}")
    lines.append("q. cancel")
    console.print(make_panel("\n".join(lines), "Quick Answers"))
    valid = [str(index) for index in range(1, len(options) + 1)] + ["q"]
    selected = wait_for_key(valid, prompt="Quick answer: ")
    if selected == "q":
        return None
    quick_index = int(selected)
    return options[quick_index - 1], quick_index


def _manual_apply_quick_answer(
    *,
    question: ClassifyQuestion,
    current_payload: dict[str, object],
    quick_answer: ManualQuickAnswer,
    allowed_snippet_ids: set[str],
) -> dict[str, object]:
    draft = dict(current_payload)
    draft.update(quick_answer.values)
    normalized = normalize_payload(
        draft,
        question=question,
        allowed_snippet_ids=allowed_snippet_ids,
    )
    validate_required_output_fields(normalized, question)
    return normalized


def _parse_quick_answer_command(
    command: str,
    *,
    max_answers: int,
) -> int | None:
    normalized = command.strip().lower()
    if normalized.startswith(":"):
        normalized = normalized[1:]
    if not normalized.startswith("p"):
        return None
    suffix = normalized[1:].strip()
    if suffix == "":
        return 0
    if not suffix.isdigit():
        return None
    index = int(suffix)
    if index < 1 or index > min(max_answers, 9):
        return None
    return index


def manual_ask_once(
    *,
    question: ClassifyQuestion,
    org_name: str,
    org_location: str,
    website_url: str,
    current_payload: dict[str, object],
    allowed_snippet_ids: set[str],
) -> ManualAskOutcome:
    ordered_fields = _manual_field_order(question)
    if not ordered_fields:
        return ManualAskOutcome(status="error")

    draft = _manual_seed_payload(question, current_payload)
    pos = 0
    quick_answer_used = 0
    entry_method = "sequential"
    normalized_payload: dict[str, object] = {}

    def open_url(url: str) -> bool:
        value = str(url or "").strip()
        if not value:
            return False
        try:
            return bool(webbrowser.open(value))
        except Exception:
            return False

    def show_entry_context(*, stage: str) -> None:
        clear()
        rows = [
            ("Stage", stage),
            ("Question", question.id),
            ("Organization", f"[{C_PRIMARY}]{org_name or '-'}[/{C_PRIMARY}]"),
            ("Location", org_location or f"[{C_MUTED}]-[/{C_MUTED}]"),
            ("Website", website_url or f"[{C_MUTED}]-[/{C_MUTED}]"),
        ]
        console.print(make_panel(make_kv_table(rows), "Classify Manual Ask"))

    while pos < len(ordered_fields):
        field_cfg = ordered_fields[pos]
        required_text = "required" if field_cfg.required else "optional"
        default_text = _format_edit_default(draft.get(field_cfg.key), field_cfg.kind)
        show_entry_context(stage=f"Field {pos + 1}/{len(ordered_fields)}")
        prompt = (
            f"[{pos + 1}/{len(ordered_fields)}] {field_cfg.key} "
            f"[{field_cfg.kind}, {required_text}] "
            "(commands: p quick answers, b back, s skip optional, x exclude, "
            "o open site, f search, q quit)"
        )
        entered = ask_text(prompt, default=default_text)
        command = entered.strip().lower()

        if command in {"q", ":q"}:
            return ManualAskOutcome(status="quit")
        if command in {"o", ":o"}:
            if not website_url:
                print_warning("No website URL available.")
            elif open_url(website_url):
                print_success(f"Opened website: {website_url}")
            else:
                print_warning(f"Could not open browser. URL: {website_url}")
            continue
        if command in {"f", ":f"}:
            query = quote_plus(" ".join(part for part in [org_name, org_location] if part).strip())
            search_url = f"https://www.google.com/search?q={query}"
            if open_url(search_url):
                print_success(f"Opened search: {search_url}")
            else:
                print_warning(f"Could not open browser. URL: {search_url}")
            continue
        if command in {"b", ":b"}:
            if pos > 0:
                pos -= 1
            continue
        if command in {"s", ":s"}:
            if field_cfg.required:
                print_warning("Cannot skip required field.")
                continue
            draft[field_cfg.key] = _manual_default_value(field_cfg)
            pos += 1
            continue
        if command in {"x", ":x"}:
            reason_choice = _prompt_classify_exclusion_reason()
            if reason_choice is None:
                continue
            reason, note = reason_choice
            if not confirm(f"Exclude as {reason.value}?", default=True):
                continue
            return ManualAskOutcome(
                status="excluded",
                entry_method=entry_method,
                quick_answer_index=quick_answer_used,
                exclude_reason=reason,
                exclude_reason_note=note,
            )
        quick_pick = _parse_quick_answer_command(
            command,
            max_answers=len(question.manual_quick_answers),
        )
        if quick_pick is not None:
            picked: tuple[ManualQuickAnswer, int] | None = None
            if quick_pick > 0:
                idx = quick_pick
                picked = (question.manual_quick_answers[idx - 1], idx)
            if picked is None:
                picked = _manual_pick_quick_answer(question)
            if picked is None:
                continue
            quick_answer, quick_index = picked
            try:
                draft = _manual_apply_quick_answer(
                    question=question,
                    current_payload=draft,
                    quick_answer=quick_answer,
                    allowed_snippet_ids=allowed_snippet_ids,
                )
            except ValueError as e:
                print_warning(str(e))
                continue
            quick_answer_used = quick_index
            entry_method = "quick_answer"
            pos = len(ordered_fields)
            break

        try:
            draft[field_cfg.key] = _manual_parse_field_value(entered, field_cfg)
        except ValueError as e:
            print_warning(str(e))
            continue
        pos += 1

    while True:
        try:
            normalized_payload = normalize_payload(
                draft,
                question=question,
                allowed_snippet_ids=allowed_snippet_ids,
            )
            validate_required_output_fields(normalized_payload, question)
            validation_error = ""
        except ValueError as e:
            normalized_payload = {}
            validation_error = str(e)

        clear()
        queue_header = (
            "[bold]Classify Manual Ask[/bold]\n"
            f"Question: [bold]{question.id}[/bold]\n"
            f"Org: [{C_PRIMARY}]{org_name or '-'}[/{C_PRIMARY}]"
        )
        console.print(make_panel(queue_header, "Manual Entry"))

        rows: list[tuple[str, str]] = []
        for field_cfg in question.output_fields:
            rows.append(
                (
                    f"{field_cfg.key}{' *' if field_cfg.required else ''}",
                    _manual_format_summary_value(draft.get(field_cfg.key), field_cfg.kind),
                )
            )
        console.print(make_panel(make_kv_table(rows), "Draft Values"))
        if validation_error:
            console.print(
                make_panel(
                    f"[{C_WARNING}]{validation_error}[/{C_WARNING}]",
                    "Validation",
                )
            )

        console.print(
            make_panel(
                make_actions_table(
                    [
                        ("s", "save draft"),
                        ("e", "edit field"),
                        ("p", "quick answers"),
                        ("x", "exclude org"),
                        ("o", "open website"),
                        ("f", "search org"),
                        ("k", "skip org"),
                        ("q", "quit"),
                    ]
                ),
                "Actions",
            )
        )
        key = wait_for_key(["s", "e", "p", "x", "o", "f", "k", "q"], prompt="Action: ")
        if key == "q":
            return ManualAskOutcome(status="quit")
        if key == "k":
            return ManualAskOutcome(status="skip")
        if key == "o":
            if not website_url:
                print_warning("No website URL available.")
            elif open_url(website_url):
                print_success(f"Opened website: {website_url}")
            else:
                print_warning(f"Could not open browser. URL: {website_url}")
            continue
        if key == "f":
            query = quote_plus(" ".join(part for part in [org_name, org_location] if part).strip())
            search_url = f"https://www.google.com/search?q={query}"
            if open_url(search_url):
                print_success(f"Opened search: {search_url}")
            else:
                print_warning(f"Could not open browser. URL: {search_url}")
            continue
        if key == "p":
            picked = _manual_pick_quick_answer(question)
            if picked is None:
                continue
            quick_answer, quick_index = picked
            try:
                draft = _manual_apply_quick_answer(
                    question=question,
                    current_payload=draft,
                    quick_answer=quick_answer,
                    allowed_snippet_ids=allowed_snippet_ids,
                )
            except ValueError as e:
                print_warning(str(e))
                continue
            quick_answer_used = quick_index
            entry_method = "quick_answer"
            continue
        if key == "x":
            reason_choice = _prompt_classify_exclusion_reason()
            if reason_choice is None:
                continue
            reason, note = reason_choice
            if not confirm(f"Exclude as {reason.value}?", default=True):
                continue
            return ManualAskOutcome(
                status="excluded",
                entry_method=entry_method,
                quick_answer_index=quick_answer_used,
                exclude_reason=reason,
                exclude_reason_note=note,
            )
        if key == "e":
            choices: list[tuple[str, str]] = []
            for field_cfg in question.output_fields:
                preview = _manual_format_summary_value(draft.get(field_cfg.key), field_cfg.kind)
                title = (
                    f"{field_cfg.key}{' *' if field_cfg.required else ''} "
                    f"[{field_cfg.kind}] = {preview}"
                )
                choices.append((title, field_cfg.key))
            selected = ask_select(
                "Select field to edit",
                choices,
                default_value=question.output_fields[0].key,
            )
            if not selected:
                continue
            for idx_field, field_cfg in enumerate(ordered_fields):
                if field_cfg.key == selected:
                    pos = idx_field
                    break
            while pos < len(ordered_fields):
                field_cfg = ordered_fields[pos]
                required_text = "required" if field_cfg.required else "optional"
                default_text = _format_edit_default(draft.get(field_cfg.key), field_cfg.kind)
                show_entry_context(stage=f"Edit field {pos + 1}/{len(ordered_fields)}")
                prompt = (
                    f"[{pos + 1}/{len(ordered_fields)}] {field_cfg.key} "
                    f"[{field_cfg.kind}, {required_text}] "
                    "(commands: p quick answers, b back, s skip optional, x exclude, q cancel edit)"
                )
                entered = ask_text(prompt, default=default_text)
                command = entered.strip().lower()
                if command in {"q", ":q"}:
                    break
                if command in {"b", ":b"}:
                    if pos > 0:
                        pos -= 1
                    continue
                if command in {"s", ":s"}:
                    if field_cfg.required:
                        print_warning("Cannot skip required field.")
                        continue
                    draft[field_cfg.key] = _manual_default_value(field_cfg)
                    pos += 1
                    continue
                if command in {"x", ":x"}:
                    reason_choice = _prompt_classify_exclusion_reason()
                    if reason_choice is None:
                        continue
                    reason, note = reason_choice
                    if not confirm(f"Exclude as {reason.value}?", default=True):
                        continue
                    return ManualAskOutcome(
                        status="excluded",
                        entry_method=entry_method,
                        quick_answer_index=quick_answer_used,
                        exclude_reason=reason,
                        exclude_reason_note=note,
                    )
                quick_pick = _parse_quick_answer_command(
                    command,
                    max_answers=len(question.manual_quick_answers),
                )
                if quick_pick is not None:
                    picked: tuple[ManualQuickAnswer, int] | None = None
                    if quick_pick > 0:
                        idx = quick_pick
                        picked = (question.manual_quick_answers[idx - 1], idx)
                    if picked is None:
                        picked = _manual_pick_quick_answer(question)
                    if picked is None:
                        continue
                    quick_answer, quick_index = picked
                    try:
                        draft = _manual_apply_quick_answer(
                            question=question,
                            current_payload=draft,
                            quick_answer=quick_answer,
                            allowed_snippet_ids=allowed_snippet_ids,
                        )
                    except ValueError as e:
                        print_warning(str(e))
                        continue
                    quick_answer_used = quick_index
                    entry_method = "quick_answer"
                    break
                try:
                    draft[field_cfg.key] = _manual_parse_field_value(entered, field_cfg)
                except ValueError as e:
                    print_warning(str(e))
                    continue
                pos += 1
            continue
        if key == "s":
            if validation_error:
                print_warning("Fix validation errors before saving.")
                continue
            confirmation = wait_for_key(["y", "n"], prompt="Confirm save (y/n): ")
            if confirmation != "y":
                continue
            route, route_reason = decide_route(normalized_payload, question)
            result = AskResult(
                payload=normalized_payload,
                raw_response="",
                prompt="manual_input",
                route=route,
                route_reason=route_reason,
                error="",
            )
            return ManualAskOutcome(
                status="completed",
                result=result,
                payload=normalized_payload,
                entry_method=entry_method,
                quick_answer_index=quick_answer_used,
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


def _effective_normalized_payload(ask_payload: dict[str, object]) -> tuple[dict[str, object], str]:
    manual_override = ask_payload.get("manual_override", {})
    if isinstance(manual_override, dict):
        manual_normalized = manual_override.get("normalized", {})
        if isinstance(manual_normalized, dict) and manual_normalized:
            return manual_normalized, "manual_override"

    normalized = ask_payload.get("normalized", {})
    if isinstance(normalized, dict) and normalized:
        source = str(ask_payload.get("source", "llm") or "llm").strip().lower()
        if source in {"manual", "llm"}:
            return normalized, source
        return normalized, "llm"
    return {}, "none"


def _format_edit_default(value: object, kind: str) -> str:
    normalized_kind = str(kind or "").strip().lower()
    if normalized_kind in {"string", "number"}:
        return "" if value is None else str(value).strip()
    if normalized_kind == "string_list":
        if not isinstance(value, list):
            return ""
        return ", ".join(str(item).strip() for item in value if str(item).strip())
    if normalized_kind == "object_list":
        if not isinstance(value, list):
            return "[]"
        return json.dumps(value, ensure_ascii=False)
    return str(value or "").strip()


def _parse_edit_value(raw_value: str, field_cfg: OutputFieldConfig) -> object:
    text = str(raw_value or "").strip()
    if field_cfg.kind == "string":
        return text.lower() if field_cfg.lowercase else text
    if field_cfg.kind == "number":
        if not text:
            return None
        try:
            return float(text)
        except ValueError as e:
            raise ValueError(f"Invalid number: {text!r}") from e
    if field_cfg.kind == "string_list":
        if not text:
            return []
        values = [part.strip() for part in text.split(",") if part.strip()]
        if field_cfg.lowercase:
            values = [item.lower() for item in values]
        if field_cfg.unique_casefold:
            seen: set[str] = set()
            deduped: list[str] = []
            for item in values:
                key = item.casefold()
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(item)
            values = deduped
        if field_cfg.max_items is not None:
            values = values[: field_cfg.max_items]
        return values
    if field_cfg.kind == "object_list":
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError("Object-list fields require valid JSON array input.") from e
        if not isinstance(parsed, list):
            raise ValueError("Object-list fields require a JSON array.")
        return parsed
    raise ValueError(f"Unsupported editable field kind: {field_cfg.kind!r}")


def _default_payload_for_question(question: ClassifyQuestion) -> dict[str, object]:
    defaults: dict[str, object] = {}
    for field_cfg in question.output_fields:
        if field_cfg.kind == "string":
            if field_cfg.allowed:
                preferred = "unknown" if "unknown" in field_cfg.allowed else field_cfg.allowed[0]
                defaults[field_cfg.key] = preferred
            else:
                defaults[field_cfg.key] = ""
            continue
        if field_cfg.kind == "number":
            defaults[field_cfg.key] = (
                float(field_cfg.min_value) if field_cfg.min_value is not None else 0.0
            )
            continue
        if field_cfg.kind in {"string_list", "object_list"}:
            defaults[field_cfg.key] = []
            continue
        defaults[field_cfg.key] = ""
    return defaults


def _prompt_review_payload_edit(
    *,
    question: ClassifyQuestion,
    current_payload: dict[str, object],
    allowed_snippet_ids: set[str],
) -> dict[str, object] | None:
    if not question.output_fields:
        print_warning("No editable output fields configured for this classify question.")
        return None

    field_map = {item.key: item for item in question.output_fields}
    choices: list[tuple[str, str]] = []
    for field_cfg in question.output_fields:
        current_value = _field_value(current_payload, field_cfg.key)
        preview = _format_edit_default(current_value, field_cfg.kind)
        choices.append((f"{field_cfg.key} [{field_cfg.kind}] = {preview}", field_cfg.key))
    choices.append(("Cancel", "__cancel__"))

    selected_key = ask_select(
        "Select field to update",
        choices,
        default_value=question.output_fields[0].key,
    )
    if not selected_key or selected_key == "__cancel__":
        return None

    field_cfg = field_map[selected_key]
    current_value = current_payload.get(field_cfg.key)
    default_text = _format_edit_default(current_value, field_cfg.kind)
    if field_cfg.kind == "object_list":
        prompt = (
            f"New value for {field_cfg.key} (JSON array; empty clears)")
    elif field_cfg.kind == "string_list":
        prompt = f"New value for {field_cfg.key} (comma-separated; empty clears)"
    else:
        prompt = f"New value for {field_cfg.key}"
    entered = ask_text(prompt, default=default_text)

    try:
        parsed_value = _parse_edit_value(entered, field_cfg)
    except ValueError as e:
        print_warning(str(e))
        return None

    draft = dict(current_payload)
    draft[field_cfg.key] = parsed_value
    try:
        normalized = normalize_payload(
            draft,
            question=question,
            allowed_snippet_ids=allowed_snippet_ids,
        )
        validate_required_output_fields(normalized, question)
        return normalized
    except ValueError as e:
        print_warning(f"Invalid update: {e}")
        return None


def _prompt_classify_exclusion_reason() -> tuple[ExcludeReason, str] | None:
    choices = [
        (f"{option.label} [{option.reason.value}]", option.reason.value)
        for option in EXCLUDE_REASON_OPTIONS
    ]
    selected = ask_select(
        "Exclude reason",
        choices,
        default_value=ExcludeReason.IRRELEVANT_PURPOSE.value,
    )
    if not selected:
        print_warning("No exclusion reason selected.")
        return None

    reason = ExcludeReason(selected)
    note = ""
    if reason is ExcludeReason.OTHER:
        note = ask_text("Reason note (required)")
        if not str(note or "").strip():
            print_warning("Reason note is required for OTHER.")
            return None
    return reason, str(note or "").strip()


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
    def normalize_review_url(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value or "").strip()
        if not text:
            return ""
        lowered = text.lower()
        if lowered in {"nan", "none", "null"}:
            return ""
        if not lowered.startswith(("http://", "https://")):
            return ""
        return text

    def open_url(url: str) -> bool:
        value = str(url or "").strip()
        if not value:
            return False
        try:
            return bool(webbrowser.open(value))
        except Exception:
            return False

    def fmt_float_conf(value: object) -> str:
        try:
            score = float(value)
        except (TypeError, ValueError):
            return f"[{C_MUTED}]-[/{C_MUTED}]"
        if score >= 0.8:
            color = C_SCORE_HIGH
        elif score >= 0.6:
            color = C_SCORE_MED
        else:
            color = C_SCORE_LOW
        return f"[{color}]{score:.2f}[/{color}]"

    def compact_list(value: object) -> str:
        if not isinstance(value, list) or not value:
            return f"[{C_MUTED}]-[/{C_MUTED}]"
        return ", ".join(str(item) for item in value)

    def review_reason_text(auto_result: str, ask_payload: dict[str, object]) -> str:
        if auto_result == "waiting_for_clean_text":
            return "Missing usable cleaned text; classify ask could not run."
        route_reason = str(ask_payload.get("route_reason", "") or "").strip()
        if route_reason:
            return route_reason
        error = str(ask_payload.get("error", "") or "").strip()
        if error:
            return f"Ask phase reported: {error}"
        return "Rule-based safety routing to manual review."

    def format_review_value(value: object, fmt: str) -> str:
        normalized = fmt.strip().lower()
        if normalized in {"confidence", "float_conf"}:
            return fmt_float_conf(value)
        if normalized in {"list", "array"}:
            return compact_list(value)
        if normalized in {"json", "object"}:
            if value is None or value == "":
                return f"[{C_MUTED}]-[/{C_MUTED}]"
            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return str(value)
        if isinstance(value, list):
            return compact_list(value)
        if isinstance(value, float):
            return f"{value:.2f}"
        text = str(value or "").strip()
        return text if text else f"[{C_MUTED}]-[/{C_MUTED}]"

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

    valid_keys = ["a", "x", "u", "o", "w", "f", "v", "s", "q"]
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
        if not isinstance(ask_payload, dict):
            ask_payload = {}
        normalized_payload, payload_source = _effective_normalized_payload(ask_payload)

        auto_result = str(row.get(cols["auto_result"], "") or "-")
        auto_result_at = str(row.get(cols["auto_result_at"], "") or "")
        review_result = str(row.get(cols["review_result"], "") or "")
        review_result_at = str(row.get(cols["review_result_at"], "") or "")
        website_url_final = normalize_review_url(row.get("_website_url_final", ""))
        website_url_candidate = normalize_review_url(row.get("_website_url", ""))
        website_url = website_url_final or website_url_candidate
        zefix_purpose = str(row.get("_zefix_purpose", "") or "").strip()
        zefix_status = str(row.get("_zefix_status", "") or "").strip()

        reason = str(normalized_payload.get("reason", "") or "")
        evidence = normalized_payload.get("evidence", [])
        snippets = ask_payload.get("snippets", []) if isinstance(ask_payload, dict) else []
        snippet_map: dict[str, str] = {}
        allowed_snippet_ids: set[str] = set()
        if isinstance(snippets, list):
            for item in snippets:
                if not isinstance(item, dict):
                    continue
                sid = str(item.get("snippet_id", "") or "").strip()
                text = str(item.get("text", "") or "").strip()
                if sid:
                    snippet_map[sid] = text
                    allowed_snippet_ids.add(sid)
        if zefix_purpose:
            allowed_snippet_ids.add(VERIFIED_PURPOSE_SNIPPET_ID)

        show_full_snippets = False

        while True:
            clear()
            console.print()
            header = (
                f"[bold]Classify Review[/bold]  [dim]{pos}/{len(queue_indices)}[/dim]\n"
                f"Question: [bold]{question.id}[/bold]\n"
                f"Purpose: [{C_MUTED}]{question.description or '-'}[/{C_MUTED}]"
            )
            console.print(make_panel(header, "Review Queue"))

            org_rows = [
                ("Name", f"[{C_PRIMARY}]{org_name}[/{C_PRIMARY}]"),
                ("Org ID", f"[{C_MUTED}]{org_id or '-'}[/{C_MUTED}]"),
                (
                    "Website",
                    website_url if website_url else f"[{C_MUTED}]-[/{C_MUTED}]",
                ),
                ("Auto result", f"[bold]{auto_result or '-'}[/bold]"),
                (
                    "Auto decided at",
                    f"[{C_MUTED}]{auto_result_at or '-'}[/{C_MUTED}]",
                ),
                (
                    "Review result",
                    f"[{C_MUTED}]{review_result or '-'}[/{C_MUTED}]",
                ),
                ("Proposal source", f"[{C_MUTED}]{payload_source}[/{C_MUTED}]"),
                (
                    "Reviewed at",
                    f"[{C_MUTED}]{review_result_at or '-'}[/{C_MUTED}]",
                ),
            ]
            console.print(make_panel(make_kv_table(org_rows), "Organization"))

            pred_rows: list[tuple[str, str]] = []
            for review_field in question.review_fields:
                value = _field_value(normalized_payload, review_field.field)
                rendered = format_review_value(value, review_field.format)
                if review_field.confidence_field:
                    confidence_value = _field_value(
                        normalized_payload,
                        review_field.confidence_field,
                    )
                    rendered = f"[bold]{rendered}[/bold] ({fmt_float_conf(confidence_value)})"
                pred_rows.append((review_field.label, rendered))

            if not pred_rows:
                for key in sorted(normalized_payload.keys()):
                    if key == "evidence":
                        continue
                    value = normalized_payload.get(key)
                    pred_rows.append((key, format_review_value(value, "auto")))

            includes_reason = any(
                review_field.field.strip().lower() == "reason"
                for review_field in question.review_fields
            )
            if "reason" in normalized_payload and not includes_reason:
                pred_rows.append(("Reason", f"[{C_MUTED}]{reason or '-'}[/{C_MUTED}]"))

            console.print(make_panel(make_kv_table(pred_rows), "LLM Classification"))

            zefix_rows = [
                (
                    "ZEFIX status",
                    zefix_status if zefix_status else f"[{C_MUTED}]-[/{C_MUTED}]",
                ),
                (
                    "ZEFIX purpose",
                    (
                        zefix_purpose[:800] + "..."
                        if len(zefix_purpose) > 800
                        else (zefix_purpose or f"[{C_MUTED}]-[/{C_MUTED}]")
                    ),
                ),
            ]
            console.print(make_panel(make_kv_table(zefix_rows), "Registry Context"))

            reason_panel = review_reason_text(auto_result, ask_payload)
            console.print(
                make_panel(
                    f"[{C_MUTED}]{reason_panel}[/{C_MUTED}]",
                    "Why Manual Review",
                )
            )

            evidence_lines: list[str] = []
            if isinstance(evidence, list) and evidence:
                for idx_e, item in enumerate(evidence, start=1):
                    if not isinstance(item, dict):
                        continue
                    sid = str(item.get("snippet_id", "") or "").strip()
                    quote = str(item.get("quote", "") or "").strip()
                    if not sid:
                        continue
                    evidence_lines.append(f"{idx_e}. {sid}: \"{quote or '-'}\"")
                    if show_full_snippets and sid in snippet_map:
                        full_text = snippet_map[sid][:500]
                        evidence_lines.append(f"   [{C_MUTED}]snippet: {full_text}[/{C_MUTED}]")
            else:
                evidence_lines.append(f"[{C_MUTED}]No evidence payload available.[/{C_MUTED}]")

            evidence_title = "Evidence"
            if show_full_snippets:
                evidence_title += " (with snippet context)"
            console.print(make_panel("\n".join(evidence_lines), evidence_title))

            console.print(
                make_panel(
                    make_actions_table(
                        [
                            ("a", "accept in scope"),
                            ("x", "mark excluded"),
                            ("u", "update proposal"),
                            ("o", "open website"),
                            ("f", "search org on web"),
                            (
                                "v",
                                "toggle snippet context",
                            ),
                            ("s", "skip"),
                            ("q", "quit"),
                        ]
                    ),
                    "Actions",
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
            if key in {"o", "w"}:
                if not website_url:
                    print_warning("No website URL available.")
                elif open_url(website_url):
                    print_success(f"Opened website: {website_url}")
                else:
                    print_warning(f"Could not open browser. URL: {website_url}")
                continue
            if key == "f":
                query = quote_plus(org_name.strip())
                search_url = f"https://www.google.com/search?q={query}"
                if open_url(search_url):
                    print_success(f"Opened search: {search_url}")
                else:
                    print_warning(f"Could not open browser. URL: {search_url}")
                continue
            if key == "v":
                show_full_snippets = not show_full_snippets
                continue
            if key == "u":
                if not normalized_payload:
                    normalized_payload = _default_payload_for_question(question)
                    print_warning(
                        "No proposal payload available; started from schema defaults. "
                        "Adjust fields as needed."
                    )
                updated_payload = _prompt_review_payload_edit(
                    question=question,
                    current_payload=normalized_payload,
                    allowed_snippet_ids=allowed_snippet_ids,
                )
                if updated_payload is None:
                    continue
                ask_payload["manual_override"] = {
                    "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "normalized": updated_payload,
                }
                write_org_artifact(artifact_path, ask_payload)
                normalized_payload = updated_payload
                payload_source = "manual_override"
                if save_callback is not None:
                    save_callback()
                print_success("Proposal updated")
                continue
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
                        "proposal_source": payload_source,
                    },
                )
                if save_callback is not None:
                    save_callback()
                stats["accepted"] += 1
                print_success("Accepted")
                break
            if key == "x":
                reason_choice = _prompt_classify_exclusion_reason()
                if reason_choice is None:
                    continue
                reason, note = reason_choice
                if not confirm(f"Exclude as {reason.value}?", default=True):
                    continue
                apply_review_summary(df, idx, question, "excluded")
                write_org_artifact(
                    classify_org_dir(org_id, question.id) / "review.json",
                    {
                        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                        "question_id": question.id,
                        "org_id": org_id,
                        "decision": "excluded",
                        "proposal_source": payload_source,
                        "exclusion_reason": reason.value,
                        "exclusion_reason_note": note,
                    },
                )
                if save_callback is not None:
                    save_callback()
                stats["excluded"] += 1
                print_success(f"Excluded ({reason.value})")
                break

    return stats


def classify_conclusions_path() -> Path:
    path = DATA_DIR / "classify" / "conclusions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_classify_conclusions(path: Path | None = None) -> dict[str, object]:
    effective_path = path or classify_conclusions_path()
    if not effective_path.exists():
        return {}
    try:
        data = json.loads(effective_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def save_classify_conclusions(payload: dict[str, object], path: Path | None = None) -> None:
    effective_path = path or classify_conclusions_path()
    effective_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def summarize_question_for_conclude(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
) -> dict[str, int]:
    cols = question_columns(question.id)
    active_mask = (
        df["_excluded_reason"].astype(str).str.strip() == ""
        if "_excluded_reason" in df.columns
        else pd.Series(True, index=df.index)
    )
    org_ids = df["_org_id"].astype(str).str.strip()
    eligible_mask = org_ids.isin(eligible_org_ids)
    scoped_mask = active_mask & eligible_mask

    auto_result = df[cols["auto_result"]].astype(str).str.strip().str.lower()
    review_result = df[cols["review_result"]].astype(str).str.strip().str.lower()

    auto_accepted_mask = scoped_mask & (auto_result == "auto_accepted")
    auto_excluded_mask = scoped_mask & (auto_result == "auto_excluded")
    needs_review_mask = scoped_mask & (auto_result == "needs_review")
    waiting_for_clean_text_mask = scoped_mask & (auto_result == STATUS_WAITING_FOR_CLEAN_TEXT)
    ask_pending_mask = scoped_mask & (auto_result == "")

    review_pending_mask = needs_review_mask & (review_result == "")
    review_accepted_mask = scoped_mask & (review_result == "accepted")
    review_excluded_mask = scoped_mask & (review_result == "excluded")

    concludable_mask = auto_excluded_mask | review_excluded_mask

    return {
        "active_eligible": int(scoped_mask.sum()),
        "ask_pending": int(ask_pending_mask.sum()),
        "auto_accepted": int(auto_accepted_mask.sum()),
        "auto_excluded": int(auto_excluded_mask.sum()),
        "needs_review": int(needs_review_mask.sum()),
        "waiting_for_clean_text": int(waiting_for_clean_text_mask.sum()),
        "review_pending": int(review_pending_mask.sum()),
        "review_accepted": int(review_accepted_mask.sum()),
        "review_excluded": int(review_excluded_mask.sum()),
        "concludable_exclusions": int(concludable_mask.sum()),
    }


def _question_conclude_masks(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
) -> dict[str, pd.Series]:
    cols = question_columns(question.id)
    active_mask = (
        df["_excluded_reason"].astype(str).str.strip() == ""
        if "_excluded_reason" in df.columns
        else pd.Series(True, index=df.index)
    )
    org_ids = df["_org_id"].astype(str).str.strip()
    eligible_mask = org_ids.isin(eligible_org_ids)
    scoped_mask = active_mask & eligible_mask

    auto_result = df[cols["auto_result"]].astype(str).str.strip().str.lower()
    review_result = df[cols["review_result"]].astype(str).str.strip().str.lower()
    needs_review = auto_result == "needs_review"

    return {
        "auto_accepted": scoped_mask & (auto_result == "auto_accepted"),
        "auto_excluded": scoped_mask & (auto_result == "auto_excluded"),
        "review_accepted": scoped_mask & (review_result == "accepted"),
        "review_excluded": scoped_mask & (review_result == "excluded"),
        "review_pending": scoped_mask & needs_review & (review_result == ""),
    }


def apply_conclude_updates(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
) -> dict[str, int]:
    if not question.conclude_apply_exclusion:
        return {"applied_total": 0, "applied_auto_excluded": 0, "applied_review_excluded": 0}
    ensure_text_columns(df, ["_excluded_reason", "_excluded_reason_note", "_excluded_at"])

    masks = _question_conclude_masks(df, question, eligible_org_ids)
    auto_mask = masks["auto_excluded"]
    review_mask = masks["review_excluded"]
    apply_mask = auto_mask | review_mask
    if int(apply_mask.sum()) == 0:
        return {"applied_total": 0, "applied_auto_excluded": 0, "applied_review_excluded": 0}

    now = datetime.now(UTC).isoformat(timespec="seconds")
    auto_note = f"classify:{question.id}:auto_excluded"
    auto_only = auto_mask & ~review_mask
    review_only = review_mask

    if int(auto_only.sum()) > 0:
        for idx in list(df.index[auto_only]):
            df.at[idx, "_excluded_reason"] = ExcludeReason.IRRELEVANT_PURPOSE.value
            df.at[idx, "_excluded_at"] = now
            existing_note = str(df.at[idx, "_excluded_reason_note"] or "").strip()
            if existing_note:
                df.at[idx, "_excluded_reason_note"] = f"{existing_note} | {auto_note}"
            else:
                df.at[idx, "_excluded_reason_note"] = auto_note

    if int(review_only.sum()) > 0:
        for idx in list(df.index[review_only]):
            org_id = str(df.at[idx, "_org_id"] or "").strip()
            review_payload = read_org_artifact(
                classify_org_dir(org_id, question.id) / "review.json"
            )

            reason_value = ExcludeReason.IRRELEVANT_PURPOSE.value
            reason_raw = str(review_payload.get("exclusion_reason", "") or "").strip().upper()
            if reason_raw in VALID_EXCLUDE_REASON_CODES:
                reason_value = reason_raw

            note_text = str(review_payload.get("exclusion_reason_note", "") or "").strip()
            provenance_note = f"classify:{question.id}:review_excluded:{reason_value}"

            df.at[idx, "_excluded_reason"] = reason_value
            df.at[idx, "_excluded_at"] = now

            existing_note = str(df.at[idx, "_excluded_reason_note"] or "").strip()
            parts: list[str] = []
            if existing_note:
                parts.append(existing_note)
            parts.append(provenance_note)
            if note_text:
                parts.append(note_text)
            df.at[idx, "_excluded_reason_note"] = " | ".join(parts)

    return {
        "applied_total": int(apply_mask.sum()),
        "applied_auto_excluded": int(auto_mask.sum()),
        "applied_review_excluded": int(review_mask.sum()),
    }


def _pick_random_index(mask: pd.Series) -> int | None:
    indices = list(mask[mask].index)
    if not indices:
        return None
    return int(random.choice(indices))


def _conclude_example_rows(
    df: pd.DataFrame,
    row_index: int,
    *,
    question: ClassifyQuestion,
    name_column: str,
) -> list[tuple[str, str]]:
    row = df.loc[row_index]
    cols = question_columns(question.id)
    org_id = str(row.get("_org_id", "") or "").strip()
    org_name = str(row.get(name_column, "") or "").strip() or "-"
    website = str(row.get("_website_url_final", "") or "").strip() or str(
        row.get("_website_url", "") or ""
    ).strip()
    auto_result = str(row.get(cols["auto_result"], "") or "").strip() or "-"
    review_result = str(row.get(cols["review_result"], "") or "").strip() or "-"

    ask_payload = read_org_artifact(classify_org_dir(org_id, question.id) / "ask.json")
    route_reason = str(ask_payload.get("route_reason", "") or "").strip() or "-"
    normalized, _ = _effective_normalized_payload(ask_payload)
    reason_raw = normalized.get("reason", "") if isinstance(normalized, dict) else ""
    reason = str(reason_raw).strip() or "-"

    return [
        ("Org", org_name),
        ("Org ID", org_id or "-"),
        ("Website", website or "-"),
        ("Auto result", auto_result),
        ("Review result", review_result),
        ("Route reason", route_reason),
        ("LLM reason", reason),
    ]


def conclude_question(
    df: pd.DataFrame,
    question: ClassifyQuestion,
    eligible_org_ids: set[str],
    *,
    interactive: bool,
    name_column: str,
    save_callback=None,
) -> dict[str, int]:
    stats = summarize_question_for_conclude(df, question, eligible_org_ids)
    if stats["ask_pending"] > 0:
        return {
            "status": 0,
            "blocked_ask_pending": int(stats["ask_pending"]),
            "blocked_review_pending": 0,
            "applied_total": 0,
            "applied_auto_excluded": 0,
            "applied_review_excluded": 0,
        }

    if stats["review_pending"] > 0:
        return {
            "status": 0,
            "blocked_ask_pending": 0,
            "blocked_review_pending": int(stats["review_pending"]),
            "applied_total": 0,
            "applied_auto_excluded": 0,
            "applied_review_excluded": 0,
        }

    if not interactive:
        return {
            "status": 1,
            "blocked_ask_pending": 0,
            "blocked_review_pending": 0,
            "applied_total": 0,
            "applied_auto_excluded": 0,
            "applied_review_excluded": 0,
        }

    masks = _question_conclude_masks(df, question, eligible_org_ids)
    last_sample_index: int | None = None
    valid_keys = ["u", "x", "a", "e", "o", "c", "q"]

    while True:
        latest_stats = summarize_question_for_conclude(df, question, eligible_org_ids)
        clear()
        summary_rows = [
            ("Question", question.id),
            ("Description", question.description or "-"),
            ("Active eligible", latest_stats["active_eligible"]),
            ("Ask pending", latest_stats["ask_pending"]),
            ("Auto accepted", latest_stats["auto_accepted"]),
            ("Auto excluded", latest_stats["auto_excluded"]),
            ("Needs review", latest_stats["needs_review"]),
            ("Review pending", latest_stats["review_pending"]),
            ("Review accepted", latest_stats["review_accepted"]),
            ("Review excluded", latest_stats["review_excluded"]),
            ("Concludable exclusions", latest_stats["concludable_exclusions"]),
        ]
        console.print(make_panel(make_kv_table(summary_rows), "Classify Conclude"))

        if last_sample_index is not None and last_sample_index in df.index:
            rows = _conclude_example_rows(
                df,
                last_sample_index,
                question=question,
                name_column=name_column,
            )
            console.print(make_panel(make_kv_table(rows), "Last Sample"))
        else:
            console.print(
                make_panel(
                    f"[{C_MUTED}]No sample selected yet.[/{C_MUTED}]",
                    "Last Sample",
                )
            )

        console.print(
            make_panel(
                make_actions_table(
                    [
                        ("u", "sample auto accepted"),
                        ("x", "sample auto excluded"),
                        ("a", "sample review accepted"),
                        ("e", "sample review excluded"),
                        ("o", "open sample website"),
                        ("c", "confirm conclude + apply"),
                        ("q", "quit"),
                    ]
                ),
                "Actions",
            )
        )

        try:
            key = wait_for_key(valid_keys)
        except KeyboardInterrupt:
            return {
                "status": 2,
                "blocked_ask_pending": 0,
                "blocked_review_pending": 0,
                "applied_total": 0,
                "applied_auto_excluded": 0,
                "applied_review_excluded": 0,
            }

        if key == "q":
            return {
                "status": 2,
                "blocked_ask_pending": 0,
                "blocked_review_pending": 0,
                "applied_total": 0,
                "applied_auto_excluded": 0,
                "applied_review_excluded": 0,
            }

        if key in {"u", "x", "a", "e"}:
            bucket_map = {
                "u": masks["auto_accepted"],
                "x": masks["auto_excluded"],
                "a": masks["review_accepted"],
                "e": masks["review_excluded"],
            }
            sampled = _pick_random_index(bucket_map[key])
            if sampled is None:
                print_warning("No rows available in that bucket.")
            else:
                last_sample_index = sampled
            continue

        if key == "o":
            if last_sample_index is None or last_sample_index not in df.index:
                print_warning("Select a sample first.")
                continue
            sample_row = df.loc[last_sample_index]
            website = str(sample_row.get("_website_url_final", "") or "").strip() or str(
                sample_row.get("_website_url", "") or ""
            ).strip()
            if not website:
                print_warning("Selected sample has no website URL.")
                continue
            try:
                opened = bool(webbrowser.open(website))
            except Exception:
                opened = False
            if opened:
                print_success(f"Opened website: {website}")
            else:
                print_warning(f"Could not open browser. URL: {website}")
            continue

        if key == "c":
            confirm_text = (
                "Finalize this question and write global exclusions for "
                "auto_excluded/review_excluded rows?"
            )
            if not confirm(confirm_text, default=False):
                continue

            applied = apply_conclude_updates(df, question, eligible_org_ids)
            if save_callback is not None:
                save_callback()

            existing = load_classify_conclusions()
            payload = existing if existing else {"version": 1, "questions": {}}
            questions_obj = payload.get("questions", {})
            if not isinstance(questions_obj, dict):
                questions_obj = {}
            questions_obj[question.id] = {
                "concluded_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "applied_total": int(applied["applied_total"]),
                "applied_auto_excluded": int(applied["applied_auto_excluded"]),
                "applied_review_excluded": int(applied["applied_review_excluded"]),
                "stats_snapshot": summarize_question_for_conclude(df, question, eligible_org_ids),
            }
            payload["version"] = 1
            payload["questions"] = questions_obj
            payload["updated_at"] = datetime.now(UTC).isoformat(timespec="seconds")
            save_classify_conclusions(payload)

            return {
                "status": 3,
                "blocked_ask_pending": 0,
                "blocked_review_pending": 0,
                "applied_total": int(applied["applied_total"]),
                "applied_auto_excluded": int(applied["applied_auto_excluded"]),
                "applied_review_excluded": int(applied["applied_review_excluded"]),
            }

    # Unreachable fallback for type-checkers.
    return {
        "status": 2,
        "blocked_ask_pending": 0,
        "blocked_review_pending": 0,
        "applied_total": 0,
        "applied_auto_excluded": 0,
        "applied_review_excluded": 0,
    }


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
