"""Post-scrape cleaning with intra-org duplicate segment removal."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from benefind.config import DATA_DIR, Settings


def scrape_clean_summary_path() -> Path:
    return DATA_DIR / "filtered" / "organizations_scrape_clean_summary.csv"

SCRAPE_CLEAN_SUMMARY_COLUMNS = [
    "_org_id",
    "_scrape_clean_run_id",
    "_scrape_clean_status",
    "_scrape_clean_issue",
    "_scrape_clean_detail",
    "_scrape_clean_pages_total",
    "_scrape_clean_pages_written",
    "_scrape_clean_segments_total",
    "_scrape_clean_segments_removed",
    "_scrape_clean_char_count_raw",
    "_scrape_clean_char_count_cleaned",
    "_scrape_clean_usable_chars",
    "_scrape_clean_min_segment_chars",
    "_scrape_clean_min_duplicate_page_ratio",
    "_scrape_clean_retain_one_duplicate_copy",
    "_scrape_source_manifest_path",
    "_scrape_source_manifest_mtime",
    "_scrape_source_manifest_signature",
    "_scrape_clean_processed_at",
]

SCRAPE_CLEAN_MANIFEST_COLUMNS = [
    "_org_id",
    "_scrape_clean_run_id",
    "_page_filename",
    "_page_order",
    "_page_source_path",
    "_page_cleaned_path",
    "_segment_count_total",
    "_segment_count_kept",
    "_segment_count_removed",
    "_char_count_raw",
    "_char_count_cleaned",
    "_usable_char_count",
    "_cleaned_at",
]

SCRAPE_CLEAN_DUPLICATES_COLUMNS = [
    "_org_id",
    "_scrape_clean_run_id",
    "_segment_hash",
    "_segment_length",
    "_segment_page_count",
    "_segment_kept_page",
    "_segment_removed_pages",
    "_segment_preview",
]


@dataclass
class _Segment:
    raw: str
    normalized: str
    digest: str


@dataclass
class _PageRecord:
    page_filename: str
    source_path: Path
    cleaned_path: Path
    page_order: int
    segments: list[_Segment]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _write_csv_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(path)


def _write_json_atomic(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _normalize_segment(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _clear_markdown_files(directory: Path) -> None:
    if not directory.exists():
        return
    for path in directory.glob("*.md"):
        if path.is_file():
            path.unlink()


def _segment_markdown(content: str) -> list[_Segment]:
    chunks = [chunk.strip() for chunk in re.split(r"\n\s*\n", str(content or "")) if chunk.strip()]
    segments: list[_Segment] = []
    for chunk in chunks:
        normalized = _normalize_segment(chunk)
        digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
        segments.append(_Segment(raw=chunk, normalized=normalized, digest=digest))
    return segments


def _clean_paths(org_id: str) -> tuple[Path, Path, Path, Path, Path, Path]:
    org_dir = DATA_DIR / "orgs" / org_id
    pages_dir = org_dir / "pages"
    pages_cleaned_dir = org_dir / "pages_cleaned"
    scrape_clean_dir = org_dir / "scrape_clean"
    clean_manifest_path = scrape_clean_dir / "manifest.csv"
    duplicates_path = scrape_clean_dir / "duplicate_segments.csv"
    run_meta_path = scrape_clean_dir / "run_meta.json"
    return (
        pages_dir,
        pages_cleaned_dir,
        scrape_clean_dir,
        clean_manifest_path,
        duplicates_path,
        run_meta_path,
    )


def _iter_latest_successful_pages(manifest_df: pd.DataFrame) -> list[tuple[int, str, Path]]:
    if manifest_df.empty:
        return []

    for column in ["_prepared_url", "_page_status", "_page_failure_detail", "_saved_markdown_path"]:
        if column not in manifest_df.columns:
            manifest_df[column] = ""
    if "_prepared_url_order" not in manifest_df.columns:
        manifest_df["_prepared_url_order"] = 0

    latest = manifest_df.drop_duplicates(subset="_prepared_url", keep="last")
    status = latest["_page_status"].astype(str).str.strip().str.lower()
    detail = latest["_page_failure_detail"].astype(str).str.strip().str.lower()
    ok_mask = (status == "success") | ((status == "skipped") & (detail == "already_success"))
    selected = latest[ok_mask].copy()
    if selected.empty:
        return []

    selected["_prepared_url_order"] = pd.to_numeric(
        selected["_prepared_url_order"], errors="coerce"
    ).fillna(0)
    selected = selected.sort_values(by=["_prepared_url_order", "_prepared_url"], kind="mergesort")

    pages: list[tuple[int, str, Path]] = []
    for _, row in selected.iterrows():
        source_raw = str(row.get("_saved_markdown_path", "") or "").strip()
        if not source_raw:
            continue
        source_path = Path(source_raw)
        if not source_path.exists():
            continue
        order = int(float(row.get("_prepared_url_order", 0) or 0))
        pages.append((order, source_path.name, source_path))
    return pages


def _ensure_scrape_clean_summary_columns(df: pd.DataFrame) -> pd.DataFrame:
    defaults: dict[str, object] = {
        "_org_id": "",
        "_scrape_clean_run_id": "",
        "_scrape_clean_status": "",
        "_scrape_clean_issue": "",
        "_scrape_clean_detail": "",
        "_scrape_clean_pages_total": 0,
        "_scrape_clean_pages_written": 0,
        "_scrape_clean_segments_total": 0,
        "_scrape_clean_segments_removed": 0,
        "_scrape_clean_char_count_raw": 0,
        "_scrape_clean_char_count_cleaned": 0,
        "_scrape_clean_usable_chars": 0,
        "_scrape_clean_min_segment_chars": 0,
        "_scrape_clean_min_duplicate_page_ratio": 0.0,
        "_scrape_clean_retain_one_duplicate_copy": False,
        "_scrape_source_manifest_path": "",
        "_scrape_source_manifest_mtime": "",
        "_scrape_source_manifest_signature": "",
        "_scrape_clean_processed_at": "",
    }
    for column, default in defaults.items():
        if column not in df.columns:
            df[column] = default

    text_columns = [
        "_org_id",
        "_scrape_clean_run_id",
        "_scrape_clean_status",
        "_scrape_clean_issue",
        "_scrape_clean_detail",
        "_scrape_source_manifest_path",
        "_scrape_source_manifest_mtime",
        "_scrape_source_manifest_signature",
        "_scrape_clean_processed_at",
    ]
    for column in text_columns:
        df[column] = df[column].astype(object).where(df[column].notna(), "")

    numeric_columns = [
        "_scrape_clean_pages_total",
        "_scrape_clean_pages_written",
        "_scrape_clean_segments_total",
        "_scrape_clean_segments_removed",
        "_scrape_clean_char_count_raw",
        "_scrape_clean_char_count_cleaned",
        "_scrape_clean_usable_chars",
        "_scrape_clean_min_segment_chars",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)

    df["_scrape_clean_min_duplicate_page_ratio"] = pd.to_numeric(
        df["_scrape_clean_min_duplicate_page_ratio"], errors="coerce"
    ).fillna(0.0)
    df["_scrape_clean_retain_one_duplicate_copy"] = df[
        "_scrape_clean_retain_one_duplicate_copy"
    ].apply(lambda value: str(value).strip().lower() in {"1", "true", "yes", "y"})

    return df[SCRAPE_CLEAN_SUMMARY_COLUMNS]


def load_latest_scrape_clean_summary(path: Path | None = None) -> pd.DataFrame:
    effective_path = path or scrape_clean_summary_path()
    if not effective_path.exists():
        return pd.DataFrame(columns=SCRAPE_CLEAN_SUMMARY_COLUMNS)
    try:
        df = pd.read_csv(effective_path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=SCRAPE_CLEAN_SUMMARY_COLUMNS)
    df = _ensure_scrape_clean_summary_columns(df)
    if df.empty:
        return df
    return df.drop_duplicates(subset="_org_id", keep="last")


def _upsert_clean_summary_row(
    summary_row: dict[str, object],
    path: Path | None = None,
) -> None:
    effective_path = path or scrape_clean_summary_path()
    existing = load_latest_scrape_clean_summary(effective_path)
    merged = pd.concat([existing, pd.DataFrame([summary_row])], ignore_index=True)
    merged = _ensure_scrape_clean_summary_columns(merged)
    merged = merged.drop_duplicates(subset="_org_id", keep="last")
    _write_csv_atomic(merged, effective_path)


def clean_scraped_pages_for_org(
    org_id: str,
    settings: Settings,
    *,
    run_id: str | None = None,
) -> dict[str, object]:
    org_id_norm = str(org_id or "").strip()
    if not org_id_norm:
        raise ValueError("org_id is required")

    (
        _pages_dir,
        pages_cleaned_dir,
        scrape_clean_dir,
        clean_manifest_path,
        duplicates_path,
        run_meta_path,
    ) = _clean_paths(org_id_norm)
    scrape_manifest_path = DATA_DIR / "orgs" / org_id_norm / "scrape" / "manifest.csv"

    now = _now_iso()
    effective_run_id = run_id or now

    min_segment_chars = int(settings.scraping.clean_min_segment_chars)
    min_duplicate_page_ratio = float(settings.scraping.clean_min_duplicate_page_ratio)
    retain_one_duplicate_copy = bool(settings.scraping.clean_retain_one_duplicate_copy)

    summary_row: dict[str, object] = {
        "_org_id": org_id_norm,
        "_scrape_clean_run_id": effective_run_id,
        "_scrape_clean_status": "ok",
        "_scrape_clean_issue": "",
        "_scrape_clean_detail": "",
        "_scrape_clean_pages_total": 0,
        "_scrape_clean_pages_written": 0,
        "_scrape_clean_segments_total": 0,
        "_scrape_clean_segments_removed": 0,
        "_scrape_clean_char_count_raw": 0,
        "_scrape_clean_char_count_cleaned": 0,
        "_scrape_clean_usable_chars": 0,
        "_scrape_clean_min_segment_chars": min_segment_chars,
        "_scrape_clean_min_duplicate_page_ratio": min_duplicate_page_ratio,
        "_scrape_clean_retain_one_duplicate_copy": retain_one_duplicate_copy,
        "_scrape_source_manifest_path": str(scrape_manifest_path),
        "_scrape_source_manifest_mtime": "",
        "_scrape_source_manifest_signature": "",
        "_scrape_clean_processed_at": now,
    }

    if not scrape_manifest_path.exists():
        summary_row["_scrape_clean_status"] = "no_manifest"
        summary_row["_scrape_clean_issue"] = "manifest_missing"
        summary_row["_scrape_clean_detail"] = str(scrape_manifest_path)
        _upsert_clean_summary_row(summary_row)
        return summary_row

    summary_row["_scrape_source_manifest_mtime"] = str(int(scrape_manifest_path.stat().st_mtime_ns))

    try:
        scrape_manifest_df = pd.read_csv(scrape_manifest_path, encoding="utf-8-sig")
    except Exception as exc:
        summary_row["_scrape_clean_status"] = "manifest_unreadable"
        summary_row["_scrape_clean_issue"] = "manifest_unreadable"
        summary_row["_scrape_clean_detail"] = f"{type(exc).__name__}: {exc}"
        _upsert_clean_summary_row(summary_row)
        return summary_row

    latest_pages = _iter_latest_successful_pages(scrape_manifest_df)
    summary_row["_scrape_clean_pages_total"] = len(latest_pages)
    pages_cleaned_dir.mkdir(parents=True, exist_ok=True)
    scrape_clean_dir.mkdir(parents=True, exist_ok=True)

    if not latest_pages:
        _clear_markdown_files(pages_cleaned_dir)
        _write_csv_atomic(pd.DataFrame(columns=SCRAPE_CLEAN_MANIFEST_COLUMNS), clean_manifest_path)
        _write_csv_atomic(pd.DataFrame(columns=SCRAPE_CLEAN_DUPLICATES_COLUMNS), duplicates_path)
        summary_row["_scrape_clean_status"] = "no_success_pages"
        summary_row["_scrape_clean_issue"] = "no_success_pages"
        _upsert_clean_summary_row(summary_row)
        return summary_row

    _clear_markdown_files(pages_cleaned_dir)

    page_records: list[_PageRecord] = []
    for order, filename, source_path in latest_pages:
        try:
            content = source_path.read_text(encoding="utf-8")
        except Exception:
            continue
        page_records.append(
            _PageRecord(
                page_filename=filename,
                source_path=source_path,
                cleaned_path=pages_cleaned_dir / filename,
                page_order=order,
                segments=_segment_markdown(content),
            )
        )

    if not page_records:
        _write_csv_atomic(pd.DataFrame(columns=SCRAPE_CLEAN_MANIFEST_COLUMNS), clean_manifest_path)
        _write_csv_atomic(pd.DataFrame(columns=SCRAPE_CLEAN_DUPLICATES_COLUMNS), duplicates_path)
        summary_row["_scrape_clean_status"] = "no_readable_pages"
        summary_row["_scrape_clean_issue"] = "no_readable_pages"
        _upsert_clean_summary_row(summary_row)
        return summary_row

    per_digest_pages: dict[str, set[str]] = {}
    per_digest_segment: dict[str, _Segment] = {}
    for page in page_records:
        digests_in_page: set[str] = set()
        for segment in page.segments:
            if len(segment.normalized) < min_segment_chars:
                continue
            digests_in_page.add(segment.digest)
            if segment.digest not in per_digest_segment:
                per_digest_segment[segment.digest] = segment
        for digest in digests_in_page:
            per_digest_pages.setdefault(digest, set()).add(page.page_filename)

    page_total = len(page_records)
    duplicate_min_pages = max(2, int(math.ceil(page_total * min_duplicate_page_ratio)))
    duplicate_digests = {
        digest
        for digest, pages in per_digest_pages.items()
        if len(pages) >= duplicate_min_pages
    }

    page_sort_keys = {
        page.page_filename: (int(page.page_order), str(page.page_filename)) for page in page_records
    }
    kept_page_for_digest: dict[str, str] = {}
    if retain_one_duplicate_copy:
        for digest in sorted(duplicate_digests):
            pages = sorted(
                per_digest_pages.get(digest, set()),
                key=lambda name: page_sort_keys[name],
            )
            if pages:
                kept_page_for_digest[digest] = pages[0]

    cleaned_manifest_rows: list[dict[str, object]] = []
    duplicate_rows: list[dict[str, object]] = []

    total_segments = 0
    total_segments_removed = 0
    total_char_raw = 0
    total_char_cleaned = 0
    total_usable_chars = 0

    for page in sorted(page_records, key=lambda value: (value.page_order, value.page_filename)):
        kept_segments: list[str] = []
        removed_count = 0
        usable_chars = 0

        for segment in page.segments:
            total_segments += 1
            digest = segment.digest
            is_duplicate_segment = digest in duplicate_digests
            keep_duplicate_here = is_duplicate_segment and (
                kept_page_for_digest.get(digest) == page.page_filename
            )

            if is_duplicate_segment and not keep_duplicate_here:
                removed_count += 1
                continue

            kept_segments.append(segment.raw)
            if not is_duplicate_segment:
                usable_chars += len(segment.raw)

        cleaned_content = "\n\n".join(kept_segments).strip()
        raw_content = "\n\n".join(segment.raw for segment in page.segments).strip()
        page.cleaned_path.write_text(cleaned_content, encoding="utf-8")

        total_segments_removed += removed_count
        total_char_raw += len(raw_content)
        total_char_cleaned += len(cleaned_content)
        total_usable_chars += usable_chars

        cleaned_manifest_rows.append(
            {
                "_org_id": org_id_norm,
                "_scrape_clean_run_id": effective_run_id,
                "_page_filename": page.page_filename,
                "_page_order": int(page.page_order),
                "_page_source_path": str(page.source_path),
                "_page_cleaned_path": str(page.cleaned_path),
                "_segment_count_total": len(page.segments),
                "_segment_count_kept": len(page.segments) - removed_count,
                "_segment_count_removed": removed_count,
                "_char_count_raw": len(raw_content),
                "_char_count_cleaned": len(cleaned_content),
                "_usable_char_count": usable_chars,
                "_cleaned_at": now,
            }
        )

    for digest in sorted(duplicate_digests):
        pages = sorted(per_digest_pages.get(digest, set()), key=lambda name: page_sort_keys[name])
        keep_page = kept_page_for_digest.get(digest, "")
        removed_pages = [page for page in pages if page != keep_page]
        segment = per_digest_segment[digest]
        duplicate_rows.append(
            {
                "_org_id": org_id_norm,
                "_scrape_clean_run_id": effective_run_id,
                "_segment_hash": digest,
                "_segment_length": len(segment.normalized),
                "_segment_page_count": len(pages),
                "_segment_kept_page": keep_page,
                "_segment_removed_pages": "|".join(removed_pages),
                "_segment_preview": segment.normalized[:240],
            }
        )

    clean_manifest_df = pd.DataFrame(cleaned_manifest_rows, columns=SCRAPE_CLEAN_MANIFEST_COLUMNS)
    duplicates_df = pd.DataFrame(duplicate_rows, columns=SCRAPE_CLEAN_DUPLICATES_COLUMNS)
    _write_csv_atomic(clean_manifest_df, clean_manifest_path)
    _write_csv_atomic(duplicates_df, duplicates_path)

    summary_row["_scrape_clean_pages_written"] = int(len(cleaned_manifest_rows))
    summary_row["_scrape_clean_segments_total"] = int(total_segments)
    summary_row["_scrape_clean_segments_removed"] = int(total_segments_removed)
    summary_row["_scrape_clean_char_count_raw"] = int(total_char_raw)
    summary_row["_scrape_clean_char_count_cleaned"] = int(total_char_cleaned)
    summary_row["_scrape_clean_usable_chars"] = int(total_usable_chars)

    signature_payload = (
        f"{summary_row['_scrape_source_manifest_mtime']}|{summary_row['_scrape_clean_pages_total']}|"
        f"{summary_row['_scrape_clean_segments_removed']}|{summary_row['_scrape_clean_usable_chars']}|"
        f"{min_segment_chars}|{min_duplicate_page_ratio}|{int(retain_one_duplicate_copy)}"
    )
    summary_row["_scrape_source_manifest_signature"] = hashlib.sha1(
        signature_payload.encode("utf-8")
    ).hexdigest()

    _upsert_clean_summary_row(summary_row)

    run_meta = {
        "_org_id": org_id_norm,
        "_scrape_clean_run_id": effective_run_id,
        "_processed_at": now,
        "_settings": {
            "clean_min_segment_chars": min_segment_chars,
            "clean_min_duplicate_page_ratio": min_duplicate_page_ratio,
            "clean_retain_one_duplicate_copy": retain_one_duplicate_copy,
        },
        "_source_manifest_path": str(scrape_manifest_path),
        "_source_manifest_mtime": summary_row["_scrape_source_manifest_mtime"],
        "_source_manifest_signature": summary_row["_scrape_source_manifest_signature"],
        "_summary": {
            "pages_total": summary_row["_scrape_clean_pages_total"],
            "pages_written": summary_row["_scrape_clean_pages_written"],
            "segments_total": summary_row["_scrape_clean_segments_total"],
            "segments_removed": summary_row["_scrape_clean_segments_removed"],
            "char_count_raw": summary_row["_scrape_clean_char_count_raw"],
            "char_count_cleaned": summary_row["_scrape_clean_char_count_cleaned"],
            "usable_chars": summary_row["_scrape_clean_usable_chars"],
            "duplicate_segments": len(duplicate_rows),
        },
    }
    _write_json_atomic(run_meta, run_meta_path)
    return summary_row
