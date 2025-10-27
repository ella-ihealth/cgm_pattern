#!/usr/bin/env python3
"""Shared helpers for building CGM detection summaries."""
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple

PatientSummary = Dict[str, Dict[str, int]]
PatternStats = Dict[str, Dict[str, int]]


def read_patient_ids(csv_path: Path) -> List[str]:
    """Return patient IDs from the first column of a CSV file."""

    ids: List[str] = []
    with csv_path.open(newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            value = row[0].strip()
            if not value:
                continue
            if value.lower() in {"patient_id", "id"} and not ids:
                continue
            ids.append(value)
    return ids


def load_detections(path: Path) -> Dict[str, Any]:
    """Parse a detections JSON file."""

    with path.open() as handle:
        return json.load(handle)


def derive_patient_ids(
    data: Dict[str, Any],
    csv_path: Path | None,
    manual: Iterable[str] | None,
) -> List[str]:
    """Return an ordered list of patient IDs, respecting CSV/manual filters."""

    ordered: List[str] = []
    seen: set[str] = set()

    def add_many(values: Iterable[str] | None) -> None:
        if not values:
            return
        for value in values:
            if value in seen:
                continue
            ordered.append(value)
            seen.add(value)

    if csv_path:
        add_many(read_patient_ids(csv_path))
    add_many(manual)

    if not ordered:
        add_many(sorted(data.keys()))

    return ordered


def summarise_patient(patient_block: Dict[str, Any]) -> Tuple[int, PatternStats]:
    """Return total detections and per-pattern stats for a patient block."""

    detections = patient_block.get("detections", {})
    per_pattern_counts: Counter[str] = Counter()
    per_pattern_days: Dict[str, set[str]] = defaultdict(set)

    total_events = 0
    for day, entries in detections.items():
        if not entries:
            continue
        for entry in entries:
            pattern_id = entry.get("pattern_id")
            if not pattern_id:
                continue
            per_pattern_counts[pattern_id] += 1
            per_pattern_days[pattern_id].add(day)
            total_events += 1

    pattern_summary: PatternStats = {
        pattern: {
            "detections": per_pattern_counts[pattern],
            "days": len(per_pattern_days[pattern]),
        }
        for pattern in sorted(per_pattern_counts)
    }

    return total_events, pattern_summary


def build_report(data: Dict[str, Any], patient_ids: Iterable[str]) -> Dict[str, Any]:
    """Create a combined report for the requested patients."""

    report: Dict[str, Any] = {
        "patients": {},
        "cohort": {
            "total_events": 0,
            "patterns": {},
        },
        "missing_patients": [],
    }

    cohort_patterns: Dict[str, Dict[str, int]] = {}

    for patient_id in patient_ids:
        patient_block = data.get(patient_id)
        if not patient_block:
            report["missing_patients"].append(patient_id)
            continue

        total_events, pattern_summary = summarise_patient(patient_block)
        report["patients"][patient_id] = {
            "total_events": total_events,
            "patterns": pattern_summary,
        }

        report["cohort"]["total_events"] += total_events
        for pattern, stats in pattern_summary.items():
            bucket = cohort_patterns.setdefault(
                pattern,
                {"detections": 0, "days": 0},
            )
            bucket["detections"] += stats["detections"]
            bucket["days"] += stats["days"]

    report["cohort"]["patterns"] = dict(sorted(cohort_patterns.items()))
    report["missing_patients"].sort()

    return report


def iter_patient_pattern_rows(report: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Yield row dicts for per-patient pattern statistics."""

    for patient_id in sorted(report.get("patients", {})):
        for pattern_id, stats in report["patients"][patient_id]["patterns"].items():
            yield {
                "patient_id": patient_id,
                "pattern_id": pattern_id,
                "detections": stats["detections"],
                "days": stats["days"],
            }


def iter_cohort_pattern_rows(report: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Yield row dicts for cohort-level pattern statistics."""

    for pattern_id, stats in report.get("cohort", {}).get("patterns", {}).items():
        yield {
            "pattern_id": pattern_id,
            "detections": stats["detections"],
            "days": stats["days"],
        }


def report_to_rows(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return per-pattern rows for DataFrame-friendly usage."""

    return list(iter_patient_pattern_rows(report))
