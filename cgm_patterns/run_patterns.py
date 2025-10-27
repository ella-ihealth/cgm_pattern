from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Iterable

from cgm_patterns.CGM_fetcher import iter_cgm_days
from cgm_patterns.engine import SlidingWindowEngine
from cgm_patterns.models import CGMDay, PatternStatus
import cgm_patterns.rules  # Ensure rules are imported and registered
from cgm_patterns.registry import registry
from cgm_patterns.cache import DailySummaryCache


class CGMSource:
    """Adapter that yields CGMDay objects using CGM_fetcher."""

    def __init__(self, start: datetime | None = None, end: datetime | None = None) -> None:
        self._start = start
        self._end = end

    def iter_days(self, patient_id: str) -> Iterable[CGMDay]:
        return iter_cgm_days(patient_id, start=self._start, end=self._end)


def read_patient_ids(csv_file: Path) -> list[str]:
    ids: list[str] = []
    with csv_file.open(newline="") as handle:
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


def build_rule_filter(allowed_patterns: set[str] | None):
    if not allowed_patterns:
        return None
    allowed = {pid.lower() for pid in allowed_patterns}

    def _filter(rule):
        return rule.id.lower() in allowed

    return _filter


def _summarize_detections(detections_by_date):
    filtered: dict[str, list[dict]] = {}
    pattern_summary: dict[str, set[str]] = {}
    for analysis_date, detections in sorted(detections_by_date.items()):
        detected = [
            {
                "pattern_id": detection.pattern_id,
                "metrics": dict(detection.metrics),
                "evidence": dict(detection.evidence),
                "confidence": detection.confidence,
                "version": detection.version,
            }
            for detection in detections
            if detection.status is PatternStatus.DETECTED
        ]
        if detected:
            filtered[analysis_date.isoformat()] = detected
            for payload in detected:
                pid = payload["pattern_id"]
                pattern_summary.setdefault(pid, set()).add(analysis_date.isoformat())
    summary = [
        {
            "pattern_id": pattern_id,
            "dates": sorted(dates),
        }
        for pattern_id, dates in sorted(pattern_summary.items())
    ]
    return filtered, summary


def run(
    csv_file: Path,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    allowed_patterns: set[str] | None = None,
    show_progress: bool = False,
    workers: int = 1,
) -> dict[str, dict]:
    patient_ids = read_patient_ids(csv_file)
    if not any(True for _ in registry.items()):
        raise RuntimeError("No CGM pattern rules are registered. Ensure rule modules are imported.")
    rule_filter = build_rule_filter(allowed_patterns)

    results: dict[str, dict] = {}
    total = len(patient_ids)
    if show_progress and total == 0:
        print("No patient IDs to process.", file=sys.stderr, flush=True)

    worker_count = max(1, workers)

    def _run_single(patient_id: str) -> tuple[str, dict[str, list[dict]], list[dict]]:
        engine = SlidingWindowEngine(
            CGMSource(start=start, end=end),
            registry,
            analysis_days=14,
            validation_days=30,
            summary_cache=DailySummaryCache(),
        )
        detections_by_date = engine.run_patient(patient_id, rule_filter=rule_filter)
        filtered, summary = _summarize_detections(detections_by_date)
        return patient_id, filtered, summary

    if worker_count == 1:
        engine = SlidingWindowEngine(
            CGMSource(start=start, end=end),
            registry,
            analysis_days=14,
            validation_days=30,
        )
        for index, patient_id in enumerate(patient_ids, start=1):
            if show_progress:
                print(
                    f"[{index}/{total}] Processing patient {patient_id}...",
                    file=sys.stderr,
                    flush=True,
                )
            detections_by_date = engine.run_patient(patient_id, rule_filter=rule_filter)
            filtered, summary = _summarize_detections(detections_by_date)
            results[patient_id] = {
                "detections": filtered,
                "summary": summary,
            }
            if show_progress:
                detected_days = len(filtered)
                detected_patterns = sum(len(entries) for entries in filtered.values())
                print(
                    f"    -> {detected_patterns} detections across {detected_days} day(s)",
                    file=sys.stderr,
                    flush=True,
                )
        if show_progress and total > 0:
            print("Completed processing all patients.", file=sys.stderr, flush=True)
        return results

    progress_lock = Lock()
    processed = 0

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {executor.submit(_run_single, pid): pid for pid in patient_ids}
        for future in as_completed(future_map):
            patient_id, filtered, summary = future.result()
            results[patient_id] = {
                "detections": filtered,
                "summary": summary,
            }
            if show_progress:
                with progress_lock:
                    processed += 1
                    detected_days = len(filtered)
                    detected_patterns = sum(len(entries) for entries in filtered.values())
                    print(
                        f"[{processed}/{total}] Processed patient {patient_id} -> {detected_patterns} detections across {detected_days} day(s)",
                        file=sys.stderr,
                        flush=True,
                    )

    if show_progress and total > 0:
        print("Completed processing all patients.", file=sys.stderr, flush=True)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run CGM patterns for patients in a CSV")
    parser.add_argument("csv_file", type=Path, help="CSV file containing patient IDs")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--patterns", nargs="*", help="Pattern IDs to run (optional)")
    parser.add_argument("--output", type=Path, help="Optional path to write JSON output")
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Suppress per-patient progress reporting.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent worker threads to use (default: 1).",
    )
    args = parser.parse_args(argv)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc) if args.start else None
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc) if args.end else None
    allowed = set(args.patterns) if args.patterns else None

    results = run(
        args.csv_file,
        start=start,
        end=end,
        allowed_patterns=allowed,
        show_progress=not args.no_progress,
        workers=args.workers,
    )

    if args.output:
        args.output.write_text(json.dumps(results, indent=2))
    else:
        print(json.dumps(results, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
