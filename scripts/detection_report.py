#!/usr/bin/env python3
"""Produce a combined CGM pattern report with event and day counts."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Iterable

from report_utils import (
    build_report,
    derive_patient_ids,
    iter_cohort_pattern_rows,
    iter_patient_pattern_rows,
    load_detections,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bundle CGM pattern summaries into a single report",
    )
    parser.add_argument(
        "detections",
        type=Path,
        help="Path to a detections JSON file",
    )
    parser.add_argument(
        "--patients-csv",
        type=Path,
        help="Optional CSV containing patient IDs to include",
    )
    parser.add_argument(
        "--patient",
        dest="patients",
        action="append",
        help="Add a patient ID to the report (repeatable)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON report to this path instead of stdout",
    )
    parser.add_argument(
        "--rows-json",
        type=Path,
        help="Write per-pattern rows JSON for notebook use",
    )
    parser.add_argument(
        "--csv-patient-patterns",
        type=Path,
        help="Write per-patient pattern stats to CSV",
    )
    parser.add_argument(
        "--csv-cohort-patterns",
        type=Path,
        help="Write cohort-level pattern stats to CSV",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="Indent level for JSON output, negative to disable",
    )
    return parser.parse_args()


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[dict]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    data = load_detections(args.detections)
    patient_ids = derive_patient_ids(data, args.patients_csv, args.patients)

    report = build_report(data, patient_ids)

    indent = None if args.indent < 0 else args.indent
    output = json.dumps(report, indent=indent, sort_keys=True)

    if args.output:
        args.output.write_text(f"{output}\n")
    else:
        print(output)

    if args.rows_json:
        rows = list(iter_patient_pattern_rows(report))
        args.rows_json.write_text(f"{json.dumps(rows, indent=indent)}\n")

    if args.csv_patient_patterns:
        write_csv(
            args.csv_patient_patterns,
            ["patient_id", "pattern_id", "detections", "days"],
            iter_patient_pattern_rows(report),
        )

    if args.csv_cohort_patterns:
        write_csv(
            args.csv_cohort_patterns,
            ["pattern_id", "detections", "days"],
            iter_cohort_pattern_rows(report),
        )

    if report["missing_patients"]:
        joined = ", ".join(report["missing_patients"])
        print(
            f"warning: {len(report['missing_patients'])} patient ID(s) not found: {joined}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
