#!/usr/bin/env python3
"""Compute how many unique days each patient experiences each pattern."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict

from report_utils import (
    build_report,
    derive_patient_ids,
    load_detections,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarise detection days per patient and pattern",
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
        help="Limit the summary to specific patient IDs (repeatable)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the summary to a JSON file instead of stdout",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="Indent level for JSON output (default: 2)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = load_detections(args.detections)
    patient_ids = derive_patient_ids(data, args.patients_csv, args.patients)

    report = build_report(data, patient_ids)

    summary: Dict[str, Dict[str, int]] = {}
    for patient_id, payload in report["patients"].items():
        summary[patient_id] = {
            pattern_id: stats["days"]
            for pattern_id, stats in payload["patterns"].items()
        }

    indent = None if args.indent < 0 else args.indent
    output = json.dumps(summary, indent=indent, sort_keys=True)

    if args.output:
        args.output.write_text(f"{output}\n")
    else:
        print(output)

    if report["missing_patients"]:
        joined = ", ".join(report["missing_patients"])
        print(
            f"warning: {len(report['missing_patients'])} patient ID(s) not found: {joined}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
