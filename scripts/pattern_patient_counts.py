#!/usr/bin/env python3
"""Count how many patients triggered each pattern across detection files."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable

from report_utils import load_detections, summarise_patient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count patients per CGM pattern across detections",
    )
    parser.add_argument(
        "detections_dir",
        type=Path,
        nargs="?",
        default=Path("detections"),
        help="Directory containing *_detections.json files (default: detections)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write counts as JSON to this path",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Write counts as CSV",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="Indent level for JSON output (default: 2)",
    )
    return parser.parse_args()


def write_csv(path: Path, rows: Iterable[Dict[str, int]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["pattern_id", "patient_count"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    detections_dir = args.detections_dir
    files = sorted(detections_dir.glob("*_detections.json"))

    if not files:
        raise SystemExit(f"no detection files found in {detections_dir}")

    pattern_patients: Dict[str, set[str]] = defaultdict(set)

    for path in files:
        data = load_detections(path)
        for patient_id, block in data.items():
            _, pattern_summary = summarise_patient(block)
            for pattern_id in pattern_summary:
                pattern_patients[pattern_id].add(patient_id)

    counts = {
        pattern_id: len(patients)
        for pattern_id, patients in sorted(
            pattern_patients.items(), key=lambda item: (-len(item[1]), item[0])
        )
    }

    indent = None if args.indent < 0 else args.indent
    payload = json.dumps(counts, indent=indent)

    if args.output:
        args.output.write_text(f"{payload}\n")
    else:
        print(payload)

    if args.csv:
        rows = (
            {"pattern_id": pattern_id, "patient_count": count}
            for pattern_id, count in counts.items()
        )
        write_csv(args.csv, rows)


if __name__ == "__main__":
    main()
