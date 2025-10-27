#!/usr/bin/env python3
"""Summarise pattern detections per patient and across a cohort."""
from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict

from report_utils import load_detections, read_patient_ids, summarise_patient


def main() -> None:
    detections_path = Path("41-50_detections.json")
    patient_csv = Path("CGM_patient_41-50.csv")

    data = load_detections(detections_path)
    patient_ids = read_patient_ids(patient_csv)
    cohort_totals: Counter[str] = Counter()
    cohort_days: Dict[str, int] = defaultdict(int)

    for patient_id in patient_ids:
        patient_block = data.get(patient_id, {})
        total_events, pattern_summary = summarise_patient(patient_block)
        cohort_totals.update(
            {
                pattern_id: stats["detections"]
                for pattern_id, stats in pattern_summary.items()
            }
        )
        for pattern_id, stats in pattern_summary.items():
            cohort_days[pattern_id] += stats["days"]

        print(patient_id)
        print(f"  total detections: {total_events}")
        ordered = sorted(
            pattern_summary.items(),
            key=lambda item: item[1]["detections"],
            reverse=True,
        )
        for pattern_id, stats in ordered:
            print(
                f"    {pattern_id}: {stats['detections']} detections across {stats['days']} days",
            )
        print()

    print("Cohort summary")
    for pattern_id, count in cohort_totals.most_common():
        days = cohort_days[pattern_id]
        print(f"  {pattern_id}: {count} detections across {days} days")


if __name__ == "__main__":
    main()
