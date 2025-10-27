"""Command-line utility for running CGM pattern detection across patients.

The tool expects per-patient JSON files containing raw CGM readings. Each file
should be named ``<patient_id>.json`` inside a data directory and include a list
of day records::

    [
        {
            "service_date": "2025-01-01",
            "readings": [
                {"timestamp": "2025-01-01T00:00:00Z", "glucose_mg_dL": 110},
                ...
            ]
        },
        ...
    ]

Use ``--patient`` repeatedly or provide a newline-delimited ``--patient-file``
listing the patient IDs to process. Results are written as JSON to stdout or to
``--output`` if provided.
"""
from __future__ import annotations

import argparse
import csv
import json
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

import cgm_patterns.rules_v1  # noqa: F401 - ensure rule registration side-effects
from cgm_patterns.engine import SlidingWindowEngine
from cgm_patterns.models import CGMDay, PatternDetection
from cgm_patterns.registry import registry


def _load_patient_ids(args: argparse.Namespace) -> list[str]:
    patient_ids: list[str] = []
    if args.patient:
        patient_ids.extend(args.patient)
    if args.patient_file:
        for path in args.patient_file:
            file_path = Path(path)
            if file_path.suffix.lower() == ".csv":
                with file_path.open(newline="") as handle:
                    reader = csv.reader(handle)
                    for idx, row in enumerate(reader):
                        if not row:
                            continue
                        value = row[0].strip()
                        if not value:
                            continue
                        if idx == 0 and value.lower() in {"patient_id", "id"}:
                            continue
                        patient_ids.append(value)
            else:
                with file_path.open() as handle:
                    for line in handle:
                        line = line.strip()
                        if line:
                            patient_ids.append(line)
    if not patient_ids:
        raise SystemExit("No patient IDs provided. Use --patient or --patient-file.")
    return patient_ids


class JsonDirectorySource:
    """Simple CGM source that reads per-patient JSON day files."""

    def __init__(self, root: Path) -> None:
        if not root.is_dir():
            raise ValueError(f"CGM data directory not found: {root}")
        self._root = root

    def iter_days(self, patient_id: str) -> Iterable[CGMDay]:
        file_path = self._root / f"{patient_id}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"Missing CGM data file for patient {patient_id}: {file_path}")

        with file_path.open() as handle:
            day_records = json.load(handle)

        for record in day_records:
            yield _record_to_day(patient_id, record)


class CallableSource:
    """Wraps a Python callable that fetches CGM day records on demand."""

    def __init__(self, fetcher: Callable[[str], Iterable[Any]]) -> None:
        self._fetcher = fetcher

    def iter_days(self, patient_id: str) -> Iterable[CGMDay]:
        for record in self._fetcher(patient_id):
            yield _record_to_day(patient_id, record)


def _record_to_day(patient_id: str, record: Any) -> CGMDay:
    """Convert a record returned by a source into a ``CGMDay``."""

    if isinstance(record, CGMDay):
        return record

    if isinstance(record, Mapping):
        if "patient_id" in record and record["patient_id"] not in (None, patient_id):
            raise ValueError("Record patient_id does not match requested patient")
        service_date = record.get("service_date")
        readings = record.get("readings")
    elif isinstance(record, Sequence) and not isinstance(record, (str, bytes, bytearray)):
        try:
            service_date, readings = record
        except ValueError:
            raise ValueError("Sequence records must be (service_date, readings)") from None
    else:
        raise TypeError("Unsupported record type returned by CGM fetcher")

    if service_date is None:
        raise ValueError("CGM record missing service_date")
    service_date = pd.to_datetime(service_date).date()

    if readings is None:
        raise ValueError("CGM record missing readings")
    if isinstance(readings, pd.DataFrame):
        frame = readings.copy()
    else:
        frame = pd.DataFrame(readings)
    if "timestamp" not in frame or "glucose_mg_dL" not in frame:
        raise ValueError("CGM readings must include 'timestamp' and 'glucose_mg_dL' columns")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)

    return CGMDay(patient_id=patient_id, service_date=service_date, readings=frame)


def _detection_to_dict(detection: PatternDetection) -> dict:
    return {
        "pattern_id": detection.pattern_id,
        "effective_date": detection.effective_date.isoformat(),
        "status": detection.status.value,
        "evidence": dict(detection.evidence),
        "metrics": dict(detection.metrics),
        "confidence": detection.confidence,
        "version": detection.version,
    }


def run(  # pragma: no cover - exercised via CLI
    patient_ids: list[str],
    source,  # DailyCGMSource-like object
    *,
    analysis_days: int,
    validation_days: int,
) -> dict[str, list[dict]]:
    engine = SlidingWindowEngine(
        source,
        registry,
        analysis_days=analysis_days,
        validation_days=validation_days,
    )

    results: dict[str, list[dict]] = {}
    for patient_id in patient_ids:
        detections_by_date = engine.run_patient(patient_id)
        serialized = [
            {
                "date": analysis_date.isoformat(),
                "detections": [_detection_to_dict(det) for det in detections],
            }
            for analysis_date, detections in sorted(detections_by_date.items())
        ]
        results[patient_id] = serialized
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run CGM pattern detection in batch")
    parser.add_argument("--data-dir", type=Path, help="Directory containing <patient_id>.json files")
    parser.add_argument("--patient", action="append", help="Patient ID to process (may be repeated)")
    parser.add_argument(
        "--patient-file",
        action="append",
        help="Path to file with newline-delimited patient IDs"
    )
    parser.add_argument(
        "--fetcher",
        help="Python callable (module:function) that returns iterable CGM day records per patient",
    )
    parser.add_argument("--analysis-days", type=int, default=7, help="Number of analysis days in the window")
    parser.add_argument("--validation-days", type=int, default=14, help="Number of validation days in the window")
    parser.add_argument("--output", type=Path, help="Optional output JSON file")
    parser.add_argument("--indent", type=int, default=None, help="Pretty-print JSON with the given indent")
    return parser.parse_args(argv)


def _resolve_callable(path: str) -> Callable[[str], Iterable[Any]]:
    try:
        module_name, func_name = path.rsplit(":", 1)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError("Fetcher must be in 'module:function' format") from exc
    module = import_module(module_name)
    func = getattr(module, func_name, None)
    if not callable(func):  # pragma: no cover - defensive branch
        raise TypeError(f"{path!r} is not callable")
    return func


def _build_source(args: argparse.Namespace):
    if args.fetcher:
        fetcher = _resolve_callable(args.fetcher)
        return CallableSource(fetcher)
    if not args.data_dir:
        raise SystemExit("Either --data-dir or --fetcher must be provided")
    return JsonDirectorySource(args.data_dir)


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - CLI entry point
    args = parse_args(argv)
    patient_ids = _load_patient_ids(args)
    source = _build_source(args)
    results = run(patient_ids, source, analysis_days=args.analysis_days, validation_days=args.validation_days)

    output_text = json.dumps(results, indent=args.indent)
    if args.output:
        args.output.write_text(output_text)
    else:
        print(output_text)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
