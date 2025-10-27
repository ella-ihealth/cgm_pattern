"""Utilities for fetching CGM data from the UC backend without saving to disk."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List, Sequence

import pandas as pd
import requests

from api_clients.cgm_client import (
    convert_excursion_trend_result,
    convert_rolling_stats_response,
)
from models.cgm_models import (
    CgmExcursionTrendResult,
    CgmRollingStatsResponse,
)
from cgm_patterns.models import (
    CGMDay,
    ExcursionTrendSummary,
    PatternInputBundle,
    RollingStatsSnapshot,
    RollingWindowSummary,
)

_BASE_URL = os.getenv("AI_RAG_UC_BACKEND_API_BASE_URL") or "https://uc-prod.ihealth-eng.com/v1/uc"
_SESSION_TOKEN = os.getenv("AI_RAG_UC_BACKEND_SESSION_TOKEN")
_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True)
class PatientCGMData:
    """Aggregate CGM artefacts fetched for a patient."""

    days: Sequence[CGMDay]
    rolling_snapshot: RollingStatsSnapshot | None
    excursion_summary: ExcursionTrendSummary | None


def _request(path: str, payload: dict) -> dict:
    if not _SESSION_TOKEN:
        raise RuntimeError("AI_RAG_UC_BACKEND_SESSION_TOKEN not set; load .env before calling CGM_fetcher")
    url = f"{_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "x-session-token": _SESSION_TOKEN,
        "content-type": "application/json",
        "accept": "application/json",
        "origin": "https://portal.ihealthunifiedcare.com",
    }
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def _format_utc_offset(offset: timedelta) -> str:
    if offset == timedelta(0):
        return "UTC"
    total_seconds = offset.total_seconds()
    sign = "+" if total_seconds > 0 else "-"
    total_minutes = int(round(abs(total_seconds) / 60))
    hours, minutes = divmod(total_minutes, 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


def _parse_flat_readings(entries: Sequence[dict], patient_id: str) -> List[CGMDay]:
    frame = pd.DataFrame(entries)
    if frame.empty:
        return []

    timestamp_key = next((key for key in ("timestamp", "utc", "time") if key in frame.columns), None)
    if timestamp_key is None:
        return []

    frame["timestamp"] = pd.to_datetime(frame[timestamp_key], errors="coerce", utc=True)
    frame = frame.dropna(subset=["timestamp"])
    if frame.empty:
        return []

    if "glucose_mg_dL" not in frame.columns:
        if "value" in frame.columns:
            frame["glucose_mg_dL"] = frame["value"]
        elif "glucoseValue" in frame.columns:
            frame["glucose_mg_dL"] = frame["glucoseValue"]

    if "glucose_mg_dL" not in frame.columns:
        return []

    frame["glucose_mg_dL"] = pd.to_numeric(frame["glucose_mg_dL"], errors="coerce")
    frame = frame.dropna(subset=["glucose_mg_dL"])
    if frame.empty:
        return []

    if "localTime" in frame.columns:
        frame["local_timestamp"] = pd.to_datetime(frame["localTime"], errors="coerce")
    else:
        frame["local_timestamp"] = pd.NaT

    if frame["local_timestamp"].notna().any():
        frame["service_date"] = frame["local_timestamp"].dt.date
    else:
        frame["service_date"] = frame["timestamp"].dt.tz_convert(timezone.utc).dt.date

    frame = frame.sort_values("timestamp")

    results: List[CGMDay] = []
    for service_date, group in frame.groupby("service_date", sort=True):
        group = group.copy()
        timezone_label = None
        local_samples = group["local_timestamp"].dropna()
        if not local_samples.empty:
            idx = local_samples.index[0]
            local_dt = local_samples.iloc[0].to_pydatetime()
            utc_dt = group.loc[idx, "timestamp"].to_pydatetime()
            offset = local_dt - utc_dt.replace(tzinfo=None)
            if abs(offset) <= timedelta(hours=14):
                timezone_label = _format_utc_offset(offset)
        readings = group.drop(columns=["service_date"])
        if "local_timestamp" in readings.columns:
            readings = readings.drop(columns=["local_timestamp"])
        results.append(
            CGMDay(
                patient_id=patient_id,
                service_date=service_date,
                readings=readings,
                local_timezone=timezone_label,
            )
        )
    return results


def _parse_days(container: dict, patient_id: str) -> List[CGMDay]:
    raw = container.get("rawData") if isinstance(container, dict) else container
    if isinstance(raw, dict):
        days = raw.get("days") or []
    elif isinstance(raw, list):
        days = raw
    else:
        days = []

    if days and isinstance(days[0], dict) and "readings" not in days[0] and any(key in days[0] for key in ("timestamp", "utc", "value", "glucoseValue")):
        return _parse_flat_readings(days, patient_id)

    results: List[CGMDay] = []
    for item in days:
        readings = pd.DataFrame(item.get("readings", []))
        if readings.empty:
            continue
        readings["timestamp"] = pd.to_datetime(readings["timestamp"], utc=True)
        service_date_str = item.get("serviceDate")
        service_dt = datetime.fromisoformat(service_date_str.replace("Z", "+00:00")) if service_date_str else None
        results.append(
            CGMDay(
                patient_id=patient_id,
                service_date=service_dt.date() if service_dt else date.today(),
                readings=readings,
                local_timezone=item.get("timezone"),
            )
        )
    return results


def _fetch_raw_days(patient_id: str, start: datetime, end: datetime) -> List[CGMDay]:
    payload = {
        "patientId": patient_id,
        "startTime": start.strftime(_TIME_FORMAT),
        "endTime": end.strftime(_TIME_FORMAT),
        "includeAvailableDates": True,
        "includeRawData": True,
    }
    response = _request("/cgm/reading", payload)
    data = response.get("data", {}) or {}

    days = _parse_days(data, patient_id)
    available_dates = data.get("availableDates") or []

    for date_str in available_dates:
        narrowed_start = datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(timezone.utc)
        narrowed_end = narrowed_start + timedelta(days=1) - timedelta(seconds=1)
        sub_payload = {
            "patientId": patient_id,
            "startTime": narrowed_start.strftime(_TIME_FORMAT),
            "endTime": narrowed_end.strftime(_TIME_FORMAT),
            "includeRawData": True,
        }
        sub_response = _request("/cgm/reading", sub_payload)
        days.extend(_parse_days(sub_response.get("data", {}) or {}, patient_id))

    return days


def build_input_bundle(
    patient_id: str,
    *,
    start: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc),
    end: datetime | None = None,
) -> PatternInputBundle:
    if end is None:
        end = datetime.now(timezone.utc)

    days = _fetch_raw_days(patient_id, start, end)
    days.sort(key=lambda d: d.service_date)

    rolling_snapshot: RollingStatsSnapshot | None = None
    excursion_summary: ExcursionTrendSummary | None = None

    try:
        rolling_resp = _request("/cgm/rolling-stats", {"patientId": patient_id})
        rolling_snapshot = convert_rolling_stats_response(CgmRollingStatsResponse(**(rolling_resp.get("data") or {})))
    except Exception:
        rolling_snapshot = None

    try:
        excursion_resp = _request("/cgm/agp/excursion-trend", {"patientId": patient_id})
        excursion_summary = convert_excursion_trend_result(CgmExcursionTrendResult(**(excursion_resp.get("data") or {})))
    except Exception:
        excursion_summary = None

    return PatternInputBundle(
        analysis_days=tuple(days),
        validation_days=tuple(days),
        analysis_summaries=(),
        validation_summaries=(),
        rolling_windows=rolling_snapshot.windows if rolling_snapshot else (),
        rolling_snapshot=rolling_snapshot,
        excursion_summary=excursion_summary,
    )


def iter_cgm_days(patient_id: str, *, start: datetime | None = None, end: datetime | None = None) -> Iterable[CGMDay]:
    bundle = build_input_bundle(
        patient_id,
        start=start or datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=end,
    )
    for day in bundle.analysis_days:
        yield day
