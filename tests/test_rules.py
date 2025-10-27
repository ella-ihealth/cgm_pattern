from __future__ import annotations

from datetime import date, timedelta

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

import cgm_patterns.rules_v1  # noqa: F401 - ensure registration side-effects
from cgm_patterns.cache import DailySummaryCache
from cgm_patterns.engine import SlidingWindowEngine
from cgm_patterns.models import CGMDay, PatternStatus
from cgm_patterns.registry import registry


class InMemorySource:
    def __init__(self, patient_id: str, days: list[CGMDay]) -> None:
        self._patient_id = patient_id
        self._days = sorted(days, key=lambda d: d.service_date)

    def iter_days(self, patient_id: str):
        assert patient_id == self._patient_id
        for day in self._days:
            yield day


def _make_day(patient_id: str, service_date: date, values: np.ndarray) -> CGMDay:
    timestamps = pd.date_range(start=pd.Timestamp(service_date), periods=len(values), freq="5min", tz="UTC")
    frame = pd.DataFrame({"timestamp": timestamps, "glucose_mg_dL": values})
    return CGMDay(patient_id=patient_id, service_date=service_date, readings=frame)


def _constant_day(patient_id: str, service_date: date, value: float = 120.0) -> CGMDay:
    values = np.full(288, value, dtype=float)
    return _make_day(patient_id, service_date, values)


def test_recurrent_post_meal_spike_detected():
    patient_id = "patient-spike"
    start = date(2024, 1, 1)
    days: list[CGMDay] = []
    for offset in range(7):
        current_date = start + timedelta(days=offset)
        values = np.full(288, 110.0, dtype=float)
        if offset in {2, 4, 6}:
            start_idx = (8 * 60) // 5  # 08:00
            values[start_idx : start_idx + 12] = 120.0
            values[start_idx + 12 : start_idx + 24] = 205.0
        days.append(_make_day(patient_id, current_date, values))

    source = InMemorySource(patient_id, days)
    engine = SlidingWindowEngine(source, registry, summary_cache=DailySummaryCache())
    detections_by_day = engine.run_patient(patient_id)
    final_detections = {det.pattern_id: det for det in detections_by_day[days[-1].service_date]}

    assert final_detections["recurrent_post_meal_spike"].status is PatternStatus.DETECTED


def test_dawn_phenomenon_detected():
    patient_id = "patient-dawn"
    start = date(2024, 2, 1)
    days: list[CGMDay] = []
    for offset in range(7):
        current_date = start + timedelta(days=offset)
        values = np.full(288, 125.0, dtype=float)
        if offset in {1, 3, 4, 6}:
            start_idx = (4 * 60) // 5  # 04:00
            length = 12  # 60 minutes
            values[start_idx : start_idx + length] = 210.0
        days.append(_make_day(patient_id, current_date, values))

    source = InMemorySource(patient_id, days)
    engine = SlidingWindowEngine(source, registry, summary_cache=DailySummaryCache())
    detections_by_day = engine.run_patient(patient_id)
    final_detections = {det.pattern_id: det for det in detections_by_day[days[-1].service_date]}

    assert final_detections["dawn_phenomenon"].status is PatternStatus.DETECTED


def test_morning_hyperglycemia_detected():
    patient_id = "patient-morning-hyper"
    start = date(2024, 4, 1)
    days: list[CGMDay] = []
    for offset in range(7):
        current_date = start + timedelta(days=offset)
        values = np.full(288, 120.0, dtype=float)
        if offset in {1, 3, 5}:  # sustained highs in the target window
            high_start = (4 * 60) // 5  # 04:00
            high_length = 6  # 30 minutes
            values[high_start : high_start + high_length] = 160.0
        days.append(_make_day(patient_id, current_date, values))

    source = InMemorySource(patient_id, days)
    engine = SlidingWindowEngine(source, registry, summary_cache=DailySummaryCache())
    detections_by_day = engine.run_patient(patient_id)
    final_detections = {det.pattern_id: det for det in detections_by_day[days[-1].service_date]}

    morning_detection = final_detections["morning_hyperglycemia"]
    assert morning_detection.status is PatternStatus.DETECTED
    assert morning_detection.metrics["morning_high_days"] >= 3


def test_summary_based_rules_detected():
    patient_id = "patient-summary"
    start = date(2024, 3, 1)
    days: list[CGMDay] = []
    for offset in range(7):
        current_date = start + timedelta(days=offset)
        values = np.full(288, 120.0, dtype=float)
        if offset in {1, 2, 4, 6}:  # drive TIR down
            high_idx_start = (10 * 60) // 5
            high_length = 120
            values[high_idx_start : high_idx_start + high_length] = 210.0
        if offset in {2, 3, 4, 6}:  # increase CV via oscillation
            values[::2] = 80.0
            values[1::2] = 200.0
        if offset in {2, 4, 5}:  # hypoglycemia windows (~90 minutes)
            hypo_start = (17 * 60) // 5
            hypo_length = 18
            values[hypo_start : hypo_start + hypo_length] = 60.0
        days.append(_make_day(patient_id, current_date, values))

    source = InMemorySource(patient_id, days)
    engine = SlidingWindowEngine(source, registry, summary_cache=DailySummaryCache())
    detections_by_day = engine.run_patient(patient_id)
    final_detections = {det.pattern_id: det for det in detections_by_day[days[-1].service_date]}

    assert final_detections["predominant_hypoglycemia"].status is PatternStatus.DETECTED
    assert final_detections["high_glycemic_variability"].status is PatternStatus.DETECTED
    assert final_detections["predominant_hyperglycemia"].status is PatternStatus.DETECTED
