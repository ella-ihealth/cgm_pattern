"""Feature engineering helpers for CGM data."""
from __future__ import annotations

from datetime import timedelta
from typing import Final

import numpy as np
import pandas as pd

from .models import CGMDay, DailyCGMSummary

_TIME_STEP_MINUTES: Final[float] = 5.0


def compute_daily_summary(day: CGMDay, high_threshold: float = 180.0, low_threshold: float = 70.0) -> DailyCGMSummary:
    """Aggregate a day's readings into reusable metrics."""

    readings = day.readings
    if readings.empty:
        return DailyCGMSummary(
            patient_id=day.patient_id,
            service_date=day.service_date,
            mean_glucose=float("nan"),
            std_glucose=float("nan"),
            percent_high=0.0,
            percent_low=0.0,
            percent_in_range=0.0,
            time_high_minutes=0.0,
            time_low_minutes=0.0,
            time_in_range_minutes=0.0,
            max_glucose=float("nan"),
            min_glucose=float("nan"),
            total_readings=0,
            coverage_ratio=0.0,
        )

    values = readings["glucose_mg_dL"].astype(float).to_numpy()
    total_minutes = len(values) * _TIME_STEP_MINUTES

    high_mask = values > high_threshold
    low_mask = values < low_threshold

    minutes_high = float(high_mask.sum()) * _TIME_STEP_MINUTES
    minutes_low = float(low_mask.sum()) * _TIME_STEP_MINUTES
    minutes_in_range = total_minutes - (minutes_high + minutes_low)

    return DailyCGMSummary(
        patient_id=day.patient_id,
        service_date=day.service_date,
        mean_glucose=float(np.nanmean(values)),
        std_glucose=float(np.nanstd(values, ddof=0)),
        percent_high=minutes_high / total_minutes if total_minutes else 0.0,
        percent_low=minutes_low / total_minutes if total_minutes else 0.0,
        percent_in_range=minutes_in_range / total_minutes if total_minutes else 0.0,
        time_high_minutes=minutes_high,
        time_low_minutes=minutes_low,
        time_in_range_minutes=minutes_in_range,
        max_glucose=float(np.nanmax(values)),
        min_glucose=float(np.nanmin(values)),
        total_readings=len(values),
        coverage_ratio=day.coverage_ratio(),
    )
