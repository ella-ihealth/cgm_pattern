"""Shared utilities for CGM pattern rule implementations."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Iterable

import math
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

from ..models import CGMDay


@dataclass(frozen=True)
class PreparedDay:
    """Normalized CGM day data with convenience views."""

    frame: pd.DataFrame
    timezone: str | None

    @property
    def local_series(self) -> pd.Series:
        return self.frame["local_time"]

    @property
    def glucose(self) -> pd.Series:
        return self.frame["glucose_mg_dL"]


def prepare_day(day: CGMDay) -> PreparedDay:
    """Return a normalized dataframe for rule computations."""

    df = day.readings.copy()
    if df.empty:
        return PreparedDay(pd.DataFrame(columns=["timestamp", "local_time", "glucose_mg_dL", "minutes"]), day.local_timezone)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["glucose_mg_dL"] = pd.to_numeric(df.get("glucose_mg_dL"), errors="coerce")
    df = df.dropna(subset=["timestamp", "glucose_mg_dL"])
    df = df.sort_values("timestamp")
    if df.empty:
        return PreparedDay(pd.DataFrame(columns=["timestamp", "local_time", "glucose_mg_dL", "minutes"]), day.local_timezone)

    if day.local_timezone:
        try:
            tz = ZoneInfo(day.local_timezone)
            df["local_time"] = df["timestamp"].dt.tz_convert(tz)
        except Exception:
            df["local_time"] = df["timestamp"]
    else:
        df["local_time"] = df["timestamp"]

    deltas = df["timestamp"].shift(-1) - df["timestamp"]
    minutes = deltas.dt.total_seconds() / 60.0
    if len(minutes.dropna()) > 0:
        fallback = float(minutes.dropna().median())
        if math.isnan(fallback) or fallback <= 0:
            fallback = 5.0
    else:
        fallback = 5.0
    minutes = minutes.fillna(fallback)
    if minutes.empty:
        minutes = pd.Series([fallback], index=df.index)
    else:
        minutes.iloc[-1] = fallback
    df["minutes"] = minutes

    return PreparedDay(df.reset_index(drop=True), day.local_timezone)


def filter_time_window(day: PreparedDay, start_hour: float, end_hour: float) -> pd.DataFrame:
    """Slice the prepared dataframe to a local-time window.

    Hours are expressed in fractional hours [0, 24). end_hour is exclusive unless the
    window wraps past midnight, in which case entries beyond start_hour or below
    end_hour are included.
    """

    frame = day.frame
    if frame.empty:
        return frame

    local = frame["local_time"].dt.hour + frame["local_time"].dt.minute / 60.0
    if start_hour <= end_hour:
        mask = (local >= start_hour) & (local < end_hour)
    else:
        mask = (local >= start_hour) | (local < end_hour)
    return frame.loc[mask]


def total_minutes(rows: pd.DataFrame | pd.Series) -> float:
    """Sum the minutes column for the provided rows/series."""

    if isinstance(rows, pd.Series):
        minutes = rows
    else:
        minutes = rows.get("minutes")
    if minutes is None or minutes.empty:
        return 0.0
    return float(minutes.sum())


def consecutive_durations(mask: pd.Series, minutes: pd.Series) -> Iterable[tuple[int, float]]:
    """Yield (start_index, duration_minutes) for contiguous true regions."""

    if mask.empty:
        return []

    durations: list[tuple[int, float]] = []
    active_index = None
    acc_minutes = 0.0
    for idx, (flag, minute_value) in enumerate(zip(mask.astype(bool), minutes)):
        if flag:
            if active_index is None:
                active_index = idx
                acc_minutes = 0.0
            acc_minutes += float(minute_value)
        elif active_index is not None:
            durations.append((active_index, acc_minutes))
            active_index = None
            acc_minutes = 0.0
    if active_index is not None:
        durations.append((active_index, acc_minutes))
    return durations


def coefficient_of_variation(series: pd.Series) -> float | None:
    if series.empty:
        return None
    mean_val = float(series.mean())
    if math.isnan(mean_val) or mean_val == 0:
        return None
    std_val = float(series.std(ddof=0))
    if math.isnan(std_val):
        return None
    return std_val / mean_val


def interquartile_range(series: pd.Series) -> float | None:
    if series.empty:
        return None
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    if math.isnan(q1) or math.isnan(q3):
        return None
    return float(q3 - q1)


def day_of_week(day: PreparedDay) -> int | None:
    frame = day.frame
    if frame.empty:
        return None
    return int(frame["local_time"].dt.dayofweek.mode().iat[0]) if not frame.empty else None


def rolling_delta(series: pd.Series, window: str, *, center: bool = False) -> pd.Series:
    """Return rolling delta (last minus first) over a time window."""

    def _delta(arr: pd.Series) -> float:
        if arr.empty:
            return float("nan")
        return float(arr.iloc[-1] - arr.iloc[0])

    result = series.sort_index().rolling(window, center=center).apply(_delta, raw=False)
    return result


def rate_of_change(series: pd.Series) -> pd.Series:
    """Return per-minute rate of change between consecutive readings."""

    series = series.sort_index()
    diffs = series.diff()
    time_diffs = series.index.to_series().diff().dt.total_seconds() / 60.0
    safe_time = time_diffs.replace(0, pd.NA)
    rates = diffs.divide(safe_time)
    rates = rates.astype("float64")
    rates[~np.isfinite(rates)] = np.nan
    rates = rates.fillna(pd.NA)
    return rates
