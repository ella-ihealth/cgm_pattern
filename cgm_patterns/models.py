"""Core data models for CGM pattern detection."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

import pandas as pd
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from .rules.utils import PreparedDay


@dataclass(frozen=True)
class PatternDescriptor:
    """Static metadata describing a pattern signature."""

    pattern_id: str
    name: str
    description: str
    version: str = "1.0.0"
    inputs: tuple[str, ...] = ("cgm",)
    config_defaults: Mapping[str, Any] = field(default_factory=dict)
    tags: Sequence[str] = field(default_factory=tuple)


class PatternStatus(str, Enum):
    """Detection status"""

    DETECTED = "detected"
    NOT_DETECTED = "not_detected"
    INSUFFICIENT_DATA = "insufficient_data"


@dataclass(frozen=True)
class PatternDetection:
    """Standardized output for a single pattern evaluation."""

    pattern_id: str
    effective_date: date
    status: PatternStatus
    evidence: Mapping[str, Any] = field(default_factory=dict)
    metrics: Mapping[str, float] = field(default_factory=dict)
    confidence: Optional[float] = None
    version: Optional[str] = None


@dataclass(frozen=True)
class CGMDay:
    """Raw CGM readings for a single day, with optional local timezone."""

    patient_id: str
    service_date: date
    readings: pd.DataFrame
    local_timezone: Optional[str] = None

    def coverage_ratio(self) -> float:
        if self.readings.empty:
            return 0.0
        expected_points = 288
        timestamps = self.readings.get("timestamp")
        if timestamps is not None:
            try:
                ordered = pd.to_datetime(timestamps, utc=True, errors="coerce").sort_values()
                diffs = ordered.diff().dropna().dt.total_seconds()
                if not diffs.empty:
                    median_seconds = float(diffs.median())
                    if median_seconds > 0:
                        expected_points = max(1, int(round(86400.0 / median_seconds)))
            except Exception:  # pragma: no cover - fall back to default cadence
                expected_points = 288
        return min(1.0, len(self.readings) / expected_points) if expected_points else 0.0

    def readings_local(self) -> pd.DataFrame:
        """Return readings converted to the declared local timezone."""

        if self.local_timezone is None:
            return self.readings
        df = self.readings.copy()
        tz = ZoneInfo(self.local_timezone)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(tz)
        return df


@dataclass(frozen=True)
class DailyCGMSummary:
    """Metrics derived from a day of CGM data (typically 24h)."""

    patient_id: str
    service_date: date
    mean_glucose: float
    std_glucose: float
    percent_high: float
    percent_low: float
    percent_in_range: float
    time_high_minutes: float
    time_low_minutes: float
    time_in_range_minutes: float
    max_glucose: float
    min_glucose: float
    total_readings: int
    coverage_ratio: float


@dataclass(frozen=True)
class RollingWindowSummary:
    """Aggregated statistics for a 14-day rolling window"""

    patient_id: str
    start_date: Optional[date]
    end_date: date
    days_worn: Optional[int]
    percent_time_active: Optional[float]
    mean_glucose: Optional[float]
    gmi: Optional[float]
    gv: Optional[float]
    time_percentages: Mapping[str, float] = field(default_factory=dict)
    extra_metrics: Mapping[str, float] = field(default_factory=dict)
    window_valid: Optional[bool] = None


@dataclass(frozen=True)
class RollingStatsSnapshot:
    """Snapshot response from the rolling-stats API."""

    patient_id: str
    window_type: Optional[str]
    generated_for_date: Optional[date]
    windows: Sequence[RollingWindowSummary] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)



@dataclass(frozen=True)
class PatternInputBundle:
    """Complete set of inputs available to a pattern evaluation."""

    analysis_days: Sequence[CGMDay]
    validation_days: Sequence[CGMDay]
    analysis_summaries: Sequence[DailyCGMSummary]
    validation_summaries: Sequence[DailyCGMSummary]
    rolling_windows: Sequence[RollingWindowSummary] = field(default_factory=tuple)
    rolling_snapshot: Optional[RollingStatsSnapshot] = None
    excursion_summary: Optional[ExcursionTrendSummary] = None
    prepared_day_cache: dict[date, "PreparedDay"] = field(default_factory=dict, repr=False)
    time_window_cache: dict[tuple[date, float, float], "pd.DataFrame"] = field(
        default_factory=dict,
        repr=False,
    )

    def sufficient_analysis_days(self, minimum: int = 5) -> bool:
        return sum(day.coverage_ratio() >= 0.7 for day in self.analysis_days) >= minimum

    def prepared_day(self, day: CGMDay) -> "PreparedDay":
        """Return a cached PreparedDay for the provided CGMDay, computing lazily."""

        key = day.service_date
        if key in self.prepared_day_cache:
            return self.prepared_day_cache[key]

        from .rules.utils import prepare_day  # Local import to avoid circular dependency

        prepared: "PreparedDay" = prepare_day(day)
        self.prepared_day_cache[key] = prepared
        return prepared

    def time_window(self, day: CGMDay, start_hour: float, end_hour: float) -> "pd.DataFrame":
        """Return a cached local-time slice for the given day and hour bounds."""

        key = (day.service_date, float(start_hour), float(end_hour))
        cached = self.time_window_cache.get(key)
        if cached is not None:
            return cached

        from .rules.utils import filter_time_window  # Local import to avoid circular dependency

        prepared = self.prepared_day(day)
        window_df = filter_time_window(prepared, start_hour, end_hour)
        self.time_window_cache[key] = window_df
        return window_df


@dataclass(frozen=True)
class PatternContext:
    """Auxiliary context passed to each pattern."""

    patient_id: str
    analysis_date: date
    thresholds: Mapping[str, Any] = field(default_factory=dict)
    pattern_settings: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    extras: Mapping[str, Any] = field(default_factory=dict)

    def pattern_threshold(self, pattern_id: str, key: str, default: Any) -> Any:
        """Return pattern-specific override, falling back to global thresholds"""

        pattern_specific = self.pattern_settings.get(pattern_id, {})
        if key in pattern_specific:
            return pattern_specific[key]
        return self.thresholds.get(key, default)


@dataclass(frozen=True)
class ExcursionEvent:
    """Single excursion block from the excursion-trend API"""

    start_local: str
    end_local: str
    duration_minutes: float
    min_mg_dl: float
    max_mg_dl: float
    mean_mg_dl: Optional[float]
    direction: Optional[str]


@dataclass(frozen=True)
class ExcursionTrendSummary:
    """Excursion trend payload covering a recent measurement window."""

    patient_id: str
    start_date: date
    end_date: date
    template_coverage_days: Optional[int]
    lookback_days: Optional[int]
    excursions: Sequence[ExcursionEvent] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)
