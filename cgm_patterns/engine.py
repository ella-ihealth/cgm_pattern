"""Sliding-window engine for incremental CGM pattern detection."""
from __future__ import annotations

from collections import deque
from datetime import date
from typing import Callable, Iterable, Protocol, Sequence

from .cache import DailySummaryCache
from .features import compute_daily_summary
from .models import (
    CGMDay,
    DailyCGMSummary,
    ExcursionTrendSummary,
    PatternContext,
    PatternDetection,
    PatternInputBundle,
    RollingStatsSnapshot,
    RollingWindowSummary,
)
from .registry import RuleRegistry
from .rule_base import PatternRule


class DailyCGMSource(Protocol):
    """Protocol for providing chronologically ordered daily CGM data."""

    def iter_days(self, patient_id: str) -> Iterable[CGMDay]:
        ...


_GLOBAL_SUMMARY_CACHE = DailySummaryCache()


class SlidingWindowEngine:
    """Maintains overlapping windows and executes registered rules."""

    def __init__(
        self,
        data_source: DailyCGMSource,
        registry: RuleRegistry,
        *,
        summary_cache: DailySummaryCache | None = None,
        analysis_days: int = 7,
        validation_days: int = 14,
        default_thresholds: dict[str, float] | None = None,
        default_pattern_settings: dict[str, dict[str, float]] | None = None,
        context_builder: Callable[[str, date], PatternContext] | None = None,
        rolling_fetcher: Callable[[str, date], RollingStatsSnapshot | Sequence[RollingWindowSummary] | None] | None = None,
        excursion_fetcher: Callable[[str, date], ExcursionTrendSummary | None] | None = None,
    ) -> None:
        if validation_days < analysis_days:
            raise ValueError("validation_days must be >= analysis_days")
        self._source = data_source
        self._registry = registry
        self._summary_cache = summary_cache or _GLOBAL_SUMMARY_CACHE
        self._analysis_days = analysis_days
        self._validation_days = validation_days
        self._default_thresholds = default_thresholds or {}
        self._default_pattern_settings = default_pattern_settings or {}
        self._context_builder = context_builder
        self._rolling_fetcher = rolling_fetcher
        self._excursion_fetcher = excursion_fetcher

    def run_patient(
        self,
        patient_id: str,
        *,
        rule_filter: Callable[[PatternRule], bool] | None = None,
    ) -> dict[date, list[PatternDetection]]:
        """Process a single patient, returning detections by date."""

        raw_window: deque[CGMDay] = deque(maxlen=self._validation_days)
        summary_window: deque[DailyCGMSummary] = deque(maxlen=self._validation_days)
        results: dict[date, list[PatternDetection]] = {}

        for day in self._source.iter_days(patient_id):
            raw_window.append(day)
            summary = self._ensure_summary(day)
            summary_window.append(summary)

            keep_dates = {d.service_date.isoformat() for d in raw_window}
            self._summary_cache.prune(patient_id, keep_dates)

            window = self._build_input_bundle(patient_id, day.service_date, raw_window, summary_window)
            context = self._build_context(patient_id, day.service_date)

            detections = self._registry.detect_all(window, context, predicate=rule_filter)
            results[day.service_date] = detections

        return results

    def _ensure_summary(self, day: CGMDay) -> DailyCGMSummary:
        cached = self._summary_cache.get(day.patient_id, day.service_date.isoformat())
        if cached is not None:
            return cached
        summary = compute_daily_summary(day)
        self._summary_cache.set(summary)
        return summary

    def _build_input_bundle(
        self,
        patient_id: str,
        analysis_date: date,
        raw_window: deque[CGMDay],
        summary_window: deque[DailyCGMSummary],
    ) -> PatternInputBundle:
        analysis_raw: Sequence[CGMDay] = list(raw_window)[-self._analysis_days :]
        analysis_summary: Sequence[DailyCGMSummary] = list(summary_window)[-self._analysis_days :]
        validation_raw: Sequence[CGMDay] = list(raw_window)
        validation_summary: Sequence[DailyCGMSummary] = list(summary_window)
        rolling_windows: Sequence[RollingWindowSummary] = ()
        rolling_snapshot: RollingStatsSnapshot | None = None
        if self._rolling_fetcher is not None:
            result = self._rolling_fetcher(patient_id, analysis_date)
            if isinstance(result, RollingStatsSnapshot):
                rolling_snapshot = result
                rolling_windows = result.windows
            elif result is not None:
                rolling_windows = tuple(result)

        excursion_summary: ExcursionTrendSummary | None = None
        if self._excursion_fetcher is not None:
            excursion_summary = self._excursion_fetcher(patient_id, analysis_date)

        return PatternInputBundle(
            analysis_days=analysis_raw,
            validation_days=validation_raw,
            analysis_summaries=analysis_summary,
            validation_summaries=validation_summary,
            rolling_windows=rolling_windows,
            rolling_snapshot=rolling_snapshot,
            excursion_summary=excursion_summary,
        )

    def _build_context(self, patient_id: str, analysis_date: date) -> PatternContext:
        if self._context_builder is not None:
            return self._context_builder(patient_id, analysis_date)
        return PatternContext(
            patient_id=patient_id,
            analysis_date=analysis_date,
            thresholds=self._default_thresholds,
            pattern_settings=self._default_pattern_settings,
        )
