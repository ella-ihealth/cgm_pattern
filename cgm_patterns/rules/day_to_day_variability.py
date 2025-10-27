"""Detect elevated weekend variability relative to weekdays."""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import coefficient_of_variation, day_of_week


def _glucose_metrics(frame: pd.DataFrame) -> dict[str, float] | None:
    glucose = frame["glucose_mg_dL"].dropna()
    if glucose.empty:
        return None
    cv_value = coefficient_of_variation(glucose)
    if cv_value is None:
        return None
    gly_range = float(glucose.max() - glucose.min())
    mean = float(glucose.mean())
    std = float(glucose.std(ddof=0)) if not math.isnan(glucose.std(ddof=0)) else 0.0
    return {
        "cv": cv_value,
        "range": gly_range,
        "mean": mean,
        "std": std,
    }


@register_rule
class DayToDayVariabilityRule(PatternRule):
    id = "day_to_day_variability"
    pattern_id = 24
    description = (
        "Weekend glucose variability exceeds weekday levels (range/CV) on ≥2 weekends within 30 days"
    )
    version = "1.0.0"
    metadata = PATTERN_METADATA[24]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 10))
        weekends_required = int(self.resolved_threshold(context, "weekend_variability_required", 2))

        cv_ratio_threshold = float(self.resolved_threshold(context, "weekend_cv_ratio_threshold", 1.15))
        range_ratio_threshold = float(self.resolved_threshold(context, "weekend_range_ratio_threshold", 1.25))
        absolute_cv_threshold = float(self.resolved_threshold(context, "weekend_absolute_cv_threshold", 0.30))
        absolute_range_threshold = float(self.resolved_threshold(context, "weekend_absolute_range_threshold", 60.0))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 30))
        baseline_weekdays_required = int(self.resolved_threshold(context, "baseline_weekdays_required", 5))

        eligible_days = [day for day in window.analysis_days if day.coverage_ratio() >= coverage_threshold]
        eligible_days = eligible_days[-analysis_window_days:]
        if len(eligible_days) < required_days:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "eligible_days": len(eligible_days),
                    "required_analysis_days": required_days,
                },
                metrics={},
                version=self.version,
            )

        weekday_metrics: list[dict[str, float]] = []
        weekend_frames: dict[tuple[int, int], list[pd.DataFrame]] = defaultdict(list)
        weekend_dates: dict[tuple[int, int], list[str]] = defaultdict(list)

        for day in eligible_days:
            prepared = window.prepared_day(day)
            dow = day_of_week(prepared)
            key = day.service_date.isoformat()
            frame = prepared.frame.dropna(subset=["glucose_mg_dL"]).reset_index(drop=True)
            if frame.empty:
                continue

            if dow is None:
                continue

            if dow <= 4:
                metrics = _glucose_metrics(frame)
                if metrics is not None:
                    weekday_metrics.append({"service_date": key, **metrics})
            elif dow >= 5:
                iso_year, iso_week, _ = day.service_date.isocalendar()
                weekend_frames[(iso_year, iso_week)].append(frame)
                weekend_dates[(iso_year, iso_week)].append(key)

        if len(weekday_metrics) < baseline_weekdays_required:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "weekday_days": len(weekday_metrics),
                    "required_weekday_baseline_days": baseline_weekdays_required,
                },
                metrics={},
                version=self.version,
            )

        weekday_cv_values = [m["cv"] for m in weekday_metrics]
        weekday_range_values = [m["range"] for m in weekday_metrics]
        weekday_cv_avg = float(sum(weekday_cv_values) / len(weekday_cv_values)) if weekday_cv_values else 0.0
        weekday_range_avg = float(sum(weekday_range_values) / len(weekday_range_values)) if weekday_range_values else 0.0

        qualifying_weekends: list[dict[str, Any]] = []
        for weekend_id, frames in weekend_frames.items():
            combined = pd.concat(frames, ignore_index=True)
            metrics = _glucose_metrics(combined)
            if metrics is None:
                continue

            weekend_cv = metrics["cv"]
            weekend_range = metrics["range"]

            cv_ratio_ok = (
                (weekday_cv_avg > 0 and weekend_cv >= weekday_cv_avg * cv_ratio_threshold)
                or weekend_cv >= absolute_cv_threshold
            )
            range_ratio_ok = (
                (weekday_range_avg > 0 and weekend_range >= weekday_range_avg * range_ratio_threshold)
                or weekend_range >= absolute_range_threshold
            )

            if not (cv_ratio_ok or range_ratio_ok):
                continue

            weekend_dates_list = sorted(weekend_dates[weekend_id])
            qualifying_weekends.append(
                {
                    "week_identifier": f"{weekend_id[0]}-W{weekend_id[1]:02d}",
                    "dates": weekend_dates_list,
                    "weekend_cv": weekend_cv,
                    "weekend_range": weekend_range,
                    "weekday_cv_avg": weekday_cv_avg,
                    "weekday_range_avg": weekday_range_avg,
                }
            )

        qualifying_count = len(qualifying_weekends)
        weekends_required = max(1, weekends_required)
        status = PatternStatus.DETECTED if qualifying_count >= weekends_required else PatternStatus.NOT_DETECTED

        metrics = {
            "analysis_days_considered": len(eligible_days),
            "weekday_baseline_days": len(weekday_metrics),
            "qualifying_weekends": qualifying_count,
            "weekends_required": weekends_required,
            "weekday_cv_avg": weekday_cv_avg,
            "weekday_range_avg": weekday_range_avg,
            "cv_ratio_threshold": cv_ratio_threshold,
            "range_ratio_threshold": range_ratio_threshold,
            "absolute_cv_threshold": absolute_cv_threshold,
            "absolute_range_threshold": absolute_range_threshold,
        }
        evidence = {
            "examples": qualifying_weekends[: min(len(qualifying_weekends), max(weekends_required, 3))],
        }
        confidence = min(1.0, qualifying_count / weekends_required)

        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
