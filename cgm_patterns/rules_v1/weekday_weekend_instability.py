"""Detect instability between weekday and weekend glycemic control."""
from __future__ import annotations

import math
from statistics import mean

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import coefficient_of_variation, day_of_week, prepare_day


@register_rule
class WeekdayWeekendInstabilityRule(PatternRule):
    id = "weekday_weekend_instability"
    pattern_id = 24
    description = "Weekend TAR or CV exceeds weekday baseline by configured delta"
    version = "1.0.0"
    metadata = PATTERN_METADATA[24]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        minimum_days = int(self.resolved_threshold(context, "instability_minimum_days", 14))
        weekends_required = int(self.resolved_threshold(context, "instability_weekends_required", 2))
        tar_delta = float(self.resolved_threshold(context, "instability_tar_delta", 0.10))
        cv_delta = float(self.resolved_threshold(context, "instability_cv_delta", 0.10))

        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))

        required_validation_days = max(validation_window_days, minimum_days)
        validation_days, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            required_validation_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

        days = validation_days[-minimum_days:]

        weekday_tar: list[float] = []
        weekday_cv: list[float] = []
        weekend_records: list[tuple[str, float, float]] = []

        for day in days:
            prepared = prepare_day(day)
            dow = day_of_week(prepared)
            if dow is None:
                continue
            summary = next((s for s in window.validation_summaries if s.service_date == day.service_date), None)
            if summary is None:
                continue
            cv_value = coefficient_of_variation(prepared.frame["glucose_mg_dL"])
            if dow < 5:
                if summary.percent_high is not None:
                    weekday_tar.append(summary.percent_high)
                if cv_value is not None:
                    weekday_cv.append(cv_value)
            else:
                weekend_records.append((day.service_date.isoformat(), summary.percent_high or 0.0, cv_value or 0.0))

        if not weekend_records or len({date for date, _, _ in weekend_records}) < weekends_required:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={"eligible_weekends": len({date for date, _, _ in weekend_records})},
                metrics={},
                version=self.version,
            )

        weekday_tar_baseline = mean(weekday_tar) if weekday_tar else 0.0
        weekday_cv_baseline = mean(weekday_cv) if weekday_cv else 0.0

        exceeding_weekends = []
        for date_str, weekend_tar, weekend_cv in weekend_records:
            tar_excess = weekend_tar - weekday_tar_baseline
            cv_excess = weekend_cv - weekday_cv_baseline
            if tar_excess >= tar_delta or cv_excess >= cv_delta:
                exceeding_weekends.append(
                    {
                        "service_date": date_str,
                        "tar_excess": tar_excess,
                        "cv_excess": cv_excess,
                    }
                )

        status = PatternStatus.DETECTED if len(exceeding_weekends) >= weekends_required else PatternStatus.NOT_DETECTED
        metrics = {
            "weekday_tar_baseline": weekday_tar_baseline,
            "weekday_cv_baseline": weekday_cv_baseline,
            "weekends_exceeding": len(exceeding_weekends),
        }
        evidence = {
            "weekend_examples": exceeding_weekends[:weekends_required],
            "required_weekends": weekends_required,
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, len(exceeding_weekends) / max(1, weekends_required)),
            version=self.version,
        )
