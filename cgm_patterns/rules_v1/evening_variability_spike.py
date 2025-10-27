"""Detect elevated evening variability between 18:00-22:00."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import coefficient_of_variation, filter_time_window, interquartile_range, prepare_day


@register_rule
class EveningVariabilitySpikeRule(PatternRule):
    id = "evening_variability_spike"
    pattern_id = 25
    description = "Evening IQR>40 mg/dL or CV>36% on ≥40% of last 7 days"
    version = "1.0.0"
    metadata = PATTERN_METADATA[25]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        fraction_required = float(self.resolved_threshold(context, "evening_days_fraction", 0.40))
        iqr_threshold = float(self.resolved_threshold(context, "evening_iqr_threshold", 40.0))
        cv_threshold = float(self.resolved_threshold(context, "evening_cv_threshold", 0.36))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))

        _, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            validation_window_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

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

        qualifying = []
        for day in eligible_days:
            prepared = prepare_day(day)
            evening = filter_time_window(prepared, 18.0, 22.0)
            if evening.empty:
                continue
            glucose = evening["glucose_mg_dL"]
            iqr = interquartile_range(glucose) or 0.0
            cv = coefficient_of_variation(glucose) or 0.0
            if iqr > iqr_threshold or cv > cv_threshold:
                qualifying.append(
                    {
                        "service_date": day.service_date.isoformat(),
                        "iqr": iqr,
                        "cv": cv,
                    }
                )

        required_occurrences = max(1, math.ceil(len(eligible_days) * fraction_required))
        status = PatternStatus.DETECTED if len(qualifying) >= required_occurrences else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "evening_variability_days": len(qualifying),
            "iqr_threshold": iqr_threshold,
            "cv_threshold": cv_threshold,
        }
        evidence = {
            "evening_examples": qualifying[:5],
            "required_days": required_occurrences,
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, len(qualifying) / max(1, required_occurrences)),
            version=self.version,
        )
