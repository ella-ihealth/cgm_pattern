"""Detect elevated glycemic variability."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class HighGlycemicVariabilityRule(PatternRule):
    id = "high_glycemic_variability"
    pattern_id = 3
    description = "Median coefficient of variation ≥36% across last 7 days"
    version = "1.0.0"
    metadata = PATTERN_METADATA[3]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        cv_threshold = float(self.resolved_threshold(context, "cv_threshold", 0.36))
        days_needed = int(self.resolved_threshold(context, "cv_days_required", 3))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))

        _, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            validation_window_days,
        )
        if insufficient_validation:
            return insufficient_validation

        eligible = [s for s in window.analysis_summaries if s.coverage_ratio >= coverage_threshold and s.mean_glucose > 0]
        eligible = eligible[-analysis_window_days:]
        if len(eligible) < required_days:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "eligible_days": len(eligible),
                    "required_analysis_days": required_days,
                },
                metrics={},
                version=self.version,
            )

        cv_values = [s.std_glucose / s.mean_glucose for s in eligible if s.mean_glucose]
        cv_values = [v for v in cv_values if not math.isnan(v) and not math.isinf(v)]
        if not cv_values:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "eligible_days": len(eligible),
                    "required_analysis_days": required_days,
                },
                metrics={"cv_values": []},
                version=self.version,
            )

        sorted_cv = sorted(cv_values)
        mid = len(sorted_cv) // 2
        if len(sorted_cv) % 2:
            median_cv = sorted_cv[mid]
        else:
            median_cv = (sorted_cv[mid - 1] + sorted_cv[mid]) / 2

        elevated_days = sum(cv >= cv_threshold for cv in cv_values)
        status = PatternStatus.DETECTED if elevated_days >= days_needed and median_cv >= cv_threshold else PatternStatus.NOT_DETECTED

        metrics = {
            "analysis_days_considered": len(eligible),
            "median_cv": median_cv,
            "elevated_cv_days": elevated_days,
            "cv_threshold": cv_threshold,
        }
        evidence = {
            "cv_values": [
                {
                    "service_date": s.service_date.isoformat(),
                    "cv": s.std_glucose / s.mean_glucose,
                }
                for s in eligible
            ][:7],
            "required_days": days_needed,
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, elevated_days / max(1, days_needed)),
            version=self.version,
        )
