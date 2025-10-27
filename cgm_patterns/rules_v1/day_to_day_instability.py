"""Detect sporadic high within-day variability when overall CV is controlled."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class DayToDayInstabilityRule(PatternRule):
    id = "day_to_day_instability"
    pattern_id = 32
    description = "CV >36% on ≥2 days while 7-day mean CV <36%"
    version = "1.0.0"
    metadata = PATTERN_METADATA[32]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 7))
        cv_threshold = float(self.resolved_threshold(context, "instability_cv_threshold", 0.36))
        recurrence_required = max(2, int(self.resolved_threshold(context, "instability_recurrence_required", 2)))

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

        cvs = [s.std_glucose / s.mean_glucose for s in eligible if s.mean_glucose]
        cvs = [cv for cv in cvs if not math.isnan(cv)]
        if not cvs:
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

        mean_cv = sum(cvs) / len(cvs)
        high_days = [
            {
                "service_date": summary.service_date.isoformat(),
                "cv": summary.std_glucose / summary.mean_glucose,
            }
            for summary in eligible
            if summary.mean_glucose and (summary.std_glucose / summary.mean_glucose) > cv_threshold
        ]

        status = PatternStatus.DETECTED if mean_cv < cv_threshold and len(high_days) >= recurrence_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible),
            "mean_cv": mean_cv,
            "high_cv_days": len(high_days),
            "cv_threshold": cv_threshold,
        }
        evidence = {
            "high_cv_examples": high_days[:recurrence_required],
            "required_days": recurrence_required,
        }
        confidence = min(1.0, len(high_days) / max(1, recurrence_required)) if mean_cv < cv_threshold else 0.0
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
