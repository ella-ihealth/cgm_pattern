"""Detect days with high glycemic variability."""
from __future__ import annotations

from typing import Any

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import coefficient_of_variation


@register_rule
class HighGlycemicVariabilityRule(PatternRule):
    id = "high_glycemic_variability"
    pattern_id = 3
    description = "CV >=30% in any one day within the last 7 days"
    version = "1.0.0"
    metadata = PATTERN_METADATA[3]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        cv_threshold = float(self.resolved_threshold(context, "high_variability_cv_threshold", 0.36))
        detection_days_required = int(self.resolved_threshold(context, "high_variability_days_required", 1))
        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))

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

        qualifying: list[dict[str, Any]] = []
        for day in eligible_days:
            prepared = window.prepared_day(day)
            frame = prepared.frame
            if frame.empty:
                continue

            glucose = frame["glucose_mg_dL"].dropna()
            if glucose.empty:
                continue

            cv_value = coefficient_of_variation(glucose)
            if cv_value is None or cv_value < cv_threshold:
                continue

            record: dict[str, Any] = {
                "service_date": day.service_date.isoformat(),
                "coefficient_of_variation": cv_value,
                "mean_glucose": float(glucose.mean()),
                "std_glucose": float(glucose.std(ddof=0)),
                "coverage_ratio": float(day.coverage_ratio()),
            }
            qualifying.append(record)

        qualifying.sort(key=lambda entry: entry["service_date"])
        qualifying_count = len(qualifying)
        required_detection_days = max(1, detection_days_required)
        status = PatternStatus.DETECTED if qualifying_count >= required_detection_days else PatternStatus.NOT_DETECTED

        metrics = {
            "analysis_days_considered": len(eligible_days),
            "high_variability_days": qualifying_count,
            "cv_threshold": cv_threshold,
            "required_detection_days": required_detection_days,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(required_detection_days, 3))],
        }
        confidence = min(1.0, qualifying_count / required_detection_days) if required_detection_days else 0.0

        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
