"""Detect overnight hypoglycemia burden."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import filter_time_window, prepare_day, total_minutes


@register_rule
class OvernightHypoglycemiaRule(PatternRule):
    id = "overnight_hypoglycemia"
    pattern_id = 5
    description = ">=15 minutes <70 mg/dL between 00:00-06:00 on ≥40% of last 7 days"
    version = "1.0.0"
    metadata = PATTERN_METADATA[5]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        low_threshold = float(self.resolved_threshold(context, "overnight_low_threshold", 70.0))
        minimum_minutes = float(self.resolved_threshold(context, "overnight_low_minutes", 15.0))
        fraction_required = float(self.resolved_threshold(context, "overnight_days_fraction", 0.40))

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
            overnight = filter_time_window(prepared, 0.0, 6.0)
            if overnight.empty:
                continue
            mask = overnight["glucose_mg_dL"] < low_threshold
            minutes = total_minutes(overnight.loc[mask])
            if minutes >= minimum_minutes:
                qualifying.append(
                    {
                        "service_date": day.service_date.isoformat(),
                        "minutes_low": minutes,
                    }
                )

        required_occurrences = max(1, math.ceil(len(eligible_days) * fraction_required))
        status = PatternStatus.DETECTED if len(qualifying) >= required_occurrences else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "overnight_low_days": len(qualifying),
            "low_minutes_threshold": minimum_minutes,
        }
        evidence = {
            "examples": qualifying[:5],
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
