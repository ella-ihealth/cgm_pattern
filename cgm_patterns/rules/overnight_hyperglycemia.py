"""Detect overnight hyperglycemia burden."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import total_minutes


@register_rule
class OvernightHyperglycemiaRule(PatternRule):
    id = "overnight_hyperglycemia"
    pattern_id = 5
    description = "BG >180 mg/dL for >50% of 22:00–06:00 on ≥3 nights within a 7-day window"
    version = "1.2.0"
    metadata = PATTERN_METADATA[5]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        high_threshold = float(self.resolved_threshold(context, "overnight_high_threshold", 180.0))
        percentage_threshold = float(self.resolved_threshold(context, "overnight_high_percentage", 0.5))
        nights_required = int(self.resolved_threshold(context, "overnight_high_nights_required", 3))
        window_start = float(self.resolved_threshold(context, "overnight_high_window_start", 22.0))
        window_end = float(self.resolved_threshold(context, "overnight_high_window_end", 6.0))

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

        qualifying = []
        for day in eligible_days:
            overnight = window.time_window(day, window_start, window_end)
            if overnight.empty:
                continue

            total_window_minutes = total_minutes(overnight)
            if total_window_minutes <= 0:
                continue

            high_mask = overnight["glucose_mg_dL"] > high_threshold
            high_minutes = total_minutes(overnight.loc[high_mask])
            if high_minutes / total_window_minutes > percentage_threshold:
                qualifying.append(
                    {
                        "service_date": day.service_date.isoformat(),
                        "percent_high": high_minutes / total_window_minutes,
                        "high_minutes": high_minutes,
                        "window_minutes": total_window_minutes,
                    }
                )

        nights_required = max(1, nights_required)
        status = PatternStatus.DETECTED if len(qualifying) >= nights_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "overnight_high_nights": len(qualifying),
            "percent_threshold": percentage_threshold,
            "high_threshold": high_threshold,
            "nights_required": nights_required,
            "window_start_hour": window_start,
            "window_end_hour": window_end,
        }
        evidence = {
            "examples": qualifying[:nights_required],
            "required_nights": nights_required,
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, len(qualifying) / max(1, nights_required)),
            version=self.version,
        )
