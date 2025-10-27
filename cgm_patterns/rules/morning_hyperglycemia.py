"""Detect recurrent morning hyperglycemia exposures."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import total_minutes


@register_rule
class MorningHyperglycemiaRule(PatternRule):
    id = "morning_hyperglycemia"
    pattern_id = 37
    description = "BG >130 mg/dL between 04:00–08:00 on ≥3 mornings within a 7-day period"
    version = "1.1.0"
    metadata = PATTERN_METADATA[37]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        high_threshold = float(self.resolved_threshold(context, "morning_low_threshold", 130.0))
        mornings_required = int(self.resolved_threshold(context, "morning_low_days_required", 3))
        window_start = float(self.resolved_threshold(context, "morning_low_window_start", 4.0))
        window_end = float(self.resolved_threshold(context, "morning_low_window_end", 8.0))

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

        qualifying_by_date: dict[str, dict[str, float | str]] = {}
        for day in eligible_days:
            morning = window.time_window(day, window_start, window_end)
            if morning.empty:
                continue

            high_mask = morning["glucose_mg_dL"] > high_threshold
            minutes_high = total_minutes(morning.loc[high_mask])

            if minutes_high <= 0:
                continue

            date_key = day.service_date.isoformat()
            candidate = {
                "service_date": date_key,
                "minutes_high": minutes_high,
            }
            previous = qualifying_by_date.get(date_key)
            if previous is None:
                qualifying_by_date[date_key] = candidate
            else:
                prev_minutes = float(previous.get("minutes_high", 0.0))
                if minutes_high > prev_minutes:
                    qualifying_by_date[date_key] = candidate

        qualifying = [qualifying_by_date[key] for key in sorted(qualifying_by_date.keys())]

        mornings_required = max(1, mornings_required)
        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count >= mornings_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "morning_high_days": qualifying_count,
            "high_threshold": high_threshold,
            "window_start_hour": window_start,
            "window_end_hour": window_end,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(mornings_required, 3))],
            "required_mornings": mornings_required,
        }
        confidence = min(1.0, qualifying_count / max(1, mornings_required))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
