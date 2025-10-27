"""Detect recurring afternoon hypoglycemia exposures."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import total_minutes


@register_rule
class AfternoonHypoglycemiaRule(PatternRule):
    id = "afternoon_hypoglycemia"
    pattern_id = 5
    description = "BG <70 mg/dL during the afternoon period 14:00–17:00 lasting ≥15 minutes on ≥2 separate afternoons within a 7-day period."
    version = "1.0.0"
    metadata = PATTERN_METADATA[5]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        low_threshold = float(self.resolved_threshold(context, "afternoon_low_threshold", 70.0))
        minimum_minutes = float(self.resolved_threshold(context, "afternoon_low_minutes", 15.0))
        afternoons_required = int(self.resolved_threshold(context, "afternoon_low_days_required", 2))
        window_start = float(self.resolved_threshold(context, "afternoon_window_start", 12.0))
        window_end = float(self.resolved_threshold(context, "afternoon_window_end", 17.0))

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
            afternoon = window.time_window(day, window_start, window_end)
            if afternoon.empty:
                continue

            low_mask = afternoon["glucose_mg_dL"] < low_threshold
            minutes_low = total_minutes(afternoon.loc[low_mask])
            if minutes_low < minimum_minutes:
                continue

            date_key = day.service_date.isoformat()
            candidate = {
                "service_date": date_key,
                "minutes_low": minutes_low,
                "lowest_glucose": float(afternoon.loc[low_mask, "glucose_mg_dL"].min()),
            }
            previous = qualifying_by_date.get(date_key)
            if previous is None:
                qualifying_by_date[date_key] = candidate
            else:
                prev_minutes = float(previous.get("minutes_low", 0.0))
                if minutes_low > prev_minutes:
                    qualifying_by_date[date_key] = candidate

        qualifying = [qualifying_by_date[key] for key in sorted(qualifying_by_date.keys())]

        afternoons_required = max(1, afternoons_required)
        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count >= afternoons_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "afternoon_low_days": qualifying_count,
            "low_threshold": low_threshold,
            "minimum_minutes": minimum_minutes,
            "window_start_hour": window_start,
            "window_end_hour": window_end,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(afternoons_required, 3))],
            "required_afternoons": afternoons_required,
        }
        confidence = min(1.0, qualifying_count / max(1, afternoons_required))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
