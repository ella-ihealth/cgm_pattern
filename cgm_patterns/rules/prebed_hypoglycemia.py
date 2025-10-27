"""Detect pre-bedtime hypoglycemia exposures."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import total_minutes


@register_rule
class PrebedHypoglycemiaRule(PatternRule):
    id = "prebed_hypoglycemia"
    pattern_id = 5
    description = "BG <70 mg/dL between 20:00–24:00 on ≥2 evenings within a 7-day period"
    version = "1.0.0"
    metadata = PATTERN_METADATA[5]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        low_threshold = float(self.resolved_threshold(context, "prebed_low_threshold", 70.0))
        evenings_required = int(self.resolved_threshold(context, "prebed_evenings_required", 2))
        window_start = float(self.resolved_threshold(context, "prebed_window_start", 20.0))
        window_end = float(self.resolved_threshold(context, "prebed_window_end", 24.0))

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
            prebed = window.time_window(day, window_start, window_end)
            if prebed.empty:
                continue
            mask = prebed["glucose_mg_dL"] < low_threshold
            lows = prebed.loc[mask]
            if lows.empty:
                continue

            minutes_low = total_minutes(lows)
            lowest_value = float(lows["glucose_mg_dL"].min()) if not lows.empty else float("nan")
            date_key = day.service_date.isoformat()
            candidate = {
                "service_date": date_key,
                "minutes_low": minutes_low,
                "lowest_glucose": lowest_value,
            }
            previous = qualifying_by_date.get(date_key)
            if previous is None:
                qualifying_by_date[date_key] = candidate
            else:
                prev_minutes = float(previous.get("minutes_low", 0.0))
                if minutes_low > prev_minutes:
                    qualifying_by_date[date_key] = candidate

        qualifying = [qualifying_by_date[key] for key in sorted(qualifying_by_date.keys())]

        evenings_required = max(1, evenings_required)
        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count >= evenings_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "prebed_low_evenings": qualifying_count,
            "low_threshold": low_threshold,
            "window_start_hour": window_start,
            "window_end_hour": window_end,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(evenings_required, 3))],
            "required_evenings": evenings_required,
        }
        confidence = min(1.0, qualifying_count / max(1, evenings_required))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
