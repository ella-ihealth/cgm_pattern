"""Detect nocturnal hypoglycemia burden during typical sleep hours."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import total_minutes


@register_rule
class NocturnalHypoglycemiaSevereRule(PatternRule):
    id = "nocturnal_hypoglycemia_severe"
    pattern_id = 5
    description = "BG <54 mg/dL between 00:00–06:00 at least once within a 7-day period"
    version = "1.3.0"
    metadata = PATTERN_METADATA[5]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        critical_threshold = float(self.resolved_threshold(context, "overnight_critical_threshold", 54.0))
        nights_required = int(self.resolved_threshold(context, "overnight_nights_required", 1))
        sleep_window_start = float(self.resolved_threshold(context, "overnight_sleep_window_start", 0.0))
        sleep_window_end = float(self.resolved_threshold(context, "overnight_sleep_window_end", 6.0))

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
            overnight = window.time_window(day, sleep_window_start, sleep_window_end)
            if overnight.empty:
                continue
            critical_mask = overnight["glucose_mg_dL"] < critical_threshold
            if not critical_mask.any():
                continue
            minutes_critical = total_minutes(overnight.loc[critical_mask])
            date_key = day.service_date.isoformat()
            candidate = {
                "service_date": date_key,
                "minutes_critical": minutes_critical,
            }
            previous = qualifying_by_date.get(date_key)
            if previous is None:
                qualifying_by_date[date_key] = candidate
            else:
                prev_minutes_critical = float(previous.get("minutes_critical", 0.0))
                if minutes_critical > prev_minutes_critical:
                    qualifying_by_date[date_key] = candidate

        qualifying = [qualifying_by_date[key] for key in sorted(qualifying_by_date.keys())]

        nights_required = max(1, nights_required)
        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count >= nights_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "overnight_low_days": qualifying_count,
            "critical_threshold": critical_threshold,
            "sleep_window_start": sleep_window_start,
            "sleep_window_end": sleep_window_end,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(nights_required, 3))],
            "required_nights": nights_required,
        }
        confidence = min(1.0, qualifying_count / max(1, nights_required))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
