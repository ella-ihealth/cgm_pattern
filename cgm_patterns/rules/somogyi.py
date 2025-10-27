"""Detect Somogyi effect (overnight lows with rebound morning highs)."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import total_minutes


@register_rule
class SomogyiEffectRule(PatternRule):
    id = "somogyi_effect"
    pattern_id = 12
    description = "Overnight BG <70 mg/dL ≥15 min with 03:00–08:00 rise and fasting >180 mg/dL on ≥2 of last 14 days"
    version = "1.2.0"
    metadata = PATTERN_METADATA[12]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 14))   # Work window: last 14 eligible days (each with ≥70 % CGM coverage; need at least 5 days to decide).
        low_window_start = float(self.resolved_threshold(context, "somogyi_low_window_start", 0.0))
        low_window_end = float(self.resolved_threshold(context, "somogyi_low_window_end", 3.0))
        low_threshold = float(self.resolved_threshold(context, "somogyi_low_threshold", 70.0))
        minimum_low_minutes = float(self.resolved_threshold(context, "somogyi_low_minutes", 15.0))
        morning_window_start = float(self.resolved_threshold(context, "somogyi_morning_window_start", 3.0))
        morning_window_end = float(self.resolved_threshold(context, "somogyi_morning_window_end", 8.0))
        rise_threshold = float(self.resolved_threshold(context, "somogyi_rise_threshold", 30.0))
        fpg_threshold = float(self.resolved_threshold(context, "somogyi_fpg_threshold", 180.0))
        qualifying_days_required = int(self.resolved_threshold(context, "somogyi_days_required", 2))

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
            overnight = window.time_window(day, low_window_start, low_window_end).reset_index(drop=True)
            if overnight.empty:
                continue

            low_mask = overnight["glucose_mg_dL"] < low_threshold
            if not low_mask.any():
                continue

            low_minutes = total_minutes(overnight.loc[low_mask])
            if low_minutes < minimum_low_minutes:
                continue

            lows = overnight.loc[low_mask]
            nadir_row = lows.loc[lows["glucose_mg_dL"].idxmin()]
            overnight_nadir = float(nadir_row["glucose_mg_dL"])

            morning = window.time_window(day, morning_window_start, morning_window_end).reset_index(drop=True)
            if morning.empty:
                continue

            fasting_row = morning.iloc[-1]
            fasting_value = float(fasting_row["glucose_mg_dL"])
            if fasting_value <= fpg_threshold:
                continue

            if fasting_row["local_time"] <= nadir_row["local_time"]:
                continue

            morning_peak = float(morning["glucose_mg_dL"].max())
            rise_amount = morning_peak - overnight_nadir
            if rise_amount < rise_threshold:
                continue

            qualifying.append(
                {
                    "service_date": day.service_date.isoformat(),
                    "overnight_low_time": nadir_row["local_time"].isoformat(),
                    "minutes_below_threshold": low_minutes,
                    "overnight_nadir": overnight_nadir,
                    "morning_peak": morning_peak,
                    "rise": rise_amount,
                    "fasting_time": fasting_row["local_time"].isoformat(),
                    "fasting_glucose": fasting_value,
                }
            )

        qualifying_days_required = max(1, qualifying_days_required)
        status = PatternStatus.DETECTED if len(qualifying) >= qualifying_days_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "somogyi_days": len(qualifying),
            "low_threshold": low_threshold,
            "minimum_low_minutes": minimum_low_minutes,
            "rise_threshold": rise_threshold,
            "fasting_threshold": fpg_threshold,
            "morning_window_start": morning_window_start,
            "morning_window_end": morning_window_end,
        }
        evidence = {
            "examples": qualifying[:qualifying_days_required],
            "required_days": qualifying_days_required,
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, len(qualifying) / max(1, qualifying_days_required)),
            version=self.version,
        )
