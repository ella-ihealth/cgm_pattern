"""Detect dawn phenomenon (overnight-to-morning rise)."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import filter_time_window, prepare_day


@register_rule
class DawnPhenomenonRule(PatternRule):
    id = "dawn_phenomenon"
    pattern_id = 14
    description = "BG rise ≥30 mg/dL from 00:00–06:00 nadir to 03:00–08:00 peak without intervening hypoglycemia"
    version = "1.2.0"
    metadata = PATTERN_METADATA[14]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        rise_threshold = float(self.resolved_threshold(context, "dawn_rise_threshold", 30.0))
        overnight_start = float(self.resolved_threshold(context, "dawn_overnight_start", 0.0))
        overnight_end = float(self.resolved_threshold(context, "dawn_overnight_end", 6.0))
        morning_start = float(self.resolved_threshold(context, "dawn_morning_start", 3.0))
        morning_end = float(self.resolved_threshold(context, "dawn_morning_end", 8.0))

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
            prepared = prepare_day(day)
            overnight = filter_time_window(prepared, overnight_start, overnight_end)
            morning = filter_time_window(prepared, morning_start, morning_end)
            if overnight.empty or morning.empty:
                continue
            overnight_glucose = overnight["glucose_mg_dL"].dropna()
            morning_glucose = morning["glucose_mg_dL"].dropna()
            if overnight_glucose.empty or morning_glucose.empty:
                continue

            nadir_idx = overnight_glucose.idxmin()
            peak_idx = morning_glucose.idxmax()
            nadir_row = overnight.loc[nadir_idx]
            peak_row = morning.loc[peak_idx]

            nadir_time = nadir_row["local_time"]
            peak_time = peak_row["local_time"]
            if peak_time <= nadir_time:
                continue

            overnight_nadir = float(nadir_row["glucose_mg_dL"])
            morning_peak = float(peak_row["glucose_mg_dL"])
            rise_amount = morning_peak - overnight_nadir

            combined = prepared.frame.set_index("local_time")
            between_window = combined.loc[(combined.index >= nadir_time) & (combined.index <= peak_time)]
            if not between_window.empty and (between_window["glucose_mg_dL"] < 70.0).any():
                continue

            if rise_amount >= rise_threshold:
                qualifying.append(
                    {
                        "service_date": day.service_date.isoformat(),
                        "overnight_nadir": overnight_nadir,
                        "nadir_time": nadir_time.isoformat(),
                        "morning_peak": morning_peak,
                        "peak_time": peak_time.isoformat(),
                        "rise": rise_amount,
                    }
                )

        required_occurrences = max(1, math.ceil(len(eligible_days) * 0.40))
        status = PatternStatus.DETECTED if len(qualifying) >= required_occurrences else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "dawn_rise_days": len(qualifying),
            "rise_threshold": rise_threshold,
            "overnight_window_start": overnight_start,
            "overnight_window_end": overnight_end,
            "morning_window_start": morning_start,
            "morning_window_end": morning_end,
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
