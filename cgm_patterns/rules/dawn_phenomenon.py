"""Detect dawn phenomenon driven by a nocturnal rise before breakfast."""
from __future__ import annotations

from typing import Any

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class DawnPhenomenonRule(PatternRule):
    id = "dawn_phenomenon"
    pattern_id = 14
    description = (
        "Stable 00:00-03:00 baseline without lows followed by ≥20 mg/dL rise between 03:00-08:00"
    )
    version = "1.0.0"
    metadata = PATTERN_METADATA[14]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 3))

        detection_days_required = int(self.resolved_threshold(context, "dawn_days_required", 3))
        rise_threshold = float(self.resolved_threshold(context, "dawn_rise_threshold", 20.0))
        overnight_low_threshold = float(
            self.resolved_threshold(context, "dawn_overnight_low_threshold", 70.0)
        )
        baseline_range_threshold = float(
            self.resolved_threshold(context, "dawn_baseline_range_threshold", 20.0)
        )

        baseline_start = float(self.resolved_threshold(context, "dawn_baseline_start_hour", 0.0))
        baseline_end = float(self.resolved_threshold(context, "dawn_baseline_end_hour", 3.0))
        rise_window_start = float(self.resolved_threshold(context, "dawn_rise_window_start_hour", 3.0))
        rise_window_end = float(self.resolved_threshold(context, "dawn_rise_window_end_hour", 8.0))

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
            overnight = window.time_window(day, baseline_start, rise_window_end)
            if overnight.empty:
                continue
            overnight_glucose = overnight["glucose_mg_dL"].dropna()
            if overnight_glucose.empty:
                continue
            if (overnight_glucose < overnight_low_threshold).any():
                continue

            baseline_window = window.time_window(day, baseline_start, baseline_end)
            rise_window = window.time_window(day, rise_window_start, rise_window_end)
            baseline_glucose = baseline_window["glucose_mg_dL"].dropna()
            rise_glucose = rise_window["glucose_mg_dL"].dropna()
            if baseline_glucose.empty or rise_glucose.empty:
                continue

            baseline_range = float(baseline_glucose.max() - baseline_glucose.min())
            if baseline_range_threshold > 0 and baseline_range > baseline_range_threshold:
                continue

            baseline_idx = baseline_glucose.idxmin()
            baseline_row = baseline_window.loc[baseline_idx]
            baseline_value = float(baseline_row["glucose_mg_dL"])

            peak_idx = rise_window["glucose_mg_dL"].idxmax()
            peak_row = rise_window.loc[peak_idx]
            peak_value = float(peak_row["glucose_mg_dL"])

            rise_value = peak_value - baseline_value
            if rise_value < rise_threshold:
                continue

            qualifying.append(
                {
                    "service_date": day.service_date.isoformat(),
                    "baseline_time": baseline_row["local_time"].isoformat(),
                    "baseline_glucose": baseline_value,
                    "peak_time": peak_row["local_time"].isoformat(),
                    "peak_glucose": peak_value,
                    "rise_mg_dl": rise_value,
                    "baseline_range": baseline_range,
                    "overnight_min": float(overnight_glucose.min()),
                }
            )

        qualifying.sort(key=lambda entry: entry["service_date"])
        qualifying_count = len(qualifying)
        required_detection_days = max(1, detection_days_required)
        status = (
            PatternStatus.DETECTED
            if qualifying_count >= required_detection_days
            else PatternStatus.NOT_DETECTED
        )

        metrics = {
            "analysis_days_considered": len(eligible_days),
            "qualifying_mornings": qualifying_count,
            "required_detection_days": required_detection_days,
            "rise_threshold": rise_threshold,
            "baseline_range_threshold": baseline_range_threshold,
        }
        evidence = {
            "examples": qualifying[: min(qualifying_count, max(required_detection_days, 3))],
        }
        confidence = min(1.0, qualifying_count / required_detection_days)

        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
