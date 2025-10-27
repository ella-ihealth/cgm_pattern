"""Detect days with dual-peak glycemic excursions."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule

def _find_extrema(smoothed: np.ndarray) -> tuple[list[int], list[int]]:
    """Return indices of local maxima and minima in the smoothed series."""

    if smoothed.size < 3:
        return [], []

    diff = np.diff(smoothed)
    sign = np.sign(diff)
    maxima: list[int] = []
    minima: list[int] = []
    for i in range(len(sign) - 1):
        left, right = sign[i], sign[i + 1]
        if left > 0 and right <= 0:
            maxima.append(i + 1)
        elif left < 0 and right >= 0:
            minima.append(i + 1)
    return maxima, minima


@register_rule
class DualPeakRule(PatternRule):
    id = "dual_peak"
    pattern_id = 25
    description = (
        "Distinct two-phase rise: first peak >180 mg/dL, partial decline, and second rise ≥30 mg/dL "
        "above nadir within 4 hours on ≥2 of last 7 days"
    )
    version = "1.0.0"
    metadata = PATTERN_METADATA[25]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        events_required = int(self.resolved_threshold(context, "dual_peak_days_required", 2))

        first_peak_threshold = float(self.resolved_threshold(context, "dual_peak_first_peak_threshold", 180.0))
        secondary_rise_threshold = float(self.resolved_threshold(context, "dual_peak_secondary_rise", 30.0))
        drop_threshold = float(self.resolved_threshold(context, "dual_peak_drop_threshold", 20.0))
        max_hours_between_peaks = float(self.resolved_threshold(context, "dual_peak_hours_between", 4.0))
        smoothing_window = int(self.resolved_threshold(context, "dual_peak_smoothing_window", 11))

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
        max_minutes_between_peaks = max_hours_between_peaks * 60.0

        for day in eligible_days:
            prepared = window.prepared_day(day)
            frame = prepared.frame.dropna(subset=["glucose_mg_dL", "minutes", "local_time"]).reset_index(drop=True)
            if frame.empty:
                continue

            values = frame["glucose_mg_dL"].to_numpy()
            minutes = frame["minutes"].to_numpy()
            times = frame["local_time"].to_numpy()
            if values.size < 3:
                continue

            window_size = smoothing_window if smoothing_window % 2 == 1 else smoothing_window + 1
            window_size = min(window_size, values.size if values.size % 2 == 1 else values.size - 1)
            if window_size < 3:
                window_size = 3
            smoothed = (
                pd.Series(values)
                .rolling(window=window_size, center=True, min_periods=1)
                .mean()
                .to_numpy()
            )

            minute_offsets = np.concatenate(([0.0], np.cumsum(minutes[:-1])))
            if len(np.unique(minute_offsets)) < len(minute_offsets):
                continue

            peaks, troughs = _find_extrema(smoothed)
            if not peaks or not troughs:
                continue

            event_candidate: dict[str, float | str] | None = None
            best_secondary_rise = 0.0

            for peak_idx in peaks:
                first_peak_value = float(smoothed[peak_idx])
                if first_peak_value <= first_peak_threshold:
                    continue

                subsequent_troughs = [t for t in troughs if t > peak_idx]
                if not subsequent_troughs:
                    continue

                for trough_idx in subsequent_troughs:
                    nadir_value = float(smoothed[trough_idx])
                    drop = first_peak_value - nadir_value
                    if drop < drop_threshold:
                        continue

                    subsequent_peaks = [p for p in peaks if p > trough_idx]
                    if not subsequent_peaks:
                        continue

                    for second_peak_idx in subsequent_peaks:
                        time_between_peaks = float(minute_offsets[second_peak_idx] - minute_offsets[peak_idx])
                        if time_between_peaks > max_minutes_between_peaks:
                            continue

                        second_peak_value = float(smoothed[second_peak_idx])
                        secondary_rise = second_peak_value - nadir_value
                        if secondary_rise < secondary_rise_threshold:
                            continue

                        # prefer events with greatest second rise amplitude
                        if secondary_rise <= best_secondary_rise:
                            continue

                        best_secondary_rise = secondary_rise
                        event_candidate = {
                            "service_date": day.service_date.isoformat(),
                            "first_peak_value": first_peak_value,
                            "second_peak_value": second_peak_value,
                            "nadir_value": nadir_value,
                            "drop_from_first": drop,
                            "secondary_rise": secondary_rise,
                            "time_between_peaks_minutes": time_between_peaks,
                            "first_peak_time": times[peak_idx].isoformat() if pd.notna(times[peak_idx]) else None,
                            "nadir_time": times[trough_idx].isoformat() if pd.notna(times[trough_idx]) else None,
                            "second_peak_time": times[second_peak_idx].isoformat() if pd.notna(times[second_peak_idx]) else None,
                        }
                    if event_candidate is not None:
                        break
                if event_candidate is not None:
                    break

            if event_candidate is not None:
                qualifying_by_date[event_candidate["service_date"]] = event_candidate

        qualifying = [qualifying_by_date[key] for key in sorted(qualifying_by_date.keys())]

        qualifying_count = len(qualifying)
        required_events = max(1, events_required)
        status = PatternStatus.DETECTED if qualifying_count >= required_events else PatternStatus.NOT_DETECTED

        avg_secondary_rise = float(np.mean([item["secondary_rise"] for item in qualifying])) if qualifying else 0.0
        avg_drop = float(np.mean([item["drop_from_first"] for item in qualifying])) if qualifying else 0.0
        avg_spacing = float(np.mean([item["time_between_peaks_minutes"] for item in qualifying])) if qualifying else 0.0

        metrics = {
            "analysis_days_considered": len(eligible_days),
            "dual_peak_days": qualifying_count,
            "first_peak_threshold": first_peak_threshold,
            "secondary_rise_threshold": secondary_rise_threshold,
            "drop_threshold": drop_threshold,
            "max_minutes_between_peaks": max_minutes_between_peaks,
            "avg_secondary_rise": avg_secondary_rise,
            "avg_drop_from_first": avg_drop,
            "avg_time_between_peaks_minutes": avg_spacing,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(required_events, 3))],
            "required_days": required_events,
        }
        confidence = min(1.0, qualifying_count / required_events)

        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
