"""Detect evening sharp glucose spikes riding on a stable baseline."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class EveningSpikeRule(PatternRule):
    id = "evening_spike"
    pattern_id = 37
    description = (
        "Evening spike: 17:00–22:00 baseline then >=50 mg/dL rise with peak >180 mg/dL,"
        " derivative >=1 mg/dL/min, recovery within 2 hours on >=3 days"
    )
    version = "1.0.0"
    metadata = PATTERN_METADATA[37]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        events_required = int(self.resolved_threshold(context, "evening_spike_days_required", 3))

        baseline_duration_min = float(self.resolved_threshold(context, "evening_spike_baseline_minutes", 30.0))
        derivative_threshold = float(self.resolved_threshold(context, "evening_spike_derivative_threshold", 1.0))
        amplitude_threshold = float(self.resolved_threshold(context, "evening_spike_amplitude_threshold", 50.0))
        peak_threshold = float(self.resolved_threshold(context, "evening_spike_peak_threshold", 180.0))
        window_start = float(self.resolved_threshold(context, "evening_spike_window_start", 17.0))
        window_end = float(self.resolved_threshold(context, "evening_spike_window_end", 22.0))
        smoothing_window = int(self.resolved_threshold(context, "evening_spike_smoothing_window", 11))
        recovery_threshold_fraction = float(self.resolved_threshold(context, "evening_spike_recovery_fraction", 0.5))
        max_time_to_peak = float(self.resolved_threshold(context, "evening_spike_max_time_to_peak", 120.0))
        max_recovery_minutes = float(self.resolved_threshold(context, "evening_spike_max_recovery_minutes", 120.0))

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
            evening = window.time_window(day, window_start, window_end)
            frame = evening.dropna(subset=["glucose_mg_dL", "minutes"]).reset_index(drop=True)
            if frame.empty:
                continue

            values = frame["glucose_mg_dL"].to_numpy()
            minutes = frame["minutes"].to_numpy()
            times = frame["local_time"].to_numpy()
            if len(values) < 3:
                continue

            window_size = smoothing_window if smoothing_window % 2 == 1 else smoothing_window + 1
            window_size = min(window_size, len(values) if len(values) % 2 == 1 else len(values) - 1)
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
            derivatives = np.gradient(smoothed, minute_offsets, edge_order=1)

            positive_minutes = minutes[minutes > 0]
            if positive_minutes.size == 0:
                continue
            baseline_length = int(np.ceil(baseline_duration_min / np.median(positive_minutes)))
            if baseline_length >= len(smoothed):
                continue

            baseline_slice = smoothed[:baseline_length]
            baseline_mean = float(np.mean(baseline_slice))
            baseline_slope = float(np.mean(np.abs(derivatives[:baseline_length])))
            if baseline_slope > derivative_threshold / 5:
                continue

            peak_idx = int(np.argmax(smoothed))
            if peak_idx <= baseline_length:
                continue
            peak_value = float(smoothed[peak_idx])
            peak_time = times[peak_idx]

            rise_amplitude = peak_value - baseline_mean
            if rise_amplitude < amplitude_threshold or peak_value <= peak_threshold:
                continue

            derivative_peak = float(derivatives[peak_idx])
            if derivative_peak < derivative_threshold:
                continue

            recovery_idx = peak_idx
            for idx in range(peak_idx + 1, len(smoothed)):
                if abs(smoothed[idx] - baseline_mean) <= recovery_threshold_fraction * amplitude_threshold:
                    recovery_idx = idx
                    break

            if recovery_idx == peak_idx:
                continue

            time_to_peak_minutes = float(np.sum(minutes[baseline_length:peak_idx]))
            if time_to_peak_minutes > max_time_to_peak:
                continue

            recovery_duration_minutes = float(np.sum(minutes[peak_idx:recovery_idx]))
            if recovery_duration_minutes > max_recovery_minutes:
                continue

            recovery_slope = float(np.mean(derivatives[peak_idx:recovery_idx + 1]))
            if recovery_slope > derivative_threshold / 5:
                continue

            recovery_time = times[recovery_idx]

            qualifying.append(
                {
                    "service_date": day.service_date.isoformat(),
                    "baseline_mean": baseline_mean,
                    "peak_value": peak_value,
                    "rise_amplitude": rise_amplitude,
                    "derivative_peak": derivative_peak,
                    "time_to_peak_minutes": time_to_peak_minutes,
                    "recovery_duration_minutes": recovery_duration_minutes,
                    "peak_time": peak_time.isoformat(),
                    "recovery_time": recovery_time.isoformat(),
                }
            )

        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count >= events_required else PatternStatus.NOT_DETECTED
        avg_amplitude = float(np.mean([payload["rise_amplitude"] for payload in qualifying])) if qualifying else 0.0
        avg_time_to_peak = float(np.mean([payload["time_to_peak_minutes"] for payload in qualifying])) if qualifying else 0.0
        avg_recovery = float(np.mean([payload["recovery_duration_minutes"] for payload in qualifying])) if qualifying else 0.0
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "evening_spike_days": qualifying_count,
            "baseline_minutes": baseline_duration_min,
            "amplitude_threshold": amplitude_threshold,
            "derivative_threshold": derivative_threshold,
            "peak_threshold": peak_threshold,
            "avg_rise_amplitude": avg_amplitude,
            "avg_time_to_peak_minutes": avg_time_to_peak,
            "avg_recovery_duration_minutes": avg_recovery,
            "window_start_hour": window_start,
            "window_end_hour": window_end,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(events_required, 3))],
            "required_days": events_required,
        }
        confidence = min(1.0, qualifying_count / max(1, events_required))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
