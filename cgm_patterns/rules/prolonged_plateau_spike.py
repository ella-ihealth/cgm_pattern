"""Detect prolonged plateau spikes indicating sustained hyperglycemia."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class ProlongedPlateauSpikeRule(PatternRule):
    id = "prolonged_plateau_spike"
    pattern_id = 1
    description = (
        "Stable baseline then >=50 mg/dL rise within 2 h followed by sustained plateau >=180 mg/dL"
        " for >=180 min or >=250 mg/dL for >=120 min with |ΔG/Δt|<=0.5 mg/dL/min on >=2 days"
    )
    version = "1.0.0"
    metadata = PATTERN_METADATA[1]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        events_required = int(self.resolved_threshold(context, "plateau_spike_days_required", 2))

        baseline_duration_min = float(self.resolved_threshold(context, "plateau_baseline_minutes", 30.0))
        derivative_threshold = float(self.resolved_threshold(context, "plateau_derivative_threshold", 1.0))
        amplitude_threshold = float(self.resolved_threshold(context, "plateau_amplitude_threshold", 50.0))
        peak_threshold = float(self.resolved_threshold(context, "plateau_peak_threshold", 180.0))
        recovery_derivative_threshold = float(self.resolved_threshold(context, "plateau_recovery_derivative_threshold", 0.5))

        plateau_threshold = float(self.resolved_threshold(context, "plateau_threshold", 180.0))
        plateau_minutes_required = float(self.resolved_threshold(context, "plateau_minutes_required", 180.0))
        high_plateau_threshold = float(self.resolved_threshold(context, "high_plateau_threshold", 250.0))
        high_plateau_minutes_required = float(self.resolved_threshold(context, "high_plateau_minutes_required", 120.0))

        window_start = float(self.resolved_threshold(context, "plateau_window_start", 0.0))
        window_end = float(self.resolved_threshold(context, "plateau_window_end", 24.0))
        smoothing_window = int(self.resolved_threshold(context, "plateau_smoothing_window", 11))

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
            day_window = window.time_window(day, window_start, window_end)
            frame = day_window.dropna(subset=["glucose_mg_dL", "minutes"]).reset_index(drop=True)
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
            if rise_amplitude < amplitude_threshold or peak_value < peak_threshold:
                continue

            derivative_peak = float(derivatives[peak_idx])
            if derivative_peak < derivative_threshold:
                continue

            # Evaluate plateau after peak
            plateau_slice = smoothed[peak_idx:]
            plateau_minutes = minutes[peak_idx:]
            if plateau_slice.size == 0:
                continue

            plateau_high_mask = plateau_slice >= high_plateau_threshold
            plateau_standard_mask = plateau_slice >= plateau_threshold

            def accumulated_minutes(mask: np.ndarray) -> float:
                if not mask.any():
                    return 0.0
                indices = mask.nonzero()[0]
                return float(plateau_minutes[indices].sum())

            high_plateau_minutes = accumulated_minutes(plateau_high_mask)
            standard_plateau_minutes = accumulated_minutes(plateau_standard_mask)

            meets_plateau = (
                high_plateau_minutes >= high_plateau_minutes_required
                or standard_plateau_minutes >= plateau_minutes_required
            )
            if not meets_plateau:
                continue

            # Check that second derivative (drop) is mild (plateau) and derivative small in plateau
            plateau_derivatives = derivatives[peak_idx:]
            if np.max(np.abs(plateau_derivatives)) > recovery_derivative_threshold:
                continue

            qualifying.append(
                {
                    "service_date": day.service_date.isoformat(),
                    "baseline_mean": baseline_mean,
                    "peak_value": peak_value,
                    "rise_amplitude": rise_amplitude,
                    "derivative_peak": derivative_peak,
                    "high_plateau_minutes": high_plateau_minutes,
                    "plateau_minutes": standard_plateau_minutes,
                    "peak_time": peak_time.isoformat(),
                }
            )

        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count >= events_required else PatternStatus.NOT_DETECTED
        avg_amplitude = float(np.mean([payload["rise_amplitude"] for payload in qualifying])) if qualifying else 0.0
        avg_high_plateau_minutes = float(np.mean([payload["high_plateau_minutes"] for payload in qualifying])) if qualifying else 0.0
        avg_plateau_minutes = float(np.mean([payload["plateau_minutes"] for payload in qualifying])) if qualifying else 0.0
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "plateau_spike_days": qualifying_count,
            "baseline_minutes": baseline_duration_min,
            "amplitude_threshold": amplitude_threshold,
            "derivative_threshold": derivative_threshold,
            "plateau_threshold": plateau_threshold,
            "high_plateau_threshold": high_plateau_threshold,
            "avg_rise_amplitude": avg_amplitude,
            "avg_plateau_minutes": avg_plateau_minutes,
            "avg_high_plateau_minutes": avg_high_plateau_minutes,
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
