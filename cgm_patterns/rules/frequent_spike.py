"""Detect frequent rapid glucose spikes with brief recovery intervals."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class FrequentSpikeRule(PatternRule):
    id = "frequent_spike"
    pattern_id = 1
    description = (
        "Multiple rapid rises >=50 mg/dL within 60 min, at least 3 per day,"
        " each followed by recovery <90 min, on >=3 days in last 7"
    )
    version = "1.0.0"
    metadata = PATTERN_METADATA[1]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        days_required = int(self.resolved_threshold(context, "frequent_spike_days_required", 3))

        rise_threshold = float(self.resolved_threshold(context, "frequent_spike_rise_threshold", 50.0))
        rise_window_minutes = float(self.resolved_threshold(context, "frequent_spike_rise_window", 60.0))
        recovery_minutes = float(self.resolved_threshold(context, "frequent_spike_recovery_minutes", 90.0))
        spikes_per_day_required = int(self.resolved_threshold(context, "frequent_spike_per_day", 3))

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
            prepared = window.prepared_day(day)
            frame = prepared.frame.dropna(subset=["glucose_mg_dL", "minutes", "local_time"]).reset_index(drop=True)
            if frame.empty:
                continue

            glucose = frame["glucose_mg_dL"].to_numpy()
            minutes = frame["minutes"].to_numpy()
            local_times = frame["local_time"].to_numpy()

            if len(glucose) < 2:
                continue

            cumulative_minutes = np.concatenate(([0.0], np.cumsum(minutes[:-1])))

            spike_count = 0
            spike_details: list[dict[str, object]] = []
            idx = 0
            while idx < len(glucose) - 1:
                baseline_value = float(glucose[idx])
                window_end_minutes = cumulative_minutes[idx] + rise_window_minutes
                j = idx + 1
                peak_idx = idx
                peak_value = baseline_value
                while j < len(glucose) and cumulative_minutes[j] <= window_end_minutes:
                    if glucose[j] > peak_value:
                        peak_value = float(glucose[j])
                        peak_idx = j
                    j += 1

                amplitude = peak_value - baseline_value
                if amplitude < rise_threshold or peak_idx == idx:
                    idx += 1
                    continue

                # recovery check: back near baseline (within amplitude/2 or drop 50%)
                recovery_limit = cumulative_minutes[peak_idx] + recovery_minutes
                recovery_idx = peak_idx
                recovery_value = peak_value
                for k in range(peak_idx + 1, len(glucose)):
                    if cumulative_minutes[k] > recovery_limit:
                        break
                    if glucose[k] <= baseline_value + amplitude / 2:
                        recovery_idx = k
                        recovery_value = float(glucose[k])
                        break

                if recovery_idx == peak_idx:
                    idx += 1
                    continue

                spike_count += 1
                spike_details.append(
                    {
                        "start_time": local_times[idx].isoformat(),
                        "peak_time": local_times[peak_idx].isoformat(),
                        "recovery_time": local_times[recovery_idx].isoformat(),
                        "baseline_glucose": baseline_value,
                        "peak_glucose": peak_value,
                        "recovery_glucose": recovery_value,
                        "rise_amplitude": amplitude,
                        "time_to_peak_minutes": cumulative_minutes[peak_idx] - cumulative_minutes[idx],
                        "recovery_minutes": cumulative_minutes[recovery_idx] - cumulative_minutes[peak_idx],
                    }
                )
                idx = recovery_idx
            if spike_count >= spikes_per_day_required:
                qualifying.append(
                    {
                        "service_date": day.service_date.isoformat(),
                        "spike_count": spike_count,
                        "examples": spike_details[:spikes_per_day_required],
                    }
                )

        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count >= days_required else PatternStatus.NOT_DETECTED
        avg_spikes = float(np.mean([payload["spike_count"] for payload in qualifying])) if qualifying else 0.0
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "frequent_spike_days": qualifying_count,
            "avg_spikes_per_day": avg_spikes,
            "rise_threshold": rise_threshold,
            "rise_window_minutes": rise_window_minutes,
            "recovery_minutes_threshold": recovery_minutes,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(days_required, 3))],
            "required_days": days_required,
        }
        confidence = min(1.0, qualifying_count / max(1, days_required))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
