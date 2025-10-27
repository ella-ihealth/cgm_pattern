"""Rule detecting repeated post-prandial spikes."""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class RecurrentPostMealSpikeRule(PatternRule):
    id = "recurrent_post_meal_spike"
    description = "Glucose rises >180 mg/dL within 2 hours on ≥3 of last 7 days"
    version = "1.0.0"

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        spike_threshold = float(self.resolved_threshold(context, "spike_glucose_threshold", 180.0))
        climb_threshold = float(self.resolved_threshold(context, "spike_climb_threshold", 50.0))
        occurrences_needed = int(self.resolved_threshold(context, "spike_days_required", 3))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))

        _, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            validation_window_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

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

        spike_days: List[Dict[str, Any]] = []
        seen_dates: set[str] = set()
        for day in eligible_days:
            df = day.readings.copy()
            if df.empty:
                continue
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df.sort_values("timestamp", inplace=True)
            df.set_index("timestamp", inplace=True)
            rolling_max = df["glucose_mg_dL"].rolling("120min").max()
            rolling_min = df["glucose_mg_dL"].rolling("120min").min()
            climb = rolling_max - rolling_min
            spike_mask = (rolling_max >= spike_threshold) & (climb >= climb_threshold)
            if spike_mask.any():
                idx = spike_mask.idxmax()
                date_key = day.service_date.isoformat()
                if date_key in seen_dates:
                    continue
                seen_dates.add(date_key)
                spike_days.append(
                    {
                        "service_date": date_key,
                        "spike_time": idx.isoformat(),
                        "max_glucose": float(rolling_max.loc[idx]),
                        "rise_mg_dl": float(climb.loc[idx]),
                    }
                )

        status = PatternStatus.DETECTED if len(spike_days) >= occurrences_needed else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "spike_days": len(spike_days),
            "spike_threshold": spike_threshold,
        }
        evidence = {
            "spike_examples": spike_days[:5],
            "required_days": occurrences_needed,
        }
        confidence = min(1.0, len(spike_days) / max(1, occurrences_needed))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
