"""Detect Somogyi effect (overnight low with rebound high)."""
from __future__ import annotations

from datetime import timedelta

import math
import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import consecutive_durations, filter_time_window, prepare_day, total_minutes


@register_rule
class SomogyiEffectRule(PatternRule):
    id = "somogyi_effect"
    pattern_id = 12
    description = "Overnight low followed by ≥100 mg/dL rebound within 2-4h"
    version = "1.0.0"
    metadata = PATTERN_METADATA[12]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        fraction_required = float(self.resolved_threshold(context, "somogyi_days_fraction", 2 / 7))
        low_threshold = float(self.resolved_threshold(context, "somogyi_low_threshold", 70.0))
        low_minutes_required = float(self.resolved_threshold(context, "somogyi_low_minutes", 15.0))
        rebound_delta = float(self.resolved_threshold(context, "somogyi_rebound_delta", 100.0))
        rebound_window_hours = float(self.resolved_threshold(context, "somogyi_rebound_window_hours", 4.0))
        rebound_min_delay_hours = float(self.resolved_threshold(context, "somogyi_rebound_min_delay_hours", 2.0))

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

        qualifying = []
        for day in eligible_days:
            prepared = prepare_day(day)
            full_frame = prepared.frame
            overnight = filter_time_window(prepared, 0.0, 8.0).reset_index(drop=True)
            if overnight.empty:
                continue

            lows = overnight["glucose_mg_dL"] < low_threshold
            durations = list(consecutive_durations(lows, overnight["minutes"]))
            if not durations:
                continue

            # Evaluate each low block for rebound behaviour.
            mask = lows.reset_index(drop=True)
            for start_idx, duration in durations:
                if duration < low_minutes_required:
                    continue
                end_idx = start_idx
                while end_idx < len(mask) and mask.iloc[end_idx]:
                    end_idx += 1
                low_segment = overnight.iloc[start_idx:end_idx]
                if low_segment.empty:
                    continue
                low_min = float(low_segment["glucose_mg_dL"].min())
                low_end_time = low_segment["local_time"].iloc[-1]

                lookahead_start = low_end_time + timedelta(hours=rebound_min_delay_hours)
                lookahead_end = low_end_time + timedelta(hours=rebound_window_hours)
                future = full_frame[
                    (full_frame["local_time"] >= lookahead_start)
                    & (full_frame["local_time"] <= lookahead_end)
                ]
                if future.empty:
                    continue
                peak = float(future["glucose_mg_dL"].max())
                rise = peak - low_min
                if rise >= rebound_delta:
                    qualifying.append(
                        {
                            "service_date": day.service_date.isoformat(),
                            "low_minutes": duration,
                            "rise": rise,
                        }
                    )
                    break

        required_occurrences = max(1, math.ceil(len(eligible_days) * fraction_required))
        status = PatternStatus.DETECTED if len(qualifying) >= required_occurrences else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "somogyi_days": len(qualifying),
            "required_rise": rebound_delta,
        }
        evidence = {
            "examples": qualifying[:3],
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
