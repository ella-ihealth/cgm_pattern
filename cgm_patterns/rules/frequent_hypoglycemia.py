"""Detect frequent hypoglycemia episodes across the full day."""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import consecutive_durations


@register_rule
class FrequentHypoglycemiaRule(PatternRule):
    id = "frequent_hypoglycemia"
    pattern_id = 36
    description = (
        "Recurrent BG <70 mg/dL lasting >=15 minutes on >=7 of 14 review days "
        "(>=40% of eligible days)"
    )
    version = "1.0.0"
    metadata = PATTERN_METADATA[36]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 10))
        low_threshold = float(self.resolved_threshold(context, "frequent_low_threshold", 70.0))
        minimum_minutes = float(self.resolved_threshold(context, "frequent_low_duration", 15.0))
        minimum_event_days = int(self.resolved_threshold(context, "frequent_low_days_required", 7))
        ratio_threshold = float(self.resolved_threshold(context, "frequent_low_day_ratio", 0.4))
        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 14))

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

        qualifying_by_date: dict[str, dict[str, Any]] = {}
        for day in eligible_days:
            prepared = window.prepared_day(day)
            frame = prepared.frame
            if frame.empty:
                continue

            low_mask = frame["glucose_mg_dL"] < low_threshold
            if not low_mask.any():
                continue

            longest_start_index: int | None = None
            longest_duration = 0.0
            for start_index, duration in consecutive_durations(low_mask, frame["minutes"]):
                if duration > longest_duration:
                    longest_duration = duration
                    longest_start_index = start_index

            if longest_duration < minimum_minutes:
                continue

            date_key = day.service_date.isoformat()
            candidate: dict[str, Any] = {
                "service_date": date_key,
                "longest_episode_minutes": longest_duration,
                "min_glucose": float(frame.loc[low_mask, "glucose_mg_dL"].min()),
            }
            if longest_start_index is not None and 0 <= longest_start_index < len(frame):
                start_time = frame.iloc[longest_start_index]["local_time"]
                if pd.notna(start_time):
                    candidate["episode_start"] = start_time.isoformat()

            previous = qualifying_by_date.get(date_key)
            if previous is None or float(previous.get("longest_episode_minutes", 0.0)) < longest_duration:
                qualifying_by_date[date_key] = candidate

        qualifying = [qualifying_by_date[key] for key in sorted(qualifying_by_date.keys())]
        qualifying_count = len(qualifying)
        total_eligible_days = len(eligible_days)

        ratio_required_days = math.ceil(ratio_threshold * total_eligible_days) if total_eligible_days else 0
        required_event_days = max(1, minimum_event_days, ratio_required_days)

        meets_count_requirement = qualifying_count >= required_event_days
        meets_ratio_requirement = (
            qualifying_count / total_eligible_days >= ratio_threshold if total_eligible_days > 0 else False
        )
        detected = meets_count_requirement and meets_ratio_requirement
        status = PatternStatus.DETECTED if detected else PatternStatus.NOT_DETECTED

        metrics = {
            "analysis_days_considered": total_eligible_days,
            "hypoglycemia_days": qualifying_count,
            "low_threshold": low_threshold,
            "minimum_duration_minutes": minimum_minutes,
            "minimum_event_days": minimum_event_days,
            "ratio_threshold": ratio_threshold,
            "required_event_days": required_event_days,
        }
        evidence = {
            "examples": qualifying[: min(len(qualifying), max(required_event_days, 3))],
            "required_days": required_event_days,
            "meets_ratio_requirement": meets_ratio_requirement,
        }
        confidence = min(1.0, qualifying_count / required_event_days) if required_event_days else 0.0

        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
