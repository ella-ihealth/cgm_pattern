"""Detect predominant hypoglycemia across recent days."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class PredominantHypoglycemiaRule(PatternRule):
    id = "predominant_hypoglycemia"
    pattern_id = 2
    description = "TBR <70% ≥4% or any <54 mg/dL on ≥40% of last 7 days"
    version = "1.0.0"
    metadata = {
        **PATTERN_METADATA[2],
        "signature_name": PATTERN_METADATA[2]["pattern_signature_name"],
        "usefulness": PATTERN_METADATA[2]["usefulness_clinical_significance"],
        "clinical_teaching_note": PATTERN_METADATA[2]["clinical_teaching_note"],
        "category_tags": ["macro", "Macro Safety"],
        "requires_context": PATTERN_METADATA[2]["requires_context"] or "None",
        "context_types": PATTERN_METADATA[2]["context_types_42_factor_taxonomy"],
        "interpretive_adjustment": PATTERN_METADATA[2]["interpretive_adjustment_by_dx"],
        "rule_definition": PATTERN_METADATA[2]["rule_definition_1_line"],
        "post_event_windows": PATTERN_METADATA[2]["post_event_windows_to_evaluate"],
        "min_days_required": PATTERN_METADATA[2]["min_days_of_data_required"],
        "min_days_meeting_rule": PATTERN_METADATA[2]["min_days_meeting_rule_recurrence"],
        "input": "CGM",
        "metric": "TBR",
        "condition": "GEQ4PCT or ANY_<54",
        "repeat": "GEQ40PCT",
    }

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 7))
        percent_low_threshold = float(self.resolved_threshold(context, "percent_low_threshold", 0.04))
        fraction_required = float(self.resolved_threshold(context, "hypo_days_fraction_threshold", 0.40))
        severe_low_threshold = float(self.resolved_threshold(context, "severe_low_threshold", 54.0))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))

        validation_pool = [
            s for s in window.validation_summaries if s.coverage_ratio >= coverage_threshold
        ]
        validation_pool = validation_pool[-validation_window_days:]
        if len(validation_pool) < validation_window_days:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "eligible_validation_days": len(validation_pool),
                    "required_validation_days": validation_window_days,
                },
                metrics={},
                version=self.version,
            )

        eligible = [s for s in window.analysis_summaries if s.coverage_ratio >= coverage_threshold]
        eligible = eligible[-analysis_window_days:]
        if len(eligible) < required_days:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "eligible_days": len(eligible),
                    "required_analysis_days": required_days,
                },
                metrics={},
                version=self.version,
            )

        qualifying_days = []
        seen_dates: set[str] = set()
        for summary in eligible:
            condition = (
                summary.percent_low >= percent_low_threshold
                or (summary.min_glucose is not None and summary.min_glucose < severe_low_threshold)
            )
            if not condition:
                continue
            date_key = summary.service_date.isoformat()
            if date_key in seen_dates:
                continue
            seen_dates.add(date_key)
            qualifying_days.append(
                {
                    "service_date": date_key,
                    "percent_low": summary.percent_low,
                    "min_glucose": summary.min_glucose,
                }
            )

        required_occurrences = max(1, math.ceil(len(eligible) * fraction_required))
        status = PatternStatus.DETECTED if len(qualifying_days) >= required_occurrences else PatternStatus.NOT_DETECTED

        metrics = {
            "analysis_days_considered": len(eligible),
            "hypoglycemia_days": len(qualifying_days),
            "percent_low_threshold": percent_low_threshold,
        }
        evidence = {
            "hypo_examples": qualifying_days[:5],
            "required_days": required_occurrences,
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, len(qualifying_days) / max(1, required_occurrences)),
            version=self.version,
        )
