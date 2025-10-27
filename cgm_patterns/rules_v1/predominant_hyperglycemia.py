"""Detect predominant hyperglycemia across recent days."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class PredominantHyperglycemiaRule(PatternRule):
    id = "predominant_hyperglycemia"
    pattern_id = 1
    description = "TAR >30% on ≥40% of last 7 days"
    version = "1.0.0"
    metadata = {
        **PATTERN_METADATA[1],
        "signature_name": PATTERN_METADATA[1]["pattern_signature_name"],
        "usefulness": PATTERN_METADATA[1]["usefulness_clinical_significance"],
        "clinical_teaching_note": PATTERN_METADATA[1]["clinical_teaching_note"],
        "category_tags": ["macro", "Macro Control"],
        "requires_context": PATTERN_METADATA[1]["requires_context"] or "None",
        "context_types": PATTERN_METADATA[1]["context_types_42_factor_taxonomy"],
        "interpretive_adjustment": PATTERN_METADATA[1]["interpretive_adjustment_by_dx"],
        "rule_definition": PATTERN_METADATA[1]["rule_definition_1_line"],
        "post_event_windows": PATTERN_METADATA[1]["post_event_windows_to_evaluate"],
        "min_days_required": PATTERN_METADATA[1]["min_days_of_data_required"],
        "min_days_meeting_rule": PATTERN_METADATA[1]["min_days_meeting_rule_recurrence"],
        "input": "CGM",
        "metric": "TAR>180",
        "condition": ">30%",
        "repeat": "GEQ40PCT",
    }

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 7))
        tar_threshold = float(self.resolved_threshold(context, "tar_threshold", 0.30))
        fraction_required = float(self.resolved_threshold(context, "tar_days_fraction_threshold", 0.40))

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

        tar_days = []
        seen_dates: set[str] = set()
        for summary in eligible:
            if summary.percent_high <= tar_threshold:
                continue
            date_key = summary.service_date.isoformat()
            if date_key in seen_dates:
                continue
            seen_dates.add(date_key)
            tar_days.append(
                {
                    "service_date": date_key,
                    "percent_high": summary.percent_high,
                    "time_high_minutes": summary.time_high_minutes,
                }
            )

        required_occurrences = max(1, math.ceil(len(eligible) * fraction_required))
        status = PatternStatus.DETECTED if len(tar_days) >= required_occurrences else PatternStatus.NOT_DETECTED

        metrics = {
            "analysis_days_considered": len(eligible),
            "tar_days": len(tar_days),
            "tar_threshold": tar_threshold,
            "fraction_required": fraction_required,
        }
        evidence = {
            "tar_examples": tar_days[:5],
            "required_days": required_occurrences,
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, len(tar_days) / max(1, required_occurrences)),
            version=self.version,
        )
