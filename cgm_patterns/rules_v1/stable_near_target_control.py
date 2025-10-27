"""Detect stable / near-target glucose control."""
from __future__ import annotations

import math

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class StableNearTargetControlRule(PatternRule):
    id = "stable_near_target_control"
    pattern_id = 4
    description = "TIR ≥70% and CV <36% on ≥40% of last 7 days"
    version = "1.0.0"
    metadata = PATTERN_METADATA[4]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        tir_threshold = float(self.resolved_threshold(context, "tir_threshold", 0.70))
        cv_threshold = float(self.resolved_threshold(context, "cv_threshold", 0.36))
        fraction_required = float(self.resolved_threshold(context, "stable_days_fraction_threshold", 0.40))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))

        _, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            validation_window_days,
        )
        if insufficient_validation:
            return insufficient_validation

        eligible = [s for s in window.analysis_summaries if s.coverage_ratio >= coverage_threshold and s.mean_glucose > 0]
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

        qualifying = []
        for summary in eligible:
            if summary.mean_glucose <= 0 or math.isnan(summary.mean_glucose):
                continue
            cv = summary.std_glucose / summary.mean_glucose if summary.mean_glucose else float("nan")
            if math.isnan(cv):
                continue
            if summary.percent_in_range >= tir_threshold and cv < cv_threshold:
                qualifying.append(
                    {
                        "service_date": summary.service_date.isoformat(),
                        "percent_in_range": summary.percent_in_range,
                        "cv": cv,
                    }
                )

        required_occurrences = max(1, math.ceil(len(eligible) * fraction_required))
        status = PatternStatus.DETECTED if len(qualifying) >= required_occurrences else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible),
            "stable_days": len(qualifying),
            "tir_threshold": tir_threshold,
            "cv_threshold": cv_threshold,
        }
        evidence = {
            "stable_examples": qualifying[:5],
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
