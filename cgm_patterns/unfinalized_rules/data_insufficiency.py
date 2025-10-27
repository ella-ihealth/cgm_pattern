"""Detect insufficient CGM data coverage and tagging context."""
from __future__ import annotations

from typing import Any, Mapping

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class DataInsufficiencyRule(PatternRule):
    id = "data_insufficiency"
    pattern_id = 40
    description = "<70% active CGM time over 14 days"
    version = "1.0.0"
    metadata = PATTERN_METADATA.get(40, {})

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "data_validation_min_coverage", 0.0))
        evaluation_days = int(self.resolved_threshold(context, "data_window_days", 14))
        active_threshold = float(self.resolved_threshold(context, "data_active_threshold", 0.70))
        tag_threshold = int(self.resolved_threshold(context, "data_tag_threshold", 3))

        recent_days, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            evaluation_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

        coverage_values = [day.coverage_ratio() for day in recent_days]
        average_active = sum(coverage_values) / len(coverage_values) if coverage_values else 0.0
        low_coverage_days = [
            {
                "service_date": day.service_date.isoformat(),
                "coverage_ratio": day.coverage_ratio(),
            }
            for day in recent_days
            if day.coverage_ratio() < active_threshold
        ]

        status = PatternStatus.DETECTED if average_active < active_threshold else PatternStatus.NOT_DETECTED

        tag_counts = self._extract_tag_counts(context.extras)
        tag_flag = None
        if tag_counts:
            meal_tags = tag_counts.get("meal", 0) + tag_counts.get("meals", 0)
            med_tags = tag_counts.get("med", 0) + tag_counts.get("medication", 0)
            total_tags = meal_tags + med_tags
            tag_flag = total_tags < tag_threshold
        else:
            meal_tags = None
            med_tags = None
            total_tags = None

        if status is PatternStatus.DETECTED and active_threshold > 0:
            confidence = min(1.0, max(0.0, (active_threshold - average_active) / active_threshold))
        else:
            confidence = 0.0

        metrics = {
            "window_days_evaluated": len(recent_days),
            "average_active_fraction": average_active,
            "active_threshold": active_threshold,
            "days_below_threshold": len(low_coverage_days),
        }
        if total_tags is not None:
            metrics.update(
                {
                    "meal_tags_14d": meal_tags,
                    "med_tags_14d": med_tags,
                    "total_tags_14d": total_tags,
                }
            )

        evidence: dict[str, Any] = {
            "low_coverage_days": low_coverage_days[:5],
            "required_average": active_threshold,
            "average_active": average_active,
        }
        if tag_counts:
            evidence["tag_summary"] = tag_counts
            evidence["few_tags_flag"] = tag_flag

        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )

    @staticmethod
    def _extract_tag_counts(extras: Mapping[str, Any] | None) -> Mapping[str, int] | None:
        if not extras:
            return None

        # Prefer explicit 14-day tag aggregates when supplied.
        keys_to_try = [
            "tag_counts_14d",
            "tag_counts",
            "meal_med_tags_14d",
        ]
        for key in keys_to_try:
            value = extras.get(key)
            if isinstance(value, Mapping):
                return {k: int(v) for k, v in value.items() if isinstance(v, (int, float))}
        return None
