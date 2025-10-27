"""Base class and utilities for pattern rules."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from .models import (
    PatternContext,
    PatternDetection,
    PatternDescriptor,
    PatternInputBundle,
    PatternStatus,
)


class PatternRule(ABC):
    """Abstract pattern rule with metadata."""

    id: str = ""
    description: str = ""
    version: str = "1.0.0"
    inputs: tuple[str, ...] = ("cgm_data",)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.id:
            raise ValueError(f"Rule {cls.__name__} must define a non-empty id")

    @property
    def descriptor(self) -> PatternDescriptor:
        """Return static metadata describing this pattern."""

        return PatternDescriptor(
            pattern_id=self.id,
            name=self.description or self.id,
            description=self.description or self.id,
            version=self.version,
            inputs=self.inputs,
        )

    @abstractmethod
    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        """Run the rule on the precomputed window."""

    def resolved_threshold(self, context: PatternContext, key: str, default: Any) -> Any:
        """Helper to fetch pattern-specific threshold overrides."""

        return context.pattern_threshold(self.id, key, default)

    def ensure_validation_window(
        self,
        window: PatternInputBundle,
        context: PatternContext,
        coverage_threshold: float,
        required_days: int,
        *,
        use_raw_days: bool = False,
    ) -> tuple[list[Any], PatternDetection | None]:
        """Return recent validation items and optionally an insufficient-data detection."""

        if required_days <= 0:
            return [], None

        if use_raw_days:
            items = [
                day
                for day in window.validation_days
                if day.coverage_ratio() >= coverage_threshold
            ]
        else:
            items = [
                summary
                for summary in window.validation_summaries
                if getattr(summary, "coverage_ratio", 0.0) >= coverage_threshold
            ]

        recent_items = items[-required_days:]
        if len(recent_items) < required_days:
            detection = PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "eligible_validation_days": len(recent_items),
                    "required_validation_days": required_days,
                },
                metrics={},
                version=self.version,
            )
            return recent_items, detection

        return recent_items, None

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return f"<{self.__class__.__name__} id={self.id!r} version={self.version!r}>"
