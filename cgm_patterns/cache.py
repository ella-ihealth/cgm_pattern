"""Caches for incremental CGM computations."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple

from .models import DailyCGMSummary


@dataclass
class DailySummaryCache:
    """Simple in-memory cache keyed by patient/date."""

    _store: Dict[Tuple[str, str], DailyCGMSummary] = field(default_factory=dict)

    def get(self, patient_id: str, service_date: str) -> DailyCGMSummary | None:
        return self._store.get((patient_id, service_date))

    def set(self, summary: DailyCGMSummary) -> None:
        key = (summary.patient_id, summary.service_date.isoformat())
        self._store[key] = summary

    def prune(self, patient_id: str, keep_dates: set[str]) -> None:
        """Remove cached entries for a patient that are no longer needed."""

        to_remove = [key for key in self._store if key[0] == patient_id and key[1] not in keep_dates]
        for key in to_remove:
            del self._store[key]
