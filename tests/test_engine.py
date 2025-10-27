from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cgm_patterns.engine import SlidingWindowEngine
from cgm_patterns.registry import RuleRegistry


class _EmptySource:
    def iter_days(self, patient_id):  # pragma: no cover - simple stub
        return []


def test_sliding_window_engine_uses_shared_cache_by_default():
    source = _EmptySource()
    registry = RuleRegistry()

    engine_one = SlidingWindowEngine(source, registry)
    engine_two = SlidingWindowEngine(source, registry)

    assert engine_one._summary_cache is engine_two._summary_cache
