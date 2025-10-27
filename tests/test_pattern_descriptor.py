from datetime import date

from cgm_patterns.models import PatternInputBundle, PatternContext
from cgm_patterns.rule_base import PatternRule


class _StubRule(PatternRule):
    id = "stub"
    description = "Stub Pattern"
    version = "2.0.0"
    inputs = ("cgm", "rolling")

    def detect(self, window: PatternInputBundle, context: PatternContext):  # pragma: no cover - not used
        raise NotImplementedError


def test_pattern_descriptor_exposes_metadata():
    rule = _StubRule()
    descriptor = rule.descriptor
    assert descriptor.pattern_id == "stub"
    assert descriptor.name == "Stub Pattern"
    assert descriptor.version == "2.0.0"
    assert descriptor.inputs == ("cgm", "rolling")
