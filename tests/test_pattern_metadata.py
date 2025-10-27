from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cgm_patterns.models import PatternContext
from cgm_patterns.pattern_metadata import should_evaluate_rule
from cgm_patterns.rule_base import PatternRule


class _BaseStubRule(PatternRule):
    id = "stub"
    description = "stub"

    def detect(self, window, context):  # pragma: no cover - not used in tests
        raise NotImplementedError


class _MetadataRule(_BaseStubRule):
    id = "meta"
    metadata = {"diagnosis_context": "T1DM"}


class _GeneralMetadataRule(_BaseStubRule):
    id = "general"
    metadata = {"diagnosis_context": "General"}


def test_should_evaluate_rule_without_metadata_allows_rule():
    rule = _BaseStubRule()
    context = PatternContext(patient_id="p", analysis_date=date.today())
    assert should_evaluate_rule(rule, context) is True


def test_should_evaluate_rule_respects_diagnosis_mismatch():
    rule = _MetadataRule()
    context = PatternContext(
        patient_id="p",
        analysis_date=date.today(),
        extras={"diagnosis_contexts": ["T2DM"]},
    )
    assert should_evaluate_rule(rule, context) is False


def test_should_evaluate_rule_allows_when_context_unknown():
    rule = _MetadataRule()
    context = PatternContext(patient_id="p", analysis_date=date.today())
    assert should_evaluate_rule(rule, context) is True


def test_should_evaluate_rule_allows_general_requirement():
    rule = _GeneralMetadataRule()
    context = PatternContext(
        patient_id="p",
        analysis_date=date.today(),
        extras={"diagnosis_context": "T2DM"},
    )
    assert should_evaluate_rule(rule, context) is True
