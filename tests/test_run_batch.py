from datetime import date
import json
from pathlib import Path

import pandas as pd

from cgm_patterns.models import CGMDay, PatternDetection, PatternStatus
from cgm_patterns.run_batch import CallableSource, JsonDirectorySource, _detection_to_dict


class _DummySource(JsonDirectorySource):
    pass


def test_json_directory_source_iter_days(tmp_path: Path):
    data = [
        {
            "service_date": "2024-01-01",
            "readings": [
                {"timestamp": "2024-01-01T00:00:00Z", "glucose_mg_dL": 120},
                {"timestamp": "2024-01-01T00:05:00Z", "glucose_mg_dL": 125},
            ],
        }
    ]
    (tmp_path / "patient-1.json").write_text(json.dumps(data))

    source = JsonDirectorySource(tmp_path)
    days = list(source.iter_days("patient-1"))

    assert len(days) == 1
    day = days[0]
    assert isinstance(day, CGMDay)
    assert day.service_date.isoformat() == "2024-01-01"
    assert isinstance(day.readings, pd.DataFrame)
    assert len(day.readings) == 2


def test_callable_source_iter_days():
    def fetcher(patient_id: str):
        yield {
            "service_date": "2024-02-01",
            "readings": [
                {"timestamp": "2024-02-01T00:00:00Z", "glucose_mg_dL": 100},
                {"timestamp": "2024-02-01T00:05:00Z", "glucose_mg_dL": 110},
            ],
        }

    source = CallableSource(fetcher)
    days = list(source.iter_days("patient-2"))

    assert len(days) == 1
    assert days[0].service_date.isoformat() == "2024-02-01"


def test_detection_to_dict_serializes_fields():
    detection = PatternDetection(
        pattern_id="rule",
        effective_date=date(2024, 1, 1),
        status=PatternStatus.DETECTED,
        evidence={"example": 1},
        metrics={"metric": 0.5},
        confidence=0.9,
        version="1.0",
    )
    payload = _detection_to_dict(detection)
    assert payload["pattern_id"] == "rule"
    assert payload["status"] == "detected"
    assert payload["effective_date"] == "2024-01-01"
    assert payload["metrics"] == {"metric": 0.5}
    assert payload["evidence"] == {"example": 1}
