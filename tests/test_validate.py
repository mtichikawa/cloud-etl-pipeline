"""
tests/test_validate.py — Unit tests for the data-quality validation layer.
No AWS dependencies.
"""

import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.validate import RecordValidator, Rule, standard_weather_rules


# ── Helpers ───────────────────────────────────────────────────────────────────

def _good_row(**overrides) -> dict:
    base = {
        "city": "New York",
        "date": "2026-04-28",
        "avg_temp_c": 12.0,
        "max_temp_c": 18.0,
        "min_temp_c": 6.0,
        "precipitation_mm": 0.5,
        "wind_speed_max_kmh": 14.0,
        "run_id": "test1234",
        "is_extreme_temp": False,
        "is_heavy_rain": False,
        "is_high_wind": False,
    }
    base.update(overrides)
    return base


# ── Empty input ────────────────────────────────────────────────────────────────

def test_empty_dataframe_returns_empty_clean_and_quarantined():
    validator = RecordValidator(standard_weather_rules())
    clean, quarantined, report = validator.apply(pd.DataFrame())
    assert clean.empty
    assert quarantined.empty
    assert report["row_count"] == 0
    assert report["passed"] == 0
    assert report["quarantined"] == 0
    assert report["rule_failures"] == {}


# ── All-valid input ────────────────────────────────────────────────────────────

def test_all_valid_input_returns_full_clean_empty_quarantined():
    df = pd.DataFrame([_good_row(), _good_row(city="Chicago"), _good_row(city="Portland")])
    validator = RecordValidator(standard_weather_rules())
    clean, quarantined, report = validator.apply(df)

    assert len(clean) == 3
    assert len(quarantined) == 0
    assert report["passed"] == 3
    assert report["quarantined"] == 0


# ── Mixed input ────────────────────────────────────────────────────────────────

def test_mixed_valid_invalid_splits_correctly():
    df = pd.DataFrame([
        _good_row(),                                  # valid
        _good_row(precipitation_mm=-5.0),             # negative precip → quarantine
        _good_row(avg_temp_c=200.0),                  # temp out of range → quarantine
        _good_row(date="not-a-date"),                 # malformed date → quarantine
        _good_row(),                                  # valid
    ])
    validator = RecordValidator(standard_weather_rules())
    clean, quarantined, report = validator.apply(df)

    assert len(clean) == 2
    assert len(quarantined) == 3
    assert report["rule_failures"]["precip_non_negative"] == 1
    assert report["rule_failures"]["temp_range"] == 1
    assert report["rule_failures"]["date_iso_format"] == 1


# ── Missing column is silently skipped ─────────────────────────────────────────

def test_rule_for_missing_column_is_skipped():
    df = pd.DataFrame([{"city": "Houston", "date": "2026-04-28"}])
    validator = RecordValidator(standard_weather_rules())
    clean, quarantined, report = validator.apply(df)

    # No avg_temp_c column → temp_range rule skipped, no quarantine from it
    assert len(clean) == 1
    assert len(quarantined) == 0


# ── Warn-severity rules don't quarantine ───────────────────────────────────────

def test_warn_severity_does_not_quarantine():
    df = pd.DataFrame([
        _good_row(city="Atlantis"),       # unknown city → warn-only
        _good_row(),                      # known city
    ])
    validator = RecordValidator(standard_weather_rules())
    clean, quarantined, report = validator.apply(df)

    # Both rows clean despite Atlantis failing city_known (severity='warn')
    assert len(clean) == 2
    assert len(quarantined) == 0
    # But the failure IS counted in the report
    assert report["rule_failures"]["city_known"] == 1
    assert report["rule_severities"]["city_known"] == "warn"


# ── Standard weather rules catch each kind of bad data ─────────────────────────

@pytest.mark.parametrize(
    "field,bad_value,expected_failing_rule",
    [
        ("avg_temp_c", -150.0, "temp_range"),
        ("avg_temp_c", 75.0, "temp_range"),
        ("precipitation_mm", -1.0, "precip_non_negative"),
        ("wind_speed_max_kmh", -10.0, "wind_non_negative"),
        ("date", "20260428", "date_iso_format"),
        ("date", "2026/04/28", "date_iso_format"),
    ],
)
def test_standard_rules_catch_bad_values(field, bad_value, expected_failing_rule):
    df = pd.DataFrame([_good_row(**{field: bad_value})])
    validator = RecordValidator(standard_weather_rules())
    clean, quarantined, report = validator.apply(df)

    assert len(clean) == 0
    assert len(quarantined) == 1
    assert report["rule_failures"][expected_failing_rule] == 1


# ── A buggy rule doesn't crash the pipeline ────────────────────────────────────

def test_buggy_rule_logged_and_skipped():
    df = pd.DataFrame([_good_row()])

    def explosive_check(s):
        raise RuntimeError("simulated rule bug")

    rules = [Rule("buggy", "city", explosive_check, severity="error")]
    validator = RecordValidator(rules)
    clean, quarantined, report = validator.apply(df)

    # Bad rule shouldn't quarantine; treated as all-valid for safety
    assert len(clean) == 1
    assert len(quarantined) == 0
    assert report["rule_failures"]["buggy"] == 0
