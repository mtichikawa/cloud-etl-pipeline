"""
src/validate.py — In-pipeline data quality validation layer.

Runs between transform and load. Splits a DataFrame into (clean, quarantined)
based on a list of Rules, with severity-aware behavior:

  - severity='error' → failing rows are quarantined (NOT loaded to main path)
  - severity='warn'  → failing rows are counted in the report but still loaded

Rules are pure functions over a pandas Series; they return a boolean mask
where True == valid. This keeps individual rules easy to test in isolation
and makes the validator agnostic to the schema being checked.

Usage:
    from src.validate import RecordValidator, standard_weather_rules

    validator = RecordValidator(standard_weather_rules())
    clean_df, quarantined_df, report = validator.apply(transformed_df)

The report is a dict suitable for inclusion in DynamoDB run metadata or for
emission as a JSON sidecar alongside the quarantine Parquet.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, List, Tuple

import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

log = logging.getLogger("validate")


# ── Rule dataclass ─────────────────────────────────────────────────────────────

@dataclass
class Rule:
    """
    One validation rule.

    Attributes:
        name: short identifier shown in the report (e.g. 'temp_range').
        column: column the rule is checked against.
        check: Callable(Series) -> Series[bool], where True means VALID.
        severity: 'error' (failures are quarantined) or 'warn' (failures
                  are reported but the rows still proceed to load).
    """
    name: str
    column: str
    check: Callable[[pd.Series], pd.Series]
    severity: str = "error"


# ── RecordValidator ────────────────────────────────────────────────────────────

class RecordValidator:
    """
    Apply a list of Rules to a DataFrame, returning (clean, quarantined, report).

    The validator is intentionally schema-agnostic: pass it a list of rules
    that match the columns of the DataFrame you give it. Rules referencing
    columns that aren't present in the DataFrame are silently skipped (the
    upstream schema check handles missing-column errors separately).
    """

    def __init__(self, rules: List[Rule]):
        self.rules = rules

    def apply(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
        if df.empty:
            return (
                df.copy(),
                df.copy(),
                {
                    "row_count": 0,
                    "passed": 0,
                    "quarantined": 0,
                    "rule_failures": {},
                    "rule_severities": {},
                },
            )

        valid_mask = pd.Series(True, index=df.index)
        rule_failures: dict[str, int] = {}
        rule_severities: dict[str, str] = {}

        for rule in self.rules:
            rule_severities[rule.name] = rule.severity

            if rule.column not in df.columns:
                # Skip silently — upstream schema validation handles missing
                # columns. We don't want to false-quarantine here.
                continue

            try:
                mask = rule.check(df[rule.column])
            except Exception as e:
                # A bad rule shouldn't crash the pipeline. Log it and skip.
                log.warning(
                    f"Rule '{rule.name}' raised {type(e).__name__}: {e} — "
                    f"treating column as all-valid for this run."
                )
                rule_failures[rule.name] = 0
                continue

            failed = int((~mask).sum())
            rule_failures[rule.name] = failed

            if rule.severity == "error":
                valid_mask &= mask

        clean_df = df[valid_mask].copy()
        quarantined_df = df[~valid_mask].copy()

        return (
            clean_df,
            quarantined_df,
            {
                "row_count": len(df),
                "passed": len(clean_df),
                "quarantined": len(quarantined_df),
                "rule_failures": rule_failures,
                "rule_severities": rule_severities,
            },
        )


# ── Standard weather rules ─────────────────────────────────────────────────────

def standard_weather_rules() -> List[Rule]:
    """
    Default rule set for the weather ETL pipeline.

    Rules are tuned for the post-transform shape (city, date, avg/max/min
    temp_c, precipitation_mm, wind_speed_max_kmh, run_id, plus derived
    extreme-event flags).

    Severity choices:
      - error: physical impossibilities (negative precipitation, temp out
        of plausible range, malformed date) — these usually mean a bad
        upstream record, not a real datum, so quarantine.
      - warn:  city not in our seeded list — could be legitimate (someone
        added a city upstream) or an error, but either way we'd rather
        load it and see than drop it.
    """
    known_cities = set(config.CITY_NAMES)

    return [
        Rule(
            name="temp_range",
            column="avg_temp_c",
            check=lambda s: s.between(-90, 60),
            severity="error",
        ),
        Rule(
            name="precip_non_negative",
            column="precipitation_mm",
            check=lambda s: s.fillna(0) >= 0,
            severity="error",
        ),
        Rule(
            name="wind_non_negative",
            column="wind_speed_max_kmh",
            check=lambda s: s.fillna(0) >= 0,
            severity="error",
        ),
        Rule(
            name="date_iso_format",
            column="date",
            check=lambda s: s.astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$"),
            severity="error",
        ),
        Rule(
            name="city_known",
            column="city",
            check=lambda s: s.isin(known_cities),
            severity="warn",
        ),
    ]
