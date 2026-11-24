"""Great Expectations-style data-contract validation (no GE dependency).

A contract is plain JSON/dicts, e.g.:
  {"expected_columns": {"id": "long", "event_time": "timestamp"},
   "required_columns": ["id"], "max_null_rate": {"amount": 0.01},
   "min_rows": 100, "freshness": {"column": "event_time", "max_age_seconds": 86400},
   "unique_columns": ["id"]}
Each expectation becomes one named check; the report's pass_rate is the
share of checks passing. Nothing here connects to Great Expectations —
it borrows the *style* (expectations -> check results -> suite verdict).
"""
from __future__ import annotations
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import pyarrow as pa
from .schema import canonical_type, type_is_compatible

@dataclass
class CheckResult:
    name: str; passed: bool; observed: object; threshold: object; details: str = ""
    def to_dict(self): return asdict(self)

@dataclass
class ValidationReport:
    checks: list[CheckResult] = field(default_factory=list)
    row_count: int = 0
    @property
    def passed(self): return all(c.passed for c in self.checks) if self.checks else False
    @property
    def pass_rate(self): return round(sum(c.passed for c in self.checks) / len(self.checks), 4) if self.checks else 0.0
    def to_dict(self):
        return {"passed": self.passed, "pass_rate": self.pass_rate, "row_count": self.row_count,
                "checks": [c.to_dict() for c in self.checks]}

@dataclass
class Contract:
    expected_columns: dict = field(default_factory=dict)
    required_columns: list = field(default_factory=list)
    max_null_rate: dict = field(default_factory=dict)
    min_rows: int = 0
    freshness: dict | None = None
    unique_columns: list = field(default_factory=list)
    @classmethod
    def from_dict(cls, d): return cls(d.get("expected_columns", {}), d.get("required_columns", []),
                                     d.get("max_null_rate", {}), d.get("min_rows", 0),
                                     d.get("freshness"), d.get("unique_columns", []))

def _to_ts(v):
    if isinstance(v, datetime): return v.timestamp()
    if isinstance(v, (int, float)): return float(v)
    try: return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except Exception: return None

def validate_table(table: pa.Table, contract: Contract, now: float | None = None) -> ValidationReport:
    now = time.time() if now is None else now
    checks: list[CheckResult] = []; n = table.num_rows
    # completeness: row count + required / expected presence
    checks.append(CheckResult("completeness.min_rows", n >= contract.min_rows, n, contract.min_rows))
    for col in contract.required_columns:
        checks.append(CheckResult(f"completeness.column_present:{col}", col in table.column_names,
                                  col in table.column_names, True))
    for col, expected in contract.expected_columns.items():
        if col not in table.column_names:
            checks.append(CheckResult(f"type_drift:{col}", False, "missing", expected)); continue
        actual = canonical_type(table.schema.field(col).type)
        checks.append(CheckResult(f"type_drift:{col}", type_is_compatible(expected, actual), actual, expected))
    # null rates
    for col, max_rate in contract.max_null_rate.items():
        if col not in table.column_names:
            checks.append(CheckResult(f"null_rate:{col}", False, "missing", max_rate)); continue
        rate = round(table.column(col).null_count / n, 6) if n else 0.0
        checks.append(CheckResult(f"null_rate:{col}", rate <= max_rate, rate, max_rate))
    # uniqueness
    for col in contract.unique_columns:
        if col not in table.column_names:
            checks.append(CheckResult(f"unique:{col}", False, "missing", "unique")); continue
        vals = table.column(col).to_pylist(); nonnull = [v for v in vals if v is not None]
        dups = len(nonnull) - len(set(nonnull))
        checks.append(CheckResult(f"unique:{col}", dups == 0, dups, 0, "duplicate count"))
    # freshness
    if contract.freshness:
        col = contract.freshness["column"]; max_age = contract.freshness["max_age_seconds"]
        if col not in table.column_names or n == 0:
            checks.append(CheckResult("freshness", False, "missing/empty", max_age))
        else:
            stamps = [s for s in (_to_ts(v) for v in table.column(col).to_pylist()) if s is not None]
            if not stamps: checks.append(CheckResult("freshness", False, "no parseable timestamps", max_age))
            else:
                age = round(now - max(stamps), 3)
                checks.append(CheckResult("freshness", age <= max_age, age, max_age,
                                          f"newest {datetime.fromtimestamp(max(stamps), timezone.utc).isoformat()}"))
    return ValidationReport(checks, n)
