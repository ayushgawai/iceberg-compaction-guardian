import time
from datetime import datetime, timezone, timedelta
import pyarrow as pa
from guardian.synthetic import generate_events
from guardian.validation import Contract, validate_table

CONTRACT = {"expected_columns": {"event_id": "long", "amount": "double", "event_time": "timestamp"},
            "required_columns": ["event_id"], "max_null_rate": {"amount": 0.05},
            "unique_columns": ["event_id"], "min_rows": 10,
            "freshness": {"column": "event_time", "max_age_seconds": 7200}}

def test_clean_data_passes():
    report = validate_table(generate_events(100, seed=1), Contract.from_dict(CONTRACT))
    assert report.passed, [c for c in report.checks if not c.passed]
    assert report.pass_rate == 1.0

def test_null_rate_and_duplicates_fail():
    t = generate_events(100, seed=1, null_rate=0.5)
    dup = pa.concat_tables([t, t.slice(0, 5)])
    report = validate_table(dup, Contract.from_dict(CONTRACT))
    failed = {c.name for c in report.checks if not c.passed}
    assert "null_rate:amount" in failed and "unique:event_id" in failed
    assert not report.passed and 0 < report.pass_rate < 1

def test_stale_data_fails_freshness():
    old = datetime.now(timezone.utc) - timedelta(days=30)
    report = validate_table(generate_events(20, seed=1, base_time=old), Contract.from_dict(CONTRACT))
    assert not [c for c in report.checks if c.name == "freshness"][0].passed

def test_type_drift_detected():
    t = generate_events(20, seed=1).drop_columns(["amount"]).append_column(
        "amount", pa.array(["x"] * 20))
    report = validate_table(t, Contract.from_dict(CONTRACT))
    assert not [c for c in report.checks if c.name == "type_drift:amount"][0].passed
