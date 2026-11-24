"""Benchmark: read time + file count before/after compaction, contract pass rate."""
from __future__ import annotations
import json, statistics, tempfile, time
from pathlib import Path
from .catalog import TableCatalog
from .compaction import compact
from .schema import Schema
from .synthetic import EVENTS_SCHEMA, generate_events
from .validation import Contract, validate_table

DEFAULT_CONTRACT = {
    "expected_columns": {"event_id": "long", "user_id": "long", "event_type": "string",
                         "amount": "double", "event_date": "string", "event_time": "timestamp"},
    "required_columns": ["event_id", "event_time"],
    "max_null_rate": {"amount": 0.05, "event_id": 0.0},
    "unique_columns": ["event_id"], "min_rows": 1,
    "freshness": {"column": "event_time", "max_age_seconds": 7200},
}

def _timed_read(cat, repeats=5):
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter(); table = cat.read(); samples.append(time.perf_counter() - t0)
        assert table.num_rows > 0
    return statistics.median(samples), table

def run_benchmark(rows: int = 30_000, files: int = 60, target_bytes: int = 256 * 1024,
                  seed: int = 42, repeats: int = 5) -> dict:
    with tempfile.TemporaryDirectory(prefix="guardian-bench-") as tmp:
        root = Path(tmp) / "events"
        cat = TableCatalog.create(root, Schema.from_json_obj(EVENTS_SCHEMA), partition_cols=["event_date"])
        events = generate_events(rows, seed=seed)
        per = rows // files
        for i in range(files):
            chunk = events.slice(i * per, per if i < files - 1 else rows - i * per)
            cat.append(chunk)  # one parquet file per append -> fragmentation
        before_files = len(cat.live_files()); before_bytes = sum(f.file_size_bytes for f in cat.live_files())
        before_s, _ = _timed_read(cat, repeats)
        result = compact(cat, target_file_size_bytes=target_bytes)
        after_files = len(cat.live_files()); after_bytes = sum(f.file_size_bytes for f in cat.live_files())
        after_s, table = _timed_read(cat, repeats)
        report = validate_table(table, Contract.from_dict(DEFAULT_CONTRACT))
        return {
            "scale": {"rows": rows, "ingest_files": files, "target_file_bytes": target_bytes,
                      "seed": seed, "repeats_median_of": repeats, "note": "local temp dir, synthetic data — illustrative, not a cluster benchmark"},
            "before": {"files": before_files, "bytes": before_bytes, "median_read_s": round(before_s, 6)},
            "after": {"files": after_files, "bytes": after_bytes, "median_read_s": round(after_s, 6)},
            "compaction": {k: v for k, v in result.to_dict().items() if k != "plan"} | {"files_to_rewrite": result.plan["files_to_rewrite"]},
            "file_reduction_ratio": round(before_files / after_files, 3) if after_files else None,
            "read_speedup_x": round(before_s / after_s, 3) if after_s else None,
            "rows_preserved": table.num_rows == rows,
            "contract": {"passed": report.passed, "pass_rate": report.pass_rate,
                         "checks": len(report.checks), "failed": [c.name for c in report.checks if not c.passed]},
        }

if __name__ == "__main__":
    print(json.dumps(run_benchmark(), indent=2))
