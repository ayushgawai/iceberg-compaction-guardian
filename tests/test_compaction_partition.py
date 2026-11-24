import pyarrow as pa
from guardian.catalog import TableCatalog
from guardian.compaction import compact, plan_compaction
from guardian.partition import prune_files
from guardian.schema import Schema
from guardian.synthetic import EVENTS_SCHEMA, generate_events

def _fragmented(tmp_path, rows=2000, files=20):
    cat = TableCatalog.create(tmp_path / "t", Schema.from_json_obj(EVENTS_SCHEMA), ["event_date"])
    cat.append(generate_events(rows, seed=3), chunks=files)
    return cat

def test_compaction_reduces_files_and_preserves_rows(tmp_path):
    cat = _fragmented(tmp_path)
    res = compact(cat, target_file_size_bytes=64 * 1024)
    assert res.after_files < res.before_files
    assert cat.read().num_rows == 2000
    assert res.rows == 2000
    assert cat.current_snapshot().operation == "compact"

def test_plan_is_noop_for_single_healthy_file(tmp_path):
    cat = TableCatalog.create(tmp_path / "t", Schema.from_json_obj(EVENTS_SCHEMA), ["event_date"])
    big = pa.table({"event_id": pa.array(range(5000), pa.int64())})
    # use synthetic events but as one file, plan with huge target: still needs >=2 bins to rewrite
    cat.append(generate_events(500, seed=1), chunks=1)
    plan = plan_compaction(cat.live_files(), target_file_size_bytes=1024 * 1024)
    assert plan.groups == []

def test_partition_pruning_keeps_only_matching_files(tmp_path):
    cat = _fragmented(tmp_path, rows=500, files=5)
    files = cat.live_files()
    dates = {f.partition["event_date"] for f in files}
    target = sorted(dates)[0]
    res = prune_files(files, [("event_date", "=", target)])
    assert res.kept and all(f.partition["event_date"] == target for f in res.kept)
    assert res.kept_count + res.pruned_count == len(files)

def test_pruning_unknown_column_keeps_everything(tmp_path):
    cat = _fragmented(tmp_path, rows=100, files=3)
    res = prune_files(cat.live_files(), [("no_such_col", "=", "x")])
    assert res.pruned_count == 0
