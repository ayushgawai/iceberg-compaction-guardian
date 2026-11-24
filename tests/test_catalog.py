import json
import pyarrow as pa
import pytest
from guardian.catalog import TableCatalog
from guardian.schema import Schema, evolve_schema, SchemaEvolutionError
from guardian.synthetic import EVENTS_SCHEMA, generate_events

@pytest.fixture()
def cat(tmp_path):
    return TableCatalog.create(tmp_path / "t", Schema.from_json_obj(EVENTS_SCHEMA), ["event_date"])

def test_create_append_read(cat):
    snap = cat.append(generate_events(100, seed=1), chunks=4)
    assert snap.operation == "append" and len(cat.live_files()) == 4
    assert cat.read().num_rows == 100
    assert cat.current_snapshot().summary["record_count"] == 100
    # reopen from disk: catalog survives round-trip
    again = TableCatalog.open(cat.root)
    assert again.read().num_rows == 100 and len(again.txn_log()) == 2  # create + append

def test_time_travel_and_rollback(cat):
    cat.append(generate_events(50, seed=1), chunks=2)
    first = cat.current_snapshot_id
    cat.append(generate_events(50, seed=2, start_id=50), chunks=2)
    assert cat.read().num_rows == 100
    assert cat.read(snapshot_id=first).num_rows == 50
    snap = cat.rollback_to(first)
    assert snap.operation == "rollback" and cat.read().num_rows == 50

def test_unknown_snapshot(cat):
    with pytest.raises(KeyError):
        cat.read(snapshot_id=999)

def test_schema_evolution_add_column_and_read_old_files(cat):
    cat.append(generate_events(20, seed=1))
    new = evolve_schema(cat.current_schema(), [{"op": "add", "name": "source", "type": "string", "default": "web"}])
    cat.evolve(new)
    t = cat.read()
    assert "source" in t.column_names and set(t.column("source").to_pylist()) == {"web"}

def test_schema_evolution_rejects_narrowing(cat):
    with pytest.raises(SchemaEvolutionError):
        evolve_schema(cat.current_schema(), [{"op": "widen", "name": "amount", "type": "float"}])
    ok = evolve_schema(cat.current_schema(), [{"op": "widen", "name": "user_id", "type": "double"}])
    assert ok.get("user_id").type == "double"

def test_append_rejects_missing_required(tmp_path):
    schema = Schema.from_json_obj([{"name": "event_id", "type": "long", "nullable": False},
                                   {"name": "amount", "type": "double"}])
    cat = TableCatalog.create(tmp_path / "req", schema, [])
    with pytest.raises(ValueError):
        cat.append(pa.table({"amount": [1.0, 2.0]}))
