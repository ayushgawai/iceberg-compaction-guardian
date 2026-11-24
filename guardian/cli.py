"""CLI: init / ingest / compact / validate / time-travel / status — all JSON out."""
from __future__ import annotations
import argparse, csv, json, sys
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as pacsv
from .catalog import TableCatalog
from .compaction import compact, plan_compaction
from .partition import prune_files
from .schema import Schema, evolve_schema
from .synthetic import EVENTS_SCHEMA, generate_events
from .validation import Contract, validate_table

def _out(obj): print(json.dumps(obj, indent=2, default=str)); return 0

def _open(args):
    return TableCatalog.open(args.table)

def cmd_init(args):
    schema = Schema.from_json_obj(json.loads(Path(args.schema).read_text())) if args.schema else Schema.from_json_obj(EVENTS_SCHEMA)
    parts = args.partition_by.split(",") if args.partition_by else []
    cat = TableCatalog.create(args.table, schema, parts)
    return _out({"table": args.table, "snapshot_id": cat.current_snapshot_id, "schema": schema.to_dict(), "partition_cols": parts})

def cmd_ingest(args):
    cat = _open(args) if Path(args.table, "metadata", "catalog.json").exists() else None
    if args.input:
        table = pacsv.read_csv(args.input)
        if cat is None:
            raise SystemExit("ingest --input needs an existing table: run `init` first")
    else:
        if cat is None:
            cat = TableCatalog.create(args.table, Schema.from_json_obj(EVENTS_SCHEMA), ["event_date"])
        # continue ids after existing rows so uniqueness contracts can pass
        existing = cat.current_snapshot().summary.get("record_count", 0) if cat.current_snapshot() else 0
        table = generate_events(args.rows, seed=args.seed, start_id=int(existing), null_rate=args.null_rate)
    snap = cat.append(table, chunks=args.files)
    return _out({"snapshot_id": snap.snapshot_id, "operation": snap.operation, "summary": snap.summary,
                 "live_files": len(cat.live_files())})

def cmd_compact(args):
    cat = _open(args)
    if args.plan_only:
        return _out(plan_compaction(cat.live_files(), args.target_bytes).to_dict())
    res = compact(cat, target_file_size_bytes=args.target_bytes)
    return _out({**res.to_dict(), "snapshot_id": cat.current_snapshot_id})

def cmd_validate(args):
    cat = _open(args)
    contract_obj = json.loads(Path(args.contract).read_text()) if args.contract else None
    if contract_obj is None:
        from .benchmark import DEFAULT_CONTRACT; contract_obj = DEFAULT_CONTRACT
    table = cat.read(snapshot_id=args.snapshot)
    report = validate_table(table, Contract.from_dict(contract_obj))
    print(json.dumps(report.to_dict(), indent=2))
    return 0 if report.passed else 1

def cmd_time_travel(args):
    cat = _open(args)
    if args.list or not args.snapshot:
        return _out({"current_snapshot_id": cat.current_snapshot_id,
                     "snapshots": [s.to_dict() for s in cat.list_snapshots()],
                     "txn_log_entries": len(cat.txn_log())})
    if args.rollback:
        snap = cat.rollback_to(args.snapshot)
        return _out({"rolled_back_to": args.snapshot, "new_snapshot_id": snap.snapshot_id, "summary": snap.summary})
    files = cat.files_at(args.snapshot)
    table = cat.read(snapshot_id=args.snapshot)
    return _out({"snapshot_id": args.snapshot, "rows": table.num_rows, "files": len(files),
                 "columns": table.column_names})

def cmd_status(args):
    cat = _open(args); files = cat.live_files()
    snap = cat.current_snapshot()
    pruned = None
    if args.prune_column:
        r = prune_files(files, [(args.prune_column, args.prune_op, args.prune_value)])
        pruned = {"kept": r.kept_count, "pruned": r.pruned_count}
    return _out({"table": args.table, "current_snapshot_id": cat.current_snapshot_id,
                 "operation": snap.operation if snap else None,
                 "rows": snap.summary.get("record_count") if snap else 0,
                 "live_files": len(files), "live_bytes": sum(f.file_size_bytes for f in files),
                 "partition_cols": cat.partition_cols, "schema": cat.current_schema().to_dict(),
                 "prune_preview": pruned})

def cmd_evolve(args):
    cat = _open(args)
    ops = json.loads(Path(args.ops).read_text()) if args.ops else json.loads(args.ops_json)
    snap = cat.evolve(evolve_schema(cat.current_schema(), ops))
    return _out({"snapshot_id": snap.snapshot_id, "schema": cat.current_schema().to_dict()})

def build_parser():
    p = argparse.ArgumentParser(prog="guardian", description="Offline Iceberg-style compaction & data-quality guardian")
    p.add_argument("--table", default="table", help="table directory")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("init"); s.add_argument("--schema"); s.add_argument("--partition-by", default="event_date"); s.set_defaults(fn=cmd_init)
    s = sub.add_parser("ingest"); s.add_argument("--rows", type=int, default=1000); s.add_argument("--files", type=int, default=10)
    s.add_argument("--seed", type=int, default=42); s.add_argument("--null-rate", type=float, default=0.0); s.add_argument("--input"); s.set_defaults(fn=cmd_ingest)
    s = sub.add_parser("compact"); s.add_argument("--target-bytes", type=int, default=262144); s.add_argument("--plan-only", action="store_true"); s.set_defaults(fn=cmd_compact)
    s = sub.add_parser("validate"); s.add_argument("--contract"); s.add_argument("--snapshot", type=int); s.set_defaults(fn=cmd_validate)
    s = sub.add_parser("time-travel"); s.add_argument("--snapshot", type=int); s.add_argument("--list", action="store_true")
    s.add_argument("--rollback", action="store_true"); s.set_defaults(fn=cmd_time_travel)
    s = sub.add_parser("status"); s.add_argument("--prune-column"); s.add_argument("--prune-op", default="=")
    s.add_argument("--prune-value", default=""); s.set_defaults(fn=cmd_status)
    s = sub.add_parser("evolve"); s.add_argument("--ops"); s.add_argument("--ops-json", default="[]"); s.set_defaults(fn=cmd_evolve)
    return p

def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except (FileNotFoundError, FileExistsError, KeyError, ValueError) as e:
        print(json.dumps({"error": type(e).__name__, "message": str(e)})); return 2

if __name__ == "__main__":
    sys.exit(main())
