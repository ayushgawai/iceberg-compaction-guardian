"""Versioned table catalog: snapshots, commits and an ordered txn log.

On-disk layout (deliberately boring, inspectable JSON + Parquet):
  <table>/metadata/catalog.json     current pointer + all snapshots/schemas
  <table>/metadata/txn_log.jsonl    one JSON object per commit, in order
  <table>/data/*.parquet            data files (never mutated in place)

A commit writes new data files first, then appends to the txn log, then
atomically replaces catalog.json (tmp file + os.replace). Readers only ever
follow catalog.json, so a crash mid-commit leaves the old snapshot visible.
This is an emulation of Iceberg's model — not Iceberg itself (see README).
"""
from __future__ import annotations
import json, os, time, uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from .schema import Schema, Field

def _now_ms(): return int(time.time() * 1000)

@dataclass
class DataFile:
    path: str                      # file name only, relative to data/
    record_count: int
    file_size_bytes: int
    partition: dict = field(default_factory=dict)
    column_stats: dict = field(default_factory=dict)  # col -> {min,max,null_count}
    def to_dict(self): return asdict(self)
    @classmethod
    def from_dict(cls, d): return cls(**d)

@dataclass
class Snapshot:
    snapshot_id: int
    parent_id: int | None
    timestamp_ms: int
    operation: str                 # append | compact | rollback | create
    added_files: list[str] = field(default_factory=list)
    removed_files: list[str] = field(default_factory=list)
    schema_id: int = 0
    summary: dict = field(default_factory=dict)
    def to_dict(self): return asdict(self)
    @classmethod
    def from_dict(cls, d): return cls(**d)

def describe_table(table: pa.Table, partition_cols: list[str]) -> tuple[dict, dict]:
    """Partition values (first row, assuming ingest groups by partition) + stats."""
    stats, part = {}, {}
    for name in table.column_names:
        col = table.column(name)
        try:
            arr = col.combine_chunks()
            stats[name] = {"null_count": arr.null_count}
            if len(arr) and pa.types.is_number(arr.type) or (len(arr) and pa.types.is_timestamp(arr.type)):
                vals = arr.to_pylist()
                nonnull = [v for v in vals if v is not None]
                if nonnull:
                    stats[name]["min"] = str(min(nonnull)); stats[name]["max"] = str(max(nonnull))
        except Exception:
            stats[name] = {"null_count": 0}
    if partition_cols and table.num_rows:
        for c in partition_cols:
            if c in table.column_names:
                part[c] = str(table.column(c)[0].as_py())
    return part, stats

class TableCatalog:
    def __init__(self, root: str | Path):
        self.root = Path(root); self.data_dir = self.root / "data"; self.meta_dir = self.root / "metadata"
        self.catalog_path = self.meta_dir / "catalog.json"; self.txn_path = self.meta_dir / "txn_log.jsonl"
        self._load()

    # -- lifecycle ---------------------------------------------------------
    @classmethod
    def create(cls, root, schema: Schema, partition_cols: list[str] | None = None) -> "TableCatalog":
        root = Path(root)
        if (root / "metadata" / "catalog.json").exists():
            raise FileExistsError(f"table already exists at {root}")
        (root / "data").mkdir(parents=True, exist_ok=True); (root / "metadata").mkdir(parents=True, exist_ok=True)
        cat = cls.__new__(cls); cat.root = root; cat.data_dir = root/"data"; cat.meta_dir = root/"metadata"
        cat.catalog_path = cat.meta_dir/"catalog.json"; cat.txn_path = cat.meta_dir/"txn_log.jsonl"
        cat.schemas = {schema.schema_id: schema}; cat.partition_cols = partition_cols or []
        cat.snapshots: dict[int, Snapshot] = {}; cat.files: dict[str, DataFile] = {}
        cat.current_snapshot_id = None; cat._next_snapshot = 1
        cat._commit(operation="create", added=[], removed=[], schema_id=schema.schema_id, summary={"record_count": 0})
        return cat

    @classmethod
    def open(cls, root) -> "TableCatalog": return cls(root)

    def _load(self):
        if not self.catalog_path.exists(): raise FileNotFoundError(f"no table at {self.root} (missing metadata/catalog.json)")
        d = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        self.schemas = {int(k): Schema.from_dict(v) for k, v in d["schemas"].items()}
        self.partition_cols = d.get("partition_cols", [])
        self.snapshots = {int(k): Snapshot.from_dict(v) for k, v in d["snapshots"].items()}
        self.files = {k: DataFile.from_dict(v) for k, v in d["files"].items()}
        self.current_snapshot_id = d["current_snapshot_id"]; self._next_snapshot = d["next_snapshot_id"]

    def _persist(self, txn_entry: dict):
        payload = {"format": "guardian-catalog/1", "schemas": {str(k): v.to_dict() for k, v in self.schemas.items()},
                   "partition_cols": self.partition_cols,
                   "snapshots": {str(k): v.to_dict() for k, v in self.snapshots.items()},
                   "files": {k: v.to_dict() for k, v in self.files.items()},
                   "current_snapshot_id": self.current_snapshot_id, "next_snapshot_id": self._next_snapshot}
        with open(self.txn_path, "a", encoding="utf-8") as fh: fh.write(json.dumps(txn_entry) + "\n")
        tmp = self.catalog_path.with_suffix(".json.tmp"); tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, self.catalog_path)

    def _commit(self, operation, added, removed, schema_id, summary) -> Snapshot:
        sid = self._next_snapshot; self._next_snapshot += 1
        snap = Snapshot(sid, self.current_snapshot_id, _now_ms(), operation,
                        [f.path for f in added], [f.path for f in removed], schema_id, summary)
        for f in added: self.files[f.path] = f
        for name in snap.removed_files: self.files.pop(name, None)
        self.snapshots[sid] = snap; self.current_snapshot_id = sid
        self._persist({"snapshot_id": sid, "parent_id": snap.parent_id, "timestamp_ms": snap.timestamp_ms,
                       "operation": operation, "added": snap.added_files, "removed": snap.removed_files,
                       "schema_id": schema_id, "summary": summary})
        return snap

    # -- writes ------------------------------------------------------------
    def write_data_file(self, table: pa.Table) -> DataFile:
        name = f"data-{uuid.uuid4().hex[:12]}.parquet"; path = self.data_dir / name
        pq.write_table(table, path)
        part, stats = describe_table(table, self.partition_cols)
        df = DataFile(name, table.num_rows, path.stat().st_size, part, stats)
        self._remember_files([df])
        return df

    def append(self, table: pa.Table, chunks: int = 1) -> Snapshot:
        """Append a table, optionally split into `chunks` small files (to emulate fragmentation)."""
        if table.num_rows == 0: raise ValueError("cannot append empty table")
        schema = self.current_schema()
        missing = [n for n in schema.names() if n not in table.column_names]
        # allow missing columns only if they have a default / are nullable (schema evolution)
        bad = [n for n in missing if not schema.get(n).nullable and schema.get(n).default is None]
        if bad: raise ValueError(f"missing required columns: {bad}")
        added, n = [], table.num_rows
        bounds = [(i * n // chunks, (i + 1) * n // chunks) for i in range(chunks)]
        for lo, hi in bounds:
            if hi > lo: added.append(self.write_data_file(table.slice(lo, hi - lo)))
        total = sum(f.record_count for f in self.files.values()) + sum(f.record_count for f in added)
        return self._commit("append", added, [], schema.schema_id,
                            {"record_count": total, "added_records": sum(f.record_count for f in added), "file_count": len(self.files) + len(added)})

    def commit_compaction(self, added: list[DataFile], removed: list[DataFile]) -> Snapshot:
        total = sum(f.record_count for f in self.files.values()) - sum(f.record_count for f in removed) + sum(f.record_count for f in added)
        schema = self.current_schema()
        remaining = len(self.files) - len(removed) + len(added)
        return self._commit("compact", added, removed, schema.schema_id,
                            {"record_count": total, "file_count": remaining,
                             "removed_files": len(removed), "added_files": len(added)})

    def evolve(self, new_schema: Schema) -> Snapshot:
        self.schemas[new_schema.schema_id] = new_schema
        return self._commit("schema-evolution", [], [], new_schema.schema_id,
                            {"record_count": self.current_snapshot().summary.get("record_count", 0) if self.current_snapshot() else 0})

    def rollback_to(self, snapshot_id: int) -> Snapshot:
        """Time-travel write: make the file set at `snapshot_id` current again.

        Implemented honestly as a new snapshot whose file set is recomputed by
        replaying the txn history up to that snapshot — old files are not
        deleted, so rolling forward again is still possible in this toy model.
        """
        target_files = self.files_at(snapshot_id)
        cur = set(self.files); want = {f.path for f in target_files}
        removed = [self.files[p] for p in cur - want]
        # files that existed then but were since removed from the live set are
        # re-added as DataFile records (bytes are still on disk in this model,
        # unless a compaction cleanup removed them — then this will surface
        # as missing files on read, which tests/callers can detect).
        by_path = {}
        for snap in self.snapshots.values():
            pass
        # reconstruct DataFile metadata from txn replay is not stored per-file
        # after removal, so re-adding requires the metadata we kept below.
        added = [self._graveyard[p] for p in want - cur if p in getattr(self, "_graveyard", {})]
        missing = [p for p in want - cur if p not in getattr(self, "_graveyard", {})]
        if missing: raise RuntimeError(f"cannot rollback: files no longer tracked: {missing}")
        snap = Snapshot(self._next_snapshot, self.current_snapshot_id, _now_ms(), "rollback", [], [], self.current_schema().schema_id, {})
        # commit manually preserving added metadata
        self._next_snapshot += 1
        for f in added: self.files[f.path] = f
        for f in removed: self.files.pop(f.path, None)
        snap.added_files = [f.path for f in added]; snap.removed_files = [f.path for f in removed]
        snap.summary = {"record_count": sum(f.record_count for f in self.files.values()), "file_count": len(self.files), "rolled_back_to": snapshot_id}
        self.snapshots[snap.snapshot_id] = snap; self.current_snapshot_id = snap.snapshot_id
        self._persist({"snapshot_id": snap.snapshot_id, "parent_id": snap.parent_id, "timestamp_ms": snap.timestamp_ms,
                       "operation": "rollback", "added": snap.added_files, "removed": snap.removed_files,
                       "schema_id": snap.schema_id, "summary": snap.summary})
        return snap

    # -- reads -------------------------------------------------------------
    @property
    def _graveyard(self):
        """DataFile metadata for every file ever added (kept in txn snapshots)."""
        # Rebuilt from an auxiliary index file written alongside commits.
        idx = self.meta_dir / "file_index.json"
        return {k: DataFile.from_dict(v) for k, v in json.loads(idx.read_text()).items()} if idx.exists() else {}

    def _remember_files(self, files):
        idx = self._graveyard; idx.update({f.path: f for f in files})
        (self.meta_dir / "file_index.json").write_text(json.dumps({k: v.to_dict() for k, v in idx.items()}, indent=2))

    def current_schema(self) -> Schema:
        snap = self.current_snapshot()
        return self.schemas[snap.schema_id if snap else 0]
    def current_snapshot(self) -> Snapshot | None:
        return self.snapshots.get(self.current_snapshot_id) if self.current_snapshot_id else None
    def list_snapshots(self): return [self.snapshots[k] for k in sorted(self.snapshots)]
    def txn_log(self):
        return [json.loads(l) for l in self.txn_path.read_text(encoding="utf-8").splitlines() if l.strip()] if self.txn_path.exists() else []
    def live_files(self): return list(self.files.values())

    def files_at(self, snapshot_id: int) -> list[DataFile]:
        if snapshot_id not in self.snapshots: raise KeyError(f"unknown snapshot {snapshot_id}")
        live: dict[str, DataFile] = {}; index = self._graveyard
        for sid in sorted(self.snapshots):
            snap = self.snapshots[sid]
            for p in snap.removed_files: live.pop(p, None)
            for p in snap.added_files:
                if p in index: live[p] = index[p]
                elif p in self.files: live[p] = self.files[p]
            if sid == snapshot_id: break
        return list(live.values())

    def read(self, snapshot_id: int | None = None, files: list[DataFile] | None = None) -> pa.Table:
        files = files if files is not None else (self.files_at(snapshot_id) if snapshot_id else self.live_files())
        schema = self.current_schema() if snapshot_id is None else self.schemas[self.snapshots[snapshot_id].schema_id]
        if not files:
            return pa.table({f.name: pa.array([], type=_pa_type(f.type)) for f in schema.fields})
        tables = []
        for f in files:
            t = pq.read_table(self.data_dir / f.path)
            # fill columns added by schema evolution
            for fld in schema.fields:
                if fld.name not in t.column_names:
                    t = t.append_column(fld.name, pa.array([fld.default] * t.num_rows, type=_pa_type(fld.type)))
            tables.append(t.select(schema.names()))
        return pa.concat_tables(tables, promote_options="default") if len(tables) > 1 else tables[0]

def _pa_type(canonical: str):
    return {"int": pa.int32(), "long": pa.int64(), "float": pa.float32(), "double": pa.float64(),
            "string": pa.string(), "bool": pa.bool_(), "timestamp": pa.timestamp("us"),
            "date": pa.date32()}.get(canonical, pa.string())
