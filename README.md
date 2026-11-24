# iceberg-compaction-guardian

Offline Iceberg-style compaction and data-quality guardian for a cloud lakehouse.

A self-contained Python reference project that **emulates** the core ideas of
Apache Iceberg — versioned snapshots, an ordered transaction log, schema
evolution, partition pruning and small-file compaction — over a local directory
of Parquet files, plus a Great Expectations-style data-contract validator.
No cluster, no cloud account, no network: everything runs offline with PyArrow.

> Status: working reference implementation. Tests pass and a benchmark output
> is committed, but this is a teaching-scale emulation, not a replacement for
> Apache Iceberg / a real lakehouse engine (see *Honest limitations*).

## Design

```
                         ┌──────────────────────────────────────────────┐
                         │                    CLI                        │
                         │  init · ingest · compact · validate           │
                         │  time-travel · status · evolve   (JSON out)   │
                         └───────┬───────────────┬───────────────┬──────┘
                                 │               │               │
                    ┌────────────▼───┐  ┌────────▼───────┐  ┌────▼─────────────┐
                    │ TableCatalog   │  │  Compaction    │  │ Contract         │
                    │ snapshots,     │  │  bin-pack by   │  │ validator        │
                    │ commits, txn   │◄─┤  partition to  │  │ completeness,    │
                    │ log, rollback  │  │  target size   │  │ freshness, null  │
                    └───────┬────────┘  └────────┬───────┘  │ rates, uniqueness│
                            │                    │          │ type drift       │
              ┌─────────────▼─────────────┐      │          └────┬─────────────┘
              │  Schema evolution +       │      │               │ reads via
              │  partition pruning        │      └──────┬────────┘ catalog.read()
              │  (widen-only, min/max)    │             │
              └─────────────┬─────────────┘             │
                            ▼                           ▼
              ┌───────────────────────────────────────────────────────┐
              │  <table>/metadata/catalog.json   (atomic replace)      │
              │  <table>/metadata/txn_log.jsonl  (one line / commit)   │
              │  <table>/metadata/file_index.json (every file ever)    │
              │  <table>/data/*.parquet          (never mutated)       │
              └───────────────────────────────────────────────────────┘
```

Reads only ever follow `catalog.json`. A commit writes new Parquet files first,
appends to `txn_log.jsonl`, then atomically replaces `catalog.json`
(tmp file + `os.replace`), so a crash mid-commit leaves the old snapshot visible.

## Design choices

| Area | Choice | Why / trade-off |
|---|---|---|
| Metadata | JSON catalog + append-only JSONL txn log | Human-inspectable, zero extra deps; not safe for concurrent writers |
| Data files | One Parquet file per append chunk, immutable | Mirrors Iceberg's immutable-file model; rollback keeps bytes on disk |
| Snapshots | Full file-set replay from snapshot history + file index | Simple and correct at this scale; O(history) per snapshot read |
| Compaction | Group by partition key, greedy bin-pack to target bytes, estimate rows/file from avg bytes/row | Never merges across partitions; output sizes are approximate, as in real engines |
| Pruning | Exact partition-value match, else file min/max stats; unknown stats ⇒ keep file | Conservative: never prunes a file that might match |
| Schema evolution | Add (nullable or with default), drop, rename, widen-only retype (`int→long→double`, `float→double`, `date→timestamp`) | Narrowing/unrelated retypes raise `SchemaEvolutionError`; old files get defaults filled at read time |
| Validation | Contract = plain JSON dict → named checks → `pass_rate` | Great Expectations-*style* only; no GE dependency |
| CLI | `argparse`, every command prints JSON, non-zero exit on failed validation | Scriptable; no pretty tables, no interactive mode |

## Quick start

```bash
pip install -e ".[dev]"        # or just: pip install pyarrow pytest

python -m guardian.cli --table ./events ingest --rows 5000 --files 25
python -m guardian.cli --table ./events status
python -m guardian.cli --table ./events validate --contract examples/contract.json
python -m guardian.cli --table ./events compact --target-bytes 262144
python -m guardian.cli --table ./events time-travel --list
python -m guardian.cli --table ./events time-travel --snapshot 2
python -m guardian.cli --table ./events time-travel --snapshot 2 --rollback
python -m guardian.cli --table ./events evolve --ops-json '[{"op":"add","name":"source","type":"string","default":"web"}]'
python -m guardian.cli --table ./events ingest --input data.csv   # existing table only
```

Ingest without `--input` generates deterministic synthetic events
(`event_id, user_id, event_type, amount, event_date, event_time`), continuing
`event_id`s after existing rows so uniqueness contracts keep passing.

## Data contracts

See `examples/contract.json`. Supported expectations: `expected_columns`
(type drift, widening-aware), `required_columns`, `max_null_rate`,
`min_rows`, `unique_columns`, and `freshness`
(`{column, max_age_seconds}` against the newest timestamp).

## Tests

```bash
python -m pytest -q
# 16 passed
```

Covers: catalog create/append/reopen, time-travel read + rollback, schema
evolution (safe + rejected narrowing), compaction file reduction with row
preservation, partition pruning, validator pass/fail paths (nulls, duplicates,
staleness, type drift), a benchmark smoke test, and a full CLI end-to-end run.

## Benchmark

```bash
python benchmarks/run_benchmark.py   # writes benchmarks/results.json
```

Committed output (`benchmarks/results.json`, this repo's sanity run —
30,000 synthetic rows ingested as 60 small files, compacted to a 256 KiB
target, median of 5 full-table reads, local temp dir):

| Metric | Before | After |
|---|---|---|
| Files | 60 | 4 |
| Bytes | 786,464 | 521,879 |
| Median full read | 0.12266 s | 0.00842 s |

- **File reduction ratio:** 15.0× · **read speedup:** 14.57× (this run)
- **Rows preserved:** yes (30,000) · **contract:** 13/13 checks, pass rate 1.0

Honest scale note: at 30k rows the read speedup is mostly per-file open/seek
overhead disappearing, and it varies a lot between runs (we have seen
~8–15× on the same machine). It demonstrates the *mechanism*, not cluster
performance. Do not extrapolate these numbers to a real lakehouse.

## Honest limitations

- **Not Iceberg.** No Iceberg metadata spec compatibility, no manifest lists,
  no hidden partitioning, no partition evolution, no row-level deletes/updates
  (no merge-on-read or copy-on-write delete files), no equality deletes.
- **Single writer only.** No locking or optimistic concurrency; two processes
  committing at once can corrupt the catalog pointer.
- **Rollback keeps data.** Removed files are not deleted by default
  (`delete_removed` is opt-in), so storage grows; snapshots whose files were
  deleted cannot be rolled back to.
- **No query engine.** Reads load whole files into memory via PyArrow;
  pruning is file-level only (no row-group statistics, no predicate pushdown
  inside files beyond what Parquet readers do natively — here, none).
- **Compaction is naive.** Greedy bin-packing within a partition, no sorting/
  clustering (no Z-order), no cost model, target sizes estimated from
  average bytes/row.
- **Validator is shallow.** Null rates, uniqueness and freshness are computed
  in memory over the whole table; no sampling, no anomaly detection, no
  Great Expectations integration.
- **Freshness** compares against wall-clock `now` unless injected, so results
  on old synthetic data depend on when you run it.

## License

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 Ayush Gawai.
