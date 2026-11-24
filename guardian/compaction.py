"""Small-file compaction: bin-pack fragmented files into fixed-size targets.

Files are grouped by partition key (never merged across partitions), then
greedily packed until the estimated output reaches target_file_size_bytes.
Only groups containing at least one file below the small-file threshold
(or >=2 files) are rewritten. Output bytes are estimated from live files'
average bytes/row, so targets are approximate — as in real engines.
"""
from __future__ import annotations
from dataclasses import dataclass, field
import pyarrow as pa
import pyarrow.parquet as pq
from .partition import partition_key

@dataclass
class CompactionPlan:
    groups: list[list] = field(default_factory=list)  # each: list[DataFile] to merge
    skipped_files: int = 0
    def to_dict(self):
        return {"groups": [[f.path for f in g] for g in self.groups],
                "files_to_rewrite": sum(len(g) for g in self.groups), "skipped_files": self.skipped_files}

@dataclass
class CompactionResult:
    before_files: int; after_files: int; before_bytes: int; after_bytes: int
    rows: int; groups_rewritten: int; plan: dict = field(default_factory=dict)
    @property
    def file_reduction_ratio(self): return round(self.before_files / self.after_files, 3) if self.after_files else 0.0
    def to_dict(self):
        return {"before_files": self.before_files, "after_files": self.after_files,
                "before_bytes": self.before_bytes, "after_bytes": self.after_bytes,
                "rows": self.rows, "groups_rewritten": self.groups_rewritten,
                "file_reduction_ratio": self.file_reduction_ratio, "plan": self.plan}

def plan_compaction(files, target_file_size_bytes: int = 8 * 1024 * 1024,
                    small_file_threshold_bytes: int | None = None) -> CompactionPlan:
    threshold = small_file_threshold_bytes if small_file_threshold_bytes is not None else target_file_size_bytes // 2
    by_part: dict[tuple, list] = {}
    for f in files: by_part.setdefault(partition_key(f), []).append(f)
    plan = CompactionPlan()
    for key, group in by_part.items():
        group = sorted(group, key=lambda f: f.file_size_bytes)
        eligible = [f for f in group if f.file_size_bytes < threshold]
        # Nothing to do for a lone healthy file; a lone small file also stays
        # (merging it alone cannot reduce file count) unless peers exist.
        if len(group) < 2 or not eligible:
            plan.skipped_files += len(group); continue
        # bin-pack the whole partition group: small files pull neighbours in
        bins, cur, cur_bytes = [], [], 0
        for f in group:
            if cur and cur_bytes + f.file_size_bytes > target_file_size_bytes:
                bins.append(cur); cur, cur_bytes = [], 0
            cur.append(f); cur_bytes += f.file_size_bytes
        if cur: bins.append(cur)
        for b in bins:
            if len(b) >= 2: plan.groups.append(b)
            else: plan.skipped_files += 1
    return plan

def execute_compaction(catalog, plan: CompactionPlan, target_file_size_bytes: int = 8 * 1024 * 1024,
                       delete_removed: bool = False) -> CompactionResult:
    before = catalog.live_files()
    before_bytes = sum(f.file_size_bytes for f in before)
    added, removed, rows = [], [], 0
    for group in plan.groups:
        table = catalog.read(files=group)
        rows += table.num_rows
        # rows per output file from average bytes/row of the inputs
        in_bytes = sum(f.file_size_bytes for f in group)
        bytes_per_row = (in_bytes / table.num_rows) if table.num_rows else target_file_size_bytes
        rows_per_file = max(1, int(target_file_size_bytes / bytes_per_row)) if bytes_per_row else table.num_rows
        for lo in range(0, table.num_rows, rows_per_file):
            added.append(catalog.write_data_file(table.slice(lo, min(rows_per_file, table.num_rows - lo))))
        removed.extend(group)
    if added or removed:
        catalog.commit_compaction(added, removed)
        if delete_removed:
            # Opt-in only: deleting bytes breaks rollback to pre-compaction snapshots.
            for f in removed:
                try: (catalog.data_dir / f.path).unlink()
                except FileNotFoundError: pass
    after = catalog.live_files()
    return CompactionResult(len(before), len(after), before_bytes,
                            sum(f.file_size_bytes for f in after),
                            sum(f.record_count for f in after), len(plan.groups), plan.to_dict())

def compact(catalog, target_file_size_bytes: int = 8*1024*1024, small_file_threshold_bytes=None,
            delete_removed: bool = False) -> CompactionResult:
    plan = plan_compaction(catalog.live_files(), target_file_size_bytes, small_file_threshold_bytes)
    return execute_compaction(catalog, plan, target_file_size_bytes, delete_removed)
