"""Partition-pruning helpers using per-file partition values + min/max stats."""
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class PruneResult:
    kept: list
    pruned: list
    @property
    def kept_count(self): return len(self.kept)
    @property
    def pruned_count(self): return len(self.pruned)

def _match(value, op, target):
    if value is None: return False
    try:
        if op == "=": return str(value) == str(target)
        if op == "!=": return str(value) != str(target)
        if op == ">": return str(value) > str(target)
        if op == ">=": return str(value) >= str(target)
        if op == "<": return str(value) < str(target)
        if op == "<=": return str(value) <= str(target)
        if op == "in": return str(value) in {str(x) for x in target}
    except Exception: return True
    raise ValueError(f"unknown operator {op}")

def prune_files(files, predicates: list[tuple[str, str, object]]) -> PruneResult:
    """Keep a file only if it *could* contain matching rows.

    Uses exact partition-value checks where possible, otherwise file-level
    min/max stats: a file is pruned when the predicate range is disjoint
    from [min, max]. Files without stats are conservatively kept.
    """
    kept, pruned = [], []
    for f in files:
        keep = True
        for col, op, target in predicates:
            if col in (f.partition or {}):
                if not _match(f.partition[col], op, target): keep = False; break
                continue
            stats = (f.column_stats or {}).get(col, {})
            lo, hi = stats.get("min"), stats.get("max")
            if lo is None or hi is None: continue
            if op == "=" and not (lo <= str(target) <= hi): keep = False; break
            if op == ">" and not (hi > str(target)): keep = False; break
            if op == ">=" and not (hi >= str(target)): keep = False; break
            if op == "<" and not (lo < str(target)): keep = False; break
            if op == "<=" and not (lo <= str(target)): keep = False; break
            if op == "in" and not any(lo <= str(v) <= hi for v in target): keep = False; break
        (kept if keep else pruned).append(f)
    return PruneResult(kept, pruned)

def partition_key(file) -> tuple:
    return tuple(sorted((file.partition or {}).items()))
