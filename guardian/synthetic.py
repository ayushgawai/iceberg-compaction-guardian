"""Synthetic event generation used by ingest / benchmark (deterministic)."""
from __future__ import annotations
import random
from datetime import datetime, timedelta, timezone
import pyarrow as pa

EVENTS_SCHEMA = [
    {"name": "event_id", "type": "long"},
    {"name": "user_id", "type": "long"},
    {"name": "event_type", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "event_date", "type": "string"},
    {"name": "event_time", "type": "timestamp"},
]
EVENT_TYPES = ["click", "view", "purchase", "signup"]

def generate_events(rows: int, seed: int = 42, start_id: int = 0,
                    null_rate: float = 0.0, base_time: datetime | None = None) -> pa.Table:
    rng = random.Random(seed)
    base = base_time or datetime.now(timezone.utc).replace(microsecond=0)
    ids, users, types, amounts, dates, times = [], [], [], [], [], []
    for i in range(rows):
        ts = base - timedelta(seconds=rng.randint(0, 3600))
        ids.append(start_id + i); users.append(rng.randint(1, 500))
        types.append(rng.choice(EVENT_TYPES))
        amounts.append(None if rng.random() < null_rate else round(rng.uniform(1, 500), 2))
        dates.append(ts.date().isoformat()); times.append(ts)
    return pa.table({"event_id": pa.array(ids, pa.int64()), "user_id": pa.array(users, pa.int64()),
                     "event_type": types, "amount": pa.array(amounts, pa.float64()),
                     "event_date": dates, "event_time": pa.array(times, pa.timestamp("us"))})
