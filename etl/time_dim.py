from __future__ import annotations

import glob
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator, List, Set, Tuple

import psycopg2.extras


_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")  # YYMMDDHHmm


@dataclass(frozen=True)
class SnapshotRow:
    snapshot_key: str
    snapshot_ts: datetime
    snapshot_date: datetime.date
    hour: int
    minute: int


def parse_snapshot_key(snapshot_key: str) -> SnapshotRow:
    """
    Parse a snapshot key in YYMMDDHHmm into a SnapshotRow.
    Assumes years are 2000+YY (e.g., 25 -> 2025).
    """
    if not _SNAPSHOT_KEY_RE.match(snapshot_key):
        raise ValueError(f"Invalid snapshot_key (expected YYMMDDHHmm): {snapshot_key}")

    yy = int(snapshot_key[0:2])
    mm = int(snapshot_key[2:4])
    dd = int(snapshot_key[4:6])
    hh = int(snapshot_key[6:8])
    mi = int(snapshot_key[8:10])

    year = 2000 + yy
    ts = datetime(year, mm, dd, hh, mi)
    return SnapshotRow(
        snapshot_key=snapshot_key,
        snapshot_ts=ts,
        snapshot_date=ts.date(),
        hour=hh,
        minute=mi,
    )


def _iter_path_segments(path: str) -> Iterator[str]:
    """
    Yield path segments and also try filename stem chunks.
    This catches snapshot keys appearing as folder names or embedded in filenames.
    """
    norm = os.path.normpath(path)
    parts = norm.split(os.sep)

    # path segments
    for p in parts:
        yield p

    # also split filename by non-alnum separators to catch "station_2509021400.xml"
    base = os.path.basename(path)
    stem = os.path.splitext(base)[0]
    for token in re.split(r"[^0-9A-Za-z]+", stem):
        if token:
            yield token


def discover_snapshot_keys_from_globs(globs: Iterable[str]) -> Set[str]:
    """
    Discover snapshot keys (YYMMDDHHmm) from filesystem paths matching given globs.
    """
    keys: Set[str] = set()

    for g in globs:
        for path in glob.glob(g, recursive=True):
            # only consider files; keys also exist as folders, but files are enough to discover them
            for seg in _iter_path_segments(path):
                if _SNAPSHOT_KEY_RE.match(seg):
                    keys.add(seg)

    return keys


def upsert_dim_time_from_paths(
    cur,
    snapshot_globs: List[str],
) -> List[str]:
    """
    Discovers snapshot keys from the provided path globs and upserts them into dw.dim_time.

    Returns: sorted list of snapshot_keys inserted/known (all keys discovered).
    """
    keys = discover_snapshot_keys_from_globs(snapshot_globs)
    if not keys:
        print("WARN: No snapshot keys discovered. Check your globs/paths.")
        return []

    rows: List[Tuple[str, datetime, object, int, int]] = []
    for k in keys:
        try:
            r = parse_snapshot_key(k)
        except ValueError:
            continue
        rows.append((r.snapshot_key, r.snapshot_ts, r.snapshot_date, r.hour, r.minute))

    psycopg2.extras.execute_values(
        cur,
        """
        insert into dw.dim_time (snapshot_key, snapshot_ts, snapshot_date, hour, minute)
        values %s
        on conflict (snapshot_key) do update
        set snapshot_ts = excluded.snapshot_ts,
            snapshot_date = excluded.snapshot_date,
            hour = excluded.hour,
            minute = excluded.minute
        """,
        rows,
        page_size=5000,
    )

    return sorted(keys)


def print_dim_time_preview(cur, limit: int = 25) -> None:
    cur.execute("select count(*) from dw.dim_time;")
    count = cur.fetchone()[0]
    print(f"dw.dim_time rows: {count}")

    cur.execute(
        """
        select snapshot_key, snapshot_ts
        from dw.dim_time
        order by snapshot_key
        limit %s
        """,
        (limit,),
    )
    rows = cur.fetchall()

    print(f"\nFirst {min(limit, len(rows))} snapshots (sorted by snapshot_key):")
    for k, ts in rows:
        print(f"- {k} -> {ts}")
