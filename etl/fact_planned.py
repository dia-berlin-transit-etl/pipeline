from __future__ import annotations

import glob
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import psycopg2.extras
import xml.etree.ElementTree as ET

_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")  # YYMMDDHHmm


# ---------- dimension lookups ----------

def load_train_map(cur) -> Dict[Tuple[str, str], int]:
    """(category, train_number) -> train_id"""
    cur.execute("select train_id, category, train_number from dw.dim_train;")
    return {(c, n): int(tid) for (tid, c, n) in cur.fetchall()}


# ---------- basic parsing helpers ----------

def parse_yyMMddHHmm(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    ts = ts.strip()
    if not _SNAPSHOT_KEY_RE.match(ts):
        return None
    yy = int(ts[0:2])
    mm = int(ts[2:4])
    dd = int(ts[4:6])
    hh = int(ts[6:8])
    mi = int(ts[8:10])
    return datetime(2000 + yy, mm, dd, hh, mi)


def to_station_search_name(name: str) -> str:
    """
    Must match how you populate dim_station.station_name_search.
    """
    s = (name or "").strip().lower()
    s = re.sub(r"\bberlin\b", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def station_name_from_timetable_filename(xml_path: str) -> str:
    """
    e.g. berlin_ostbahnhof_timetable.xml -> "berlin ostbahnhof"
    """
    stem = Path(xml_path).stem.lower().strip()
    stem = re.sub(r"(?:_timetable|_timetables)$", "", stem)
    stem = stem.replace("_", " ")
    stem = re.sub(r"[^a-z0-9\s]", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem


# ---------- station resolve with pg_trgm ----------

def resolve_station_eva(
    cur,
    *,
    station_raw: str,
    snapshot_key: str,
    source_path: str,
    threshold: float = 0.85,
    cache: Optional[Dict[str, Optional[int]]] = None,
) -> Optional[int]:
    """
    Uses pg_trgm similarity on dim_station.station_name_search.
    Logs top candidate + score ALWAYS (station_resolve_log).
    Auto-links only if score >= threshold; else upserts into needs_review.
    """

    station_search = to_station_search_name(station_raw)
    if not station_search:
        return None

    if cache is not None and station_search in cache:
        return cache[station_search]

    # Find best candidate by trigram distance, compute similarity score
    cur.execute(
        """
        select
            station_eva,
            station_name,
            similarity(station_name_search, %s) as score
        from dw.dim_station
        order by station_name_search <-> %s
        limit 1
        """,
        (station_search, station_search),
    )
    row = cur.fetchone()

    best_eva: Optional[int] = None
    best_name: Optional[str] = None
    best_score: Optional[float] = None

    if row:
        best_eva = int(row[0]) if row[0] is not None else None
        best_name = row[1]
        best_score = float(row[2]) if row[2] is not None else None

    auto_linked = bool(best_score is not None and best_score >= threshold and best_eva is not None)

    # Always log the attempt
    cur.execute(
        """
        insert into dw.station_resolve_log (
            snapshot_key, source_path, station_raw, station_search,
            best_station_eva, best_station_name, best_score, auto_linked
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (snapshot_key, source_path, station_raw, station_search, best_eva, best_name, best_score, auto_linked),
    )

    if auto_linked:
        if cache is not None:
            cache[station_search] = best_eva
        return best_eva

    # Borderline/failed -> needs_review (de-dupe by station_search)
    cur.execute(
        """
        insert into dw.needs_review (
            station_search, station_raw,
            best_station_eva, best_station_name, best_score,
            last_snapshot_key, last_source_path, last_seen_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, now())
        on conflict (station_search) do update
        set station_raw = excluded.station_raw,
            best_station_eva = excluded.best_station_eva,
            best_station_name = excluded.best_station_name,
            best_score = excluded.best_score,
            last_snapshot_key = excluded.last_snapshot_key,
            last_source_path = excluded.last_source_path,
            last_seen_at = now()
        """,
        (station_search, station_raw, best_eva, best_name, best_score, snapshot_key, source_path),
    )

    if cache is not None:
        cache[station_search] = None
    return None


def get_station_eva_for_timetable(
    cur,
    *,
    root: ET.Element,
    xml_path: str,
    snapshot_key: str,
    threshold: float,
    cache: Dict[str, Optional[int]],
) -> Optional[int]:
    """
    Prefer numeric EVA on root if present; else resolve station name via pg_trgm.
    """
    eva_attr = root.get("eva")
    if eva_attr:
        try:
            return int(eva_attr)
        except ValueError:
            pass

    station_attr = root.get("station")
    if station_attr:
        return resolve_station_eva(
            cur,
            station_raw=station_attr,
            snapshot_key=snapshot_key,
            source_path=xml_path,
            threshold=threshold,
            cache=cache,
        )

    # Fallback: filename-derived
    station_guess = station_name_from_timetable_filename(os.path.basename(xml_path))
    return resolve_station_eva(
        cur,
        station_raw=station_guess,
        snapshot_key=snapshot_key,
        source_path=xml_path,
        threshold=threshold,
        cache=cache,
    )


# ---------- fact ingestion ----------

def upsert_fact_movement_from_timetables(
    cur,
    snapshot_key: str,
    timetables_glob: str,
    *,
    threshold: float = 0.85,
    page_size: int = 5000,
) -> int:
    """
    Planned ingestion for ONE snapshot_key.

    IMPORTANT: Pass timetables_glob that only points to that snapshot folder, e.g.
      timetables/**/2509021400/*.xml
    """
    if not _SNAPSHOT_KEY_RE.match(snapshot_key):
        raise ValueError(f"Invalid snapshot_key (expected YYMMDDHHmm): {snapshot_key}")

    train_map = load_train_map(cur)
    station_cache: Dict[str, Optional[int]] = {}

    rows: List[Tuple] = []

    for path in glob.glob(timetables_glob, recursive=True):
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            continue

        station_eva = get_station_eva_for_timetable(
            cur,
            root=root,
            xml_path=path,
            snapshot_key=snapshot_key,
            threshold=threshold,
            cache=station_cache,
        )
        if station_eva is None:
            # unresolved station -> skip, but it is recorded in needs_review already
            continue

        for s in root.findall("./s"):
            stop_id = s.get("id")
            if not stop_id:
                continue

            tl = s.find("tl")
            if tl is None:
                continue

            cat = (tl.get("c") or "").strip()
            num = (tl.get("n") or "").strip()
            if not cat or not num:
                continue

            train_id = train_map.get((cat, num))
            if train_id is None:
                # robust: insert-on-the-fly
                cur.execute(
                    """
                    insert into dw.dim_train (category, train_number)
                    values (%s, %s)
                    on conflict (category, train_number) do nothing
                    returning train_id
                    """,
                    (cat, num),
                )
                got = cur.fetchone()
                if got:
                    train_id = int(got[0])
                else:
                    cur.execute(
                        "select train_id from dw.dim_train where category=%s and train_number=%s",
                        (cat, num),
                    )
                    train_id = int(cur.fetchone()[0])
                train_map[(cat, num)] = train_id

            ar = s.find("ar")
            dp = s.find("dp")

            ar_hidden = (ar is not None and ar.get("hi") == "1")
            dp_hidden = (dp is not None and dp.get("hi") == "1")

            # if both hidden, ignore this stop for passenger-facing movement
            if ar_hidden and dp_hidden:
                continue

            is_hidden = ar_hidden or dp_hidden

            ar_ts = None
            ar_pp = None
            if ar is not None and not ar_hidden:
                ar_ts = parse_yyMMddHHmm(ar.get("pt"))
                ar_pp = ar.get("pp") or None

            dp_ts = None
            dp_pp = None
            if dp is not None and not dp_hidden:
                dp_ts = parse_yyMMddHHmm(dp.get("pt"))
                dp_pp = dp.get("pp") or None

            rows.append(
                (
                    snapshot_key,
                    station_eva,
                    train_id,
                    stop_id,
                    ar_ts,
                    dp_ts,
                    ar_pp,
                    dp_pp,
                    is_hidden,
                )
            )

    if not rows:
        return 0

    psycopg2.extras.execute_values(
        cur,
        """
        insert into dw.fact_movement (
            snapshot_key,
            station_eva,
            train_id,
            stop_id,
            planned_arrival_ts,
            planned_departure_ts,
            planned_arrival_platform,
            planned_departure_platform,
            changed_arrival_ts,
            changed_departure_ts,
            arrival_cancelled,
            departure_cancelled,
            arrival_delay_min,
            departure_delay_min,
            is_hidden
        )
        values %s
        on conflict (snapshot_key, station_eva, stop_id) do update
        set train_id = excluded.train_id,
            planned_arrival_ts = excluded.planned_arrival_ts,
            planned_departure_ts = excluded.planned_departure_ts,
            planned_arrival_platform = excluded.planned_arrival_platform,
            planned_departure_platform = excluded.planned_departure_platform,
            is_hidden = excluded.is_hidden
        """,
        [
            (
                sk, eva, tid, sid,
                ar_ts, dp_ts, ar_plat, dp_plat,
                None, None,
                False, False,
                None, None,
                hidden
            )
            for (sk, eva, tid, sid, ar_ts, dp_ts, ar_plat, dp_plat, hidden) in rows
        ],
        page_size=page_size,
    )

    return len(rows)
