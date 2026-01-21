from __future__ import annotations

import glob
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import psycopg2.extras
import xml.etree.ElementTree as ET

_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")  # YYMMDDHHmm

GENERIC = {
    "bahnhof", "hauptbahnhof",
    "station", "haltepunkt",
    "sbahn", "ubahn",
    "bahn",
}


def _split_path_list(path_str: Optional[str]) -> List[str]:
    if not path_str:
        return []
    parts = [p.strip() for p in path_str.split("|")]
    return [p for p in parts if p]


def get_prev_next_station_raw(
    *,
    ar: Optional[ET.Element],
    dp: Optional[ET.Element],
    ar_hidden: bool,
    dp_hidden: bool,
) -> Tuple[Optional[str], Optional[str]]:
    prev_raw: Optional[str] = None
    next_raw: Optional[str] = None

    if ar is not None and not ar_hidden:
        ar_path = ar.get("cpth") or ar.get("ppth")
        ar_list = _split_path_list(ar_path)
        if ar_list:
            prev_raw = ar_list[-1]

    if dp is not None and not dp_hidden:
        dp_path = dp.get("cpth") or dp.get("ppth")
        dp_list = _split_path_list(dp_path)
        if dp_list:
            next_raw = dp_list[0]

    return prev_raw, next_raw


# ---------- dimension lookups ----------

def load_train_map(cur) -> Dict[Tuple[str, str], int]:
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


def _core_tokens(station_search: str) -> List[str]:
    return [t for t in station_search.split() if len(t) >= 2 and t not in GENERIC]


def _core_token_regex(station_search: str) -> Optional[str]:
    toks = _core_tokens(station_search)
    if not toks:
        return None
    return r"\m(" + "|".join(re.escape(t) for t in toks) + r")\M"


def _core_search_string(station_search: str) -> str:
    toks = _core_tokens(station_search)
    return " ".join(toks)


def to_station_search_name(name: str) -> str:
    s = (name or "").strip().lower()

    s = (s.replace("ß", "s")
           .replace("ä", "a")
           .replace("ö", "o")
           .replace("ü", "u"))

    s = re.sub(r"(?<=\w)_(?=\w)", "", s)

    s = re.sub(r"\bhbf\b\.?", " hauptbahnhof ", s)
    s = re.sub(r"(?<=\w)hbf\b\.?", "hauptbahnhof", s)

    s = re.sub(r"\bbf\b\.?", " bahnhof ", s)
    s = re.sub(r"(?<=\w)(?<!h)bf\b\.?", "bahnhof", s)

    s = re.sub(r"\bstr\b\.?", " strase ", s)
    s = re.sub(r"(?<=\w)str\b\.?", "strase", s)
    s = re.sub(r"\b(\w+)\s+strase\b", r"\1strase", s)

    s = re.sub(r"\bberlin\b", " ", s)

    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def station_name_from_timetable_filename(xml_path: str) -> str:
    stem = Path(xml_path).stem.lower().strip()
    stem = re.sub(r"(?:_timetable|_timetables)$", "", stem)
    return stem


# ---------- station resolve with pg_trgm ----------

def resolve_station_eva(
    cur,
    *,
    station_raw: str,
    snapshot_key: str,
    source_path: str,
    threshold: float,
    cache: Optional[Dict[str, Optional[int]]] = None,
) -> Optional[int]:
    station_search_full = to_station_search_name(station_raw)
    if not station_search_full:
        return None

    score_query = _core_search_string(station_search_full) or station_search_full
    cache_key = score_query

    if cache is not None and cache_key in cache:
        return cache[cache_key]

    core_pat = _core_token_regex(station_search_full)
    has_core = bool(core_pat)

    if core_pat:
        cur.execute(
            """
            select station_eva, station_name,
                   similarity(station_name_search, %s) as score
            from dw.dim_station
            where station_name_search ~ %s
            order by station_name_search <-> %s
            limit 1
            """,
            (score_query, core_pat, score_query),
        )
    else:
        cur.execute(
            """
            select station_eva, station_name,
                   similarity(station_name_search, %s) as score
            from dw.dim_station
            order by station_name_search <-> %s
            limit 1
            """,
            (score_query, score_query),
        )

    row = cur.fetchone()

    best_eva: Optional[int] = None
    best_name: Optional[str] = None
    best_score: Optional[float] = None

    if row:
        best_eva = int(row[0]) if row[0] is not None else None
        best_name = row[1]
        best_score = float(row[2]) if row[2] is not None else None

    auto_linked = bool(
        best_score is not None
        and best_eva is not None
        and (
            (has_core and best_score >= threshold)
            or ((not has_core) and best_score >= 0.72)
        )
    )

    cur.execute(
        """
        insert into dw.station_resolve_log (
            snapshot_key, source_path, station_raw, station_search,
            best_station_eva, best_station_name, best_score, auto_linked
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (snapshot_key, source_path, station_raw, station_search_full, best_eva, best_name, best_score, auto_linked),
    )

    if auto_linked:
        if cache is not None:
            cache[cache_key] = best_eva
        return best_eva

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
        (cache_key, station_raw, best_eva, best_name, best_score, snapshot_key, source_path),
    )

    if cache is not None:
        cache[cache_key] = None
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

    station_guess_raw = station_name_from_timetable_filename(os.path.basename(xml_path))
    station_guess = to_station_search_name(station_guess_raw)
    return resolve_station_eva(
        cur,
        station_raw=station_guess,
        snapshot_key=snapshot_key,
        source_path=xml_path,
        threshold=threshold,
        cache=cache,
    )


# ---------- snapshot discovery ----------

def iter_timetable_snapshots(timetables_root: str = "timetables") -> Iterator[str]:
    for p in glob.glob(os.path.join(timetables_root, "**", "[0-9]" * 10), recursive=True):
        if os.path.isdir(p):
            key = os.path.basename(p)
            if _SNAPSHOT_KEY_RE.match(key):
                yield key


def timetables_glob_for_snapshot(snapshot_key: str, timetables_root: str = "timetables") -> str:
    return os.path.join(timetables_root, "**", snapshot_key, "*.xml")


# ---------- fact ingestion (ONE snapshot) ----------

def upsert_fact_movement_for_snapshot(
    cur,
    snapshot_key: str,
    *,
    threshold: float,
    timetables_root: str = "timetables",
    page_size: int = 5000,
) -> int:
    timetables_glob = timetables_glob_for_snapshot(snapshot_key, timetables_root=timetables_root)

    train_map = load_train_map(cur)
    station_cache: Dict[str, Optional[int]] = {}

    rows: List[Tuple] = []

    for path in glob.glob(timetables_glob, recursive=True):
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            continue

        stops = root.findall("./s")
        if not stops:
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
            continue

        for s in stops:
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

            if cat.lower() == "bus":
                continue

            train_id = train_map.get((cat, num))
            if train_id is None:
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

            if ar_hidden and dp_hidden:
                continue

            planned_ar_ts = parse_yyMMddHHmm(ar.get("pt")) if (ar is not None and not ar_hidden) else None
            planned_dp_ts = parse_yyMMddHHmm(dp.get("pt")) if (dp is not None and not dp_hidden) else None

            prev_raw, next_raw = get_prev_next_station_raw(
                ar=ar, dp=dp, ar_hidden=ar_hidden, dp_hidden=dp_hidden
            )

            previous_station_eva = None
            if prev_raw:
                previous_station_eva = resolve_station_eva(
                    cur,
                    station_raw=prev_raw,
                    snapshot_key=snapshot_key,
                    source_path=path,
                    threshold=threshold,
                    cache=station_cache,
                )

            next_station_eva = None
            if next_raw:
                next_station_eva = resolve_station_eva(
                    cur,
                    station_raw=next_raw,
                    snapshot_key=snapshot_key,
                    source_path=path,
                    threshold=threshold,
                    cache=station_cache,
                )

            if previous_station_eva == station_eva:
                previous_station_eva = None
            if next_station_eva == station_eva:
                next_station_eva = None

            rows.append(
                (
                    snapshot_key,
                    station_eva,
                    train_id,
                    stop_id,
                    planned_ar_ts,
                    planned_dp_ts,
                    previous_station_eva,
                    next_station_eva,
                    ar_hidden,
                    dp_hidden,
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
            previous_station_eva,
            next_station_eva,

            changed_arrival_ts,
            changed_departure_ts,
            changed_previous_station_eva,
            changed_next_station_eva,

            arrival_cancelled,
            departure_cancelled,
            arrival_delay_min,
            departure_delay_min,

            arrival_is_hidden,
            departure_is_hidden
        )
        values %s
        on conflict (snapshot_key, station_eva, stop_id) do update
        set
            train_id = excluded.train_id,
            planned_arrival_ts = excluded.planned_arrival_ts,
            planned_departure_ts = excluded.planned_departure_ts,
            previous_station_eva = excluded.previous_station_eva,
            next_station_eva = excluded.next_station_eva,
            arrival_is_hidden = excluded.arrival_is_hidden,
            departure_is_hidden = excluded.departure_is_hidden
        """,
        [
            (
                sk, eva, tid, sid,
                ar_ts, dp_ts,
                prev_eva, next_eva,

                None, None,  # changed_arrival_ts / changed_departure_ts
                None, None,  # changed_previous_station_eva / changed_next_station_eva

                False, False,  # cancellations
                None, None,    # delays

                ar_hidden, dp_hidden,
            )
            for (sk, eva, tid, sid, ar_ts, dp_ts, prev_eva, next_eva, ar_hidden, dp_hidden) in rows
        ],
        page_size=page_size,
    )

    return len(rows)


# ---------- fact ingestion (ALL snapshots) ----------

def upsert_fact_movement_from_all_timetables(
    cur,
    *,
    timetables_root: str = "timetables",
    threshold: float,
    page_size: int = 5000,
    commit_every: int = 1,
) -> Dict[str, int]:
    snapshot_keys = sorted(set(iter_timetable_snapshots(timetables_root)))

    results: Dict[str, int] = {}
    for i, sk in enumerate(snapshot_keys, start=1):
        n = upsert_fact_movement_for_snapshot(
            cur,
            sk,
            threshold=threshold,
            timetables_root=timetables_root,
            page_size=page_size,
        )
        results[sk] = n

        if commit_every > 0 and hasattr(cur, "connection") and (i % commit_every == 0):
            cur.connection.commit()

    return results
