# fact_planned.py (updated to ingest ALL snapshots under timetables/)
from __future__ import annotations

import glob
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

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
    """
    Split ppth/cpth field: "A|B|C" -> ["A","B","C"]
    """
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
    """
    Returns (previous_station_name, next_station_name) raw strings.

    Uses cpth if present else ppth.
    - prev from arrival: last station in (c)pth
    - next from departure: first station in (c)pth
    """
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


def _core_tokens(station_search: str) -> List[str]:
    return [t for t in station_search.split() if len(t) >= 2 and t not in GENERIC]


def _core_token_regex(station_search: str) -> Optional[str]:
    toks = _core_tokens(station_search)
    if not toks:
        return None
    # whole-word match for any core token
    # e.g. \m(gesundbrunnen|leipzig)\M
    return r"\m(" + "|".join(re.escape(t) for t in toks) + r")\M"


def _core_search_string(station_search: str) -> str:
    """
    Remove GENERIC tokens (and 1-char junk) from a normalized station_search string.
    Used for scoring so that qualifiers like 's bahn' don't drag the similarity down.
    """
    toks = _core_tokens(station_search)
    return " ".join(toks)


def to_station_search_name(name: str) -> str:
    """
    Must match how you populate dim_station.station_name_search.
    """
    s = (name or "").strip().lower()
    s = s.replace("ß", "ss")

    # hbf (word + suffix)
    s = re.sub(r"\bhbf\b\.?", " hauptbahnhof ", s)
    s = re.sub(r"(?<=\w)hbf\b\.?", "hauptbahnhof", s)

    # bf (word + suffix) — suffix excludes "...hbf"
    s = re.sub(r"\bbf\b\.?", " bahnhof ", s)
    s = re.sub(r"(?<=\w)(?<!h)bf\b\.?", "bahnhof", s)

    # str (word + suffix)
    s = re.sub(r"\bstr\b\.?", " strasse ", s)
    s = re.sub(r"(?<=\w)str\b\.?", "strasse", s)

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
    threshold: float,
    cache: Optional[Dict[str, Optional[int]]] = None,
) -> Optional[int]:
    """
    Candidate restriction uses core tokens.
    Scoring uses core-search-string so that qualifiers like '(S-Bahn)' don't drag score down.
    Logs top candidate + score ALWAYS (station_resolve_log).
    Auto-links only if score >= threshold; else upserts into needs_review.
    """
    station_search_full = to_station_search_name(station_raw)
    if not station_search_full:
        return None

    # Use core string for scoring + caching (so "Berlin Hbf (S-Bahn)" and "Berlin Hbf" share cache)
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
        has_core
        and best_score is not None
        and best_score >= threshold
        and best_eva is not None
    )

    # Always log the attempt (log BOTH raw + full search + score query)
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

    station_guess = station_name_from_timetable_filename(os.path.basename(xml_path))
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
    """
    Finds snapshot folders under timetables/**/<snapshot_key>/
    Example: timetables/2509/250902/2509021400/*.xml (your structure may vary)
    """
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

            # FILTER: skip buses entirely
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

            # If both hidden, it is not a passenger-relevant stop
            if ar_hidden and dp_hidden:
                continue

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
            
            # Prevent self-loops
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
                    ar_ts,
                    dp_ts,
                    ar_pp,
                    dp_pp,
                    ar_hidden,
                    dp_hidden,
                    previous_station_eva,
                    next_station_eva,
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
            arrival_is_hidden,
            departure_is_hidden,
            previous_station_eva,
            next_station_eva
        )
        values %s
        on conflict (snapshot_key, station_eva, stop_id) do update
        set train_id = excluded.train_id,
            planned_arrival_ts = excluded.planned_arrival_ts,
            planned_departure_ts = excluded.planned_departure_ts,
            planned_arrival_platform = excluded.planned_arrival_platform,
            planned_departure_platform = excluded.planned_departure_platform,
            arrival_is_hidden = excluded.arrival_is_hidden,
            departure_is_hidden = excluded.departure_is_hidden,
            previous_station_eva = excluded.previous_station_eva,
            next_station_eva = excluded.next_station_eva
        """,
        [
            (
                sk, eva, tid, sid,
                ar_ts, dp_ts, ar_plat, dp_plat,
                None, None,
                False, False,
                None, None,
                ar_hidden,
                dp_hidden,
                prev_eva,
                next_eva,
            )
            for (sk, eva, tid, sid, ar_ts, dp_ts, ar_plat, dp_plat, ar_hidden, dp_hidden, prev_eva, next_eva) in rows
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
    """
    Walk all snapshot folders under timetables_root and ingest each.
    Returns dict snapshot_key -> inserted_row_count.
    """
    # distinct + sorted for stable progress output
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

        # optional: you can commit outside this function; but if you want chunk commits:
        if commit_every > 0 and hasattr(cur, "connection") and (i % commit_every == 0):
            cur.connection.commit()

    return results
