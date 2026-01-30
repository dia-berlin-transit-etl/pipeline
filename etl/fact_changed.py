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



# Parsing helpers

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



# Station name normalization (match dim_station.station_name_search)

def to_station_search_name(name: str) -> str:
    s = (name or "").strip().lower()

    # German folding
    s = (s.replace("ß", "s")
           .replace("ä", "a")
           .replace("ö", "o")
           .replace("ü", "u"))

    # underscore inside words (umlaut placeholder): s_d -> sd
    s = re.sub(r"(?<=\w)_(?=\w)", "", s)

    # hbf / bf
    s = re.sub(r"\bhbf\b\.?", " hauptbahnhof ", s)
    s = re.sub(r"(?<=\w)hbf\b\.?", "hauptbahnhof", s)

    s = re.sub(r"\bbf\b\.?", " bahnhof ", s)
    s = re.sub(r"(?<=\w)(?<!h)bf\b\.?", "bahnhof", s)

    # str -> strase and join "osdorfer strase" -> "osdorferstrase"
    s = re.sub(r"\bstr\b\.?", " strase ", s)
    s = re.sub(r"(?<=\w)str\b\.?", "strase", s)
    s = re.sub(r"\b(\w+)\s+strase\b", r"\1strase", s)

    # drop berlin token
    s = re.sub(r"\bberlin\b", " ", s)

    # strip everything else
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def station_name_from_timetable_filename(xml_path: str) -> str:
    stem = Path(xml_path).stem.lower().strip()
    stem = re.sub(r"(?:_timetable|_timetables|_changes|_timetable_changes)$", "", stem)
    return stem



# dim_train helpers


def load_train_map(cur) -> Dict[Tuple[str, str], int]:
    cur.execute("select train_id, category, train_number from dw.dim_train;")
    return {(c, n): int(tid) for (tid, c, n) in cur.fetchall()}


def get_or_create_train_id(cur, train_map: Dict[Tuple[str, str], int], *, category: str, number: str) -> Optional[int]:
    cat = (category or "").strip()
    num = (number or "").strip()
    if not cat or not num:
        return None

    # optional: skip buses entirely
    if cat.lower() == "bus":
        return None

    key = (cat, num)
    tid = train_map.get(key)
    if tid is not None:
        return tid

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
        tid = int(got[0])
    else:
        cur.execute("select train_id from dw.dim_train where category=%s and train_number=%s", (cat, num))
        row = cur.fetchone()
        tid = int(row[0]) if row else None

    if tid is not None:
        train_map[key] = tid
    return tid



# Station resolve (pg_trgm)


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

    # always log the attempt
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

    # borderline/failed => needs_review
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


def get_station_eva_for_changes_file(
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

    # fallback: filename-derived
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



# Snapshot discovery (changes)


def iter_change_snapshots(timetable_changes_root: str = "timetable_changes") -> Iterator[str]:
    for p in glob.glob(os.path.join(timetable_changes_root, "**", "[0-9]" * 10), recursive=True):
        if os.path.isdir(p):
            key = os.path.basename(p)
            if _SNAPSHOT_KEY_RE.match(key):
                yield key


def changes_glob_for_snapshot(snapshot_key: str, timetable_changes_root: str = "timetable_changes") -> str:
    return os.path.join(timetable_changes_root, "**", snapshot_key, "*.xml")



# dim_time upsert (needed for FK)


def ensure_dim_time(cur, snapshot_key: str) -> None:
    ts = parse_yyMMddHHmm(snapshot_key)
    if ts is None:
        return
    cur.execute(
        """
        insert into dw.dim_time (snapshot_key, snapshot_ts, snapshot_date, hour, minute)
        values (%s, %s, %s::date, %s, %s)
        on conflict (snapshot_key) do update
        set snapshot_ts = excluded.snapshot_ts,
            snapshot_date = excluded.snapshot_date,
            hour = excluded.hour,
            minute = excluded.minute
        """,
        (snapshot_key, ts, ts.date(), ts.hour, ts.minute),
    )



# Changes parsing


def _cancel_update_from_cs(el: Optional[ET.Element]) -> Optional[bool]:
    """
    Returns:
      True  -> explicitly cancelled now (cs='c')
      False -> explicitly not cancelled now (cs='p' or cs='a')  [revoked / planned / added]
      None  -> no information (cs absent/unknown) -> carry forward from base row
    """
    if el is None:
        return None
    cs = el.get("cs")
    if not cs:
        return None
    cs = cs.strip().lower()
    if cs == "c":
        return True
    if cs in ("p", "a"):
        return False
    return None

def _is_added_by_ps_or_cs(el: Optional[ET.Element]) -> bool:
    """
    Added stops can show up as ps="a" (your observed case) and sometimes also cs="a".
    """
    if el is None:
        return False
    ps = (el.get("ps") or "").strip().lower()
    if ps == "a":
        return True
    cs = (el.get("cs") or "").strip().lower()
    return cs == "a"


def _stop_id_suffix_int(stop_id: str) -> Optional[int]:
    """
    stop_id often looks like "...-YYMMDDHHmm-<idx>" where idx can be >= 100 for added stops.
    """
    try:
        last = stop_id.rsplit("-", 1)[-1]
        return int(last)
    except Exception:
        return None


def _parse_changed_time(el: Optional[ET.Element]) -> Optional[datetime]:
    if el is None:
        return None
    return parse_yyMMddHHmm(el.get("ct"))


def _parse_planned_time(el: Optional[ET.Element]) -> Optional[datetime]:
    """
    In changes feed, pt can appear (often for added stops).
    """
    if el is None:
        return None
    return parse_yyMMddHHmm(el.get("pt"))


def _is_hidden(el: Optional[ET.Element]) -> bool:
    if el is None:
        return False
    return (el.get("hi") or "").strip() == "1"


def _split_path_list(path_str: Optional[str]) -> List[str]:
    if not path_str:
        return []
    parts = [p.strip() for p in path_str.split("|")]
    return [p for p in parts if p]


def _prev_next_from_any_path(
    *,
    ar: Optional[ET.Element],
    dp: Optional[ET.Element],
) -> Tuple[Optional[str], Optional[str]]:
    """
    For changes files you may have both cpth and ppth (especially on added stops).
    Use cpth if present, else ppth.
    - prev from arrival: last station in (c)pth
    - next from departure: first station in (c)pth
    """
    prev_raw: Optional[str] = None
    next_raw: Optional[str] = None

    if ar is not None:
        ar_path = ar.get("cpth") or ar.get("ppth")
        ar_list = _split_path_list(ar_path)
        if ar_list:
            prev_raw = ar_list[-1]

    if dp is not None:
        dp_path = dp.get("cpth") or dp.get("ppth")
        dp_list = _split_path_list(dp_path)
        if dp_list:
            next_raw = dp_list[0]

    return prev_raw, next_raw


def _delay_minutes(planned: Optional[datetime], changed: Optional[datetime]) -> Optional[int]:
    if planned is None or changed is None:
        return None
    delta = changed - planned
    return int(round(delta.total_seconds() / 60.0))



# Core ingestion (ONE changes snapshot)


def upsert_fact_movement_from_changes_snapshot(
    cur,
    snapshot_key: str,
    *,
    threshold: float,
    timetable_changes_root: str = "timetable_changes",
    page_size: int = 5000,
) -> int:
    """
    Adds support for "added stops" that exist only in timetable_changes:
    - Detect as added if any of:
        * ar/dp has ps="a"
        * ar/dp has cs="a"
        * stop_id suffix idx >= 100
    - If no base row exists in dw.fact_movement for (station_eva, stop_id) with snapshot_key <= S:
        * insert a new row using pt for planned_* (if present),
          ct for changed_* (if present),
          tl for train_id,
          (c)pth for prev/next.
    """
    ensure_dim_time(cur, snapshot_key)

    changes_glob = changes_glob_for_snapshot(snapshot_key, timetable_changes_root=timetable_changes_root)
    station_cache: Dict[str, Optional[int]] = {}
    train_map = load_train_map(cur)

    # (station_eva, stop_id) -> changed info (last wins within same snapshot)
    change_map: Dict[Tuple[int, str], Dict[str, object]] = {}

    for path in glob.glob(changes_glob, recursive=True):
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            continue

        stops = root.findall("./s")
        if not stops:
            continue

        station_eva = get_station_eva_for_changes_file(
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
            stop_id = (s.get("id") or "").strip()
            if not stop_id:
                continue

            ar = s.find("ar")
            dp = s.find("dp")

            # --- detect added stop (ps="a" or cs="a" or stop_id suffix >= 100)
            idx = _stop_id_suffix_int(stop_id)
            is_added = (
                _is_added_by_ps_or_cs(ar)
                or _is_added_by_ps_or_cs(dp)
                or (idx is not None and idx >= 100)
            )


            tl = s.find("tl")
            cat = (tl.get("c") or "").strip() if tl is not None else ""
            num = (tl.get("n") or "").strip() if tl is not None else ""
            train_id = get_or_create_train_id(cur, train_map, category=cat, number=num)


            ar_cancel_update = _cancel_update_from_cs(ar)
            dp_cancel_update = _cancel_update_from_cs(dp)

            # times
            ar_ct = _parse_changed_time(ar)
            dp_ct = _parse_changed_time(dp)

            ar_pt = _parse_planned_time(ar)
            dp_pt = _parse_planned_time(dp)


            ar_hidden = _is_hidden(ar)
            dp_hidden = _is_hidden(dp)

            # path (use cpth else ppth)
            prev_raw, next_raw = _prev_next_from_any_path(ar=ar, dp=dp)

            changed_prev_eva: Optional[int] = None
            changed_next_eva: Optional[int] = None
            if prev_raw:
                changed_prev_eva = resolve_station_eva(
                    cur,
                    station_raw=prev_raw,
                    snapshot_key=snapshot_key,
                    source_path=path,
                    threshold=threshold,
                    cache=station_cache,
                )
            if next_raw:
                changed_next_eva = resolve_station_eva(
                    cur,
                    station_raw=next_raw,
                    snapshot_key=snapshot_key,
                    source_path=path,
                    threshold=threshold,
                    cache=station_cache,
                )

            # prevent self-loops
            if changed_prev_eva == station_eva:
                changed_prev_eva = None
            if changed_next_eva == station_eva:
                changed_next_eva = None

            has_cancel_signal = (ar_cancel_update is not None) or (dp_cancel_update is not None)

            # Keep only meaningful changes OR added stops (because added stops may only have pt/ct/ppth/cpth)
            has_any_time_signal = bool(ar_ct or dp_ct or ar_pt or dp_pt)
            has_any_path_signal = bool(changed_prev_eva or changed_next_eva)
            if not (has_any_time_signal or has_cancel_signal or has_any_path_signal or is_added):
                continue

            change_map[(station_eva, stop_id)] = {
                "is_added": is_added,
                "train_id": train_id,

                "planned_arrival_ts_from_pt": ar_pt,
                "planned_departure_ts_from_pt": dp_pt,

                "changed_arrival_ts": ar_ct,
                "changed_departure_ts": dp_ct,

                "arrival_cancel_update": ar_cancel_update,
                "departure_cancel_update": dp_cancel_update,

                "changed_previous_station_eva": changed_prev_eva,
                "changed_next_station_eva": changed_next_eva,

                "arrival_is_hidden": ar_hidden,
                "departure_is_hidden": dp_hidden,
            }

    if not change_map:
        return 0

    wanted_list = list(change_map.keys())

    cur.execute(
        """
        with wanted(station_eva, stop_id) as (
            select * from unnest(%s::bigint[], %s::text[])
        ),
        latest as (
            select distinct on (fm.station_eva, fm.stop_id)
                fm.station_eva, fm.stop_id,
                fm.train_id,
                fm.planned_arrival_ts, fm.planned_departure_ts,
                fm.previous_station_eva, fm.next_station_eva,
                fm.arrival_is_hidden, fm.departure_is_hidden,
                fm.arrival_cancelled, fm.departure_cancelled
            from wanted w
            join dw.fact_movement fm
              on fm.station_eva = w.station_eva
             and fm.stop_id = w.stop_id
             and fm.snapshot_key <= %s
            order by fm.station_eva, fm.stop_id, fm.snapshot_key desc
        )
        select
            station_eva, stop_id, train_id,
            planned_arrival_ts, planned_departure_ts,
            previous_station_eva, next_station_eva,
            arrival_is_hidden, departure_is_hidden,
            arrival_cancelled, departure_cancelled
        from latest
        """,
        (
            [int(eva) for (eva, _sid) in wanted_list],
            [str(sid) for (_eva, sid) in wanted_list],
            snapshot_key,
        ),
    )
    base_rows = cur.fetchall()

    base_map: Dict[Tuple[int, str], Tuple] = {}
    for r in base_rows:
        base_map[(int(r[0]), str(r[1]))] = r

    out_rows: List[Tuple] = []
    for key, ch in change_map.items():
        base = base_map.get(key)

        station_eva, stop_id = key
        is_added = bool(ch.get("is_added"))
        train_id_from_xml = ch.get("train_id")
        pt_ar = ch.get("planned_arrival_ts_from_pt")
        pt_dp = ch.get("planned_departure_ts_from_pt")

        changed_ar_ts: Optional[datetime] = ch.get("changed_arrival_ts")  
        changed_dp_ts: Optional[datetime] = ch.get("changed_departure_ts")

        ar_update = ch.get("arrival_cancel_update")   # Optional[bool]
        dp_update = ch.get("departure_cancel_update") # Optional[bool]

        changed_prev_eva = ch.get("changed_previous_station_eva")
        changed_next_eva = ch.get("changed_next_station_eva")

        xml_ar_hidden = bool(ch.get("arrival_is_hidden"))
        xml_dp_hidden = bool(ch.get("departure_is_hidden"))

        if base is None:
            # Only allowed if it's an "added stop" 
            if not is_added:
                continue

            # Build "base" from XML
            train_id = int(train_id_from_xml) if train_id_from_xml is not None else None
            if train_id is None:
                # cannot insert without train_id (fact_movement.train_id is NOT NULL in your schema)
                continue

            planned_ar_ts = pt_ar if isinstance(pt_ar, datetime) else None
            planned_dp_ts = pt_dp if isinstance(pt_dp, datetime) else None

            base_prev_eva = None
            base_next_eva = None

            # For added stops, treat changed_prev/next as the base prev/next (it is the best we have)
            if isinstance(changed_prev_eva, int):
                base_prev_eva = changed_prev_eva
            if isinstance(changed_next_eva, int):
                base_next_eva = changed_next_eva

            # If cs is absent on an added stop, default to not cancelled.
            arrival_cancelled = False if ar_update is None else bool(ar_update)
            departure_cancelled = False if dp_update is None else bool(dp_update)

            arrival_delay_min = None
            departure_delay_min = None
            if (not arrival_cancelled) and (planned_ar_ts is not None) and (changed_ar_ts is not None):
                arrival_delay_min = _delay_minutes(planned_ar_ts, changed_ar_ts)
            if (not departure_cancelled) and (planned_dp_ts is not None) and (changed_dp_ts is not None):
                departure_delay_min = _delay_minutes(planned_dp_ts, changed_dp_ts)

            out_rows.append(
                (
                    snapshot_key,
                    int(station_eva),
                    int(train_id),
                    str(stop_id),

                    planned_ar_ts,
                    planned_dp_ts,
                    base_prev_eva,
                    base_next_eva,

                    changed_ar_ts,
                    changed_dp_ts,
                    changed_prev_eva if isinstance(changed_prev_eva, int) else None,
                    changed_next_eva if isinstance(changed_next_eva, int) else None,

                    arrival_cancelled,
                    departure_cancelled,

                    arrival_delay_min,
                    departure_delay_min,

                    bool(xml_ar_hidden),
                    bool(xml_dp_hidden),
                )
            )
            continue

        (
            _base_station_eva,
            _base_stop_id,
            base_train_id,
            planned_ar_ts,
            planned_dp_ts,
            base_prev_eva,
            base_next_eva,
            base_ar_hidden,
            base_dp_hidden,
            base_ar_cancelled,
            base_dp_cancelled,
        ) = base

        if planned_ar_ts is None and isinstance(pt_ar, datetime):
            planned_ar_ts = pt_ar
        if planned_dp_ts is None and isinstance(pt_dp, datetime):
            planned_dp_ts = pt_dp

        # carry forward cancellation unless explicitly updated by cs
        arrival_cancelled = base_ar_cancelled if ar_update is None else bool(ar_update)
        departure_cancelled = base_dp_cancelled if dp_update is None else bool(dp_update)

        # delay minutes only if not cancelled and ct exists
        arrival_delay_min = None
        departure_delay_min = None

        if (not arrival_cancelled) and (changed_ar_ts is not None) and (planned_ar_ts is not None):
            arrival_delay_min = _delay_minutes(planned_ar_ts, changed_ar_ts)
        
        if (not departure_cancelled) and (changed_dp_ts is not None) and (planned_dp_ts is not None):
            departure_delay_min = _delay_minutes(planned_dp_ts, changed_dp_ts)

        # if we have hidden flags from XML, use them; else carry forward base flags
        out_ar_hidden = bool(xml_ar_hidden) if (ar is not None or dp is not None) else bool(base_ar_hidden)
        out_dp_hidden = bool(xml_dp_hidden) if (ar is not None or dp is not None) else bool(base_dp_hidden)

        out_rows.append(
            (
                snapshot_key,
                int(station_eva),
                int(base_train_id),
                str(stop_id),

                planned_ar_ts,
                planned_dp_ts,
                base_prev_eva,
                base_next_eva,

                changed_ar_ts,
                changed_dp_ts,
                changed_prev_eva if isinstance(changed_prev_eva, int) else None,
                changed_next_eva if isinstance(changed_next_eva, int) else None,

                arrival_cancelled,
                departure_cancelled,

                arrival_delay_min,
                departure_delay_min,

                bool(out_ar_hidden),
                bool(out_dp_hidden),
            )
        )

    if not out_rows:
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
            -- keep planned context for this snapshot row
            train_id = excluded.train_id,
            planned_arrival_ts = excluded.planned_arrival_ts,
            planned_departure_ts = excluded.planned_departure_ts,
            previous_station_eva = excluded.previous_station_eva,
            next_station_eva = excluded.next_station_eva,
            arrival_is_hidden = excluded.arrival_is_hidden,
            departure_is_hidden = excluded.departure_is_hidden,

            -- overwrite changed fields for this snapshot
            changed_arrival_ts = excluded.changed_arrival_ts,
            changed_departure_ts = excluded.changed_departure_ts,
            changed_previous_station_eva = excluded.changed_previous_station_eva,
            changed_next_station_eva = excluded.changed_next_station_eva,
            arrival_cancelled = excluded.arrival_cancelled,
            departure_cancelled = excluded.departure_cancelled,
            arrival_delay_min = excluded.arrival_delay_min,
            departure_delay_min = excluded.departure_delay_min
        """,
        out_rows,
        page_size=page_size,
    )

    return len(out_rows)


# Ingest ALL changes snapshots

def upsert_fact_movement_from_all_timetable_changes(
    cur,
    *,
    timetable_changes_root: str = "timetable_changes",
    threshold: float,
    page_size: int = 5000,
    commit_every: int = 1,
) -> Dict[str, int]:
    snapshot_keys = sorted(set(iter_change_snapshots(timetable_changes_root)))
    results: Dict[str, int] = {}

    for i, sk in enumerate(snapshot_keys, start=1):
        n = upsert_fact_movement_from_changes_snapshot(
            cur,
            sk,
            threshold=threshold,
            timetable_changes_root=timetable_changes_root,
            page_size=page_size,
        )
        results[sk] = n

        if commit_every > 0 and hasattr(cur, "connection") and (i % commit_every == 0):
            cur.connection.commit()

    return results
