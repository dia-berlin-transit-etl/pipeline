from __future__ import annotations

import json
import re
from typing import List, Tuple

import psycopg2.extras


def to_station_search_name(name: str) -> str:
    """
    Search helper string used for pg_trgm / fuzzystrmatch lookups.
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


def upsert_dim_station_from_json(cur, station_json_path: str) -> int:
    """
    Inserts one row per *main* EVA number (evaNumbers[].isMain == true) into dw.dim_station.
    Returns number of rows inserted/updated (attempted).
    """
    with open(station_json_path, "r", encoding="utf-8") as f:
        stations_obj = json.load(f)

    stations = stations_obj.get("result")
    if not isinstance(stations, list):
        raise ValueError("station_data.json unexpected format: expected top-level key 'result' containing a list.")

    rows: List[Tuple[int, str, str, float, float]] = []

    for st in stations:
        if not isinstance(st, dict):
            continue

        name = st.get("name")
        evas = st.get("evaNumbers") or []
        if not name or not isinstance(evas, list):
            continue

        for e in evas:
            if not isinstance(e, dict):
                continue
            if e.get("isMain") is not True:
                continue

            eva_no = e.get("number")
            coords = (e.get("geographicCoordinates") or {}).get("coordinates")
            if eva_no is None or not coords or len(coords) != 2:
                continue

            lon, lat = coords[0], coords[1]
            rows.append(
                (
                    int(eva_no),
                    name,
                    to_station_search_name(name),
                    float(lon),
                    float(lat),
                )
            )

    if not rows:
        return 0

    psycopg2.extras.execute_values(
        cur,
        """
        insert into dw.dim_station (station_eva, station_name, station_name_search, lon, lat)
        values %s
        on conflict (station_eva) do update
        set station_name = excluded.station_name,
            station_name_search = excluded.station_name_search,
            lon = excluded.lon,
            lat = excluded.lat
        """,
        rows,
        page_size=1000,
    )

    return len(rows)


def print_dim_station_preview(cur, limit: int = 30) -> None:
    cur.execute("select count(*) from dw.dim_station;")
    count = cur.fetchone()[0]
    print(f"dw.dim_station rows: {count}")

    cur.execute(
        """
        select station_eva, station_name, station_name_search, lat, lon
        from dw.dim_station
        order by station_name
        limit %s
        """,
        (limit,),
    )
    rows = cur.fetchall()

    print(f"\nFirst {min(limit, len(rows))} stations (sorted by name):")
    for eva, name, search, lat, lon in rows:
        print(f"- {name} | EVA={eva} | search='{search}' | lat={lat:.6f} lon={lon:.6f}")
