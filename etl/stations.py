from __future__ import annotations

import json
import re
from typing import Dict, List

import psycopg2.extras


def normalize_station_name(s: str) -> str:
    """
    Normalizes station names for internal matching:
    - lowercases
    - replaces German umlauts/ß
    - strips punctuation
    - removes 'berlin' as a standalone word
    """
    s = s.strip().lower()
    s = s.replace("ß", "ss")
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\bberlin\b", " ", s)  # remove "berlin" as a standalone word
    s = re.sub(r"\s+", " ", s).strip()
    return s


def upsert_dim_station_from_json(cur, station_json_path: str) -> Dict[str, List[int]]:
    """
    Inserts one row per *main* EVA number (evaNumbers[].isMain == true) into dw.dim_station.
    Returns mapping: normalized_station_name -> list of eva numbers (ints).
    """
    with open(station_json_path, "r", encoding="utf-8") as f:
        stations_obj = json.load(f)

    stations = stations_obj.get("result")
    if not isinstance(stations, list):
        raise ValueError(
            "station_data.json unexpected format: expected top-level key 'result' containing a list."
        )

    name_to_evas: Dict[str, List[int]] = {}
    rows = []

    for st in stations:
        if not isinstance(st, dict):
            continue

        name = st.get("name")
        evas = st.get("evaNumbers") or []
        if not name or not isinstance(evas, list):
            continue

        norm = normalize_station_name(name)
        eva_list: List[int] = []

        for e in evas:
            if not isinstance(e, dict):
                continue

            # Only keep the "main" EVA number(s)
            if e.get("isMain") is not True:
                continue

            eva_no = e.get("number")
            coords = (e.get("geographicCoordinates") or {}).get("coordinates")
            if eva_no is None or not coords or len(coords) != 2:
                continue

            lon, lat = coords[0], coords[1]
            rows.append((int(eva_no), name, float(lon), float(lat)))
            eva_list.append(int(eva_no))

        if eva_list:
            name_to_evas.setdefault(norm, []).extend(eva_list)

    if rows:
        psycopg2.extras.execute_values(
            cur,
            """
            insert into dw.dim_station (station_eva, station_name, lon, lat)
            values %s
            on conflict (station_eva) do update
            set station_name = excluded.station_name,
                lon = excluded.lon,
                lat = excluded.lat
            """,
            rows,
            page_size=1000,
        )

    # de-dup eva lists
    for k, v in name_to_evas.items():
        name_to_evas[k] = sorted(set(v))

    return name_to_evas


def print_dim_station_preview(cur, limit: int = 25) -> None:
    """
    Prints a quick preview of dim_station for sanity checking.
    """
    cur.execute("select count(*) from dw.dim_station;")
    count = cur.fetchone()[0]
    print(f"dw.dim_station rows: {count}")

    cur.execute(
        """
        select station_eva, station_name, lat, lon
        from dw.dim_station
        order by station_name
        limit %s
        """,
        (limit,),
    )
    rows = cur.fetchall()

    print(f"\nFirst {min(limit, len(rows))} stations (sorted by name):")
    for eva, name, lat, lon in rows:
        print(f"- {name} | EVA={eva} | lat={lat:.6f} lon={lon:.6f}")
