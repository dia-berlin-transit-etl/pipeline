from __future__ import annotations

import os
import re
import json
import glob
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional, Tuple

import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET


def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="public_transport_db",
        user="efe",
        # no password -> libpq will use ~/.pgpass if it matches
    )


def normalize_station_name(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("ß", "ss")
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\bberlin\b", " ", s) # remove "berlin" as a standalone word
    s = re.sub(r"\s+", " ", s).strip()
    return s


def upsert_dim_station_from_json(cur, station_json_path: str) -> dict[str, list[int]]:
    """
    Inserts one row per *main* EVA number (evaNumbers[].isMain == true) into dw.dim_station.
    Returns mapping: normalized_station_name -> list of eva numbers (ints).
    """
    with open(station_json_path, "r", encoding="utf-8") as f:
        stations_obj = json.load(f)
        stations = stations_obj["result"]

    name_to_evas: dict[str, list[int]] = {}
    rows = []

    for st in stations:
        name = st.get("name")
        evas = st.get("evaNumbers") or []
        if not name or not evas:
            continue

        norm = normalize_station_name(name)
        eva_list: list[int] = []

        for e in evas:
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

    for k, v in name_to_evas.items():
        name_to_evas[k] = sorted(set(v))

    return name_to_evas


def print_dim_station_preview(cur, limit: int = 25) -> None:
    # count rows
    cur.execute("select count(*) from dw.dim_station;")
    count = cur.fetchone()[0]
    print(f"dw.dim_station rows: {count}")

    # preview a few rows
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


def main() -> None:
    station_json_path = "DBahn-berlin/station_data.json"

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            mapping = upsert_dim_station_from_json(cur, station_json_path)
            conn.commit()

            # Show results
            print_dim_station_preview(cur, limit=30)

            # Optional: show a couple normalized keys to confirm "Berlin" stripping
            sample = list(mapping.items())[:10]
            print("\nSample normalized-name -> EVA mapping (first 10):")
            for k, v in sample:
                print(f"- {k} -> {v}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()