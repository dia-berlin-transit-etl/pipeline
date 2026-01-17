from __future__ import annotations

import glob
from typing import Dict, Tuple

import psycopg2.extras
import xml.etree.ElementTree as ET


def upsert_dim_train_from_timetables(cur, timetables_glob: str) -> Dict[Tuple[str, str], int]:
    """
    Extracts (tl/@c, tl/@n) from timetable XMLs and upserts into dw.dim_train.
    Filters out category == 'Bus' (case-insensitive).
    Returns mapping: (category, train_number) -> train_id
    """
    pairs: set[Tuple[str, str]] = set()

    for path in glob.glob(timetables_glob, recursive=True):
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            continue

        for tl in root.findall(".//tl"):
            c = tl.get("c")  # category
            n = tl.get("n")  # train number as string
            if not c or not n:
                continue

            c = c.strip()
            n = n.strip()

            # FILTER: skip buses
            if c.lower() == "bus":
                continue

            pairs.add((c, n))

    rows = list(pairs)
    if rows:
        psycopg2.extras.execute_values(
            cur,
            """
            insert into dw.dim_train (category, train_number)
            values %s
            on conflict (category, train_number) do nothing
            """,
            rows,
            page_size=2000,
        )

    # build mapping for later fact ingestion
    cur.execute("select train_id, category, train_number from dw.dim_train;")
    train_map: Dict[Tuple[str, str], int] = {}
    for train_id, category, train_number in cur.fetchall():
        # keep map consistent with filter too (defensive)
        if (category or "").strip().lower() == "bus":
            continue
        train_map[(category, train_number)] = int(train_id)

    return train_map
