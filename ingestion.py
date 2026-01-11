# ingestion.py
from __future__ import annotations

import argparse
import os
import re
from typing import List

from etl.stations import upsert_dim_station_from_json, print_dim_station_preview
from etl.trains import upsert_dim_train_from_timetables
from etl.time_dim import upsert_dim_time_from_paths, print_dim_time_preview
from etl.fact_planned import upsert_fact_movement_from_timetables

import psycopg2


def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="public_transport_db",
        user="efe",
        # no password -> libpq will use ~/.pgpass if it matches
    )

_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")

def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Berlin DB dataset into Postgres DW schema.")
    ap.add_argument(
        "--step",
        choices=["stations", "trains", "time", "planned"],
        help="Which ingestion step to run.",
    )

    # planned step args
    ap.add_argument(
        "--snapshot",
        help="Snapshot key YYMMDDHHmm (e.g. 2509021400). Required for --step planned.",
    )
    ap.add_argument(
        "--threshold",
        type=float,
        default=0.85,
        help="Auto-link threshold for station name resolution (pg_trgm similarity).",
    )
    
    args = ap.parse_args()

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if args.step == "stations":
                n = upsert_dim_station_from_json(cur, "DBahn-berlin/station_data.json")
                conn.commit()

                print(f"Upserted station rows (attempted): {n}")
                print_dim_station_preview(cur, limit=30)
            
            elif args.step == "trains":
                train_map = upsert_dim_train_from_timetables(cur, "timetables/**/*.xml")
                conn.commit()

                cur.execute("select count(*) from dw.dim_train;")
                print("dw.dim_train rows:", cur.fetchone()[0])

                # show a few examples
                cur.execute(
                    """
                    select category, train_number
                    from dw.dim_train
                    order by category, train_number
                    limit 20
                    """
                )
                print("\nFirst 20 trains:")
                for c, n in cur.fetchall():
                    print(f"- {c} {n}")
            
            elif args.step == "time":
                keys = upsert_dim_time_from_paths(
                    cur,
                    snapshot_globs=["timetables/**/*.xml", "timetable_changes/**/*.xml"],
                )
                conn.commit()
                print_dim_time_preview(cur, limit=30)
            
            elif args.step == "planned":
                if not args.snapshot or not _SNAPSHOT_KEY_RE.match(args.snapshot):
                    raise SystemExit("For --step planned you must pass --snapshot YYMMDDHHmm (e.g. 2509021400).")

                snapshot_key = args.snapshot

                # IMPORTANT: only load XMLs for THIS snapshot folder
                # Layout you described: timetables/week/hour/station_timetable.xml
                # so hour folder name == snapshot_key
                timetables_glob = os.path.join("timetables", "**", snapshot_key, "*.xml")

                n = upsert_fact_movement_from_timetables(
                    cur,
                    snapshot_key=snapshot_key,
                    timetables_glob=timetables_glob,
                    threshold=args.threshold,
                )
                conn.commit()

                # quick feedback
                cur.execute("select count(*) from dw.fact_movement where snapshot_key=%s;", (snapshot_key,))
                cnt = cur.fetchone()[0]

                cur.execute("select count(*) from dw.needs_review;")
                review_cnt = cur.fetchone()[0]

                cur.execute(
                    "select auto_linked, count(*) from dw.station_resolve_log where snapshot_key=%s group by auto_linked;",
                    (snapshot_key,),
                )
                rows = cur.fetchall()

                print(f"planned ingestion done for snapshot={snapshot_key}")
                print(f"prepared rows: {n}")
                print(f"fact rows now for snapshot: {cnt}")
                print(f"needs_review total rows: {review_cnt}")
                print("resolve log breakdown (this snapshot):", rows)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
