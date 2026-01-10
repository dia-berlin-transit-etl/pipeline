# ingestion.py
from __future__ import annotations

import argparse
import psycopg2

from etl.ingest_stations import upsert_dim_station_from_json, print_dim_station_preview
from etl.ingest_trains import upsert_dim_train_from_timetables



def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="public_transport_db",
        user="efe",
        # no password -> libpq will use ~/.pgpass if it matches
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Berlin DB dataset into Postgres DW schema.")
    ap.add_argument(
        "--step",
        choices=["stations", "trains"],
        help="Which ingestion step to run (start simple; add more steps later).",
    )
    
    args = ap.parse_args()

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if args.step in ("stations"):
                mapping = upsert_dim_station_from_json(cur, "DBahn-berlin/station_data.json")
                conn.commit()

                # Show results
                print_dim_station_preview(cur)

                # Optional: show a couple normalized keys to confirm normalization
                sample = list(mapping.items())[:10]
                print("\nSample normalized-name -> EVA mapping (first 10):")
                for k, v in sample:
                    print(f"- {k} -> {v}")
            elif args.step in ("trains"):
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


    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
