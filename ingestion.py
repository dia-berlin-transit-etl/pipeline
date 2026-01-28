from __future__ import annotations

import argparse
import re

import psycopg2

from etl.stations import upsert_dim_station_from_json, print_dim_station_preview
from etl.trains import upsert_dim_train_from_timetables
from etl.time_dim import upsert_dim_time_from_paths, print_dim_time_preview
from etl.fact_planned import upsert_fact_movement_from_all_timetables

# NEW: changes ingestion
from etl.fact_changed import upsert_fact_movement_from_all_timetable_changes


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
        choices=["stations", "trains", "time", "planned", "changed"],
        help="Which ingestion step to run.",
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
                n = upsert_dim_station_from_json(cur, "./station_data.json")
                conn.commit()

                print(f"Upserted station rows (attempted): {n}")
                print_dim_station_preview(cur, limit=30)

            elif args.step == "trains":
                _ = upsert_dim_train_from_timetables(cur, "timetables/**/*.xml")
                conn.commit()

                cur.execute("select count(*) from dw.dim_train;")
                print("dw.dim_train rows:", cur.fetchone()[0])

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
                # include BOTH sources so changed snapshots pass the FK on fact_movement.snapshot_key
                _ = upsert_dim_time_from_paths(
                    cur,
                    snapshot_globs=["timetables/**/*.xml", "timetable_changes/**/*.xml"],
                )
                conn.commit()
                print_dim_time_preview(cur, limit=30)

            elif args.step == "planned":
                threshold = args.threshold
                commit_every = 10

                results = upsert_fact_movement_from_all_timetables(
                    cur,
                    timetables_root="timetables",
                    threshold=threshold,
                    page_size=5000,
                    commit_every=commit_every,
                )
                conn.commit()

                cur.execute("select count(*) from dw.fact_movement;")
                total_fact = cur.fetchone()[0]

                cur.execute("select count(*) from dw.needs_review;")
                review_cnt = cur.fetchone()[0]

                top = sorted(results.items(), key=lambda kv: kv[0])[:10]
                print(f"planned ingestion done for ALL snapshots (threshold={threshold})")
                print(f"snapshots processed: {len(results)}")
                print(f"total prepared rows (sum): {sum(results.values())}")
                print(f"total fact_movement rows now: {total_fact}")
                print(f"needs_review total rows: {review_cnt}")
                print("first 10 snapshot counts:", top)

                cur.execute(
                    "select auto_linked, count(*) from dw.station_resolve_log group by auto_linked order by auto_linked;"
                )
                print("resolve log breakdown (overall):", cur.fetchall())

            elif args.step == "changed":
                # IMPORTANT: run --step time first (or keep time globs above),
                # so dim_time contains 15-min snapshots.
                threshold = args.threshold
                commit_every = 10

                results = upsert_fact_movement_from_all_timetable_changes(
                    cur,
                    timetable_changes_root="timetable_changes",
                    threshold=threshold,
                    page_size=5000,
                    commit_every=commit_every,
                )
                conn.commit()

                cur.execute("select count(*) from dw.fact_movement;")
                total_fact = cur.fetchone()[0]

                # quick feedback
                cur.execute("select count(*) from dw.station_resolve_log;")
                resolve_log_total = cur.fetchone()[0]

                cur.execute("select count(*) from dw.needs_review;")
                review_cnt = cur.fetchone()[0]

                top = sorted(results.items(), key=lambda kv: kv[0])[:10]
                print(f"changed ingestion done for ALL snapshots (threshold={threshold})")
                print(f"snapshots processed: {len(results)}")
                print(f"total upserted rows (sum): {sum(results.values())}")
                print(f"total fact_movement rows now: {total_fact}")
                print(f"station_resolve_log total rows now: {resolve_log_total}")
                print(f"needs_review total rows: {review_cnt}")
                print("first 10 snapshot counts:", top)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
