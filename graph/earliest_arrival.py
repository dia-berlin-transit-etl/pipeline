# earliest_arrival.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional, Dict, Tuple

import psycopg2

from postgres_connector import PostgresConnector

def get_conn():
    return PostgresConnector().connect()


def parse_snapshot_key_to_dt(k: str | int) -> datetime:
    s = str(k).strip()
    if len(s) != 10 or not s.isdigit():
        raise ValueError(f"Invalid snapshot key (expected 10 digits YYMMDDHHmm): {k!r}")
    yy = int(s[0:2])
    mm = int(s[2:4])
    dd = int(s[4:6])
    hh = int(s[6:8])
    mi = int(s[8:10])
    return datetime(2000 + yy, mm, dd, hh, mi)


def eva_by_station_name(conn, station_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT station_eva FROM dw.dim_station WHERE station_name = %s",
            (station_name,),
        )
        rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Station name not found: {station_name!r}")
    if len(rows) > 1:
        evas = [int(r[0]) for r in rows]
        raise ValueError(f"Ambiguous station_name {station_name!r} (multiple EVA): {evas}")

    return int(rows[0][0])


@dataclass(frozen=True)
class Connection:
    from_station: int
    to_station: int
    dep_ts: datetime
    arr_ts: datetime
    train_id: int


# Row type from SQL (latest-as-of per (station_eva, stop_id))
# (station_eva, train_id, planned_arr, planned_dep, changed_arr, changed_dep,
#  arr_cancel, dep_cancel, arr_hidden, dep_hidden)
LatestRow = Tuple[int, int, Optional[datetime], Optional[datetime], Optional[datetime], Optional[datetime],
                  bool, bool, bool, bool]


def load_latest_movements_as_of(conn, snapshot_key: str) -> List[LatestRow]:
    sql = """
    WITH latest AS (
      SELECT DISTINCT ON (fm.station_eva, fm.stop_id)
        fm.station_eva,
        fm.train_id,
        fm.planned_arrival_ts,
        fm.planned_departure_ts,
        fm.changed_arrival_ts,
        fm.changed_departure_ts,
        fm.arrival_cancelled,
        fm.departure_cancelled,
        fm.arrival_is_hidden,
        fm.departure_is_hidden,
        fm.snapshot_key,
        fm.movement_key
      FROM dw.fact_movement fm
      WHERE fm.snapshot_key <= %s
      ORDER BY fm.station_eva, fm.stop_id, fm.snapshot_key DESC, fm.movement_key DESC
    )
    SELECT
      station_eva,
      train_id,
      planned_arrival_ts,
      planned_departure_ts,
      changed_arrival_ts,
      changed_departure_ts,
      arrival_cancelled,
      departure_cancelled,
      arrival_is_hidden,
      departure_is_hidden
    FROM latest;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_key,))
        return cur.fetchall()


def _effective_arrival(planned_arr: Optional[datetime], changed_arr: Optional[datetime]) -> Optional[datetime]:
    return changed_arr or planned_arr


def _effective_departure(planned_dep: Optional[datetime], changed_dep: Optional[datetime]) -> Optional[datetime]:
    return changed_dep or planned_dep


def build_connections(rows: List[LatestRow]) -> List[Connection]:
    """
    Group by (train_id, service_date), sort stops by time, connect consecutive stops.
    """
    # runs[(train_id, service_date)] = list of (station_eva, eff_arr, eff_dep, flags...)
    runs: Dict[Tuple[int, date], List[tuple]] = {}

    for (
        station_eva, train_id,
        planned_arr, planned_dep, changed_arr, changed_dep,
        arr_cancel, dep_cancel, arr_hidden, dep_hidden
    ) in rows:
        eff_dep = _effective_departure(planned_dep, changed_dep)
        eff_arr = _effective_arrival(planned_arr, changed_arr)

        if eff_dep is not None:
            service_dt = eff_dep.date()
        elif eff_arr is not None:
            service_dt = eff_arr.date()
        else:
            continue  # no time -> unusable

        runs.setdefault((int(train_id), service_dt), []).append(
            (int(station_eva), eff_arr, eff_dep, arr_cancel, dep_cancel, arr_hidden, dep_hidden)
        )

    connections: List[Connection] = []

    for (train_id, _service_dt), stops in runs.items():
        # sort by "best available time" (dep preferred)
        stops_sorted = sorted(stops, key=lambda x: x[2] or x[1])  # eff_dep or eff_arr

        for i in range(len(stops_sorted) - 1):
            a_station, a_arr, a_dep, a_arr_cancel, a_dep_cancel, a_arr_hidden, a_dep_hidden = stops_sorted[i]
            b_station, b_arr, b_dep, b_arr_cancel, b_dep_cancel, b_arr_hidden, b_dep_hidden = stops_sorted[i + 1]

            if a_station == b_station:
                continue

            # usable departure at a
            if a_dep is None or a_dep_cancel or a_dep_hidden:
                continue

            # usable arrival at b
            if b_arr is None or b_arr_cancel or b_arr_hidden:
                continue

            if b_arr < a_dep:
                continue

            connections.append(
                Connection(
                    from_station=a_station,
                    to_station=b_station,
                    dep_ts=a_dep,
                    arr_ts=b_arr,
                    train_id=int(train_id),
                )
            )

    connections.sort(key=lambda c: c.dep_ts)
    return connections


def earliest_arrival_csa(
    connections: List[Connection],
    *,
    src_eva: int,
    dst_eva: int,
    depart_t0: datetime,
) -> tuple[Optional[datetime], List[Connection]]:
    best: Dict[int, datetime] = {src_eva: depart_t0}
    parent: Dict[int, tuple[int, Connection]] = {}

    for c in connections:
        best_dst = best.get(dst_eva)
        if best_dst is not None and c.dep_ts >= best_dst:
            break

        t_from = best.get(c.from_station)
        if t_from is None or t_from > c.dep_ts:
            continue

        t_to = best.get(c.to_station)
        if t_to is None or c.arr_ts < t_to:
            best[c.to_station] = c.arr_ts
            parent[c.to_station] = (c.from_station, c)

    if dst_eva not in best:
        return None, []

    path: List[Connection] = []
    cur = dst_eva
    while cur != src_eva:
        prev, conn_used = parent[cur]
        path.append(conn_used)
        cur = prev
    path.reverse()
    return best[dst_eva], path


def pretty_print_route(conn, src_eva: int, dst_eva: int, path: List[Connection], depart_t0: datetime) -> None:
    if not path:
        print("No route found.")
        return

    def name_of(eva: int) -> str:
        with conn.cursor() as cur:
            cur.execute("SELECT station_name FROM dw.dim_station WHERE station_eva = %s", (eva,))
            row = cur.fetchone()
        return row[0] if row else str(eva)

    print("\nRoute:")
    print(f"  Start: {name_of(src_eva)} (EVA {src_eva}) at {depart_t0}")
    for i, c in enumerate(path, start=1):
        print(
            f"  {i}) {name_of(c.from_station)} -> {name_of(c.to_station)} | "
            f"dep {c.dep_ts} | arr {c.arr_ts} | train_id={c.train_id}"
        )
    print(f"  End:   {name_of(dst_eva)} (EVA {dst_eva}) at {path[-1].arr_ts}")
    print(f"  Transfers (hops): {len(path) - 1}")


if __name__ == "__main__":
    DEPART_T0_KEY = "2510131100"
    DEPART_T0 = parse_snapshot_key_to_dt(DEPART_T0_KEY)

    SOURCE_STATION = "Berlin-Buch"
    TARGET_STATION = "Berlin Yorckstr.(S2)"

    with get_conn() as conn:
        src_eva = eva_by_station_name(conn, SOURCE_STATION)
        dst_eva = eva_by_station_name(conn, TARGET_STATION)

        latest_rows = load_latest_movements_as_of(conn, DEPART_T0_KEY)
        connections = build_connections(latest_rows)

        connections = [c for c in connections if c.dep_ts >= DEPART_T0]

        ea, path = earliest_arrival_csa(
            connections,
            src_eva=src_eva,
            dst_eva=dst_eva,
            depart_t0=DEPART_T0,
        )

        print("\nEarliest arrival:", ea)
        pretty_print_route(conn, src_eva, dst_eva, path, DEPART_T0)

