import heapq
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import networkx as nx
import folium
from graph_setup import get_conn, load_station_nodes, load_planned_edges


def load_graph(conn) -> nx.Graph:
    """Build graph with station nodes and edges from the database."""
    G = nx.Graph()
    load_station_nodes(G, conn)
    load_planned_edges(G, conn)
    return G


# Task 4.1: Shortest path by graph hops


def find_shortest_path(G: nx.Graph, start: int, end: int) -> List[Tuple[str, int]]:
    """Find shortest path between two stations by number of hops (edges)."""
    if start not in G or end not in G:
        return []
    try:
        path = nx.shortest_path(G, source=start, target=end)
        return [(G.nodes[n].get("name", "?"), n) for n in path]
    except nx.NetworkXNoPath:
        return []


# Task 4.2: Earliest arrival time


def load_timetable(conn, departure_time: datetime, hours_ahead: int = 3) -> Dict:
    """Load timetable data for earliest arrival computation.

    Loads connections from multiple snapshots covering the time window.
    Uses both departure->next and previous->arrival to capture all edges.

    Returns dict mapping (from_eva, to_eva) -> list of (departure_ts, arrival_ts, train_id)
    """
    snapshot_keys = []
    current = departure_time.replace(minute=0, second=0, microsecond=0)
    end_time = departure_time + timedelta(hours=hours_ahead)
    while current <= end_time:
        snapshot_keys.append(current.strftime("%y%m%d%H00"))
        current += timedelta(hours=1)

    # Get edges from both directions:
    # 1. station with planned_departure -> next_station
    # 2. previous_station -> station with planned_arrival
    sql = """
    SELECT DISTINCT from_eva, to_eva, departure_ts, arrival_ts, train_id FROM (
        -- Edges from departure side (station -> next_station)
        SELECT
            fm.station_eva AS from_eva,
            fm.next_station_eva AS to_eva,
            fm.planned_departure_ts AS departure_ts,
            COALESCE(
                (SELECT MIN(fm2.planned_arrival_ts)
                 FROM dw.fact_movement fm2
                 WHERE fm2.train_id = fm.train_id
                   AND fm2.station_eva = fm.next_station_eva
                   AND fm2.snapshot_key = ANY(%s)
                   AND fm2.planned_arrival_ts > fm.planned_departure_ts),
                fm.planned_departure_ts + INTERVAL '3 minutes'
            ) AS arrival_ts,
            fm.train_id
        FROM dw.fact_movement fm
        WHERE fm.snapshot_key = ANY(%s)
            AND fm.next_station_eva IS NOT NULL
            AND fm.planned_departure_ts IS NOT NULL
            AND NOT fm.departure_is_hidden

        UNION

        -- Edges from arrival side (previous_station -> station)
        SELECT
            fm.previous_station_eva AS from_eva,
            fm.station_eva AS to_eva,
            COALESCE(
                (SELECT MAX(fm2.planned_departure_ts)
                 FROM dw.fact_movement fm2
                 WHERE fm2.train_id = fm.train_id
                   AND fm2.station_eva = fm.previous_station_eva
                   AND fm2.snapshot_key = ANY(%s)
                   AND fm2.planned_departure_ts < fm.planned_arrival_ts),
                fm.planned_arrival_ts - INTERVAL '3 minutes'
            ) AS departure_ts,
            fm.planned_arrival_ts AS arrival_ts,
            fm.train_id
        FROM dw.fact_movement fm
        WHERE fm.snapshot_key = ANY(%s)
            AND fm.previous_station_eva IS NOT NULL
            AND fm.planned_arrival_ts IS NOT NULL
            AND NOT fm.arrival_is_hidden
    ) edges
    WHERE from_eva IS NOT NULL AND to_eva IS NOT NULL
    ORDER BY from_eva, departure_ts
    """

    timetable = {}
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_keys, snapshot_keys, snapshot_keys, snapshot_keys))
        for from_eva, to_eva, dep_ts, arr_ts, train_id in cur.fetchall():
            key = (int(from_eva), int(to_eva))
            if key not in timetable:
                timetable[key] = []
            conn_tuple = (dep_ts, arr_ts, train_id)
            if conn_tuple not in timetable[key]:
                timetable[key].append(conn_tuple)

    return timetable


def find_earliest_arrival(
    G: nx.Graph,
    timetable: Dict,
    train_info: Dict,
    start: int,
    end: int,
    departure_time: datetime,
) -> Tuple[
    datetime | None, List[Tuple[str, int, datetime | None, datetime | None, str | None]]
]:
    """Find earliest arrival using Dijkstra on time-expanded graph.

    Args:
        G: Station graph (for neighbors and station names)
        timetable: Dict mapping (from_eva, to_eva) -> [(dep_ts, arr_ts, train_id), ...]
        train_info: Dict mapping train_id -> (category, train_number)
        start: Starting station EVA
        end: Destination station EVA
        departure_time: Earliest allowed departure time

    Returns:
        (arrival_time, route) where route is list of (station_name, eva, departure, arrival, train)
    """
    if start not in G or end not in G:
        return None, []
    if start == end:
        return departure_time, [
            (G.nodes[start].get("name", "?"), start, None, None, None)
        ]

    # Priority queue: (arrival_time, station_eva, path)
    pq = [(departure_time, start, [(start, None, departure_time, None)])]
    best_arrival: Dict[int, datetime] = {}

    while pq:
        current_time, station, path = heapq.heappop(pq)

        if station == end:
            route = []
            for eva, dep, arr, tid in path:
                name = G.nodes[eva].get("name", "?") if eva in G else "?"
                train_str = (
                    f"{train_info[tid][0]} {train_info[tid][1]}"
                    if tid and tid in train_info
                    else None
                )
                route.append((name, eva, dep, arr, train_str))
            return current_time, route

        if station in best_arrival and best_arrival[station] <= current_time:
            continue
        best_arrival[station] = current_time

        # Explore all outgoing connections from timetable
        for (from_eva, to_eva), connections in timetable.items():
            if from_eva != station:
                continue
            for dep_ts, arr_ts, train_id in connections:
                if dep_ts >= current_time and (
                    to_eva not in best_arrival or arr_ts < best_arrival[to_eva]
                ):
                    heapq.heappush(
                        pq,
                        (arr_ts, to_eva, path + [(to_eva, dep_ts, arr_ts, train_id)]),
                    )

    return None, []


# =============================================================================
# Utility functions
# =============================================================================


def resolve_station(name_or_eva, name_to_eva: dict) -> int | None:
    """Resolve station name/EVA to EVA number with fuzzy matching."""
    if isinstance(name_or_eva, int):
        return name_or_eva
    try:
        return int(name_or_eva)
    except ValueError:
        pass

    if name_or_eva in name_to_eva:
        return name_to_eva[name_or_eva]

    def norm(s):
        return (
            s.lower()
            .replace("hbf", "hauptbahnhof")
            .replace("bf", "bahnhof")
            .replace("str.", "straße")
            .replace("str", "straße")
        )

    query = norm(name_or_eva)
    for name, eva in name_to_eva.items():
        if norm(name) == query:
            return eva

    matches = [(n, e) for n, e in name_to_eva.items() if query in norm(n)]
    return matches[0][1] if matches else None


def export_path_map(G: nx.Graph, route, out_html="shortest_path_map.html"):
    """Export map with full network and highlighted path."""
    path_nodes = {eva for _, eva, *_ in route}
    path_edges = set()
    for i in range(len(route) - 1):
        u, v = route[i][1], route[i + 1][1]
        path_edges.add((min(u, v), max(u, v)))

    lats = [a["lat"] for _, a in G.nodes(data=True) if "lat" in a]
    lons = [a["lon"] for _, a in G.nodes(data=True) if "lon" in a]
    m = folium.Map(
        location=[sum(lats) / len(lats), sum(lons) / len(lons)],
        zoom_start=11,
        tiles="CartoDB Dark_Matter",
    )

    # Background edges
    for u, v, _ in G.edges(data=True):
        au, av = G.nodes[u], G.nodes[v]
        if "lat" in au and "lat" in av and (min(u, v), max(u, v)) not in path_edges:
            folium.PolyLine(
                [(au["lat"], au["lon"]), (av["lat"], av["lon"])],
                weight=2,
                opacity=0.25,
                color="#888",
            ).add_to(m)

    # Path edges, only those in the route
    for i in range(len(route) - 1):
        u, v = route[i][1], route[i + 1][1]
        au, av = G.nodes[u], G.nodes[v]
        tooltip = f"{au.get('name')} → {av.get('name')}"
        if len(route[i]) > 4 and route[i + 1][4]:  # Has train info
            tooltip += f" | {route[i + 1][4]}"
        folium.PolyLine(
            [(au["lat"], au["lon"]), (av["lat"], av["lon"])],
            weight=6,
            opacity=0.9,
            color="#00ff88",
            tooltip=tooltip,
        ).add_to(m)

    # Nodes
    for eva, a in G.nodes(data=True):
        if "lat" not in a:
            continue
        on_path = eva in path_nodes
        folium.CircleMarker(
            (a["lat"], a["lon"]),
            radius=10 if on_path else 5,
            color="#00ff88" if on_path else "#3388ff",
            fill=True,
            fill_opacity=0.9,
            tooltip=a.get("name"),
        ).add_to(m)

    # Legend
    start_name = route[0][0] if route else "?"
    end_name = route[-1][0] if route else "?"
    legend = f"""<div style="position:fixed;bottom:30px;left:30px;background:rgba(0,0,0,0.8);
        padding:15px;border-radius:8px;color:white;font-family:monospace">
        <b style="color:#00ff88">Route: {start_name} → {end_name}</b><br/>
        Stations: {len(route)}</div>"""
    m.get_root().html.add_child(folium.Element(legend))
    m.save(out_html)
    print(f"Wrote {out_html}")


if __name__ == "__main__":
    with get_conn() as conn:
        G = load_graph(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT station_name, station_eva FROM dw.dim_station")
            name_to_eva = dict(cur.fetchall())
            cur.execute("SELECT train_id, category, train_number FROM dw.dim_train")
            train_info = {t: (c, n) for t, c, n in cur.fetchall()}
            cur.execute("SELECT MIN(snapshot_key) FROM dw.dim_time WHERE minute = 0")
            sample_snapshot = cur.fetchone()[0]

    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # Task 4.1: Shortest path by hops
    start, end = "Berlin-Grunewald", "Nöldnerplatz"
    s_eva, e_eva = resolve_station(start, name_to_eva), resolve_station(
        end, name_to_eva
    )

    if s_eva and e_eva:
        print(f"\n=== Task 4.1: Shortest Path (hops) ===")
        route = find_shortest_path(G, s_eva, e_eva)
        print(f"{start} → {end}: {len(route)} stations")
        for i, (name, eva) in enumerate(route):
            print(f"  {i}. {name}")
        export_path_map(G, route, "shortest_path_map.html")

    # Task 4.2: Earliest arrival (departure time as input)
    departure_time = datetime(2025, 9, 3, 14, 0)  # Input: 2025-09-03 14:00

    if s_eva and e_eva:
        print(f"\n=== Task 4.2: Earliest Arrival ===")

        with get_conn() as conn:
            timetable = load_timetable(conn, departure_time, hours_ahead=3)

        arrival_time, route = find_earliest_arrival(
            G, timetable, train_info, s_eva, e_eva, departure_time
        )

        if arrival_time:
            print(f"{start} → {end}")
            print(f"Departure: {departure_time.strftime('%Y-%m-%d %H:%M')}")
            print(f"Arrival: {arrival_time.strftime('%Y-%m-%d %H:%M')}")
            print(
                f"Journey time: {int((arrival_time - departure_time).total_seconds() / 60)} minutes"
            )
            for name, eva, dep, arr, train in route:
                dep_str = dep.strftime("%H:%M") if dep else "--:--"
                arr_str = arr.strftime("%H:%M") if arr else "--:--"
                train_str = f" [{train}]" if train else ""
                print(f"  {name}: dep {dep_str}, arr {arr_str}{train_str}")
            export_path_map(G, route, "earliest_arrival_map.html")
        else:
            print(f"No connection found from {start} to {end} after {departure_time}")
