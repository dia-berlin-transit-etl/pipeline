import psycopg2
import networkx as nx
import folium

from postgres_connector import PostgresConnector

def get_conn():
    return PostgresConnector().connect()

def load_station_nodes(G: nx.Graph, conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT station_eva, station_name, lon, lat
            FROM dw.dim_station
            """
        )
        rows = cur.fetchall()

    for eva, name, lon, lat in rows:
        G.add_node(int(eva), name=name, lon=float(lon), lat=float(lat))

    return len(rows)


def load_planned_edges(G: nx.Graph, conn) -> int:
    """
    Adds undirected edges between stations based on planned topology only (baseline snapshots only),
    and attaches lightweight metadata per edge:
      - sample category/train_number + sample snapshot_key + sample stop_id
      - n_distinct_trains + first/last snapshot seen
    """
    sql = """
    WITH base_snapshots AS (
      SELECT snapshot_key
      FROM dw.dim_time
      WHERE minute = 0
    ),
    edge_occ AS (
      -- prev -> current (arrival side)
      SELECT
        LEAST(fm.previous_station_eva, fm.station_eva) AS u,
        GREATEST(fm.previous_station_eva, fm.station_eva) AS v,
        fm.snapshot_key,
        fm.stop_id,
        fm.train_id
      FROM dw.fact_movement fm
      JOIN base_snapshots bs ON bs.snapshot_key = fm.snapshot_key
      WHERE fm.previous_station_eva IS NOT NULL
        AND fm.previous_station_eva <> fm.station_eva
        AND fm.arrival_is_hidden = FALSE

      UNION ALL

      -- current -> next (departure side)
      SELECT
        LEAST(fm.station_eva, fm.next_station_eva) AS u,
        GREATEST(fm.station_eva, fm.next_station_eva) AS v,
        fm.snapshot_key,
        fm.stop_id,
        fm.train_id
      FROM dw.fact_movement fm
      JOIN base_snapshots bs ON bs.snapshot_key = fm.snapshot_key
      WHERE fm.next_station_eva IS NOT NULL
        AND fm.next_station_eva <> fm.station_eva
        AND fm.departure_is_hidden = FALSE
    ),
    agg AS (
      SELECT
        u, v,
        COUNT(DISTINCT train_id) AS n_distinct_trains,
        MIN(snapshot_key) AS first_seen_snapshot,
        MAX(snapshot_key) AS last_seen_snapshot
      FROM edge_occ
      GROUP BY u, v
    ),
    sample AS (
      -- "first we stumble upon": pick a stable representative row per (u,v)
      SELECT DISTINCT ON (u, v)
        u, v,
        snapshot_key AS sample_snapshot,
        stop_id     AS sample_stop_id,
        train_id    AS sample_train_id
      FROM edge_occ
      ORDER BY u, v, snapshot_key ASC, stop_id ASC
    )
    SELECT
      a.u, a.v,
      dt.category,
      dt.train_number,
      s.sample_snapshot,
      s.sample_stop_id,
      a.n_distinct_trains,
      a.first_seen_snapshot,
      a.last_seen_snapshot
    FROM agg a
    JOIN sample s USING (u, v)
    JOIN dw.dim_train dt ON dt.train_id = s.sample_train_id
    WHERE a.u IS NOT NULL AND a.v IS NOT NULL;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    for (
        u,
        v,
        category,
        train_number,
        sample_snapshot,
        sample_stop_id,
        n_distinct_trains,
        first_seen_snapshot,
        last_seen_snapshot,
    ) in rows:
        G.add_edge(
            int(u),
            int(v),
            category=category,
            train_number=train_number,
            sample_snapshot=sample_snapshot,
            sample_stop_id=sample_stop_id,
            n_distinct_trains=int(n_distinct_trains) if n_distinct_trains is not None else None,
            first_seen_snapshot=first_seen_snapshot,
            last_seen_snapshot=last_seen_snapshot,
        )

    return len(rows)


def export_leaflet_map(
    G: nx.Graph,
    out_html: str = "stations_map.html",
    zoom_start: int = 11,
    node_radius: int = 5,
    node_weight: int = 1,
    edge_weight: int = 2,
    edge_opacity: float = 0.35,
    max_edges: int | None = 5000,
) -> None:
    # center map around mean lat/lon
    lats = [a["lat"] for _, a in G.nodes(data=True) if "lat" in a and "lon" in a]
    lons = [a["lon"] for _, a in G.nodes(data=True) if "lat" in a and "lon" in a]
    if not lats or not lons:
        raise ValueError("No valid coordinates found on nodes.")

    center = [sum(lats) / len(lats), sum(lons) / len(lons)]
    m = folium.Map(
        location=center,
        zoom_start=zoom_start,
        tiles="CartoDB Dark_Matter",
        control_scale=True,
    )

    arrow_lr = "\u2194"
    arrow_r  = "\u2192"

    # edges (draw first so nodes appear on top)
    edge_count = 0
    for u, v, edata in G.edges(data=True):
        au = G.nodes[u]
        av = G.nodes[v]
        if "lat" not in au or "lon" not in au or "lat" not in av or "lon" not in av:
            continue

        u_name = au.get("name", str(u))
        v_name = av.get("name", str(v))

        cat = edata.get("category")
        num = edata.get("train_number")
        sample_snapshot = edata.get("sample_snapshot")
        sample_stop_id = edata.get("sample_stop_id")
        n_trains = edata.get("n_distinct_trains")
        first_seen = edata.get("first_seen_snapshot")
        last_seen = edata.get("last_seen_snapshot")

        tooltip = f"{u_name} {arrow_lr} {v_name}"
        if cat and num:
            tooltip += f" | {cat} {num}"

        popup_lines = [
            f"<b>{u_name}</b> {arrow_lr} <b>{v_name}</b>",
            f"Sample train: {cat} {num}" if (cat and num) else "Sample train: (unknown)",
            f"Sample snapshot: {sample_snapshot}" if sample_snapshot else "Sample snapshot: (unknown)",
            f"Sample stop_id: {sample_stop_id}" if sample_stop_id else "Sample stop_id: (unknown)",
            f"Distinct trains on this edge: {n_trains}" if n_trains is not None else "Distinct trains: (unknown)",
            f"Seen (baseline snapshots): {first_seen} {arrow_r} {last_seen}" if (first_seen and last_seen) else "Seen range: (unknown)",
        ]
        popup_html = "<br/>".join(popup_lines)

        folium.PolyLine(
            locations=[(au["lat"], au["lon"]), (av["lat"], av["lon"])],
            weight=edge_weight,
            opacity=edge_opacity,
            tooltip=tooltip,
            popup=folium.Popup(popup_html, max_width=400),
        ).add_to(m)

        edge_count += 1
        if max_edges is not None and edge_count >= max_edges:
            break

    # nodes
    for eva, a in G.nodes(data=True):
        if "lat" not in a or "lon" not in a:
            continue
        name = a.get("name") or str(eva)
        folium.CircleMarker(
            location=(a["lat"], a["lon"]),
            radius=node_radius,
            weight=node_weight,
            fill=True,
            fill_opacity=0.9,
            tooltip=name,
            popup=f"{name} (EVA {eva})",
        ).add_to(m)

    m.save(out_html)
    print(f"Wrote {out_html} with {G.number_of_nodes()} nodes and {edge_count} edges drawn.")


def eva_by_station_name(conn, station_name: str) -> int:
    """
    Look up EVA by exact station_name (as given in stations.json).
    Raises ValueError if not found or ambiguous.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT station_eva
            FROM dw.dim_station
            WHERE station_name = %s
            """,
            (station_name,),
        )
        rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Station name not found in dw.dim_station: {station_name!r}")
    if len(rows) > 1:
        evas = [int(r[0]) for r in rows]
        raise ValueError(f"Ambiguous station_name {station_name!r} (multiple EVA): {evas}")
    return int(rows[0][0])


def shortest_path_by_name(G: nx.Graph, conn, src_name: str, dst_name: str) -> list[int]:
    """
    Unweighted shortest path (fewest hops) between two stations given by station_name.
    Returns: list of EVA IDs along the path.
    """
    src_eva = eva_by_station_name(conn, src_name)
    dst_eva = eva_by_station_name(conn, dst_name)

    if src_eva not in G:
        raise ValueError(f"Source EVA {src_eva} not in graph (did you load nodes/edges?)")
    if dst_eva not in G:
        raise ValueError(f"Target EVA {dst_eva} not in graph (did you load nodes/edges?)")

    return nx.shortest_path(G, source=src_eva, target=dst_eva)


def pretty_print_path(G: nx.Graph, path_evas: list[int]) -> None:
    """
    Prints EVA path as station names.
    """
    names = [G.nodes[eva].get("name", str(eva)) for eva in path_evas]
    print(" -> ".join(names))
    print(f"Hops: {max(0, len(path_evas) - 1)}")


if __name__ == "__main__":
    G = nx.Graph()
    with get_conn() as conn:
        n_nodes = load_station_nodes(G, conn)
        n_edges = load_planned_edges(G, conn)

        print(f"Loaded {n_nodes} station nodes.")
        print(f"Loaded {n_edges} unique planned edges (with metadata).")
        print("Graph:", G.number_of_nodes(), "nodes,", G.number_of_edges(), "edges")

        # shortest path query (edit these names)
        src = "Karl-Bonhoeffer-Nervenklinik"
        dst = "Tiergarten"
        path = shortest_path_by_name(G, conn, src, dst)
        print(f"\nShortest path (fewest hops) from {src!r} to {dst!r}:")
        pretty_print_path(G, path)

    export_leaflet_map(G, out_html="stations_map.html")
