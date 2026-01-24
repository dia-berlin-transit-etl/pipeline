import json
import psycopg2
import networkx as nx
import folium


def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="public_transport_db",
        user="efe",
    )


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
    Adds undirected edges between stations based on planned topology only:
    - baseline snapshots: dim_time.minute = 0
    - prev edge requires arrival_is_hidden = false
    - next edge requires departure_is_hidden = false
    - deduplicated in SQL
    """
    sql = """
    WITH base_snapshots AS (
      SELECT snapshot_key
      FROM dw.dim_time
      WHERE minute = 0
    ),
    edges AS (
      SELECT
        LEAST(fm.previous_station_eva, fm.station_eva) AS u,
        GREATEST(fm.previous_station_eva, fm.station_eva) AS v
      FROM dw.fact_movement fm
      JOIN base_snapshots bs ON bs.snapshot_key = fm.snapshot_key
      WHERE fm.previous_station_eva IS NOT NULL
        AND fm.previous_station_eva <> fm.station_eva
        AND fm.arrival_is_hidden = FALSE

      UNION

      SELECT
        LEAST(fm.station_eva, fm.next_station_eva) AS u,
        GREATEST(fm.station_eva, fm.next_station_eva) AS v
      FROM dw.fact_movement fm
      JOIN base_snapshots bs ON bs.snapshot_key = fm.snapshot_key
      WHERE fm.next_station_eva IS NOT NULL
        AND fm.next_station_eva <> fm.station_eva
        AND fm.departure_is_hidden = FALSE
    )
    SELECT DISTINCT u, v
    FROM edges
    WHERE u IS NOT NULL AND v IS NOT NULL;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    for u, v in rows:
        G.add_edge(int(u), int(v))

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

    # edges (drawn first so nodes appear on top)
    edge_count = 0
    for u, v in G.edges():
        au = G.nodes[u]
        av = G.nodes[v]

        if "lat" not in au or "lon" not in au or "lat" not in av or "lon" not in av:
            continue

        folium.PolyLine(
            locations=[(au["lat"], au["lon"]), (av["lat"], av["lon"])],
            weight=edge_weight,
            opacity=edge_opacity,
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
    if max_edges is not None and edge_count < G.number_of_edges():
        print(f"Note: limited edge rendering to max_edges={max_edges} (graph has {G.number_of_edges()} edges).")


if __name__ == "__main__":
    G = nx.Graph()
    with get_conn() as conn:
        n_nodes = load_station_nodes(G, conn)
        n_edges = load_planned_edges(G, conn)

    print(f"Loaded {n_nodes} station nodes.")
    print(f"Loaded {n_edges} unique planned edges.")
    print("Graph:", G.number_of_nodes(), "nodes,", G.number_of_edges(), "edges")

    export_leaflet_map(G, out_html="stations_map.html")
