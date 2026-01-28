# spark_etl/movements_etl.py
# Combines timetables + timetable_changes into ONE event log parquet,
# then collapses to "latest state per (stop_id, station_eva)" and writes movements parquet.
#
# Key addition vs your current file:
# - Load dim_station parquet (stations ETL output)
# - Fill station_eva for planned rows (and missing changes rows) via:
#     1) exact join on normalized station_name_search
#     2) fuzzy fallback (token-restricted candidate join + normalized Levenshtein score)
#
# Paths can be overridden via env vars:
#   TIMETABLES_ROOT, CHANGES_ROOT, STATIONS_PARQUET, LOGS_OUT, MOVEMENTS_OUT

import glob
import os
import re
from typing import Dict, Iterator, Optional, List

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
from pyspark.sql import functions as sf
from pyspark.sql.window import Window


# ---------- snapshot discovery ----------

_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")  # YYMMDDHHmm


def iter_timetable_snapshots(timetables_root: str) -> Iterator[str]:
    for p in glob.glob(os.path.join(timetables_root, "**", "[0-9]" * 10), recursive=True):
        if os.path.isdir(p):
            key = os.path.basename(p)
            pattern = os.path.join(p, "*.xml")
            has_xml_files = bool(glob.glob(pattern, recursive=True))
            if _SNAPSHOT_KEY_RE.match(key) and has_xml_files:
                yield key



# ---------- basic transforms ----------

def cast_timestamp(df, cols, fmt="yyMMddHHmm"):
    for col_name in cols:
        df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), fmt))
    return df


def parse_snapshot_from_filename(df, col_name: str):
    # matches: ".../<snapshot>/<station>_....xml"
    reg = r"\/(\d+)\/([-\w]+)_"
    return (
        df.withColumns(
            {
                "station": sf.regexp_extract(sf.col(col_name), reg, 2),
                "snapshot_ts": sf.regexp_extract(sf.col(col_name), reg, 1),
            }
        )
        .drop(col_name)
    )


def cast_boolean_for_hidden(df, cols):
    for col_name in cols:
        df = df.withColumn(col_name, sf.when(sf.col(col_name) == 1, sf.lit(True)).otherwise(sf.lit(False)))
    return df


def extract_data_from_xml(spark, file_path, schema):
    df = (
        spark.read.format("xml")
        .option("rootTag", "timetable")
        .option("rowTag", "s")
        .option("attributePrefix", "")
        .schema(schema)
        .load(file_path)
        .withColumn("fileName", sf.input_file_name())
    )
    # you had this; keep it (tune if needed)
    df = df.coalesce(48)
    return df


def transform_and_cast_df(df, cols_with_timestamp, cols_boolean=None):
    df = df.transform(parse_snapshot_from_filename, "fileName").transform(cast_timestamp, cols_with_timestamp)
    if cols_boolean is not None:
        df = df.transform(cast_boolean_for_hidden, cols_boolean)
    return df


def process_event_status(df, p_status_col_name, c_status_col_name, clt_name, pt_name, prefix):
    added_key = prefix + "_added"
    cancelled_key = prefix + "_cancelled"
    is_added = ((sf.col(c_status_col_name) == sf.lit("a")) | (sf.col(p_status_col_name) == sf.lit("a"))) & sf.col(pt_name).isNotNull()
    is_cancelled = (sf.col(c_status_col_name) == sf.lit("c")) & sf.col(clt_name).isNotNull()
    return df.withColumns({added_key: is_added, cancelled_key: is_cancelled}).drop(*[p_status_col_name, c_status_col_name, clt_name])


def flatten_df(df, dict_to_map: Dict[str, str]):
    return df.select(*[sf.col(old).alias(new) for old, new in dict_to_map.items()])



def get_last_values_for_stop(df, order_col, keys, cols):
    window_all = (
        Window.partitionBy(*keys)
        .orderBy(sf.col(order_col).asc_nulls_first())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    w_last_row = Window.partitionBy(*keys).orderBy(sf.col(order_col).desc_nulls_last())

    for col in cols:
        df = df.withColumn(col, sf.last(col, ignorenulls=True).over(window_all))

    result = df.withColumn("row", sf.row_number().over(w_last_row)).filter(sf.col("row") == 1).drop("row")
    return result


GENERIC = ["bahnhof", "hauptbahnhof", "station", "haltepunkt", "sbahn", "ubahn", "bahn"]


def to_station_search_name_col(c):
    s = sf.lower(sf.trim(c))

    # German folding
    s = sf.regexp_replace(s, "ß", "s")
    s = sf.regexp_replace(s, "ä", "a")
    s = sf.regexp_replace(s, "ö", "o")
    s = sf.regexp_replace(s, "ü", "u")

    # remove '_' between word chars (Spark-safe)
    s = sf.regexp_replace(s, r"([a-z0-9])_([a-z0-9])", r"$1$2")

    # hbf rules
    s = sf.regexp_replace(s, r"\bhbf\b\.?", " hauptbahnhof ")
    s = sf.regexp_replace(s, r"([a-z0-9])hbf\b\.?", r"$1hauptbahnhof")

    # bf rules
    s = sf.regexp_replace(s, r"\bbf\b\.?", " bahnhof ")
    s = sf.regexp_replace(s, r"([a-z0-9])bf\b\.?", r"$1bahnhof")

    # str rules
    s = sf.regexp_replace(s, r"\bstr\b\.?", " strase ")
    s = sf.regexp_replace(s, r"([a-z0-9])str\b\.?", r"$1strase")
    s = sf.regexp_replace(s, r"\b(\w+)\s+strase\b", r"$1strase")

    # remove "berlin"
    s = sf.regexp_replace(s, r"\bberlin\b", " ")

    # strip everything else
    s = sf.regexp_replace(s, r"[^a-z0-9\s]", " ")
    s = sf.regexp_replace(s, r"\s+", " ")
    s = sf.trim(s)
    return s


def add_station_eva_with_exact_then_fuzzy(df, dim_station_df, fuzzy_threshold=0.75):
    """
    df must have: station (string)
    df may have: station_eva (int/long) => preserved if already present
    dim_station_df must have: station_eva, station_name_search
    """

    dim = (
        dim_station_df.select(
            sf.col("station_eva").cast("long").alias("station_eva_dim"),
            sf.col("station_name_search").alias("station_name_search_dim"),
        )
        .dropDuplicates(["station_name_search_dim"])
    )

    df2 = df.withColumn("station_name_search", to_station_search_name_col(sf.col("station")))

    # 1) exact join on normalized search name
    joined = (
        df2.join(sf.broadcast(dim), df2.station_name_search == dim.station_name_search_dim, how="left")
        .drop("station_name_search_dim")
        .withColumn("station_eva_exact", sf.col("station_eva_dim").cast("long"))
        .drop("station_eva_dim")
    )

    # prefer existing station_eva if already present (e.g., changes feed might provide it)
    if "station_eva" in joined.columns:
        joined = joined.withColumn("station_eva_pref", sf.coalesce(sf.col("station_eva").cast("long"), sf.col("station_eva_exact")))
    else:
        joined = joined.withColumn("station_eva_pref", sf.col("station_eva_exact"))

    # 2) fuzzy only for unresolved keys (DISTINCT!)
    unresolved_keys = (
        joined.filter(sf.col("station_eva_pref").isNull())
        .select("station_name_search")
        .where(sf.col("station_name_search").isNotNull() & (sf.length("station_name_search") > 0))
        .dropDuplicates(["station_name_search"])
    )

    # If there are no unresolved keys, short-circuit
    # (Spark will optimize this away; keeping simple)
    unresolved_tok = (
        unresolved_keys.withColumn("tokens", sf.split(sf.col("station_name_search"), r"\s+"))
        .withColumn(
            "core_tokens",
            sf.expr(
                f"filter(tokens, t -> length(t) >= 2 AND NOT array_contains(array({','.join([repr(x) for x in GENERIC])}), t))"
            ),
        )
        .withColumn("match_token", sf.when(sf.size("core_tokens") > 0, sf.col("core_tokens")[0]).otherwise(sf.col("tokens")[0]))
        .select("station_name_search", "match_token")
    )

    # candidate restriction: dim rows containing match_token
    candidates = unresolved_tok.join(
        sf.broadcast(dim),
        sf.instr(dim.station_name_search_dim, unresolved_tok.match_token) > 0,
        how="inner",
    )

    # normalized Levenshtein similarity
    scored = (
        candidates.withColumn("dist", sf.levenshtein(sf.col("station_name_search"), sf.col("station_name_search_dim")))
        .withColumn("max_len", sf.greatest(sf.length("station_name_search"), sf.length("station_name_search_dim")))
        .withColumn("score", sf.when(sf.col("max_len") > 0, 1.0 - (sf.col("dist") / sf.col("max_len"))).otherwise(sf.lit(0.0)))
    )

    w = Window.partitionBy("station_name_search").orderBy(sf.col("score").desc(), sf.col("dist").asc())
    best = (
        scored.withColumn("rn", sf.row_number().over(w))
        .filter(sf.col("rn") == 1)
        .filter(sf.col("score") >= sf.lit(float(fuzzy_threshold)))
        .select(
            sf.col("station_name_search").alias("station_name_search_map"),
            sf.col("station_eva_dim").alias("station_eva_fuzzy"),
            sf.col("score").alias("station_eva_fuzzy_score"),
        )
    )

    out = (
        joined.join(sf.broadcast(best), joined.station_name_search == best.station_name_search_map, how="left")
        .drop("station_name_search_map")
        .withColumn("station_eva", sf.coalesce(sf.col("station_eva_pref"), sf.col("station_eva_fuzzy")).cast("long"))
        .drop("station_eva_pref", "station_eva_exact")  # keep station_eva_fuzzy_score if you want to inspect
    )

    return out


# ---------- main ETL ----------

def main():
    timetables_root = os.getenv("TIMETABLES_ROOT", "/opt/spark-data/timetables")
    changes_root = os.getenv("CHANGES_ROOT", "/opt/spark-data/timetable_changes")
    stations_parquet = os.getenv("STATIONS_PARQUET", "/opt/spark-data/dim_station")
    out_all_logs = os.getenv("LOGS_OUT", "/opt/spark-data/logs")
    movements_out = os.getenv("MOVEMENTS_OUT", "/opt/spark-data/movements")

    snapshot_keys_planned = sorted(set(iter_timetable_snapshots(timetables_root)))
    snapshot_keys_changes = sorted(set(iter_timetable_snapshots(changes_root)))
    all_snapshot_keys = sorted(set(snapshot_keys_planned + snapshot_keys_changes))

    print(f"Amount of keys planned: {len(snapshot_keys_planned)}")
    print(f"Amount of keys changes: {len(snapshot_keys_changes)}")
    print(f"Amount of keys together: {len(all_snapshot_keys)}")

    spark = SparkSession.builder.appName("Berlin Public Transport ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Load station dimension once
    dim_station = spark.read.parquet(stations_parquet).select(
        sf.col("station_eva").cast("long").alias("station_eva"),
        sf.col("station_name_search").alias("station_name_search"),
    )

    # Schemas
    timetableSchema = StructType(
        [
            StructField("id", StringType(), False),
            StructField(
                "tl",
                StructType(
                    [
                        StructField("n", IntegerType(), False),
                        StructField("c", StringType(), False),
                    ]
                ),
                True,
            ),
            StructField("ar", StructType([StructField("pt", StringType(), False)]), True),
            StructField("dp", StructType([StructField("pt", StringType(), False)]), True),
        ]
    )

    changesSchema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("eva", IntegerType(), True),
            StructField(
                "tl",
                StructType(
                    [
                        StructField("n", IntegerType(), False),
                        StructField("c", StringType(), False),
                    ]
                ),
                True,
            ),
            StructField(
                "ar",
                StructType(
                    [
                        StructField("pt", StringType(), True),
                        StructField("ps", StringType(), True),
                        StructField("ct", StringType(), True),
                        StructField("cs", StringType(), True),
                        StructField("hi", IntegerType(), True),
                        StructField("clt", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "dp",
                StructType(
                    [
                        StructField("pt", StringType(), True),
                        StructField("ps", StringType(), True),
                        StructField("ct", StringType(), True),
                        StructField("cs", StringType(), True),
                        StructField("hi", IntegerType(), True),
                        StructField("clt", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    flatten_mapping_planned_fields = {
        "id": "stop_id",
        "tl.n": "train_number",
        "tl.c": "train_category",
        "ar.pt": "arr_pt_raw",
        "dp.pt": "dep_pt_raw",
        "fileName": "fileName",
    }

    flatten_mapping_changes_fields = {
        "eva": "station_eva",  # may be null; we will fill from station name
        "ar.ps": "arr_ps_raw",
        "ar.ct": "arr_ct_raw",
        "ar.cs": "arr_cs_raw",
        "ar.clt": "arr_clt_raw",
        "ar.hi": "arr_hi_raw",
        "dp.ps": "dep_ps_raw",
        "dp.ct": "dep_ct_raw",
        "dp.cs": "dep_cs_raw",
        "dp.clt": "dep_clt_raw",
        "dp.hi": "dep_hi_raw",
    }

    def write_into_parquet(df):
        df = df.withColumn("snapshot_date", sf.to_date("snapshot_ts"))

        (
            df.repartition(4, "snapshot_date")
            .write.mode("append")
            .partitionBy("snapshot_date")
            .parquet(out_all_logs)
        )

    # ---- process snapshots ----
    for key in all_snapshot_keys:
        print(f"[ETL] snapshot={key}")

        if key in snapshot_keys_planned:
            path_to_timetable = f"{timetables_root}/{key}/*.xml"
            timetable_df = extract_data_from_xml(spark, path_to_timetable, timetableSchema)
            timetable_df = flatten_df(timetable_df, flatten_mapping_planned_fields)
            timetable_df = (
                transform_and_cast_df(timetable_df, ["arr_pt_raw", "dep_pt_raw", "snapshot_ts"])
                .withColumnsRenamed({"arr_pt_raw": "arr_pt", "dep_pt_raw": "dep_pt"})
            )

            # planned feed doesn't provide eva in XML -> start null, then fill via stations parquet
            timetable_df = timetable_df.withColumn("station_eva", sf.lit(None).cast(LongType()))

            timetable_df = timetable_df.withColumns(
                {
                    "arr_ct": sf.lit(None).cast(TimestampType()),
                    "arr_hi": sf.lit(False),
                    "arr_added": sf.lit(False),
                    "arr_cancelled": sf.lit(False),
                    "dep_ct": sf.lit(None).cast(TimestampType()),
                    "dep_hi": sf.lit(False),
                    "dep_added": sf.lit(False),
                    "dep_cancelled": sf.lit(False),
                }
            )

            # Fill station_eva
            timetable_df = add_station_eva_with_exact_then_fuzzy(timetable_df, dim_station, fuzzy_threshold=0.75)

            write_into_parquet(timetable_df)

        if key in snapshot_keys_changes:
            path_to_timetable_changes = f"{changes_root}/**/{key}/*.xml"
            changes_df = extract_data_from_xml(spark, path_to_timetable_changes, changesSchema)
            changes_df = flatten_df(changes_df, {**flatten_mapping_changes_fields, **flatten_mapping_planned_fields})

            changes_df = (
                transform_and_cast_df(
                    changes_df,
                    ["arr_ct_raw", "dep_ct_raw", "snapshot_ts", "arr_pt_raw", "dep_pt_raw", "arr_clt_raw", "dep_clt_raw"],
                    ["dep_hi_raw", "arr_hi_raw"],
                )
                .withColumnsRenamed(
                    {
                        "arr_pt_raw": "arr_pt",
                        "dep_pt_raw": "dep_pt",
                        "arr_ct_raw": "arr_ct",
                        "dep_ct_raw": "dep_ct",
                        "dep_hi_raw": "dep_hi",
                        "arr_hi_raw": "arr_hi",
                    }
                )
            )

            changes_df = (
                changes_df.transform(process_event_status, "dep_ps_raw", "dep_cs_raw", "dep_clt_raw", "dep_pt", "dep")
                .transform(process_event_status, "arr_ps_raw", "arr_cs_raw", "arr_clt_raw", "arr_pt", "arr")
            )

            # If station_eva from XML is missing/null, fill it via stations parquet
            changes_df = add_station_eva_with_exact_then_fuzzy(changes_df, dim_station, fuzzy_threshold=0.75)

            write_into_parquet(changes_df)

    # ---- collapse logs to latest state per stop ----
    events = spark.read.parquet(out_all_logs)

    # If some stations still couldn't be resolved, drop them for the final movement fact (or handle separately)
    events = events.filter(sf.col("station_eva").isNotNull())

    keys = ["stop_id", "station_eva"]
    order = "snapshot_ts"
    left_cols = [c for c in events.columns if c not in keys + ["snapshot_date"]]

    movement_df = get_last_values_for_stop(events, order, keys, left_cols)

    movement_df = movement_df.withColumns(
        {
            "arr_delay_min": sf.when(sf.col("arr_pt").isNull(), None)
            .when(sf.col("arr_ct").isNotNull(), sf.floor((sf.unix_timestamp("arr_ct") - sf.unix_timestamp("arr_pt")) / 60))
            .otherwise(0),
            "dep_delay_min": sf.when(sf.col("dep_pt").isNull(), None)
            .when(sf.col("dep_ct").isNotNull(), sf.floor((sf.unix_timestamp("dep_ct") - sf.unix_timestamp("dep_pt")) / 60))
            .otherwise(0),
        }
    )

    movement_df.printSchema()

    movement_df = movement_df.repartition(8, "snapshot_date")
    movement_df.write.mode("overwrite").partitionBy("snapshot_date").parquet(movements_out)

    spark.stop()


if __name__ == "__main__":
    main()
