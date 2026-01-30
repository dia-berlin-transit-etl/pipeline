from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType, DateType
import xml.etree.ElementTree as ET
import logging
import re
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("spark_etl")


def test_distributed_write(spark, path):
    log.info("Running preflight distributed write test to %s", path)
    test_df = spark.range(0, 100).repartition(4)  # force multiple executors
    test_path = f"{path}/_spark_write_test"

    try:
        test_df.write.mode("overwrite").parquet(test_path)
        spark.read.parquet(test_path).count()  # verify readable
        log.info("Preflight write test succeeded")
    except Exception as e:
        log.error("Preflight write test FAILED: %s", e)
        raise


# ==================== SCHEMAS (Task 3.1) ====================
# Three schemas are defined:
# - TIMETABLE_SCHEMA: Raw planned timetable data from XML
# - CHANGES_SCHEMA: Raw changes data from XML
# - MOVEMENT_SCHEMA: Final resolved movement data (one row per stop_id, station_eva)


TIMETABLE_SCHEMA = StructType([
    StructField("snapshot_key", StringType()),
    StructField("station_eva", LongType()),
    StructField("station_name", StringType()),
    StructField("stop_id", StringType()),
    StructField("category", StringType()),       # tl@c  (fact_planned: cat)
    StructField("train_number", StringType()),    # tl@n  (fact_planned: num)
    StructField("owner", StringType()),           # tl@o
    StructField("ar_pt", StringType()),           # ar@pt (raw YYMMDDHHmm)
    StructField("dp_pt", StringType()),           # dp@pt (raw YYMMDDHHmm)
    StructField("arrival_is_hidden", BooleanType()),   # ar hi="1"
    StructField("departure_is_hidden", BooleanType()), # dp hi="1"
])

CHANGES_SCHEMA = StructType(
    [
        StructField("snapshot_key", StringType()),
        StructField("station_eva", LongType()),
        StructField("station_name", StringType()),
        StructField("stop_id", StringType()),
        StructField("category", StringType()),
        StructField("train_number", StringType()),
        StructField("owner", StringType()),
        StructField("ar_pt", StringType()),  # ar@pt
        StructField("ar_ct", StringType()),  # ar@ct (changed time)
        StructField("ar_ps", StringType()),  # ar@ps (planned status)
        StructField("ar_cs", StringType()),  # ar@cs (cancellation status)
        StructField(
            "ar_clt", StringType()
        ),  # ar@clt (cancellation time) - REQUIRED for valid cancellation
        StructField("arrival_is_hidden", BooleanType()),
        StructField("dp_pt", StringType()),  # dp@pt
        StructField("dp_ct", StringType()),  # dp@ct
        StructField("dp_ps", StringType()),  # dp@ps
        StructField("dp_cs", StringType()),  # dp@cs
        StructField(
            "dp_clt", StringType()
        ),  # dp@clt (cancellation time) - REQUIRED for valid cancellation
        StructField("departure_is_hidden", BooleanType()),
        StructField("is_added_by_suffix", BooleanType()),
    ]
)

# Schema for the final resolved movement data (one row per stop_id, station_eva)
# This is the output of resolve_latest_stop_state() written to final_movements parquet
MOVEMENT_SCHEMA = StructType(
    [
        StructField("snapshot_key", StringType()),
        StructField("station_eva", LongType()),
        StructField("stop_id", StringType()),
        StructField("station_name", StringType()),
        StructField("category", StringType()),
        StructField("train_number", StringType()),
        StructField("planned_arrival_ts", TimestampType()),
        StructField("planned_departure_ts", TimestampType()),
        StructField("actual_arrival_ts", TimestampType()),
        StructField("actual_departure_ts", TimestampType()),
        StructField("arrival_cancelled", BooleanType()),
        StructField("departure_cancelled", BooleanType()),
        StructField("arrival_is_hidden", BooleanType()),
        StructField("departure_is_hidden", BooleanType()),
    ]
)

# ==================== PARSING (Task 3.1 – Extract) ====================
# Each parser runs inside mapPartitions: one task per batch of XMLs
# Snapshot key is derived from the folder path (YYMMDDHHmm).


def to_station_search_name(name: str) -> str:

    s = (name or "").strip().lower()

    s = s.replace("ß", "s").replace("ä", "a").replace("ö", "o").replace("ü", "u")

    s = re.sub(r"(?<=\w)_(?=\w)", "", s)

    s = re.sub(r"\bhbf\b\.?", " hauptbahnhof ", s)
    s = re.sub(r"(?<=\w)hbf\b\.?", "hauptbahnhof", s)

    s = re.sub(r"\bbf\b\.?", " bahnhof ", s)
    s = re.sub(r"(?<=\w)(?<!h)bf\b\.?", "bahnhof", s)

    s = re.sub(r"\bstr\b\.?", " strase ", s)
    s = re.sub(r"(?<=\w)str\b\.?", "strase", s)
    s = re.sub(r"\b(\w+)\s+strase\b", r"\1strase", s)

    s = re.sub(r"\bberlin\b", " ", s)

    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def create_station_lookup_df(spark, station_data_path):
    """Create a DataFrame with station name → EVA mappings from station_data.json.

    Creates a small lookup table (~133 rows for Berlin stations) with normalized
    station names for joining. 

    Reads the JSON via Spark to work in distributed environments where the driver
    may not have direct filesystem access to the data path.

    Args:
        spark: SparkSession instance
        station_data_path: Path to station_data.json file (can be file:// URL or local path)

    Returns:
        DataFrame with columns: search_name (normalized), ref_station_eva
    """

    if not station_data_path.startswith("file://"):
        spark_path = f"file://{station_data_path}"
    else:
        spark_path = station_data_path

    raw_df = spark.read.option("multiLine", True).json(spark_path)

    # Schema: root.result[].name, root.result[].evaNumbers[].number, root.result[].evaNumbers[].isMain
    stations_df = raw_df.select(sf.explode("result").alias("station"))

    stations_df = stations_df.select(
        sf.col("station.name").alias("name"),
        sf.col("station.evaNumbers").alias("evaNumbers"),
    )

    # Get the main EVA (where isMain=true) or first EVA as fallback
    stations_df = stations_df.withColumn(
        "main_eva",
        sf.coalesce(
            sf.expr("filter(evaNumbers, x -> x.isMain = true)[0].number"),
            sf.col("evaNumbers")[0]["number"],
        ),
    )

    # Apply normalization UDF and select final columns
    _search_name_udf = sf.udf(to_station_search_name, StringType())

    lookup_df = (
        stations_df.filter(sf.col("name").isNotNull() & sf.col("main_eva").isNotNull())
        .select(
            _search_name_udf("name").alias("search_name"),
            sf.col("main_eva").cast(LongType()).alias("ref_station_eva"),
        )
        .dropDuplicates(["search_name"])
    )

    count = lookup_df.count()
    log.info(
        "Created station lookup DataFrame with %d entries from %s",
        count,
        station_data_path,
    )
    return lookup_df


def _parse_xml_root(filepath, content):
    """ extract snapshot_key, station_eva, station_name, and parsed root. """
    match = re.search(r"/(\d{10})/", filepath)
    snapshot_key = match.group(1) if match else None
    root = ET.fromstring(content)
    eva_str = root.get("eva")
    station_eva = int(eva_str) if eva_str and eva_str.isdigit() else None
    return snapshot_key, station_eva, root.get("station"), root


def _parse_tl(s):
    """ extract category, number, owner from <tl> element """
    tl = s.find("tl")
    if tl is None:
        return None, None, None
    return (
        (tl.get("c") or "").strip(),
        (tl.get("n") or "").strip(),
        (tl.get("o") or "").strip(),
    )


def parse_timetable_partition(iterator):
    """Parse planned timetable XMLs."""
    for filepath, content in iterator:
        try:
            snapshot_key, station_eva, station_name, root = _parse_xml_root(filepath, content)
            for s in root.findall("s"):
                stop_id = s.get("id")
                if not stop_id:
                    continue
                cat, num, owner = _parse_tl(s)
                if not cat or not num or cat.lower() == "bus":
                    continue

                ar, dp = s.find("ar"), s.find("dp")
                ar_hidden = ar is not None and ar.get("hi") == "1"
                dp_hidden = dp is not None and dp.get("hi") == "1"
                if ar_hidden and dp_hidden:
                    continue
                planned_ar_ts = ar.get("pt") if ar is not None and not ar_hidden else None
                planned_dp_ts = dp.get("pt") if dp is not None and not dp_hidden else None

                yield (snapshot_key, station_eva, station_name, stop_id,
                       cat, num, owner, planned_ar_ts, planned_dp_ts,
                       ar_hidden, dp_hidden)
        except Exception:
            continue


def parse_changes_partition(iterator):
    """Parse timetable-change XMLs. """
    for filepath, content in iterator:
        try:
            snapshot_key, station_eva, station_name, root = _parse_xml_root(filepath, content)
            for s in root.findall("s"):
                stop_id = (s.get("id") or "").strip()
                if not stop_id:
                    continue
                cat, num, owner = _parse_tl(s)
                if cat and cat.lower() == "bus":
                    continue

                ar, dp = s.find("ar"), s.find("dp")

                try:
                    suffix = int(stop_id.rsplit("-", 1)[-1])
                except (ValueError, IndexError):
                    suffix = -1

                yield (
                    snapshot_key, station_eva, station_name, stop_id,
                    cat, num, owner,
                    ar.get("pt") if ar is not None else None,
                    ar.get("ct") if ar is not None else None,
                    ar.get("ps") if ar is not None else None,
                    ar.get("cs") if ar is not None else None,
                    ar.get("clt") if ar is not None else None, 
                    ar is not None and ar.get("hi") == "1",
                    dp.get("pt") if dp is not None else None,
                    dp.get("ct") if dp is not None else None,
                    dp.get("ps") if dp is not None else None,
                    dp.get("cs") if dp is not None else None,
                    dp.get("clt") if dp is not None else None, 
                    dp is not None and dp.get("hi") == "1",
                    suffix >= 100,
                )
        except Exception:
            continue


# ==================== ETL HELPERS (Task 3.1) ====================

def extract(spark, path, parser, schema, min_partitions):
    """Read XMLs via wholeTextFiles, parse with mapPartitions, return DataFrame."""
    rdd = spark.sparkContext.wholeTextFiles(path, minPartitions=min_partitions)
    return spark.createDataFrame(rdd.mapPartitions(parser), schema)


def cast_timestamps(df, columns):
    """Cast YYMMDDHHmm string columns to timestamps and add snapshot_ts. """
    for col in columns:
        df = df.withColumn(col, sf.to_timestamp(col, "yyMMddHHmm"))
    return df.withColumn("snapshot_ts", sf.to_timestamp("snapshot_key", "yyMMddHHmm"))


def backfill_station_eva(timetable_df, station_lookup_df, similarity_threshold=0.5):
    """Backfill missing station_eva on timetable rows using name matching.

    Uses normalized station name with a two-stage matching strategy:
    1. Exact match on normalized name
    2. Fuzzy match using Levenshtein similarity (score >= threshold) & Jaccard with set > 0.3

    Returns:
        DataFrame with station_eva backfilled where possible
    """
    if station_lookup_df is None:
        log.warning("No station lookup table provided, skipping backfill")
        return timetable_df

    _search_name_udf = sf.udf(to_station_search_name, StringType())

    timetable_with_norm = timetable_df.withColumn(
        "_search_name", _search_name_udf("station_name")
    )

    # --- Stage 1: Exact match ---
    lookup_renamed = station_lookup_df.select(
        sf.col("search_name").alias("_ref_search_name"),
        sf.col("ref_station_eva").alias("_ref_eva"),
    )

    result_df = (
        timetable_with_norm.alias("t")
        .join(
            sf.broadcast(lookup_renamed.alias("ref")),
            sf.col("t._search_name") == sf.col("ref._ref_search_name"),
            "left",
        )
        .withColumn(
            "station_eva",
            sf.coalesce(sf.col("t.station_eva"), sf.col("ref._ref_eva")),
        )
        .drop("_ref_search_name", "_ref_eva")
    )

    # --- Stage 2: Fuzzy match for still-unmatched records ---
    still_missing = result_df.filter(
        sf.col("station_eva").isNull() & sf.col("_search_name").isNotNull()
    )
    already_matched = result_df.filter(sf.col("station_eva").isNotNull())

    missing_names = still_missing.select("_search_name").distinct()
    missing_count = missing_names.count()

    if missing_count > 0:
        log.info(
            "Stage 2: Fuzzy matching for %d distinct unmatched names (threshold=%.2f)",
            missing_count,
            similarity_threshold,
        )

        fuzzy_candidates = (
            missing_names.crossJoin(sf.broadcast(station_lookup_df))
            # Levenshtein similarity
            .withColumn(
                "_lev_dist",
                sf.levenshtein(sf.col("_search_name"), sf.col("search_name")),
            )
            .withColumn(
                "_max_len",
                sf.greatest(sf.length("_search_name"), sf.length("search_name")),
            )
            .withColumn(
                "_lev_score",
                sf.when(sf.col("_max_len") == 0, sf.lit(0.0)).otherwise(
                    1.0 - (sf.col("_lev_dist") / sf.col("_max_len"))
                ),
            )
            # Jaccard token similarity
            .withColumn("_tokens1", sf.split("_search_name", " "))
            .withColumn("_tokens2", sf.split("search_name", " "))
            .withColumn(
                "_intersect", sf.size(sf.array_intersect("_tokens1", "_tokens2"))
            )
            .withColumn("_union", sf.size(sf.array_union("_tokens1", "_tokens2")))
            .withColumn(
                "_jaccard",
                sf.when(sf.col("_union") == 0, sf.lit(0.0)).otherwise(
                    sf.col("_intersect") / sf.col("_union")
                ),
            )
            # RequireS both levenshtein score and jaccard similarity thresholds
            .filter(
                (sf.col("_lev_score") >= similarity_threshold)
                & (sf.col("_jaccard") > 0.3)
            )
            .withColumnRenamed("_lev_score", "_score")
            .drop(
                "_lev_dist", "_max_len", "_tokens1", "_tokens2", "_intersect", "_union"
            )
        )

        # Keep best match per search name
        w = Window.partitionBy("_search_name").orderBy(sf.col("_score").desc())
        best_fuzzy = (
            fuzzy_candidates.withColumn("_rn", sf.row_number().over(w))
            .filter(sf.col("_rn") == 1)
            .select(
                "_search_name",
                sf.col("ref_station_eva").alias("_fuzzy_eva"),
                sf.col("search_name").alias("_matched_name"),
                "_score",
            )
        )

        # Log fuzzy matches
        fuzzy_match_count = best_fuzzy.count()
        log.info("Fuzzy matched %d/%d names", fuzzy_match_count, missing_count)
        if fuzzy_match_count > 0:
            log.info("Fuzzy matches:")
            best_fuzzy.orderBy(sf.col("_score").desc()).show(20, truncate=False)

        # Join fuzzy matches back to missing records
        fuzzy_filled = (
            still_missing.join(best_fuzzy, "_search_name", "left")
            .withColumn(
                "station_eva",
                sf.coalesce(sf.col("station_eva"), sf.col("_fuzzy_eva")),
            )
            .drop("_fuzzy_eva", "_matched_name", "_score")
        )

        result_df = already_matched.unionByName(fuzzy_filled)

    # Log final statistics
    total_count = result_df.count()
    matched_count = result_df.filter(sf.col("station_eva").isNotNull()).count()
    unmatched_count = total_count - matched_count
    log.info(
        "Station EVA backfill: %d/%d records have station_eva (%.1f%%)",
        matched_count,
        total_count,
        100.0 * matched_count / total_count if total_count > 0 else 0,
    )

    # FOR DEBUGGING: Log remaining unmatched names with their best candidates below threshold
    """
    if unmatched_count > 0:
        unmatched_names = (
            result_df.filter(sf.col("station_eva").isNull())
            .select("station_name", "_search_name")
            .distinct()
        )
        unmatched_distinct = unmatched_names.count()
        log.info(
            "Still unmatched: %d records, %d distinct names",
            unmatched_count,
            unmatched_distinct,
        )

        candidates = (
            unmatched_names.crossJoin(sf.broadcast(station_lookup_df))
            .withColumn(
                "_score",
                1.0
                - (
                    sf.levenshtein(sf.col("_search_name"), sf.col("search_name"))
                    / sf.greatest(sf.length("_search_name"), sf.length("search_name"))
                ),
            )
        )
        w = Window.partitionBy("station_name").orderBy(sf.col("_score").desc())
        top_candidates = (
            candidates.withColumn("_rank", sf.row_number().over(w))
            .filter(sf.col("_rank") <= 3)
            .select(
                "station_name",
                "_search_name",
                sf.col("search_name").alias("candidate"),
                sf.col("ref_station_eva").alias("candidate_eva"),
                sf.round("_score", 3).alias("score"),
            )
            .orderBy("station_name", sf.col("score").desc())
        )
        log.info("Unmatched names with best candidates (below threshold):")
        top_candidates.show(30, truncate=False)
    """

    # Drop the temporary column
    result_df = result_df.drop("_search_name")

    return result_df


def derive_change_flags(df):
    """Derive cancellation and added-stop flags on changes DataFrame. """
    return (
        df
        # Cancellation requires BOTH cs='c' AND clt is NOT NULL
        .withColumn(
            "arrival_cancelled", (sf.col("ar_cs") == "c") & sf.col("ar_clt").isNotNull()
        )
        .withColumn(
            "departure_cancelled",
            (sf.col("dp_cs") == "c") & sf.col("dp_clt").isNotNull(),
        )
        .withColumn(
            "arrival_is_added",
            (sf.col("ar_ps") == "a")
            | (sf.col("ar_cs") == "a")
            | sf.col("is_added_by_suffix"),
        )
        .withColumn(
            "departure_is_added",
            (sf.col("dp_ps") == "a")
            | (sf.col("dp_cs") == "a")
            | sf.col("is_added_by_suffix"),
        )
    )


# ==================== RESOLVE LATEST STATE (for 3.2 & 3.3) ====================
# TODO: save this to parquet instead of timetable/changes separately
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def resolve_latest_stop_state(timetable_df, changes_df):
    """
    Resolve the latest state for each (stop_id, station_eva) pair.
    """

    # 1. Filter out NULL keys first
    timetable_df = timetable_df.filter(
        F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull()
    )
    changes_df = changes_df.filter(
        F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull()
    )

    # 2. Normalize column names
    timetable_df = (
        timetable_df.withColumnRenamed("ar_pt", "planned_arrival_ts")
        .withColumnRenamed("dp_pt", "planned_departure_ts")
    )

    changes_df = (
        changes_df.withColumnRenamed("ar_ct", "changed_arrival_ts")
        .withColumnRenamed("dp_ct", "changed_departure_ts")
        .withColumnRenamed("ar_pt", "planned_arrival_ts")
        .withColumnRenamed("dp_pt", "planned_departure_ts")
        .withColumn("changed_arrival_ts", F.col("changed_arrival_ts").cast("timestamp"))
        .withColumn(
            "changed_departure_ts", F.col("changed_departure_ts").cast("timestamp")
        )
        .withColumn("planned_arrival_ts", F.col("planned_arrival_ts").cast("timestamp"))
        .withColumn(
            "planned_departure_ts", F.col("planned_departure_ts").cast("timestamp")
        )
    )

    # 3. Add missing columns so schemas match
    planned_events = (
        timetable_df.withColumn("changed_arrival_ts", F.lit(None).cast("timestamp"))
        .withColumn("changed_departure_ts", F.lit(None).cast("timestamp"))
        # cancellation is False for planned events, not NULL
        .withColumn("arrival_cancelled", F.lit(False).cast("boolean"))
        .withColumn("departure_cancelled", F.lit(False).cast("boolean"))
        .withColumn("arrival_is_hidden", F.col("arrival_is_hidden").cast("boolean"))
        .withColumn("departure_is_hidden", F.col("departure_is_hidden").cast("boolean"))
    )

    change_events = changes_df # artifact

    # Columns that define stop state
    common_cols = [
        "station_eva",
        "stop_id",
        "snapshot_key",
        "station_name",
        "category",
        "train_number",
        "planned_arrival_ts",
        "planned_departure_ts",
        "changed_arrival_ts",
        "changed_departure_ts",
        "arrival_cancelled",
        "departure_cancelled",
        "arrival_is_hidden",
        "departure_is_hidden",
    ]

    # CRITICAL: Filter changes-only stops that aren't marked as "added"
    # A changes-only stop is one that exists in changes but NOT in timetables.
    # These should only be kept if marked as added (arrival_is_added OR departure_is_added).
    #
    # Get the set of (stop_id, station_eva) from timetables
    timetable_keys = planned_events.select("stop_id", "station_eva").distinct()


    # Changes that have a timetable entry - keep these
    changes_with_timetable = change_events.join(
        timetable_keys,
        ["stop_id", "station_eva"],
        "inner"
    )

    # Changes that DON'T have a timetable entry - only keep if added
    changes_only = change_events.join(
        timetable_keys,
        ["stop_id", "station_eva"],
        "left_anti"
    )

    # Filter changes-only to keep only added stops
    if "arrival_is_added" in changes_df.columns and "departure_is_added" in changes_df.columns:
        changes_only_added = changes_only.filter(
            F.col("arrival_is_added") | F.col("departure_is_added")
        )
        log.info("Changes-only stops: %d total, %d marked as added (keeping only added)",
                 changes_only.count(), changes_only_added.count())
    else:
        # Fallback if is_added columns don't exist
        changes_only_added = changes_only
        log.warning("is_added columns not found - keeping all changes-only stops")

    # combining timetable events + changes with timetable + added-only changes
    filtered_changes = changes_with_timetable.unionByName(changes_only_added)

    events = planned_events.select(common_cols).unionByName(
        filtered_changes.select(common_cols)
    )

    # 3. Carry forward last known non-null value per field

    w_history = (
        Window.partitionBy("stop_id", "station_eva")
        .orderBy(F.col("snapshot_key").asc_nulls_first())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow) # should be more efficient but if it breaks restore it back to Window.unboundedFollowing 
    )

    w_latest = Window.partitionBy("stop_id", "station_eva").orderBy(
        F.col("snapshot_key").desc_nulls_last()
    )

    normal_state_cols = [
        "station_name",
        "category",
        "train_number",
        "planned_arrival_ts",
        "planned_departure_ts",
        "changed_arrival_ts",
        "changed_departure_ts",
        "arrival_is_hidden",
        "departure_is_hidden",
    ]

    # Apply last(ignorenulls=True) to ALL state columns including cancellation
    all_state_cols = normal_state_cols + ["arrival_cancelled", "departure_cancelled"]
    for c in all_state_cols:
        events = events.withColumn(c, F.last(c, ignorenulls=True).over(w_history))

    resolved = (
        events.withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 4. Final derived fields
    resolved = (
        resolved.withColumn(
            "actual_arrival_ts", F.coalesce("changed_arrival_ts", "planned_arrival_ts")
        )
        .withColumn(
            "actual_departure_ts",
            F.coalesce("changed_departure_ts", "planned_departure_ts"),
        )
        .withColumn("arrival_cancelled", F.coalesce("arrival_cancelled", F.lit(False)))
        .withColumn(
            "departure_cancelled", F.coalesce("departure_cancelled", F.lit(False))
        )
        .withColumn("arrival_is_hidden", F.coalesce("arrival_is_hidden", F.lit(False)))
        .withColumn(
            "departure_is_hidden", F.coalesce("departure_is_hidden", F.lit(False))
        )
    )

    # Filter to keep only rows with meaningful data
    resolved = resolved.filter(
        (F.col("actual_arrival_ts").isNotNull())
        | (F.col("actual_departure_ts").isNotNull())
    )

    resolved = resolved.drop("changed_arrival_ts", "changed_departure_ts") # Final version doesn't include these

    return resolved


# ==================== TASK 3.2 – Average daily delay ====================


def compute_average_daily_delay(
    resolved_df, start_date=None, end_date=None, station_eva=None
):
    """Average delay per station, excluding cancelled/hidden stops.

    Operates on resolved final state (one row per stop) so each train is
    counted once.  delay = changed_time − planned_time in minutes.

    IMPORTANT: Delay is only computed when there's a changed time (ct).
    If there's no change, the stop ran on time (or we don't know), so
    delay is NULL, not 0.

    Corresponds to fact_changed.py: _delay_minutes() (L441–L445) applied
    inside upsert_fact_movement_from_changes_snapshot() (L755–L762).
    """
    if start_date:
        resolved_df = resolved_df.filter(
            sf.col("actual_arrival_ts")
            >= sf.to_timestamp(sf.lit(start_date), "yyyy-MM-dd")
        )
    if end_date:
        resolved_df = resolved_df.filter(
            sf.col("actual_arrival_ts")
            < sf.to_timestamp(sf.lit(end_date), "yyyy-MM-dd")
        )
    if station_eva:
        resolved_df = resolved_df.filter(sf.col("station_eva") == station_eva)

    # CRITICAL FIX: Delay = changed_time - planned_time, NOT actual - planned
    # Only compute delay when there's actually a changed time (ct).
    # If changed_time is NULL, delay should be NULL (not 0).
    # This matches Python fact_changed.py:_delay_minutes() which requires
    # both planned and changed times to be non-NULL.
    df = resolved_df.withColumn(
        "arrival_delay_min",
        sf.when(
            sf.col("actual_arrival_ts").isNotNull()
            & sf.col("planned_arrival_ts").isNotNull()
            & ~sf.col("arrival_cancelled")
            & ~sf.col("arrival_is_hidden"),
            sf.round(
                (
                    sf.unix_timestamp("actual_arrival_ts")
                    - sf.unix_timestamp("planned_arrival_ts")
                )
                / 60.0
            ),
        ).cast("int"),
    ).withColumn(
        "departure_delay_min",
        sf.when(
            sf.col("actual_departure_ts").isNotNull()
            & sf.col("planned_departure_ts").isNotNull()
            & ~sf.col("departure_cancelled")
            & ~sf.col("departure_is_hidden"),
            sf.round(
                (
                    sf.unix_timestamp("actual_departure_ts")
                    - sf.unix_timestamp("planned_departure_ts")
                )
                / 60.0
            ),
        ).cast("int"),
    )

    avg_arr = (
        df.filter(sf.col("arrival_delay_min").isNotNull())
        .groupBy("station_eva")
        .agg(sf.round(sf.avg("arrival_delay_min"), 2).alias("avg_arrival_delay_min"))
    )
    avg_dep = (
        df.filter(sf.col("departure_delay_min").isNotNull())
        .groupBy("station_eva")
        .agg(
            sf.round(sf.avg("departure_delay_min"), 2).alias("avg_departure_delay_min")
        )
    )

    joined = avg_arr.join(avg_dep, "station_eva", "outer")
    return joined.fillna(
        {
            "avg_arrival_delay_min": 0.0,
            "avg_departure_delay_min": 0.0,
        }
    )


# ==================== TASK 3.3 – Peak-hour departures ====================


def compute_peak_hour_departure_counts(resolved_df):
    """Average number of departures per station during peak hours
    (07:00–09:00, 17:00–19:00), excluding cancelled/hidden departures.

    Uses resolved final state: actual departure = changed if available, else planned.
    No direct counterpart in fact_planned.py / fact_changed.py (query-only logic).

    Days with no peak hour departures count as 0 (not omitted from average).
    """
    # Get all valid departures (not cancelled, not hidden)
    valid_departures = resolved_df.filter(
        sf.col("actual_departure_ts").isNotNull()
        & ~sf.col("departure_cancelled")
        & ~sf.col("departure_is_hidden")
    ).withColumn("dep_date", sf.to_date("actual_departure_ts"))

    # Get all (station_eva, date) combinations with any departure
    all_station_dates = valid_departures.select("station_eva", "dep_date").distinct()

    # Filter to peak hours only
    peak = valid_departures.filter(
        (
            (sf.hour("actual_departure_ts") >= 7)
            & (sf.hour("actual_departure_ts") < 9)
        )
        | (
            (sf.hour("actual_departure_ts") >= 17)
            & (sf.hour("actual_departure_ts") < 19)
        )
    )

    # Count peak departures per station per day
    peak_daily = peak.groupBy("station_eva", "dep_date").agg(
        sf.count("*").alias("daily_departure_count")
    )

    # Left join to include days with 0 peak departures
    daily_with_zeros = all_station_dates.join(
        peak_daily, ["station_eva", "dep_date"], "left"
    ).fillna(0, subset=["daily_departure_count"])

    return daily_with_zeros.groupBy("station_eva").agg(
        sf.avg("daily_departure_count").alias("avg_peak_hour_departures_per_day")
    )


# ==================== MAIN ====================


def main(spark, station_data_path="/opt/spark-data/DBahn-berlin/station_data.json"):

    t0 = time.time()

    # --- Create station lookup table from station_data.json ---
    station_lookup_df = None
    if station_data_path:
        try:
            log.info("Creating station lookup table from %s...", station_data_path)
            station_lookup_df = create_station_lookup_df(spark, station_data_path)
            station_lookup_df.cache()  # Small table, cache for reuse
        except Exception as e:
            log.warning("Failed to load station_data.json: %s", e)

    # --- Task 3.1: Extract & Transform ---
    log.info("Extracting timetables...")
    timetable_df = extract(
        spark,
        "file:///opt/spark-data/timetables/*/*/*.xml",
        parse_timetable_partition,
        TIMETABLE_SCHEMA,
        min_partitions=50,
    )
    log.info("Extracting changes...")
    changes_df = extract(
        spark,
        "file:///opt/spark-data/timetable_changes/*/*/*.xml",
        parse_changes_partition,
        CHANGES_SCHEMA,
        min_partitions=50,
    )

    log.info("Backfilling station EVA on timetables...")
    timetable_df = backfill_station_eva(timetable_df, station_lookup_df)

    log.info("Transforming...")
    timetable_df = cast_timestamps(timetable_df, ["ar_pt", "dp_pt"])
    changes_df = derive_change_flags(
        cast_timestamps(
            changes_df, ["ar_pt", "ar_ct", "ar_clt", "dp_pt", "dp_ct", "dp_clt"]
        )
    )

    # --- Task 3.1: Load ---

    log.info("Writing timetables parquet...")
    t1 = time.time()
    timetable_df.write.partitionBy("snapshot_key").mode("overwrite").parquet(
        "file:///opt/spark-data/movements/timetables"
    )
    log.info("Timetables parquet written (%.1fs)", time.time() - t1)

    log.info("Writing changes parquet...")
    t1 = time.time()
    changes_df.write.partitionBy("snapshot_key").mode("overwrite").parquet(
        "file:///opt/spark-data/movements/changes"
    )
    log.info("Changes parquet written (%.1fs)", time.time() - t1)
    log.info("Task 3.1 ETL complete (%.1fs total)", time.time() - t0)

    # --- Verify ---
    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    ch = spark.read.parquet("file:///opt/spark-data/movements/changes")
    tt_count = tt.count()
    ch_count = ch.count()
    log.info("Timetable records: %d", tt_count)
    log.info("Changes records:   %d", ch_count)

    # --- Resolve final state (shared by 3.2 and 3.3) ---
    log.info("Resolving latest stop states...")
    t1 = time.time()
    resolved = resolve_latest_stop_state(tt, ch)
    log.info("Resolved %d stop states (%.1fs)", resolved.count(), time.time() - t1)


    log.info("Writing final resolved movements to Parquet...")
    t_write = time.time()

    final_output = resolved.withColumn(
        "snapshot_date",
        sf.to_date(
            sf.coalesce(sf.col("actual_departure_ts"), sf.col("planned_departure_ts"))
        ),
    )
    # correct snapshot_date and the write partition depends on the resolved departure time
    final_output.cache()
    (
        final_output.repartition("snapshot_date")
        .write.mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet("file:///opt/spark-data/movements/final_movements")
    )
    log.info("Final movements written (%.1fs)", time.time() - t_write)
    final_count = final_output.count()
    log.info("Final movements row count: %d", final_count)
    resolved.unpersist()

    # --- Task 3.2 ---
    log.info("Task 3.2 – Computing average daily delay per station...")
    t1 = time.time()
    avg_delay = compute_average_daily_delay(
        final_output,
        start_date="2025-10-04",
        end_date="2025-10-07",
        station_eva=8011162,
    )
    avg_delay.show(10)
    log.info("Task 3.2 complete (%.1fs)", time.time() - t1)

    # --- Task 3.3 ---
    log.info("Task 3.3 – Computing average peak-hour departures per station...")
    t1 = time.time()
    peak_counts = compute_peak_hour_departure_counts(final_output)
    peak_counts.show(10)
    log.info("Task 3.3 complete (%.1fs)", time.time() - t1)
    final_output.unpersist()

    log.info("All tasks complete (%.1fs wall time)", time.time() - t0)


if __name__ == "__main__":
    import sys

    spark = (
        SparkSession.builder.appName("Berlin Public Transport ETL")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    main(spark)

    spark.stop()
