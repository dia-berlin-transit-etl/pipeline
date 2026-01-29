from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType, DateType
import xml.etree.ElementTree as ET
import logging
import re
import time
import json
import os

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
# - TIMETABLE_SCHEMA: Raw planned timetable data from XML (ar@pt, dp@pt, etc.)
# - CHANGES_SCHEMA: Raw changes data from XML (ar@ct, dp@ct, ar@cs, dp@cs, etc.)
# - MOVEMENT_SCHEMA: Final resolved movement data (one row per stop_id, station_eva)
#
# Raw column names use the XML attribute names (pt, ct, ps, cs, hi)
# to stay close to the source format.  Derived columns after transform
# use the fact_movement naming convention (arrival_*, departure_*).

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
MOVEMENT_SCHEMA = StructType([
    StructField("station_eva", LongType()),
    StructField("stop_id", StringType()),
    StructField("station_name", StringType()),
    StructField("category", StringType()),
    StructField("train_number", StringType()),
    StructField("planned_arrival_ts", TimestampType()),
    StructField("planned_departure_ts", TimestampType()),
    StructField("changed_arrival_ts", TimestampType()),
    StructField("changed_departure_ts", TimestampType()),
    StructField("actual_arrival_ts", TimestampType()),
    StructField("actual_departure_ts", TimestampType()),
    StructField("arrival_cancelled", BooleanType()),
    StructField("departure_cancelled", BooleanType()),
    StructField("arrival_is_hidden", BooleanType()),
    StructField("departure_is_hidden", BooleanType()),
    StructField("snapshot_date", DateType()),  # partition column
])

# ==================== PARSING (Task 3.1 – Extract) ====================
# Each parser runs inside mapPartitions: one task per batch of XMLs, not
# one task per XML.  Snapshot key is derived from the folder path (YYMMDDHHmm).


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


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein (edit) distance between two strings.

    This respects character order and measures the minimum number of
    single-character edits (insertions, deletions, substitutions)
    needed to transform s1 into s2.

    Returns the edit distance (0 = identical).
    """
    if not s1:
        return len(s2) if s2 else 0
    if not s2:
        return len(s1)

    # Normalize both strings first
    s1 = to_station_search_name(s1)
    s2 = to_station_search_name(s2)

    if s1 == s2:
        return 0

    len1, len2 = len(s1), len(s2)

    # Use two rows instead of full matrix for memory efficiency
    prev_row = list(range(len2 + 1))
    curr_row = [0] * (len2 + 1)

    for i in range(1, len1 + 1):
        curr_row[0] = i
        for j in range(1, len2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            curr_row[j] = min(
                prev_row[j] + 1,      # deletion
                curr_row[j - 1] + 1,  # insertion
                prev_row[j - 1] + cost  # substitution
            )
        prev_row, curr_row = curr_row, prev_row

    return prev_row[len2]


def normalized_edit_distance(name1: str, name2: str) -> float:
    """Calculate normalized edit distance between two station names.

    Normalizes by the length of the longer string to get a value between 0 and 1.
    0.0 = identical, 1.0 = completely different.

    This respects word order unlike token set distance.
    """
    if not name1 or not name2:
        return 1.0  # Maximum distance for empty strings

    s1 = to_station_search_name(name1)
    s2 = to_station_search_name(name2)

    if not s1 or not s2:
        return 1.0

    if s1 == s2:
        return 0.0

    dist = levenshtein_distance(s1, s2)
    max_len = max(len(s1), len(s2))

    return dist / max_len if max_len > 0 else 0.0


def token_jaccard_similarity(name1: str, name2: str) -> float:
    """Calculate Jaccard similarity between token sets of two station names.

    Jaccard = |intersection| / |union|
    Returns value between 0.0 (no overlap) and 1.0 (identical).

    Used as a secondary metric after edit distance to break ties.
    """
    if not name1 or not name2:
        return 0.0

    tokens1 = set(to_station_search_name(name1).split())
    tokens2 = set(to_station_search_name(name2).split())

    if not tokens1 or not tokens2:
        return 0.0

    intersection = len(tokens1.intersection(tokens2))
    union = len(tokens1.union(tokens2))

    return intersection / union if union > 0 else 0.0


def create_station_lookup_df(spark, station_data_path):
    """Create a DataFrame with station name → EVA mappings from station_data.json.

    Creates a small lookup table (~133 rows for Berlin stations) with normalized
    station names for joining. This is more efficient in distributed Spark than
    using Python UDFs with broadcast variables.

    Reads the JSON via Spark to work in distributed environments where the driver
    may not have direct filesystem access to the data path.

    Args:
        spark: SparkSession instance
        station_data_path: Path to station_data.json file (can be file:// URL or local path)

    Returns:
        DataFrame with columns: search_name (normalized), ref_station_eva
    """
    # Ensure path has file:// prefix for Spark
    if not station_data_path.startswith("file://"):
        spark_path = f"file://{station_data_path}"
    else:
        spark_path = station_data_path

    # Read JSON via Spark (works in distributed mode)
    raw_df = spark.read.option("multiLine", True).json(spark_path)

    # Extract station name and main EVA from nested structure
    # Schema: root.result[].name, root.result[].evaNumbers[].number, root.result[].evaNumbers[].isMain
    stations_df = raw_df.select(sf.explode("result").alias("station"))

    # Extract name and evaNumbers array
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
    """ extract category, number, owner from <tl> element.

    Corresponds to tl@c, tl@n reads in both fact_planned.py (L332–L338)
    and fact_changed.py (L519–L521).
    """
    tl = s.find("tl")
    if tl is None:
        return None, None, None
    return (
        (tl.get("c") or "").strip(),
        (tl.get("n") or "").strip(),
        (tl.get("o") or "").strip(),
    )


def parse_timetable_partition(iterator):
    """Parse planned timetable XMLs.

    Corresponds to fact_planned.py: upsert_fact_movement_for_snapshot(),
    specifically the per-<s> loop (L327–L422):
      - s@id → stop_id                              (L328–L330)
      - tl@c, tl@n: skip Bus / empty                (L336–L342)
      - hi="1" on ar/dp → hidden flags               (L369–L373)
      - Skip stop only if BOTH hidden                (L372–L373)
      - ar@pt / dp@pt only when not hidden           (L375–L376)
    """
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
    """Parse timetable-change XMLs.

    Corresponds to fact_changed.py: upsert_fact_movement_from_changes_snapshot(),
    specifically the per-<s> loop (L502–L595):
      - s@id → stop_id                                 (L503–L505)
      - tl@c, tl@n: skip Bus                           (L519–L522)
      - ar@ct, dp@ct → changed times                   (L529–L530)
      - ar@cs, dp@cs → cancellation signals             (L525–L526)
      - ar@clt, dp@clt → cancellation times             (required for valid cancellation)
      - ar@ps, dp@ps → planned status (added detect)   (L513–L516)
      - hi="1" → hidden flags                           (L537–L538)
      - stop_id suffix >= 100 → added-stop heuristic    (L511, _stop_id_suffix_int)
    """
    for filepath, content in iterator:
        try:
            snapshot_key, station_eva, station_name, root = _parse_xml_root(filepath, content)
            for s in root.findall("s"):
                stop_id = (s.get("id") or "").strip()
                if not stop_id:
                    continue
                cat, num, owner = _parse_tl(s)
                if cat.lower() == "bus":
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
                    ar.get("clt") if ar is not None else None,  # cancellation time
                    ar is not None and ar.get("hi") == "1",
                    dp.get("pt") if dp is not None else None,
                    dp.get("ct") if dp is not None else None,
                    dp.get("ps") if dp is not None else None,
                    dp.get("cs") if dp is not None else None,
                    dp.get("clt") if dp is not None else None,  # cancellation time
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
    """Cast YYMMDDHHmm string columns to timestamps and add snapshot_ts.

    Corresponds to fact_planned.py / fact_changed.py: parse_yyMMddHHmm().
    """
    for col in columns:
        df = df.withColumn(col, sf.to_timestamp(col, "yyMMddHHmm"))
    return df.withColumn("snapshot_ts", sf.to_timestamp("snapshot_key", "yyMMddHHmm"))


def backfill_station_eva(timetable_df, station_lookup_df, fuzzy_threshold=0.3):
    """Backfill missing station_eva on timetable rows using DataFrame joins.

    Uses a two-stage lookup strategy:
    1. Exact match: normalized station name matching
    2. Fuzzy match: Edit distance (Levenshtein) + Jaccard similarity
       - Primary: Normalized edit distance (respects character/word order)
       - Secondary: Jaccard similarity on tokens (for tie-breaking)

    Args:
        timetable_df: DataFrame with timetable records (may have null station_eva)
        station_lookup_df: DataFrame with (search_name, ref_station_eva) from station_data.json
        fuzzy_threshold: Maximum normalized edit distance for fuzzy match (default: 0.3 = 30% different)

    Returns:
        DataFrame with station_eva backfilled where possible
    """
    from pyspark.sql.types import DoubleType

    _search_name_udf = sf.udf(to_station_search_name, StringType())
    _edit_distance_udf = sf.udf(normalized_edit_distance, DoubleType())
    _jaccard_udf = sf.udf(token_jaccard_similarity, DoubleType())

    # Add normalized search_name column to timetable
    result_df = timetable_df.withColumn("search_name", _search_name_udf("station_name"))

    # Rename lookup columns to avoid ambiguity after join
    lookup_renamed = station_lookup_df.withColumnRenamed(
        "search_name", "ref_search_name"
    )

    # --- Stage 1: Exact match on station_lookup_df ---
    result_df = (
        result_df.alias("t")
        .join(
            sf.broadcast(lookup_renamed.alias("ref")),
            sf.col("t.search_name") == sf.col("ref.ref_search_name"),
            "left",
        )
        .withColumn(
            "station_eva",
            sf.coalesce(sf.col("t.station_eva"), sf.col("ref.ref_station_eva")),
        )
        .drop("ref_station_eva", "ref_search_name")
    )

    # --- Stage 2: Fuzzy matching using edit distance ---
    if fuzzy_threshold > 0:
        # Drop search_name before fuzzy matching to avoid ambiguity in cross join
        still_missing = result_df.filter(sf.col("station_eva").isNull()).drop(
            "search_name"
        )
        already_matched = result_df.filter(sf.col("station_eva").isNotNull()).drop(
            "search_name"
        )

        missing_count = still_missing.count()
        if missing_count > 0:
            log.info(
                "Fuzzy matching %d unmatched records (edit distance threshold=%.2f)...",
                missing_count,
                fuzzy_threshold,
            )

            # Cross join missing records with lookup table and compute distance metrics
            # Use original station_lookup_df (with search_name) for fuzzy comparison
            cross_matched = (
                still_missing.filter(sf.col("station_name").isNotNull())
                .crossJoin(sf.broadcast(station_lookup_df))
                .withColumn(
                    "edit_distance",
                    _edit_distance_udf(sf.col("station_name"), sf.col("search_name")),
                )
                .withColumn(
                    "jaccard_sim",
                    _jaccard_udf(sf.col("station_name"), sf.col("search_name")),
                )
                .filter(
                    # Normalized edit distance threshold (e.g., max 30% different)
                    (sf.col("edit_distance") <= fuzzy_threshold)
                    &
                    # Require at least some token overlap to avoid matching unrelated names
                    (sf.col("jaccard_sim") >= 0.3)
                )
            )

            # Find best match per station_name:
            # - Primary: lowest edit distance (order-preserving)
            # - Secondary: highest Jaccard similarity (for tie-breaking)
            w = Window.partitionBy("station_name").orderBy(
                sf.col("edit_distance").asc(), sf.col("jaccard_sim").desc()
            )
            best_matches = (
                cross_matched.withColumn("_rn", sf.row_number().over(w))
                .filter(sf.col("_rn") == 1)
                .select(
                    "station_name",
                    sf.col("ref_station_eva").alias("fuzzy_eva"),
                    sf.col("search_name").alias("matched_name"),
                    "edit_distance",
                    "jaccard_sim",
                )
            )

            # Log some examples of fuzzy matches for debugging
            log.info("Sample fuzzy matches:")
            best_matches.select(
                "station_name", "matched_name", "fuzzy_eva", "edit_distance", "jaccard_sim"
            ).show(10, truncate=False)

            # Join best matches back to missing rows
            fuzzy_filled = (
                still_missing.alias("m")
                .join(best_matches.alias("f"), "station_name", "left")
                .withColumn(
                    "station_eva",
                    sf.coalesce(sf.col("m.station_eva"), sf.col("f.fuzzy_eva")),
                )
                .drop("fuzzy_eva", "matched_name", "edit_distance", "jaccard_sim")
            )

            # Combine matched and fuzzy-filled
            result_df = already_matched.unionByName(fuzzy_filled)
    else:
        # No fuzzy matching, just drop search_name
        result_df = result_df.drop("search_name")

    return result_df


def derive_change_flags(df):
    """Derive cancellation and added-stop flags on changes DataFrame.

    Matches spark_etl_by_steps.py process_event_status() logic:
      - cs='c' AND clt IS NOT NULL → cancelled=True
      - Otherwise → cancelled=False

    Note: This differs slightly from the Python reference (fact_changed.py) which uses
    tri-state logic (True/False/NULL for carry-forward). But spark_etl_by_steps.py
    is the verified correct implementation, and it uses binary True/False.
    """
    return (
        df
        # Cancellation requires BOTH cs='c' AND clt is NOT NULL
        # This matches spark_etl_by_steps.py:process_event_status() L95
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

    This mirrors the logic in spark_etl_by_steps.py:get_last_values_for_stop()
    which uses sf.last(col, ignorenulls=True) for ALL columns including cancellation.

    Key differences from previous implementation:
    1. Filter out NULL station_eva BEFORE resolution (matching spark_etl_by_steps.py line 441)
    2. Use last(ignorenulls=True) for cancellation instead of max() - cancellation
       should follow the same pattern as other columns
    3. Preserve planned times from changes feed (ar_pt, dp_pt) since changes XML
       also contains these values
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
        .withColumn("planned_arrival_ts", F.col("planned_arrival_ts").cast("timestamp"))
        .withColumn(
            "planned_departure_ts", F.col("planned_departure_ts").cast("timestamp")
        )
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

    change_events = (
        changes_df
        .withColumn("station_name", F.col("station_name"))
        .withColumn("category", F.col("category"))
        .withColumn("train_number", F.col("train_number"))
    )

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

    # Split change events into:
    # 1. Updates to existing stops (have corresponding timetable entry) - keep all
    # 2. New stops (no timetable entry) - only keep if marked as "added"
    change_keys = change_events.select("stop_id", "station_eva").distinct()

    # Changes that have a timetable entry - keep these
    changes_with_timetable = change_events.join(
        timetable_keys,
        ["stop_id", "station_eva"],
        "inner"
    )

    # Changes that DON'T have a timetable entry - only keep if "added"
    changes_only = change_events.join(
        timetable_keys,
        ["stop_id", "station_eva"],
        "left_anti"
    )

    # Filter changes-only to keep only "added" stops
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

    # Combine: timetable events + changes with timetable + added-only changes
    filtered_changes = changes_with_timetable.unionByName(changes_only_added)

    events = planned_events.select(common_cols).unionByName(
        filtered_changes.select(common_cols)
    )


    # 3. Carry forward last known non-null value per field

    w_history = (
        Window.partitionBy("stop_id", "station_eva")
        .orderBy(F.col("snapshot_key").asc_nulls_first())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
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
    # This matches spark_etl_by_steps.py:get_last_values_for_stop() which uses
    # sf.last(col, ignorenulls=True) for all columns.
    #
    # CRITICAL FIX: Previously cancellation used F.max() which made it "cumulative"
    # (once cancelled, always cancelled). But spark_etl_by_steps.py uses last()
    # which takes the last non-null value. Since planned events set cancelled=False
    # and only valid cancellations (cs='c' AND clt IS NOT NULL) set cancelled=True,
    # using last() gives the correct behavior.
    all_state_cols = normal_state_cols + ["arrival_cancelled", "departure_cancelled"]
    for c in all_state_cols:
        events = events.withColumn(c, F.last(c, ignorenulls=True).over(w_history))

    resolved = (
        events.withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "snapshot_key")
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
    # Note: spark_etl_by_steps.py doesn't have this filter, but it helps reduce noise
    resolved = resolved.filter(
        (F.col("actual_arrival_ts").isNotNull())
        | (F.col("actual_departure_ts").isNotNull())
    )

    # Final deduplication to ensure exactly one row per (stop_id, station_eva)
    # This matches the behavior of spark_etl_by_steps.py:get_last_values_for_stop()
    resolved = resolved.dropDuplicates(["stop_id", "station_eva"])

    return resolved


# ==================== TASK 3.2 – Average daily delay ====================

def compute_average_daily_delay(resolved_df, start_date=None, end_date=None, station_eva=None):
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
            sf.col("changed_arrival_ts").isNotNull()
            & sf.col("planned_arrival_ts").isNotNull()
            & ~sf.col("arrival_cancelled")
            & ~sf.col("arrival_is_hidden"),
            sf.round(
                (
                    sf.unix_timestamp("changed_arrival_ts")
                    - sf.unix_timestamp("planned_arrival_ts")
                )
                / 60.0
            ),
        ).cast("int"),
    ).withColumn(
        "departure_delay_min",
        sf.when(
            sf.col("changed_departure_ts").isNotNull()
            & sf.col("planned_departure_ts").isNotNull()
            & ~sf.col("departure_cancelled")
            & ~sf.col("departure_is_hidden"),
            sf.round(
                (
                    sf.unix_timestamp("changed_departure_ts")
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
        .agg(sf.round(sf.avg("departure_delay_min"), 2).alias("avg_departure_delay_min"))
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
    """
    peak = resolved_df.filter(
        sf.col("actual_departure_ts").isNotNull()
        & ~sf.col("departure_cancelled")
        & ~sf.col("departure_is_hidden")
        & (
            (
                (sf.hour("actual_departure_ts") >= 7)
                & (sf.hour("actual_departure_ts") < 9)
            )
            | (
                (sf.hour("actual_departure_ts") >= 17)
                & (sf.hour("actual_departure_ts") < 19)
            )
        )
    )
    daily = (
        peak.withColumn("dep_date", sf.to_date("actual_departure_ts"))
        .groupBy("station_eva", "dep_date")
        .agg(sf.count("*").alias("daily_departure_count"))
    )
    return daily.groupBy("station_eva").agg(
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
    resolved.cache()
    resolved_count = resolved.count()  # materialize once for both 3.2 and 3.3
    log.info("Resolved %d stop states (%.1fs)", resolved_count, time.time() - t1)

    # --- Task 3.2 ---
    # TODO: assert avg_arrival_delay_min/avg_departure_delay_min type as minutes
    log.info("Task 3.2 – Computing average daily delay per station...")
    t1 = time.time()
    avg_delay = compute_average_daily_delay(
        resolved, start_date="2025-10-04", end_date="2025-10-07", station_eva=8011162
    )
    avg_delay.show(10)
    log.info("Task 3.2 complete (%.1fs)", time.time() - t1)

    # --- Task 3.3 ---
    log.info("Task 3.3 – Computing average peak-hour departures per station...")
    t1 = time.time()
    peak_counts = compute_peak_hour_departure_counts(resolved)
    peak_counts.show(10)
    log.info("Task 3.3 complete (%.1fs)", time.time() - t1)

    # CORRECTION: writing final output with snapshot_date partition
    log.info("Writing final resolved movements to Parquet...")
    t_write = time.time()

    final_output = resolved.withColumn(
        "snapshot_date",
        sf.to_date(
            sf.coalesce(sf.col("actual_departure_ts"), sf.col("planned_departure_ts"))
        ),
    )

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
    log.info("All tasks complete (%.1fs wall time)", time.time() - t0)


def compare_timetables_final(spark):
    """Compare timetable DataFrame with final resolved movements."""
    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    tt_count = tt.count()
    final_count = final.count()
    log.info("Timetable records: %d", tt_count)
    log.info("Final resolved records: %d", final_count)

    # Find timetable records not in final
    tt_keys = tt.select("station_eva", "stop_id", "snapshot_key").distinct()
    final_keys = final.select("station_eva", "stop_id").distinct()

    missing_in_final = tt_keys.join(final_keys, ["station_eva", "stop_id"], "left_anti")
    missing_count = missing_in_final.count()
    log.info("Timetable records missing in final: %d", missing_count)
    if missing_count > 0:
        missing_in_final.show(20, truncate=False)


if __name__ == "__main__":
    import sys

    spark = (
        SparkSession.builder.appName("Berlin Public Transport ETL")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Check for command line args to run tests
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        log.info("Running diagnostic tests... Use spark_tests.py instead for more options.")
        from spark_tests import run_all_diagnostic_tests
        run_all_diagnostic_tests(spark)
    else:
        main(spark)

    spark.stop()

