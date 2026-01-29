from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import xml.etree.ElementTree as ET
import logging
import re
import time
import json
import os

GENERIC = {
    "bahnhof",
    "station",
    "haltepunkt",
    "sbahn",
    "ubahn",
    "bahn",
    "berlin",
}

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

CHANGES_SCHEMA = StructType([
    StructField("snapshot_key", StringType()),
    StructField("station_eva", LongType()),
    StructField("station_name", StringType()),
    StructField("stop_id", StringType()),
    StructField("category", StringType()),
    StructField("train_number", StringType()),
    StructField("owner", StringType()),
    StructField("ar_pt", StringType()),           # ar@pt
    StructField("ar_ct", StringType()),           # ar@ct (changed time)
    StructField("ar_ps", StringType()),           # ar@ps (planned status)
    StructField("ar_cs", StringType()),           # ar@cs (cancellation status)
    StructField("arrival_is_hidden", BooleanType()),
    StructField("dp_pt", StringType()),           # dp@pt
    StructField("dp_ct", StringType()),           # dp@ct
    StructField("dp_ps", StringType()),           # dp@ps
    StructField("dp_cs", StringType()),           # dp@cs
    StructField("departure_is_hidden", BooleanType()),
    StructField("is_added_by_suffix", BooleanType()),
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
    # remove any punctuation
    s = re.sub(r"[^\w\s]", "", s)
    return s


def parse_tokens(s):
    """Split a string into common tokens in GENERIC and rest (lowercase)."""
    tokens = set(s.lower().split())
    generic_tokens = tokens.intersection(GENERIC)
    specific_tokens = tokens.difference(GENERIC)
    return generic_tokens, specific_tokens


def extract_specific_tokens(name: str) -> str:
    """Extract non-generic tokens from a normalized station name.

    Returns a space-separated string of specific tokens (excludes GENERIC words).
    This is used for fuzzy matching - we only compare the meaningful parts.
    """
    if not name:
        return ""
    # Normalize first
    normalized = to_station_search_name(name)
    # Split into tokens and filter out generic ones
    tokens = normalized.split()
    specific = [t for t in tokens if t not in GENERIC]
    return " ".join(sorted(specific))  # Sort for consistency


def token_set_distance(name1: str, name2: str) -> int:
    """Calculate token-level distance between two station names.

    Uses a token-based approach where:
    - Each name is split into tokens
    - Distance = number of tokens that differ between the sets
    - This maps "berlin hauptbahnhof" to 2 tokens, not 18 characters

    Returns the symmetric difference size (tokens in one but not both).
    """
    if not name1 or not name2:
        return 999  # Large distance for empty strings

    # Normalize and tokenize
    tokens1 = set(to_station_search_name(name1).split())
    tokens2 = set(to_station_search_name(name2).split())

    # Symmetric difference = tokens in one set but not both
    diff = tokens1.symmetric_difference(tokens2)
    return len(diff)


def token_jaccard_similarity(name1: str, name2: str) -> float:
    """Calculate Jaccard similarity between token sets of two station names.

    Jaccard = |intersection| / |union|
    Returns value between 0.0 (no overlap) and 1.0 (identical).
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
    # Use expr for compatibility with older Spark versions
    stations_df = stations_df.withColumn(
        "main_eva",
        sf.coalesce(
            # First try: find EVA where isMain=true using SQL expression
            sf.expr("filter(evaNumbers, x -> x.isMain = true)[0].number"),
            # Fallback: use first EVA in array
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
                    ar is not None and ar.get("hi") == "1",
                    dp.get("pt") if dp is not None else None,
                    dp.get("ct") if dp is not None else None,
                    dp.get("ps") if dp is not None else None,
                    dp.get("cs") if dp is not None else None,
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


def backfill_station_eva(timetable_df, station_lookup_df, fuzzy_threshold=2):
    """Backfill missing station_eva on timetable rows using DataFrame joins.

    Uses a two-stage lookup strategy:
    1. Exact match: normalized station name matching
    2. Fuzzy match: Token-level distance + Jaccard similarity
       - Uses token set distance (symmetric difference) instead of char-level Levenshtein
       - "berlin hauptbahnhof" maps to 2 tokens, not 18 characters
       - Uses Jaccard similarity as a secondary ranking metric

    Args:
        timetable_df: DataFrame with timetable records (may have null station_eva)
        station_lookup_df: DataFrame with (search_name, ref_station_eva) from station_data.json
        fuzzy_threshold: Maximum token set distance for fuzzy match (default: 2 tokens)

    Returns:
        DataFrame with station_eva backfilled where possible
    """
    from pyspark.sql.types import IntegerType, DoubleType

    _search_name_udf = sf.udf(to_station_search_name, StringType())
    _token_distance_udf = sf.udf(token_set_distance, IntegerType())
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

    # --- Stage 2: Fuzzy matching using token-level distance ---
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
                "Fuzzy matching %d unmatched records (token threshold=%d)...",
                missing_count,
                fuzzy_threshold,
            )

            # Cross join missing records with lookup table and compute token-level metrics
            # Use original station_lookup_df (with search_name) for fuzzy comparison
            cross_matched = (
                still_missing.filter(sf.col("station_name").isNotNull())
                .crossJoin(sf.broadcast(station_lookup_df))
                .withColumn(
                    "token_distance",
                    _token_distance_udf(sf.col("station_name"), sf.col("search_name")),
                )
                .withColumn(
                    "jaccard_sim",
                    _jaccard_udf(sf.col("station_name"), sf.col("search_name")),
                )
                .filter(
                    # Token distance threshold (e.g., max 2 different tokens)
                    (sf.col("token_distance") <= fuzzy_threshold)
                    &
                    # Require at least some token overlap
                    (sf.col("jaccard_sim") >= 0.3)
                )
            )

            # Find best match per station_name:
            # - Primary: lowest token distance
            # - Secondary: highest Jaccard similarity
            w = Window.partitionBy("station_name").orderBy(
                sf.col("token_distance").asc(), sf.col("jaccard_sim").desc()
            )
            best_matches = (
                cross_matched.withColumn("_rn", sf.row_number().over(w))
                .filter(sf.col("_rn") == 1)
                .select(
                    "station_name",
                    sf.col("ref_station_eva").alias("fuzzy_eva"),
                    "token_distance",
                    "jaccard_sim",
                )
            )

            # Log some examples of fuzzy matches for debugging
            log.info("Sample fuzzy matches:")
            best_matches.select(
                "station_name", "fuzzy_eva", "token_distance", "jaccard_sim"
            ).show(10, truncate=False)

            # Join best matches back to missing rows
            fuzzy_filled = (
                still_missing.alias("m")
                .join(best_matches.alias("f"), "station_name", "left")
                .withColumn(
                    "station_eva",
                    sf.coalesce(sf.col("m.station_eva"), sf.col("f.fuzzy_eva")),
                )
                .drop("fuzzy_eva", "token_distance", "jaccard_sim")
            )

            # Combine matched and fuzzy-filled
            result_df = already_matched.unionByName(fuzzy_filled)
    else:
        # No fuzzy matching, just drop search_name
        result_df = result_df.drop("search_name")

    return result_df


def derive_change_flags(df):
    """Derive cancellation and added-stop flags on changes DataFrame.

    Corresponds to fact_changed.py:
      - _cancel_update_from_cs()  (L341–L358): cs='c' → cancelled
      - _is_added_by_ps_or_cs()   (L360–L370): ps='a' or cs='a' → added
      - _stop_id_suffix_int()     (L373–L381): suffix >= 100 → added
    """
    return (
        df
        .withColumn("arrival_cancelled", sf.col("ar_cs") == "c")
        .withColumn("departure_cancelled", sf.col("dp_cs") == "c")
        .withColumn("arrival_is_added",
                     (sf.col("ar_ps") == "a")
                     | (sf.col("ar_cs") == "a")
                     | sf.col("is_added_by_suffix"))
        .withColumn("departure_is_added",
                     (sf.col("dp_ps") == "a")
                     | (sf.col("dp_cs") == "a")
                     | sf.col("is_added_by_suffix"))
    )


# ==================== RESOLVE LATEST STATE (for 3.2 & 3.3) ====================
# TODO: save this to parquet instead of timetable/changes separately
def resolve_latest_stop_state(timetable_df, changes_df):
    """Resolve the final observed state for each (station_eva, stop_id).

    Corresponds to fact_changed.py chaining logic (L602–L636 + L644–L794):
      - Base row = latest snapshot_key per (station_eva, stop_id)  (L602–L636)
      - Overlay changed times onto planned baseline                (L728–L794)
      - Added stops with no planned base → insert from change XML  (L665–L726)
    Spark replaces the SQL DISTINCT ON with a Window row_number(),
    and the per-row upsert with a DataFrame outer join + coalesce.
    """
    # Latest change row per (station_eva, stop_id)
    w = Window.partitionBy("station_eva", "stop_id").orderBy(sf.desc("snapshot_key"))
    latest_changes = (
        changes_df
        .withColumn("_rn", sf.row_number().over(w))
        .filter(sf.col("_rn") == 1)
        .drop("_rn")
    )

    # CORRECTION: dropDuplicates was non-deterministic so I'm switching to window ordering like efe's
    w_planned = Window.partitionBy("station_eva", "stop_id").orderBy(
        sf.desc("snapshot_key")
    )

    planned = (
        timetable_df.select(
            "station_eva",
            "stop_id",
            "station_name",
            "category",
            "train_number",
            sf.col("snapshot_key"),  # for window ordering
            sf.col("dp_pt").alias("planned_departure_ts"),
            sf.col("ar_pt").alias("planned_arrival_ts"),
        )
        .withColumn("_rn", sf.row_number().over(w_planned))
        .filter(sf.col("_rn") == 1)
        .drop("_rn", "snapshot_key")
    )

    changed = latest_changes.select(
        "station_eva", "stop_id",
        sf.col("station_name").alias("ch_station_name"),
        sf.col("category").alias("ch_category"),
        sf.col("train_number").alias("ch_train_number"),
        sf.col("dp_pt").alias("ch_planned_departure_ts"),
        sf.col("dp_ct").alias("changed_departure_ts"),
        sf.col("ar_pt").alias("ch_planned_arrival_ts"),
        sf.col("ar_ct").alias("changed_arrival_ts"),
        sf.col("departure_cancelled"),
        sf.col("arrival_cancelled"),
        sf.col("departure_is_hidden").alias("ch_departure_is_hidden"),
        sf.col("arrival_is_hidden").alias("ch_arrival_is_hidden"),
    )

    merged = planned.join(changed, ["station_eva", "stop_id"], "outer")

    # Actual time = changed if available, else latest change planned (most
    # authoritative), fall back to timetable planned as last resort.
    return (
        merged
        .withColumn("actual_departure_ts",
                     sf.coalesce("changed_departure_ts", "ch_planned_departure_ts",
                                 "planned_departure_ts"))
        .withColumn("actual_arrival_ts",
                     sf.coalesce("changed_arrival_ts", "ch_planned_arrival_ts",
                                 "planned_arrival_ts"))
        .withColumn("station_name", sf.coalesce("ch_station_name", "station_name"))
        .withColumn("category", sf.coalesce("ch_category", "category"))
        .withColumn("train_number", sf.coalesce("ch_train_number", "train_number"))
        # cf. fact_changed.py L751: carry forward cancellation; default False
        .withColumn("departure_cancelled",
                     sf.coalesce("departure_cancelled", sf.lit(False)))
        .withColumn("arrival_cancelled",
                     sf.coalesce("arrival_cancelled", sf.lit(False)))
        .withColumn("departure_is_hidden",
                     sf.coalesce("ch_departure_is_hidden", sf.lit(False)))
        .withColumn("arrival_is_hidden",
                     sf.coalesce("ch_arrival_is_hidden", sf.lit(False)))
    )


# ==================== TASK 3.2 – Average daily delay ====================

def compute_average_daily_delay(resolved_df, start_date=None, end_date=None, station_eva=None):
    """Average delay per station, excluding cancelled/hidden stops.

    Operates on resolved final state (one row per stop) so each train is
    counted once.  delay = actual_time − planned_time in minutes.

    Corresponds to fact_changed.py: _delay_minutes() (L441–L445) applied
    inside upsert_fact_movement_from_changes_snapshot() (L755–L762),
    but computed here on the resolved final state rather than per-snapshot.
    """
    if start_date:
        resolved_df = resolved_df.filter(
            sf.col("actual_arrival_ts")
            >= sf.to_timestamp(sf.lit(start_date), "yyyy-MM-dd")
        )
    if end_date:
        resolved_df = resolved_df.filter(
            sf.col("actual_arrival_ts") < sf.to_timestamp(sf.lit(end_date), "yyyy-MM-dd")
        )
    if station_eva:
        resolved_df = resolved_df.filter(sf.col("station_eva") == station_eva)
    df = resolved_df.withColumn(
        "arrival_delay_min",
        sf.when(
            sf.col("actual_arrival_ts").isNotNull()
            & sf.col("planned_arrival_ts").isNotNull()
            & ~sf.col("arrival_cancelled") & ~sf.col("arrival_is_hidden"),
            (sf.unix_timestamp("actual_arrival_ts")
             - sf.unix_timestamp("planned_arrival_ts")) / 60,
        ).cast("int"),   # float??
    ).withColumn(
        "departure_delay_min",
        sf.when(
            sf.col("actual_departure_ts").isNotNull()
            & sf.col("planned_departure_ts").isNotNull()
            & ~sf.col("departure_cancelled") & ~sf.col("departure_is_hidden"),
            (sf.unix_timestamp("actual_departure_ts")
             - sf.unix_timestamp("planned_departure_ts")) / 60,
        ).cast("int"),   # float??
    )

    avg_arr = (df.filter(sf.col("arrival_delay_min").isNotNull())
               .groupBy("station_eva")
               .agg(sf.avg("arrival_delay_min").alias("avg_arrival_delay_min")))
    avg_dep = (df.filter(sf.col("departure_delay_min").isNotNull())
               .groupBy("station_eva")
               .agg(sf.avg("departure_delay_min").alias("avg_departure_delay_min")))

    joined = avg_arr.join(avg_dep, "station_eva", "outer")
    return joined.fillna({
        "avg_arrival_delay_min": 0.0,
        "avg_departure_delay_min": 0.0,
    })

# ==================== TASK 3.3 – Peak-hour departures ====================

def compute_peak_hour_departure_counts(resolved_df):
    """Average number of departures per station during peak hours
    (07:00–09:00, 17:00–19:00), excluding cancelled/hidden departures.

    Uses resolved final state: actual departure = changed if available, else planned.
    No direct counterpart in fact_planned.py / fact_changed.py (query-only logic).
    """
    peak = resolved_df.filter(
        sf.col("actual_departure_ts").isNotNull()
        & ~sf.col("departure_cancelled") & ~sf.col("departure_is_hidden")
        & (
            ((sf.hour("actual_departure_ts") >= 7)
             & (sf.hour("actual_departure_ts") < 9))
            | ((sf.hour("actual_departure_ts") >= 17)
               & (sf.hour("actual_departure_ts") < 19))
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
        cast_timestamps(changes_df, ["ar_pt", "ar_ct", "dp_pt", "dp_ct"])
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

    resolved.unpersist()
    log.info("All tasks complete (%.1fs wall time)", time.time() - t0)
    spark.stop()


def compare_backfill_station_eva(
    spark,
    station_data_path="/opt/spark-data/DBahn-berlin/station_data.json",
    fuzzy_threshold=2,
):
    """Test backfill_station_eva with exact + token-level fuzzy matching.

    Args:
        fuzzy_threshold: Maximum token set distance (default: 2 tokens difference)
    """
    test_distributed_write(spark, "file:///opt/spark-data/movements")

    # Load station lookup
    station_lookup_df = create_station_lookup_df(spark, station_data_path)
    station_lookup_df.cache()

    # Load timetable data
    timetable_df = extract(
        spark,
        "file:///opt/spark-data/timetables/*/*/*.xml",
        parse_timetable_partition,
        TIMETABLE_SCHEMA,
        min_partitions=50,
    )

    initial_missing = timetable_df.filter(sf.col("station_eva").isNull()).count()
    total_records = timetable_df.count()
    log.info("Total timetable records: %d", total_records)
    log.info("Initial records missing station_eva: %d", initial_missing)

    # 1. Exact matching only
    log.info("--- Stage 1: Exact matching ---")
    result_exact = backfill_station_eva(
        timetable_df, station_lookup_df, fuzzy_threshold=0
    )
    n_missing_exact = result_exact.filter(sf.col("station_eva").isNull()).count()
    n_filled_exact = initial_missing - n_missing_exact
    log.info(
        "After exact match: %d filled, %d still missing",
        n_filled_exact,
        n_missing_exact,
    )

    # 2. Fuzzy matching on remaining 
    log.info("--- Stage 2: Exact + Fuzzy matching (threshold=%d) ---", fuzzy_threshold)
    result_fuzzy = backfill_station_eva(
        timetable_df, station_lookup_df, fuzzy_threshold=fuzzy_threshold
    )
    n_missing_fuzzy = result_fuzzy.filter(sf.col("station_eva").isNull()).count()
    n_filled_fuzzy = initial_missing - n_missing_fuzzy
    n_fuzzy_recovered = n_missing_exact - n_missing_fuzzy
    log.info(
        "After fuzzy match: %d filled total, %d still missing",
        n_filled_fuzzy,
        n_missing_fuzzy,
    )
    log.info("Fuzzy matching recovered: %d additional records", n_fuzzy_recovered)

    # --- Summary ---
    log.info("=" * 60)
    log.info("SUMMARY:")
    log.info("  Total records:      %d", total_records)
    log.info("  Initial missing:    %d", initial_missing)
    log.info(
        "  After exact match:  %d filled (%d missing)", n_filled_exact, n_missing_exact
    )
    log.info(
        "  After fuzzy match:  %d filled (%d missing)", n_filled_fuzzy, n_missing_fuzzy
    )
    log.info("  Fuzzy recovered:    %d additional", n_fuzzy_recovered)
    log.info("=" * 60)

    # Show stations still missing
    if n_missing_fuzzy > 0:
        log.info("Stations still missing EVA (showing normalized names):")
        _search_name_udf = sf.udf(to_station_search_name, StringType())
        missing_stations = (
            result_fuzzy.filter(sf.col("station_eva").isNull())
            .select("station_name")
            .distinct()
            .withColumn("normalized", _search_name_udf("station_name"))
        )
        missing_stations.show(30, truncate=False)

        # Also show what's in the lookup table for comparison
        log.info("Station lookup table entries (for comparison):")
        station_lookup_df.select("search_name", "ref_station_eva").show(
            20, truncate=False
        )

    station_lookup_df.unpersist()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Berlin Public Transport ETL")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # compare_backfill_station_eva(spark)
    main(spark)
    spark.stop()

    # main(spark)
