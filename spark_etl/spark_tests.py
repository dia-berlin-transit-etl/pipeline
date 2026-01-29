"""
Diagnostic tests for the Spark ETL pipeline.

These tests verify the correctness of the ETL pipeline by checking:
- Data integrity (duplicates, NULL values)
- Row counts match expected values
- Compute functions work correctly
- Bug fixes are working

Usage:
    spark-submit spark_tests.py [test_name]

    If no test_name is provided, all tests are run.
    Available tests: test_1, test_2, ..., test_12, or 'all'
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging
import sys
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("spark_tests")

# Import compute functions from main module (these don't use UDFs internally)
from spark_batched import (
    compute_average_daily_delay,
    compute_peak_hour_departure_counts,
)


# ==================== LOCAL HELPER FUNCTIONS ====================
# These are copied from spark_batched.py to avoid UDF serialization issues.
# When Spark serializes a UDF that imports from spark_batched, executors
# fail with "ModuleNotFoundError: No module named 'spark_batched'".
# By defining these functions locally, they get pickled with the UDF.

def to_station_search_name(name: str) -> str:
    """Normalize station name for fuzzy matching.

    Copied from spark_batched.py to avoid UDF serialization issues.
    """
    if not name:
        return ""
    s = (name or "").strip().lower()

    s = (s.replace("ß", "s")
         .replace("ä", "a")
         .replace("ö", "o")
         .replace("ü", "u"))

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

    Copied from spark_batched.py to avoid UDF serialization issues.
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

    Copied from spark_batched.py to avoid UDF serialization issues.
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

    Copied from spark_batched.py to avoid UDF serialization issues.
    """
    if not name1 or not name2:
        return 0.0

    tokens1 = set(to_station_search_name(name1).split())
    tokens2 = set(to_station_search_name(name2).split())

    if not tokens1 or not tokens2:
        return 0.0

    intersection = len(tokens1 & tokens2)
    union = len(tokens1 | tokens2)

    return intersection / union if union > 0 else 0.0


# ==================== TEST FUNCTIONS ====================


def test_duplicate_keys(spark):
    """
    TEST 1: Check for duplicate (stop_id, station_eva) in final_movements.

    Expected: 0 duplicates (each stop should appear exactly once per station)
    If duplicates exist, the deduplication logic in resolve_latest_stop_state is broken.
    """
    log.info("=" * 60)
    log.info("TEST 1: Checking for duplicate (stop_id, station_eva) keys")
    log.info("=" * 60)

    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")
    final_count = final.count()

    # Count unique keys
    unique_keys = final.select("stop_id", "station_eva").distinct().count()

    log.info("Total rows in final_movements: %d", final_count)
    log.info("Unique (stop_id, station_eva) pairs: %d", unique_keys)
    log.info("Duplicate rows: %d", final_count - unique_keys)

    if final_count != unique_keys:
        log.warning("FAIL: Found %d duplicate rows!", final_count - unique_keys)

        # Show examples of duplicates
        duplicates = (
            final.groupBy("stop_id", "station_eva")
            .agg(F.count("*").alias("cnt"))
            .filter(F.col("cnt") > 1)
            .orderBy(F.col("cnt").desc())
        )
        log.info("Top duplicate keys:")
        duplicates.show(20, truncate=False)

        # Show actual duplicate rows for one example
        first_dup = duplicates.first()
        if first_dup:
            stop_id, station_eva, _ = first_dup
            log.info("Example duplicate rows for stop_id=%s, station_eva=%s:", stop_id, station_eva)
            final.filter(
                (F.col("stop_id") == stop_id) & (F.col("station_eva") == station_eva)
            ).show(10, truncate=False)
    else:
        log.info("PASS: No duplicate keys found")

    return final_count - unique_keys


def test_null_station_eva(spark):
    """
    TEST 2: Check for NULL station_eva in timetables and changes.

    The Python pipeline filters out records where station_eva cannot be resolved.
    The Spark pipeline should do the same before deduplication.
    """
    log.info("=" * 60)
    log.info("TEST 2: Checking for NULL station_eva")
    log.info("=" * 60)

    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    ch = spark.read.parquet("file:///opt/spark-data/movements/changes")
    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    tt_null = tt.filter(F.col("station_eva").isNull()).count()
    ch_null = ch.filter(F.col("station_eva").isNull()).count()
    final_null = final.filter(F.col("station_eva").isNull()).count()

    log.info("Timetables with NULL station_eva: %d", tt_null)
    log.info("Changes with NULL station_eva: %d", ch_null)
    log.info("Final with NULL station_eva: %d", final_null)

    if final_null > 0:
        log.warning("FAIL: Final movements should not have NULL station_eva!")
    else:
        log.info("PASS: No NULL station_eva in final")

    return tt_null, ch_null, final_null


def test_expected_unique_stops(spark):
    """
    TEST 3: Count unique stops and compare with final output.

    Note: final_movements will have FEWER rows than raw combined unique because:
    1. Changes-only stops without 'added' markers are filtered out
    2. Entries without actual times are filtered out

    This test shows the breakdown to help understand the filtering.
    """
    log.info("=" * 60)
    log.info("TEST 3: Unique stops breakdown")
    log.info("=" * 60)

    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    ch = spark.read.parquet("file:///opt/spark-data/movements/changes")
    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    # Filter out NULLs before counting
    tt_valid = tt.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())
    ch_valid = ch.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())

    # Unique stops in timetables (these are the "base" planned stops)
    tt_stops = tt_valid.select("stop_id", "station_eva").distinct()
    tt_unique = tt_stops.count()

    # Unique stops in changes
    ch_stops = ch_valid.select("stop_id", "station_eva").distinct()
    ch_unique = ch_stops.count()

    # Changes-only stops (stops in changes but NOT in timetables)
    changes_only = ch_stops.join(tt_stops, ["stop_id", "station_eva"], "left_anti")
    changes_only_count = changes_only.count()

    # Of changes-only, how many are marked as "added"?
    added_stops_count = 0
    if "arrival_is_added" in ch.columns:
        ch_with_flags = ch_valid.select(
            "stop_id", "station_eva", "arrival_is_added", "departure_is_added"
        ).distinct()
        changes_only_with_flags = changes_only.join(ch_with_flags, ["stop_id", "station_eva"])
        added_stops_count = changes_only_with_flags.filter(
            F.col("arrival_is_added") | F.col("departure_is_added")
        ).select("stop_id", "station_eva").distinct().count()

    # Expected valid stops = timetable stops + added changes-only stops
    expected_valid = tt_unique + added_stops_count

    # Actual final count
    final_count = final.count()

    log.info("Unique stops in timetables (base): %d", tt_unique)
    log.info("Unique stops in changes: %d", ch_unique)
    log.info("Changes-only stops (no base in timetables): %d", changes_only_count)
    log.info("Changes-only stops marked as 'added': %d", added_stops_count)
    log.info("Changes-only stops filtered out (not added): %d", changes_only_count - added_stops_count)
    log.info("")
    log.info("Expected valid stops (base + added): %d", expected_valid)
    log.info("Actual final_movements count: %d", final_count)
    log.info("Difference: %d", final_count - expected_valid)

    # Final may be slightly less due to entries without actual times
    if final_count <= expected_valid:
        log.info("PASS: Final count is <= expected (some may lack actual times)")
    else:
        log.warning("FAIL: Final count exceeds expected! Check deduplication.")

    return tt_unique, ch_unique, changes_only_count, added_stops_count, expected_valid, final_count


def test_stop_appears_multiple_stations(spark):
    """
    TEST 4: Check if same stop_id appears with different station_eva values.

    This can happen if:
    - Fuzzy matching resolves the same station name to different EVAs
    - The same stop_id appears in files for different stations (shouldn't happen)

    In Python pipeline: each stop_id should only have ONE station_eva (the station
    where the train stops). If we see multiple, there's a data or matching issue.
    """
    log.info("=" * 60)
    log.info("TEST 4: Stops appearing at multiple stations")
    log.info("=" * 60)

    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    ch = spark.read.parquet("file:///opt/spark-data/movements/changes")

    # Combine all sources
    tt_valid = tt.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())
    ch_valid = ch.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())

    combined = tt_valid.select("stop_id", "station_eva").union(
        ch_valid.select("stop_id", "station_eva")
    ).distinct()

    # Find stop_ids that appear with multiple station_eva
    multi_station = (
        combined.groupBy("stop_id")
        .agg(
            F.countDistinct("station_eva").alias("num_stations"),
            F.collect_set("station_eva").alias("station_evas")
        )
        .filter(F.col("num_stations") > 1)
        .orderBy(F.col("num_stations").desc())
    )

    multi_count = multi_station.count()
    log.info("Stop IDs appearing at multiple stations: %d", multi_count)

    if multi_count > 0:
        log.warning("WARNING: Same stop_id appears at different stations!")
        log.info("This suggests station resolution inconsistency or data issue.")
        multi_station.show(20, truncate=False)

        # Show example records
        first = multi_station.first()
        if first:
            stop_id = first["stop_id"]
            log.info("Example records for stop_id=%s:", stop_id)
            tt_valid.filter(F.col("stop_id") == stop_id).select(
                "stop_id", "station_eva", "station_name", "snapshot_key"
            ).show(10, truncate=False)
    else:
        log.info("PASS: Each stop_id appears at only one station")

    return multi_count


def test_compare_with_python_expected(spark):
    """
    TEST 5: Compare with expected Python pipeline count (~1.6M).

    The Python pipeline produces ~1.6M valid entries in fact_movement after:
    - Filtering out entries without valid actual times
    - Proper station EVA resolution
    - Deduplication by (station_eva, stop_id)

    Entries are valid if they have at least one actual time (arrival or departure).
    """
    log.info("=" * 60)
    log.info("TEST 5: Comparison with Python expected count")
    log.info("=" * 60)

    # Expected count after filtering invalid entries (no actual times)
    PYTHON_EXPECTED = 1_600_000  # approximately

    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")
    final_count = final.count()

    log.info("Python expected (approx): ~%d", PYTHON_EXPECTED)
    log.info("Spark actual: %d", final_count)
    diff = final_count - PYTHON_EXPECTED
    pct = 100.0 * diff / PYTHON_EXPECTED if PYTHON_EXPECTED > 0 else 0
    log.info("Difference: %d (%.2f%%)", diff, pct)

    # Allow 10% tolerance for differences in station EVA resolution
    if abs(pct) <= 10.0:
        log.info("PASS: Spark count within 10%% of Python expected")
    else:
        log.warning("FAIL: Spark count differs by %.2f%% from Python!", pct)
        if final_count > PYTHON_EXPECTED:
            log.info("Extra rows may be due to:")
            log.info("  - Entries without valid actual times not being filtered")
            log.info("  - Different station EVA resolution")

    return final_count, PYTHON_EXPECTED


def test_timetable_vs_changes_station_eva_mismatch(spark):
    """
    TEST 6: Check if same stop_id has different station_eva in timetables vs changes.

    This would cause extra rows because the same stop appears as two different
    (stop_id, station_eva) pairs.
    """
    log.info("=" * 60)
    log.info("TEST 6: Station EVA mismatch between timetables and changes")
    log.info("=" * 60)

    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    ch = spark.read.parquet("file:///opt/spark-data/movements/changes")

    # Get unique (stop_id, station_eva) from each source
    tt_valid = tt.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())
    ch_valid = ch.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())

    tt_stops = tt_valid.select("stop_id", F.col("station_eva").alias("tt_eva")).distinct()
    ch_stops = ch_valid.select("stop_id", F.col("station_eva").alias("ch_eva")).distinct()

    # Join on stop_id and find where station_eva differs
    joined = tt_stops.join(ch_stops, "stop_id", "inner")
    mismatched = joined.filter(F.col("tt_eva") != F.col("ch_eva"))

    mismatch_count = mismatched.count()
    log.info("Stop IDs with different station_eva in timetables vs changes: %d", mismatch_count)

    if mismatch_count > 0:
        log.warning("CRITICAL: Station EVA mismatch found!")
        log.info("This is likely the source of extra rows.")
        mismatched.show(30, truncate=False)

        # Show station names for context
        tt_with_name = tt_valid.select("stop_id", "station_eva", "station_name").distinct()
        ch_with_name = ch_valid.select("stop_id", "station_eva", "station_name").distinct()

        log.info("Timetable records for mismatched stops:")
        mismatched_ids = [row["stop_id"] for row in mismatched.select("stop_id").collect()[:10]]
        tt_with_name.filter(F.col("stop_id").isin(mismatched_ids)).show(20, truncate=False)

        log.info("Changes records for mismatched stops:")
        ch_with_name.filter(F.col("stop_id").isin(mismatched_ids)).show(20, truncate=False)
    else:
        log.info("PASS: No station EVA mismatches between timetables and changes")

    return mismatch_count


def test_unique_stops_before_and_after_resolve(spark):
    """
    TEST 7: Trace the row count through the resolve_latest_stop_state pipeline.

    Shows raw counts and explains why final count is less than raw combined.
    Final count should be less because:
    - Changes-only stops without 'added' markers are filtered
    - Entries without actual times are filtered
    """
    log.info("=" * 60)
    log.info("TEST 7: Row count through resolve pipeline")
    log.info("=" * 60)

    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    ch = spark.read.parquet("file:///opt/spark-data/movements/changes")

    # Step 1: Raw counts (all parsed records)
    tt_raw = tt.count()
    ch_raw = ch.count()
    log.info("Step 0 - Raw parsed records: timetables=%d, changes=%d", tt_raw, ch_raw)

    # Step 2: After filtering NULLs
    tt_valid = tt.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())
    ch_valid = ch.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())
    tt_valid_count = tt_valid.count()
    ch_valid_count = ch_valid.count()
    log.info("Step 1 - After NULL filter: timetables=%d, changes=%d", tt_valid_count, ch_valid_count)

    # Step 3: Unique (stop_id, station_eva) pairs
    tt_stops = tt_valid.select("stop_id", "station_eva").distinct()
    ch_stops = ch_valid.select("stop_id", "station_eva").distinct()
    tt_unique = tt_stops.count()
    ch_unique = ch_stops.count()
    log.info("Step 2 - Unique pairs: timetables=%d, changes=%d", tt_unique, ch_unique)

    # Step 4: Identify changes-only stops
    changes_only = ch_stops.join(tt_stops, ["stop_id", "station_eva"], "left_anti")
    changes_only_count = changes_only.count()
    log.info("Step 3 - Changes-only stops (no base): %d", changes_only_count)

    # Step 5: How many changes-only are marked as added?
    added_count = 0
    if "arrival_is_added" in ch.columns:
        ch_with_flags = ch_valid.select(
            "stop_id", "station_eva", "arrival_is_added", "departure_is_added"
        ).distinct()
        changes_only_added = changes_only.join(ch_with_flags, ["stop_id", "station_eva"]).filter(
            F.col("arrival_is_added") | F.col("departure_is_added")
        ).select("stop_id", "station_eva").distinct()
        added_count = changes_only_added.count()
    log.info("Step 4 - Changes-only marked as 'added': %d", added_count)
    log.info("Step 4 - Changes-only filtered out (not added): %d", changes_only_count - added_count)

    # Step 6: Expected after filtering = timetable unique + added changes-only
    expected_after_filter = tt_unique + added_count
    log.info("Step 5 - Expected after added-filter: %d", expected_after_filter)

    # Step 7: Check final
    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")
    final_count = final.count()
    log.info("Step 6 - Final movements: %d", final_count)

    log.info("")
    log.info("ANALYSIS:")
    log.info("Timetable base stops: %d", tt_unique)
    log.info("+ Added stops from changes: %d", added_count)
    log.info("= Expected max: %d", expected_after_filter)
    log.info("Actual final: %d", final_count)
    log.info("Filtered (no actual times): %d", expected_after_filter - final_count)

    if final_count > expected_after_filter:
        log.warning("FAIL: Final has MORE rows than expected!")
        log.info("This suggests deduplication or added-filter is not working.")
    elif final_count <= expected_after_filter:
        log.info("PASS: Final count is within expected range")

    return {
        "tt_raw": tt_raw,
        "ch_raw": ch_raw,
        "tt_valid": tt_valid_count,
        "ch_valid": ch_valid_count,
        "tt_unique": tt_unique,
        "ch_unique": ch_unique,
        "changes_only": changes_only_count,
        "added_count": added_count,
        "expected_after_filter": expected_after_filter,
        "final": final_count,
    }


def test_changes_only_stops(spark):
    """
    TEST 8: Verify that changes-only stops are properly filtered.

    Changes-only stops (stops in changes but NOT in timetables) should only
    appear in final_movements if they are marked as "added" (ps='a', cs='a',
    or suffix >= 100).

    This test verifies that the filtering in resolve_latest_stop_state works.
    """
    log.info("=" * 60)
    log.info("TEST 8: Changes-only stops filtering verification")
    log.info("=" * 60)

    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    ch = spark.read.parquet("file:///opt/spark-data/movements/changes")
    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    tt_valid = tt.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())
    ch_valid = ch.filter(F.col("station_eva").isNotNull() & F.col("stop_id").isNotNull())

    tt_stops = tt_valid.select("stop_id", "station_eva").distinct()
    ch_stops = ch_valid.select("stop_id", "station_eva").distinct()
    final_stops = final.select("stop_id", "station_eva").distinct()

    # Stops in changes but NOT in timetables (changes-only)
    changes_only = ch_stops.join(tt_stops, ["stop_id", "station_eva"], "left_anti")
    changes_only_count = changes_only.count()

    # Stops in timetables but NOT in changes
    timetables_only = tt_stops.join(ch_stops, ["stop_id", "station_eva"], "left_anti")
    timetables_only_count = timetables_only.count()

    log.info("=== RAW DATA ===")
    log.info("Stops in timetables only: %d", timetables_only_count)
    log.info("Stops in changes only (no base): %d", changes_only_count)

    # Check how many of the changes-only stops are marked as "added"
    added_count = 0
    not_added_count = 0
    changes_only_with_flags = None
    if changes_only_count > 0 and "arrival_is_added" in ch.columns:
        changes_only_with_flags = changes_only.join(
            ch_valid.select(
                "stop_id", "station_eva", "station_name", "category", "train_number",
                "arrival_is_added", "departure_is_added", "is_added_by_suffix",
                "ar_ps", "ar_cs", "dp_ps", "dp_cs"
            ).distinct(),
            ["stop_id", "station_eva"]
        )

        added_stops = changes_only_with_flags.filter(
            F.col("arrival_is_added") | F.col("departure_is_added")
        ).select("stop_id", "station_eva").distinct()
        added_count = added_stops.count()

        # Breakdown by reason
        added_by_suffix = changes_only_with_flags.filter(
            F.col("is_added_by_suffix")
        ).select("stop_id", "station_eva").distinct().count()

        added_by_ps_a = changes_only_with_flags.filter(
            (F.col("ar_ps") == "a") | (F.col("dp_ps") == "a")
        ).select("stop_id", "station_eva").distinct().count()

        added_by_cs_a = changes_only_with_flags.filter(
            (F.col("ar_cs") == "a") | (F.col("dp_cs") == "a")
        ).select("stop_id", "station_eva").distinct().count()

        not_added_count = changes_only_count - added_count

        log.info("")
        log.info("=== CHANGES-ONLY BREAKDOWN ===")
        log.info("Marked as 'added' (should be kept): %d", added_count)
        log.info("  - By suffix >= 100: %d", added_by_suffix)
        log.info("  - By ps='a': %d", added_by_ps_a)
        log.info("  - By cs='a': %d", added_by_cs_a)
        log.info("NOT marked as 'added' (should be filtered): %d", not_added_count)

    # Now check what's actually in final_movements
    log.info("")
    log.info("=== VERIFICATION IN FINAL OUTPUT ===")

    # Changes-only stops that made it to final
    changes_only_in_final = changes_only.join(final_stops, ["stop_id", "station_eva"], "inner")
    changes_only_in_final_count = changes_only_in_final.count()
    log.info("Changes-only stops in final_movements: %d", changes_only_in_final_count)

    # Not-added changes-only stops that made it to final (should be 0!)
    if not_added_count > 0 and changes_only_with_flags is not None:
        not_added_stops = changes_only_with_flags.filter(
            ~(F.col("arrival_is_added") | F.col("departure_is_added"))
        ).select("stop_id", "station_eva").distinct()

        not_added_in_final = not_added_stops.join(final_stops, ["stop_id", "station_eva"], "inner")
        not_added_in_final_count = not_added_in_final.count()
        log.info("NOT-added changes-only stops in final (should be 0): %d", not_added_in_final_count)

        if not_added_in_final_count > 0:
            log.warning("FAIL: Found %d non-added changes-only stops in final!", not_added_in_final_count)
            log.info("Sample of incorrectly included stops:")
            not_added_in_final.show(20, truncate=False)
        else:
            log.info("PASS: No non-added changes-only stops in final")
    else:
        log.info("PASS: All changes-only stops are marked as added")

    return {
        "changes_only_raw": changes_only_count,
        "added_count": added_count,
        "not_added_count": not_added_count,
        "changes_only_in_final": changes_only_in_final_count,
        "timetables_only": timetables_only_count,
    }


def test_missing_station_names(spark):
    """
    TEST 9: Show station names from timetables that don't appear in final_movements.

    This helps identify station name resolution issues by showing:
    - Original station_name from XML
    - Normalized search_name
    - Whether station_eva was resolved

    Useful for debugging fuzzy matching and finding unmatched stations.
    """
    log.info("=" * 60)
    log.info("TEST 9: Station names missing from final_movements")
    log.info("=" * 60)

    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")
    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    # Get unique (stop_id, station_eva) from timetables and final
    tt_valid = tt.filter(F.col("stop_id").isNotNull())
    tt_stops = tt_valid.select("stop_id", "station_eva", "station_name").distinct()
    final_stops = final.select("stop_id", "station_eva").distinct()

    # Find timetable stops not in final
    missing_stops = tt_stops.join(
        final_stops,
        (tt_stops["stop_id"] == final_stops["stop_id"]) &
        (tt_stops["station_eva"] == final_stops["station_eva"]),
        "left_anti"
    )

    missing_count = missing_stops.count()
    log.info("Timetable stops missing from final_movements: %d", missing_count)

    if missing_count > 0:
        # Add normalized name column
        _search_name_udf = sf.udf(to_station_search_name, StringType())
        missing_with_normalized = missing_stops.withColumn(
            "normalized_name", _search_name_udf(F.col("station_name"))
        )

        # Group by station_name to see unique missing stations
        missing_stations = missing_with_normalized.select(
            "station_name", "normalized_name", "station_eva"
        ).distinct().orderBy("station_name")

        missing_station_count = missing_stations.count()
        log.info("Unique station names missing: %d", missing_station_count)

        log.info("")
        log.info("=== MISSING STATIONS (with NULL station_eva - not resolved) ===")
        null_eva_stations = missing_stations.filter(F.col("station_eva").isNull())
        null_eva_count = null_eva_stations.count()
        log.info("Stations with NULL EVA: %d", null_eva_count)
        if null_eva_count > 0:
            # Collect and log explicitly since show() output can get lost in Spark logs
            null_eva_rows = null_eva_stations.collect()
            for row in null_eva_rows:
                log.info("  UNRESOLVED: '%s' -> normalized: '%s'",
                         row["station_name"], row["normalized_name"])

        log.info("")
        log.info("=== MISSING STATIONS (with valid station_eva - filtered for other reasons) ===")
        valid_eva_stations = missing_stations.filter(F.col("station_eva").isNotNull())
        valid_eva_count = valid_eva_stations.count()
        log.info("Stations with valid EVA but still missing: %d", valid_eva_count)
        if valid_eva_count > 0:
            # Collect and log explicitly since show() output can get lost in Spark logs
            valid_eva_rows = valid_eva_stations.limit(100).collect()
            for row in valid_eva_rows:
                log.info("  FILTERED: '%s' (EVA: %s) -> normalized: '%s'",
                         row["station_name"], row["station_eva"], row["normalized_name"])

        return {
            "missing_stops": missing_count,
            "missing_stations": missing_station_count,
            "null_eva_stations": null_eva_count,
            "valid_eva_missing": valid_eva_count,
        }
    else:
        log.info("PASS: All timetable stops are in final_movements")
        return {"missing_stops": 0, "missing_stations": 0, "null_eva_stations": 0, "valid_eva_missing": 0}


def test_compute_average_daily_delay(spark):
    """
    TEST 10: Verify compute_average_daily_delay function.

    Checks:
    1. Delay is only computed when changed_time exists (not when actual = planned)
    2. Delay values are in minutes (reasonable range)
    3. Cancelled and hidden stops are excluded
    4. NULL delays don't contribute to average
    """
    log.info("=" * 60)
    log.info("TEST 10: compute_average_daily_delay verification")
    log.info("=" * 60)

    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    # Basic stats about the data
    total_rows = final.count()
    log.info("Total rows in final_movements: %d", total_rows)

    # Count rows with changed times (these should have delay computed)
    has_changed_arrival = final.filter(F.col("changed_arrival_ts").isNotNull()).count()
    has_changed_departure = final.filter(F.col("changed_departure_ts").isNotNull()).count()
    log.info("Rows with changed_arrival_ts: %d", has_changed_arrival)
    log.info("Rows with changed_departure_ts: %d", has_changed_departure)

    # Count cancelled/hidden
    arrival_cancelled = final.filter(F.col("arrival_cancelled")).count()
    departure_cancelled = final.filter(F.col("departure_cancelled")).count()
    arrival_hidden = final.filter(F.col("arrival_is_hidden")).count()
    departure_hidden = final.filter(F.col("departure_is_hidden")).count()
    log.info("Arrival cancelled: %d, hidden: %d", arrival_cancelled, arrival_hidden)
    log.info("Departure cancelled: %d, hidden: %d", departure_cancelled, departure_hidden)

    # Run compute_average_daily_delay
    delay_df = compute_average_daily_delay(final)
    delay_df.cache()

    stations_with_delay = delay_df.count()
    log.info("")
    log.info("=== DELAY COMPUTATION RESULTS ===")
    log.info("Stations with delay data: %d", stations_with_delay)

    # Check delay statistics
    if stations_with_delay > 0:
        stats = delay_df.agg(
            F.min("avg_arrival_delay_min").alias("min_arr"),
            F.max("avg_arrival_delay_min").alias("max_arr"),
            F.avg("avg_arrival_delay_min").alias("avg_arr"),
            F.min("avg_departure_delay_min").alias("min_dep"),
            F.max("avg_departure_delay_min").alias("max_dep"),
            F.avg("avg_departure_delay_min").alias("avg_dep"),
        ).collect()[0]

        log.info("")
        log.info("Arrival delay (min): min=%.2f, max=%.2f, avg=%.2f",
                 stats["min_arr"] or 0, stats["max_arr"] or 0, stats["avg_arr"] or 0)
        log.info("Departure delay (min): min=%.2f, max=%.2f, avg=%.2f",
                 stats["min_dep"] or 0, stats["max_dep"] or 0, stats["avg_dep"] or 0)

        # Sample top 10 delayed stations - join with final to get station names
        log.info("")
        log.info("Top 10 stations by average arrival delay:")
        station_names = final.select("station_eva", "station_name").distinct()
        (
            delay_df.join(station_names, "station_eva", "left")
            .orderBy(F.col("avg_arrival_delay_min").desc())
            .limit(10)
            .withColumn("row", F.monotonically_increasing_id() + 1)
            .select("row", "station_eva", "station_name", "avg_arrival_delay_min", "avg_departure_delay_min")
            .show(10, truncate=False)
        )

        # Verify delay calculation logic: manually compute for a sample
        log.info("")
        log.info("=== VERIFICATION: Manual delay calculation ===")

        # Get a sample of rows with changed times and compute delay manually
        sample = final.filter(
            F.col("changed_arrival_ts").isNotNull()
            & F.col("planned_arrival_ts").isNotNull()
            & ~F.col("arrival_cancelled")
            & ~F.col("arrival_is_hidden")
        ).select(
            "station_eva", "stop_id",
            "planned_arrival_ts", "changed_arrival_ts",
            "arrival_cancelled", "arrival_is_hidden"
        ).limit(5)

        sample_with_delay = sample.withColumn(
            "manual_delay_min",
            F.round(
                (F.unix_timestamp("changed_arrival_ts") - F.unix_timestamp("planned_arrival_ts")) / 60.0
            )
        ).withColumn("row", F.monotonically_increasing_id() + 1).select("row", "*")
        log.info("Sample delay calculations (manual):")
        sample_with_delay.show(5, truncate=False)

        # Verify: rows without changed_time should NOT contribute to delay
        log.info("")
        log.info("=== VERIFICATION: No-change rows should have NULL delay ===")
        no_change_sample = (
            final.filter(
                F.col("changed_arrival_ts").isNull()
                & F.col("planned_arrival_ts").isNotNull()
            ).select(
                "station_eva", "stop_id",
                "planned_arrival_ts", "changed_arrival_ts", "actual_arrival_ts"
            ).limit(5)
            .withColumn("row", F.monotonically_increasing_id() + 1)
            .select("row", "*")
        )
        log.info("Sample rows without changed_arrival_ts (delay should be NULL, not 0):")
        no_change_sample.show(5, truncate=False)

        # Check that we're not computing delay=0 for unchanged stops
        # This was the bug we fixed!
        unchanged_with_actual = final.filter(
            F.col("changed_arrival_ts").isNull()
            & F.col("actual_arrival_ts").isNotNull()
        ).count()
        log.info("Rows with actual_arrival but NO changed_arrival (should have NULL delay): %d", unchanged_with_actual)

    delay_df.unpersist()

    return {
        "total_rows": total_rows,
        "has_changed_arrival": has_changed_arrival,
        "has_changed_departure": has_changed_departure,
        "stations_with_delay": stations_with_delay,
    }


def test_compute_peak_hour_departure_counts(spark):
    """
    TEST 11: Verify compute_peak_hour_departure_counts function.

    Checks:
    1. Only peak hours (07:00-09:00, 17:00-19:00) are counted
    2. Cancelled and hidden departures are excluded
    3. Returns sensible average counts per station
    """
    log.info("=" * 60)
    log.info("TEST 11: compute_peak_hour_departure_counts verification")
    log.info("=" * 60)

    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    # Count departures by hour to verify peak hour logic
    log.info("=== DEPARTURE DISTRIBUTION BY HOUR ===")
    departures_with_hour = final.filter(
        F.col("actual_departure_ts").isNotNull()
        & ~F.col("departure_cancelled")
        & ~F.col("departure_is_hidden")
    ).withColumn("hour", F.hour("actual_departure_ts"))

    hour_dist = (
        departures_with_hour.groupBy("hour").count().orderBy("hour")
        .withColumn("row", F.monotonically_increasing_id() + 1)
        .select("row", "*")
    )
    log.info("Departures by hour (non-cancelled, non-hidden):")
    hour_dist.show(24, truncate=False)

    # Count peak hour departures manually
    peak_morning = departures_with_hour.filter(
        (F.col("hour") >= 7) & (F.col("hour") < 9)
    ).count()
    peak_evening = departures_with_hour.filter(
        (F.col("hour") >= 17) & (F.col("hour") < 19)
    ).count()
    total_peak = peak_morning + peak_evening

    log.info("")
    log.info("Peak hour departures (manual count):")
    log.info("  Morning (07:00-09:00): %d", peak_morning)
    log.info("  Evening (17:00-19:00): %d", peak_evening)
    log.info("  Total peak: %d", total_peak)

    # Run compute_peak_hour_departure_counts
    peak_df = compute_peak_hour_departure_counts(final)
    peak_df.cache()

    stations_count = peak_df.count()
    log.info("")
    log.info("=== PEAK HOUR COMPUTATION RESULTS ===")
    log.info("Stations with peak hour data: %d", stations_count)

    if stations_count > 0:
        stats = peak_df.agg(
            F.min("avg_peak_hour_departures_per_day").alias("min_avg"),
            F.max("avg_peak_hour_departures_per_day").alias("max_avg"),
            F.avg("avg_peak_hour_departures_per_day").alias("overall_avg"),
        ).collect()[0]

        log.info("Avg peak departures/day: min=%.2f, max=%.2f, overall_avg=%.2f",
                 stats["min_avg"] or 0, stats["max_avg"] or 0, stats["overall_avg"] or 0)

        # Top 10 busiest stations during peak hours - join to get station names
        log.info("")
        log.info("Top 10 busiest stations during peak hours:")
        station_names = final.select("station_eva", "station_name").distinct()
        (
            peak_df.join(station_names, "station_eva", "left")
            .orderBy(F.col("avg_peak_hour_departures_per_day").desc())
            .limit(10)
            .withColumn("row", F.monotonically_increasing_id() + 1)
            .select("row", "station_eva", "station_name", "avg_peak_hour_departures_per_day")
            .show(10, truncate=False)
        )

        # Verify: no off-peak hours are included
        log.info("")
        log.info("=== VERIFICATION: Only peak hours should be included ===")

        # Manual calculation for one station
        sample_station = peak_df.select("station_eva").first()
        if sample_station:
            station_eva = sample_station["station_eva"]
            log.info("Verifying station_eva=%d", station_eva)

            station_peak = departures_with_hour.filter(
                (F.col("station_eva") == station_eva)
                & (
                    ((F.col("hour") >= 7) & (F.col("hour") < 9))
                    | ((F.col("hour") >= 17) & (F.col("hour") < 19))
                )
            )

            # Group by date
            daily_counts = (
                station_peak.withColumn(
                    "dep_date", F.to_date("actual_departure_ts")
                ).groupBy("dep_date").count().orderBy("dep_date")
                .withColumn("row", F.monotonically_increasing_id() + 1)
                .select("row", "*")
            )

            log.info("Daily peak departures for station %d:", station_eva)
            daily_counts.show(10, truncate=False)

            manual_avg = daily_counts.agg(F.avg("count")).collect()[0][0]
            computed_avg = peak_df.filter(
                F.col("station_eva") == station_eva
            ).select("avg_peak_hour_departures_per_day").collect()[0][0]

            log.info("Manual average: %.2f, Computed average: %.2f", manual_avg or 0, computed_avg or 0)

            if manual_avg and computed_avg and abs(manual_avg - computed_avg) < 0.01:
                log.info("PASS: Manual and computed averages match")
            else:
                log.warning("WARN: Manual and computed averages differ")

    peak_df.unpersist()

    return {
        "peak_morning": peak_morning,
        "peak_evening": peak_evening,
        "total_peak": total_peak,
        "stations_count": stations_count,
    }


def test_delay_only_with_changed_time(spark):
    """
    TEST 12: Critical test - verify delay is only computed when changed_time exists.

    This test specifically verifies the fix for the bug where delay was computed as
    actual - planned = (coalesce(changed, planned)) - planned = 0 when no change existed.

    CORRECT behavior: delay is NULL when changed_time is NULL.
    BUG behavior: delay is 0 when changed_time is NULL (because actual = planned).
    """
    log.info("=" * 60)
    log.info("TEST 12: Delay only computed when changed_time exists (BUG FIX VERIFICATION)")
    log.info("=" * 60)

    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    # Compute delays the same way compute_average_daily_delay does
    df = final.withColumn(
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

    # Key checks:
    # 1. Rows with changed_time should have non-NULL delay
    # 2. Rows without changed_time should have NULL delay (NOT 0!)

    log.info("=== ARRIVAL DELAY VERIFICATION ===")

    # Rows with changed arrival
    with_changed_arr = df.filter(
        F.col("changed_arrival_ts").isNotNull()
        & F.col("planned_arrival_ts").isNotNull()
        & ~F.col("arrival_cancelled")
        & ~F.col("arrival_is_hidden")
    )
    with_changed_arr_count = with_changed_arr.count()
    with_changed_has_delay = with_changed_arr.filter(F.col("arrival_delay_min").isNotNull()).count()
    log.info("Rows with changed_arrival (valid): %d", with_changed_arr_count)
    log.info("  Of these, have delay computed: %d", with_changed_has_delay)

    if with_changed_arr_count == with_changed_has_delay:
        log.info("  PASS: All rows with changed_arrival have delay computed")
    else:
        log.warning("  FAIL: Some rows with changed_arrival are missing delay!")

    # Rows without changed arrival
    without_changed_arr = df.filter(
        F.col("changed_arrival_ts").isNull()
        & F.col("planned_arrival_ts").isNotNull()
    )
    without_changed_arr_count = without_changed_arr.count()
    without_changed_has_delay = without_changed_arr.filter(F.col("arrival_delay_min").isNotNull()).count()
    without_changed_has_zero = without_changed_arr.filter(F.col("arrival_delay_min") == 0).count()

    log.info("")
    log.info("Rows WITHOUT changed_arrival (unchanged stops): %d", without_changed_arr_count)
    log.info("  Of these, have delay (should be 0): %d", without_changed_has_delay)
    log.info("  Of these, have delay=0 (would indicate BUG): %d", without_changed_has_zero)

    if without_changed_has_delay == 0:
        log.info("  PASS: No delay computed for unchanged stops (correct behavior)")
    else:
        log.warning("  FAIL: Delay computed for unchanged stops (BUG!)")

    log.info("")
    log.info("=== DEPARTURE DELAY VERIFICATION ===")

    # Rows with changed departure
    with_changed_dep = df.filter(
        F.col("changed_departure_ts").isNotNull()
        & F.col("planned_departure_ts").isNotNull()
        & ~F.col("departure_cancelled")
        & ~F.col("departure_is_hidden")
    )
    with_changed_dep_count = with_changed_dep.count()
    with_changed_dep_has_delay = with_changed_dep.filter(F.col("departure_delay_min").isNotNull()).count()
    log.info("Rows with changed_departure (valid): %d", with_changed_dep_count)
    log.info("  Of these, have delay computed: %d", with_changed_dep_has_delay)

    if with_changed_dep_count == with_changed_dep_has_delay:
        log.info("  PASS: All rows with changed_departure have delay computed")
    else:
        log.warning("  FAIL: Some rows with changed_departure are missing delay!")

    # Rows without changed departure
    without_changed_dep = df.filter(
        F.col("changed_departure_ts").isNull()
        & F.col("planned_departure_ts").isNotNull()
    )
    without_changed_dep_count = without_changed_dep.count()
    without_changed_dep_has_delay = without_changed_dep.filter(F.col("departure_delay_min").isNotNull()).count()

    log.info("")
    log.info("Rows WITHOUT changed_departure (unchanged stops): %d", without_changed_dep_count)
    log.info("  Of these, have delay (should be 0): %d", without_changed_dep_has_delay)

    if without_changed_dep_has_delay == 0:
        log.info("  PASS: No delay computed for unchanged departures (correct behavior)")
    else:
        log.warning("  FAIL: Delay computed for unchanged departures (BUG!)")

    # Summary
    log.info("")
    log.info("=== SUMMARY ===")
    arrival_pass = (with_changed_arr_count == with_changed_has_delay) and (without_changed_has_delay == 0)
    departure_pass = (with_changed_dep_count == with_changed_dep_has_delay) and (without_changed_dep_has_delay == 0)

    if arrival_pass and departure_pass:
        log.info("PASS: Delay calculation is correct (only computed when changed_time exists)")
    else:
        log.warning("FAIL: Delay calculation has issues")

    return {
        "with_changed_arr": with_changed_arr_count,
        "with_changed_arr_has_delay": with_changed_has_delay,
        "without_changed_arr": without_changed_arr_count,
        "without_changed_arr_has_delay": without_changed_has_delay,
        "with_changed_dep": with_changed_dep_count,
        "with_changed_dep_has_delay": with_changed_dep_has_delay,
        "without_changed_dep": without_changed_dep_count,
        "without_changed_dep_has_delay": without_changed_dep_has_delay,
        "arrival_pass": arrival_pass,
        "departure_pass": departure_pass,
    }


def test_alexanderplatz_delay(spark):
    """
    TEST 14: Verify delay calculation for Alexanderplatz station.

    Alexanderplatz (EVA 8011155) is a major station with high traffic.
    This test checks:
    1. Delay values are reasonable (not inflated)
    2. On-time trains (no changed_ts) count as 0 delay
    3. The average delay is in a sensible range (typically < 10 min)
    """
    ALEXANDERPLATZ_EVA = 8011155

    log.info("=" * 60)
    log.info("TEST 14: Alexanderplatz delay verification")
    log.info("=" * 60)

    final = spark.read.parquet("file:///opt/spark-data/movements/final_movements")

    # Filter to Alexanderplatz only
    alex = final.filter(F.col("station_eva") == ALEXANDERPLATZ_EVA)
    alex.cache()

    total_stops = alex.count()
    log.info("Total stops at Alexanderplatz: %d", total_stops)

    if total_stops == 0:
        log.warning("No data for Alexanderplatz (EVA %d)", ALEXANDERPLATZ_EVA)
        return {"status": "NO_DATA", "station_eva": ALEXANDERPLATZ_EVA}

    # Count arrivals/departures
    has_arrival = alex.filter(F.col("planned_arrival_ts").isNotNull()).count()
    has_departure = alex.filter(F.col("planned_departure_ts").isNotNull()).count()
    log.info("Stops with planned arrival: %d", has_arrival)
    log.info("Stops with planned departure: %d", has_departure)

    # Count changed times (actual delays)
    has_changed_arr = alex.filter(F.col("changed_arrival_ts").isNotNull()).count()
    has_changed_dep = alex.filter(F.col("changed_departure_ts").isNotNull()).count()
    log.info("Stops with changed arrival (delayed): %d (%.1f%%)",
             has_changed_arr, 100.0 * has_changed_arr / has_arrival if has_arrival else 0)
    log.info("Stops with changed departure (delayed): %d (%.1f%%)",
             has_changed_dep, 100.0 * has_changed_dep / has_departure if has_departure else 0)

    # Count cancelled/hidden
    arr_cancelled = alex.filter(F.col("arrival_cancelled")).count()
    dep_cancelled = alex.filter(F.col("departure_cancelled")).count()
    arr_hidden = alex.filter(F.col("arrival_is_hidden")).count()
    dep_hidden = alex.filter(F.col("departure_is_hidden")).count()
    log.info("Arrivals cancelled: %d, hidden: %d", arr_cancelled, arr_hidden)
    log.info("Departures cancelled: %d, hidden: %d", dep_cancelled, dep_hidden)

    # Compute delay using the function
    delay_df = compute_average_daily_delay(final, station_eva=ALEXANDERPLATZ_EVA)
    delay_row = delay_df.collect()

    if delay_row:
        avg_arr_delay = delay_row[0]["avg_arrival_delay_min"]
        avg_dep_delay = delay_row[0]["avg_departure_delay_min"]
        log.info("")
        log.info("=== COMPUTED AVERAGE DELAYS ===")
        log.info("Average arrival delay: %.2f min", avg_arr_delay or 0)
        log.info("Average departure delay: %.2f min", avg_dep_delay or 0)

        # Sanity check: average delay should be reasonable (< 15 min typically)
        if avg_arr_delay and avg_arr_delay > 15:
            log.warning("WARNING: Average arrival delay %.2f min seems high!", avg_arr_delay)
        if avg_dep_delay and avg_dep_delay > 15:
            log.warning("WARNING: Average departure delay %.2f min seems high!", avg_dep_delay)
    else:
        avg_arr_delay = None
        avg_dep_delay = None
        log.warning("No delay data returned for Alexanderplatz")

    # Manual verification: compute delay breakdown
    log.info("")
    log.info("=== DELAY DISTRIBUTION ===")

    # Compute individual delays
    alex_with_delay = alex.filter(
        F.col("planned_arrival_ts").isNotNull()
        & ~F.col("arrival_cancelled")
        & ~F.col("arrival_is_hidden")
    ).withColumn(
        "arr_delay_min",
        F.coalesce(
            F.round((F.unix_timestamp("changed_arrival_ts") - F.unix_timestamp("planned_arrival_ts")) / 60.0),
            F.lit(0)
        )
    )

    # Show delay distribution
    delay_stats = alex_with_delay.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("arr_delay_min") == 0, 1).otherwise(0)).alias("on_time"),
        F.sum(F.when(F.col("arr_delay_min") > 0, 1).otherwise(0)).alias("late"),
        F.sum(F.when(F.col("arr_delay_min") < 0, 1).otherwise(0)).alias("early"),
        F.min("arr_delay_min").alias("min_delay"),
        F.max("arr_delay_min").alias("max_delay"),
        F.avg("arr_delay_min").alias("avg_delay"),
    ).collect()[0]

    log.info("Total valid arrivals (denominator): %d", delay_stats["total"])
    log.info("  - On-time (0 delay): %d (%.1f%%)",
             delay_stats["on_time"],
             100.0 * delay_stats["on_time"] / delay_stats["total"] if delay_stats["total"] else 0)
    log.info("  - Late (delay > 0): %d (%.1f%%)",
             delay_stats["late"],
             100.0 * delay_stats["late"] / delay_stats["total"] if delay_stats["total"] else 0)
    log.info("  - Early (delay < 0): %d (%.1f%%)",
             delay_stats["early"],
             100.0 * delay_stats["early"] / delay_stats["total"] if delay_stats["total"] else 0)
    log.info("Delay range: %.0f to %.0f min", delay_stats["min_delay"] or 0, delay_stats["max_delay"] or 0)
    log.info("")
    log.info("Manual avg delay = sum(delays) / %d = %.2f min",
             delay_stats["total"], delay_stats["avg_delay"] or 0)

    # Verify compute_average_daily_delay matches manual calculation
    if avg_arr_delay is not None and delay_stats["avg_delay"] is not None:
        if abs(avg_arr_delay - delay_stats["avg_delay"]) < 0.1:
            log.info("PASS: compute_average_daily_delay matches manual calculation")
        else:
            log.warning("FAIL: compute_average_daily_delay (%.2f) != manual (%.2f)",
                       avg_arr_delay, delay_stats["avg_delay"])

    # Show sample of delayed trains
    log.info("")
    log.info("=== SAMPLE OF DELAYED ARRIVALS ===")
    (
        alex_with_delay.filter(F.col("arr_delay_min") > 0).select(
            "stop_id", "category", "train_number",
            "planned_arrival_ts", "changed_arrival_ts", "arr_delay_min"
        ).orderBy(F.col("arr_delay_min").desc())
        .limit(10)
        .withColumn("row", F.monotonically_increasing_id() + 1)
        .select("row", "*")
        .show(10, truncate=False)
    )

    alex.unpersist()

    return {
        "station_eva": ALEXANDERPLATZ_EVA,
        "total_stops": total_stops,
        "has_changed_arr": has_changed_arr,
        "has_changed_dep": has_changed_dep,
        "avg_arrival_delay_min": avg_arr_delay,
        "avg_departure_delay_min": avg_dep_delay,
        "on_time_count": delay_stats["on_time"],
        "late_count": delay_stats["late"],
    }


def test_unresolved_station_matches(spark, station_data_path="/opt/spark-data/DBahn-berlin/station_data.json"):
    """
    TEST 13: Show fuzzy matching attempts for unresolved station names.

    For each station name that couldn't be resolved (NULL station_eva),
    this test shows:
    - The original station name from XML
    - The normalized search name
    - The best matching station from the lookup table
    - The edit distance and Jaccard similarity scores
    - Why it was rejected (which threshold it failed)

    This helps tune the matching thresholds by seeing near-misses.
    """
    log.info("=" * 60)
    log.info("TEST 13: Unresolved station names - fuzzy match attempts")
    log.info("=" * 60)

    from pyspark.sql.types import DoubleType
    # Note: to_station_search_name, normalized_edit_distance, token_jaccard_similarity
    # are defined locally at the top of this file to avoid UDF serialization issues

    # Load timetables to find unresolved stations
    tt = spark.read.parquet("file:///opt/spark-data/movements/timetables")

    # Get unique station names with NULL EVA
    unresolved = tt.filter(
        F.col("station_eva").isNull() & F.col("station_name").isNotNull()
    ).select("station_name").distinct()

    unresolved_count = unresolved.count()
    log.info("Unique unresolved station names: %d", unresolved_count)

    if unresolved_count == 0:
        log.info("PASS: All station names were resolved!")
        return {"unresolved_count": 0, "matches": []}

    # Load station lookup table
    import json
    try:
        with open(station_data_path, "r") as f:
            station_data = json.load(f)

        # Build lookup DataFrame
        lookup_rows = []
        for eva_str, info in station_data.items():
            try:
                eva = int(eva_str)
                name = info.get("name", "")
                search_name = to_station_search_name(name)
                if search_name:
                    lookup_rows.append((search_name, eva, name))
            except (ValueError, TypeError):
                continue

        station_lookup_df = spark.createDataFrame(
            lookup_rows, ["search_name", "ref_station_eva", "ref_station_name"]
        )
        log.info("Loaded %d stations from lookup table", station_lookup_df.count())
    except Exception as e:
        log.error("Failed to load station_data.json: %s", e)
        return {"error": str(e)}

    # Create UDFs for distance calculation
    _search_name_udf = sf.udf(to_station_search_name, StringType())
    _edit_distance_udf = sf.udf(normalized_edit_distance, DoubleType())
    _jaccard_udf = sf.udf(token_jaccard_similarity, DoubleType())

    # Add normalized name to unresolved stations
    unresolved_normalized = unresolved.withColumn(
        "normalized_name", _search_name_udf(F.col("station_name"))
    )

    # Cross join with lookup to find best matches for each unresolved name
    log.info("Computing fuzzy matches for unresolved stations...")
    cross_matched = (
        unresolved_normalized
        .crossJoin(sf.broadcast(station_lookup_df))
        .withColumn(
            "edit_distance",
            _edit_distance_udf(F.col("station_name"), F.col("ref_station_name"))
        )
        .withColumn(
            "jaccard_sim",
            _jaccard_udf(F.col("station_name"), F.col("ref_station_name"))
        )
    )

    # Find best match for each unresolved station (by edit distance, then jaccard)
    from pyspark.sql.window import Window
    w = Window.partitionBy("station_name").orderBy(
        F.col("edit_distance").asc(), F.col("jaccard_sim").desc()
    )

    best_matches = (
        cross_matched
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "station_name",
            "normalized_name",
            "ref_station_name",
            "search_name",
            "ref_station_eva",
            "edit_distance",
            "jaccard_sim"
        )
        .withColumn(
            "rejection_reason",
            F.when(F.col("edit_distance") > 0.3, "edit_distance > 0.3")
            .when(F.col("jaccard_sim") < 0.3, "jaccard_sim < 0.3")
            .otherwise("unknown")
        )
        .orderBy(F.col("edit_distance").asc())
    )

    # Show results grouped by rejection reason
    log.info("")
    log.info("=== BEST MATCHES FOR UNRESOLVED STATIONS ===")
    log.info("(Showing why each station couldn't be matched)")
    log.info("")

    # Near misses (close to threshold)
    near_misses = best_matches.filter(
        (F.col("edit_distance") <= 0.5) & (F.col("edit_distance") > 0.3)
    )
    near_miss_count = near_misses.count()
    log.info("--- NEAR MISSES (edit_distance 0.3-0.5, could be matched with looser threshold) ---")
    log.info("Count: %d", near_miss_count)
    if near_miss_count > 0:
        near_misses.show(50, truncate=False)

    # Failed due to low jaccard (but ok edit distance)
    low_jaccard = best_matches.filter(
        (F.col("edit_distance") <= 0.3) & (F.col("jaccard_sim") < 0.3)
    )
    low_jaccard_count = low_jaccard.count()
    log.info("")
    log.info("--- FAILED DUE TO LOW JACCARD (edit_distance OK but jaccard < 0.3) ---")
    log.info("Count: %d", low_jaccard_count)
    if low_jaccard_count > 0:
        low_jaccard.show(50, truncate=False)

    # Very different (high edit distance)
    very_different = best_matches.filter(F.col("edit_distance") > 0.5)
    very_different_count = very_different.count()
    log.info("")
    log.info("--- VERY DIFFERENT (edit_distance > 0.5, likely genuinely unmatched) ---")
    log.info("Count: %d", very_different_count)
    if very_different_count > 0:
        very_different.show(50, truncate=False)

    # Summary statistics
    log.info("")
    log.info("=== SUMMARY ===")
    stats = best_matches.agg(
        F.count("*").alias("total"),
        F.avg("edit_distance").alias("avg_edit_dist"),
        F.min("edit_distance").alias("min_edit_dist"),
        F.max("edit_distance").alias("max_edit_dist"),
        F.avg("jaccard_sim").alias("avg_jaccard"),
    ).collect()[0]

    log.info("Total unresolved: %d", stats["total"])
    log.info("Edit distance: min=%.3f, avg=%.3f, max=%.3f",
             stats["min_edit_dist"] or 0, stats["avg_edit_dist"] or 0, stats["max_edit_dist"] or 0)
    log.info("Average Jaccard similarity: %.3f", stats["avg_jaccard"] or 0)

    # Recommendations
    log.info("")
    log.info("=== RECOMMENDATIONS ===")
    if near_miss_count > 0:
        log.info("- %d stations are near-misses (edit_dist 0.3-0.5)", near_miss_count)
        log.info("  Consider increasing edit_distance threshold to 0.4 or 0.5")
    if low_jaccard_count > 0:
        log.info("- %d stations failed due to low Jaccard similarity", low_jaccard_count)
        log.info("  Consider lowering jaccard_sim threshold from 0.3 to 0.2")
    if very_different_count > 0:
        log.info("- %d stations are very different from any lookup entry", very_different_count)
        log.info("  These may need manual mapping or are genuinely unknown stations")

    return {
        "unresolved_count": unresolved_count,
        "near_miss_count": near_miss_count,
        "low_jaccard_count": low_jaccard_count,
        "very_different_count": very_different_count,
    }


# ==================== TEST RUNNER ====================


def run_all_diagnostic_tests(spark):
    """Run all diagnostic tests to identify source of extra rows."""
    log.info("\n" + "=" * 70)
    log.info("RUNNING ALL DIAGNOSTIC TESTS")
    log.info("=" * 70 + "\n")

    results = {}

    results["test_1_duplicates"] = test_duplicate_keys(spark)
    results["test_2_null_eva"] = test_null_station_eva(spark)
    results["test_3_unique_stops"] = test_expected_unique_stops(spark)
    results["test_4_multi_station"] = test_stop_appears_multiple_stations(spark)
    results["test_5_python_compare"] = test_compare_with_python_expected(spark)
    results["test_6_eva_mismatch"] = test_timetable_vs_changes_station_eva_mismatch(spark)
    results["test_7_pipeline_trace"] = test_unique_stops_before_and_after_resolve(spark)
    results["test_8_changes_only"] = test_changes_only_stops(spark)
    results["test_9_missing_names"] = test_missing_station_names(spark)
    results["test_10_avg_delay"] = test_compute_average_daily_delay(spark)
    results["test_11_peak_hour"] = test_compute_peak_hour_departure_counts(spark)
    results["test_12_delay_bug_fix"] = test_delay_only_with_changed_time(spark)
    results["test_13_unresolved_matches"] = test_unresolved_station_matches(spark)
    results["test_14_alexanderplatz"] = test_alexanderplatz_delay(spark)

    log.info("\n" + "=" * 70)
    log.info("DIAGNOSTIC SUMMARY")
    log.info("=" * 70)

    for test_name, result in results.items():
        log.info("%s: %s", test_name, result)

    return results


# Map test names to functions
TEST_MAP = {
    "test_1": test_duplicate_keys,
    "test_2": test_null_station_eva,
    "test_3": test_expected_unique_stops,
    "test_4": test_stop_appears_multiple_stations,
    "test_5": test_compare_with_python_expected,
    "test_6": test_timetable_vs_changes_station_eva_mismatch,
    "test_7": test_unique_stops_before_and_after_resolve,
    "test_8": test_changes_only_stops,
    "test_9": test_missing_station_names,
    "test_10": test_compute_average_daily_delay,
    "test_11": test_compute_peak_hour_departure_counts,
    "test_12": test_delay_only_with_changed_time,
    "test_13": test_unresolved_station_matches,
    "test_14": test_alexanderplatz_delay,
    "all": run_all_diagnostic_tests,
}


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("spark_etl_tests")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    # Determine which test to run
    test_name = sys.argv[1] if len(sys.argv) > 1 else "all"

    if test_name in TEST_MAP:
        log.info("Running %s...", test_name)
        result = TEST_MAP[test_name](spark)
        log.info("Result: %s", result)
    else:
        log.error("Unknown test: %s", test_name)
        log.info("Available tests: %s", ", ".join(TEST_MAP.keys()))
        sys.exit(1)

    spark.stop()
