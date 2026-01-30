# sparketl.py
# Fast(er) Spark ETL:
# - reads ALL timetables + changes in one go (no per-snapshot loop)
# - uses spark-xml (requires --packages com.databricks:spark-xml_2.12:0.18.0)
# - writes one partitioned output (overwrite) -> idempotent

import os
import re
from typing import Dict, List, Optional, Union

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    LongType,
)
from pyspark.sql import functions as sf
from pyspark.sql.window import Window


# ---------- helpers ----------

_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")  # YYMMDDHHmm


def cast_timestamp(df, cols, fmt="yyMMddHHmm"):
    for col_name in cols:
        df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), fmt))
    return df


def parse_snapshot(df, col_name: str):
    # matches: ".../<snapshot>/<station>_....xml"
    reg = r"\/(\d{10})\/([-\w]+)_"
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
        df = df.withColumn(
            col_name, sf.when(sf.col(col_name) == 1, sf.lit(True)).otherwise(sf.lit(False))
        )
    return df


def normalize_station(df):
    s = sf.lower(sf.trim(sf.col("station")))

    # German folding
    s = sf.translate(s, "ßäöü", "saou")

    # remove '_' between word chars
    s = sf.regexp_replace(s, r"(?<=\w)_(?=\w)", "")

    # hbf / bf / str rules
    s = sf.regexp_replace(s, r"\bhbf\b\.?", " hauptbahnhof ")
    s = sf.regexp_replace(s, r"(?<=\w)hbf\b\.?", "hauptbahnhof")

    s = sf.regexp_replace(s, r"\bbf\b\.?", " bahnhof ")
    s = sf.regexp_replace(s, r"(?<=\w)(?<!h)bf\b\.?", "bahnhof")

    s = sf.regexp_replace(s, r"\bstr\b\.?", " strase ")
    s = sf.regexp_replace(s, r"(?<=\w)str\b\.?", "strase")
    s = sf.regexp_replace(s, r"\b(\w+)\s+strase\b", r"\1strase")

    # drop "berlin"
    s = sf.regexp_replace(s, r"\bberlin\b", " ")

    # strip everything else
    s = sf.regexp_replace(s, r"[^a-z0-9\s]", " ")
    s = sf.regexp_replace(s, r"\s+", " ")
    s = sf.trim(s)

    return df.withColumn("station", s)


def extract_data_from_xml(
    spark: SparkSession,
    paths: Union[str, List[str]],
    schema: StructType,
):
    """
    paths:
      - a single glob string (recommended), e.g. "file:///opt/.../*/*/*.xml"
      - or a list of paths/globs
    """
    reader = (
        spark.read.format("xml")
        .option("rootTag", "timetable")
        .option("rowTag", "s")
        .option("attributePrefix", "")
        .schema(schema)
    )

    if isinstance(paths, list):
        df = reader.load(paths)
    else:
        df = reader.load(paths)

    return df.withColumn("fileName", sf.input_file_name())


def transform_and_cast_df(df, cols_with_timestamp, cols_boolean=None):
    df = (
        df.transform(parse_snapshot, "fileName")
        .transform(cast_timestamp, cols_with_timestamp)
        .transform(normalize_station)
    )
    if cols_boolean is not None:
        df = df.transform(cast_boolean_for_hidden, cols_boolean)
    return df


def process_event_status(df, p_status_col_name, c_status_col_name, clt_name, pt_name, prefix):
    added_key = prefix + "_added"
    cancelled_key = prefix + "_cancelled"

    is_added = (
        ((sf.col(c_status_col_name) == sf.lit("a")) | (sf.col(p_status_col_name) == sf.lit("a")))
        & sf.col(pt_name).isNotNull()
    )
    is_cancelled = (sf.col(c_status_col_name) == sf.lit("c")) & sf.col(clt_name).isNotNull()

    return df.withColumns({added_key: is_added, cancelled_key: is_cancelled}).drop(
        *[p_status_col_name, c_status_col_name, clt_name]
    )


def flatten_df(df, mapping: Dict[str, str]):
    return df.select(*[sf.col(old).alias(new) for old, new in mapping.items()])


def get_non_key_cols(df, keys):
    return [c for c in df.columns if c not in keys]


def get_last_values_for_stop(df, order_col, keys, cols=None):
    if cols is None:
        cols = get_non_key_cols(df, keys)

    window_all = (
        Window.partitionBy(*keys)
        .orderBy(sf.col(order_col).asc_nulls_first())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    w_last_row = Window.partitionBy(*keys).orderBy(sf.col(order_col).desc_nulls_last())

    for col in cols:
        df = df.withColumn(col, sf.last(col, ignorenulls=True).over(window_all))

    return df.withColumn("_rn", sf.row_number().over(w_last_row)).filter(sf.col("_rn") == 1).drop("_rn")


# ---------- main ----------

def main():
    # IMPORTANT: match the "golden" example’s directory depth
    timetables_glob = os.getenv("TIMETABLES_GLOB", "file:///opt/spark-data/timetables/*/*/*.xml")
    changes_glob = os.getenv("CHANGES_GLOB", "file:///opt/spark-data/timetable_changes/*/*/*.xml")

    out_path = os.getenv("OUT_PATH", "file:///opt/spark-data/movements/output_last_state")

    spark = SparkSession.builder.appName("Berlin Public Transport ETL").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

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
            StructField("eva", LongType(), True),  # may be missing/null
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
        "eva": "station_eva",
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

    df_list = []

    # --- planned ---
    print("Processing all PLANNED data...")
    timetableDF = extract_data_from_xml(spark, timetables_glob, timetableSchema)
    timetableDF = flatten_df(timetableDF, flatten_mapping_planned_fields)
    timetableDF = transform_and_cast_df(timetableDF, ["arr_pt_raw", "dep_pt_raw", "snapshot_ts"])
    timetableDF = timetableDF.withColumnsRenamed({"arr_pt_raw": "arr_pt", "dep_pt_raw": "dep_pt"})

    timetableDF = timetableDF.withColumns(
        {
            "station_eva": sf.lit(None).cast(LongType()),
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
    df_list.append(timetableDF)

    # --- changes ---
    print("Processing all CHANGES data...")
    timetableChangesDF = extract_data_from_xml(spark, changes_glob, changesSchema)
    timetableChangesDF = flatten_df(
        timetableChangesDF, {**flatten_mapping_changes_fields, **flatten_mapping_planned_fields}
    )

    timetableChangesDF = (
        transform_and_cast_df(
            timetableChangesDF,
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
        .transform(process_event_status, "dep_ps_raw", "dep_cs_raw", "dep_clt_raw", "dep_pt", "dep")
        .transform(process_event_status, "arr_ps_raw", "arr_cs_raw", "arr_clt_raw", "arr_pt", "arr")
    )

    df_list.append(timetableChangesDF)

    # --- union + last state ---
    df = df_list[0].unionByName(df_list[1], allowMissingColumns=True)

    print("Calculating LAST STATE (Deduplication)...")
    keys = ["stop_id", "station"]
    order = "snapshot_ts"
    cols = get_non_key_cols(df, keys)
    movementDf = get_last_values_for_stop(df, order, keys, cols)

    print("Calculating delays...")
    movementDf = movementDf.withColumns(
        {
            "arr_delay_min": sf.when(sf.col("arr_pt").isNull(), None)
            .when(
                sf.col("arr_ct").isNotNull(),
                sf.floor((sf.unix_timestamp("arr_ct") - sf.unix_timestamp("arr_pt")) / 60),
            )
            .otherwise(0),
            "dep_delay_min": sf.when(sf.col("dep_pt").isNull(), None)
            .when(
                sf.col("dep_ct").isNotNull(),
                sf.floor((sf.unix_timestamp("dep_ct") - sf.unix_timestamp("dep_pt")) / 60),
            )
            .otherwise(0),
        }
    )

    print("Writing to Parquet (idempotent overwrite)...")
    movementDf = movementDf.withColumn("snapshot_date", sf.to_date("snapshot_ts"))

    (
        movementDf.repartition(8, "snapshot_date")
        .write.mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(out_path)
    )

    movementDf.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
