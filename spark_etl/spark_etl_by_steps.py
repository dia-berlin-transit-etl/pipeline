import glob
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from typing import Dict, Iterator

# ---------- snapshot discovery ----------
# SAME FUNCTION as in fact_planned, fact_changes -> need to create utils for re-used stuff


_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")  # YYMMDDHHmm

def iter_timetable_snapshots(timetables_root: str = "timetables") -> Iterator[str]:
    for p in glob.glob(os.path.join(timetables_root, "**", "[0-9]" * 10), recursive=True):
        if os.path.isdir(p):
            key = os.path.basename(p)
            pattern = os.path.join(p, "*.xml")
            has_xml_files = bool(glob.glob(pattern, recursive=True))
            if _SNAPSHOT_KEY_RE.match(key) and has_xml_files:
                yield key


def timetables_glob_for_snapshot(snapshot_key: str, timetables_root: str = "timetables") -> str:
    return os.path.join(timetables_root, "**", snapshot_key, "*.xml")

# ---------- snapshot discovery ----------


def cast_timestamp(df, cols, fmt="yyMMddHHmm"):
        for col_name in cols:
            df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), fmt))
        return df

def parse_snaphot(df, col_name: str):
    reg = r'\/(\d+)\/([-\w]+)_'

    return df.withColumns({"station": sf.regexp_extract(sf.col(col_name), reg, 2), 'snapshot_ts': sf.regexp_extract(sf.col(col_name), reg, 1)}).drop(col_name)

def cast_boolean_for_hidden(df, cols):
        for col_name in cols:
            df = df.withColumn(col_name, sf.when(sf.col(col_name) == 1, sf.lit(True)).otherwise(sf.lit(False)))
        return df

def extract_data_from_xml(spark, file_path, schema):
    df = spark.read.format("xml")\
        .option("rootTag", "timetable") \
        .option("rowTag", "s")\
        .option("attributePrefix", "") \
        .schema(schema) \
        .load(file_path) \
        .withColumn("fileName", sf.input_file_name())
    
    df = df.coalesce(48)
    return df

def transform_and_cast_df(df, cols_with_timestamp, cols_boolean=None):
     df = df.transform(parse_snaphot, 'fileName').transform(cast_timestamp, cols_with_timestamp)
     if (cols_boolean is not None):
        df = df.transform(cast_boolean_for_hidden, cols_boolean)
     return df

def process_event_status(df, p_status_col_name, c_status_col_name, clt_name, pt_name, prefix):
        addedKey = prefix + '_added'
        cancelledKey = prefix + '_cancelled'
        is_added = ((sf.col(c_status_col_name) == sf.lit("a")) | (sf.col(p_status_col_name) == sf.lit("a"))) & sf.col(pt_name).isNotNull()
        is_cancelled = ((sf.col(c_status_col_name) == sf.lit("c")) & sf.col(clt_name).isNotNull())
        return df.withColumns({
                            addedKey: is_added,
                            cancelledKey: is_cancelled}).drop(*[p_status_col_name, c_status_col_name, clt_name])

def flatten_df(df, dict_to_map: Dict[str, str]):
     df = df.select(*[sf.col(old).alias(new) for old, new in dict_to_map.items()])
     return df

def get_last_values_for_stop(df, orderCol, keys, cols=None):
    if (cols is None):
        cols = get_non_key_cols(df, keys)
    window = Window.partitionBy(*keys).orderBy(sf.col(orderCol).asc_nulls_first()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    w_last = Window.partitionBy(*keys).orderBy(sf.col(orderCol).desc_nulls_last())
    
    for col in cols:
        df = df.withColumn(col, sf.last(col, ignorenulls=True).over(window))
    
    result = df.withColumn("row", sf.row_number().over(w_last)).filter(sf.col("row") == 1).drop("row")
    return result

def get_non_key_cols(df, keys):
    return [c for c in df.columns if c not in keys]

def basename(path: str) -> str:
    return path.rstrip("/").split("/")[-1]

def main():
    timetables_root = "/opt/spark-data/timetables"
    changes_root = "/opt/spark-data/timetable_changes"  

    snapshot_keys_planned = sorted(set(iter_timetable_snapshots(timetables_root)))
    snapshot_keys_changes = sorted(set(iter_timetable_snapshots(changes_root)))
    all_snapshot_keys = sorted(set(snapshot_keys_planned + snapshot_keys_changes))

    print(f"Amount of keys planned: {len(snapshot_keys_planned)}")
    print(f"Amount of keys changes: {len(snapshot_keys_changes)}")
    print(f"Amount of keys together: {len(all_snapshot_keys)}")

    out_all_logs = '/opt/spark-data/logs'

    timetableSchema = StructType([
    StructField("id", StringType(), False),
    StructField("tl", StructType([
        StructField("n", IntegerType(), False),
        StructField("c", StringType(), False)
    ]), True),

    StructField("ar", StructType([
        StructField("pt", StringType(), False)
    ]), True),

    StructField("dp", StructType([
        StructField("pt", StringType(), False)
    ]), True),])

    changesSchema = StructType([
        StructField("id", StringType(), False),
        StructField("eva", IntegerType(), False),
        StructField("tl", StructType([
            StructField("n", IntegerType(), False),
            StructField("c", StringType(), False)
        ]), True),

        StructField("ar", StructType([
            StructField("pt", StringType(), True),
            StructField("ps", StringType(), True),
            StructField("ct", StringType(), True),
            StructField("cs", StringType(), True),
            StructField("hi", IntegerType(), True),
            StructField("clt", StringType(), True),
        ]), True),

        StructField("dp", StructType([
            StructField("pt", StringType(), True),
            StructField("ps", StringType(), True),
            StructField("ct", StringType(), True),
            StructField("cs", StringType(), True),
            StructField("hi", IntegerType(), True),
            StructField("clt", StringType(), True),
        ]), True),
    ])

    flatten_mapping_planned_fields = {
        "id": 'stop_id',
        "tl.n": "train_number",
        "tl.c":"train_category",
        "ar.pt":"arr_pt_raw",
        "dp.pt":"dep_pt_raw",
        "fileName": "fileName"
    }

    flatten_mapping_changes_fields = {
        "eva": "station_eva",
        "ar.ps":"arr_ps_raw",
        "ar.ct":"arr_ct_raw",
        "ar.cs":"arr_cs_raw",
        "ar.clt":"arr_clt_raw",
        "ar.hi":"arr_hi_raw",
        "dp.ps":"dep_ps_raw",
        "dp.ct":"dep_ct_raw",
        "dp.cs":"dep_cs_raw",
        "dp.clt":"dep_clt_raw",
        "dp.hi":"dep_hi_raw",
    }

    spark = (SparkSession.builder.appName("Berlin Public Transport ETL").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # snapshot_keys_planned = ['2509021200']
    # snapshot_keys_changes = ['2509021600', '2509230430']
    # all_snapshot_keys = sorted(set(snapshot_keys_planned + snapshot_keys_changes))

    for key in all_snapshot_keys:
        print(f"[ETL] snapshot={key}")

        def write_into_parquet(df):
            df = df.withColumn("snapshot_date", sf.to_date("snapshot_ts"))

            (df.repartition(4, "snapshot_date")
            .write.mode("append")
            .partitionBy("snapshot_date")
            .parquet(out_all_logs))

        if (key in snapshot_keys_planned):
            path_to_timetable = f"{timetables_root}/{key}/*.xml"
            timetableDF = extract_data_from_xml(spark, path_to_timetable, timetableSchema)
            timetableDF = flatten_df(timetableDF, flatten_mapping_planned_fields)
            timetableDF = transform_and_cast_df(timetableDF, ["arr_pt_raw", "dep_pt_raw", "snapshot_ts"]).withColumnsRenamed({"arr_pt_raw":"arr_pt", "dep_pt_raw": "dep_pt"})
            timetableDF = timetableDF.withColumns({"station_eva": sf.lit(None).cast(IntegerType()), "arr_ct": sf.lit(None).cast(TimestampType()), "arr_hi": sf.lit(False), "arr_added": sf.lit(False), "arr_cancelled": sf.lit(False), "dep_ct": sf.lit(None).cast(TimestampType()), "dep_hi": sf.lit(False), "dep_added": sf.lit(False), "dep_cancelled": sf.lit(False),})
            write_into_parquet(timetableDF)

        if (key in snapshot_keys_changes):
            path_to_timetable_changes = f"{changes_root}/**/{key}/*.xml"
            timetableChangesDF = extract_data_from_xml(spark, path_to_timetable_changes, changesSchema)
            timetableChangesDF = flatten_df(timetableChangesDF, {**flatten_mapping_changes_fields, **flatten_mapping_planned_fields})
            timetableChangesDF = transform_and_cast_df(timetableChangesDF, ["arr_ct_raw", "dep_ct_raw", "snapshot_ts","arr_pt_raw","dep_pt_raw","arr_clt_raw", "dep_clt_raw"], ['dep_hi_raw', 'arr_hi_raw']) \
                                .withColumnsRenamed({"arr_pt_raw":"arr_pt", "dep_pt_raw": "dep_pt", "arr_ct_raw": "arr_ct", "dep_ct_raw": "dep_ct", 'dep_hi_raw': 'dep_hi', 'arr_hi_raw': 'arr_hi'})
            timetableChangesDF = timetableChangesDF.transform(process_event_status, 'dep_ps_raw', 'dep_cs_raw', 'dep_clt_raw', 'dep_pt', 'dep') \
                                .transform(process_event_status, 'arr_ps_raw', 'arr_cs_raw', 'arr_clt_raw', 'arr_pt', 'arr')
            write_into_parquet(timetableChangesDF)

    events = spark.read.parquet(out_all_logs)

    #keys = ['stop_id', 'station_eva']
    keys = ['stop_id', 'station']
    order = 'snapshot_ts'
    leftCols = [c for c in events.columns if c not in keys + ["snapshot_date"]]
    movementDf = get_last_values_for_stop(events, order, keys, leftCols)
    movementDf = movementDf.withColumns({
        "arr_delay_min": sf.when(sf.col("arr_pt").isNull(), None).when(sf.col("arr_ct").isNotNull(), sf.floor((sf.unix_timestamp("arr_ct") - sf.unix_timestamp("arr_pt")) / 60)).otherwise(0),\
        "dep_delay_min": sf.when(sf.col("dep_pt").isNull(), None).when(sf.col("dep_ct").isNotNull(), sf.floor((sf.unix_timestamp("dep_ct") - sf.unix_timestamp("dep_pt")) / 60)).otherwise(0)
        })

    movementDf.printSchema()
    path_to_parquet = "/opt/spark-data/movements"

    movementDf = movementDf.repartition(8, "snapshot_date")
    movementDf.write.mode("overwrite").partitionBy("snapshot_date").parquet(path_to_parquet)

    spark.stop()

if __name__ == "__main__":
    main()
