import glob
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from typing import Dict, List

# 
# ---------- snapshot discovery ----------
_SNAPSHOT_KEY_RE = re.compile(r"^\d{10}$")  # YYMMDDHHmm

def get_valid_snapshot_paths(root_path: str) -> List[str]:
    """
    Scan directories and returns a list of paths to XML files for valid snapshots.
    """
    valid_paths = []

    for p in glob.glob(os.path.join(root_path, "**", "[0-9]" * 10), recursive=True):
        if os.path.isdir(p):
            key = os.path.basename(p)
            if _SNAPSHOT_KEY_RE.match(key):
                xml_pattern = os.path.join(p, "*.xml")
                if any(glob.iglob(xml_pattern)): 
                    valid_paths.append(xml_pattern)
    return valid_paths

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


def normalize_station(df):
    str = sf.col("station")

    s = sf.lower(sf.trim(str))

    # German folding (more robust for fuzzy matching / filenames)
    s = sf.translate(s, "ßäöü", "saou")

    # If filenames use '_' as a placeholder inside words (e.g. s_d for süd),
    # don't turn it into a space — remove it when it's between word chars.
    s = sf.regexp_replace(s, r"(?<=\w)_(?=\w)", "")

    # hbf (word + suffix)
    s = sf.regexp_replace(s, r"\bhbf\b\.?", " hauptbahnhof ")
    s = sf.regexp_replace(s, r"(?<=\w)hbf\b\.?", "hauptbahnhof")

    # bf (word + suffix) — suffix excludes "...hbf"
    s = sf.regexp_replace(s, r"\bbf\b\.?", " bahnhof ")
    s = sf.regexp_replace(s, r"(?<=\w)(?<!h)bf\b\.?", "bahnhof")

    # str (word + suffix)
    s = sf.regexp_replace(s, r"\bstr\b\.?", " strase ")
    s = sf.regexp_replace(s, r"(?<=\w)str\b\.?", "strase")
    s = sf.regexp_replace(s, r"\b(\w+)\s+strase\b", r"\1strase")

    s = sf.regexp_replace(s, r"\bberlin\b", " ")

    # Keep underscore already handled; now strip everything else to spaces
    s = sf.regexp_replace(s, r"[^a-z0-9\s]", " ")
    s = sf.regexp_replace(s, r"\s+", " ")
    s = sf.trim(s)

    return df.withColumn('station', s).drop('station_file_name')


def extract_data_from_xml1(spark, file_paths: List[str], schema):
    if len(file_paths) == 0:
        return None
    
    df = spark.read.format("xml")\
        .option("rootTag", "timetable") \
        .option("rowTag", "s")\
        .option("attributePrefix", "") \
        .schema(schema) \
        .load(",".join(file_paths)) \
        .withColumn("fileName", sf.input_file_name())
    
    return df

def extract_data_from_xml(spark, file_path, schema):

    df = spark.read.format("xml")\
        .option("rootTag", "timetable") \
        .option("rowTag", "s")\
        .option("attributePrefix", "") \
        .schema(schema) \
        .load(file_path) \
        .withColumn("fileName", sf.input_file_name())
    
    return df

def transform_and_cast_df(df, cols_with_timestamp, cols_boolean=None):
     df = df.transform(parse_snaphot, 'fileName').transform(cast_timestamp, cols_with_timestamp).transform(normalize_station)
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

def main():
    timetables_root = "/opt/spark-data/timetables"
    changes_root = "/opt/spark-data/timetable_changes"  
    path_to_parquet = "/opt/spark-data/output"

    #planned_paths = get_valid_snapshot_paths(timetables_root)
    #changes_paths = get_valid_snapshot_paths(changes_root)

    planned_paths = '/opt/spark-data/timetables/2509021600/alexanderplatz_timetable.xml' #"/opt/spark-data/timetables/*/*.xml"
    changes_paths = '/opt/spark-data/timetable_changes/250902_250909/2509021600/ahrensfelde_change.xml', '/opt/spark-data/timetable_changes/250902_250909/2509021600/albrechtshof_change.xml', '/opt/spark-data/timetable_changes/250902_250909/2509021600/alexanderplatz_change.xml'#"/opt/spark-data/timetable_changes/*/*/*.xml"  
    
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
    spark.sparkContext.setLogLevel("INFO")

    df_list = []
    
    if planned_paths:
        print("Processing all PLANNED data...")
        timetableDF = extract_data_from_xml(spark, planned_paths, timetableSchema)
        timetableDF = flatten_df(timetableDF, flatten_mapping_planned_fields)
        timetableDF = transform_and_cast_df(timetableDF, ["arr_pt_raw", "dep_pt_raw", "snapshot_ts"])
        timetableDF = timetableDF.withColumnsRenamed({"arr_pt_raw":"arr_pt", "dep_pt_raw": "dep_pt"})
        timetableDF = timetableDF.withColumns({
            "station_eva": sf.lit(None).cast(IntegerType()),
            "arr_ct": sf.lit(None).cast(TimestampType()), 
            "arr_hi": sf.lit(False), 
            "arr_added": sf.lit(False), 
            "arr_cancelled": sf.lit(False), 
            "dep_ct": sf.lit(None).cast(TimestampType()), 
            "dep_hi": sf.lit(False), 
            "dep_added": sf.lit(False), 
            "dep_cancelled": sf.lit(False),
            })
        
        df_list.append(timetableDF)

    if changes_paths:
        print("Processing all CHANGES data...")
        timetableChangesDF = extract_data_from_xml(spark, changes_paths, changesSchema)
        timetableChangesDF = flatten_df(timetableChangesDF, {**flatten_mapping_changes_fields, **flatten_mapping_planned_fields})
        timetableChangesDF = transform_and_cast_df(timetableChangesDF, ["arr_ct_raw", "dep_ct_raw", "snapshot_ts","arr_pt_raw","dep_pt_raw","arr_clt_raw", "dep_clt_raw"], ['dep_hi_raw', 'arr_hi_raw']) \
                            .withColumnsRenamed({"arr_pt_raw":"arr_pt", "dep_pt_raw": "dep_pt", "arr_ct_raw": "arr_ct", "dep_ct_raw": "dep_ct", 'dep_hi_raw': 'dep_hi', 'arr_hi_raw': 'arr_hi'})
        timetableChangesDF = timetableChangesDF.transform(process_event_status, 'dep_ps_raw', 'dep_cs_raw', 'dep_clt_raw', 'dep_pt', 'dep') \
                            .transform(process_event_status, 'arr_ps_raw', 'arr_cs_raw', 'arr_clt_raw', 'arr_pt', 'arr')
        df_list.append(timetableChangesDF)

    if df_list:
        if len(df_list) > 1:
            df = df_list[0].unionByName(df_list[1], allowMissingColumns=True)
        else:
            df = df_list[0]
        
        print("Calculating LAST STATE (Deduplication)...")
        
        keys = ['stop_id', 'station']
        order = 'snapshot_ts'
        leftCols = [c for c in df.columns if c not in keys + ["snapshot_date"]]
        
        movementDf = get_last_values_for_stop(df, order, keys, leftCols)

        print("Calculating delays...")
        movementDf = movementDf.withColumns({
            "arr_delay_min": sf.when(sf.col("arr_pt").isNull(), None)
                               .when(sf.col("arr_ct").isNotNull(), sf.floor((sf.unix_timestamp("arr_ct") - sf.unix_timestamp("arr_pt")) / 60))
                               .otherwise(0),
            "dep_delay_min": sf.when(sf.col("dep_pt").isNull(), None)
                               .when(sf.col("dep_ct").isNotNull(), sf.floor((sf.unix_timestamp("dep_ct") - sf.unix_timestamp("dep_pt")) / 60))
                               .otherwise(0)
        })

        print("Writing to Parquet...")
        movementDf = movementDf.withColumn("snapshot_date", sf.to_date("snapshot_ts"))
        movementDf.coalesce(4).write.mode("overwrite").partitionBy("snapshot_date").parquet(path_to_parquet)

        movementDf.printSchema()

    spark.stop()

if __name__ == "__main__":
    main()
