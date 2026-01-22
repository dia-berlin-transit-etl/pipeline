from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from typing import Dict, List, Optional


def cast_timestamp(df, cols, fmt="yyMMddHHmm"):
        for col_name in cols:
            df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), fmt))
        return df

def parse_snaphot(df, col_name):
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
    return [item.name for item in df.schema.fields if item.name not in keys]
     
def main():
    spark = SparkSession \
        .builder \
        .appName("Berlin Public Transport ETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    timetableSchema = StructType([
        StructField("id", StringType(), False),
        StructField("tl", StructType([
            StructField("n", IntegerType(), False),
            StructField("o", StringType(), False)
        ]), True),

        StructField("ar", StructType([
            StructField("pt", StringType(), False)
        ]), True),

        StructField("dp", StructType([
            StructField("pt", StringType(), False)
        ]), True),
    ])

    changesSchema = StructType([
        StructField("id", StringType(), False),
        StructField("eva", IntegerType(), False),
        StructField("tl", StructType([
            StructField("n", IntegerType(), False),
            StructField("o", StringType(), False)
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
        "tl.o":"train_owner",
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

    path_to_timetables = "/opt/spark-data/timetables/**/*.xml"
    path_to_timetable_changes = "/opt/spark-data/timetable_changes/**/**/*.xml"
    simple_path_timetable = "/opt/spark-data/timetables/2510141200/k_llnische_heide_timetable.xml"
    simple_path_to_timetable_changes = '/opt/spark-data/timetable_changes/251014_251021/2510141200/k_llnische_heide_change.xml'

    timetableDF = extract_data_from_xml(spark, path_to_timetables, timetableSchema)
    timetableDF = flatten_df(timetableDF, flatten_mapping_planned_fields)
    timetableDF = transform_and_cast_df(timetableDF, ["arr_pt_raw", "dep_pt_raw", "snapshot_ts"]).withColumnsRenamed({"arr_pt_raw":"arr_pt", "dep_pt_raw": "dep_pt"})

    timetableChangesDF = extract_data_from_xml(spark, path_to_timetable_changes, changesSchema)
    timetableChangesDF = flatten_df(timetableChangesDF, {**flatten_mapping_changes_fields, **flatten_mapping_planned_fields})
    timetableChangesDF = transform_and_cast_df(timetableChangesDF, ["arr_ct_raw", "dep_ct_raw", "snapshot_ts","arr_pt_raw","dep_pt_raw","arr_clt_raw", "dep_clt_raw"], ['dep_hi_raw', 'arr_hi_raw']) \
                                .withColumnsRenamed({"arr_pt_raw":"arr_pt", "dep_pt_raw": "dep_pt", "arr_ct_raw": "arr_ct", "dep_ct_raw": "dep_ct", 'dep_hi_raw': 'dep_hi', 'arr_hi_raw': 'arr_hi'})

    keys = ['stop_id', 'station_eva']
    leftCols = ['arr_ps_raw','arr_ct','arr_cs_raw','arr_clt_raw','arr_hi','dep_ps_raw','dep_ct','dep_cs_raw','dep_clt_raw','dep_hi','train_number','train_owner','arr_pt','dep_pt','station','snapshot_ts']
    timetableChangesDF = get_last_values_for_stop(timetableChangesDF, "snapshot_ts", keys, leftCols)

    timetableChangesDF = timetableChangesDF.transform(process_event_status, 'dep_ps_raw', 'dep_cs_raw', 'dep_clt_raw', 'dep_pt', 'dep') \
                            .transform(process_event_status, 'arr_ps_raw', 'arr_cs_raw', 'arr_clt_raw', 'arr_pt', 'arr')
    
    timetableDF_alligned = timetableDF.withColumns({"station_eva": sf.lit(None), "arr_ct": sf.lit(None), "arr_hi": sf.lit(False), "arr_added": sf.lit(False), "arr_cancelled": sf.lit(False), "dep_ct": sf.lit(None), "dep_hi": sf.lit(False), "dep_added": sf.lit(False), "dep_cancelled": sf.lit(False),})
    timetableChangesDF.printSchema()
    c = timetableChangesDF.alias('c')
    p = timetableDF_alligned.alias('p')

    movementDf = c.join(p, on=['stop_id', 'station'], how="outer").select(
           sf.coalesce(sf.col("c.stop_id"), sf.col("p.stop_id")).alias("stop_id"),
           sf.coalesce(sf.col("c.station"), sf.col("p.station")).alias("station"),
           sf.coalesce(sf.col("p.arr_pt"), sf.col("c.arr_pt")).alias("arr_pt"),
           sf.coalesce(sf.col("p.dep_pt"), sf.col("c.dep_pt")).alias("dep_pt"),
           sf.coalesce(sf.col("p.train_number"), sf.col("c.train_number")).alias("train_number"),
           sf.coalesce(sf.col("p.train_owner"), sf.col("c.train_owner")).alias("train_owner"),
           *[sf.coalesce(sf.col(f'p.{col}'), sf.col(f'c.{col}')).alias(f'{col}') for col in ['station_eva', 'arr_ct', 'arr_hi', 'arr_added', 'arr_cancelled', 'dep_ct', 'dep_hi', 'dep_added', 'dep_cancelled', 'snapshot_ts']]
    ) 
    movementDf = movementDf.dropDuplicates()
    movementDf = movementDf.withColumns({"arr_delay_min": sf.when(sf.col("arr_ct").isNotNull() & sf.col("arr_pt").isNotNull(), sf.floor((sf.unix_timestamp("arr_ct") - sf.unix_timestamp("arr_pt")) / 60)),\
                                         "dep_delay_min": sf.when(sf.col("dep_ct").isNotNull() & sf.col("dep_pt").isNotNull(), sf.floor((sf.unix_timestamp("dep_ct") - sf.unix_timestamp("dep_pt")) / 60))})
    
    movementDf = movementDf.withColumn("snapshot_date", sf.to_date("snapshot_ts"))
    movementDf.printSchema()
    path_to_parquet = '/opt/spark-data/movements'
    simple_path_to_parquet = './movements'
    movementDf.write.mode("overwrite").partitionBy("snapshot_date").parquet(path_to_parquet)

    spark.stop()

if __name__ == "__main__":
    main()
