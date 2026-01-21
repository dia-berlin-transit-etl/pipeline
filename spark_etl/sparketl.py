from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import functions as sf

spark = SparkSession \
    .builder \
    .appName("Berlin Public Transport ETL") \
    .getOrCreate()

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


def cast_timestamp(df, cols, fmt="yyMMddHHmm"):
    for col_name in cols:
        df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), fmt))
    return df

def parse_snaphot(df, col_name, suffix):
    reg = r'\/(\d+)\/([-\w]+)_'

    return df.withColumns({
    "station":
    sf.regexp_extract(sf.col(col_name), reg, 2), 'snapshot_ts': sf.regexp_extract(sf.col(col_name), reg, 1)}
).drop(col_name)

#.load("../timetables/**/*.xml")
# .load("../timetables/2509021200/alexanderplatz_timetable.xml") \
timetableDf = spark.read.format("xml")\
	.option("rootTag", "timetable") \
	.option("rowTag", "s")\
	.option("attributePrefix", "") \
	.schema(timetableSchema) \
	.load("../timetables/2510141200/k_llnische_heide_timetable.xml") \
    .withColumn("fileName", sf.input_file_name())

#timetableDf.show()

timetableDf_flat = timetableDf.select(
    sf.col("id").alias('stop_id'),
    sf.col("tl.n").alias("train_number"),
    sf.col("tl.o").alias("train_owner"),
    sf.col("ar.pt").alias("arr_pt_raw"),
    sf.col("dp.pt").alias("dep_pt_raw"),
    sf.col("fileName")
)

tDf = timetableDf_flat.transform(parse_snaphot, 'fileName', 'timetable').transform(cast_timestamp, ["arr_pt_raw", "dep_pt_raw", "snapshot_ts"])
#tDf.show()

def cast_boolean_for_hidden(df, col_name):
    return df.withColumn(col_name, sf.when(sf.col(col_name) == 1, sf.lit(True)).otherwise(sf.lit(False)))

def process_event_status(df, status_col_name, prefix):
    plannedKey = 'is_' + prefix + '_planned'
    addedKey = 'is_' + prefix + '_added'
    cancelledKey = 'is_' + prefix + '_cancelled'
    return df.withColumns({plannedKey: sf.when(sf.col(status_col_name) == 'p', sf.lit(True)).otherwise(sf.lit(False)),
                           addedKey: sf.when(sf.col(status_col_name) == 'a', sf.lit(True)).otherwise(sf.lit(False)),
                           cancelledKey: sf.when(sf.col(status_col_name) == 'c', sf.lit(True)).otherwise(sf.lit(False))}).drop(status_col_name)


#.load("../timetable_changes/**/**/*.xml") 
# .load("../timetable_changes/250902_250909/2509021600/alexanderplatz_change.xml") \ timetables\2510141200\k_llnische_heide_timetable.xml
changesDf = spark.read.format("xml")\
	.option("rootTag", "timetable") \
	.option("rowTag", "s")\
	.option("attributePrefix", "") \
	.schema(changesSchema) \
    .load('../timetable_changes/251014_251021/2510141200/k_llnische_heide_change.xml') \
    .withColumn("fileName", sf.input_file_name())

changesDf_flat = changesDf.select(
    sf.col("id").alias('stop_id'),
    sf.col("eva").alias("station_eva"),
    sf.col("tl.n").alias("train_number"),
    sf.col("tl.o").alias("train_owner"),
    sf.col("ar.pt").alias("arr_pt_raw"),
    sf.col("ar.ps").alias("arr_ps_raw"),
    sf.col("ar.ct").alias("arr_ct_raw"),
    sf.col("ar.cs").alias("arr_cs_raw"),
    sf.col("ar.clt").alias("arr_clt_raw"),
    sf.col("ar.hi").alias("arr_hi_raw"),
    sf.col("dp.pt").alias("dep_pt_raw"),
    sf.col("dp.ps").alias("dep_ps_raw"),
    sf.col("dp.ct").alias("dep_ct_raw"),
    sf.col("dp.cs").alias("dep_cs_raw"),
    sf.col("dp.clt").alias("dep_clt_raw"),
    sf.col("dp.hi").alias("dep_hi_raw"),
    sf.col("fileName")
)

cDf = changesDf_flat.transform(parse_snaphot, 'fileName', 'changes') \
    .transform(cast_timestamp, ["arr_ct_raw", "dep_ct_raw", "snapshot_ts","arr_pt_raw","dep_pt_raw","arr_clt_raw", "dep_clt_raw"])\
    .transform(cast_boolean_for_hidden, 'dep_hi_raw').transform(cast_boolean_for_hidden, 'arr_hi_raw')

cDf= cDf.transform(process_event_status, 'dep_ps_raw', 'dep_ps').transform(process_event_status, 'dep_cs_raw', 'dep_cs').transform(process_event_status, 'arr_cs_raw', 'arr_cs').transform(process_event_status, 'arr_ps_raw', 'arr_ps')
cDf.show()
spark.stop()