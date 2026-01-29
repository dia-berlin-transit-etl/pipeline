# spark_etl/stations.py
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Station JSON -> dim_station parquet").getOrCreate()

station_json_path = "/opt/spark-data/station_data.json"
out_parquet_path = "/opt/spark-data/movements/dim_station"

raw = spark.read.option("multiLine", True).json(station_json_path)

# result[] -> rows
st = raw.select(F.explode(F.col("result")).alias("st")).select("st.*")

# evaNumbers[] -> rows, keep only isMain == true
ev = (
    st.select(
        F.col("name").alias("station_name"),
        F.explode(F.col("evaNumbers")).alias("e")
    )
    .filter(F.col("station_name").isNotNull())
    .filter(F.col("e.isMain") == F.lit(True))
)

# Extract only what you need (no coords / lon / lat)
base = (
    ev.select(
        F.col("e.number").cast("long").alias("station_eva"),
        F.col("station_name"),
    )
    .filter(F.col("station_eva").isNotNull())
)

def to_station_search_name_col(c):
    s = F.lower(F.trim(c))
    s = F.regexp_replace(s, "ß", "s")
    s = F.regexp_replace(s, "ä", "a")
    s = F.regexp_replace(s, "ö", "o")
    s = F.regexp_replace(s, "ü", "u")
    s = F.regexp_replace(s, r"([a-z0-9])_([a-z0-9])", r"$1$2")
    s = F.regexp_replace(s, r"\bhbf\b\.?", " hauptbahnhof ")
    s = F.regexp_replace(s, r"([a-z0-9])hbf\b\.?", r"$1hauptbahnhof")
    s = F.regexp_replace(s, r"\bbf\b\.?", " bahnhof ")
    # safer than lookbehind in Spark:
    s = F.regexp_replace(s, r"([a-gi-z0-9])bf\b\.?", r"$1bahnhof")
    s = F.regexp_replace(s, r"\bstr\b\.?", " strase ")
    s = F.regexp_replace(s, r"([a-z0-9])str\b\.?", r"$1strase")
    s = F.regexp_replace(s, r"\b(\w+)\s+strase\b", r"$1strase")
    s = F.regexp_replace(s, r"\bberlin\b", " ")
    s = F.regexp_replace(s, r"[^a-z0-9\s]", " ")
    s = F.regexp_replace(s, r"\s+", " ")
    return F.trim(s)

dim_station = (
    base.withColumn("station_name_search", to_station_search_name_col(F.col("station_name")))
        .select("station_eva", "station_name", "station_name_search")
        .dropDuplicates(["station_eva"])
)

dim_station.write.mode("overwrite").parquet(out_parquet_path)
spark.stop()
