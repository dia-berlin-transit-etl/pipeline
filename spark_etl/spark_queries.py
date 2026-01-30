import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window


def to_station_search_name(name: str) -> str:
    """
    Search helper string used for pg_trgm / fuzzystrmatch lookups.
    """
    s = (name or "").strip().lower()

    # German folding (more robust for fuzzy matching / filenames)
    s = (s.replace("ß", "s") # not ss but s
           .replace("ä", "a")
           .replace("ö", "o")
           .replace("ü", "u"))

    # If filenames use '_' as a placeholder inside words (e.g. s_d for süd),
    # don't turn it into a space — remove it when it's between word chars.
    s = re.sub(r"(?<=\w)_(?=\w)", "", s)

    # hbf (word + suffix)
    s = re.sub(r"\bhbf\b\.?", " hauptbahnhof ", s)
    s = re.sub(r"(?<=\w)hbf\b\.?", "hauptbahnhof", s)

    # bf (word + suffix) — suffix excludes "...hbf"
    s = re.sub(r"\bbf\b\.?", " bahnhof ", s)
    s = re.sub(r"(?<=\w)(?<!h)bf\b\.?", "bahnhof", s)

    # str (word + suffix)
    s = re.sub(r"\bstr\b\.?", " strase ", s)
    s = re.sub(r"(?<=\w)str\b\.?", "strase", s)
    s = re.sub(r"\b(\w+)\s+strase\b", r"\1strase", s)

    s = re.sub(r"\bberlin\b", " ", s)

    # Keep underscore already handled; now strip everything else to spaces
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def compute_peak_hour_departure_counts(resolved_df):
    stations = resolved_df.select('station_eva').distinct()
    df = resolved_df.filter(
        sf.col("actual_departure_ts").isNotNull()
        & ~sf.col("departure_cancelled")
        & ~sf.col("departure_is_hidden")) \
        .withColumns({'actual_dep_hour': sf.hour(sf.col('actual_departure_ts')), 'actual_dep_day': sf.to_date(sf.col('actual_departure_ts'))})
    dep_days = df.select('actual_dep_day').distinct()
    all_dep_and_station_pairs = stations.crossJoin(dep_days)
    departures_by_peak_hours = df.filter(sf.col('actual_dep_hour').isin([7,8,17,18]))
    departures_by_peak_hours = departures_by_peak_hours.groupBy('station_eva', 'actual_dep_day').count().withColumnRenamed('count', 'dep_count')
    avg_departure_by_station = all_dep_and_station_pairs.join(departures_by_peak_hours, ['actual_dep_day', 'station_eva'], 'left') \
        .fillna(0, 'dep_count') \
        .groupBy('station_eva').avg('dep_count').withColumnRenamed('avg(dep_count)', 'avg_dep_count')
    return avg_departure_by_station


def compute_avg_daily_delay(
    df,
    *,
    start_date: str,
    end_date: str,
    station_eva: int,
    use_fallback: bool = True,
):
    base = (
        df.filter(sf.col("station_eva") == sf.lit(int(station_eva)))
          .filter(sf.col("snapshot_date") >= sf.to_date(sf.lit(start_date)))
          .filter(sf.col("snapshot_date") <  sf.to_date(sf.lit(end_date)))
    )

    arr_obs_ts = sf.col("changed_arrival_ts") if not use_fallback else sf.coalesce(sf.col("changed_arrival_ts"), sf.col("actual_arrival_ts"))
    dep_obs_ts = sf.col("changed_departure_ts") if not use_fallback else sf.coalesce(sf.col("changed_departure_ts"), sf.col("actual_departure_ts"))

    arrival_delay_min = (arr_obs_ts.cast("long") - sf.col("planned_arrival_ts").cast("long")) / sf.lit(60.0)
    departure_delay_min = (dep_obs_ts.cast("long") - sf.col("planned_departure_ts").cast("long")) / sf.lit(60.0)

    base = (
        base
        .withColumn("arrival_delay_min", sf.when(arr_obs_ts.isNotNull() & sf.col("planned_arrival_ts").isNotNull(), arrival_delay_min))
        .withColumn("departure_delay_min", sf.when(dep_obs_ts.isNotNull() & sf.col("planned_departure_ts").isNotNull(), departure_delay_min))
    )

    arr_obs = (
        base
        .select("snapshot_date", sf.col("arrival_delay_min").alias("delay_min"))
        .where(sf.col("delay_min").isNotNull())
        .where(sf.col("delay_min") >= 0)
        .where(sf.coalesce(sf.col("arrival_cancelled"), sf.lit(False)) == sf.lit(False))
        .where(sf.coalesce(sf.col("arrival_is_hidden"), sf.lit(False)) == sf.lit(False))
    )
    dep_obs = (
        base
        .select("snapshot_date", sf.col("departure_delay_min").alias("delay_min"))
        .where(sf.col("delay_min").isNotNull())
        .where(sf.col("delay_min") >= 0)
        .where(sf.coalesce(sf.col("departure_cancelled"), sf.lit(False)) == sf.lit(False))
        .where(sf.coalesce(sf.col("departure_is_hidden"), sf.lit(False)) == sf.lit(False))
    )
    delay_obs = arr_obs.unionByName(dep_obs)

    
    daily = (
        delay_obs.groupBy("snapshot_date")
        .agg(
            sf.avg("delay_min").alias("daily_avg_delay_min"),
            sf.count("*").alias("n_delay_observations"),
        )
        .orderBy("snapshot_date")
    )

    overall = (
        daily.agg(
            sf.avg("daily_avg_delay_min").alias("avg_daily_delay_min"),
            sf.sum("n_delay_observations").alias("total_delay_observations"),
            sf.count("*").alias("n_days"),
        )
        .withColumn("station_eva", sf.lit(int(station_eva)))
        .select("station_eva", "avg_daily_delay_min", "n_days", "total_delay_observations")
    )

    return daily, overall



def main():
    #traindep = get_avg_number_train_dep(movementDf)
    #traindep.show()
    #avg_daily_delay = get_avg_daily_delay(movementDf, 8011155)


    path_to_movements_parquet = "file:///opt/spark-data/movements/final_movements"

    spark = SparkSession.builder.appName("Berlin Public Transport").getOrCreate()

    df = spark.read.parquet(path_to_movements_parquet)
    dim_station_df = spark.read.parquet("/opt/spark-data/movements/dim_station")


    # 2) Sanity: do we even have rows for this station + window?
    station_eva = 8011162
    start_date = "2025-09-02"
    end_date = "2025-10-16"
    #start_date = "2025-10-04"
    #end_date = "2025-10-07"

    base = (
        df.filter(sf.col("station_eva") == sf.lit(station_eva))
          .filter(sf.col("snapshot_date") >= sf.to_date(sf.lit(start_date)))
          .filter(sf.col("snapshot_date") <  sf.to_date(sf.lit(end_date)))
    )

    print("rows in window for station =", base.count())

    base.select(
        sf.count("*").alias("rows"),
        sf.sum(sf.col("changed_arrival_ts").isNotNull().cast("int")).alias("n_changed_arr"),
        sf.sum(sf.col("changed_departure_ts").isNotNull().cast("int")).alias("n_changed_dep"),
        sf.sum(sf.col("actual_arrival_ts").isNotNull().cast("int")).alias("n_actual_arr"),
        sf.sum(sf.col("actual_departure_ts").isNotNull().cast("int")).alias("n_actual_dep"),
    ).show(truncate=False)


    daily, overall = compute_avg_daily_delay(
        df,
        start_date=start_date,
        end_date=end_date,
        station_eva=station_eva,
        use_fallback=True,   # IMPORTANT with your data (changed_* is mostly NULL)
    )

    daily.show(50, truncate=False)
    overall.show(truncate=False)


    spark.stop()
    
    

if __name__ == "__main__":
    main()