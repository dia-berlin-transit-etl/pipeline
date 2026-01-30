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


def compute_avg_daily_delay(
    df,
    *,
    start_date: str,
    end_date: str,
    station_eva: int,
    use_fallback: bool = True,   # True => use actual_* when changed_* is NULL
):
    # Filter to station + date window (end_date exclusive, like your SQL example)
    base = (
        df.filter(sf.col("station_eva") == sf.lit(int(station_eva)))
          .filter(sf.col("snapshot_date") >= sf.to_date(sf.lit(start_date)))
          .filter(sf.col("snapshot_date") <  sf.to_date(sf.lit(end_date)))
    )

    # Latest row per (station_eva, stop_id, snapshot_date) using snapshot_key desc
    w = (
        Window
        .partitionBy("station_eva", "stop_id", "snapshot_date")
        .orderBy(sf.col("snapshot_key").desc_nulls_last())
    )
    latest = base.withColumn("rn", sf.row_number().over(w)).filter(sf.col("rn") == 1).drop("rn")

    # Choose which observed timestamp to compare against planned
    # If use_fallback: coalesce(changed, actual); else only changed
    arr_obs_ts = sf.col("changed_arrival_ts") if not use_fallback else sf.coalesce(sf.col("changed_arrival_ts"), sf.col("actual_arrival_ts"))
    dep_obs_ts = sf.col("changed_departure_ts") if not use_fallback else sf.coalesce(sf.col("changed_departure_ts"), sf.col("actual_departure_ts"))

    # Compute delay in minutes (double)
    arrival_delay_min = (arr_obs_ts.cast("long") - sf.col("planned_arrival_ts").cast("long")) / sf.lit(60.0)
    departure_delay_min = (dep_obs_ts.cast("long") - sf.col("planned_departure_ts").cast("long")) / sf.lit(60.0)

    latest = (
        latest
        .withColumn("arrival_delay_min", sf.when(arr_obs_ts.isNotNull() & sf.col("planned_arrival_ts").isNotNull(), arrival_delay_min))
        .withColumn("departure_delay_min", sf.when(dep_obs_ts.isNotNull() & sf.col("planned_departure_ts").isNotNull(), departure_delay_min))
    )

    # Build delay observations stream: arrival UNION ALL departure (like your SQL)
    arr_obs = (
        latest
        .select("snapshot_date", sf.col("arrival_delay_min").alias("delay_min"))
        .where(sf.col("delay_min").isNotNull())
        .where(sf.col("delay_min") >= 0)
        .where(sf.coalesce(sf.col("arrival_cancelled"), sf.lit(False)) == sf.lit(False))
        .where(sf.coalesce(sf.col("arrival_is_hidden"), sf.lit(False)) == sf.lit(False))
    )
    dep_obs = (
        latest
        .select("snapshot_date", sf.col("departure_delay_min").alias("delay_min"))
        .where(sf.col("delay_min").isNotNull())
        .where(sf.col("delay_min") >= 0)
        .where(sf.coalesce(sf.col("departure_cancelled"), sf.lit(False)) == sf.lit(False))
        .where(sf.coalesce(sf.col("departure_is_hidden"), sf.lit(False)) == sf.lit(False))
    )
    delay_obs = arr_obs.unionByName(dep_obs)

    # Daily average delay + counts
    daily = (
        delay_obs.groupBy("snapshot_date")
        .agg(
            sf.avg("delay_min").alias("daily_avg_delay_min"),
            sf.count("*").alias("n_delay_observations"),
        )
        .orderBy("snapshot_date")
    )

    # Average daily delay over the period (equal weight per day)
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





def get_daily_delay_averages1(df, station):
    #station = to_station_search_name(station)
    daily_delays = df.filter(sf.col('station_eva') == station)\
        .filter(((sf.col('arr_delay_min').isNotNull()) & (sf.col('arr_delay_min') >= 0) & (~sf.col('arr_cancelled')) & (~sf.col('arr_hi'))) | \
                ((sf.col('dep_delay_min').isNotNull()) & (sf.col('dep_delay_min') >= 0) & (~sf.col('dep_cancelled')) & (~sf.col('dep_hi')))) \
        .fillna(0, subset=['arr_delay_min', 'dep_delay_min']) \
        .withColumn('delay_min', sf.col('arr_delay_min') + sf.col('dep_delay_min')) \
        .groupBy('snapshot_date').avg('delay_min').withColumnRenamed('avg(delay_min)', 'delay_average')

    return daily_delays

def get_avg_daily_delay1(df, station):
    avg_daily_delay = get_daily_delay_averages(df, station).select(sf.avg('delay_average'))
    avg_daily_delay.show()
    return avg_daily_delay.collect()[0]

def get_avg_number_train_dep1(df):
    stations = df.select('station_eva').distinct()

    df = df.filter((~sf.col('dep_cancelled')) & (~sf.col('dep_hi')) & (sf.col('dep_pt').isNotNull()) & (~sf.lower(sf.col('train_category')).contains('bus'))) \
        .dropDuplicates(['stop_id', 'station_eva', 'dep_pt']) \
        .withColumn('actual_dep', sf.when(sf.col('dep_ct').isNotNull(), sf.col('dep_ct')).otherwise(sf.col('dep_pt'))) \
        .withColumns({'actual_dep_hour': sf.hour(sf.col('actual_dep')), 'actual_dep_day': sf.to_date(sf.col('actual_dep'))})
    
    dep_days = df.select('actual_dep_day').distinct()
    all_dep_and_station_pairs = stations.crossJoin(dep_days)
    departures_by_peak_hours = df.filter(sf.col('actual_dep_hour').isin([7,8,17,18]))
    departures_by_peak_hours = departures_by_peak_hours.groupBy('station_eva', 'actual_dep_day').count().withColumnRenamed('count', 'dep_count')
    avg_departure_by_station = all_dep_and_station_pairs.join(departures_by_peak_hours, ['actual_dep_day', 'station_eva'], 'left') \
        .fillna(0, 'dep_count') \
        .groupBy('station_eva').avg('dep_count').withColumnRenamed('avg(dep_count)', 'avg_dep_count')
    return avg_departure_by_station

def get_daily_delay_averages(df, station):
    station = to_station_search_name(station)
    df = df.withColumns({
            "arr_delay_min": sf.when(sf.col("planned_arrival_ts").isNull(), None)
                               .when(sf.col("changed_arrival_ts").isNotNull(), sf.floor((sf.unix_timestamp("changed_arrival_ts") - sf.unix_timestamp("planned_arrival_ts")) / 60))
                               .otherwise(0),
            "dep_delay_min": sf.when(sf.col("planned_departure_ts").isNull(), None)
                               .when(sf.col("changed_departure_ts").isNotNull(), sf.floor((sf.unix_timestamp("changed_departure_ts") - sf.unix_timestamp("planned_departure_ts")) / 60))
                               .otherwise(0)
        })
    
    daily_delays = df.filter(sf.col('station_eva') == station)\
        .filter(((sf.col('arr_delay_min').isNotNull()) & (sf.col('arr_delay_min') >= 0) & (~sf.col('arrival_cancelled')) & (~sf.col('arrival_is_hidden'))) | \
                ((sf.col('dep_delay_min').isNotNull()) & (sf.col('dep_delay_min') >= 0) & (~sf.col('departure_cancelled')) & (~sf.col('departure_is_hidden')))) \
        .fillna(0, subset=['arr_delay_min', 'dep_delay_min']) \
        .withColumn('delay_min', sf.col('arr_delay_min') + sf.col('dep_delay_min')) \
        .groupBy('snapshot_date').avg('delay_min').withColumnRenamed('avg(delay_min)', 'delay_average')

    return daily_delays

def get_avg_daily_delay(df, station):
    avg_daily_delay = get_daily_delay_averages(df, station).select(sf.avg('delay_average'))
    avg_daily_delay.show()
    return avg_daily_delay.collect()[0]

def get_avg_number_train_dep(df):
    stations = df.select('station_eva').distinct()

    df = df.filter((~sf.col('departure_cancelled')) & (~sf.col('departure_is_hidden')) & (sf.col('planned_departure_ts').isNotNull()) & (~sf.lower(sf.col('train_category')).contains('bus'))) \
        .dropDuplicates(['stop_id', 'station_eva', 'planned_departure_ts']) \
        .withColumn('actual_dep', sf.when(sf.col('changed_departure_ts').isNotNull(), sf.col('changed_departure_ts')).otherwise(sf.col('planned_departure_ts'))) \
        .withColumns({'actual_dep_hour': sf.hour(sf.col('actual_dep')), 'actual_dep_day': sf.to_date(sf.col('actual_dep'))})
    
    dep_days = df.select('actual_dep_day').distinct()
    all_dep_and_station_pairs = stations.crossJoin(dep_days)
    departures_by_peak_hours = df.filter(sf.col('actual_dep_hour').isin([7,8,17,18]))
    departures_by_peak_hours = departures_by_peak_hours.groupBy('station_eva', 'actual_dep_day').count().withColumnRenamed('count', 'dep_count')
    avg_departure_by_station = all_dep_and_station_pairs.join(departures_by_peak_hours, ['actual_dep_day', 'station_eva'], 'left') \
        .fillna(0, 'dep_count') \
        .groupBy('station_eva').avg('dep_count').withColumnRenamed('avg(dep_count)', 'avg_dep_count')
    return avg_departure_by_station

def main():
    #traindep = get_avg_number_train_dep(movementDf)
    #traindep.show()
    #avg_daily_delay = get_avg_daily_delay(movementDf, 8011155)


    path_to_movements_parquet = "file:///opt/spark-data/movements/final_movements"
    spark = SparkSession.builder.appName("Berlin Public Transport").getOrCreate()

    df = spark.read.parquet(path_to_movements_parquet)

    # 1) Sanity: available snapshot_date range
    df.select(
        sf.min("snapshot_date").alias("min_snapshot_date"),
        sf.max("snapshot_date").alias("max_snapshot_date"),
    ).show(truncate=False)

    # 2) Sanity: do we even have rows for this station + window?
    station_eva = 8011162
    start_date = "2025-10-04"
    end_date = "2025-10-07"

    base = (
        df.filter(sf.col("station_eva") == sf.lit(station_eva))
          .filter(sf.col("snapshot_date") >= sf.to_date(sf.lit(start_date)))
          .filter(sf.col("snapshot_date") <  sf.to_date(sf.lit(end_date)))
    )

    print("rows in window for station =", base.count())

    # 3) Inspect whether delay inputs exist (new schema columns)
    base.select(
        "snapshot_date",
        "planned_arrival_ts", "changed_arrival_ts",
        "planned_departure_ts", "changed_departure_ts",
        "actual_arrival_ts", "actual_departure_ts",
        "arrival_cancelled", "departure_cancelled",
        "arrival_is_hidden", "departure_is_hidden",
        "category", "train_number",
        "stop_id", "snapshot_key",
    ).show(50, truncate=False)

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