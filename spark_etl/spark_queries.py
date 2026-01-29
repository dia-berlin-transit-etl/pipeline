import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf


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




def compute_average_daily_delay_from_resolved(
    resolved_df,
    start_date=None,   # "yyyy-MM-dd"
    end_date=None,     # "yyyy-MM-dd" (exclusive)
    station_eva=None,
    use_fallback=False,   # set True if you want delay even without changed_* timestamps
):
    df = resolved_df

    # Filter by snapshot_date (already a DATE in your schema)
    if start_date:
        df = df.filter(sf.col("snapshot_date") >= sf.to_date(sf.lit(start_date)))
    if end_date:
        df = df.filter(sf.col("snapshot_date") < sf.to_date(sf.lit(end_date)))
    if station_eva is not None:
        df = df.filter(sf.col("station_eva") == sf.lit(station_eva))

    # Hidden flags: prefer change-hidden when available, else final hidden
    arr_hidden = sf.coalesce(sf.col("ch_arrival_is_hidden"), sf.col("arrival_is_hidden"), sf.lit(False))
    dep_hidden = sf.coalesce(sf.col("ch_departure_is_hidden"), sf.col("departure_is_hidden"), sf.lit(False))

    # --- Arrival delay ---
    # Primary (DB semantics): ct - pt from the same change record
    arrival_delay_primary = sf.when(
        sf.col("changed_arrival_ts").isNotNull()
        & sf.col("ch_planned_arrival_ts").isNotNull()
        & ~sf.coalesce(sf.col("arrival_cancelled"), sf.lit(False))
        & ~arr_hidden,
        (sf.col("changed_arrival_ts").cast("long") - sf.col("ch_planned_arrival_ts").cast("long")) / 60.0
    )

    # Optional fallback: actual - planned (less faithful, but fills more values)
    arrival_delay_fallback = sf.when(
        sf.col("actual_arrival_ts").isNotNull()
        & sf.col("planned_arrival_ts").isNotNull()
        & ~sf.coalesce(sf.col("arrival_cancelled"), sf.lit(False))
        & ~arr_hidden,
        (sf.col("actual_arrival_ts").cast("long") - sf.col("planned_arrival_ts").cast("long")) / 60.0
    )

    arrival_delay = sf.coalesce(arrival_delay_primary, arrival_delay_fallback) if use_fallback else arrival_delay_primary

    # --- Departure delay ---
    departure_delay_primary = sf.when(
        sf.col("changed_departure_ts").isNotNull()
        & sf.col("ch_planned_departure_ts").isNotNull()
        & ~sf.coalesce(sf.col("departure_cancelled"), sf.lit(False))
        & ~dep_hidden,
        (sf.col("changed_departure_ts").cast("long") - sf.col("ch_planned_departure_ts").cast("long")) / 60.0
    )

    departure_delay_fallback = sf.when(
        sf.col("actual_departure_ts").isNotNull()
        & sf.col("planned_departure_ts").isNotNull()
        & ~sf.coalesce(sf.col("departure_cancelled"), sf.lit(False))
        & ~dep_hidden,
        (sf.col("actual_departure_ts").cast("long") - sf.col("planned_departure_ts").cast("long")) / 60.0
    )

    departure_delay = sf.coalesce(departure_delay_primary, departure_delay_fallback) if use_fallback else departure_delay_primary

    df = df.withColumn("arrival_delay_min", arrival_delay)\
           .withColumn("departure_delay_min", departure_delay)

    # TRUE "average daily": compute per-day average first, then average those days
    daily = df.groupBy("station_eva", "snapshot_date").agg(
        sf.avg("arrival_delay_min").alias("daily_avg_arrival_delay_min"),
        sf.avg("departure_delay_min").alias("daily_avg_departure_delay_min"),
    )

    out = daily.groupBy("station_eva").agg(
        sf.avg("daily_avg_arrival_delay_min").alias("avg_daily_arrival_delay_min"),
        sf.avg("daily_avg_departure_delay_min").alias("avg_daily_departure_delay_min"),
    )

    return out.fillna({
        "avg_daily_arrival_delay_min": 0.0,
        "avg_daily_departure_delay_min": 0.0,
    })




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

    # 3) Inspect whether delay inputs exist (this tells you why delays become null)
    base.select(
        "snapshot_date",
        "ch_planned_arrival_ts", "changed_arrival_ts",
        "ch_planned_departure_ts", "changed_departure_ts",
        "planned_arrival_ts", "actual_arrival_ts",
        "planned_departure_ts", "actual_departure_ts",
        "arrival_cancelled", "departure_cancelled",
        "ch_arrival_is_hidden", "ch_departure_is_hidden",
        "arrival_is_hidden", "departure_is_hidden",
    ).show(50, truncate=False)

    # 4) Compute delays
    out = compute_average_daily_delay_from_resolved(
        df,
        start_date=start_date,
        end_date=end_date,
        station_eva=station_eva,
        use_fallback=False,   # switch to True if changed_* is sparse
    )

    out.show(truncate=False)

    df.printSchema()
    

    spark.stop()

if __name__ == "__main__":
    main()