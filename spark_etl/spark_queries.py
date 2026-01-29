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


def get_daily_delay_averages1(df, station):
    station = to_station_search_name(station)
    daily_delays = df.filter(sf.col('station') == station)\
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
    stations = df.select('station').distinct()

    df = df.filter((~sf.col('dep_cancelled')) & (~sf.col('dep_hi')) & (sf.col('dep_pt').isNotNull()) & (~sf.lower(sf.col('train_category')).contains('bus'))) \
        .dropDuplicates(['stop_id', 'station', 'dep_pt']) \
        .withColumn('actual_dep', sf.when(sf.col('dep_ct').isNotNull(), sf.col('dep_ct')).otherwise(sf.col('dep_pt'))) \
        .withColumns({'actual_dep_hour': sf.hour(sf.col('actual_dep')), 'actual_dep_day': sf.to_date(sf.col('actual_dep'))})
    
    dep_days = df.select('actual_dep_day').distinct()
    all_dep_and_station_pairs = stations.crossJoin(dep_days)
    departures_by_peak_hours = df.filter(sf.col('actual_dep_hour').isin([7,8,17,18]))
    departures_by_peak_hours = departures_by_peak_hours.groupBy('station', 'actual_dep_day').count().withColumnRenamed('count', 'dep_count')
    avg_departure_by_station = all_dep_and_station_pairs.join(departures_by_peak_hours, ['actual_dep_day', 'station'], 'left') \
        .fillna(0, 'dep_count') \
        .groupBy('station').avg('dep_count').withColumnRenamed('avg(dep_count)', 'avg_dep_count')
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
    
    daily_delays = df.filter(sf.col('station') == station)\
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
    stations = df.select('station').distinct()

    df = df.filter((~sf.col('departure_cancelled')) & (~sf.col('departure_is_hidden')) & (sf.col('planned_departure_ts').isNotNull()) & (~sf.lower(sf.col('train_category')).contains('bus'))) \
        .dropDuplicates(['stop_id', 'station', 'planned_departure_ts']) \
        .withColumn('actual_dep', sf.when(sf.col('changed_departure_ts').isNotNull(), sf.col('changed_departure_ts')).otherwise(sf.col('planned_departure_ts'))) \
        .withColumns({'actual_dep_hour': sf.hour(sf.col('actual_dep')), 'actual_dep_day': sf.to_date(sf.col('actual_dep'))})
    
    dep_days = df.select('actual_dep_day').distinct()
    all_dep_and_station_pairs = stations.crossJoin(dep_days)
    departures_by_peak_hours = df.filter(sf.col('actual_dep_hour').isin([7,8,17,18]))
    departures_by_peak_hours = departures_by_peak_hours.groupBy('station', 'actual_dep_day').count().withColumnRenamed('count', 'dep_count')
    avg_departure_by_station = all_dep_and_station_pairs.join(departures_by_peak_hours, ['actual_dep_day', 'station'], 'left') \
        .fillna(0, 'dep_count') \
        .groupBy('station').avg('dep_count').withColumnRenamed('avg(dep_count)', 'avg_dep_count')
    return avg_departure_by_station

def main():
    path_to_movements_parquet = "/opt/spark-data/output"
    spark = (SparkSession.builder.appName("Berlin Public Transport").getOrCreate())
    movementDf = spark.read.parquet(path_to_movements_parquet)
    traindep = get_avg_number_train_dep(movementDf)
    traindep.show()
    avg_daily_delay = get_avg_daily_delay(movementDf, 'alexanderplatz')
    print(avg_daily_delay)
    spark.stop()

if __name__ == "__main__":
    main()
