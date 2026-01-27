from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

def get_daily_delay_averages(df, station):
    daily_delays = df.filter(sf.col('station') == station)\
        .filter(((sf.col('arr_delay_min') >= 0) & (~sf.col('arr_cancelled')) & (~sf.col('arr_hi'))) | \
                ((sf.col('dep_delay_min') >= 0) & (~sf.col('dep_cancelled')) & (~sf.col('dep_hi')))) \
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

    df = df.filter((~sf.col('dep_cancelled')) & (~sf.col('dep_hi')) & (sf.col('dep_pt').isNotNull())) \
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

def main():
    path_to_movements_parquet = "/opt/spark-data/movements"
    spark = (SparkSession.builder.appName("Berlin Public Transport").getOrCreate())
    movementDf = spark.read.parquet(path_to_movements_parquet)
    get_avg_number_train_dep(movementDf)
    get_avg_daily_delay(movementDf, 'alexanderplatz')
    
    spark.stop()

if __name__ == "__main__":
    main()
