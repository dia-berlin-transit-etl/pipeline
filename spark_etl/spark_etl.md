# Large-Scale ETL and Analysis in Spark

## A description of ETL pipeline

## The resulting schema

# Spark queries

## Task 3.2

```python
def get_daily_delay_averages(df, station):
    # station = to_station_search_name(station)
    daily_delays = df.filter(sf.col('station') == station)\
        .filter(((sf.col('arr_delay_min').isNotNull()) & (sf.col('arr_delay_min') >= 0) & (~sf.col('arr_cancelled')) & (~sf.col('arr_hi'))) | \
                ((sf.col('dep_delay_min').isNotNull()) & (sf.col('dep_delay_min') >= 0) & (~sf.col('dep_cancelled')) & (~sf.col('dep_hi')))) \
        .fillna(0, subset=['arr_delay_min', 'dep_delay_min']) \
        .withColumn('delay_min', sf.col('arr_delay_min') + sf.col('dep_delay_min')) \
        .groupBy('snapshot_date').avg('delay_min').withColumnRenamed('avg(delay_min)', 'delay_average')

    return daily_delays

def get_avg_daily_delay(df, station):
    avg_daily_delay = get_daily_delay_averages(df, station).select(sf.avg('delay_average'))
    avg_daily_delay.show()
    return avg_daily_delay.collect()[0]

path_to_movements_parquet = "/opt/spark-data/movements"
spark = SparkSession.builder.appName("Berlin Public Transport").getOrCreate()
movementDf = spark.read.parquet(path_to_movements_parquet)

station = 'alexanderplatz' #change for a given station,

avg_dayily_delay = get_avg_daily_delay(movementDf, station)

spark.stop()
```

We read a resulting Parquet dataset from Task 3.1 into `movementDf`, also normalize the given station name to search. To find the average daily delay for a station over the collected period at fist we compute `daily_delays` as average of delays per day, then average over these days.

To compute `daily_delays` we exclude invalid observations by filtering out `NULL` delays, negative delays, cancelled events, and hidden events. Since it could be only arrival or departure was delayed we fill null values of `arr_delay_min`, `dep_delay_min` as 0, then we compute complete train delay `delay_min = arr_delay_min + dep_delay_min` and get average delay min by observed day.

## Task 3.3

```python
def get_avg_number_train_dep(df):
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


path_to_movements_parquet = "/opt/spark-data/movements"
spark = SparkSession.builder.appName("Berlin Public Transport").getOrCreate()
movementDf = spark.read.parquet(path_to_movements_parquet)


avg_number_train_dep = get_avg_number_train_dep(movementDf)

spark.stop()

```

We read a resulting Parquet dataset from Task 3.1 into `movementDf`. To compute the average number of train departures per station during peak hours we exclude invalid observations by filtering out cancelled and hidden departure events, records without planned departure times, and non-train services such as buses. As an intermeidate step we compute `actual_dep` as `dep_ct` if it exists else `dep_pt`, from which `actual_dep_hour` and `actual_dep_day` are extracted. We take only train departures during peak hours (07:00 to 09:00 and 17:00 to 19:00) `('actual_dep_hour').isin([7,8,17,18])` and for each station and day, we count the number of such departures. Since it might be that a station does not appear on a given day within those time ranges, we left-join the counts to the complete set of all station–day pairs, fill missing counts with 0, and finally compute the average daily number of peak-hour departures per station.
