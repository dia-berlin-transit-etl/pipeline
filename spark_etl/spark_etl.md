# Large-Scale ETL and Analysis in Spark

## A description of ETL pipeline

- We create a station lookup DataFrame with station name → EVA mappings from `station_data.json`. Result: a small lookup table (~133 rows for Berlin stations) with normalized
  station names for joining.

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


path_to_movements_parquet = "file:///opt/spark-data/movements/final_movements"
spark = SparkSession.builder.appName("Berlin Public Transport").getOrCreate()
df = spark.read.parquet(path_to_movements_parquet)

avg_number_train_dep = compute_peak_hour_departure_counts(df)

spark.stop()

```

We load the Parquet dataset from Task 3.1 into a Spark DataFrame. To compute the average number of train departures per station during peak hours, we first pre-process the data by filtering out cancelled or hidden departure events and records missing a planned departure time.

The `actual_departure_ts` is derived by prioritizing the changed timestamp (where available) over the planned one. From this value, we extract the hour and date. The dataset is then filtered for peak hours (07:00–09:00 and 17:00–19:00).

For each station and day, we count the number of such departures. Since some stations may have no activity on certain days within those time ranges, we left-join the counts to the complete set of all station–day pairs, filling missing counts with 0. Finally, we aggregate the data to compute the average daily number of peak-hour departures per station.
