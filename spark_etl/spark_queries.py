from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

def get_daily_delay_averages(df, station):
    daily_delays = df.filter(sf.col('station') == station)\
        .filter(((sf.col('arr_delay_min') >= 0) & (~sf.col('arr_cancelled')) & (~sf.col('arr_hi'))) | \
                ((sf.col('dep_delay_min') >= 0) & (~sf.col('dep_cancelled')) & (~sf.col('arr_hi')))) \
        .fillna(0, subset=['arr_delay_min', 'dep_delay_min']) \
        .withColumn('delay_min', sf.col('arr_delay_min') + sf.col('dep_delay_min')) \
        .groupBy('snapshot_date').avg('delay_min').withColumnRenamed('avg(delay_min)', 'delay_average')

    return daily_delays

def get_avg_daily_delay(df, station):
    avg_daily_delay = get_daily_delay_averages(df, station).select(sf.avg('delay_average'))
    avg_daily_delay.show()
    return avg_daily_delay.collect()[0]

def get_avg_number_train_dep_(df, station, start, end):
    return

def main():
    path_to_movements_parquet = "/opt/spark-data/movements"
    spark = (SparkSession.builder.appName("Berlin Public Transport").getOrCreate())
    movementDf = spark.read.parquet(path_to_movements_parquet)
    get_avg_daily_delay(movementDf, 'alexanderplatz')
    
    spark.stop()

if __name__ == "__main__":
    main()
