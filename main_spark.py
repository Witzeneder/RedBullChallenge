# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, first, rank, row_number
from pyspark.sql.window import Window

'''     Thoughts

        'active_devices_per_day.csv'        --| tv_day, device_id
        'device_to_cluster_candidates.csv'  --| cluster_id, device_id, rank, distance
        
        Used: 
        Java 11
        Spark 3.2.0
        Hadoop 3.2.2
        
        As I got this here as a batch of data, I will use Spark to ad-hoc create the 
        analysis by:
        - Reading CSV's
        - Joining them based on the device_id (results in huge dataset of combinations)
        - Create window to partition and order dataset
        - Filter based on the desired number_of_devices per day/cluster
        - Write into CSV
        
        Ideas Stream
        If I wouldn't get them as a batch of data like this, I'd rather use a streaming based
        approach from Spark, using it to read from Kafka etc. and instead of saving it into a csv
        file, I'd dump it in some storage system which gets updated
        
        For the first CSV "active_devices_per_day" I'd then even argue it is not needed to have
        it in this format, as we don't really care about the device_id and the tv_day. We care
        about if the device_id and the tv_day combination - if it was active on a given day.
        We could use a bloom filter for each day to add device_ids to it and later test on the filter
        to match clusters.
        The other way round for the "device_to_cluster_candidates.csv" we could use combination of the
        rank and distance metric to add to a QDigest algorithm to calculate where given values land on 
        the quantile spectrum in context of the cluster analysis 
'''


def sort_clusters1(tv_csv_path, cluster_csv_path, number_of_devices: int):
    spark_session = SparkSession.builder.appName('session').getOrCreate()
    df1 = spark_session.read.csv(tv_csv_path, header=True, inferSchema=True)
    df2 = spark_session.read.csv(cluster_csv_path, header=True, inferSchema=True)

    # Merge into one based on the device_id
    merged_df = df1.join(df2, on='device_id')

    # Partition (DAY|Cluster)
    # Order on (RANK|Distance)
    window_spec = Window.partitionBy("tv_day", "cluster_id").orderBy("rank", "distance")

    # Create new column "rank_num" based the window_spec
    df = merged_df.withColumn("rank_num", rank().over(window_spec))

    # Filter for the first number_of_devices rank_num
    df = df.filter(df.rank_num <= number_of_devices)

    # Group the dataframe based on (Day|Cluster|Device_id)
    # Aggregate based on the min (RANK|Distance)
    # Order them by  (Day|Cluster)
    df = df.groupBy("tv_day", "cluster_id", "device_id") \
        .agg(min("rank").alias("rank"), min("distance").alias("distance")) \
        .orderBy("tv_day", "cluster_id")

    # Get the unique tv_day values
    unique_tv_days = df.select("tv_day").distinct().collect()

    # Iterate over the unique tv_day values and create individual csv files
    # It is written into multiple csv files because of the way spark handles
    # the dataframes (already partitioned)
    for tv_day in unique_tv_days:
        filtered_df = df.filter("tv_day = '{}'".format(tv_day[0]))
        filtered_df.write.format("csv").option("header", "true").mode("overwrite").save(f"{tv_day[0]}.csv")


if __name__ == '__main__':
    sort_clusters1(
        'active_devices_per_day.csv',
        'device_to_cluster_candidates.csv',
        10
    )
