from pyspark.sql import SparkSession


def do_hosts_cumulated_full_transformation(spark, dataframe):
    query = f"""
    SELECT 
        host,
        ARRAY_AGG(DISTINCT DATE(event_time)) AS host_activity_datelist
    FROM events
    GROUP BY 1
    """
    dataframe.createOrReplaceTempView("events")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("host_cumulated") \
      .getOrCreate()
    output_df = do_hosts_cumulated_full_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("hosts_cumulated_full_join")