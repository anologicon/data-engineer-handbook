from pyspark.sql import SparkSession


def do_actors_history_scd_full_load_transformation(spark, dataframe):
    query = f"""
        WITH did_change_cte AS (
            SELECT
                actorid,
                actor,
                quality_class,
                is_active,
                year,
                LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year) <> quality_class OR
                LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY year) <> is_active OR
                LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year) IS NULL OR
                LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY year) IS NULL AS did_change
            FROM actors
        ),

        streak_identfier_cte AS (
            SELECT 
                *,
                SUM(CASE WHEN did_change THEN 1 ELSE 0 END) OVER (PARTITION BY actorid ORDER BY year) AS streak_identifier
            FROM did_change_cte
        ),

        start_end_date AS (
            SELECT 
                actorid,
                actor,
                is_active,
                quality_class,
                streak_identifier,
                year AS start_date,
                COALESCE(LAG(year, 1) OVER (PARTITION BY actorid ORDER BY streak_identifier DESC), 9999) AS end_date
            FROM streak_identfier_cte
            WHERE did_change IS TRUE
        )
        SELECT 
            actorid,
            actor,
            quality_class,
            is_active,
            start_date,
            end_date
        FROM start_end_date
    """
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_history_scd_full_load") \
      .getOrCreate()
    output_df = do_actors_history_scd_full_load_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_history_scd_full_load")