from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
spark = SparkSession.builder.appName("SparkHomework").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

medals_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps_df = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
matches_df = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
match_details_df = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv").alias("match_details")
medals_matches_players_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")

match_details_df.sortWithinPartitions(col("match_id")).write.mode("overwrite").bucketBy(16, 'match_id').saveAsTable("bootcamp.match_details_bucketed")
matches_df.sortWithinPartitions(col("match_id")).write.mode("overwrite").bucketBy(16, 'match_id').saveAsTable("bootcamp.matches_bucketed")
medals_matches_players_df.sortWithinPartitions(col("match_id")).write.mode("overwrite").bucketBy(16, 'match_id').saveAsTable("bootcamp.medals_matches_players_bucketed")

df_broadcast = medals_matches_players_df \
        .join(broadcast(medals_df), on="medal_id", how="left") \
        .join(matches_df, on="match_id", how="left") \
        .join(broadcast(maps_df), on="mapid", how="left")

df_bucket = df_bucket = spark.sql("""
select
    match_id,
    medal_id,
    player_gamertag,
    playlist_id,
    player_total_kills,
    mapid,
    completion_date
    
from bootcamp.match_details_bucketed detail
left join bootcamp.matches_bucketed metches using(match_id)
left join bootcamp.medals_matches_players_bucketed medals_metches using(match_id,player_gamertag)
""")

df_bucket.createOrReplaceTempView("player_match_bucket")


spark.sql("""
with avg_kills as (
select 
    player_gamertag,
    avg(player_total_kills) as avg_kills
from player_match_bucket
group by 1
)
select
    player_gamertag,
    avg_kills
from avg_kills
order by avg_kills desc limit 1
""").show()

spark.sql("""
with group_cte as (
select
    distinct
    match_id,
    playlist_id
from player_match_bucket
)
select
    playlist_id,
    count(1) played_playlist
from group_cte
group by 1
order by 2 desc
limit 1
""").show()

spark.sql("""
with dedup as (
select
    match_id,
    mapid,
    row_number() over (partition by match_id, mapid order by completion_date) as r_
from player_match_bucket
)
select
    mapid,
    count(1) as plyed_count
from dedup where r_ = 1
group by 1
order by 2 desc
limit 1
""").show()

spark.sql("""
select
    mapid,
    count(1) as killing_spree_count
from player_match_bucket
where medal_id = '2430242797' /* killing spree */
group by 1
order by 2 desc
limit 1
""").show()


df_bucket.sortWithinPartitions(col('match_id')).write.mode('overwrite').saveAsTable('bootcamp.joined_dataset_0')
df_bucket.sortWithinPartitions(col('match_id'), col('mapid')).write.mode('overwrite').saveAsTable('bootcamp.joined_dataset_1')
df_bucket.sortWithinPartitions(col('match_id'), col('mapid'), col('playlist_id')).write.mode('overwrite').saveAsTable('bootcamp.joined_dataset_2')

spark.sql(""" 
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'version_0'
FROM bootcamp.joined_dataset_0.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'version_1'
FROM bootcamp.joined_dataset_1.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'version_2'
FROM bootcamp.joined_dataset_2.files
""").show()