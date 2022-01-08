USE video_analytics;
 
CREATE TABLE IF NOT EXISTS Merged_data_all
(
liked STRING,user_id STRING,video_end_type INT,minutes_played INT,video_id STRING,geo_cd STRING,channel_id STRING,creator_id STRING ,timestamp_col STRING ,disliked STRING) PARTITIONED BY (batchid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE IF NOT EXISTS enriched_data
(
liked STRING,user_id STRING,video_end_type INT,minutes_played INT,video_id STRING,geo_cd STRING,channel_id STRING,creator_id STRING ,timestamp_col STRING ,disliked STRING,batchid INT, status STRING
)
STORED AS PARQUET;
