create database video_analytics;

use video_analytics;

CREATE TABLE IF NOT EXISTS users_creator
(
user_id STRING,
creators_array ARRAY<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '&';

LOAD DATA LOCAL INPATH '/mnt/bigdatapgp/saurav_510636/project/Lookup data/user-creator.txt' OVERWRITE INTO TABLE users_creator;

create external table if not exists channel_geo_map
(
channel_id String,
geo_cd string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping"=":key,geo:geo_cd")
tblproperties("hbase.table.name"="channel-geo-map");

create external table if not exists subscribed_users
(
user_id STRING,
subscn_start_dt STRING,
subscn_end_dt STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping"=":key,subscn:startdt,subscn:enddt")
tblproperties("hbase.table.name"="subscribed-users");

create external table if not exists video_creator_map
(
video_id STRING,
creator_id STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping"=":key,creator:creatorid")
tblproperties("hbase.table.name"="video-creator-map");

