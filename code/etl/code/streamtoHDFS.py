import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import spark.implicits._

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "20.0.31.221:9092").option("subscribe", "jsonvideoanalytics").load()

newdf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

struct = new StructType()
  .add("liked", DataTypes.StringType)
.add("user_id", DataTypes.StringType)
.add("video_end_type", DataTypes.StringType)
.add("minutes_played", DataTypes.StringType)
.add("video_id", DataTypes.StringType)
.add("geo_cd", DataTypes.StringType)
.add("channel_id", DataTypes.StringType)
.add("creator_id", DataTypes.StringType)
.add("timestamp", DataTypes.StringType)
.add("disliked", DataTypes.StringType)

dataDf = newdf.select(from_json(col("value").cast("string"), struct).as("data"))

dataFlattenedDf = dataDf.selectExpr("data.liked", "data.user_id", "data.video_end_type", "data.minutes_played", "data.video_id", "data.geo_cd", "data.channel_id", "data.creator_id", "data.timestamp", "data.disliked")

    
dataFlattenedDf.createOrReplaceTempView("dataFlattened")
spark.read.csv("/tmp/video_creator_map/").createOrReplaceTempView("video_creator_map")
spark.read.csv("/tmp/channel_geo_map/").createOrReplaceTempView("channel_geo_map")

  
 spark.sql("""SELECT
IF (i.liked IS NULL, 0, i.liked) AS liked,i.user_id ,IF (i.video_end_type IS NULL, 3, i.video_end_type) AS video_end_type ,i.minutes_played,i.video_id,sg.geo_cd ,i.channel_id ,sa.creator_id  ,i.timestamp  ,i.disliked ,
IF((i.liked="true" AND i.disliked="true")
OR i.user_id IS NULL
OR i.video_id IS NULL
OR i.timestamp IS NULL
OR i.geo_cd IS NULL
OR i.user_id=''
OR i.video_id=''
OR i.timestamp=''
OR i.geo_cd=''
OR sg.geo_cd IS NULL
OR sg.geo_cd=''
OR sa.creator_id IS NULL
OR sa.creator_id='', 'fail', 'pass') AS status
FROM dataFlattened i LEFT OUTER JOIN channel_geo_map sg ON i.channel_id = sg.channel_id
LEFT OUTER JOIN video_creator_map sa ON i.video_id = sa.video_id""").coalesce(1).writeStream
.outputMode("append")
.format("orc")
.option("path", "hdfs://nameservice1/user/hive/warehouse/video_analytics.db/enriched_data/")
.option("checkpointLocation", "hdfs://nameservice1/tmp/checkpointvideoanalyticsjob/").start().awaitTermination()