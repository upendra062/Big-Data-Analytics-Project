import com.databricks.spark.xml._
import spark.implicits._
from pyspark.sql.functions import lit

df = spark.read.format("csv").option("header", "true").load("/tmp/data_gen.csv")

dftoWrite = df.withColumn("batchid",lit("2021122920"))

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
dftoWrite.write.insertInto("video_analytics.Merged_data_all")

xmlData = spark.read.format("com.databricks.spark.xml").option("rowTag", "record").load("/tmp/data_gen.xml")
xmlDataWrite = xmlData .withColumn("batchid",lit("2020013115"))
xmlDataWrite.write.insertInto("video_analytics.Merged_data_all")


xmlDataWrite = xmlData .withColumn("batchid",lit("2021122920"))