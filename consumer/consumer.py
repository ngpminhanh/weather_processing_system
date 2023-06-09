# .\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 E:\Study\2022_2023_HK2\big_data\code\consumer.py

scala_version = '2.12'
spark_version = '3.4.0'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.0'
]

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'

from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("weather2") \
        .config("spark.jars.packages", ",".join(packages))\
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType, FloatType, DoubleType

schema = StructType([
    StructField("date", StringType(), False),
    StructField("hour", StringType(), False),
    StructField("prcp", StringType(), False),
    StructField("stp", StringType(), False),
    StructField("smax", StringType(), False),
    StructField("smin", StringType(), False),
    StructField("gbrd", StringType(), False),
    StructField("temp", StringType(), False),
    StructField("dewp", StringType(), False),
    StructField("tmax", StringType(), False),
    StructField("tmin", StringType(), False),
    StructField("dmax", StringType(), False),
    StructField("dmin", StringType(), False),
    StructField("hmax", StringType(), False),
    StructField("hmin", StringType(), False),
    StructField("hmdy", StringType(), False),
    StructField("wdct", StringType(), False),
    StructField("gust", StringType(), False),
    StructField("wdsp", StringType(), False),
    StructField("region", StringType(), False),
    StructField("state", StringType(), False),
    StructField("station", StringType(), False),
    StructField("station_code", StringType(), False),
    StructField("latitude", StringType(), False),
    StructField("longitude", StringType(), False),
    StructField("height", StringType(), False),
    StructField("year", StringType(), False)
])


# Kafka readStream
kafka_df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather6") \
    .option("startingOffsets", "earliest") \
    .load() 
# .option("subscribe", "weather4") \
# .option("startingOffsets", "earliest") \
# .option("startingOffsets", """{"weather4":{"0":0}}""") \
# .option("endingOffsets", """{"weather4":{"0":100}}""") \
kafka_df1.printSchema()


weatherdf = kafka_df1.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
weatherdf.printSchema()

from pyspark.sql.functions import *


# query_df = weatherdf.select(col("stp").cast('double'))
query_df = weatherdf.select("*")

query_df.printSchema()

for col in query_df.columns:
     query_df = query_df.withColumn(col, query_df[col].cast("string"))

# query_df = query_df.select("CAST(state AS STRING)", 
#                              "CAST(year AS STRING)",
#                              "CAST(Avg_atm_p AS STRING)", 
#                              "CAST(Max_air_p AS STRING)",
#                              "CAST(Min_air_p AS STRING)", 
#                              "CAST(Avg_Humidity AS STRING)",
#                              "CAST(Max_temp AS STRING)", 
#                              "CAST(Min_temp AS STRING)",
#                              "CAST(No_Station AS STRING)")

# query_df = query_df.selectExpr("CAST(stp AS STRING)")

write_stream = query_df.writeStream\
        .outputMode("append")\
        .format("csv")\
        .option("path", "output/filesink") \
        .option("header", True) \
        .option("checkpointLocation", "checkpoint/filesink_checkpoint") \
        .start()
write_stream.awaitTermination()
