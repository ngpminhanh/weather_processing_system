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
        .appName("weather") \
        .config("spark.jars.packages", ",".join(packages))\
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType, FloatType

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
    .option("assign", """{"weather4":[0,1]}""") \
    .option("startingOffsets", """{"weather4":{"0":-2, "1":-2}}""") \
    .load() 

kafka_df2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[2,3]}""") \
    .option("startingOffsets", """{"weather4":{"2":-2, "3":-2}}""") \
    .load() 

kafka_df3 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[4,5]}""") \
    .option("startingOffsets", """{"weather4":{"4":-2, "5":-2}}""") \
    .load() 

kafka_df4 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[6,7]}""") \
    .option("startingOffsets", """{"weather4":{"6":-2, "7":-2}}""") \
    .load() 

kafka_df5 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[8,9]}""") \
    .option("startingOffsets", """{"weather4":{"8":-2, "9":-2}}""") \
    .load() 

kafka_df6 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("assign", """{"weather4":[10,11]}""") \
    .option("startingOffsets", """{"weather4":{"10":-2, "11":-2}}""") \
    .load() 

kafka_df1.printSchema()


weatherdf1 = kafka_df1.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


weatherdf2 = kafka_df2.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


weatherdf3 = kafka_df3.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


weatherdf4 = kafka_df4.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


weatherdf5 = kafka_df5.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


weatherdf6 = kafka_df6.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

weatherdf1.printSchema()

from pyspark.sql.functions import *


# query_df = weatherdf.select(col("stp").cast('double'))
query_df1 = weatherdf1.filter((col("stp").cast('double') > -1) & 
                           (col("smax").cast('double') > -1) &
                           (col("smin").cast('double') > -1) &
                           (col("hmdy").cast('double') > -1) &
                           (col("tmax").cast('double') > -1) &
                           (col("tmin").cast('double') > -1)) \
                .groupBy("year", "state") \
                .agg(
                    avg(col("stp").cast('double')).alias("Avg_atm_p"),
                    max(col("smax").cast('double')).alias("Max_air_p"),
                    min(col("smin").cast('double')).alias("Min_air_p"),
                    avg(col("hmdy").cast('double')).alias("Avg_Humidity"),
                    max(col("tmax").cast('double')).alias("Max_temp"),
                    min(col("tmin").cast('double')).alias("Min_temp"),
                    approx_count_distinct("station_code").alias("No_Station")
                ) \
                .select(col("state"), col("year"),
                        col("Avg_atm_p"),
                        col("Max_air_p"),
                        col("Min_air_p"),
                        col("Avg_Humidity"),
                        col("Max_temp"),
                        col("Min_temp"),
                        col("No_Station"),
                        )


query_df2 = weatherdf2.filter((col("stp").cast('double') > -1000) & 
                           (col("smax").cast('double') > -1000) &
                           (col("smin").cast('double') > -1000) &
                           (col("hmdy").cast('double') > -1000) &
                           (col("tmax").cast('double') > -1000) &
                           (col("tmin").cast('double') > -1000)) \
                .groupBy("year", "state") \
                .agg(
                    avg(col("stp").cast('double')).alias("Avg_atm_p"),
                    max(col("smax").cast('double')).alias("Max_air_p"),
                    min(col("smin").cast('double')).alias("Min_air_p"),
                    avg(col("hmdy").cast('double')).alias("Avg_Humidity"),
                    max(col("tmax").cast('double')).alias("Max_temp"),
                    min(col("tmin").cast('double')).alias("Min_temp"),
                    approx_count_distinct("station_code").alias("No_Station")
                ) \
                .select(col("state"), col("year"),
                        col("Avg_atm_p"),
                        col("Max_air_p"),
                        col("Min_air_p"),
                        col("Avg_Humidity"),
                        col("Max_temp"),
                        col("Min_temp"),
                        col("No_Station"),
                        )
query_df1.printSchema()

query_df1 = query_df1.selectExpr("CAST(state AS STRING)", 
                             "CAST(year AS STRING)",
                             "CAST(Avg_atm_p AS STRING)", 
                             "CAST(Max_air_p AS STRING)",
                             "CAST(Min_air_p AS STRING)", 
                             "CAST(Avg_Humidity AS STRING)",
                             "CAST(Max_temp AS STRING)", 
                             "CAST(Min_temp AS STRING)",
                             "CAST(No_Station AS STRING)")

query_df2 = query_df2.selectExpr("CAST(state AS STRING)", 
                             "CAST(year AS STRING)",
                             "CAST(Avg_atm_p AS STRING)", 
                             "CAST(Max_air_p AS STRING)",
                             "CAST(Min_air_p AS STRING)", 
                             "CAST(Avg_Humidity AS STRING)",
                             "CAST(Max_temp AS STRING)", 
                             "CAST(Min_temp AS STRING)",
                             "CAST(No_Station AS STRING)")

# query_df = query_df.selectExpr("CAST(stp AS STRING)")

write_stream1 = query_df1.writeStream\
        .outputMode("update")\
        .format("console")\
        .start() 


write_stream2 = query_df2.writeStream\
        .outputMode("update")\
        .format("console")\
        .start() 

spark.streams.awaitAnyTermination()
