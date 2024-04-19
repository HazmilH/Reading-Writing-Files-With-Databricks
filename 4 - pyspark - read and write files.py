# Databricks notebook source
display(
    dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow")
)

# COMMAND ----------

# MAGIC %md #### Basic data ingestion

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow")

# COMMAND ----------

print(df)

# COMMAND ----------

df.show(3)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md or...

# COMMAND ----------

df = spark.read.format("csv").load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow")

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.format("csv").load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019*")

# COMMAND ----------

df.count()

# COMMAND ----------

files = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019*"

# COMMAND ----------

# MAGIC %md #### Ingestion data options

# COMMAND ----------

# MAGIC %md ##### Header

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(files)
display(df)

# COMMAND ----------

# MAGIC %md ##### Infer Schema

# COMMAND ----------

# MAGIC %md Check your schema

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %md Automatic schema detection

# COMMAND ----------

df = (spark.read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("samplingRatio", 0.001)
            .load(files))

df.printSchema()

# COMMAND ----------

# MAGIC %md ##### Define a schema

# COMMAND ----------

# MAGIC %md Manual

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField("vendor_id", StringType(), nullable=True),
    StructField("pickup_datetime", TimestampType(), nullable=True),
    StructField("dropoff_datetime", TimestampType(), nullable=True),
    StructField("passenger_count", IntegerType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("pickup_longitude", DoubleType(), nullable=True),
    StructField("pickup_latitude", StringType(), nullable=True),
    StructField("rate_code", IntegerType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("dropoff_longitude", DoubleType(), nullable=True),
    StructField("dropoff_latitude", DoubleType(), nullable=True),
    StructField("payment_type", StringType(), nullable=True),
    StructField("fare_amount", StringType(), nullable=True),
    StructField("surcharge", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True)
])

# COMMAND ----------

df = spark.read.format("csv").schema(schema).load(files)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md DDL

# COMMAND ----------

schema_string = "vendor_id INTEGER, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, passenger_count INT, trip_distance DOUBLE, pickup_longitude DOUBLE, pickup_latitude STRING, rate_code INT, store_and_fwd_flag STRING, dropoff_longitude DOUBLE, dropoff_latitude DOUBLE, payment_type STRING, fare_amount STRING, surcharge DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, total_amount DOUBLE"

# COMMAND ----------

df = spark.read.format("csv").schema(schema_string).option("header", True).load(files)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ##### Adding metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = (spark.read
      .format("csv")
      .option("header", True)
      .schema(schema_string)
      .load(files)
      .withColumn("ingestion_time", current_timestamp().cast("long"))
      .withColumn("source_file_name", input_file_name()))

display(df)

# COMMAND ----------

# MAGIC %md #### Writing data to a file

# COMMAND ----------

df.count()

# COMMAND ----------

dfs = df\
        .sample(withReplacement=False, fraction=0.01, seed=7)\
        .cache()

# COMMAND ----------

dfs.count()

# COMMAND ----------

dfs.write.format("json").save("/FileStore/small1")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/small1"))

# COMMAND ----------

# MAGIC %fs 
# MAGIC head "dbfs:/FileStore/small1/part-00005-tid-3671075815550631840-c65594a3-dd03-4869-af83-49b5ebaf2ff8-653-1-c000.json"

# COMMAND ----------

dfs.write.format("json").save("/FileStore/small1")

# COMMAND ----------

# MAGIC %md ##### Save modes

# COMMAND ----------

# MAGIC %md Overwrite

# COMMAND ----------

dfs.write\
    .format("json")\
    .mode("overwrite")\
    .save("/FileStore/small1")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/small1"))

# COMMAND ----------

# MAGIC %md Append

# COMMAND ----------

dfs.write\
    .format("json")\
    .mode("append")\
    .save("/FileStore/small1")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/small1"))

# COMMAND ----------

# MAGIC %md Ignore

# COMMAND ----------

dfs.write\
    .format("json")\
    .mode("ignore")\
    .save("/FileStore/small1")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/small1"))

# COMMAND ----------

# MAGIC %md Error / Errorifexists - default behaviour

# COMMAND ----------

dfs.write\
    .format("json")\
    .mode("error")\
    .save("/FileStore/small1")

# COMMAND ----------

# MAGIC %md ##### Compresion

# COMMAND ----------

dfs.write\
    .format("json")\
    .option("compression", "gzip")\
    .mode("overwrite")\
    .save("/FileStore/small1_compress")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/small1_compress"))

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/small1_compress/part-00004-tid-3974682560693346850-c38e89e4-6036-4fd9-b7da-6d0dc1a9283d-704-1-c000.json.gz

# COMMAND ----------

display( spark.read.json("/FileStore/small1_compress") )

# COMMAND ----------

dfs.write\
    .format("parquet")\
    .option("compression", "snappy")\
    .mode("overwrite")\
    .save("/FileStore/small1_compress")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/small1_compress"))

# COMMAND ----------

# MAGIC %md ##### Logs

# COMMAND ----------

display(dbutils.fs.head("dbfs:/FileStore/small1_compress/_committed_3974682560693346850"))


# COMMAND ----------

# MAGIC %md #### Saving data as a table or view

# COMMAND ----------

# MAGIC %md To table

# COMMAND ----------

dfs.write.saveAsTable("ny_taxi_small")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from ny_taxi_small

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc ny_taxi_small

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended ny_taxi_small

# COMMAND ----------

display(
    dbutils.fs.ls("dbfs:/user/hive/warehouse/ny_taxi_small")
)

# COMMAND ----------

# MAGIC %md To Global Temporary View

# COMMAND ----------

dfs.createOrReplaceGlobalTempView("ny_taxi_small_gv")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from global_temp.ny_taxi_small_gv

# COMMAND ----------

# MAGIC %md To Temporary View

# COMMAND ----------

dfs.createOrReplaceTempView("ny_taxi_small_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * 
# MAGIC from ny_taxi_small_v

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended ny_taxi_small_v

# COMMAND ----------

# MAGIC %md Droping a table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table ny_taxi_small
