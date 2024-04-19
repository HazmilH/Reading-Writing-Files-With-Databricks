# Databricks notebook source
display( dbutils.fs.ls("/databricks-datasets") )

# COMMAND ----------

# MAGIC %md ### NY Taxi

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/"))

# COMMAND ----------

spark.read.text("dbfs:/databricks-datasets/nyctaxi/readme_nyctaxi.txt").show(1000,truncate=False)

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow")

# COMMAND ----------

spark.read.option("header",True).csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow").display()

# COMMAND ----------

# MAGIC %md ### Retail org

# COMMAND ----------

display( dbutils.fs.ls("/databricks-datasets/retail-org/"))

# COMMAND ----------

spark.read.text("dbfs:/databricks-datasets/retail-org/README.md").show(1000,truncate=False)

# COMMAND ----------

# MAGIC %md ### Flights

# COMMAND ----------

display( dbutils.fs.ls("/databricks-datasets/asa/airlines"))

# COMMAND ----------

display(
    spark.read.option("header", True).csv("dbfs:/databricks-datasets/asa/airlines/2008.csv")
)
