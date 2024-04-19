# Databricks notebook source
# MAGIC %md #### Help!

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

# MAGIC %md #### Browsing DBFS

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

display(
    dbutils.fs.ls("/")
)

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

for f in dbutils.fs.ls("/databricks-datasets"):
    if f.size > 0: print(f"File: {f.path} size in MB: {f.size/(1024*1024)}")

# COMMAND ----------

for f in dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/yellow"):
    if f.size > 0: print(f"File: {f.path} size in MB: {f.size/(1024*1024)}")

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/README.md")

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/README.md

# COMMAND ----------

# MAGIC %md #### Managing folders

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/new")

# COMMAND ----------

dbutils.fs.cp("dbfs:/databricks-datasets/README.md", "dbfs:/FileStore/new")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/new

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/new")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/new", True)

# COMMAND ----------

# MAGIC %md #### Mounting Azure Storage

# COMMAND ----------

dbutils.fs.mount(
 source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
 mount_point = "/mnt/<target>",
 extra_configs = {
 "fs.azure.account.auth.type.<storage-account-name>.blob.core.windows.net": "Key",
 "fs.azure.account.key.<storage-account-name>.blob.core.windows.net": "<your-storage-account-key>"
 }
)

# COMMAND ----------

dbutils.fs.mount(
 source = "wasbs://mountingdemo@databrickslakehouse99.blob.core.windows.net",
 mount_point = "/mnt/azuredata",
 extra_configs = {
 "fs.azure.account.auth.type.databrickslakehouse99.blob.core.windows.net": "Key",
 "fs.azure.account.key.databrickslakehouse99.blob.core.windows.net": "<your-key>"
 }
)

# COMMAND ----------

display( dbutils.fs.ls("dbfs:/mnt/azuredata/") )

# COMMAND ----------

dbutils.fs.unmount("/mnt/azuredata")

# COMMAND ----------

# MAGIC %md #### Data analysis

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/asa/small/small.csv

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/databricks-datasets/asa/small/small.csv")

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md #### Run another notebook

# COMMAND ----------

dbutils.notebook.run("./3 - another notebook",1000)

# COMMAND ----------

# MAGIC %run "./3 - another notebook"

# COMMAND ----------

# MAGIC %md #### Other

# COMMAND ----------

dbutils.help()
