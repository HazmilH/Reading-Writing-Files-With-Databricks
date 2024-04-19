# Databricks notebook source
# MAGIC %md ![The best YouTube channel about Databricks](https://i.imgur.com/0QDeTtV.png)

# COMMAND ----------

# MAGIC %md ### Challange 1: Handling files with Databricks 

# COMMAND ----------

# MAGIC %md * Dificulty level: pretty low
# MAGIC * Estimated time: 30 min

# COMMAND ----------

# MAGIC %md You are working as a data engineer in a startup. The entire data flow depends on your work. Your colleague from the Data Science team has just informed you that they need to perform quick analytics about customers and products for management. The data is provided in the form of CSV files. While they have extensive knowledge in their own field, they don't know how to load the data. They need your help to load that data into tables

# COMMAND ----------

# MAGIC %md There are two necessary files:
# MAGIC
# MAGIC 1. One with customers - `dbfs:/databricks-datasets/retail-org/customers/customers.csv`
# MAGIC 2. Another one with products - `dbfs:/databricks-datasets/retail-org/products/products.csv`

# COMMAND ----------

# MAGIC %md The files will only be available in this location for a limited time, so you'll need to copy them as soon as possible. Good luck!

# COMMAND ----------

# MAGIC %md ====================================

# COMMAND ----------

# MAGIC %md #### Customers 

# COMMAND ----------

# MAGIC %md To attact customers data, you decided to use pyspark. But before you will do that, you need to create a target folder /FileStore/raw/customers

# COMMAND ----------

<FILL IN>

# COMMAND ----------

# MAGIC %md Copy customers file to that location

# COMMAND ----------

dbutils.fs.<FILL IN>

# COMMAND ----------

# MAGIC %md Check a size of that file in MB

# COMMAND ----------

<FILL IN>:
    print(f.size/(1024*1014))

# COMMAND ----------

# MAGIC %md Quickly and easly display file content. Do not worry about headers or columns types

# COMMAND ----------

display(spark.read<FILL IN>)

# COMMAND ----------

# MAGIC %md Create a pyspark dataframe, based on that file. Plus:
# MAGIC * use first row as a header
# MAGIC * automaticly infer schema
# MAGIC * add additional columns: "ingestion_date" and "input_file_name" 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = (spark.read
            .option(<FILL IN>, True)
            .option(<FILL IN>, True)
            .csv("/FileStore/raw/customers")
            .withColumn("ingestion_date", <FILL IN>)
            .withColumn("input_file_name", <FILL IN>))

display(df)

# COMMAND ----------

# MAGIC %md Automaticly infering a schema takes time... You believe that you will be getting this request more frequntly in the future and that future data sets will be way larger. You have devided to declare schema manually, insted of infering in automaticly.

# COMMAND ----------

schema = "customer_id INT, tax_id DOUBLE, tax_code STRING, customer_name STRING, state STRING, city STRING, postcode STRING, street STRING, number STRING, unit STRING, region STRING, district STRING, lon DOUBLE, lat DOUBLE, ship_to_address STRING, valid_from INT, valid_to DOUBLE, units_purchased DOUBLE, loyalty_segment INT, ingestion_date TIMESTAMP, input_file_name STRING"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = (spark.read
            .<FILL IN>)

display(df)

# COMMAND ----------

# MAGIC %md **CHECK**: You will occasionally check the rusults. Run a cell below to see if task has been completed properly. Make sure that your dataframe is called "df"

# COMMAND ----------

from pyspark.sql import DataFrame

assert df is None or isinstance(df,DataFrame), "DataFrame 'df' does not exist"
assert df.columns == ['customer_id', 'tax_id', 'tax_code', 'customer_name', 'state','city','postcode','street','number','unit','region','district','lon','lat','ship_to_address','valid_from','valid_to','units_purchased','loyalty_segment','ingestion_date','input_file_name'], "Columns do not match. Have you read a first row as a header?"
assert df.dtypes == [('customer_id', 'int'),('tax_id', 'double'),('tax_code', 'string'),('customer_name', 'string'),('state', 'string'),('city', 'string'),('postcode', 'string'),('street', 'string'),('number', 'string'),('unit', 'string'),('region', 'string'),('district', 'string'),('lon', 'double'),('lat', 'double'),('ship_to_address', 'string'),('valid_from', 'int'),('valid_to', 'double'),('units_purchased', 'double'),('loyalty_segment', 'int'),('ingestion_date', 'timestamp'),('input_file_name', 'string')], "Please check if you column types are the same as columns types you got when infering schema automaticly: [('customer_id', 'int'),('tax_id', 'double'),('tax_code', 'string'),('customer_name', 'string'),('state', 'string'),('city', 'string'),('postcode', 'string'),('street', 'string'),('number', 'string'),('unit', 'string'),('region', 'string'),('district', 'string'),('lon', 'double'),('lat', 'double'),('ship_to_address', 'string'),('valid_from', 'int'),('valid_to', 'double'),('units_purchased', 'double'),('loyalty_segment', 'int'),('ingestion_date', 'timestamp'),('input_file_name', 'string')]"

print("Well done!")

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE** Sometimes, the column types recognized during automatic schema inference are questionable, and whenever possible, they should be verified manually.

# COMMAND ----------

# MAGIC %md Your data science collegues told you that they are using a desktop software for models and that they can handle only part of that data. 
# MAGIC Create a second dataframe which is consist of random sample (10%) of the previous one. Make sure that each row appears only once.

# COMMAND ----------

df.count()

# COMMAND ----------

df2 = df.s<FILL IN>

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md Create global temporary view, for second dataframe, as 'customers_view'

# COMMAND ----------

df2.<FILL IN>("customers_view")

# COMMAND ----------

# MAGIC %md Save that dataframe as a table named customers_table

# COMMAND ----------

df2.<FILL IN>("customers_table")

# COMMAND ----------

# MAGIC %md Check if table is managed or unmanaged and in that location files are stored

# COMMAND ----------

# MAGIC %sql
# MAGIC <FILL IN> extended customers_table

# COMMAND ----------

# MAGIC %md List all the files in that location, using magic command fs

# COMMAND ----------

<FILL IN> "dbfs:/user/hive/warehouse/customers_table"

# COMMAND ----------

# MAGIC %md Your collegue told you that he does not need a customer data anymore. Drop the table and check if files still exists

# COMMAND ----------

# MAGIC %sql
# MAGIC d<FILL IN>e customers_table

# COMMAND ----------

# MAGIC %<FILL IN> ls "dbfs:/user/hive/warehouse/customers_table"

# COMMAND ----------

# MAGIC %md ##### Products

# COMMAND ----------

# MAGIC %md For handling file with products, you have decided to use SQL. Nevertheless, you need to create a folder /FileStore/raw/products first.

# COMMAND ----------

dbutils.<FILL IN>("/FileStore/raw/products")

# COMMAND ----------

# MAGIC %md Copy dbfs:/databricks-datasets/retail-org/products/products.csv to that folder

# COMMAND ----------

dbutils.<FILL IN>("dbfs:/databricks-datasets/retail-org/products/products.csv", "/FileStore/raw/products")

# COMMAND ----------

# MAGIC %md Display content of files using SQL. Dont worry about header or column types

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC <FILL IN>`/FileStore/raw/products`

# COMMAND ----------

# MAGIC %md Things does not seems to be as easy as it looks like. Header has not been recognize, schema is wrong, even columns have not been displayed properly, because a delimiter is ";" instead of ",". You have decided to register this file as a table "products_table" and fix above issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS products_table;
# MAGIC
# MAGIC CREATE TABLE products_table (
# MAGIC   product_id STRING, 
# MAGIC   product_category STRING, 
# MAGIC   product_name STRING, 
# MAGIC   sales_price DOUBLE, 
# MAGIC   EAN13 LONG, 
# MAGIC   EAN5 LONG, 
# MAGIC   product_unit STRING
# MAGIC )
# MAGIC <FILL IN>;

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK**

# COMMAND ----------

assert spark.table("products_table"), "Table 'products_table' does not exist"

print("Well done!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products_table;

# COMMAND ----------

# MAGIC %md Check if table is managed or unmanaged and what is location of files

# COMMAND ----------

# MAGIC %sql
# MAGIC <FILL IN> products_table

# COMMAND ----------

# MAGIC %md Your collegues told you that this table is no longer needed. Drop it and check if files in aboves location still exists. 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC <FILL IN> products_table; 

# COMMAND ----------

# MAGIC %fs <FILL IN> dbfs:/FileStore/raw/products

# COMMAND ----------

# MAGIC %md ##### Cleanup

# COMMAND ----------

# MAGIC %md After helping your collegues, you have been officialy called a "Hero of the day". After big bonus, high salary rise has followed. You dont know anymore what to do with that money, but what you know is that every good data engineer is cleaning up environment after a job is finished. 

# COMMAND ----------

# MAGIC %md Delete whole dbfs:/FileStore/raw folder with one command

# COMMAND ----------

<FILL IN>("dbfs:/FileStore/raw", <FILL IN>)
