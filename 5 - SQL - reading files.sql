-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.head("dbfs:/databricks-datasets/asa/small/small.csv")

-- COMMAND ----------

-- MAGIC %md ##### Simple file read

-- COMMAND ----------

-- MAGIC %md quering single file

-- COMMAND ----------

select *
from csv.`dbfs:/databricks-datasets/asa/small/small.csv`

-- COMMAND ----------

-- MAGIC %md quering directory of files

-- COMMAND ----------

select *
from csv.`dbfs:/databricks-datasets/asa/small/`
limit 3

-- COMMAND ----------

select *
from json.`dbfs:/databricks-datasets/iot/iot_devices.json`

-- COMMAND ----------

select *
from delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`

-- COMMAND ----------

-- MAGIC %md Create table and views

-- COMMAND ----------

create or replace table nytrips as

select *
from delta.`dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`
limit 3

-- COMMAND ----------

select *
from nytrips

-- COMMAND ----------

DESCRIBE EXTENDED nytrips

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/nytrips

-- COMMAND ----------

-- MAGIC %md ##### Providing additional options / unmanaged tables

-- COMMAND ----------

drop table flights

-- COMMAND ----------

-- MAGIC %fs head "dbfs:/databricks-datasets/asa/small/small.csv"

-- COMMAND ----------

CREATE TABLE flights (
 Year INT,
  Month INT,
  DayofMonth INT,
  DayOfWeek INT,
  DepTime INT,
  CRSDepTime INT,
  ArrTime INT,
  CRSArrTime INT,
  UniqueCarrier STRING,
  FlightNum INT,
  TailNum STRING,
  ActualElapsedTime INT,
  CRSElapsedTime INT,
  AirTime INT,
  ArrDelay INT,
  DepDelay INT,
  Origin STRING,
  Dest STRING,
  Distance INT,
  TaxiIn INT,
  TaxiOut INT,
  Cancelled INT,
  CancellationCode STRING,
  Diverted INT,
  CarrierDelay INT,
  WeatherDelay INT,
  NASDelay INT,
  SecurityDelay INT,
  LateAircraftDelay INT
)
using csv
options (
  header = "true", 
  delimiter = ","
)
location "dbfs:/databricks-datasets/asa/small/"

-- COMMAND ----------

select *
from flights
limit 3

-- COMMAND ----------

DESCRIBE EXTENDED flights

-- COMMAND ----------

-- MAGIC %md ##### Insert data to existing table

-- COMMAND ----------

create or replace table iot as
select *
from json.`dbfs:/databricks-datasets/iot/iot_devices.json`

-- COMMAND ----------

select count(*) from iot

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/iot/iot_devices.json","/FileStore/iot_new.csv")

-- COMMAND ----------

INSERT OVERWRITE iot
select *
from json.`/FileStore/iot_new.csv`

-- COMMAND ----------

select count(*) from iot

-- COMMAND ----------

INSERT INTO iot
select *
from json.`/FileStore/iot_new.csv`

-- COMMAND ----------

select count(*) from iot

-- COMMAND ----------

-- MAGIC %md ##### Cleaning

-- COMMAND ----------

DROP TABLE IF EXISTS flights;
DROP TABLE IF EXISTS iot;
DROP TABLE IF EXISTS nytrips;
