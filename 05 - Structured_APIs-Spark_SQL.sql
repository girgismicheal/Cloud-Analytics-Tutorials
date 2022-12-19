-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Working with Spark SQL
-- MAGIC Spark SQL allows developers to leverage SQL constructs with distributed datasets. 
-- MAGIC 
-- MAGIC Spark SQL uses a Hive metastore to manage the metadata of persistent relational entities (e.g. databases, tables, columns, partitions) in a relational database (for fast access).
-- MAGIC 
-- MAGIC In this notebook, we will explore how:
-- MAGIC 
-- MAGIC 1. CREATing tables in Spark
-- MAGIC 2. Basic SELECT statement
-- MAGIC 3. The use of LIMIT
-- MAGIC 4. CREATE and DROP IF EXISTS
-- MAGIC 5. Various ANSI SQL constructs supported in SPARK

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ##Example I
-- MAGIC Load Adult.csv and analyze the dataset for the most unique words. T

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val adultData = spark
-- MAGIC   .read
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .option("header", "true")
-- MAGIC   .csv("/FileStore/tables/samples/Adult.csv")
-- MAGIC // Creating a temp view is an important step to using your dataframe in Spark SQL
-- MAGIC adultData.createOrReplaceTempView("adult")

-- COMMAND ----------

/* Sample the data */
SELECT *
FROM adult
LIMIT 20

-- COMMAND ----------

/* Get the divorced entries count */

SELECT count(*) As Divorced_Entries
FROM adult 
WHERe marital_status="Divorced"

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/adult_words", true)

-- COMMAND ----------

DROP TABLE IF EXISTS adult_words;
CREATE TABLE adult_words (
  word STRING,
  count LONG);

INSERT INTO adult_words
SELECT education, count(*) As Count
FROM adult
GROUP BY education

UNION

SELECT marital_status, count(*)
FROM adult
GROUP BY marital_status

UNION

SELECT race, count(*)
FROM adult
GROUP BY race

UNION 

SELECT sex, count(*)
FROM adult
GROUP BY sex

UNION

SELECT native_country, count(*)
FROM adult
GROUP BY native_country

UNION

SELECT occupation, count(*)
FROM adult
GROUP BY occupation

-- COMMAND ----------

SELECT word, SUM(count) As Support
FROM adult_words
GROUP BY word
ORDER BY Support DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Example II
-- MAGIC Create and load flights data from multiple JSON & CSV files.

-- COMMAND ----------

/* 
Create the table flights and load JSON data into this table.
Note: Ensure that you have imported FileStore/tables/flight-data/json
*/
DROP TABLE IF EXISTS flights;
CREATE TABLE flights (
  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
USING JSON OPTIONS (path '/FileStore/tables/flight-data/json/*.json')


-- COMMAND ----------

/* Sample the data to ensure that the loading was a success */
SELECT *
FROM flights
LIMIT 100

-- COMMAND ----------

DROP TABLE IF EXISTS flights_csv;
CREATE TABLE flights_csv (
  DEST_COUNTRY_NAME STRING,
  ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
  count LONG)
USING csv OPTIONS (header true, path '/FileStore/tables/flight-data/csv/*.csv');


-- COMMAND ----------

/** Preview the dataset **/
SELECT *
FROM flights_csv
LIMIT 10;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC /* Remove existing Parquet files */
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/flights_from_select", true)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/partitioned_flights", true)

-- COMMAND ----------

DROP TABLE IF EXISTS flights_from_select;
CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS flights_from_select
  AS SELECT * FROM flights


-- COMMAND ----------

DROP TABLE IF EXISTS partitioned_flights;
CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5


-- COMMAND ----------

SELECT *
FROM partitioned_flights
LIMIT 5;

-- COMMAND ----------

DROP TABLE IF EXISTS hive_flights;
CREATE EXTERNAL TABLE hive_flights (
  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/FileStore/tables/data/flight-data-hive/'


-- COMMAND ----------

DROP TABLE IF EXISTS hive_flights_2;
CREATE EXTERNAL TABLE hive_flights_2
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/data/flight-data-hive/' AS SELECT * FROM flights


-- COMMAND ----------

INSERT INTO flights_from_select
  SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count 
  FROM flights 
  LIMIT 20


-- COMMAND ----------

DESCRIBE TABLE flights_csv


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Misc SQL Constructs
-- MAGIC 
-- MAGIC - Creating View from data cohorts
-- MAGIC - Creating local and global temp views 
-- MAGIC - SQL environment constructs to show databases, tables, etc.

-- COMMAND ----------

DROP VIEW IF EXISTS just_usa_view;

CREATE VIEW just_usa_view AS
  SELECT * FROM flights WHERE dest_country_name = 'United States'


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
  SELECT * FROM flights WHERE dest_country_name = 'United States'


-- COMMAND ----------

-- DROP VIEW IF EXISTS just_usa_global_view_temp;
CREATE OR REPLACE GLOBAL TEMP VIEW just_usa_global_view_temp AS
  SELECT * FROM flights WHERE dest_country_name = 'United States'


-- COMMAND ----------

SHOW TABLES


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
  SELECT * FROM flights WHERE dest_country_name = 'United States'


-- COMMAND ----------

SELECT * FROM just_usa_view_temp


-- COMMAND ----------

EXPLAIN SELECT * FROM just_usa_view


-- COMMAND ----------

EXPLAIN SELECT * FROM flights WHERE dest_country_name = 'United States'


-- COMMAND ----------

DROP VIEW IF EXISTS just_usa_view;


-- COMMAND ----------

SHOW DATABASES


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS some_db


-- COMMAND ----------

USE some_db


-- COMMAND ----------

SHOW tables;

-- COMMAND ----------

SELECT * FROM default.flights


-- COMMAND ----------

SELECT current_database()


-- COMMAND ----------

USE default;


-- COMMAND ----------

-- Drop the database using IF EXISTS alongside all its objects
DROP DATABASE IF EXISTS some_db CASCADE;


-- COMMAND ----------

SELECT
  CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1
       WHEN DEST_COUNTRY_NAME = 'Egypt' THEN 0
       ELSE -1 END As Country_Index
FROM partitioned_flights


-- COMMAND ----------

CREATE VIEW IF NOT EXISTS nested_data AS
  SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights


-- COMMAND ----------

SELECT * FROM nested_data


-- COMMAND ----------

SELECT country.DEST_COUNTRY_NAME, count FROM nested_data


-- COMMAND ----------

SELECT country.*, count FROM nested_data


-- COMMAND ----------

SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
  collect_set(ORIGIN_COUNTRY_NAME) as origin_set
FROM flights GROUP BY DEST_COUNTRY_NAME


-- COMMAND ----------

SELECT DEST_COUNTRY_NAME, ARRAY(1, 2, 3) FROM flights


-- COMMAND ----------

SELECT DEST_COUNTRY_NAME as new_name, collect_list(count)[0]
FROM flights GROUP BY DEST_COUNTRY_NAME


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW flights_agg AS
  SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts
  FROM flights GROUP BY DEST_COUNTRY_NAME


-- COMMAND ----------

SELECT explode(collected_counts), DEST_COUNTRY_NAME FROM flights_agg


-- COMMAND ----------

SHOW FUNCTIONS


-- COMMAND ----------

SHOW SYSTEM FUNCTIONS


-- COMMAND ----------

SHOW USER FUNCTIONS


-- COMMAND ----------

SHOW FUNCTIONS "s*";


-- COMMAND ----------

SHOW FUNCTIONS LIKE "collect*";


-- COMMAND ----------

SELECT dest_country_name FROM flights
GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5


-- COMMAND ----------

SELECT * FROM flights
WHERE origin_country_name IN (SELECT dest_country_name FROM flights
      GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5)


-- COMMAND ----------

SELECT * FROM flights f1
WHERE EXISTS (SELECT 1 FROM flights f2
            WHERE f1.dest_country_name = f2.origin_country_name)
AND EXISTS (SELECT 1 FROM flights f2
            WHERE f2.dest_country_name = f1.origin_country_name)


-- COMMAND ----------

SELECT *, (SELECT max(count) FROM flights) AS maximum FROM flights


-- COMMAND ----------

SET spark.sql.shuffle.partitions=20


-- COMMAND ----------


