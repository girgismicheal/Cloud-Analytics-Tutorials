// Databricks notebook source
// MAGIC %md
// MAGIC ### Introduction to Spark with Scala
// MAGIC This notebook provides various constructs for Spark using Spark Scala.
// MAGIC 
// MAGIC Topics Covered:
// MAGIC - Spark Session
// MAGIC - Command cells default languages and language switching
// MAGIC - Using basic Spark commands in Scala
// MAGIC - Using file system commands on your nodes
// MAGIC - Loading data into DataFrames
// MAGIC - Using Spark Context to load RDDs
// MAGIC - Previewing sample data
// MAGIC - Shuffling data partitions
// MAGIC - Creating a tableview off your DF for SparkSQL
// MAGIC - Spark SQL contexts and commands
// MAGIC - Comparing DF constructs with SQL constructs

// COMMAND ----------

/* SparkSession associated with this workbook */
spark


// COMMAND ----------

/*
Notebook commands are preset to a specific language. For example "Scala" is the default language. However, that can be switched explicitly in the command cell using the following tags
	%md - Markdown
	%sql - SQL
	%scala - Scala
	%python - Python
	%r - R
	%sh - Shell
    %fs - Filesystem
    
Additionally, use "//" at the beginining of the line for in-line comments.
Use /* comments */ construct for mulit-line comments
*/


// COMMAND ----------

/* Display the list of all uploaded files in your filestore */
display(dbutils.fs.ls("/FileStore/tables"))

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /FileStore/tables/

// COMMAND ----------

/* Using spark context directly to load the sales data returns the low-level APIs */
val textFile = sc.textFile("/FileStore/tables/Sales.csv")
textFile.count()


// COMMAND ----------

/* Loading the same data using the SparkSession returns high-level APIs (DataFrames) */
val salesData = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/FileStore/tables/Sales.csv")
salesData.count()

// COMMAND ----------

/* Sample the firt record */
salesData.first()

// COMMAND ----------

/* Print each record in sales data */
salesData.collect.foreach(println)

// COMMAND ----------

/* Take the top records using take(n), should provide a similar result as salesData.first() */
salesData.take(1)

// COMMAND ----------

salesData.take(2)

// COMMAND ----------

/* Create a dataframe with 1,000 records */
val myRange = spark.range(1000).toDF("number")


// COMMAND ----------

/* Display the newly created range DataFrame */
display(myRange)

// COMMAND ----------

/* Creating a dataset with the number column that are even numbers */
val evenNos = myRange.where("number % 2 = 0")

/* This operation resulted in a dataset not a DataFrame because we are using Scala not Python */

// COMMAND ----------

/* You can display the result using display or show the dataset directly */
evenNos

// COMMAND ----------

/* Count the even numbers */
evenNos.count()


// COMMAND ----------

/** Read CSV flight data - notice that we are loading all the CSV data into flightData frame **/
val flightData = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/FileStore/tables/flight-data/csv/*.csv")


// COMMAND ----------

/* Show a sample of the first 3 records */
flightData.take(3)


// COMMAND ----------

/* Explain the sorting of the flight data */
flightData.sort("count").explain()


// COMMAND ----------

flightData.sort("count")

// COMMAND ----------

/* Set spark to shuffle with up to 5 partitions */
spark.conf.set("spark.sql.shuffle.partitions", "5")


// COMMAND ----------

/* Sort by count and see the first 2 entries */
flightData.sort("count").take(2)


// COMMAND ----------

/* To use Spark SQL on a DataFrame or to perform certain complex filtering, we need to create a temp view from the DataFrame */
flightData.createOrReplaceTempView("flight_data")


// COMMAND ----------

/* Use the SqlContext to query this data vs. dataframe aggregate functions */
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data
GROUP BY DEST_COUNTRY_NAME
""")

val dataFrameWay = flightData
  .groupBy('DEST_COUNTRY_NAME)
  .count()

sqlWay.explain
dataFrameWay.explain


// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DEST_COUNTRY_NAME, count(1) AS Support
// MAGIC FROM flight_data
// MAGIC GROUP BY DEST_COUNTRY_NAME

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DEST_COUNTRY_NAME, count(1) AS Support
// MAGIC FROM flight_data
// MAGIC GROUP BY DEST_COUNTRY_NAME

// COMMAND ----------

val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data
GROUP BY DEST_COUNTRY_NAME
""")

// COMMAND ----------

spark.sql("SELECT max(count) from flight_data").take(1)


// COMMAND ----------


import org.apache.spark.sql.functions.max

flightData.select(max("count")).take(1)


// COMMAND ----------

// in Scala
val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()


// COMMAND ----------

import org.apache.spark.sql.functions.desc

flightData
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .show()


// COMMAND ----------

// in Scala
flightData
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .explain()


// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
// MAGIC FROM flight_data
// MAGIC GROUP BY DEST_COUNTRY_NAME
// MAGIC ORDER BY sum(count) DESC
// MAGIC LIMIT 5
