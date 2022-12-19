// Databricks notebook source
// MAGIC %md
// MAGIC ##Aggregation and Transformation Libraries
// MAGIC Spark is bundled with many libraries for data aggregation and transformation. Most importantly these libraries work with all Spark Structured APIs - DataSets, DataFrames, SQL.

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /FileStore/tables/retail-data

// COMMAND ----------

/* Load CSV files into a dataset with a max of 5 partitions */
val ds = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/retail-data/all/*.csv")
  .coalesce(5)
ds.cache()
ds.createOrReplaceTempView("dfTable")


// COMMAND ----------

ds.count() == 541909


// COMMAND ----------

/* Count the StockCode column */
import org.apache.spark.sql.functions.count
ds.select(count("StockCode")).show() // 541909


// COMMAND ----------

/* Count Distinct StockCode values */
import org.apache.spark.sql.functions.countDistinct
ds.select(countDistinct("StockCode")).show() // 4070


// COMMAND ----------

/* 
This returns the approximate number of unique non-null values in a group. It is designed to provide aggregations across large data sets where responsiveness is more critial than absolute precision that you would get from countDistinct
*/
import org.apache.spark.sql.functions.approx_count_distinct
ds.select(approx_count_distinct("StockCode", 0.1)).show() // 3364


// COMMAND ----------

/* Pick the first and last Stock code values */
import org.apache.spark.sql.functions.{first, last}
ds.select(first("StockCode"), last("StockCode")).show()


// COMMAND ----------

/* Select the min quantity and max quantity */
import org.apache.spark.sql.functions.{min, max}
ds.select(min("Quantity"), max("Quantity")).show()


// COMMAND ----------

/* Aggregate SUM of quantity */
import org.apache.spark.sql.functions.sum
ds.select(sum("Quantity")).show() // 5176450


// COMMAND ----------

/* Sum distinct quantity values  */
import org.apache.spark.sql.functions.sumDistinct
ds.select(sumDistinct("Quantity")).show() // 29310


// COMMAND ----------

/* Use of aggregate functions, alias, expr, and selectExpr */
import org.apache.spark.sql.functions.{sum, count, avg, expr}

ds.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()


// COMMAND ----------

/* Statistical functions */
import org.apache.spark.sql.functions.{var_pop, stddev_pop}
import org.apache.spark.sql.functions.{var_samp, stddev_samp}
ds.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()


// COMMAND ----------

import org.apache.spark.sql.functions.{skewness, kurtosis}
ds.select(skewness("Quantity"), kurtosis("Quantity")).show()


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}
ds.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()


// COMMAND ----------

/* Collect set and list from a collection */
import org.apache.spark.sql.functions.{collect_set, collect_list}
ds.agg(collect_set("Country"), collect_list("Country")).show()


// COMMAND ----------

/* Group by multiple columns */
ds.groupBy("InvoiceNo", "CustomerId").count().show()


// COMMAND ----------

/* GroupBy InvoiceNo, count the quantity and compare the result with the expr */
import org.apache.spark.sql.functions.count

ds.groupBy("InvoiceNo").agg(
  count("Quantity").alias("quan"),
  expr("count(Quantity)")).show()


// COMMAND ----------

/* Convert InvoiceDate to date/time and push that into a Temp View */
import org.apache.spark.sql.functions.{col, to_date}
val dfWithDate = ds.withColumn("date", to_date(col("InvoiceDate"),
  "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


// COMMAND ----------

/* Add where clause with embedded SQL filters */
import org.apache.spark.sql.functions.col
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity")).show()


// COMMAND ----------

val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")


// COMMAND ----------

val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
  .orderBy("Date")
rolledUpDF.show()


// COMMAND ----------

rolledUpDF.where("Country IS NULL").show()


// COMMAND ----------

rolledUpDF.where("Date IS NULL").show()


// COMMAND ----------

/* Create a cube off the Date and Country fields */
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{grouping_id, sum, expr}

dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
.orderBy(expr("grouping_id()").desc)
.show()


// COMMAND ----------

// in Scala
val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()


// COMMAND ----------

pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()


// COMMAND ----------


