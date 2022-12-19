// Databricks notebook source
// MAGIC %md
// MAGIC ##Working with Typed Dataset APIs
// MAGIC Datasets are typed. In this notebook, we will explore how:
// MAGIC 
// MAGIC 1. DataFrames can be mapped to a Typed structure.
// MAGIC 2. Creating and using Scala functions on a typed collection. 
// MAGIC 3. Using JOINs on DataSets
// MAGIC 4. Grouping DataSet data by specific columns

// COMMAND ----------

/* Scala class for flight */
case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String, count: BigInt)


// COMMAND ----------

// MAGIC %md
// MAGIC ###Parquet
// MAGIC Parquet is an open source file format available to any project in the Hadoop ecosystem. Apache Parquet is designed for efficient as well as performant flat columnar storage format of data compared to row based files like CSV or TSV files.
// MAGIC 
// MAGIC Read more about this file format [here.](https://databricks.com/glossary/what-is-parquet)

// COMMAND ----------

/* Reads a Parquet file and map that to a typed dataset using the Flight class */
val flightsDF = spark.read
  .parquet("/FileStore/tables/flight-data/parquet/2010-summary.parquet/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet")
val flights = flightsDF.as[Flight]


// COMMAND ----------

/* Use show() function to preview the flight dataset */
flights.show(2)


// COMMAND ----------

flights.first.DEST_COUNTRY_NAME // United States


// COMMAND ----------

/* In this example, we use a Scala function to veriy that the variable flight row (Type Flight) has the same origin and destination countries */
def originIsDestination(flight_row: Flight): Boolean = {
  return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
}


// COMMAND ----------

/* Using lambda, we can easily filter out the flights that meet the condition in the function */
val localFlights = flights.filter(flight_row => originIsDestination(flight_row)).first()


// COMMAND ----------

/* Using a collect function, we can create a collect data records that meets the conditions of our filter */
flights.collect().filter(flight_row => originIsDestination(flight_row))


// COMMAND ----------

val destinations = flights.map(f => f.DEST_COUNTRY_NAME)


// COMMAND ----------

val sampleDistations = destinations.take(5)


// COMMAND ----------

val flightsMeta = spark.range(500)
  .map(x => (x, scala.util.Random.nextLong))

// COMMAND ----------

/* Using the map function to create a structured dataset of the Type FlightMetadata */
case class FlightMetadata(count: BigInt, randomData: BigInt)

/* 500 entries with an index column and another with random numbers */
val flightsMeta = spark.range(500)
  .map(x => (x, scala.util.Random.nextLong))
  .withColumnRenamed("_1", "count")
  .withColumnRenamed("_2", "randomData")
  .as[FlightMetadata]


// COMMAND ----------

/* Now, we can join the datasets using the count columns */
val flights2 = flights
  .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))


// COMMAND ----------

/* This expressions selects from the multiple-dataset, but picking the DEST_COUNTRY_NAME from the 1st dataset */
flights2.selectExpr("_1.DEST_COUNTRY_NAME")


// COMMAND ----------

/* Picking the first two records - that shows the two Flight and FlightMeta data objects */
flights2.take(2)


// COMMAND ----------

/* 
Using the groupBy function on DEST_COUNTRY_NAME will result in a DataFrame.
Using the groupByKey function using the same DEST_COUNTRY_NAME will result in a dataset.
*/
flights.groupBy("DEST_COUNTRY_NAME").count()


// COMMAND ----------

flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()


// COMMAND ----------

flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain


// COMMAND ----------


