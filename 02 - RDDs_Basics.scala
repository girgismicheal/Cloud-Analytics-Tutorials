// Databricks notebook source
/*
Loading this data file using the SparkContext will return RDD data APIs.
*/
val textFile = spark.sparkContext.textFile("/FileStore/tables/samples/Adult.csv")
textFile.count()


// COMMAND ----------

/* RDDs can be printed to the console */
textFile.collect.foreach(println)

// COMMAND ----------

/* count the number of lines involved */
textFile.distinct().count()

// COMMAND ----------

/* Get the number of divorced entries */
val divorcedEntries = textFile.filter(line => line.contains("Divorced"))
divorcedEntries.count()


// COMMAND ----------

/* We use scala to split the files by the comma delimiter, map the words and reduce them by the keys - counting their occurence */
val counts = textFile.flatMap(line => line.split(","))
                    .map(word => (word, 1))
                    .reduceByKey(_+_)

counts.foreach(println)


// COMMAND ----------

/* In scala, the immutable transformation pipeline can be executed in a pipeline replacing the lambda with "_" representing parameters in the sequence */
val allwords = textFile.flatMap(_.split("\\W+"))
val words = allwords.filter(!_.isEmpty)
val pairs = words.map((_,1))
val reducedByKey = pairs.reduceByKey(_+_)
val top10words = reducedByKey.takeOrdered(10)(Ordering[Int].reverse.on(_._2))
top10words.foreach(println) 

