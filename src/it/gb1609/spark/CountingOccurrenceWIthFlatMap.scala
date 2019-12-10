package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CountingOccurrenceWIthFlatMap extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkContext = new SparkContext("local[*]", "Counting")
  val lines = sparkContext.textFile("data_testing/book.txt").map(x => x.toLowerCase)
  val words = lines.flatMap(x => x.split("\\W+"))

  val words_to_order = words.map(x => (x, 1)).reduceByKey((x, y) => x + y).sortBy(x=>x._2).collect()
  words_to_order.foreach(println)


}
