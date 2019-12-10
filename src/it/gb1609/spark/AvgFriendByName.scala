package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AvgFriendByName extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkContext = new SparkContext("local[*]", "FakeF")
  val lines = sparkContext.textFile("data_testing/fakefriends.csv")

  val tuples = lines.map(line => {
    val fields = line.split(",")
    val name = fields(1)
    val numFriends = fields(3).toInt
    (name, numFriends)
  })

  val totalByNames = tuples.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  val averagesByName = totalByNames.mapValues(x => x._1 / x._2)
  val results = averagesByName.collect()
  results.sortBy(x=>x._2).reverse.foreach(println)


}
