package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AvgFriendByAge extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line: String) = {
    println(line)
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  val sparkContext = new SparkContext("local[*]", "FakeF")
  val lines = sparkContext.textFile("data_testing/fakefriends.csv")

  val ages = lines.map(parseLine)

  val totalByAges = ages.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  val averagesByAge = totalByAges.mapValues(x => x._1 / x._2)
  val results = averagesByAge.collect()
  results.sorted.foreach(println)


}
