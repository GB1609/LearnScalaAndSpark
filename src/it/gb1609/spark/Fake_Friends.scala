package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Fake_Friends extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  val sparkContext = new SparkContext("local[*]", "FakeF")
  val lines = sparkContext.textFile("data_testing/fakefriends.csv")

  for (l <- lines) {
    println(l)
  }
  val ages = lines.map(parseLine)
/*
  val totalByAges = ages.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
*/

}
