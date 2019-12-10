package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SumAmountByPeople extends App {

  def split(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkContext = new SparkContext("local[*]", "Counting")
  val dates = sparkContext.textFile("data_testing/customer-orders.csv").map(split)
  val results = dates.reduceByKey((x, y) => x + y).sortBy(x => x._2).collect()
  results.foreach(println)
}
