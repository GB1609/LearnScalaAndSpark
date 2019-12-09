package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BeginWithSpark extends App {

  def testing_functions(x: Int): Int = {
    (Math.pow(x, 3) - (Math.pow(x, 2) * 10)).toInt
  }

  val test_list = 1 to 100000

  var begin = System.currentTimeMillis()

  for (x <- test_list)
    println(testing_functions(x))

  var end = System.currentTimeMillis()

  println("TIME SERIAL: " + (end - begin))

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkContext = new SparkContext("local[*]", "TestingSpark")

  begin = System.currentTimeMillis()
  var toPrint=(test_list).map(testing_functions(_))
  end = System.currentTimeMillis()
  for (x <- toPrint)
    println(x)
  println("TIME SPARK: " + (end - begin))

}
