package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object BeginWithSpark extends App {

  def testing_functions(x: Int): Int = {
    (Math.pow(x, 3) - (Math.pow(x, 2) * 10)).toInt
  }

  val testList = 1 to 100000000

  var begin = System.currentTimeMillis()

  val serialUse = testList.map(testing_functions)
  println(serialUse.length)
  var end = System.currentTimeMillis()

  println("TIME SERIAL: " + (end - begin))

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkContext = new SparkContext("local[*]", "TestingSpark")
  val rddTestList = sparkContext.parallelize(testList)

  begin = System.currentTimeMillis()
  val useSpark = rddTestList.map(testing_functions)
  println(useSpark.count)
  end = System.currentTimeMillis()
  println("TIME SPARK: " + (end - begin))

}
