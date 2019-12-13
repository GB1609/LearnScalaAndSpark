package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object IntToSparkSQL extends App {

  case class Person(id: Int, name: String, age: Int, numFriends: Int)

  def createPerson(line: String) = {
    val fields = line.split(",")
    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder.appName("IntSQL").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp").getOrCreate()
  val lines = sparkSession.sparkContext.textFile("data_testing/fakefriends.csv")
  val peoples = lines.map(createPerson)

  import sparkSession.implicits._

  val schemaPeople = peoples.toDS().cache()
  schemaPeople.createOrReplaceTempView("peoples")
  val results = sparkSession.sql("Select * from peoples where age>12 and age<19").collect()

  //or with functions
  schemaPeople.select("name").filter(schemaPeople("age") > 14 and schemaPeople("age") < 19).show()

  sparkSession.stop()
}
