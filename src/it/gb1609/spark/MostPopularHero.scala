package it.gb1609.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularHero extends App {

  def parseNames(line: String): Option[(Int, String)] = {
    var fields = line.split("\\s")
    if (fields.length > 1) {
      Some(fields(0).toInt,fields(1).replaceAll("\"",""))
    }
    else
      None
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MostPopularSuperhero")
  val name = sc.textFile("data_testing/Marvel-names.txt")
  val nmRDD = name.flatMap(parseNames)
  }
